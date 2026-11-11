package zenoh

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
)

// Interop with zenoh-pico (the official C implementation), driven through
// testdata/zpico/zpico.c, which the tests compile against a zenoh-pico build:
//
//	git clone -b 1.10.2 https://github.com/eclipse-zenoh/zenoh-pico
//	cmake -S zenoh-pico -B pico-build -DBUILD_TESTING=OFF -DZ_FEATURE_UNSTABLE_API=1
//	cmake --build pico-build
//	ZENOH_PICO=$PWD/pico-build ZENOH_ROUTER=go go test -run Pico ./zenoh/

var picoBin struct {
	once sync.Once
	path string
	err  error
}

// picoHarness compiles zpico.c once per test binary.
func picoHarness(t *testing.T) string {
	t.Helper()
	build := os.Getenv("ZENOH_PICO")
	if build == "" {
		t.Skip("set ZENOH_PICO to a zenoh-pico cmake build directory")
	}
	picoBin.once.Do(func() {
		cache, err := os.ReadFile(filepath.Join(build, "CMakeCache.txt"))
		if err != nil {
			picoBin.err = err
			return
		}
		m := regexp.MustCompile(`(?m)^zenohpico_SOURCE_DIR:STATIC=(.*)$`).FindSubmatch(cache)
		if m == nil {
			picoBin.err = fmt.Errorf("no zenohpico_SOURCE_DIR in %s/CMakeCache.txt", build)
			return
		}
		// Built next to the zenoh-pico build (one binary, rebuilt each run;
		// written under a temporary name and renamed, for parallel runs).
		picoBin.path = filepath.Join(build, "zpico-tiny-zenoh-go")
		tmp := fmt.Sprintf("%s.%d", picoBin.path, os.Getpid())
		lib := filepath.Join(build, "lib")
		out, err := exec.Command("cc", "-O1", "-std=gnu11", "-DZENOH_LINUX",
			"-I"+filepath.Join(build, "include"), "-I"+filepath.Join(string(m[1]), "include"),
			"testdata/zpico/zpico.c", "-L"+lib, "-lzenohpico", "-Wl,-rpath,"+lib, "-lpthread",
			"-o", tmp).CombinedOutput()
		if err != nil {
			picoBin.err = fmt.Errorf("cc: %v\n%s", err, out)
			return
		}
		picoBin.err = os.Rename(tmp, picoBin.path)
	})
	if picoBin.err != nil {
		t.Fatal(picoBin.err)
	}
	return picoBin.path
}

type picoEvent struct {
	Ev         string  `json:"ev"`
	ZID        string  `json:"zid"`
	Kind       string  `json:"kind"`
	Key        string  `json:"key"`
	Encoding   string  `json:"encoding"`
	Payload    string  `json:"payload"`
	Attachment *string `json:"attachment"`
	Params     string  `json:"params"`
	Size       int     `json:"size"`
	Sum        string  `json:"sum"`
	What       string  `json:"what"`
}

type picoProc struct {
	t      *testing.T
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	events chan picoEvent
	zid    string
}

// startPico runs the harness: args are its flags then CMD and its args.
// Long-lived commands are waited for until they're ready.
func startPico(t *testing.T, args ...string) *picoProc {
	t.Helper()
	cmd := exec.Command(picoHarness(t), args...)
	cmd.Stderr = os.Stderr
	stdin, _ := cmd.StdinPipe()
	out, _ := cmd.StdoutPipe()
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	p := &picoProc{t: t, cmd: cmd, stdin: stdin, events: make(chan picoEvent, 256)}
	go func() {
		sc := bufio.NewScanner(out)
		for sc.Scan() {
			var ev picoEvent
			if json.Unmarshal(sc.Bytes(), &ev) == nil {
				p.events <- ev
			} else {
				t.Logf("zpico: %s", sc.Bytes())
			}
		}
		close(p.events)
	}()
	t.Cleanup(p.stop)
	return p
}

// startPicoReady starts a long-lived harness command and waits for it.
func startPicoReady(t *testing.T, args ...string) *picoProc {
	t.Helper()
	p := startPico(t, args...)
	p.zid = p.next("ready").ZID
	return p
}

func (p *picoProc) stop() {
	_ = p.stdin.Close()
	done := make(chan struct{})
	go func() { _ = p.cmd.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		_ = p.cmd.Process.Kill()
		<-done
	}
}

// next returns the next event, which must be of type ev.
func (p *picoProc) next(ev string) picoEvent {
	p.t.Helper()
	select {
	case e, ok := <-p.events:
		if !ok {
			p.t.Fatalf("zpico exited waiting for %q", ev)
		}
		if e.Ev != ev {
			p.t.Fatalf("zpico: got %+v, want %q", e, ev)
		}
		return e
	case <-time.After(5 * time.Second):
		p.t.Fatalf("zpico: timeout waiting for %q", ev)
	}
	return picoEvent{}
}

// until collects events up to (excluding) the first of type ev.
func (p *picoProc) until(ev string) []picoEvent {
	p.t.Helper()
	var got []picoEvent
	for {
		select {
		case e, ok := <-p.events:
			if !ok {
				p.t.Fatalf("zpico exited waiting for %q (got %+v)", ev, got)
			}
			if e.Ev == ev {
				return got
			}
			if e.Ev == "ready" {
				continue
			}
			if e.Ev == "fail" {
				p.t.Fatalf("zpico: %s failed", e.What)
			}
			got = append(got, e)
		case <-time.After(5 * time.Second):
			p.t.Fatalf("zpico: timeout waiting for %q (got %+v)", ev, got)
		}
	}
}

// runPico runs a one-shot harness command and returns its events.
func runPico(t *testing.T, args ...string) []picoEvent {
	t.Helper()
	p := startPico(t, args...)
	_ = p.stdin.Close()
	return p.until("done")
}

// picoPattern is zpico.c's pattern(): byte(j*7 + seed).
func picoPattern(n, seed int) []byte {
	b := make([]byte, n)
	for j := range b {
		b[j] = byte(j*7 + seed)
	}
	return b
}

func fnvSum(b []byte) string {
	h := fnv.New32a()
	h.Write(b)
	return fmt.Sprintf("%08x", h.Sum32())
}

// picoBig is larger than zenoh-pico's 2048-byte batches (so it travels in
// FRAGMENTs both ways) and within its default 4096-byte reassembly limit.
const picoBig = 3000

func TestPicoPubGoSub(t *testing.T) {
	loc := routerLocator(t)
	pfx := uniq(t)
	s := openSession(t)
	sink := newSink()
	sub, err := s.DeclareSubscriber(pfx+"/**", sink.handler)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Undeclare()
	settle()

	runPico(t, "-e", loc, "put", pfx+"/put", "hello", "text/plain;charset=utf-8", "att")
	runPico(t, "-e", loc, "del", pfx+"/put")
	p := startPicoReady(t, "-e", loc, "pub", pfx+"/pub", "2", fmt.Sprint(picoBig))
	p.until("done")
	got := sink.wait(t, 4)
	if g := got[0]; g.KeyExpr != pfx+"/put" || string(g.Payload) != "hello" ||
		g.Encoding.String() != "text/plain;charset=utf-8" || string(g.Attachment) != "att" {
		t.Errorf("put = %+v (enc %s)", g, g.Encoding)
	}
	if g := got[1]; g.KeyExpr != pfx+"/put" || g.Kind != SampleKindDelete {
		t.Errorf("del = %+v", g)
	}
	for i, g := range got[2:] {
		if g.KeyExpr != pfx+"/pub" || !bytes.Equal(g.Payload, picoPattern(picoBig, i)) {
			t.Errorf("pub %d = %s, %d bytes", i, g.KeyExpr, len(g.Payload))
		}
	}
}

func TestGoPubPicoSub(t *testing.T) {
	loc := routerLocator(t)
	pfx := uniq(t)
	p := startPicoReady(t, "-e", loc, "sub", pfx+"/**")
	s := openSession(t)
	settle()

	if err := s.Put(pfx+"/a", []byte("hi"), WithEncoding(EncodingApplicationJSON), WithAttachment([]byte("att"))); err != nil {
		t.Fatal(err)
	}
	pub, err := s.DeclarePublisher(pfx + "/b")
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Undeclare()
	big := picoPattern(picoBig, 1)
	_ = pub.Put(big)
	_ = pub.Delete()

	want := []struct{ key, kind, enc, payload, att string }{
		{pfx + "/a", "PUT", "application/json", "hi", "att"},
		{pfx + "/b", "PUT", "zenoh/bytes", "", ""},
		{pfx + "/b", "DELETE", "zenoh/bytes", "", ""},
	}
	for i, w := range want {
		e := p.next("sample")
		if e.Key != w.key || e.Kind != w.kind || e.Encoding != w.enc || (i != 1 && e.Payload != w.payload) {
			t.Errorf("sample %d = %+v", i, e)
		}
		if w.att != "" && (e.Attachment == nil || *e.Attachment != w.att) {
			t.Errorf("sample %d attachment = %v", i, e.Attachment)
		}
		if i == 1 && (e.Size != picoBig || e.Sum != fnvSum(big)) {
			t.Errorf("big sample: size %d sum %s", e.Size, e.Sum)
		}
	}
}

func TestPicoGetGoQueryable(t *testing.T) {
	loc := routerLocator(t)
	pfx := uniq(t)
	s := openSession(t)
	q, err := s.DeclareQueryable(pfx+"/**", func(q *Query) {
		switch q.Parameters() {
		case "fail=1":
			_ = q.ReplyErr([]byte("nope"), EncodingZenohString)
		case "big=1":
			_ = q.Reply(pfx+"/big", picoPattern(picoBig, 2))
		default:
			_ = q.Reply(pfx+"/one", []byte("go:"+q.Parameters()+":"+string(q.Payload())), WithEncoding(EncodingTextPlain))
		}
	}, WithComplete(true))
	if err != nil {
		t.Fatal(err)
	}
	defer q.Undeclare()
	settle()

	got := runPico(t, "-e", loc, "get", pfx+"/**?x=1", "body")
	if len(got) != 1 || got[0].Ev != "reply" || got[0].Key != pfx+"/one" || got[0].Payload != "go:x=1:body" ||
		got[0].Encoding != "text/plain" {
		t.Errorf("get = %+v", got)
	}
	got = runPico(t, "-e", loc, "get", pfx+"/**?fail=1")
	if len(got) != 1 || got[0].Ev != "err" || got[0].Payload != "nope" {
		t.Errorf("get err = %+v", got)
	}
	got = runPico(t, "-e", loc, "get", pfx+"/**?big=1")
	if len(got) != 1 || got[0].Size != picoBig || got[0].Sum != fnvSum(picoPattern(picoBig, 2)) {
		t.Errorf("get big = %+v", got)
	}
	got = runPico(t, "-e", loc, "querier", pfx+"/one", "3")
	if len(got) != 3 || got[2].Payload != "go:i=2:" {
		t.Errorf("querier = %+v", got)
	}
}

func TestGoGetPicoQueryable(t *testing.T) {
	loc := routerLocator(t)
	pfx := uniq(t)
	p := startPicoReady(t, "-e", loc, "qbl", pfx+"/**", pfx+"/pico")
	s := openSession(t)
	settle()

	get := func(sel string, opts ...GetOption) []Reply {
		t.Helper()
		ch, err := s.Get(sel, append(opts, WithTimeout(3*time.Second))...)
		if err != nil {
			t.Fatal(err)
		}
		var rs []Reply
		for r := range ch {
			rs = append(rs, r)
		}
		if e := p.next("query"); e.Key != pfx+"/**" && e.Key != pfx+"/pico" {
			t.Errorf("pico saw query on %q", e.Key)
		}
		return rs
	}
	rs := get(pfx+"/pico?a=1", WithPayload([]byte("body"), EncodingTextPlain), WithQueryAttachment([]byte("qa")))
	if len(rs) != 1 || rs[0].Sample == nil || rs[0].Sample.KeyExpr != pfx+"/pico" ||
		string(rs[0].Sample.Payload) != "pico:a=1:body" || string(rs[0].Sample.Attachment) != "qa" ||
		rs[0].Sample.Encoding != EncodingTextPlain {
		t.Errorf("get = %+v", rs)
	}
	if len(rs) == 1 && (rs[0].ReplierID == nil || rs[0].ReplierID.ZID.String() != p.zid) {
		t.Errorf("replier = %+v, want zid %s", rs[0].ReplierID, p.zid)
	}
	rs = get(pfx + "/**?fail=1")
	if len(rs) != 1 || rs[0].Err == nil || string(rs[0].Err.Payload) != "nope" {
		t.Errorf("get err = %+v", rs)
	}
	rs = get(pfx + "/**?big=3000")
	if len(rs) != 1 || rs[0].Sample == nil || !bytes.Equal(rs[0].Sample.Payload, picoPattern(picoBig, 0)) {
		t.Errorf("get big: %+v", rs)
	}
	qr, err := s.DeclareQuerier(pfx + "/**")
	if err != nil {
		t.Fatal(err)
	}
	defer qr.Undeclare()
	settle()
	ch, _ := qr.Get("n=7", WithTimeout(3*time.Second))
	var n int
	for r := range ch {
		n++
		if r.Sample == nil || string(r.Sample.Payload) != "pico:n=7:" {
			t.Errorf("querier reply %+v", r)
		}
	}
	if n != 1 {
		t.Errorf("querier: %d replies", n)
	}
}

func TestPicoLiveliness(t *testing.T) {
	loc := routerLocator(t)
	pfx := uniq(t)
	s := openSession(t)

	// pico token -> Go liveliness subscriber and get.
	sink := newSink()
	ls, err := s.Liveliness().DeclareSubscriber(pfx+"/**", sink.handler, true)
	if err != nil {
		t.Fatal(err)
	}
	defer ls.Undeclare()
	settle()
	tok := startPicoReady(t, "-e", loc, "token", pfx+"/pico")
	if got := sink.wait(t, 1); got[0].KeyExpr != pfx+"/pico" || got[0].Kind != SampleKindPut {
		t.Errorf("token put = %+v", got[0])
	}
	ch, _ := s.Liveliness().Get(pfx+"/**", WithTimeout(3*time.Second))
	var alive []string
	for r := range ch {
		if r.Sample != nil {
			alive = append(alive, r.Sample.KeyExpr)
		}
	}
	if len(alive) != 1 || alive[0] != pfx+"/pico" {
		t.Errorf("liveliness get = %v", alive)
	}

	// Go token -> pico liveliness subscriber (history) and get.
	gt, err := s.Liveliness().DeclareToken(pfx + "/go")
	if err != nil {
		t.Fatal(err)
	}
	settle()
	sink.wait(t, 0)
	lsub := startPicoReady(t, "-e", loc, "lsub", pfx+"/**")
	hist := map[string]bool{}
	for range 2 {
		e := lsub.next("token")
		hist[e.Key] = e.Kind == "PUT"
	}
	if !hist[pfx+"/go"] || !hist[pfx+"/pico"] {
		t.Errorf("pico liveliness history = %v", hist)
	}
	// zenohd 1.10.2 keeps re-sending the replies of a zenoh-pico liveliness
	// get while two or more tokens match, until the get times out (with or
	// without Go nodes involved), so only the set of keys is checked.
	got := runPico(t, "-e", loc, "lget", pfx+"/**")
	keys := map[string]bool{}
	for _, e := range got {
		keys[e.Key] = true
	}
	if len(keys) != 2 || !keys[pfx+"/go"] || !keys[pfx+"/pico"] {
		t.Errorf("pico liveliness get = %v", keys)
	}
	if len(got) != 2 {
		t.Logf("pico liveliness get: %d replies for 2 tokens", len(got))
	}

	_ = gt.Undeclare()
	if e := lsub.next("token"); e.Key != pfx+"/go" || e.Kind != "DELETE" {
		t.Errorf("pico saw %+v, want DELETE of go token", e)
	}
	tok.stop() // the pico session closes: its token goes
	if e := lsub.next("token"); e.Key != pfx+"/pico" || e.Kind != "DELETE" {
		t.Errorf("pico saw %+v, want DELETE of pico token", e)
	}
	if got := sink.wait(t, 1); got[len(got)-1].KeyExpr != pfx+"/pico" || got[len(got)-1].Kind != SampleKindDelete {
		t.Errorf("go saw %+v", got[len(got)-1])
	}
}

// TestPicoPeerUnicast: zenoh-pico in peer mode, connected to and from Go
// peers directly over TCP, without a router.
func TestPicoPeerUnicast(t *testing.T) {
	picoHarness(t)
	pfx := "tiny-zenoh-go/test/" + t.Name()

	// pico listens, Go connects.
	loc := freeLocator(t)
	psub := startPicoReady(t, "-m", "peer", "-l", loc, "sub", pfx+"/**")
	g := openPeer(t, loc)
	waitFor(t, "go peer connected to pico", func() bool { return len(g.PeersZID()) == 1 })
	settle()
	_ = g.Put(pfx+"/1", []byte("to-pico"))
	if e := psub.next("sample"); e.Key != pfx+"/1" || e.Payload != "to-pico" {
		t.Errorf("pico got %+v", e)
	}
	sink := newSink()
	sub, _ := g.DeclareSubscriber(pfx+"/**", sink.handler)
	defer sub.Undeclare()
	q, _ := g.DeclareQueryable(pfx+"/q", func(q *Query) { _ = q.Reply(pfx+"/q", []byte("go-peer")) })
	defer q.Undeclare()
	settle()

	// Go listens, a pico peer connects.
	gl := g.Locators()[0]
	runPico(t, "-m", "peer", "-e", gl, "put", pfx+"/2", "from-pico")
	if got := sink.wait(t, 1); got[0].KeyExpr != pfx+"/2" || string(got[0].Payload) != "from-pico" {
		t.Errorf("go got %+v", got[0])
	}
	got := runPico(t, "-m", "peer", "-e", gl, "get", pfx+"/q")
	if len(got) != 1 || got[0].Payload != "go-peer" {
		t.Errorf("pico peer get = %+v", got)
	}
}

// TestPicoMulticastPeer: zenoh-pico and Go peers on one UDP multicast group
// (multicast transport, JOIN), without any unicast link.
func TestPicoMulticastPeer(t *testing.T) {
	group := mcastLocator(t)
	picoHarness(t)
	pfx := uniq(t)
	// zenoh-pico's multicast batches are 2048 bytes (Z_BATCH_MULTICAST_SIZE);
	// group members must agree on the size.
	g, err := Open(Config{Mode: ModePeer, Listen: []string{group}, BatchSize: 2048,
		Scouting: ScoutingConfig{Disable: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	sink := newSink()
	sub, _ := g.DeclareSubscriber(pfx+"/p/**", sink.handler)
	defer sub.Undeclare()

	psub := startPicoReady(t, "-m", "peer", "-l", group, "sub", pfx+"/**")
	waitFor(t, "pico in the group", func() bool {
		for _, z := range g.MulticastPeers() {
			if z.String() == psub.zid {
				return true
			}
		}
		return false
	})
	big := picoPattern(picoBig, 3)
	_ = g.Put(pfx+"/go", []byte("to-pico"))
	_ = g.Put(pfx+"/gobig", big)
	if e := psub.next("sample"); e.Key != pfx+"/go" || e.Payload != "to-pico" {
		t.Errorf("pico got %+v", e)
	}
	if e := psub.next("sample"); e.Key != pfx+"/gobig" || e.Size != picoBig || e.Sum != fnvSum(big) {
		t.Errorf("pico got %s: %d bytes", e.Key, e.Size)
	}

	runPico(t, "-m", "peer", "-l", group, "put", pfx+"/p/pico", "from-pico")
	p := startPicoReady(t, "-m", "peer", "-l", group, "pub", pfx+"/p/big", "1", fmt.Sprint(picoBig))
	p.until("done")
	got := sink.wait(t, 2)
	if got[0].KeyExpr != pfx+"/p/pico" || string(got[0].Payload) != "from-pico" {
		t.Errorf("go got %+v", got[0])
	}
	if got[1].KeyExpr != pfx+"/p/big" || !bytes.Equal(got[1].Payload, picoPattern(picoBig, 0)) {
		t.Errorf("go got %s: %d bytes", got[1].KeyExpr, len(got[1].Payload))
	}
}

// TestPicoScoutsGoRouter: a zenoh-pico client without a connect locator
// finds a Go router by multicast scouting.
func TestPicoScoutsGoRouter(t *testing.T) {
	group := mcastLocator(t) // udp/<addr>#iface=<if>
	picoHarness(t)
	addr, iface, _ := strings.Cut(strings.TrimPrefix(group, "udp/"), "#iface=")
	r, err := Open(Config{Mode: ModeRouter, Listen: []string{"tcp/0.0.0.0:0"},
		Scouting: ScoutingConfig{Address: addr, Interface: iface}})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	pfx := uniq(t)
	s, err := Open(Config{Connect: []string{r.Locators()[0]}, Scouting: ScoutingConfig{Disable: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	sink := newSink()
	sub, _ := s.DeclareSubscriber(pfx+"/**", sink.handler)
	defer sub.Undeclare()
	settle()

	runPico(t, "-s", group, "put", pfx+"/scouted", "hello")
	if got := sink.wait(t, 1); got[0].KeyExpr != pfx+"/scouted" {
		t.Errorf("got %+v", got[0])
	}
}
