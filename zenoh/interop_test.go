package zenoh

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// These tests need a running router:
//
//	docker run -d --name zenohd -p 7447:7447 eclipse/zenoh
//	ZENOH_ROUTER=tcp/127.0.0.1:7447 go test ./zenoh/
//
// ZENOH_ROUTER=go runs them against an in-process tiny-zenoh-go router instead.
func routerLocator(t *testing.T) string {
	t.Helper()
	loc := os.Getenv("ZENOH_ROUTER")
	if loc == "" {
		t.Skip("set ZENOH_ROUTER=tcp/host:port (or go) to run against a router")
	}
	if loc == "go" {
		return goRouterLocator(t)
	}
	if !strings.Contains(loc, "/") {
		loc = "tcp/" + loc
	}
	return loc
}

var goRouter struct {
	once sync.Once
	s    *Session
	err  error
}

// goRouterLocator starts (once per test binary) a tiny-zenoh-go router on loopback.
func goRouterLocator(t *testing.T) string {
	goRouter.once.Do(func() {
		goRouter.s, goRouter.err = Open(Config{Mode: ModeRouter,
			Listen: []string{"tcp/127.0.0.1:0"}, Scouting: ScoutingConfig{Disable: true}})
	})
	if goRouter.err != nil {
		t.Fatalf("go router: %v", goRouter.err)
	}
	return goRouter.s.Locators()[0]
}

func openSession(t *testing.T) *Session {
	t.Helper()
	s, err := Open(Config{Connect: []string{routerLocator(t)}})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// uniq returns a key prefix unique to this test run.
func uniq(t *testing.T) string {
	return fmt.Sprintf("tiny-zenoh-go/test/%s/%d", t.Name(), time.Now().UnixNano())
}

type sampleSink struct {
	mu sync.Mutex
	ss []Sample
	ch chan struct{}
}

func newSink() *sampleSink { return &sampleSink{ch: make(chan struct{}, 64)} }

func (k *sampleSink) handler(s Sample) {
	k.mu.Lock()
	k.ss = append(k.ss, s)
	k.mu.Unlock()
	k.ch <- struct{}{}
}

func (k *sampleSink) wait(t *testing.T, n int) []Sample {
	t.Helper()
	for i := range n {
		select {
		case <-k.ch:
		case <-time.After(3 * time.Second):
			t.Fatalf("timeout: got %d of %d samples", i, n)
		}
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	return append([]Sample(nil), k.ss...)
}

// settle gives the router a moment to propagate declarations.
func settle() { time.Sleep(200 * time.Millisecond) }

func TestPubSubThroughRouter(t *testing.T) {
	pfx := uniq(t)
	a, b := openSession(t), openSession(t)
	sink := newSink()
	sub, err := b.DeclareSubscriber(pfx+"/**", sink.handler)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Undeclare()
	settle()

	if err := a.Put(pfx+"/x", []byte("hello"), WithEncoding(EncodingTextPlain.WithSchema("charset=utf-8")),
		WithAttachment([]byte("meta"))); err != nil {
		t.Fatal(err)
	}
	// Publisher path: declares a numeric key-expr id (D_KEYEXPR) and sends PUSHes
	// with a scope instead of the full key.
	pub, err := a.DeclarePublisher(pfx+"/y", WithEncoding(EncodingApplicationJSON))
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Undeclare()
	if err := pub.Put([]byte(`{"n":1}`)); err != nil {
		t.Fatal(err)
	}
	if err := pub.Delete(); err != nil {
		t.Fatal(err)
	}

	got := sink.wait(t, 3)
	if got[0].KeyExpr != pfx+"/x" || string(got[0].Payload) != "hello" ||
		got[0].Encoding.String() != "text/plain;charset=utf-8" || string(got[0].Attachment) != "meta" {
		t.Errorf("sample 0 = %+v (enc %s)", got[0], got[0].Encoding)
	}
	if got[1].KeyExpr != pfx+"/y" || string(got[1].Payload) != `{"n":1}` || got[1].Encoding != EncodingApplicationJSON {
		t.Errorf("sample 1 = %+v", got[1])
	}
	if got[2].KeyExpr != pfx+"/y" || got[2].Kind != SampleKindDelete {
		t.Errorf("sample 2 = %+v", got[2])
	}
	t.Logf("router timestamped samples: %v", got[0].Timestamp != nil)
}

func TestSubscriberChanAndUndeclare(t *testing.T) {
	pfx := uniq(t)
	a, b := openSession(t), openSession(t)
	sub, ch, err := b.DeclareSubscriberChan(pfx + "/*")
	if err != nil {
		t.Fatal(err)
	}
	settle()
	for i := range 50 {
		_ = a.Put(fmt.Sprintf("%s/%d", pfx, i), []byte{byte(i)})
	}
	for i := range 50 {
		select {
		case s := <-ch:
			if s.Payload[0] != byte(i) {
				t.Fatalf("out of order: got %d want %d", s.Payload[0], i)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("timeout at %d", i)
		}
	}
	_ = sub.Undeclare()
	if _, ok := <-ch; ok {
		t.Error("channel should close after Undeclare")
	}
}

func TestLargePayloadFragmentation(t *testing.T) {
	pfx := uniq(t)
	a, b := openSession(t), openSession(t)
	sink := newSink()
	sub, _ := b.DeclareSubscriber(pfx, sink.handler)
	defer sub.Undeclare()
	settle()
	big := make([]byte, 300_000) // several batches => FRAGMENTs both ways
	for i := range big {
		big[i] = byte(i * 7)
	}
	if err := a.Put(pfx, big); err != nil {
		t.Fatal(err)
	}
	got := sink.wait(t, 1)
	if string(got[0].Payload) != string(big) {
		t.Errorf("payload mismatch: got %d bytes", len(got[0].Payload))
	}
}

func TestQueryableThroughRouter(t *testing.T) {
	pfx := uniq(t)
	a, b := openSession(t), openSession(t)
	q, err := b.DeclareQueryable(pfx+"/**", func(q *Query) {
		if q.Parameters() == "fail=1" {
			_ = q.ReplyErr([]byte("nope"), EncodingZenohString)
			return
		}
		_ = q.Reply(pfx+"/one", []byte("1:"+q.Parameters()+":"+string(q.Payload())))
		_ = q.Reply(pfx+"/two", []byte("2"))
	}, WithComplete(true))
	if err != nil {
		t.Fatal(err)
	}
	defer q.Undeclare()
	settle()

	replies, err := a.Get(pfx+"/**?x=1", WithPayload([]byte("body"), EncodingTextPlain), WithTimeout(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for r := range replies {
		s, err := r.Result()
		if err != nil {
			t.Fatalf("reply error: %v", err)
		}
		got[s.KeyExpr] = string(s.Payload)
		if r.ReplierID == nil || !r.ReplierID.ZID.Equal(b.ZID()) {
			t.Errorf("replier id = %+v, want zid %s", r.ReplierID, b.ZID())
		}
	}
	if got[pfx+"/one"] != "1:x=1:body" || got[pfx+"/two"] != "2" {
		t.Errorf("replies = %v", got)
	}

	errs, _ := a.Get(pfx+"/**?fail=1", WithTimeout(3*time.Second))
	var n int
	for r := range errs {
		n++
		if r.Err == nil || string(r.Err.Payload) != "nope" {
			t.Errorf("want error reply 'nope', got %+v", r)
		}
	}
	if n != 1 {
		t.Errorf("got %d error replies, want 1", n)
	}
}

func TestGetRouterAdminSpace(t *testing.T) {
	s := openSession(t)
	// zenohd's admin space is served by the router itself (a Rust queryable).
	replies, err := s.Get("@/"+s.RouterZID().String()+"/router", WithTimeout(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	var n int
	for r := range replies {
		smp, err := r.Result()
		if err != nil {
			t.Fatalf("admin reply error: %v", err)
		}
		n++
		t.Logf("admin %s (%s): %.120s...", smp.KeyExpr, smp.Encoding, smp.Payload)
		if !strings.Contains(string(smp.Payload), s.ZID().String()) {
			t.Errorf("router admin info does not list our session %s", s.ZID())
		}
	}
	if n == 0 {
		t.Error("no reply from router admin space")
	}
}

func TestGetTimeoutAndLocalQueryable(t *testing.T) {
	pfx := uniq(t)
	s := openSession(t)
	// A queryable in the same session answers the session's own Get.
	q, _ := s.DeclareQueryable(pfx, func(q *Query) { _ = q.Reply(pfx, []byte("local")) })
	defer q.Undeclare()
	replies, _ := s.Get(pfx, WithTimeout(3*time.Second))
	var got []string
	for r := range replies {
		smp, err := r.Result()
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, string(smp.Payload))
	}
	if len(got) != 1 || got[0] != "local" {
		t.Errorf("local get = %v", got)
	}

	// A detached query that never finishes must end with a Timeout error.
	q2, _ := s.DeclareQueryable(pfx+"/slow", func(q *Query) { q.Detach() })
	defer q2.Undeclare()
	start := time.Now()
	replies, _ = s.Get(pfx+"/slow", WithTimeout(300*time.Millisecond))
	var last Reply
	for r := range replies {
		last = r
	}
	if last.Err == nil || string(last.Err.Payload) != "Timeout" {
		t.Errorf("want Timeout error, got %+v", last)
	}
	if time.Since(start) > 2*time.Second {
		t.Errorf("timeout took %v", time.Since(start))
	}
}

func TestLiveliness(t *testing.T) {
	pfx := uniq(t)
	a, b := openSession(t), openSession(t)
	tok1, err := a.Liveliness().DeclareToken(pfx + "/one")
	if err != nil {
		t.Fatal(err)
	}
	settle()

	// History: the already-alive token is reported first.
	sink := newSink()
	ls, err := b.Liveliness().DeclareSubscriber(pfx+"/**", sink.handler, true)
	if err != nil {
		t.Fatal(err)
	}
	defer ls.Undeclare()
	tok2, _ := a.Liveliness().DeclareToken(pfx + "/two")
	got := sink.wait(t, 2)
	if got[0].KeyExpr != pfx+"/one" || got[1].KeyExpr != pfx+"/two" || got[1].Kind != SampleKindPut {
		t.Errorf("liveliness puts = %+v", got)
	}

	replies, _ := b.Liveliness().Get(pfx+"/**", WithTimeout(3*time.Second))
	alive := map[string]bool{}
	for r := range replies {
		alive[r.Sample.KeyExpr] = true
	}
	if !alive[pfx+"/one"] || !alive[pfx+"/two"] || len(alive) != 2 {
		t.Errorf("liveliness get = %v", alive)
	}

	_ = tok1.Undeclare()
	got = sink.wait(t, 1)
	if got[2].KeyExpr != pfx+"/one" || got[2].Kind != SampleKindDelete {
		t.Errorf("undeclare event = %+v", got[2])
	}
	// Closing the session drops its tokens: router reports DELETE for /two.
	_ = a.Close()
	got = sink.wait(t, 1)
	if got[3].KeyExpr != pfx+"/two" || got[3].Kind != SampleKindDelete {
		t.Errorf("session-close event = %+v", got[3])
	}
	_ = tok2
}

// TestPeerThroughRouter: a peer-mode session attached to zenohd exchanges
// data, queries and liveliness with a client of the same router.
func TestPeerThroughRouter(t *testing.T) {
	pfx := uniq(t)
	p := openPeer(t, routerLocator(t))
	if !p.RouterZID().Equal(openSession(t).RouterZID()) || len(p.RoutersZID()) != 1 {
		t.Fatalf("peer routers = %v", p.RoutersZID())
	}
	c := openSession(t)

	sinkP, sinkC := newSink(), newSink()
	subP, _ := p.DeclareSubscriber(pfx+"/**", sinkP.handler)
	defer subP.Undeclare()
	subC, _ := c.DeclareSubscriber(pfx+"/**", sinkC.handler)
	defer subC.Undeclare()
	q, _ := p.DeclareQueryable(pfx+"/q", func(q *Query) { _ = q.Reply(pfx+"/q", []byte("from-peer")) })
	defer q.Undeclare()
	tok, _ := p.Liveliness().DeclareToken(pfx + "/alive")
	defer tok.Undeclare()
	settle()

	_ = p.Put(pfx+"/p", []byte("p"))
	_ = c.Put(pfx+"/c", []byte("c"))
	both := func(got []Sample) bool { // local delivery may come first: order-free
		a, b := string(got[0].Payload), string(got[1].Payload)
		return a+b == "pc" || a+b == "cp"
	}
	if got := sinkC.wait(t, 2); !both(got) {
		t.Errorf("client got %+v", got)
	}
	if got := sinkP.wait(t, 2); !both(got) {
		t.Errorf("peer got %+v", got)
	}
	replies, _ := c.Get(pfx+"/q", WithTimeout(3*time.Second))
	var n int
	for r := range replies {
		if smp, err := r.Result(); err != nil || string(smp.Payload) != "from-peer" {
			t.Errorf("reply %+v %v", smp, err)
		}
		n++
	}
	if n != 1 {
		t.Errorf("%d replies", n)
	}
	lv, _ := c.Liveliness().Get(pfx+"/**", WithTimeout(3*time.Second))
	n = 0
	for range lv {
		n++
	}
	if n != 1 {
		t.Errorf("client sees %d peer tokens", n)
	}
}
