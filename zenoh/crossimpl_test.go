package zenoh

import (
	"bufio"
	"encoding/json"
	"os"
	"os/exec"
	"testing"
	"time"
)

// Cross-implementation tests: tiny-zenoh-go against the official Rust Zenoh stack,
// driven through its Python binding (testdata/zpeer.py).
//
//	ZENOH_ROUTER=tcp/127.0.0.1:7447 ZENOH_PY=/path/to/python-with-eclipse-zenoh \
//	  go test -run Rust ./zenoh/

type peerEvent struct {
	Ready      bool            `json:"ready"`
	Done       bool            `json:"done"`
	Key        string          `json:"key"`
	Kind       string          `json:"kind"`
	Payload    string          `json:"payload"`
	Encoding   string          `json:"encoding"`
	Attachment *string         `json:"attachment"`
	Ok         *peerEventInner `json:"ok"`
	Err        *string         `json:"err"`
	Size       int             `json:"size"` // big* modes
	Sum        string          `json:"sum"`
	Shm        bool            `json:"shm"`
	Priority   int             `json:"priority"`
	Matching   *bool           `json:"matching"`
	Miss       int             `json:"miss"`
}

type peerEventInner struct {
	Key     string `json:"key"`
	Payload string `json:"payload"`
}

type rustPeer struct {
	t      *testing.T
	cmd    *exec.Cmd
	events chan peerEvent
}

// startPeer runs zpeer.py and, if waitReady, blocks until it has declared
// its entities.
func startPeer(t *testing.T, waitReady bool, args ...string) *rustPeer {
	t.Helper()
	return startRust(t, routerLocator(t), waitReady, args...)
}

// startRust runs zpeer.py with target = router locator or JSON config.
func startRust(t *testing.T, target string, waitReady bool, args ...string) *rustPeer {
	t.Helper()
	py := os.Getenv("ZENOH_PY")
	if py == "" {
		t.Skip("set ZENOH_PY to a python with eclipse-zenoh installed")
	}
	cmd := exec.Command(py, append([]string{"-I", "testdata/zpeer.py", target}, args...)...)
	cmd.Stderr = os.Stderr
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	p := &rustPeer{t: t, cmd: cmd, events: make(chan peerEvent, 64)}
	go func() {
		sc := bufio.NewScanner(out)
		for sc.Scan() {
			var ev peerEvent
			if json.Unmarshal(sc.Bytes(), &ev) == nil {
				p.events <- ev
			}
		}
		close(p.events)
	}()
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	if waitReady {
		if ev := p.next(); !ev.Ready {
			t.Fatalf("peer not ready: %+v", ev)
		}
		settle()
	}
	return p
}

func (p *rustPeer) next() peerEvent {
	p.t.Helper()
	select {
	case ev, ok := <-p.events:
		if !ok {
			p.t.Fatal("rust peer exited early")
		}
		return ev
	case <-time.After(10 * time.Second):
		p.t.Fatal("timeout waiting for rust peer")
	}
	return peerEvent{}
}

func TestRustSubscriberReceivesGo(t *testing.T) {
	pfx := uniq(t)
	py := startPeer(t, true, "sub", pfx+"/**", "3")
	s := openSession(t)
	if err := s.Put(pfx+"/a", []byte("go-put"), WithEncoding(EncodingTextPlain.WithSchema("v=1")),
		WithAttachment([]byte("go-att"))); err != nil {
		t.Fatal(err)
	}
	pub, _ := s.DeclarePublisher(pfx + "/b") // uses a declared key-expr id
	_ = pub.Put([]byte("go-pub"))
	_ = pub.Delete()

	e := py.next()
	if e.Key != pfx+"/a" || e.Payload != "go-put" || e.Encoding != "text/plain;v=1" ||
		e.Attachment == nil || *e.Attachment != "go-att" {
		t.Errorf("rust got %+v", e)
	}
	if e = py.next(); e.Key != pfx+"/b" || e.Payload != "go-pub" || e.Kind != "PUT" {
		t.Errorf("rust got %+v", e)
	}
	if e = py.next(); e.Key != pfx+"/b" || e.Kind != "DELETE" {
		t.Errorf("rust got %+v", e)
	}
}

func TestGoSubscriberReceivesRust(t *testing.T) {
	pfx := uniq(t)
	s := openSession(t)
	sink := newSink()
	sub, _ := s.DeclareSubscriber(pfx, sink.handler)
	defer sub.Undeclare()
	settle()
	startPeer(t, false, "pub", pfx)
	got := sink.wait(t, 2)
	if string(got[0].Payload) != "from-rust" || got[0].Encoding.String() != "text/plain;x" ||
		string(got[0].Attachment) != "att" || got[0].Kind != SampleKindPut {
		t.Errorf("go got %+v (enc %s)", got[0], got[0].Encoding)
	}
	if got[1].Kind != SampleKindDelete || got[1].KeyExpr != pfx {
		t.Errorf("go got %+v", got[1])
	}
}

func TestGoGetRustQueryable(t *testing.T) {
	pfx := uniq(t)
	startPeer(t, true, "queryable", pfx+"/**", "5")
	s := openSession(t)
	replies, err := s.Get(pfx+"/q?a=1", WithPayload([]byte("hi"), EncodingTextPlain), WithTimeout(5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for r := range replies {
		smp, err := r.Result()
		if err != nil {
			t.Fatalf("reply error: %v", err)
		}
		got = append(got, smp.KeyExpr+"="+string(smp.Payload))
	}
	if len(got) != 1 || got[0] != pfx+"/q=py:a=1:hi" {
		t.Errorf("replies = %v", got)
	}
	errs, _ := s.Get(pfx+"/q?fail", WithTimeout(5*time.Second))
	var n int
	for r := range errs {
		n++
		if r.Err == nil || string(r.Err.Payload) != "rust-err" {
			t.Errorf("want rust-err, got %+v", r)
		}
	}
	if n != 1 {
		t.Errorf("%d error replies", n)
	}
}

func TestRustGetGoQueryable(t *testing.T) {
	pfx := uniq(t)
	s := openSession(t)
	q, _ := s.DeclareQueryable(pfx+"/**", func(q *Query) {
		_ = q.Reply(pfx+"/x", []byte("go:"+string(q.Payload())), WithEncoding(EncodingTextPlain))
		_ = q.Reply(pfx+"/y", []byte("go2"))
	})
	defer q.Undeclare()
	settle()
	py := startPeer(t, false, "get", pfx+"/**")
	got := map[string]string{}
	for {
		e := py.next()
		if e.Done {
			break
		}
		if e.Ok == nil {
			t.Fatalf("rust got error reply: %+v", e)
		}
		got[e.Ok.Key] = e.Ok.Payload
	}
	if got[pfx+"/x"] != "go:qbody" || got[pfx+"/y"] != "go2" || len(got) != 2 {
		t.Errorf("rust replies = %v", got)
	}
}

func TestRustLivelinessCross(t *testing.T) {
	pfx := uniq(t)
	// Rust token, seen by Go.
	startPeer(t, true, "token", pfx+"/rust", "4")
	s := openSession(t)
	replies, _ := s.Liveliness().Get(pfx+"/**", WithTimeout(3*time.Second))
	var keys []string
	for r := range replies {
		keys = append(keys, r.Sample.KeyExpr)
	}
	if len(keys) != 1 || keys[0] != pfx+"/rust" {
		t.Errorf("go liveliness get = %v", keys)
	}
	// Go token, seen by a Rust liveliness subscriber (history: rust + go tokens).
	tok, _ := s.Liveliness().DeclareToken(pfx + "/go")
	settle()
	py := startPeer(t, true, "livsub", pfx+"/**", "3")
	seen := map[string]bool{}
	for range 2 {
		seen[py.next().Key] = true
	}
	if !seen[pfx+"/rust"] || !seen[pfx+"/go"] {
		t.Errorf("rust liveliness history = %v", seen)
	}
	_ = tok.Undeclare()
	if e := py.next(); e.Key != pfx+"/go" || e.Kind != "DELETE" {
		t.Errorf("rust liveliness delete = %+v", e)
	}
}
