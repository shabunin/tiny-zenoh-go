package zenoh

import (
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

// MaxSessions: one node more than allowed is refused (CLOSE, max
// sessions), and admitted again once another left.
func TestMaxSessions(t *testing.T) {
	r, err := Open(Config{Mode: ModeRouter, Listen: []string{"tcp/127.0.0.1:0"},
		Scouting: ScoutingConfig{Disable: true}, MaxSessions: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	c1, c2 := clientOf(t, r), clientOf(t, r)
	if _, err := Open(Config{Connect: r.Locators()}); err == nil || !strings.Contains(err.Error(), "CLOSE (reason 0x3)") {
		t.Fatalf("third session: %v, want a max sessions refusal", err)
	}
	// The two admitted ones still work.
	got := make(chan string, 1)
	sub, err := c2.DeclareSubscriber("lim/**", func(s Sample) {
		select {
		case got <- string(s.Payload):
		default:
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Undeclare()
	waitFor(t, "data between the admitted sessions", func() bool { _ = c1.Put("lim/a", []byte("x")); return len(got) > 0 })

	_ = c1.Close()
	var c3 *Session
	waitFor(t, "a session after one left", func() bool {
		c3, err = Open(Config{Connect: r.Locators()})
		return err == nil
	})
	defer c3.Close()

	// Negative: no limit.
	full, err := Open(Config{Mode: ModeRouter, Listen: []string{"tcp/127.0.0.1:0"},
		Scouting: ScoutingConfig{Disable: true}, MaxSessions: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer full.Close()
	for range 5 {
		clientOf(t, full)
	}
}

// AcceptPending: links beyond the handshakes allowed at once are closed
// right away, and silent ones give their place back at AcceptTimeout.
func TestAcceptPending(t *testing.T) {
	r, err := Open(Config{Mode: ModeRouter, Listen: []string{"tcp/127.0.0.1:0"},
		Scouting: ScoutingConfig{Disable: true}, AcceptPending: 2, AcceptTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	addr := strings.TrimPrefix(r.Locators()[0], "tcp/")
	dial := func() net.Conn {
		t.Helper()
		c, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = c.Close() })
		return c
	}
	silent := []net.Conn{dial(), dial()}
	waitFor(t, "two pending handshakes", func() bool { return r.accepting.Load() == 2 })
	start := time.Now()
	extra := dial()
	_ = extra.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := extra.Read(make([]byte, 1)); err != io.EOF && !strings.Contains(err.Error(), "reset") {
		t.Fatalf("link beyond accept_pending: %v, want it closed", err)
	}
	if d := time.Since(start); d > 500*time.Millisecond {
		t.Fatalf("closed after %v, want at once", d)
	}
	if _, err := Open(Config{Connect: r.Locators(), ConnectTimeout: time.Second, AcceptTimeout: time.Second}); err == nil {
		t.Fatal("a session opened while the handshakes allowed were all taken")
	}
	// The silent links time out: there is room again.
	for _, c := range silent {
		_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
		if _, err := io.ReadAll(c); err != nil {
			t.Fatalf("silent link: %v, want it closed at the accept timeout", err)
		}
	}
	waitFor(t, "no pending handshake", func() bool { return r.accepting.Load() == 0 })
	clientOf(t, r)
}

// MaxDeclarations: a node holding as many declarations as allowed keeps
// working (redeclaring and undeclaring included); one more disconnects it.
func TestMaxDeclarations(t *testing.T) {
	r, err := Open(Config{Mode: ModeRouter, Listen: []string{"tcp/127.0.0.1:0"},
		Scouting: ScoutingConfig{Disable: true}, MaxDeclarations: 6})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	held := func() int {
		r.mu.Lock()
		defer r.mu.Unlock()
		n := 0
		for _, f := range r.faces {
			n += len(f.subs) + len(f.qbls) + len(f.tokens) + len(f.remoteExprs)
			if f.router != nil {
				n += len(f.router.interests)
			}
		}
		return n
	}
	c, err := Open(Config{Connect: r.Locators()})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	base := held() // what a client declares by itself (its interest)
	if base >= 6 {
		t.Fatalf("a bare client holds %d declarations", base)
	}
	nop := func(Sample) {}
	var subs []*Subscriber
	for i := base; i < 6; i++ {
		sub, err := c.DeclareSubscriber("lim/s"+string(rune('a'+i)), nop)
		if err != nil {
			t.Fatal(err)
		}
		subs = append(subs, sub)
	}
	waitFor(t, "the allowed declarations", func() bool { return held() == 6 })
	// At the limit: replacing one is fine.
	_ = subs[0].Undeclare()
	waitFor(t, "the undeclaration", func() bool { return held() == 5 })
	if _, err := c.Liveliness().DeclareToken("lim/token"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the token", func() bool { return held() == 6 })
	if len(r.Transports()) != 1 {
		t.Fatal("the client was disconnected within its limit")
	}
	// One more: disconnected.
	if _, err := c.DeclareQueryable("lim/q", func(*Query) {}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the disconnection", func() bool { return len(r.Transports()) == 0 })
}

// Routed queries: the timeout a node asks for is capped, and a node with
// MaxPendingQueries queries running gets no replies to further ones.
func TestRoutedQueryLimits(t *testing.T) {
	r, err := Open(Config{Mode: ModeRouter, Listen: []string{"tcp/127.0.0.1:0"},
		Scouting: ScoutingConfig{Disable: true}, MaxPendingQueries: 2, MaxQueryTimeout: 500 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	a, b := clientOf(t, r), clientOf(t, r)
	release := make(chan struct{})
	q, err := a.DeclareQueryable("lim/q", func(q *Query) {
		<-release
		_ = q.Reply("lim/q", []byte("late"))
	})
	if err != nil {
		t.Fatal(err)
	}
	defer q.Undeclare()
	defer close(release)
	pub, err := b.DeclareQuerier("lim/q", WithTimeout(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the queryable", func() bool { st, _ := pub.MatchingStatus(); return st.Matching })

	type result struct {
		replies int
		took    time.Duration
	}
	run := func() chan result {
		ch := make(chan result, 1)
		start := time.Now()
		replies, err := b.Get("lim/q", WithTimeout(time.Minute))
		if err != nil {
			t.Fatal(err)
		}
		go func() {
			n := 0
			for range replies {
				n++
			}
			ch <- result{n, time.Since(start)}
		}()
		return ch
	}
	routing := func() int {
		r.mu.Lock()
		defer r.mu.Unlock()
		return len(r.routes)
	}
	q1, q2 := run(), run()
	waitFor(t, "two routed queries", func() bool { return routing() == 2 })
	// The third is over the limit: ended at once.
	if res := <-run(); res.replies != 0 || res.took > 300*time.Millisecond {
		t.Fatalf("query over the limit: %+v, want no reply at once", res)
	}
	// The first two end at the router's cap, not after their minute.
	for _, ch := range []chan result{q1, q2} {
		select {
		case res := <-ch:
			if res.replies != 0 || res.took < 400*time.Millisecond {
				t.Fatalf("capped query: %+v", res)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("a query outlived the router's cap")
		}
	}
	waitFor(t, "the routes to be gone", func() bool { return routing() == 0 })
	// There is room again.
	run()
	waitFor(t, "a query after the others ended", func() bool { return routing() == 1 })
}
