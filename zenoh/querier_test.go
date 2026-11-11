package zenoh

import (
	"strings"
	"testing"
	"time"
)

// Queriers and matching status, locally and against zenoh-rust.

type matchWatch chan bool

func (w matchWatch) cb(st MatchingStatus) { w <- st.Matching }

func (w matchWatch) expect(t *testing.T, want bool) {
	t.Helper()
	select {
	case got := <-w:
		if got != want {
			t.Fatalf("matching = %v, want %v", got, want)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("no matching status change (waiting for %v)", want)
	}
}

func (w matchWatch) quiet(t *testing.T) {
	t.Helper()
	select {
	case got := <-w:
		t.Fatalf("unexpected matching status %v", got)
	case <-time.After(300 * time.Millisecond):
	}
}

func lonePeer(t *testing.T) *Session {
	t.Helper()
	s, err := Open(Config{Mode: ModePeer, Scouting: ScoutingConfig{Disable: true}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestMatchingLocal(t *testing.T) {
	s := lonePeer(t)
	pub, err := s.DeclarePublisher("m/a/b")
	if err != nil {
		t.Fatal(err)
	}
	if st, _ := pub.MatchingStatus(); st.Matching {
		t.Fatal("publisher matching with no subscriber")
	}
	w := make(matchWatch, 8)
	if _, err := pub.DeclareMatchingListener(w.cb); err != nil {
		t.Fatal(err)
	}
	w.quiet(t)
	other, _ := s.DeclareSubscriber("m/x", func(Sample) {})
	w.quiet(t)
	sub, _ := s.DeclareSubscriber("m/*/b", func(Sample) {})
	w.expect(t, true)
	sub2, _ := s.DeclareSubscriber("m/**", func(Sample) {})
	w.quiet(t) // still matching: no news
	_ = sub.Undeclare()
	w.quiet(t)
	_ = sub2.Undeclare()
	w.expect(t, false)
	_ = other.Undeclare()

	// A listener declared while matching hears it at once.
	sub, _ = s.DeclareSubscriber("m/a/b", func(Sample) {})
	w.expect(t, true)
	w2 := make(matchWatch, 8)
	l2, _ := pub.DeclareMatchingListener(w2.cb)
	w2.expect(t, true)
	_ = l2.Undeclare()
	_ = sub.Undeclare()
	w.expect(t, false)
	w2.quiet(t)

	if err := pub.Undeclare(); err != nil {
		t.Fatal(err)
	}
	if _, err := pub.MatchingStatus(); err != ErrUndeclared {
		t.Fatalf("status after undeclare: %v", err)
	}
}

func TestQuerierLocal(t *testing.T) {
	s := lonePeer(t)
	qr, err := s.DeclareQuerier("q/a/b", WithTarget(TargetAllComplete), WithTimeout(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	w := make(matchWatch, 8)
	_, _ = qr.DeclareMatchingListener(w.cb)
	reply := func(q *Query) { _ = q.Reply(q.KeyExpr(), []byte(q.Parameters())) }
	partial, _ := s.DeclareQueryable("q/**", reply)
	w.quiet(t) // not complete: no match for AllComplete
	narrow, _ := s.DeclareQueryable("q/a/*/c", reply, WithComplete(true))
	w.quiet(t) // complete, but does not include q/a/b
	full, _ := s.DeclareQueryable("q/a/*", reply, WithComplete(true))
	w.expect(t, true)

	ch, err := qr.Get("x=1")
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for r := range ch {
		if r.Sample == nil {
			t.Fatalf("error reply %v", r.Err)
		}
		got = append(got, string(r.Sample.Payload))
	}
	// Latest consolidation: one reply per key.
	if len(got) != 1 || got[0] != "x=1" {
		t.Fatalf("replies %q", got)
	}
	_ = full.Undeclare()
	w.expect(t, false)
	_ = narrow.Undeclare()
	_ = partial.Undeclare()
	_ = qr.Undeclare()
	if _, err := qr.Get(""); err != ErrUndeclared {
		t.Fatalf("get after undeclare: %v", err)
	}
}

// Go entities through the router (zenohd or the Go router): matching status
// follows Rust subscribers and queryables appearing and going away.
func TestMatchingGoSeesRust(t *testing.T) {
	pfx := uniq(t)
	s := openSession(t)
	pub, _ := s.DeclarePublisher(pfx + "/p")
	pw := make(matchWatch, 8)
	_, _ = pub.DeclareMatchingListener(pw.cb)
	qr, _ := s.DeclareQuerier(pfx+"/q", WithTimeout(5*time.Second))
	qw := make(matchWatch, 8)
	_, _ = qr.DeclareMatchingListener(qw.cb)
	pw.quiet(t)
	qw.quiet(t)

	sub := startPeer(t, true, "sub", pfx+"/*", "1")
	pw.expect(t, true)
	qw.quiet(t)
	_ = pub.Put([]byte("hello"))
	if e := sub.next(); e.Payload != "hello" {
		t.Fatalf("rust got %+v", e)
	}
	pw.expect(t, false) // the subscriber exits after one sample

	startPeer(t, false, "queryable", pfx+"/**", "2")
	qw.expect(t, true)
	ch, err := qr.Get("k=v")
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for r := range ch {
		if r.Sample != nil {
			got = append(got, string(r.Sample.Payload))
		}
	}
	if len(got) != 1 || !strings.HasPrefix(got[0], "py:k=v") {
		t.Fatalf("querier replies %q", got)
	}
	qw.expect(t, false) // the queryable is undeclared after 2s
}

// Rust publishers and queriers through the router see Go subscribers and
// queryables.
func TestMatchingRustSeesGo(t *testing.T) {
	pfx := uniq(t)
	s := openSession(t)
	rp := startPeer(t, true, "matching", "pub", pfx+"/p", "30")
	rq := startPeer(t, true, "matching", "querier-complete", pfx+"/q/x", "30")
	expect := func(p *rustPeer, want bool) {
		t.Helper()
		if e := p.next(); e.Matching == nil || *e.Matching != want {
			t.Fatalf("rust matching event %+v, want %v", e, want)
		}
	}
	sub, _ := s.DeclareSubscriber(pfx+"/**", func(Sample) {})
	expect(rp, true)
	_ = sub.Undeclare()
	expect(rp, false)

	qbl, _ := s.DeclareQueryable(pfx+"/q/*", func(*Query) {}, WithComplete(true))
	expect(rq, true)
	_ = qbl.Undeclare()
	expect(rq, false)
}

// Peers: matching status over a direct link to a Rust peer, both ways.
func TestMatchingRustPeer(t *testing.T) {
	pfx := uniq(t)
	s, err := Open(Config{Mode: ModePeer, Listen: []string{"tcp/127.0.0.1:0"},
		Scouting: ScoutingConfig{Disable: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	qr, _ := s.DeclareQuerier(pfx + "/q")
	qw := make(matchWatch, 8)
	_, _ = qr.DeclareMatchingListener(qw.cb)
	conf := authRustConf(t, map[string]any{"mode": "peer", "connect/endpoints": s.Locators()})
	rp := startRust(t, conf, true, "matching", "querier", pfx+"/r", "30")
	startRust(t, conf, false, "queryable", pfx+"/*", "2")
	qw.expect(t, true)
	qw.expect(t, false)

	qbl, _ := s.DeclareQueryable(pfx+"/r", func(*Query) {})
	if e := rp.next(); e.Matching == nil || !*e.Matching {
		t.Fatalf("rust matching event %+v", e)
	}
	_ = qbl.Undeclare()
	if e := rp.next(); e.Matching == nil || *e.Matching {
		t.Fatalf("rust matching event %+v", e)
	}
}

// Undeclaring a querier drops its running queries.
func TestQuerierUndeclareDropsQueries(t *testing.T) {
	pfx := uniq(t)
	srv, cli := openSession(t), openSession(t)
	release := make(chan struct{})
	q, _ := srv.DeclareQueryable(pfx, func(q *Query) {
		<-release
		_ = q.Reply(q.KeyExpr(), []byte("late"))
	})
	defer q.Undeclare()
	defer close(release)
	qr, _ := cli.DeclareQuerier(pfx, WithTimeout(20*time.Second))
	settle()
	ch, err := qr.Get("")
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	_ = qr.Undeclare()
	select {
	case r, ok := <-ch:
		if ok {
			t.Fatalf("reply after undeclare: %+v", r)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("query still running after querier undeclare")
	}
}
