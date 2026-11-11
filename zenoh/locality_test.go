package zenoh

import (
	"testing"
	"time"
)

func TestLocalitySubscribers(t *testing.T) {
	r := openRouter(t)
	a, b := clientOf(t, r), clientOf(t, r)
	key := uniq(t)
	local, remote, any := newSink(), newSink(), newSink()
	for _, s := range []struct {
		k *sampleSink
		l Locality
	}{{local, LocalitySessionLocal}, {remote, LocalityRemote}, {any, LocalityAny}} {
		sub, err := a.DeclareSubscriber(key, s.k.handler, WithAllowedOrigin(s.l))
		if err != nil {
			t.Fatal(err)
		}
		defer sub.Undeclare()
	}
	time.Sleep(300 * time.Millisecond)

	_ = a.Put(key, []byte("own"))
	local.wait(t, 1)
	any.wait(t, 1)
	remote.expectNone(t, 200*time.Millisecond)

	_ = b.Put(key, []byte("theirs"))
	remote.wait(t, 1)
	any.wait(t, 1)
	local.expectNone(t, 200*time.Millisecond)
}

func TestLocalityDestination(t *testing.T) {
	r := openRouter(t)
	a, b := clientOf(t, r), clientOf(t, r)
	key := uniq(t)
	mine, theirs := newSink(), newSink()
	s1, err := a.DeclareSubscriber(key, mine.handler)
	if err != nil {
		t.Fatal(err)
	}
	defer s1.Undeclare()
	s2, err := b.DeclareSubscriber(key, theirs.handler)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Undeclare()
	time.Sleep(300 * time.Millisecond)

	_ = a.Put(key, []byte("l"), WithAllowedDestination(LocalitySessionLocal))
	mine.wait(t, 1)
	theirs.expectNone(t, 200*time.Millisecond)

	pub, err := a.DeclarePublisher(key, WithAllowedDestination(LocalityRemote))
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Undeclare()
	_ = pub.Put([]byte("r"))
	theirs.wait(t, 1)
	mine.expectNone(t, 200*time.Millisecond)
}

// A session-local subscriber is not declared: other sessions' publishers
// don't match it, and our own match it unless they publish remotely only.
func TestLocalityMatching(t *testing.T) {
	r := openRouter(t)
	a, b := clientOf(t, r), clientOf(t, r)
	key := uniq(t)
	sub, err := a.DeclareSubscriber(key, func(Sample) {}, WithAllowedOrigin(LocalitySessionLocal))
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Undeclare()
	time.Sleep(300 * time.Millisecond)
	status := func(s *Session, opts ...PutOption) bool {
		p, err := s.DeclarePublisher(key, opts...)
		if err != nil {
			t.Fatal(err)
		}
		defer p.Undeclare()
		time.Sleep(200 * time.Millisecond)
		st, err := p.MatchingStatus()
		if err != nil {
			t.Fatal(err)
		}
		return st.Matching
	}
	if status(b) {
		t.Error("remote publisher matches a session-local subscriber")
	}
	if !status(a) {
		t.Error("own publisher doesn't match")
	}
	if status(a, WithAllowedDestination(LocalityRemote)) {
		t.Error("remote-only publisher matches a session-local subscriber")
	}
}

func TestLocalityQueries(t *testing.T) {
	r := openRouter(t)
	a, b := clientOf(t, r), clientOf(t, r)
	key := uniq(t)
	reply := func(q *Query) { _ = q.Reply(q.KeyExpr(), []byte("x")) }
	ql, err := a.DeclareQueryable(key+"/local", reply, WithAllowedOrigin(LocalitySessionLocal))
	if err != nil {
		t.Fatal(err)
	}
	defer ql.Undeclare()
	qr, err := a.DeclareQueryable(key+"/remote", reply, WithAllowedOrigin(LocalityRemote), WithComplete(true))
	if err != nil {
		t.Fatal(err)
	}
	defer qr.Undeclare()
	time.Sleep(300 * time.Millisecond)

	count := func(s *Session, sel string, opts ...GetOption) int {
		replies, err := collect(s.Get(sel, append(opts, WithTimeout(2*time.Second))...))
		if err != nil {
			t.Fatal(err)
		}
		n := 0
		for _, r := range replies {
			if r.Sample != nil {
				n++
			}
		}
		return n
	}
	cases := []struct {
		name string
		s    *Session
		sel  string
		opts []GetOption
		want int
	}{
		{"own get, local queryable", a, key + "/local", nil, 1},
		{"remote get, local queryable", b, key + "/local", nil, 0},
		{"own get, remote queryable", a, key + "/remote", nil, 0},
		{"remote get, remote queryable", b, key + "/remote", nil, 1},
		{"own remote-only get, local queryable", a, key + "/local", []GetOption{WithAllowedDestination(LocalityRemote)}, 0},
		{"own local-only get, both", a, key + "/*", []GetOption{WithAllowedDestination(LocalitySessionLocal)}, 1},
	}
	for _, c := range cases {
		start := time.Now()
		if got := count(c.s, c.sel, c.opts...); got != c.want {
			t.Errorf("%s: %d replies, want %d", c.name, got, c.want)
		}
		if time.Since(start) > time.Second {
			t.Errorf("%s: took %v (waited for the timeout)", c.name, time.Since(start))
		}
	}
	// A querier's matching status follows its destination.
	qrr, err := b.DeclareQuerier(key+"/local", WithAllowedDestination(LocalityAny))
	if err != nil {
		t.Fatal(err)
	}
	defer qrr.Undeclare()
	time.Sleep(200 * time.Millisecond)
	if st, _ := qrr.MatchingStatus(); st.Matching {
		t.Error("remote querier matches a session-local queryable")
	}
}

// Peers push their declarations to each other: a session-local subscriber
// must not be pushed, at link-up or later.
func TestLocalityPeers(t *testing.T) {
	a := openRegionNode(t, regionOpts{mode: ModePeer, listen: true})
	key := uniq(t)
	early, err := a.DeclareSubscriber(key+"/early", func(Sample) {}, WithAllowedOrigin(LocalitySessionLocal))
	if err != nil {
		t.Fatal(err)
	}
	defer early.Undeclare()
	b := openRegionNode(t, regionOpts{mode: ModePeer}, loc(a))
	waitFor(t, "peers linked", func() bool { return len(b.PeersZID()) == 1 })
	late, err := a.DeclareSubscriber(key+"/late", func(Sample) {}, WithAllowedOrigin(LocalitySessionLocal))
	if err != nil {
		t.Fatal(err)
	}
	defer late.Undeclare()
	any, err := a.DeclareSubscriber(key+"/any", func(Sample) {})
	if err != nil {
		t.Fatal(err)
	}
	defer any.Undeclare()
	matching := func(k string) bool {
		p, err := b.DeclarePublisher(k)
		if err != nil {
			t.Fatal(err)
		}
		defer p.Undeclare()
		st, _ := p.MatchingStatus()
		return st.Matching
	}
	waitFor(t, "normal subscriber known", func() bool { return matching(key + "/any") })
	if matching(key+"/early") || matching(key+"/late") {
		t.Fatal("session-local subscriber declared to a peer")
	}
}
