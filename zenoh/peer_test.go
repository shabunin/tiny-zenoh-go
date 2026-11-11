package zenoh

import (
	"fmt"
	"os"
	"testing"
	"time"
)

// openPeer opens a peer-mode session listening on loopback, without
// multicast scouting (so parallel tests don't discover each other).
func openPeer(t *testing.T, connect ...string) *Session {
	t.Helper()
	s, err := Open(Config{
		Mode:     ModePeer,
		Listen:   []string{"tcp/127.0.0.1:0"},
		Connect:  connect,
		Scouting: ScoutingConfig{Disable: true},
	})
	if err != nil {
		t.Fatalf("Open peer: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// waitFor polls cond for up to 3s.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(3 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		if cond() {
			return
		}
	}
	t.Fatalf("timeout waiting for %s", what)
}

func connected(a, b *Session) bool {
	for _, z := range a.PeersZID() {
		if z.Equal(b.ZID()) {
			return true
		}
	}
	return false
}

func TestPeerToPeerPubSub(t *testing.T) {
	a := openPeer(t)
	b := openPeer(t, a.Locators()...)
	waitFor(t, "peers connected", func() bool { return connected(a, b) && connected(b, a) })

	// Subscriber declared before and after connecting must both get data.
	sinkA := newSink()
	subA, _ := a.DeclareSubscriber("p2p/**", sinkA.handler)
	defer subA.Undeclare()
	sinkB := newSink()
	subB, _ := b.DeclareSubscriber("p2p/b/*", sinkB.handler)
	defer subB.Undeclare()
	settle()

	_ = b.Put("p2p/x", []byte("from-b"), WithAttachment([]byte("att")))
	pub, _ := a.DeclarePublisher("p2p/b/y")
	_ = pub.Put([]byte("from-a"))
	_ = pub.Delete()

	got := sinkA.wait(t, 3) // own publisher's samples reach own subscriber too
	var fromB *Sample
	for i := range got {
		if got[i].KeyExpr == "p2p/x" {
			fromB = &got[i]
		}
	}
	if fromB == nil || string(fromB.Payload) != "from-b" || string(fromB.Attachment) != "att" {
		t.Errorf("a got %+v", got)
	}
	gotB := sinkB.wait(t, 2)
	if string(gotB[0].Payload) != "from-a" || gotB[1].Kind != SampleKindDelete || gotB[1].KeyExpr != "p2p/b/y" {
		t.Errorf("b got %+v", gotB)
	}

	// Writer-side filtering: b has no subscriber on p2p/x, so a must not send.
	// (Checked indirectly: after b undeclares, nothing arrives.)
	_ = subB.Undeclare()
	settle()
	_ = pub.Put([]byte("nobody"))
	time.Sleep(100 * time.Millisecond)
	sinkB.mu.Lock()
	n := len(sinkB.ss)
	sinkB.mu.Unlock()
	if n != 2 {
		t.Errorf("b got %d samples after undeclare", n)
	}
}

func TestPeerToPeerQuery(t *testing.T) {
	a := openPeer(t)
	q, _ := a.DeclareQueryable("p2pq/**", func(q *Query) {
		_ = q.Reply("p2pq/one", []byte("a:"+q.Parameters()+":"+string(q.Payload())))
	}, WithComplete(true))
	defer q.Undeclare()
	b := openPeer(t, a.Locators()...)
	waitFor(t, "peers connected", func() bool { return connected(a, b) && connected(b, a) })
	settle()

	replies, err := b.Get("p2pq/**?k=v", WithPayload([]byte("body"), EncodingTextPlain), WithTimeout(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for r := range replies {
		smp, err := r.Result()
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, smp.KeyExpr+"="+string(smp.Payload))
	}
	if len(got) != 1 || got[0] != "p2pq/one=a:k=v:body" {
		t.Errorf("replies = %v", got)
	}

	// A query nobody can answer completes immediately (no remote queryable).
	start := time.Now()
	replies, _ = b.Get("elsewhere/**", WithTimeout(3*time.Second))
	for range replies {
		t.Error("unexpected reply")
	}
	if time.Since(start) > time.Second {
		t.Errorf("unanswerable query took %v", time.Since(start))
	}

	// A peer going away mid-query releases the query.
	slow, _ := a.DeclareQueryable("slow", func(q *Query) { q.Detach() })
	defer slow.Undeclare()
	settle()
	replies, _ = b.Get("slow", WithTimeout(5*time.Second))
	time.Sleep(100 * time.Millisecond)
	start = time.Now()
	_ = a.Close()
	for range replies {
	}
	if time.Since(start) > 2*time.Second {
		t.Errorf("query not released when peer left (%v)", time.Since(start))
	}
}

func TestPeerToPeerLiveliness(t *testing.T) {
	a := openPeer(t)
	tok, _ := a.Liveliness().DeclareToken("p2pl/a")
	b := openPeer(t, a.Locators()...)
	waitFor(t, "peers connected", func() bool { return connected(a, b) && connected(b, a) })
	settle()

	replies, _ := b.Liveliness().Get("p2pl/**", WithTimeout(2*time.Second))
	var keys []string
	for r := range replies {
		keys = append(keys, r.Sample.KeyExpr)
	}
	if len(keys) != 1 || keys[0] != "p2pl/a" {
		t.Errorf("liveliness get = %v", keys)
	}

	sink := newSink()
	ls, _ := b.Liveliness().DeclareSubscriber("p2pl/**", sink.handler, true)
	defer ls.Undeclare()
	tok2, _ := a.Liveliness().DeclareToken("p2pl/a2")
	_ = tok.Undeclare()
	got := sink.wait(t, 3)
	if got[0].KeyExpr != "p2pl/a" || got[1].KeyExpr != "p2pl/a2" || got[1].Kind != SampleKindPut ||
		got[2].KeyExpr != "p2pl/a" || got[2].Kind != SampleKindDelete {
		t.Errorf("events = %+v", got)
	}
	// Peer disappears: its remaining token is reported gone.
	_ = a.Close()
	got = sink.wait(t, 1)
	if got[3].KeyExpr != "p2pl/a2" || got[3].Kind != SampleKindDelete {
		t.Errorf("disconnect event = %+v", got[3])
	}
	_ = tok2
}

func TestPeerReconnect(t *testing.T) {
	a := openPeer(t)
	loc := a.Locators()[0]
	b := openPeer(t, loc)
	waitFor(t, "connected", func() bool { return connected(b, a) })
	// Restart a on the same address with the same ZID: b's connector must
	// reconnect and re-declare its subscriber.
	zid := a.ZID()
	_ = a.Close()
	waitFor(t, "disconnected", func() bool { return !connected(b, a) })
	sink := newSink()
	sub, _ := b.DeclareSubscriber("re/**", sink.handler)
	defer sub.Undeclare()
	a2, err := Open(Config{Mode: ModePeer, ZID: zid, Listen: []string{loc}, Scouting: ScoutingConfig{Disable: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer a2.Close()
	waitFor(t, "reconnected", func() bool { return connected(b, a2) })
	waitFor(t, "subscriber known", func() bool {
		f := a2.faceByZID(b.ZID())
		if f == nil {
			return false
		}
		a2.mu.Lock()
		defer a2.mu.Unlock()
		return len(f.subs) == 1
	})
	_ = a2.Put("re/x", []byte("again"))
	if got := sink.wait(t, 1); string(got[0].Payload) != "again" {
		t.Errorf("got %+v", got[0])
	}
}

func TestPeerDuplicateConnections(t *testing.T) {
	a := openPeer(t)
	// b connects to a twice through the same locator: one face only.
	b := openPeer(t, a.Locators()[0], a.Locators()[0])
	waitFor(t, "connected", func() bool { return connected(a, b) && connected(b, a) })
	time.Sleep(300 * time.Millisecond)
	if n := len(b.PeersZID()); n != 1 {
		t.Errorf("b has %d peer faces", n)
	}
	if n := len(a.PeersZID()); n != 1 {
		t.Errorf("a has %d peer faces", n)
	}
}

// TestPeerScouting discovers peers by UDP multicast. Opt-in, since it needs
// working multicast and may see other Zenoh nodes on the network.
func TestPeerScouting(t *testing.T) {
	if os.Getenv("ZENOH_MULTICAST") == "" {
		t.Skip("set ZENOH_MULTICAST=1 to test multicast scouting")
	}
	addr := fmt.Sprintf("224.0.0.224:%d", 17000+time.Now().Nanosecond()%1000) // private group port
	open := func() *Session {
		s, err := Open(Config{Mode: ModePeer, Scouting: ScoutingConfig{Address: addr}})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = s.Close() })
		return s
	}
	a, b := open(), open()
	waitFor(t, "scouted connection", func() bool { return connected(a, b) && connected(b, a) })

	var hellos []Hello
	done := make(chan struct{})
	sc, err := Scout(MatchPeer, ScoutingConfig{Address: addr}, func(h Hello) {
		if h.ZID.Equal(a.ZID()) && len(hellos) == 0 {
			hellos = append(hellos, h)
			close(done)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sc.Stop()
	select {
	case <-done:
		if hellos[0].WhatAmI != WhatAmIPeer || len(hellos[0].Locators) == 0 {
			t.Errorf("hello = %+v", hellos[0])
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no HELLO from peer a")
	}
}
