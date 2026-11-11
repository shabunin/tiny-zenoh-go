package zenoh

import (
	"testing"
	"time"
)

// openRouter starts a tiny-zenoh-go router on loopback.
func openRouter(t *testing.T) *Session {
	t.Helper()
	r, err := Open(Config{Mode: ModeRouter, Listen: []string{"tcp/127.0.0.1:0"},
		Scouting: ScoutingConfig{Disable: true}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	return r
}

func clientOf(t *testing.T, r *Session) *Session {
	t.Helper()
	s, err := Open(Config{Connect: r.Locators()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// TestGoRouterRustClients: only Rust endpoints, only Go routing. A Rust
// declared publisher and querier send nothing until the router answers their
// INTERESTs with matching subscribers/queryables, so this checks interest
// handling end to end.
func TestGoRouterRustClients(t *testing.T) {
	r := openRouter(t)
	loc := r.Locators()[0]
	pfx := uniq(t)

	sub := startRust(t, loc, true, "sub", pfx+"/data/**", "3")
	startRust(t, loc, false, "publisher", pfx+"/data/x", "3")
	for i := range 3 {
		if e := sub.next(); e.Payload != "pub-"+string(rune('0'+i)) || e.Key != pfx+"/data/x" {
			t.Errorf("rust sub got %+v", e)
		}
	}

	startRust(t, loc, true, "queryable", pfx+"/q/**", "6")
	qr := startRust(t, loc, false, "querier", pfx+"/q/one")
	var got []string
	for e := qr.next(); !e.Done; e = qr.next() {
		if e.Ok == nil {
			t.Fatalf("querier got %+v", e)
		}
		got = append(got, e.Ok.Payload)
	}
	if len(got) != 1 || got[0] != "py::qbody" {
		t.Errorf("querier replies = %v", got)
	}

	startRust(t, loc, true, "token", pfx+"/tok", "6")
	ls := startRust(t, loc, true, "livsub", pfx+"/**", "1")
	if e := ls.next(); e.Key != pfx+"/tok" || e.Kind != "PUT" {
		t.Errorf("liveliness = %+v", e)
	}
	if n := len(r.ClientsZID()); n < 2 { // queryable + token holders are still connected
		t.Errorf("router sees %d clients", n)
	}
}

// TestGoRouterMixed: Go client, Rust client and Rust peer on one Go router.
func TestGoRouterMixed(t *testing.T) {
	r := openRouter(t)
	loc := r.Locators()[0]
	pfx := uniq(t)
	c := clientOf(t, r)
	sink := newSink()
	sub, _ := c.DeclareSubscriber(pfx+"/**", sink.handler)
	defer sub.Undeclare()
	settle()

	// A Rust peer attached to the router publishes with a declared publisher.
	conf := rustPeerConf("", loc)
	startRust(t, conf, false, "publisher", pfx+"/from-peer", "2")
	got := sink.wait(t, 2)
	if got[0].KeyExpr != pfx+"/from-peer" || string(got[1].Payload) != "pub-1" {
		t.Errorf("go client got %+v", got)
	}

	// The router's own session participates too.
	rsink := newSink()
	rsub, _ := r.DeclareSubscriber(pfx+"/**", rsink.handler)
	defer rsub.Undeclare()
	_ = c.Put(pfx+"/to-router", []byte("hi"))
	if got := rsink.wait(t, 1); string(got[0].Payload) != "hi" {
		t.Errorf("router session got %+v", got)
	}
}

// TestGoRouterInterestUpdates checks Future interests: a client learns about
// subscribers appearing and disappearing elsewhere.
func TestGoRouterInterestUpdates(t *testing.T) {
	r := openRouter(t)
	a, b := clientOf(t, r), clientOf(t, r)
	pfx := uniq(t)

	// a's liveliness subscriber (a Future interest on tokens).
	sink := newSink()
	ls, _ := a.Liveliness().DeclareSubscriber(pfx+"/**", sink.handler, false)
	defer ls.Undeclare()
	settle()
	t1, _ := b.Liveliness().DeclareToken(pfx + "/one")
	t2, _ := b.Liveliness().DeclareToken(pfx + "/one") // same key: aggregated
	_ = t1.Undeclare()                                 // still alive through t2
	time.Sleep(100 * time.Millisecond)
	_ = t2.Undeclare()
	got := sink.wait(t, 2)
	if got[0].Kind != SampleKindPut || got[1].Kind != SampleKindDelete {
		t.Errorf("events = %+v", got)
	}
	time.Sleep(100 * time.Millisecond)
	sink.mu.Lock()
	if len(sink.ss) != 2 {
		t.Errorf("extra events: %+v", sink.ss)
	}
	sink.mu.Unlock()

	// A query to a client whose queryable vanished must still complete.
	q, _ := b.DeclareQueryable(pfx+"/slow", func(q *Query) { q.Detach() })
	settle()
	replies, _ := a.Get(pfx+"/slow", WithTimeout(5*time.Second))
	time.Sleep(100 * time.Millisecond)
	start := time.Now()
	_ = q.Undeclare()
	_ = b.Close() // b leaves: the router must send a the final
	for range replies {
	}
	if time.Since(start) > 2*time.Second {
		t.Errorf("query not released (%v)", time.Since(start))
	}
}
