package zenoh

import (
	"testing"
	"time"

	"github.com/shabunin/tiny-zenoh-go/link"
	"github.com/shabunin/tiny-zenoh-go/protocol"
	"github.com/shabunin/tiny-zenoh-go/transport"
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

// TestRouterAggregateInterest: a raw client sends an aggregate
// Current+Future INTEREST (what zenoh-pico's publishers and queriers send)
// and must get one declaration on the interest's own key while matching
// entities exist, undeclared when the last one goes, like zenoh-rust.
func TestRouterAggregateInterest(t *testing.T) {
	r := openRouter(t)
	pfx := uniq(t)
	l, err := link.Dial(r.Locators()[0], 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	decls := make(chan *protocol.Declare, 16)
	tr, err := transport.Open(l, transport.Config{ZID: protocol.ZID(NewZID()), WhatAmI: protocol.Client},
		transport.Callbacks{OnMessage: func(m protocol.Message, _ bool) error {
			if d, ok := m.(*protocol.Declare); ok {
				decls <- d
			}
			return nil
		}})
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()
	next := func() *protocol.Declare {
		t.Helper()
		select {
		case d := <-decls:
			return d
		case <-time.After(3 * time.Second):
			t.Fatal("timeout waiting for a DECLARE")
			return nil
		}
	}

	s := clientOf(t, r)
	sub1, _ := s.DeclareSubscriber(pfx+"/**", func(Sample) {})
	sub2, _ := s.DeclareSubscriber(pfx+"/a/*", func(Sample) {})
	settle()
	key := pfx + "/a/b"
	_ = tr.Send(true, &protocol.Interest{ID: 5, Mode: protocol.InterestCurrentFuture,
		Options:  protocol.InterestKeyExprs | protocol.InterestSubscribers | protocol.InterestAggregate,
		WireExpr: &protocol.WireExpr{Suffix: key}, QoS: protocol.QoSDeclare})

	d := next()
	ds, ok := d.Body.(*protocol.DeclareSubscriber)
	if !ok || ds.WireExpr.Suffix != key || !d.HasInterestID || d.InterestID != 5 {
		t.Fatalf("want one D_SUBSCRIBER on %s for interest 5, got %+v", key, d)
	}
	if d := next(); func() bool { _, ok := d.Body.(*protocol.DeclareFinal); return !ok }() {
		t.Fatalf("want D_FINAL, got %+v", d)
	}
	_ = sub1.Undeclare() // sub2 still matches: nothing changes
	settle()
	_ = sub2.Undeclare()
	u, ok := next().Body.(*protocol.UndeclareSubscriber)
	if !ok || u.ID != ds.ID {
		t.Fatalf("want U_SUBSCRIBER %d, got %+v", ds.ID, u)
	}
	sub3, _ := s.DeclareSubscriber(key, func(Sample) {})
	defer sub3.Undeclare()
	if ds, ok := next().Body.(*protocol.DeclareSubscriber); !ok || ds.WireExpr.Suffix != key {
		t.Fatalf("want D_SUBSCRIBER on %s again, got %+v", key, ds)
	}
	select {
	case d := <-decls:
		t.Errorf("unexpected %+v", d)
	case <-time.After(300 * time.Millisecond):
	}
}
