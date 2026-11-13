package zenoh

import (
	"errors"
	"net"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shabunin/tiny-zenoh-go/protocol"
	"github.com/shabunin/tiny-zenoh-go/transport"
)

// Router mesh tests: Go routers linked with each other and with zenohd.
// The zenohd variants need ZENOH_MESH="tcp/host:port[,tcp/host:port]" naming
// one or two zenohd routers that are not connected to each other.

func meshRouters(t *testing.T, n int) []string {
	t.Helper()
	v := os.Getenv("ZENOH_MESH")
	locs := strings.Split(v, ",")
	if v == "" || len(locs) < n {
		t.Skipf("set ZENOH_MESH to %d zenohd locator(s)", n)
	}
	return locs[:n]
}

func openRouterTo(t *testing.T, connect ...string) *Session {
	t.Helper()
	r, err := Open(Config{Mode: ModeRouter, Listen: []string{"tcp/127.0.0.1:0"},
		Connect: connect, Scouting: ScoutingConfig{Disable: true}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	return r
}

func clientAt(t *testing.T, loc string) *Session {
	t.Helper()
	s, err := Open(Config{Connect: []string{loc}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func collect(ch <-chan Reply, err error) ([]Reply, error) {
	if err != nil {
		return nil, err
	}
	var out []Reply
	for r := range ch {
		out = append(out, r)
	}
	return out, nil
}

// checkMesh exercises pub/sub, queries and liveliness between a and b,
// sessions attached to different routers.
func checkMesh(t *testing.T, a, b *Session) {
	t.Helper()
	pfx := uniq(t)

	sink := newSink()
	sub, err := b.DeclareSubscriber(pfx+"/data/**", sink.handler)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "data across the mesh", func() bool {
		_ = a.Put(pfx+"/data/x", []byte("hello"))
		select {
		case <-sink.ch:
			return true
		case <-time.After(100 * time.Millisecond):
			return false
		}
	})
	_ = sub.Undeclare()

	q, err := b.DeclareQueryable(pfx+"/q/**", func(q *Query) {
		_ = q.Reply(q.KeyExpr(), []byte("answer from b"))
	})
	if err != nil {
		t.Fatal(err)
	}
	defer q.Undeclare()
	waitFor(t, "query across the mesh", func() bool {
		replies, err := collect(a.Get(pfx+"/q/one", WithTimeout(time.Second)))
		return err == nil && len(replies) == 1 && replies[0].Err == nil &&
			string(replies[0].Sample.Payload) == "answer from b"
	})

	tok, err := b.Liveliness().DeclareToken(pfx + "/alive")
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "liveliness across the mesh", func() bool {
		replies, err := collect(a.Liveliness().Get(pfx+"/**", WithTimeout(time.Second)))
		return err == nil && len(replies) == 1 && replies[0].Sample.KeyExpr == pfx+"/alive"
	})
	events := newSink()
	ls, err := a.Liveliness().DeclareSubscriber(pfx+"/**", events.handler, false)
	if err != nil {
		t.Fatal(err)
	}
	defer ls.Undeclare()
	settle() // the subscriber's interest and the undeclaration take different links
	_ = tok.Undeclare()
	if e := events.wait(t, 1)[0]; e.Kind != SampleKindDelete || e.KeyExpr != pfx+"/alive" {
		t.Errorf("liveliness event = %+v", e)
	}
}

func TestGoRouterMesh(t *testing.T) {
	r1 := openRouter(t)
	r2 := openRouterTo(t, r1.Locators()...)
	waitFor(t, "routers linked", func() bool { return len(r1.RoutersZID()) == 1 && len(r2.RoutersZID()) == 1 })
	checkMesh(t, clientOf(t, r1), clientOf(t, r2))
	checkMesh(t, clientOf(t, r2), clientOf(t, r1))
	checkMesh(t, r1, r2) // the routers' own sessions
}

// TestGoRouterMeshChain: r1 - r2 - r3, so r2 forwards on behalf of others.
func TestGoRouterMeshChain(t *testing.T) {
	r1 := openRouter(t)
	r2 := openRouterTo(t, r1.Locators()...)
	r3 := openRouterTo(t, r2.Locators()...)
	waitFor(t, "routers linked", func() bool { return len(r2.RoutersZID()) == 2 })
	checkMesh(t, clientOf(t, r1), clientOf(t, r3))
	checkMesh(t, clientOf(t, r3), clientOf(t, r1))
}

func TestGoRouterMeshZenohd(t *testing.T) {
	zd := meshRouters(t, 1)[0]
	r := openRouterTo(t, zd)
	waitFor(t, "linked to zenohd", func() bool { return len(r.RoutersZID()) == 1 })
	checkMesh(t, clientAt(t, zd), clientOf(t, r))
	checkMesh(t, clientOf(t, r), clientAt(t, zd))
}

// TestGoRouterBridgesZenohd: zenohd - go router - zenohd2.
func TestGoRouterBridgesZenohd(t *testing.T) {
	zds := meshRouters(t, 2)
	r := openRouterTo(t, zds...)
	waitFor(t, "linked to both", func() bool { return len(r.RoutersZID()) == 2 })
	checkMesh(t, clientAt(t, zds[0]), clientAt(t, zds[1]))
	checkMesh(t, clientAt(t, zds[1]), clientAt(t, zds[0]))
	checkMesh(t, clientAt(t, zds[0]), clientOf(t, r))
}

// TestMeshRejoinHold: right after the link to a neighbour router went down,
// a new transport from that router is refused for meshRejoinHold (zenoh-rust
// would not see it as a new child of its trees and never send it the
// declarations lost with the link); clients and peers are not held.
func TestMeshRejoinHold(t *testing.T) {
	r := openRouterTo(t)
	z, _ := protocol.ParseZID("e00042")
	r.mu.Lock()
	r.meshGone[string(z.Bytes())] = time.Now()
	r.mu.Unlock()
	if err := r.admit(newFace(r), z, protocol.Router); !errors.Is(err, errRejoinHold) {
		t.Fatalf("router just gone: %v, want errRejoinHold", err)
	}
	c, _ := protocol.ParseZID("e00043")
	r.mu.Lock()
	r.meshGone[string(c.Bytes())] = time.Now()
	r.mu.Unlock()
	// Admitted faces have no transport: unregister them for Close.
	forget := func(z ZID) {
		r.mu.Lock()
		delete(r.faces, string(z.Bytes()))
		r.mu.Unlock()
	}
	if err := r.admit(newFace(r), c, protocol.Client); err != nil {
		t.Fatalf("client: %v", err)
	}
	forget(c)
	r.mu.Lock()
	r.meshGone[string(z.Bytes())] = time.Now().Add(-meshRejoinHold)
	r.mu.Unlock()
	if err := r.admit(newFace(r), z, protocol.Router); err != nil {
		t.Fatalf("after the hold: %v", err)
	}
	forget(z)
}

// TestOutboxOrder checks that messages queued under the session lock reach a
// face in the order they were queued, whichever outbox is sent first: link
// states sent out of order lost links at the receivers for good (a link
// state naming a NodeId before the one mapping it), and the graphs that
// disagreed looped data.
func TestOutboxOrder(t *testing.T) {
	r := openRouterTo(t)
	z, _ := protocol.ParseZID("e00044")
	c1, c2 := net.Pipe()
	go r.accept(c2, "tcp")
	var mu sync.Mutex
	var got []uint16
	done := make(chan struct{})
	u, err := transport.Open(c1, transport.Config{ZID: z, WhatAmI: protocol.Router, Lease: 2 * time.Second},
		transport.Callbacks{OnMessage: func(m protocol.Message, _ bool) error {
			if o, ok := m.(*protocol.OAM); ok && o.ID >= 0x7700 {
				mu.Lock()
				got = append(got, o.ID)
				if len(got) == 4 {
					close(done)
				}
				mu.Unlock()
			}
			return nil
		}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = u.Close() })
	var f *face
	for deadline := time.Now().Add(2 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		r.mu.Lock()
		f = r.faces[string(z.Bytes())]
		live := f != nil && f.live
		r.mu.Unlock()
		if live {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no face for the neighbour")
		}
	}
	oam := func(id uint16) *protocol.OAM {
		return &protocol.OAM{ID: id, Enc: protocol.ExtUnit, QoS: protocol.QoSDeclare}
	}
	a, b := outbox{}, outbox{}
	r.mu.Lock()
	a.add(f, oam(0x7701))
	a.add(f, oam(0x7702))
	b.add(f, oam(0x7703))
	r.mu.Unlock()
	r.mu.Lock()
	b.add(f, oam(0x7704))
	r.mu.Unlock()
	b.send() // the later outbox first
	a.send()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatalf("got %x, want 4 messages", got)
	}
	mu.Lock()
	defer mu.Unlock()
	if !slices.Equal(got, []uint16{0x7701, 0x7702, 0x7703, 0x7704}) {
		t.Fatalf("got %x, want them in the order queued", got)
	}
}

// fakeRouter connects a scripted neighbour router z to r over a pipe.
func fakeRouter(t *testing.T, r *Session, z ZID) *transport.Unicast {
	t.Helper()
	c1, c2 := net.Pipe()
	go r.accept(c2, "tcp")
	u, err := transport.Open(c1, transport.Config{ZID: z, WhatAmI: protocol.Router, Lease: 2 * time.Second},
		transport.Callbacks{OnMessage: func(protocol.Message, bool) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = u.Close() })
	return u
}

// TestMeshOffTreeData checks that data a neighbour router brings on a tree
// it is not our parent on (zenohd mis-tags data after renumbering routers)
// is neither delivered nor passed on, and that data on the tree does go.
func TestMeshOffTreeData(t *testing.T) {
	r := openRouterTo(t)
	n1, _ := protocol.ParseZID("e00051")
	n2, _ := protocol.ParseZID("e00052")
	var got atomic.Int32
	c := clientAt(t, loc(r))
	if _, err := c.DeclareSubscriber("t/**", func(Sample) { got.Add(1) }); err != nil {
		t.Fatal(err)
	}
	u1, u2 := fakeRouter(t, r, n1), fakeRouter(t, r, n2)
	// Each neighbour: itself (psid 0) linked to r (psid 1); n2 also names
	// n1 (psid 2), linked to r. The graph: n1 - r - n2.
	states := func(self ZID, other *ZID) *protocol.OAM {
		list := []linkState{{psid: 0, sn: 1, zid: self, whatami: protocol.Router, hasWAI: true, links: []uint64{1}},
			{psid: 1, sn: 0, zid: r.zid, whatami: protocol.Router, hasWAI: true}}
		if other != nil {
			list = append(list, linkState{psid: 2, sn: 1, zid: *other, whatami: protocol.Router, hasWAI: true, links: []uint64{1}})
		}
		return &protocol.OAM{ID: oamLinkState, Enc: protocol.ExtZBuf, Buf: encodeLinkStates(list), QoS: protocol.QoSDeclare}
	}
	if err := u1.Send(true, states(n1, nil)); err != nil {
		t.Fatal(err)
	}
	if err := u2.Send(true, states(n2, &n1)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(4 * treesDelay) // the trees
	push := func(u *transport.Unicast, node uint16) {
		t.Helper()
		if err := u.Send(true, &protocol.Push{WireExpr: protocol.WireExpr{Suffix: "t/x"}, NodeID: node,
			QoS: protocol.DefaultQoS, Body: &protocol.Put{Payload: []byte("x")}}); err != nil {
			t.Fatal(err)
		}
	}
	push(u2, 2) // from n2 on n1's tree: r's parent there is n1
	push(u2, 1) // from n2 on r's own tree: ours, come back
	time.Sleep(200 * time.Millisecond)
	if n := got.Load(); n != 0 {
		t.Fatalf("%d off-tree samples delivered", n)
	}
	push(u1, 0) // n1's own data, on its tree
	push(u2, 0)
	for deadline := time.Now().Add(2 * time.Second); got.Load() < 2; time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("%d of 2 on-tree samples delivered", got.Load())
		}
	}
}
