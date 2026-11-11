package zenoh

import (
	"testing"
	"time"

	"github.com/shabunin/tiny-zenoh-go/protocol"
)

// regionOf follows zenoh-rust compute_region_of.
func TestRegionOf(t *testing.T) {
	south := func(id int, w WhatAmI) *Region { return &Region{South: true, ID: id, Mode: w} }
	north := &North
	R, P, C := protocol.Router, protocol.Peer, protocol.Client
	cases := []struct {
		name   string
		mode   Mode
		remote WhatAmI
		ours   *Region
		theirs Bound
		region Region
		rb     Bound
		err    bool
	}{
		{"router-router auto", ModeRouter, R, nil, BoundNone, North, BoundNorth, false},
		{"router-peer auto", ModeRouter, P, nil, BoundNone, *south(0, P), BoundNorth, false},
		{"router-client auto", ModeRouter, C, nil, BoundNone, *south(0, C), BoundNorth, false},
		{"peer-peer auto", ModePeer, P, nil, BoundNone, North, BoundNorth, false},
		{"peer-client auto", ModePeer, C, nil, BoundNone, *south(0, C), BoundNorth, false},
		{"peer-router auto", ModePeer, R, nil, BoundNone, North, BoundSouth, false},
		{"client-peer auto", ModeClient, P, nil, BoundNone, North, BoundSouth, false},
		{"client-client auto", ModeClient, C, nil, BoundNone, North, BoundNone, true},
		// The remote is a custom gateway that puts us south of it.
		{"peer under peer gateway", ModePeer, P, nil, BoundSouth, North, BoundSouth, false},
		// We are a custom gateway.
		{"peer gateway of peer", ModePeer, P, south(1, P), BoundNone, *south(1, P), BoundNorth, false},
		{"peer gateway, peer north", ModePeer, P, north, BoundNone, North, BoundNorth, false},
		{"custom north vs auto south", ModeRouter, P, north, BoundNone, North, BoundNone, true},
		{"remote says north, auto says south", ModePeer, R, nil, BoundNorth, North, BoundNone, true},
		{"north-north same mode", ModePeer, P, north, BoundNorth, North, BoundNorth, false},
		{"north-north mixed modes", ModeRouter, P, north, BoundNorth, North, BoundNone, true},
		{"south-south", ModePeer, P, south(0, P), BoundSouth, North, BoundNone, true},
		{"we south, remote north", ModePeer, P, south(0, P), BoundNorth, *south(0, P), BoundNorth, false},
		{"we north, remote south", ModePeer, P, north, BoundSouth, North, BoundSouth, false},
	}
	for _, c := range cases {
		s := &Session{cfg: Config{Mode: c.mode}}
		r, rb, err := s.regionOf(regionPeer{whatami: c.remote}, c.ours, c.theirs)
		if (err != nil) != c.err {
			t.Errorf("%s: err %v", c.name, err)
			continue
		}
		if err == nil && (r != c.region || rb != c.rb) {
			t.Errorf("%s: got %v/%v, want %v/%v", c.name, r, rb, c.region, c.rb)
		}
	}
}

func TestGatewayFilters(t *testing.T) {
	z1, z2 := NewZID(), NewZID()
	p := regionPeer{zid: z1, whatami: protocol.Peer, regionName: "lab", ifaces: []string{"eth0"}}
	cases := []struct {
		f    GatewayFilter
		want bool
	}{
		{GatewayFilter{}, true},
		{GatewayFilter{Modes: MatchPeer}, true},
		{GatewayFilter{Modes: MatchRouter | MatchClient}, false},
		{GatewayFilter{ZIDs: []ZID{z2, z1}}, true},
		{GatewayFilter{ZIDs: []ZID{z2}}, false},
		{GatewayFilter{RegionNames: []string{"lab"}}, true},
		{GatewayFilter{RegionNames: []string{"plant"}}, false},
		{GatewayFilter{Interfaces: []string{"eth0", "wlan0"}}, true},
		{GatewayFilter{Interfaces: []string{"wlan0"}}, false},
		{GatewayFilter{Modes: MatchPeer, RegionNames: []string{"plant"}}, false},
		{GatewayFilter{RegionNames: []string{"plant"}, Negated: true}, true},
	}
	for i, c := range cases {
		if got := c.f.matches(p); got != c.want {
			t.Errorf("filter %d: got %v", i, got)
		}
	}
	// A node that announces no name never matches a name filter.
	if (&GatewayFilter{RegionNames: []string{"lab"}}).matches(regionPeer{whatami: protocol.Peer}) {
		t.Error("unnamed node matched a region name")
	}
	// A link without known interfaces matches any interface filter (as in
	// zenoh-rust, where all() of nothing is true).
	if !(&GatewayFilter{Interfaces: []string{"x"}}).matches(regionPeer{whatami: protocol.Peer}) {
		t.Error("interface filter on a link without interfaces")
	}
	if (&Subregion{Filters: []GatewayFilter{}}).matches(p) {
		t.Error("empty filter list matched")
	}
	if !(&Subregion{}).matches(p) {
		t.Error("nil filter list did not match")
	}
}

func TestRegionConfigValidation(t *testing.T) {
	bad := []Config{
		{Mode: ModePeer, RegionName: "this-region-name-is-longer-than-32-bytes"},
		{Mode: ModeClient, Gateway: GatewayConfig{South: []Subregion{{}}}},
		{Mode: ModePeer, Gateway: GatewayConfig{South: []Subregion{{Filters: []GatewayFilter{{RegionNames: []string{""}}}}}}},
	}
	for i, c := range bad {
		c.Scouting.Disable = true
		c.Listen = []string{}
		if s, err := Open(c); err == nil {
			_ = s.Close()
			t.Errorf("config %d accepted", i)
		}
	}
}

// -----------------------------------------------------------------------------
// Go nodes
// -----------------------------------------------------------------------------

type regionOpts struct {
	mode    Mode
	name    string
	gateway []Subregion
	gossip  bool // gossip on (and listen, so that it can be dialed)
	listen  bool
}

func openRegionNode(t *testing.T, o regionOpts, connect ...string) *Session {
	t.Helper()
	cfg := Config{
		Mode:       o.mode,
		Connect:    connect,
		Scouting:   ScoutingConfig{Disable: true},
		Gossip:     GossipConfig{Disable: !o.gossip},
		RegionName: o.name,
		Gateway:    GatewayConfig{South: o.gateway},
		Listen:     []string{},
	}
	if o.listen || o.gossip || o.mode == ModeRouter {
		cfg.Listen = []string{"tcp/127.0.0.1:0"}
	}
	s, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func loc(s *Session) string { return s.Locators()[0] }

// faceOf returns s's face to node z.
func faceOf(s *Session, z ZID) *face {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.faces[string(z.Bytes())]
}

// expectNone checks that nothing arrives for a while.
func (k *sampleSink) expectNone(t *testing.T, d time.Duration) {
	t.Helper()
	select {
	case <-k.ch:
		k.mu.Lock()
		defer k.mu.Unlock()
		t.Fatalf("unexpected sample %q", k.ss[len(k.ss)-1].Payload)
	case <-time.After(d):
	}
}

// pubSubVia checks that n puts on from reach a subscriber on to exactly once
// each, once to's subscriber is known where from publishes.
func pubSubVia(t *testing.T, from, to *Session, ready func() bool) {
	t.Helper()
	key := uniq(t)
	k := newSink()
	sub, err := to.DeclareSubscriber(key, k.handler)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Undeclare()
	pub, err := from.DeclarePublisher(key)
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Undeclare()
	if ready != nil {
		waitFor(t, "route ready", ready)
	} else {
		time.Sleep(300 * time.Millisecond)
	}
	for i := 0; i < 5; i++ {
		_ = pub.Put([]byte{'0' + byte(i)})
	}
	got := k.wait(t, 5)
	for i, s := range got {
		if string(s.Payload) != string([]byte{'0' + byte(i)}) {
			t.Fatalf("sample %d: %q", i, s.Payload)
		}
	}
	k.expectNone(t, 300*time.Millisecond)
}

// A router does not relay between the peers of its peer region (zenoh 1.10):
// with the peers also linked to each other, each sample arrives once.
func TestRouterPeerRegionNoDuplicates(t *testing.T) {
	r := openRegionNode(t, regionOpts{mode: ModeRouter})
	a := openRegionNode(t, regionOpts{mode: ModePeer, listen: true}, loc(r))
	b := openRegionNode(t, regionOpts{mode: ModePeer}, loc(r), loc(a))
	waitFor(t, "b linked to a and r", func() bool { return connected(b, a) && len(b.RoutersZID()) == 1 && len(a.RoutersZID()) == 1 })
	if f := faceOf(r, a.ZID()); f == nil || f.region != defaultSouth(protocol.Peer) {
		t.Fatalf("router sees peer in region %v", f.region)
	}
	if f := faceOf(a, r.ZID()); f == nil || f.remoteBound != BoundSouth || !f.routed {
		t.Fatalf("peer sees router: bound %v", f.remoteBound)
	}
	pubSubVia(t, a, b, nil)
}

// Peers attached to the same router, and to nothing else, find each other
// through the router's gossip and connect directly.
func TestRouterGossipsPeerRegion(t *testing.T) {
	r := openRegionNode(t, regionOpts{mode: ModeRouter, gossip: true})
	a := openRegionNode(t, regionOpts{mode: ModePeer, gossip: true}, loc(r))
	b := openRegionNode(t, regionOpts{mode: ModePeer, gossip: true}, loc(r))
	waitFor(t, "peers linked", func() bool { return connected(a, b) })
	pubSubVia(t, a, b, nil)
}

// A peer is the gateway of its clients (zenoh 1.10 auto preset): it routes
// between them, and between them and its north peers.
func TestPeerGatewayForClients(t *testing.T) {
	p := openRegionNode(t, regionOpts{mode: ModePeer, listen: true})
	n := openRegionNode(t, regionOpts{mode: ModePeer}, loc(p))
	c1 := openRegionNode(t, regionOpts{mode: ModeClient}, loc(p))
	c2 := openRegionNode(t, regionOpts{mode: ModeClient}, loc(p))
	waitFor(t, "all attached", func() bool { return connected(n, p) && len(p.ClientsZID()) == 2 })
	t.Run("client-client", func(t *testing.T) { pubSubVia(t, c1, c2, nil) })
	t.Run("client-north", func(t *testing.T) { pubSubVia(t, c1, n, nil) })
	t.Run("north-client", func(t *testing.T) { pubSubVia(t, n, c2, nil) })
	t.Run("query", func(t *testing.T) {
		key := uniq(t)
		q, err := n.DeclareQueryable(key, func(q *Query) { _ = q.Reply(key, []byte("from-n")) })
		if err != nil {
			t.Fatal(err)
		}
		defer q.Undeclare()
		time.Sleep(200 * time.Millisecond)
		replies, err := collect(c1.Get(key))
		if err != nil {
			t.Fatal(err)
		}
		if len(replies) != 1 || string(replies[0].Sample.Payload) != "from-n" {
			t.Fatalf("replies %v", replies)
		}
	})
	t.Run("liveliness", func(t *testing.T) {
		key := uniq(t)
		tok, err := c2.Liveliness().DeclareToken(key)
		if err != nil {
			t.Fatal(err)
		}
		defer tok.Undeclare()
		waitFor(t, "token seen by north peer", func() bool {
			r, err := collect(n.Liveliness().Get(key))
			return err == nil && len(r) == 1
		})
	})
}

// Custom subregions on a router, by region name: peers of different
// subregions talk through the router, peers of one subregion don't (they'd
// link directly).
func TestRouterSubregionsByName(t *testing.T) {
	gw := []Subregion{
		{Filters: []GatewayFilter{{RegionNames: []string{"red"}}}},
		{Filters: []GatewayFilter{{RegionNames: []string{"blue"}}}},
	}
	r := openRegionNode(t, regionOpts{mode: ModeRouter, gateway: gw})
	red := openRegionNode(t, regionOpts{mode: ModePeer, name: "red"}, loc(r))
	blue := openRegionNode(t, regionOpts{mode: ModePeer, name: "blue"}, loc(r))
	red2 := openRegionNode(t, regionOpts{mode: ModePeer, name: "red"}, loc(r))
	waitFor(t, "attached", func() bool { return len(r.PeersZID()) == 3 })
	for _, ti := range r.Transports() {
		if ti.ZID.Equal(blue.ZID()) && (ti.Region != (Region{South: true, ID: 1, Mode: protocol.Peer}) ||
			ti.RegionName != "blue" || ti.Gateway) {
			t.Fatalf("blue: %+v", ti)
		}
	}
	for _, ti := range blue.Transports() {
		if !ti.Gateway || ti.Region != North {
			t.Fatalf("router seen from blue: %+v", ti)
		}
	}
	t.Run("red-blue", func(t *testing.T) { pubSubVia(t, red, blue, nil) })
	t.Run("blue-red", func(t *testing.T) { pubSubVia(t, blue, red, nil) })
	t.Run("red-red", func(t *testing.T) {
		key := uniq(t)
		k := newSink()
		sub, err := red2.DeclareSubscriber(key, k.handler)
		if err != nil {
			t.Fatal(err)
		}
		defer sub.Undeclare()
		time.Sleep(300 * time.Millisecond)
		_ = red.Put(key, []byte("x"))
		k.expectNone(t, 500*time.Millisecond)
	})
}

// A peer as the gateway of a subregion of peers: the south peer treats it
// as its gateway (it is told so in OPEN), traffic crosses both ways.
func TestPeerGatewayForPeers(t *testing.T) {
	gw := []Subregion{{Filters: []GatewayFilter{{Modes: MatchPeer, RegionNames: []string{"cell"}}}}}
	g := openRegionNode(t, regionOpts{mode: ModePeer, gateway: gw, listen: true})
	n := openRegionNode(t, regionOpts{mode: ModePeer}, loc(g))
	s := openRegionNode(t, regionOpts{mode: ModePeer, name: "cell"}, loc(g))
	waitFor(t, "attached", func() bool { return connected(n, g) && connected(s, g) })
	if f := faceOf(s, g.ZID()); !f.routed || f.remoteBound != BoundSouth {
		t.Fatalf("south peer sees gateway: routed %v bound %v", f.routed, f.remoteBound)
	}
	if f := faceOf(g, s.ZID()); f.router == nil || !f.region.South {
		t.Fatalf("gateway sees south peer in %v", f.region)
	}
	if f := faceOf(g, n.ZID()); !f.push || f.region.South {
		t.Fatalf("gateway sees north peer in %v", f.region)
	}
	t.Run("south-north", func(t *testing.T) { pubSubVia(t, s, n, nil) })
	t.Run("north-south", func(t *testing.T) { pubSubVia(t, n, s, nil) })
}

// A peer whose region conflicts with ours is refused.
func TestRegionConflictRefused(t *testing.T) {
	// The router puts every peer north, which a router cannot do with a
	// peer (zenoh-rust: "auto preset conflicts with custom configuration").
	r := openRegionNode(t, regionOpts{mode: ModeRouter,
		gateway: []Subregion{{Filters: []GatewayFilter{{Modes: MatchClient}}}}})
	_, err := Open(Config{Mode: ModeClient, Connect: []string{loc(r)}, Scouting: ScoutingConfig{Disable: true}})
	if err != nil {
		t.Fatalf("client refused: %v", err)
	}
	p := openRegionNode(t, regionOpts{mode: ModePeer}, loc(r))
	time.Sleep(300 * time.Millisecond)
	if connected(p, r) {
		t.Fatal("peer admitted")
	}
}

// Several gateways for one peer: routers a1, b2, c3 in a mesh, peer d4
// attached to a1 and b2 (it tells both, by gossip, that it is linked to
// both). Whichever router a client publishes at, only one copy reaches the
// peer, as with zenoh-rust (checked with the same topology and ZIDs).
func TestMultiGatewayNoDuplicates(t *testing.T) {
	zid := func(s string) ZID {
		z, err := protocol.ParseZID(s)
		if err != nil {
			t.Fatal(err)
		}
		return z
	}
	router := func(id string, connect ...string) *Session {
		s, err := Open(Config{Mode: ModeRouter, ZID: zid(id), Connect: connect,
			Listen: []string{"tcp/127.0.0.1:0"}, Scouting: ScoutingConfig{Disable: true}})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = s.Close() })
		return s
	}
	a1 := router("a1")
	b2 := router("b2", loc(a1))
	c3 := router("c3", loc(a1), loc(b2))
	d, err := Open(Config{Mode: ModePeer, ZID: zid("d4"), Connect: []string{loc(a1), loc(b2)},
		Listen: []string{}, Scouting: ScoutingConfig{Disable: true}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	waitFor(t, "mesh and peer up", func() bool {
		return len(c3.RoutersZID()) == 2 && len(d.RoutersZID()) == 2
	})
	waitFor(t, "gateways known", func() bool {
		for _, r := range []*Session{a1, b2} {
			r.mu.Lock()
			n := r.southNets[defaultSouth(protocol.Peer)]
			ok := n != nil && len(n.gatewaysOf(d.ZID())) == 2
			r.mu.Unlock()
			if !ok {
				return false
			}
		}
		return true
	})
	for _, at := range []*Session{a1, b2, c3} {
		c, err := Open(Config{Mode: ModeClient, Connect: []string{loc(at)}})
		if err != nil {
			t.Fatal(err)
		}
		t.Run("client at "+at.ZID().String(), func(t *testing.T) {
			pubSubVia(t, c, d, nil)
		})
		_ = c.Close()
	}
}
