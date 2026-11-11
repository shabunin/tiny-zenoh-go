package zenoh

import (
	"testing"
	"time"

	"github.com/shabunin/tiny-zenoh-go/protocol"
)

// Router subregions: a gateway router keeps one router mesh per region.

func zidOf(t *testing.T, s string) ZID {
	t.Helper()
	z, err := protocol.ParseZID(s)
	if err != nil {
		t.Fatal(err)
	}
	return z
}

// regionRouter opens a test router (zid "" = random).
func regionRouter(t *testing.T, id, name string, gw []Subregion, connect ...string) *Session {
	t.Helper()
	cfg := Config{Mode: ModeRouter, Connect: connect, RegionName: name,
		Gateway: GatewayConfig{South: gw}, Listen: []string{"tcp/127.0.0.1:0"},
		Scouting: ScoutingConfig{Disable: true}}
	if id != "" {
		cfg.ZID = zidOf(t, id)
	}
	s, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func openClient(t *testing.T, at *Session) *Session {
	t.Helper()
	c, err := Open(Config{Mode: ModeClient, Connect: []string{loc(at)}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// cellRouters: routers named "cell" form a subregion; clients are in one of
// their own (with a custom configuration, unmatched clients are refused).
var cellRouters = []Subregion{
	{Filters: []GatewayFilter{{Modes: MatchRouter, RegionNames: []string{"cell"}}}},
	{Filters: []GatewayFilter{{Modes: MatchClient}}},
}

// meshKnows reports whether s's mesh of region r has node z in its graph.
func meshKnows(s *Session, r Region, z ZID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.meshes[r]
	return m != nil && m.net.knows(zkey(z))
}

func TestRouterSubregion(t *testing.T) {
	g := regionRouter(t, "", "", cellRouters)
	n := regionRouter(t, "", "", nil, loc(g))
	s1 := regionRouter(t, "", "cell", nil, loc(g))
	s2 := regionRouter(t, "", "cell", nil, loc(s1), loc(g))
	cell := Region{South: true, ID: 0, Mode: protocol.Router}
	waitFor(t, "meshes", func() bool {
		return meshKnows(g, cell, s1.ZID()) && meshKnows(g, cell, s2.ZID()) &&
			meshKnows(g, North, n.ZID()) && meshKnows(s2, North, g.ZID()) && meshKnows(n, North, g.ZID())
	})
	if meshKnows(n, North, s1.ZID()) || meshKnows(g, North, s1.ZID()) || meshKnows(s1, North, n.ZID()) {
		t.Fatal("the meshes are not separate")
	}
	if f := faceOf(g, s1.ZID()); !f.mesh || f.region != cell {
		t.Fatalf("gateway's face of s1: mesh %v region %v", f.mesh, f.region)
	}
	if f := faceOf(s1, g.ZID()); !f.mesh || f.region != North || f.remoteBound != BoundSouth {
		t.Fatalf("s1's face of the gateway: mesh %v region %v bound %v", f.mesh, f.region, f.remoteBound)
	}

	cn, c1, c2, cg := openClient(t, n), openClient(t, s1), openClient(t, s2), openClient(t, g)
	pairs := []struct {
		name     string
		from, to *Session
	}{
		{"north-south", cn, c2}, {"south-north", c2, cn}, {"south-south", c1, c2},
		{"gateway-south", g, c2}, {"south-gateway", c1, g}, {"gateway client-south", cg, c1},
		{"router sessions", s2, n},
	}
	for _, p := range pairs {
		t.Run(p.name, func(t *testing.T) { pubSubVia(t, p.from, p.to, nil) })
	}
	t.Run("query", func(t *testing.T) {
		key := uniq(t)
		q, err := c2.DeclareQueryable(key, func(q *Query) { _ = q.Reply(key, []byte("x")) })
		if err != nil {
			t.Fatal(err)
		}
		defer q.Undeclare()
		time.Sleep(300 * time.Millisecond)
		replies, err := collect(cn.Get(key))
		if err != nil || len(replies) != 1 || replies[0].Sample == nil {
			t.Fatalf("replies %v, %v", replies, err)
		}
	})
	t.Run("liveliness", func(t *testing.T) {
		key := uniq(t)
		tok, err := cn.Liveliness().DeclareToken(key)
		if err != nil {
			t.Fatal(err)
		}
		waitFor(t, "token south", func() bool {
			replies, err := collect(c2.Liveliness().Get(key))
			return err == nil && len(replies) == 1
		})
		_ = tok.Undeclare()
		waitFor(t, "token gone", func() bool {
			replies, err := collect(c2.Liveliness().Get(key))
			return err == nil && len(replies) == 0
		})
	})
}

// Two gateways between the north and a router subregion: each sample
// crosses once, and declarations don't bounce between the gateways (no
// entity outlives its source).
func TestRouterSubregionTwoGateways(t *testing.T) {
	n := regionRouter(t, "a1", "", nil)
	g1 := regionRouter(t, "e5", "", cellRouters, loc(n))
	g2 := regionRouter(t, "f6", "", cellRouters, loc(n))
	s1 := regionRouter(t, "b2", "cell", nil, loc(g1), loc(g2))
	s2 := regionRouter(t, "c3", "cell", nil, loc(s1))
	cell := Region{South: true, ID: 0, Mode: protocol.Router}
	waitFor(t, "meshes", func() bool {
		return meshKnows(g1, cell, s2.ZID()) && meshKnows(g2, cell, s2.ZID()) &&
			meshKnows(g1, cell, g2.ZID()) && meshKnows(g2, cell, g1.ZID()) &&
			meshKnows(g1, North, g2.ZID()) && meshKnows(g2, North, g1.ZID())
	})
	cn, c2, cg1 := openClient(t, n), openClient(t, s2), openClient(t, g1)
	pairs := []struct {
		name     string
		from, to *Session
	}{
		{"north-south", cn, c2}, {"south-north", c2, cn}, {"gateway client-south", cg1, c2},
		{"south-gateway client", c2, cg1}, {"gateway-south", g1, c2}, {"south-gateway", c2, g2},
		{"gateway-gateway", g1, g2},
	}
	for _, p := range pairs {
		t.Run(p.name, func(t *testing.T) { pubSubVia(t, p.from, p.to, nil) })
	}
	t.Run("query once", func(t *testing.T) {
		key := uniq(t)
		q, err := c2.DeclareQueryable(key, func(q *Query) { _ = q.Reply(key, []byte("x")) })
		if err != nil {
			t.Fatal(err)
		}
		defer q.Undeclare()
		time.Sleep(300 * time.Millisecond)
		replies, err := collect(cn.Get(key))
		if err != nil || len(replies) != 1 {
			t.Fatalf("replies %v, %v", replies, err)
		}
	})
	t.Run("no stale declarations", func(t *testing.T) {
		key := uniq(t)
		known := func(s *Session) bool {
			s.mu.Lock()
			defer s.mu.Unlock()
			for _, m := range s.meshes {
				if _, own := m.own[kindSub][key]; own || len(m.remote[kindSub][key]) > 0 {
					return true
				}
			}
			return false
		}
		for _, at := range []*Session{c2, cn} {
			sub, err := at.DeclareSubscriber(key, func(Sample) {})
			if err != nil {
				t.Fatal(err)
			}
			waitFor(t, "known", func() bool { return known(n) && known(s2) && known(g1) && known(g2) })
			_ = sub.Undeclare()
			waitFor(t, "forgotten", func() bool { return !known(n) && !known(s2) && !known(g1) && !known(g2) })
		}
	})
}

// A Rust router as the gateway of a subregion of Go routers: they see it as
// a router of their mesh, south of it, and route through it.
func TestRouterSubregionRustGateway(t *testing.T) {
	l := freeLocator(t)
	startRust(t, jsonConf(map[string]any{
		"mode":                       "router",
		"listen/endpoints":           []string{l},
		"scouting/multicast/enabled": false,
		"gateway": map[string]any{"south": []any{
			map[string]any{"filters": []any{map[string]any{"modes": []string{"router"}, "region_names": []string{"cell"}}}},
			map[string]any{"filters": []any{map[string]any{"modes": []string{"client"}}}},
		}},
	}), true, "token", "tiny-zenoh-go/test/subregion/hold", "60")
	n := regionRouter(t, "", "", nil, l)
	s1 := regionRouter(t, "", "cell", nil, l)
	s2 := regionRouter(t, "", "cell", nil, loc(s1))
	waitFor(t, "meshes", func() bool {
		return len(n.RoutersZID()) == 1 && len(s1.RoutersZID()) == 2 && meshKnows(s2, North, s1.ZID())
	})
	var gw ZID
	for _, z := range n.RoutersZID() {
		gw = z
	}
	if f := faceOf(s1, gw); !f.mesh || f.remoteBound != BoundSouth {
		t.Fatalf("s1's face of the Rust gateway: mesh %v bound %v", f.mesh, f.remoteBound)
	}
	if meshKnows(n, North, s1.ZID()) {
		t.Fatal("the north router sees the south mesh")
	}
	t.Run("north-south", func(t *testing.T) { pubSubVia(t, n, s2, nil) })
	t.Run("south-north", func(t *testing.T) { pubSubVia(t, s2, n, nil) })
	t.Run("south-south", func(t *testing.T) { pubSubVia(t, s1, s2, nil) })
	t.Run("query", func(t *testing.T) {
		key := uniq(t)
		q, err := s2.DeclareQueryable(key, func(q *Query) { _ = q.Reply(key, []byte("x")) })
		if err != nil {
			t.Fatal(err)
		}
		defer q.Undeclare()
		time.Sleep(300 * time.Millisecond)
		replies, err := collect(n.Get(key))
		if err != nil || len(replies) != 1 || replies[0].Sample == nil {
			t.Fatalf("replies %v, %v", replies, err)
		}
	})
}

// A Go router as the gateway of a subregion of Rust routers.
func TestRouterSubregionGoGatewayRust(t *testing.T) {
	g := regionRouter(t, "", "", cellRouters)
	router := func(name string, connect ...string) map[string]any {
		c := map[string]any{
			"mode":                       "router",
			"connect/endpoints":          connect,
			"listen/endpoints":           []string{freeLocator(t)},
			"scouting/multicast/enabled": false,
		}
		if name != "" {
			c["region_name"] = name
		}
		return c
	}
	s1conf := router("cell", loc(g))
	startRust(t, jsonConf(s1conf), true, "token", "tiny-zenoh-go/test/subregion/s1", "60")
	s1loc := s1conf["listen/endpoints"].([]string)[0]
	cell := Region{South: true, ID: 0, Mode: protocol.Router}
	waitFor(t, "s1", func() bool {
		g.mu.Lock()
		defer g.mu.Unlock()
		m := g.meshes[cell]
		return m != nil && len(m.net.links) == 1
	})
	for _, dir := range []string{"north-south", "south-north", "south-gateway client"} {
		t.Run(dir, func(t *testing.T) {
			key := uniq(t)
			south := jsonConf(router("cell", s1loc))
			north := jsonConf(router("", loc(g)))
			subAt, pubAt := south, north
			switch dir {
			case "south-north":
				subAt, pubAt = north, south
			case "south-gateway client":
				subAt = loc(g)
				pubAt = south
			}
			sub := startRust(t, subAt, true, "sub", key, "3")
			time.Sleep(500 * time.Millisecond)
			startRust(t, pubAt, false, "publisher", key, "3")
			if got := rustSamples(t, sub, 3); got[0] != "pub-0" || got[2] != "pub-2" {
				t.Fatalf("got %v", got)
			}
			noMoreRust(t, sub, 300*time.Millisecond)
		})
	}
}
