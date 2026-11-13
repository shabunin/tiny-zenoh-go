package zenoh

import (
	"encoding/json"
	"testing"
	"time"
)

// Regions against zenoh-rust 1.10 (zpeer.py). These check the two handshake
// extensions both ways (a Rust gateway sorting Go nodes by the region name
// they announce; Go nodes acting on the bound a Rust gateway tells them, and
// the reverse) and routing across region gateways in mixed setups.

func jsonConf(kv map[string]any) string {
	b, _ := json.Marshal(kv)
	return string(b)
}

// rustSamples waits for n samples from a Rust subscriber.
func rustSamples(t *testing.T, p *rustPeer, n int) []string {
	t.Helper()
	var out []string
	for len(out) < n {
		if e := p.next(); e.Key != "" {
			out = append(out, e.Payload)
		}
	}
	return out
}

// noMoreRust checks that a Rust subscriber gets nothing more for a while.
func noMoreRust(t *testing.T, p *rustPeer, d time.Duration) {
	t.Helper()
	for _, e := range p.early {
		if e.Key != "" {
			t.Fatalf("unexpected sample %q", e.Payload)
		}
	}
	p.early = nil
	deadline := time.After(d)
	for {
		select {
		case e, ok := <-p.events:
			if !ok {
				return
			}
			if e.Key != "" {
				t.Fatalf("unexpected sample %q", e.Payload)
			}
		case <-deadline:
			return
		}
	}
}

// A Rust router sorts Go peers into subregions by the name they announce.
// It refuses a peer that matches no subregion (its custom configuration
// would put a peer north of a router), so traffic between red and blue at
// all proves it read the names.
func TestRegionsRustRouterByName(t *testing.T) {
	l := freeLocator(t)
	startRust(t, jsonConf(map[string]any{
		"mode":                       "router",
		"listen/endpoints":           []string{l},
		"scouting/multicast/enabled": false,
		"gateway": map[string]any{"south": []any{
			map[string]any{"filters": []any{map[string]any{"region_names": []string{"red"}}}},
			map[string]any{"filters": []any{map[string]any{"region_names": []string{"blue"}}}},
		}},
	}), true, "token", "tiny-zenoh-go/test/regions/hold", "60")
	red := openRegionNode(t, regionOpts{mode: ModePeer, name: "red"}, l)
	blue := openRegionNode(t, regionOpts{mode: ModePeer, name: "blue"}, l)
	red2 := openRegionNode(t, regionOpts{mode: ModePeer, name: "red"}, l)
	waitFor(t, "attached", func() bool {
		return len(red.RoutersZID()) == 1 && len(blue.RoutersZID()) == 1 && len(red2.RoutersZID()) == 1
	})
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
	t.Run("unnamed refused", func(t *testing.T) {
		p := openRegionNode(t, regionOpts{mode: ModePeer}, l)
		time.Sleep(500 * time.Millisecond)
		if len(p.RoutersZID()) != 0 {
			t.Fatal("unnamed peer admitted")
		}
	})
}

// A Rust peer as the gateway of a subregion of peers; the Go south peer
// learns from OPEN that it is south of it.
func TestRegionsRustPeerGateway(t *testing.T) {
	l := freeLocator(t)
	startRust(t, jsonConf(map[string]any{
		"mode":                       "peer",
		"listen/endpoints":           []string{l},
		"scouting/multicast/enabled": false,
		"gateway": map[string]any{"south": []any{map[string]any{"filters": []any{
			map[string]any{"modes": []string{"peer"}, "region_names": []string{"cell"}}}}}},
	}), true, "token", "tiny-zenoh-go/test/regions/hold", "60")
	s := openRegionNode(t, regionOpts{mode: ModePeer, name: "cell"}, l)
	n := openRegionNode(t, regionOpts{mode: ModePeer}, l)
	waitFor(t, "attached", func() bool { return len(s.PeersZID()) == 1 && len(n.PeersZID()) == 1 })
	g := s.PeersZID()[0]
	if f := faceOf(s, g); !f.routed || f.remoteBound != BoundSouth {
		t.Fatalf("south peer: routed %v bound %v", f.routed, f.remoteBound)
	}
	if f := faceOf(n, g); !f.push || f.remoteBound != BoundNorth {
		t.Fatalf("north peer: push %v bound %v", f.push, f.remoteBound)
	}
	t.Run("south-north", func(t *testing.T) { pubSubVia(t, s, n, nil) })
	t.Run("north-south", func(t *testing.T) { pubSubVia(t, n, s, nil) })
}

// A Go peer as the gateway of a subregion of Rust peers: the Rust south
// peer is sorted by its name and told it is south of us.
func TestRegionsGoPeerGatewayRust(t *testing.T) {
	g := openRegionNode(t, regionOpts{mode: ModePeer, listen: true,
		gateway: []Subregion{{Filters: []GatewayFilter{{Modes: MatchPeer, RegionNames: []string{"cell"}}}}}})
	peer := func(name string) map[string]any {
		c := map[string]any{
			"mode":                       "peer",
			"connect/endpoints":          []string{loc(g)},
			"listen/endpoints":           []string{},
			"scouting/multicast/enabled": false,
			"scouting/gossip/enabled":    false,
		}
		if name != "" {
			c["region_name"] = name
		}
		return c
	}
	for _, dir := range []string{"south-north", "north-south"} {
		t.Run(dir, func(t *testing.T) {
			key := uniq(t)
			subName, pubName := "", "cell"
			if dir == "north-south" {
				subName, pubName = "cell", ""
			}
			sub := startRust(t, jsonConf(peer(subName)), true, "sub", key, "3")
			waitFor(t, "subscriber attached", func() bool { return len(g.PeersZID()) == 1 })
			startRust(t, jsonConf(peer(pubName)), false, "publisher", key, "3")
			// Checked while both are attached: either leaves once done.
			south := func() (n int) {
				g.mu.Lock()
				defer g.mu.Unlock()
				for _, f := range g.faces {
					if f.region.South && f.regionName == "cell" {
						n++
					}
				}
				return n
			}
			waitFor(t, "a face in the cell subregion", func() bool { return len(g.PeersZID()) == 2 && south() == 1 })
			if n := south(); n != 1 {
				t.Fatalf("%d faces in the cell subregion", n)
			}
			if got := rustSamples(t, sub, 3); got[0] != "pub-0" || got[2] != "pub-2" {
				t.Fatalf("got %v", got)
			}
		})
	}
}

// A Go peer is the gateway of its Rust clients (auto preset).
func TestRegionsRustClientsViaGoPeer(t *testing.T) {
	g := openRegionNode(t, regionOpts{mode: ModePeer, listen: true})
	peerConf := jsonConf(map[string]any{
		"mode":                       "peer",
		"connect/endpoints":          []string{loc(g)},
		"listen/endpoints":           []string{},
		"scouting/multicast/enabled": false,
	})
	t.Run("client-peer", func(t *testing.T) {
		key := uniq(t)
		sub := startRust(t, peerConf, true, "sub", key, "3")
		startRust(t, loc(g), false, "publisher", key, "3")
		rustSamples(t, sub, 3)
		noMoreRust(t, sub, 300*time.Millisecond)
	})
	t.Run("peer-client", func(t *testing.T) {
		key := uniq(t)
		sub := startRust(t, loc(g), true, "sub", key, "3")
		startRust(t, peerConf, false, "publisher", key, "3")
		rustSamples(t, sub, 3)
	})
	t.Run("client-client", func(t *testing.T) {
		key := uniq(t)
		sub := startRust(t, loc(g), true, "sub", key, "3")
		startRust(t, loc(g), false, "publisher", key, "3")
		rustSamples(t, sub, 3)
	})
}

// A Go peer that is a gateway (of clients) and has a gateway (a router):
// its clients reach the router's side and back. A client's publisher sends
// only once a subscriber is declared to it, which the peer can only do if
// it learnt the router's subscribers.
func TestRegionsGoPeerUnderRouter(t *testing.T) {
	r := routerLocator(t)
	g := openRegionNode(t, regionOpts{mode: ModePeer, listen: true}, r)
	waitFor(t, "peer attached", func() bool { return len(g.RoutersZID()) == 1 })
	t.Run("up", func(t *testing.T) {
		key := uniq(t)
		sub := startRust(t, r, true, "sub", key, "3")
		startRust(t, loc(g), false, "publisher", key, "3")
		rustSamples(t, sub, 3)
		noMoreRust(t, sub, 300*time.Millisecond)
	})
	t.Run("down", func(t *testing.T) {
		key := uniq(t)
		sub := startRust(t, loc(g), true, "sub", key, "3")
		startRust(t, r, false, "publisher", key, "3")
		rustSamples(t, sub, 3)
	})
	t.Run("query up", func(t *testing.T) {
		key := uniq(t)
		startRust(t, r, true, "queryable", key, "10")
		c, err := Open(Config{Mode: ModeClient, Connect: []string{loc(g)}})
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		replies, err := collect(c.Get(key))
		if err != nil || len(replies) != 1 || replies[0].Sample == nil {
			t.Fatalf("replies %v, %v", replies, err)
		}
	})
}
