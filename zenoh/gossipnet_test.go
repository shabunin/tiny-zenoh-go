package zenoh

import (
	"encoding/json"
	"testing"
	"time"
)

// Multihop gossip. Topology A - B - C - D where C dials B and D and listens
// nowhere (so nobody can dial C), and only A autoconnects. A can learn of D
// only from D's link state relayed D -> C -> B -> A, which single-hop
// gossip never does; then A dials D directly.
// Peers don't route for each other, so data between A and D proves the
// direct link.

type mhOpts struct {
	listen, auto, multihop bool
}

func openMHPeer(t *testing.T, o mhOpts, connect ...string) *Session {
	t.Helper()
	cfg := Config{
		Mode:     ModePeer,
		Connect:  connect,
		Scouting: ScoutingConfig{Disable: true},
		Gossip:   GossipConfig{Multihop: o.multihop, NoAutoconnect: !o.auto},
	}
	if o.listen {
		cfg.Listen = []string{"tcp/127.0.0.1:0"}
	} else {
		cfg.Listen = []string{}
	}
	s, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open peer: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func rustMHConf(listen string, auto bool, connect ...string) string {
	c := map[string]any{
		"mode":                       "peer",
		"scouting/multicast/enabled": false,
		"scouting/gossip/multihop":   true,
		"listen/endpoints":           []string{},
		"connect/endpoints":          append([]string{}, connect...),
	}
	if listen != "" {
		c["listen/endpoints"] = []string{listen}
	}
	if !auto {
		c["scouting/gossip/autoconnect"] = map[string][]string{"peer": {}}
	}
	b, _ := json.Marshal(c)
	return string(b)
}

func TestGossipMultihopGo(t *testing.T) {
	for _, multihop := range []bool{true, false} {
		name := "multihop"
		if !multihop {
			name = "singlehop"
		}
		t.Run(name, func(t *testing.T) {
			b := openMHPeer(t, mhOpts{listen: true, multihop: multihop})
			d := openMHPeer(t, mhOpts{listen: true, multihop: multihop})
			c := openMHPeer(t, mhOpts{multihop: multihop}, b.Locators()[0], d.Locators()[0])
			waitFor(t, "c-b, c-d", func() bool { return connected(c, b) && connected(c, d) })
			a := openMHPeer(t, mhOpts{listen: true, auto: true, multihop: multihop}, b.Locators()[0])
			if !multihop {
				time.Sleep(2 * time.Second)
				if connected(a, d) {
					t.Fatal("a reached d with single-hop gossip")
				}
				return
			}
			waitFor(t, "a-d via multihop gossip", func() bool { return connected(a, d) && connected(d, a) })
			checkPubSub(t, a, d)
			checkPubSub(t, d, a)
			if connected(b, d) || connected(a, c) {
				t.Errorf("relays connected: b-d %v a-c %v", connected(b, d), connected(a, c))
			}
		})
	}
}

// Rust relays (B, C), Go ends (A, D).
func TestGossipMultihopRustRelays(t *testing.T) {
	bloc := freeLocator(t)
	startRust(t, rustMHConf(bloc, false), true, "token", uniq(t)+"/b", "30")
	d := openMHPeer(t, mhOpts{listen: true, multihop: true})
	startRust(t, rustMHConf("", false, bloc, d.Locators()[0]), true, "token", uniq(t)+"/c", "30")
	waitFor(t, "c-d", func() bool { return len(d.PeersZID()) == 1 })
	a := openMHPeer(t, mhOpts{listen: true, auto: true, multihop: true}, bloc)
	waitFor(t, "a-d via rust multihop gossip", func() bool { return connected(a, d) && connected(d, a) })
	checkPubSub(t, a, d)
}

// Go relays (B, C), Rust ends (A, D): Rust A sees Rust D's liveliness token,
// which only a direct link carries.
func TestGossipMultihopGoRelays(t *testing.T) {
	key := uniq(t) + "/d"
	dloc := freeLocator(t)
	startRust(t, rustMHConf(dloc, false), true, "token", key, "30")
	b := openMHPeer(t, mhOpts{listen: true, multihop: true})
	c := openMHPeer(t, mhOpts{multihop: true}, b.Locators()[0], dloc)
	waitFor(t, "c-b, c-d", func() bool { return connected(c, b) && len(c.PeersZID()) == 2 })
	ra := startRust(t, rustMHConf("tcp/127.0.0.1:0", true, b.Locators()[0]), true, "livsub", key, "1")
	if e := ra.next(); e.Key != key || e.Kind != "PUT" {
		t.Fatalf("rust a got %+v", e)
	}
}
