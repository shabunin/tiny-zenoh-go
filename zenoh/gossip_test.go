package zenoh

import (
	"testing"
	"time"
)

// Gossip scouting: peers configured to reach only a middle peer B end up
// linked directly. Peers do not route for each other, so data from A reaching
// C proves the direct link.

func openGossipPeer(t *testing.T, disable bool, connect ...string) *Session {
	t.Helper()
	s, err := Open(Config{
		Mode:     ModePeer,
		Listen:   []string{"tcp/127.0.0.1:0"},
		Connect:  connect,
		Scouting: ScoutingConfig{Disable: true},
		Gossip:   GossipConfig{Disable: disable},
	})
	if err != nil {
		t.Fatalf("Open peer: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func checkPubSub(t *testing.T, from, to *Session) {
	t.Helper()
	pfx := uniq(t)
	sink := newSink()
	sub, err := to.DeclareSubscriber(pfx, sink.handler)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Undeclare()
	waitFor(t, "subscriber known", func() bool {
		from.mu.Lock()
		defer from.mu.Unlock()
		f := from.faces[string(to.ZID().Bytes())]
		return f != nil && len(f.subs) > 0
	})
	_ = from.Put(pfx, []byte("via-gossip"))
	if got := sink.wait(t, 1); string(got[0].Payload) != "via-gossip" {
		t.Fatalf("got %q", got[0].Payload)
	}
}

func TestGossipGoPeers(t *testing.T) {
	b := openGossipPeer(t, false)
	a := openGossipPeer(t, false, b.Locators()[0])
	waitFor(t, "a-b", func() bool { return connected(a, b) })
	c := openGossipPeer(t, false, b.Locators()[0])
	waitFor(t, "a-c via gossip", func() bool { return connected(a, c) && connected(c, a) })
	checkPubSub(t, a, c)
	checkPubSub(t, c, a)

	// Only one transport per pair, even though both sides learnt of each other.
	time.Sleep(300 * time.Millisecond)
	if n := len(a.PeersZID()); n != 2 {
		t.Errorf("a has %d peers, want 2", n)
	}
}

func TestGossipDisabled(t *testing.T) {
	b := openGossipPeer(t, false)
	a := openGossipPeer(t, true, b.Locators()[0])
	c := openGossipPeer(t, true, b.Locators()[0])
	waitFor(t, "a-b, c-b", func() bool { return connected(a, b) && connected(c, b) })
	time.Sleep(time.Second)
	if connected(a, c) {
		t.Fatal("a and c connected with gossip disabled")
	}
}

// A Rust peer in the middle: Go peers learn of each other from its gossip.
func TestGossipRustMiddle(t *testing.T) {
	loc := freeLocator(t)
	startRust(t, rustPeerConf(loc, ""), true, "token", uniq(t), "30")
	a := openGossipPeer(t, false, loc)
	waitFor(t, "a-rust", func() bool { return len(a.PeersZID()) == 1 })
	c := openGossipPeer(t, false, loc)
	waitFor(t, "a-c via rust gossip", func() bool { return connected(a, c) && connected(c, a) })
	checkPubSub(t, a, c)
}

// A Go peer in the middle: a Rust peer and a Go peer learn of each other from
// our gossip.
func TestGossipGoMiddle(t *testing.T) {
	pfx := uniq(t)
	b := openGossipPeer(t, false)
	rs := startRust(t, rustPeerConf("tcp/127.0.0.1:0", b.Locators()[0]), true, "sub", pfx, "1")
	waitFor(t, "rust-b", func() bool { return len(b.PeersZID()) == 1 })
	rz := b.PeersZID()[0]
	c := openGossipPeer(t, false, b.Locators()[0])
	waitFor(t, "c-rust via go gossip", func() bool { return len(c.PeersZID()) == 2 })
	waitFor(t, "rust subscriber known", func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		f := c.faces[string(rz.Bytes())]
		return f != nil && len(f.subs) > 0
	})
	_ = c.Put(pfx, []byte("via-gossip"))
	if e := rs.next(); e.Key != pfx || e.Payload != "via-gossip" {
		t.Fatalf("rust got %+v", e)
	}
}
