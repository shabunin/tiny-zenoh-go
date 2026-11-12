package zenoh

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/shabunin/tiny-zenoh-go/protocol"
)

// TestMultiGatewayFailover: routers a1, b2, c3 in a mesh, peer d4 attached
// to the gateways a1 and b2 (b2, the higher ZID, forwards). When b2 goes,
// a1 takes over: a client at c3 reaches the peer again within a few leases.
func TestMultiGatewayFailover(t *testing.T) {
	zid := func(s string) ZID {
		z, err := protocol.ParseZID(s)
		if err != nil {
			t.Fatal(err)
		}
		return z
	}
	lease := time.Second
	router := func(id string, connect ...string) *Session {
		s, err := Open(Config{Mode: ModeRouter, ZID: zid(id), Connect: connect, Lease: lease,
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
	d, err := Open(Config{Mode: ModePeer, ZID: zid("d4"), Connect: []string{loc(a1), loc(b2)}, Lease: lease,
		Listen: []string{}, Scouting: ScoutingConfig{Disable: true}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	waitFor(t, "mesh and peer up", func() bool {
		return len(c3.RoutersZID()) == 2 && len(d.RoutersZID()) == 2
	})
	var got atomic.Int64
	if _, err := d.DeclareSubscriber("gwf/k", func(Sample) { got.Add(1) }); err != nil {
		t.Fatal(err)
	}
	c, err := Open(Config{Mode: ModeClient, Connect: []string{loc(c3)}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	waitFor(t, "data before", func() bool { _ = c.Put("gwf/k", []byte("x")); return got.Load() > 0 })
	_ = b2.Close()
	t0 := time.Now()
	n := got.Load()
	for got.Load() == n {
		if time.Since(t0) > 8*time.Second {
			a1.mu.Lock()
			net := a1.southNets[defaultSouth(protocol.Peer)]
			var gw []ZID
			if net != nil {
				gw = net.gatewaysOf(d.ZID())
			}
			a1.mu.Unlock()
			t.Fatalf("no data 8 s after the forwarding gateway went; a1 thinks d4's gateways are %v", gw)
		}
		_ = c.Put("gwf/k", []byte("y"))
		time.Sleep(20 * time.Millisecond)
	}
	t.Logf("a1 took over after %v", time.Since(t0).Round(10*time.Millisecond))
}
