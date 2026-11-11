package zenoh

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/shabunin/gozenoh/link"
)

// mcastLocator returns a fresh group locator on the default interface; the
// tests need a host where multicast works (ZENOH_MULTICAST=1).
func mcastLocator(t *testing.T) string {
	t.Helper()
	if os.Getenv("ZENOH_MULTICAST") == "" {
		t.Skip("set ZENOH_MULTICAST=1 to run multicast tests")
	}
	ifname, err := link.DefaultMulticastInterface()
	if err != nil {
		t.Skip(err)
	}
	return fmt.Sprintf("udp/224.0.0.%d:%d#iface=%s", 200+rand.IntN(50), 20000+rand.IntN(20000), ifname)
}

func openMcastPeer(t *testing.T, group string) *Session {
	t.Helper()
	s, err := Open(Config{Mode: ModePeer, Listen: []string{group}, Scouting: ScoutingConfig{Disable: true}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestMulticastGoPeers(t *testing.T) {
	group := mcastLocator(t)
	pfx := uniq(t)
	a, b := openMcastPeer(t, group), openMcastPeer(t, group)
	waitFor(t, "group members", func() bool {
		return slices.ContainsFunc(a.MulticastPeers(), b.ZID().Equal) && slices.ContainsFunc(b.MulticastPeers(), a.ZID().Equal)
	})
	if len(a.faceList()) != 0 {
		t.Error("multicast made a unicast face")
	}
	sink := newSink()
	sub, _ := b.DeclareSubscriber(pfx+"/**", sink.handler)
	defer sub.Undeclare()
	big := pattern(50 << 10) // fragmented over 8 KiB batches
	_ = a.Put(pfx+"/small", []byte("hi"))
	_ = a.Put(pfx+"/big", big)
	got := sink.wait(t, 2)
	if string(got[0].Payload) != "hi" || sum(got[1].Payload) != sum(big) {
		t.Errorf("got %q and %d bytes", got[0].Payload, len(got[1].Payload))
	}
}

func TestMulticastRustPeer(t *testing.T) {
	group := mcastLocator(t)
	pfx := uniq(t)
	s := openMcastPeer(t, group)
	c, _ := json.Marshal(map[string]any{
		"mode": "peer", "scouting/multicast/enabled": false, "connect/endpoints": []string{},
		"listen/endpoints": []string{group},
	})
	conf := string(c)

	t.Run("RustReceives", func(t *testing.T) {
		rs := startRust(t, conf, true, "sub", pfx+"/a/**", "1")
		// Rust hears us once our next JOIN (every 2.5 s) reached it.
		stop := make(chan struct{})
		defer close(stop)
		go func() {
			for i := 0; ; i++ {
				_ = s.Put(fmt.Sprintf("%s/a/%d", pfx, i), []byte("from-go"))
				select {
				case <-stop:
					return
				case <-time.After(200 * time.Millisecond):
				}
			}
		}()
		if e := rs.next(); e.Payload != "from-go" {
			t.Errorf("rust got %+v", e)
		}
	})

	t.Run("GoReceives", func(t *testing.T) {
		sink := newSink()
		sub, _ := s.DeclareSubscriber(pfx+"/b", sink.handler)
		defer sub.Undeclare()
		startRust(t, conf, false, "publisher", pfx+"/b", "5")
		got := sink.wait(t, 5)
		for i, smp := range got {
			if string(smp.Payload) != fmt.Sprintf("pub-%d", i) {
				t.Errorf("go got %q", smp.Payload)
			}
		}
		if len(s.MulticastPeers()) == 0 {
			t.Error("rust peer not seen on the group")
		}
	})
}
