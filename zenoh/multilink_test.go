package zenoh

import (
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/shabunin/gozenoh/link"
	"github.com/shabunin/gozenoh/transport"
)

func openMLPeer(t *testing.T, maxLinks int, listen []string, connect ...string) *Session {
	t.Helper()
	s, err := Open(Config{
		Mode:     ModePeer,
		Listen:   listen,
		Connect:  connect,
		Scouting: ScoutingConfig{Disable: true},
		MaxLinks: maxLinks,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// links returns the link count of s's transport to its single neighbour.
func links(s *Session) int {
	for _, f := range s.faceList() {
		if f.tr != nil {
			return f.tr.Links()
		}
	}
	return 0
}

func TestMultiLinkGoPeers(t *testing.T) {
	pfx := uniq(t)
	a := openMLPeer(t, 2, []string{"tcp/127.0.0.1:0", "udp/127.0.0.1:0"})
	b := openMLPeer(t, 2, nil, a.Locators()...)
	waitFor(t, "two links", func() bool { return links(a) == 2 && links(b) == 2 })
	if n := len(a.faceList()); n != 1 {
		t.Errorf("a has %d faces", n)
	}

	sink := newSink()
	sub, _ := a.DeclareSubscriber(pfx+"/**", sink.handler)
	defer sub.Undeclare()
	settle()
	_ = b.Put(pfx+"/rel", []byte("r"))
	_ = b.Put(pfx+"/be", []byte("b"), WithReliability(BestEffort))
	got := sink.wait(t, 2)
	if string(got[0].Payload)+string(got[1].Payload) != "rb" && string(got[0].Payload)+string(got[1].Payload) != "br" {
		t.Errorf("got %q %q", got[0].Payload, got[1].Payload)
	}
}

func TestMultiLinkRedial(t *testing.T) {
	a := openMLPeer(t, 2, []string{"tcp/127.0.0.1:0", "tcp/127.0.0.1:0"})
	var mu sync.Mutex
	var dialed []transport.Link
	b, err := Open(Config{
		Mode: ModePeer, Connect: a.Locators(), Scouting: ScoutingConfig{Disable: true}, MaxLinks: 2,
		Dial: func(loc string, timeout time.Duration) (transport.Link, error) {
			l, err := link.Dial(loc, timeout)
			if err == nil {
				mu.Lock()
				dialed = append(dialed, l)
				mu.Unlock()
			}
			return l, err
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	waitFor(t, "two links", func() bool { return links(b) == 2 })
	tr := b.faceList()[0].tr
	mu.Lock()
	_ = dialed[1].Close()
	mu.Unlock()
	// The connector redials at once, into the same transport.
	waitFor(t, "relinked", func() bool {
		mu.Lock()
		n := len(dialed)
		mu.Unlock()
		return n == 3 && links(b) == 2 && links(a) == 2
	})
	if b.faceList()[0].tr != tr || len(b.faceList()) != 1 {
		t.Error("transport was replaced instead of kept")
	}
}

func TestMultiLinkRustPeer(t *testing.T) {
	pfx := uniq(t)
	s := openMLPeer(t, 2, []string{"tcp/127.0.0.1:0", "udp/127.0.0.1:0"})
	var locs []string
	for _, l := range s.Locators() {
		if strings.HasPrefix(l, "tcp/") || strings.HasPrefix(l, "udp/") {
			locs = append(locs, l)
		}
	}
	c, _ := json.Marshal(map[string]any{
		"mode": "peer", "scouting/multicast/enabled": false, "listen/endpoints": []string{},
		"connect/endpoints":           locs,
		"transport/unicast/max_links": 2,
	})
	rs := startRust(t, string(c), true, "sub", pfx+"/**", "2")
	waitFor(t, "two links from rust", func() bool { return links(s) == 2 })
	_ = s.Put(pfx+"/rel", []byte("r"))
	_ = s.Put(pfx+"/be", []byte("b"), WithReliability(BestEffort))
	seen := map[string]bool{}
	for range 2 {
		seen[rs.next().Payload] = true
	}
	if !seen["r"] || !seen["b"] {
		t.Errorf("rust got %v", seen)
	}

	sink := newSink()
	sub, _ := s.DeclareSubscriber(pfx+"/b", sink.handler)
	defer sub.Undeclare()
	time.Sleep(300 * time.Millisecond)
	startRust(t, string(c), false, "pub", pfx+"/b")
	sink.wait(t, 2)
}
