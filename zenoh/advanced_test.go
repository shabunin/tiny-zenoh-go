package zenoh

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// Advanced pub/sub (zenoh-ext): Go only, then against zenoh-rust both ways.

// lose numbers and caches a publication without sending it, as if the
// network had dropped it.
func (ap *AdvancedPublisher) lose(payload string) {
	ap.prepare(SampleKindPut, []byte(payload), nil)
}

type advSink struct {
	mu     sync.Mutex
	got    []string
	misses []uint32
	ch     chan struct{}
}

func newAdvSink() *advSink { return &advSink{ch: make(chan struct{}, 256)} }

func (k *advSink) sample(s Sample) {
	k.mu.Lock()
	k.got = append(k.got, string(s.Payload))
	k.mu.Unlock()
	k.ch <- struct{}{}
}

func (k *advSink) miss(m Miss) {
	k.mu.Lock()
	k.misses = append(k.misses, m.Count)
	k.mu.Unlock()
}

// wait waits for n samples in total and returns them all.
func (k *advSink) wait(t *testing.T, n int) []string {
	t.Helper()
	deadline := time.After(10 * time.Second)
	for {
		k.mu.Lock()
		if len(k.got) >= n {
			out := append([]string(nil), k.got...)
			k.mu.Unlock()
			return out
		}
		k.mu.Unlock()
		select {
		case <-k.ch:
		case <-deadline:
			k.mu.Lock()
			defer k.mu.Unlock()
			t.Fatalf("got %q, want %d samples", k.got, n)
		}
	}
}

func (k *advSink) expect(t *testing.T, want ...string) {
	t.Helper()
	got := k.wait(t, len(want))
	time.Sleep(200 * time.Millisecond) // nothing more should come
	k.mu.Lock()
	got = append([]string(nil), k.got...)
	k.mu.Unlock()
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func advPub(t *testing.T, s *Session, key string, conf AdvancedPublisherConfig) *AdvancedPublisher {
	t.Helper()
	p, err := s.DeclareAdvancedPublisher(key, conf)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Undeclare() })
	return p
}

func advSub(t *testing.T, s *Session, key string, k *advSink, conf AdvancedSubscriberConfig) *AdvancedSubscriber {
	t.Helper()
	sub, err := s.DeclareAdvancedSubscriber(key, k.sample, conf)
	if err != nil {
		t.Fatal(err)
	}
	sub.DeclareMissListener(k.miss)
	t.Cleanup(func() { _ = sub.Undeclare() })
	return sub
}

func puts(t *testing.T, p *AdvancedPublisher, vals ...string) {
	t.Helper()
	for _, v := range vals {
		if err := p.Put([]byte(v)); err != nil {
			t.Fatal(err)
		}
	}
}

func TestAdvancedHistory(t *testing.T) {
	s := lonePeer(t)
	p := advPub(t, s, "adv/h", AdvancedPublisherConfig{Cache: &CacheConfig{MaxSamples: 3},
		MissDetection: &MissDetectionConfig{}})
	puts(t, p, "0", "1", "2", "3", "4")
	k := newAdvSink()
	advSub(t, s, "adv/*", k, AdvancedSubscriberConfig{History: &HistoryConfig{MaxSamples: 2}})
	k.wait(t, 2)
	puts(t, p, "5")
	k.expect(t, "3", "4", "5")

	// Without a limit: the whole cache.
	k2 := newAdvSink()
	advSub(t, s, "adv/h", k2, AdvancedSubscriberConfig{History: &HistoryConfig{}})
	k2.expect(t, "3", "4", "5")

	// Timestamp ordering when the publisher has no sequence numbers.
	pt := advPub(t, s, "adv/t", AdvancedPublisherConfig{Cache: &CacheConfig{MaxSamples: 10}})
	puts(t, pt, "a", "b")
	k3 := newAdvSink()
	advSub(t, s, "adv/t", k3, AdvancedSubscriberConfig{History: &HistoryConfig{MaxAge: time.Minute}})
	k3.expect(t, "a", "b")
}

func TestAdvancedRecovery(t *testing.T) {
	s := lonePeer(t)
	p := advPub(t, s, "adv/r", AdvancedPublisherConfig{Cache: &CacheConfig{MaxSamples: 10},
		MissDetection: &MissDetectionConfig{}})
	rec, miss := newAdvSink(), newAdvSink()
	advSub(t, s, "adv/r", rec, AdvancedSubscriberConfig{Recovery: &RecoveryConfig{}})
	advSub(t, s, "adv/r", miss, AdvancedSubscriberConfig{})
	puts(t, p, "a")
	p.lose("b")
	p.lose("c")
	puts(t, p, "d")
	rec.expect(t, "a", "b", "c", "d") // retransmitted from the cache, in order
	miss.expect(t, "a", "d")
	if len(miss.misses) != 1 || miss.misses[0] != 2 {
		t.Fatalf("misses %v, want [2]", miss.misses)
	}
	if len(rec.misses) != 0 {
		t.Fatalf("recovering subscriber reported misses %v", rec.misses)
	}
}

func TestAdvancedHeartbeat(t *testing.T) {
	s := lonePeer(t)
	p := advPub(t, s, "adv/hb", AdvancedPublisherConfig{Cache: &CacheConfig{MaxSamples: 10},
		MissDetection: &MissDetectionConfig{Heartbeat: 100 * time.Millisecond}})
	k := newAdvSink()
	advSub(t, s, "adv/hb", k, AdvancedSubscriberConfig{Recovery: &RecoveryConfig{Heartbeat: true}})
	puts(t, p, "a")
	p.lose("b") // the last one: only the heartbeat tells about it
	k.expect(t, "a", "b")

	// Periodic queries find it too.
	k2 := newAdvSink()
	p2 := advPub(t, s, "adv/pq", AdvancedPublisherConfig{Cache: &CacheConfig{MaxSamples: 10},
		MissDetection: &MissDetectionConfig{}})
	advSub(t, s, "adv/pq", k2, AdvancedSubscriberConfig{Recovery: &RecoveryConfig{PeriodicQueries: 100 * time.Millisecond}})
	puts(t, p2, "x")
	p2.lose("y")
	k2.expect(t, "x", "y")
}

// A subscriber learns about a publisher that becomes reachable only after it
// published, and fetches its history.
func TestAdvancedLatePublisher(t *testing.T) {
	loc := freeLocator(t)
	// The publisher's session keeps dialing loc in the background.
	pubS, err := Open(Config{Mode: ModePeer, Connect: []string{loc}, Scouting: ScoutingConfig{Disable: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer pubS.Close()
	p := advPub(t, pubS, "adv/late/x", AdvancedPublisherConfig{Cache: &CacheConfig{MaxSamples: 5},
		MissDetection: &MissDetectionConfig{}, PublisherDetection: true})
	puts(t, p, "1", "2") // nobody hears these

	subS, err := Open(Config{Mode: ModePeer, Listen: []string{loc}, Scouting: ScoutingConfig{Disable: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer subS.Close()
	k := newAdvSink()
	advSub(t, subS, "adv/late/**", k, AdvancedSubscriberConfig{History: &HistoryConfig{DetectLatePublishers: true}})
	k.wait(t, 2)
	puts(t, p, "3")
	k.expect(t, "1", "2", "3")
}

// ---------------------------------------------------------------------------
// Against zenoh-rust (zenoh-ext through the Python binding)
// ---------------------------------------------------------------------------

func payloads(t *testing.T, p *rustPeer, n int) (got []string, misses int) {
	t.Helper()
	for len(got) < n {
		e := p.next()
		if e.Miss != 0 {
			misses += e.Miss
			continue
		}
		got = append(got, e.Payload)
	}
	return got, misses
}

func TestAdvancedGoPubRustSub(t *testing.T) {
	pfx := uniq(t)
	s := openSession(t)
	p := advPub(t, s, pfx+"/x", AdvancedPublisherConfig{Cache: &CacheConfig{MaxSamples: 10},
		MissDetection: &MissDetectionConfig{Heartbeat: 300 * time.Millisecond}, PublisherDetection: true})
	puts(t, p, "go-0", "go-1", "go-2")

	// History: a Rust subscriber joining late gets the cached samples.
	rs := startPeer(t, true, "advsub", pfx+"/**", "6", "hist")
	puts(t, p, "go-3")
	p.lose("go-4") // recovered by the Rust subscriber from our cache
	puts(t, p, "go-5")
	got, misses := payloads(t, rs, 6)
	if want := "go-0,go-1,go-2,go-3,go-4,go-5"; strings.Join(got, ",") != want || misses != 0 {
		t.Fatalf("rust got %v (misses %d), want %s", got, misses, want)
	}

	// Heartbeat recovery: a subscriber with no history learns the last sn
	// from the heartbeat and fetches everything up to it (the whole cache
	// at first), including a lost last sample.
	rh := startPeer(t, true, "advsub", pfx+"/x", "8", "hb")
	puts(t, p, "go-6")
	p.lose("go-7")
	want := "go-0,go-1,go-2,go-3,go-4,go-5,go-6,go-7"
	if got, _ := payloads(t, rh, 8); strings.Join(got, ",") != want {
		t.Fatalf("rust heartbeat subscriber got %v, want %s", got, want)
	}
}

func TestAdvancedRustPubGoSub(t *testing.T) {
	pfx := uniq(t)
	s := openSession(t)
	startPeer(t, true, "advpub", pfx+"/r", "3", "30")

	hist := newAdvSink()
	advSub(t, s, pfx+"/**", hist, AdvancedSubscriberConfig{History: &HistoryConfig{DetectLatePublishers: true, MaxSamples: 10}})
	hist.expect(t, "adv-0", "adv-1", "adv-2")

	// Heartbeat only, no history: the heartbeat (last sn 2) makes us fetch
	// everything we missed.
	hb := newAdvSink()
	advSub(t, s, pfx+"/r", hb, AdvancedSubscriberConfig{Recovery: &RecoveryConfig{Heartbeat: true}})
	hb.expect(t, "adv-0", "adv-1", "adv-2")
	if len(hb.misses) != 0 {
		t.Fatalf("misses %v", hb.misses)
	}
}

func TestAdvancedTimeRange(t *testing.T) {
	for _, c := range []struct {
		in, out string
	}{
		{"[now(-5s)..]", "[now(-5s)..]"},
		{"[now(-1.5s)..now()]", "[now(-1.5s)..now()]"},
		{"]now(-2m)..now(1h)[", "]now(-120s)..now(3600s)["},
		{"[2024-01-02T03:04:05Z..]", "[2024-01-02T03:04:05Z..]"},
		{"[..]", "[..]"},
	} {
		r, err := ParseTimeRange(c.in)
		if err != nil {
			t.Fatalf("%s: %v", c.in, err)
		}
		if r.String() != c.out {
			t.Errorf("%s -> %s, want %s", c.in, r, c.out)
		}
	}
	r, _ := ParseTimeRange("[now(-10s)..now(-5s)]")
	now := time.Now()
	if r.Contains(now) || !r.Contains(now.Add(-7*time.Second)) || r.Contains(now.Add(-11*time.Second)) {
		t.Error("Contains")
	}
	if _, err := ParseTimeRange("now(-5s)"); err == nil {
		t.Error("accepted a range without brackets")
	}
	if got := SinceNow(5 * time.Second).String(); got != "[now(-5s)..]" {
		t.Errorf("SinceNow: %s", got)
	}
	_ = fmt.Sprint()
}
