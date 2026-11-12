package zenoh

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestRouteCacheLocalSubs: our own subscribers see a put right after they
// are declared and none after they are undeclared, with routes cached
// before (both the scan of a few subscribers and the per-key index of many).
func TestRouteCacheLocalSubs(t *testing.T) {
	for _, extra := range []int{0, subsIndexMin} {
		t.Run(fmt.Sprintf("extra=%d", extra), func(t *testing.T) {
			s, err := Open(Config{Mode: ModePeer, Scouting: ScoutingConfig{Disable: true}})
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			for i := range extra {
				if _, err := s.DeclareSubscriber(fmt.Sprintf("other/%d", i), func(Sample) {}); err != nil {
					t.Fatal(err)
				}
			}
			pub, err := s.DeclarePublisher("rc/a")
			if err != nil {
				t.Fatal(err)
			}
			var n atomic.Int32
			_ = pub.Put([]byte("before")) // caches "nobody"
			_ = s.Put("rc/a", []byte("before"))
			sub, err := s.DeclareSubscriber("rc/*", func(Sample) { n.Add(1) })
			if err != nil {
				t.Fatal(err)
			}
			_ = pub.Put([]byte("x"))
			_ = s.Put("rc/a", []byte("y"))
			if got := n.Load(); got != 2 {
				t.Fatalf("after declare: %d samples, want 2", got)
			}
			if err := sub.Undeclare(); err != nil {
				t.Fatal(err)
			}
			_ = pub.Put([]byte("x"))
			_ = s.Put("rc/a", []byte("y"))
			if got := n.Load(); got != 2 {
				t.Fatalf("after undeclare: %d samples, want 2", got)
			}
		})
	}
}

// TestRouteCacheRouter: a router forwards a publisher's data to subscribers
// as they come and go, though it has its route cached all along.
func TestRouteCacheRouter(t *testing.T) {
	r := openRouter(t)
	p := clientOf(t, r)
	pub, err := p.DeclarePublisher("rc/k")
	if err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { // keep the routes warm
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			_ = pub.Put([]byte("v"))
			_ = p.Put("rc/k", []byte("w"))
			time.Sleep(time.Millisecond)
		}
	}()
	defer func() { close(stop); wg.Wait() }()

	recv := func(s *Session, key string) *atomic.Int32 {
		var n atomic.Int32
		if _, err := s.DeclareSubscriber(key, func(Sample) { n.Add(1) }); err != nil {
			t.Fatal(err)
		}
		return &n
	}
	// A subscriber on a new client, then on a second one.
	c1, err := Open(Config{Connect: r.Locators()})
	if err != nil {
		t.Fatal(err)
	}
	n1 := recv(c1, "rc/**")
	waitFor(t, "first subscriber", func() bool { return n1.Load() > 0 })
	c2 := clientOf(t, r)
	n2 := recv(c2, "rc/k")
	waitFor(t, "second subscriber", func() bool { return n2.Load() > 0 })

	// The first client goes: its face is gone from the routes (sending to
	// it would fail quietly, so look at the router's own count of faces
	// routed to).
	_ = c1.Close()
	waitFor(t, "first client gone", func() bool {
		rt := r.forwardRouteFor(t, "rc/k")
		return rt != nil && len(rt.faces) == 1
	})
	// A subscriber of the publisher's own session, from a key the router
	// had cached as matching nobody.
	_ = p.Put("rc/other", []byte("z"))
	n3 := recv(c2, "rc/other")
	waitFor(t, "subscriber on a key cached as unwanted", func() bool {
		_ = p.Put("rc/other", []byte("z"))
		return n3.Load() > 0
	})

	// Undeclared: the router stops forwarding to c2 once it learns.
	c3 := clientOf(t, r)
	sub, err := c3.DeclareSubscriber("rc/k", func(Sample) {})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "third subscriber routed", func() bool {
		rt := r.forwardRouteFor(t, "rc/k")
		return rt != nil && len(rt.faces) == 2
	})
	_ = sub.Undeclare()
	waitFor(t, "undeclared subscriber unrouted", func() bool {
		rt := r.forwardRouteFor(t, "rc/k")
		return rt != nil && len(rt.faces) == 1
	})
}

// forwardRouteFor is the router's route for data on key from a face that
// has sent some lately (cached or computed afresh), nil if none has.
func (s *Session) forwardRouteFor(t *testing.T, key string) *dataRoute {
	t.Helper()
	s.mu.Lock()
	var faces []*face
	for _, f := range s.faces {
		faces = append(faces, f)
	}
	s.mu.Unlock()
	for _, f := range faces {
		f.routes.mu.RLock()
		_, sent := f.routes.m[key]
		f.routes.mu.RUnlock()
		if sent {
			return s.forwardRoute(f, 0, key)
		}
	}
	return nil
}

// TestRouteCacheChurn: data flows through a router and between peers while
// subscribers, publishers and whole sessions come and go. Run with -race; at
// the end a new subscriber still gets the data.
func TestRouteCacheChurn(t *testing.T) {
	r := openRouter(t)
	peer := openPeer(t)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	run := func(f func(i int)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				f(i)
			}
		}()
	}
	for _, hub := range []*Session{r, peer} {
		p, err := Open(Config{Mode: ModePeer, Connect: hub.Locators(), Scouting: ScoutingConfig{Disable: true}})
		if hub == r {
			p, err = Open(Config{Connect: hub.Locators()})
		}
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = p.Close() })
		pub, err := p.DeclarePublisher("churn/a")
		if err != nil {
			t.Fatal(err)
		}
		run(func(i int) {
			_ = pub.Put([]byte("p"))
			_ = p.Put(fmt.Sprintf("churn/k%d", i%50), []byte("q"))
		})
		run(func(i int) { // local subscribers come and go
			sub, err := p.DeclareSubscriber(fmt.Sprintf("churn/k%d", i%50), func(Sample) {})
			if err == nil {
				_ = sub.Undeclare()
			}
		})
		run(func(i int) { // remote sessions come and go
			cfg := Config{Connect: hub.Locators()}
			if hub == peer {
				cfg = Config{Mode: ModePeer, Connect: hub.Locators(), Scouting: ScoutingConfig{Disable: true}}
			}
			c, err := Open(cfg)
			if err != nil {
				return
			}
			_, _ = c.DeclareSubscriber("churn/**", func(Sample) {})
			time.Sleep(5 * time.Millisecond)
			_ = c.Close()
		})
	}
	time.Sleep(1500 * time.Millisecond)
	close(stop)
	wg.Wait()

	// Still routing: a fresh subscriber behind the router gets the data.
	c := clientOf(t, r)
	var n atomic.Int32
	if _, err := c.DeclareSubscriber("churn/a", func(Sample) { n.Add(1) }); err != nil {
		t.Fatal(err)
	}
	p := clientOf(t, r)
	pub, err := p.DeclarePublisher("churn/a")
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "data after churn", func() bool {
		_ = pub.Put([]byte("x"))
		return n.Load() > 0
	})
}
