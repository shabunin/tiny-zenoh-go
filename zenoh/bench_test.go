package zenoh

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shabunin/tiny-zenoh-go/shm"
)

// End-to-end benchmarks over loopback TCP: publication throughput, ping-pong
// latency and query round trips, peer to peer and through a Go router.
//
//	go test ./zenoh -run XXX -bench . -benchtime 3s

var benchSizes = []int{8, 1024, 64 << 10, 1 << 20}

func benchSession(b *testing.B, cfg Config) *Session {
	b.Helper()
	cfg.Scouting.Disable = true
	s, err := Open(cfg)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = s.Close() })
	return s
}

// benchPair returns two connected sessions: peers connected directly, or
// two clients of a router.
func benchPair(b *testing.B, topo string, shared bool) (a, z *Session) {
	b.Helper()
	var opts *shm.Options
	if shared {
		opts = &shm.Options{PoolSize: 64 << 20}
	}
	switch topo {
	case "peer":
		z = benchSession(b, Config{Mode: ModePeer, Listen: []string{"tcp/127.0.0.1:0"}, SharedMemory: opts})
		a = benchSession(b, Config{Mode: ModePeer, Connect: z.Locators(), SharedMemory: opts})
		for deadline := time.Now().Add(3 * time.Second); !connected(a, z); time.Sleep(5 * time.Millisecond) {
			if time.Now().After(deadline) {
				b.Fatal("peers did not connect")
			}
		}
	case "router":
		r := benchSession(b, Config{Mode: ModeRouter, Listen: []string{"tcp/127.0.0.1:0"}, SharedMemory: opts})
		a = benchSession(b, Config{Mode: ModeClient, Connect: r.Locators(), SharedMemory: opts})
		z = benchSession(b, Config{Mode: ModeClient, Connect: r.Locators(), SharedMemory: opts})
	}
	return a, z
}

// waitMatching waits until pub has a subscriber.
func waitMatching(b *testing.B, pub *Publisher) {
	b.Helper()
	for deadline := time.Now().Add(3 * time.Second); ; time.Sleep(2 * time.Millisecond) {
		if m, err := pub.MatchingStatus(); err == nil && m.Matching {
			return
		}
		if time.Now().After(deadline) {
			b.Fatal("no matching subscriber")
		}
	}
}

func BenchmarkPubSub(b *testing.B) {
	for _, topo := range []string{"peer", "router"} {
		for _, size := range benchSizes {
			b.Run(fmt.Sprintf("%s/%s", topo, sizeName(size)), func(b *testing.B) {
				benchPubSub(b, topo, size, false)
			})
		}
	}
}

func BenchmarkPubSubSHM(b *testing.B) {
	if m, err := shm.New(shm.Options{PoolSize: 1 << 20}); err != nil {
		b.Skip(err)
	} else {
		_ = m.Close()
	}
	for _, size := range []int{64 << 10, 1 << 20, 16 << 20} {
		b.Run(sizeName(size), func(b *testing.B) { benchPubSub(b, "peer", size, true) })
	}
}

func benchPubSub(b *testing.B, topo string, size int, shared bool) {
	a, z := benchPair(b, topo, shared)
	var got atomic.Int64
	done := make(chan struct{}, 1)
	var want atomic.Int64
	sub, err := z.DeclareSubscriber("bench/thr", func(Sample) {
		if got.Add(1) == want.Load() {
			done <- struct{}{}
		}
	})
	if err != nil {
		b.Fatal(err)
	}
	defer sub.Undeclare()
	pub, err := a.DeclarePublisher("bench/thr", WithCongestionControl(CongestionBlock))
	if err != nil {
		b.Fatal(err)
	}
	defer pub.Undeclare()
	waitMatching(b, pub)

	payload := make([]byte, size)
	var buf *shm.Buf
	if shared {
		if buf, err = a.ShmProvider().Alloc(size); err != nil {
			b.Fatal(err)
		}
		defer buf.Close()
	}
	got.Store(0)
	want.Store(int64(b.N))
	b.SetBytes(int64(size))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if shared {
			err = pub.Put(nil, WithShmBuf(buf))
		} else {
			err = pub.Put(payload)
		}
		if err != nil {
			b.Fatal(err)
		}
	}
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		b.Fatalf("received %d of %d", got.Load(), b.N)
	}
	b.StopTimer()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "msg/s")
}

func BenchmarkPingPong(b *testing.B) {
	for _, topo := range []string{"peer", "router"} {
		for _, size := range []int{8, 1024} {
			b.Run(fmt.Sprintf("%s/%s", topo, sizeName(size)), func(b *testing.B) {
				a, z := benchPair(b, topo, false)
				pong := make(chan struct{}, 1)
				echo, err := z.DeclarePublisher("bench/pong", WithCongestionControl(CongestionBlock), WithExpress(true))
				if err != nil {
					b.Fatal(err)
				}
				defer echo.Undeclare()
				s1, err := z.DeclareSubscriber("bench/ping", func(smp Sample) { _ = echo.Put(smp.Payload) })
				if err != nil {
					b.Fatal(err)
				}
				defer s1.Undeclare()
				s2, err := a.DeclareSubscriber("bench/pong", func(Sample) { pong <- struct{}{} })
				if err != nil {
					b.Fatal(err)
				}
				defer s2.Undeclare()
				ping, err := a.DeclarePublisher("bench/ping", WithCongestionControl(CongestionBlock), WithExpress(true))
				if err != nil {
					b.Fatal(err)
				}
				defer ping.Undeclare()
				waitMatching(b, ping)
				waitMatching(b, echo)
				payload := make([]byte, size)
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					if err := ping.Put(payload); err != nil {
						b.Fatal(err)
					}
					select {
					case <-pong:
					case <-time.After(5 * time.Second):
						b.Fatal("no pong")
					}
				}
				b.StopTimer()
				b.ReportMetric(float64(b.Elapsed().Microseconds())/float64(b.N), "µs/rtt")
			})
		}
	}
}

func BenchmarkQuery(b *testing.B) {
	for _, topo := range []string{"peer", "router"} {
		b.Run(topo, func(b *testing.B) {
			a, z := benchPair(b, topo, false)
			q, err := z.DeclareQueryable("bench/q", func(q *Query) { _ = q.Reply("bench/q", []byte("answer")) }, WithComplete(true))
			if err != nil {
				b.Fatal(err)
			}
			defer q.Undeclare()
			querier, err := a.DeclareQuerier("bench/q")
			if err != nil {
				b.Fatal(err)
			}
			defer querier.Undeclare()
			for deadline := time.Now().Add(3 * time.Second); ; time.Sleep(2 * time.Millisecond) {
				if m, err := querier.MatchingStatus(); err == nil && m.Matching {
					break
				}
				if time.Now().After(deadline) {
					b.Fatal("no queryable")
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				replies, err := querier.Get("")
				if err != nil {
					b.Fatal(err)
				}
				n := 0
				for range replies {
					n++
				}
				if n != 1 {
					b.Fatalf("%d replies", n)
				}
			}
		})
	}
}

func sizeName(n int) string {
	switch {
	case n >= 1<<20:
		return fmt.Sprintf("%dMiB", n>>20)
	case n >= 1<<10:
		return fmt.Sprintf("%dKiB", n>>10)
	}
	return fmt.Sprintf("%dB", n)
}
