//go:build linux && !tinygo

package zenoh

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"
	"time"

	"github.com/shabunin/tiny-zenoh-go/shm"
)

// pattern matches zpeer.py's pattern().
func pattern(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte((i*7 + 3) % 251)
	}
	return b
}

func sum(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }

func openShmPeer(t *testing.T, connect ...string) *Session {
	t.Helper()
	s, err := Open(Config{
		Mode:         ModePeer,
		Listen:       []string{"tcp/127.0.0.1:0"},
		Connect:      connect,
		Scouting:     ScoutingConfig{Disable: true},
		SharedMemory: &shm.Options{PoolSize: 4 << 20},
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func shmFaces(s *Session) int {
	n := 0
	for _, f := range s.faceList() {
		<-f.ready
		if f.shm != nil {
			n++
		}
	}
	return n
}

func TestShmGoPeers(t *testing.T) {
	pfx := uniq(t)
	a := openShmPeer(t)
	b := openShmPeer(t, a.Locators()[0])
	waitFor(t, "peers", func() bool { return connected(a, b) && connected(b, a) })
	if shmFaces(a) != 1 || shmFaces(b) != 1 {
		t.Fatal("SHM not negotiated between Go peers")
	}
	sink := newSink()
	sub, _ := b.DeclareSubscriber(pfx+"/**", sink.handler)
	defer sub.Undeclare()
	q, _ := a.DeclareQueryable(pfx+"/q", func(q *Query) {
		_ = q.Reply(q.KeyExpr(), append([]byte("re:"), q.Payload()...))
	})
	defer q.Undeclare()
	settle()

	w0, r0 := shmStats.wrapped.Load(), shmStats.resolved.Load()
	big := pattern(200 << 10)
	_ = a.Put(pfx+"/big", big)
	_ = a.Put(pfx+"/small", []byte("tiny"))
	got := sink.wait(t, 2)
	if !bytes.Equal(got[0].Payload, big) || string(got[1].Payload) != "tiny" {
		t.Errorf("payloads differ: %d bytes, %q", len(got[0].Payload), got[1].Payload)
	}
	replies, _ := b.Get(pfx+"/q", WithPayload(big, Encoding{}))
	n := 0
	for r := range replies {
		s, err := r.Result()
		if err != nil || !bytes.Equal(s.Payload, append([]byte("re:"), big...)) {
			t.Errorf("reply: %v (%d bytes)", err, len(s.Payload))
		}
		n++
	}
	if n != 1 {
		t.Errorf("%d replies", n)
	}
	// put + query body + reply each went through shared memory.
	if w, r := shmStats.wrapped.Load()-w0, shmStats.resolved.Load()-r0; w != 3 || r != 3 {
		t.Errorf("wrapped %d, resolved %d; want 3, 3", w, r)
	}
}

func TestShmOnlyIfBothEnable(t *testing.T) {
	a := openShmPeer(t)
	b := openPeer(t, a.Locators()[0])
	waitFor(t, "peers", func() bool { return connected(a, b) && connected(b, a) })
	if shmFaces(a) != 0 || shmFaces(b) != 0 {
		t.Error("SHM negotiated with a peer that did not enable it")
	}
	pfx := uniq(t)
	sink := newSink()
	sub, _ := b.DeclareSubscriber(pfx, sink.handler)
	defer sub.Undeclare()
	settle()
	big := pattern(100 << 10)
	_ = a.Put(pfx, big)
	if got := sink.wait(t, 1); !bytes.Equal(got[0].Payload, big) {
		t.Error("payload differs")
	}
}

// Rust peers (zenoh 1.10 enables SHM by default) against a Go peer with SHM.
func TestShmRustPeer(t *testing.T) {
	pfx := uniq(t)
	s := openShmPeer(t)
	conf := func() string {
		c := map[string]any{
			"mode": "peer", "scouting/multicast/enabled": false, "listen/endpoints": []string{},
			"connect/endpoints": s.Locators(),
			// zenoh-rust mlocks its pool and gives up on SHM sending if that
			// fails; keep it under a typical 8 MiB RLIMIT_MEMLOCK.
			"transport/shared_memory/transport_optimization/pool_size": 2 << 20,
		}
		b, _ := json.Marshal(c)
		return string(b)
	}
	big := pattern(100 << 10)

	t.Run("RustReceives", func(t *testing.T) {
		rs := startRust(t, conf(), true, "bigsub", pfx+"/a/**", "3")
		waitFor(t, "rust subscriber", func() bool {
			for _, f := range s.faceList() {
				s.mu.Lock()
				n := len(f.subs)
				s.mu.Unlock()
				if n > 0 && f.shm != nil {
					return true
				}
			}
			return false
		})
		for i := range 3 {
			_ = s.Put(pfx+"/a/x", big[:len(big)-i])
		}
		for i := range 3 {
			e := rs.next()
			if e.Size != len(big)-i || e.Sum != sum(big[:len(big)-i]) || !e.Shm {
				t.Errorf("rust got size %d shm %v (sum ok %v)", e.Size, e.Shm, e.Sum == sum(big[:len(big)-i]))
			}
		}
	})

	t.Run("GoReceives", func(t *testing.T) {
		sink := newSink()
		sub, _ := s.DeclareSubscriber(pfx+"/b", sink.handler)
		defer sub.Undeclare()
		r0 := shmStats.resolved.Load()
		rs := startRust(t, conf(), false, "bigpub", pfx+"/b", "102400", "20")
		for _, smp := range sink.wait(t, 20) {
			if !bytes.Equal(smp.Payload, big) {
				t.Errorf("payload differs (%d bytes)", len(smp.Payload))
			}
		}
		// zenoh-rust creates its SHM pool lazily: the first puts may be inline.
		if n := shmStats.resolved.Load() - r0; n == 0 {
			t.Error("no payload came through shared memory")
		} else {
			t.Logf("%d of 20 payloads came through shared memory", n)
		}
		rs.next() // done
	})

	t.Run("GoGetsRustQueryable", func(t *testing.T) {
		rs := startRust(t, conf(), true, "bigqueryable", pfx+"/q", "102400", "5")
		waitFor(t, "rust queryable", func() bool {
			for _, f := range s.faceList() {
				s.mu.Lock()
				n := len(f.qbls)
				s.mu.Unlock()
				if n > 0 {
					return true
				}
			}
			return false
		})
		replies, _ := s.Get(pfx+"/q", WithPayload(big, Encoding{}), WithTimeout(5*time.Second))
		n := 0
		for r := range replies {
			smp, err := r.Result()
			if err != nil || !bytes.Equal(smp.Payload, big) {
				t.Errorf("reply %v, %d bytes", err, len(smp.Payload))
			}
			n++
		}
		if n != 1 {
			t.Errorf("%d replies", n)
		}
		if e := rs.next(); e.Size != len(big) || e.Sum != sum(big) || !e.Shm {
			t.Errorf("rust query body: size %d shm %v", e.Size, e.Shm)
		}
	})

	t.Run("RustGetsGoQueryable", func(t *testing.T) {
		var body []byte
		q, _ := s.DeclareQueryable(pfx+"/gq", func(q *Query) {
			body = q.Payload()
			_ = q.Reply(q.KeyExpr(), big)
		})
		defer q.Undeclare()
		rs := startRust(t, conf(), false, "bigget", pfx+"/gq", "102400")
		e := rs.next()
		if e.Size != len(big) || e.Sum != sum(big) || !e.Shm {
			t.Errorf("rust reply: size %d shm %v", e.Size, e.Shm)
		}
		if !rs.next().Done {
			t.Error("no done")
		}
		if !bytes.Equal(body, big) {
			t.Errorf("query body: %d bytes", len(body))
		}
	})
}
