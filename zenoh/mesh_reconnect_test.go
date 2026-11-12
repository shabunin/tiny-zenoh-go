package zenoh

import (
	"encoding/binary"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// cutProxy forwards TCP connections to target until cut: then it stops
// reading either way (so senders' sockets fill up, as on a link that is
// down) and refuses new connections; restored, it forwards again.
type cutProxy struct {
	ln     net.Listener
	target string
	cut    atomic.Bool
	mu     sync.Mutex
	conns  []net.Conn
}

func newCutProxy(t *testing.T, target string) *cutProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &cutProxy{ln: ln, target: target}
	t.Cleanup(func() {
		_ = ln.Close()
		p.mu.Lock()
		for _, c := range p.conns {
			_ = c.Close()
		}
		p.mu.Unlock()
	})
	go func() {
		for {
			in, err := ln.Accept()
			if err != nil {
				return
			}
			if p.cut.Load() {
				_ = in.Close()
				continue
			}
			out, err := net.Dial("tcp", target)
			if err != nil {
				_ = in.Close()
				continue
			}
			p.mu.Lock()
			p.conns = append(p.conns, in, out)
			p.mu.Unlock()
			go p.pipe(out, in)
			go p.pipe(in, out)
		}
	}()
	return p
}

func (p *cutProxy) pipe(dst, src net.Conn) {
	defer dst.Close()
	defer src.Close()
	buf := make([]byte, 32<<10)
	for {
		for p.cut.Load() {
			time.Sleep(10 * time.Millisecond)
		}
		n, err := src.Read(buf)
		if err != nil {
			return
		}
		if p.cut.Load() {
			continue // what was on its way when the link went down
		}
		if _, err := dst.Write(buf[:n]); err != nil {
			return
		}
	}
}

func (p *cutProxy) locator() string { return "tcp/" + p.ln.Addr().String() }

// TestMeshMassReconnect: a 3x3 grid of routers loses every link for longer
// than the lease and gets them back. Data published once the mesh is whole
// again must arrive promptly. (In the lab, 49 routers held such samples up
// to 8 s, recomputing their trees for every link state under the session
// lock; a grid this small does not show it, see BenchmarkComputeTrees.)
func TestMeshMassReconnect(t *testing.T) {
	const rows, cols = 3, 3
	lease := time.Second
	routers := make([]*Session, rows*cols)
	locs := make([]string, rows*cols)
	lns := make([]net.Listener, rows*cols)
	for i := range routers {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		locs[i] = ln.Addr().String()
		lns[i] = ln
	}
	for _, ln := range lns {
		_ = ln.Close()
	}
	var proxies []*cutProxy
	for i := range routers {
		var connect []string
		r, c := i/cols, i%cols
		for _, j := range []int{i + 1, i + cols} {
			if (j == i+1 && c+1 >= cols) || (j == i+cols && r+1 >= rows) {
				continue
			}
			p := newCutProxy(t, locs[j])
			proxies = append(proxies, p)
			connect = append(connect, p.locator())
		}
		s, err := Open(Config{Mode: ModeRouter, Listen: []string{"tcp/" + locs[i]}, Connect: connect,
			Lease: lease, Scouting: ScoutingConfig{Disable: true}})
		if err != nil {
			t.Fatal(err)
		}
		routers[i] = s
		t.Cleanup(func() { _ = s.Close() })
	}
	pub := clientAt(t, "tcp/"+locs[0])
	sub := clientAt(t, "tcp/"+locs[len(locs)-1])

	var mu sync.Mutex
	type arrival struct{ sent, got time.Time }
	var arrivals []arrival
	if _, err := sub.DeclareSubscriber("mass/flow", func(s Sample) {
		mu.Lock()
		arrivals = append(arrivals, arrival{time.Unix(0, int64(binary.BigEndian.Uint64(s.Payload))), time.Now()})
		mu.Unlock()
	}); err != nil {
		t.Fatal(err)
	}
	p, err := pub.DeclarePublisher("mass/flow")
	if err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		buf := make([]byte, 8)
		for {
			select {
			case <-stop:
				return
			case <-time.After(10 * time.Millisecond):
			}
			binary.BigEndian.PutUint64(buf, uint64(time.Now().UnixNano()))
			_ = p.Put(buf)
		}
	}()
	// The routers dial neighbours that may not listen yet, so the grid can
	// take a few connector backoffs to form on a loaded machine.
	for deadline := time.Now().Add(15 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		mu.Lock()
		n := len(arrivals)
		mu.Unlock()
		if n > 10 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timeout waiting for data across the grid")
		}
	}

	for _, px := range proxies {
		px.cut.Store(true)
	}
	time.Sleep(3 * lease)
	for _, px := range proxies {
		px.cut.Store(false)
	}
	restored := time.Now()
	time.Sleep(6 * time.Second)
	close(stop)
	<-done

	mu.Lock()
	defer mu.Unlock()
	var first time.Time
	var worst time.Duration
	var worstAt time.Time
	for _, a := range arrivals {
		if a.sent.Before(restored) {
			continue
		}
		if first.IsZero() || a.got.Before(first) {
			first = a.got
		}
		if d := a.got.Sub(a.sent); d > worst {
			worst, worstAt = d, a.sent
		}
	}
	if first.IsZero() {
		t.Fatal("nothing after the links came back")
	}
	t.Logf("data back %v after the restore; worst latency %v (sent %v after the restore)",
		first.Sub(restored).Round(time.Millisecond), worst.Round(time.Millisecond),
		worstAt.Sub(restored).Round(time.Millisecond))
	if worst > 2*time.Second {
		t.Errorf("a sample took %v", worst)
	}
}
