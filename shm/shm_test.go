//go:build linux && !tinygo

package shm

import (
	"bytes"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shabunin/tiny-zenoh-go/protocol"
)

func pair(t *testing.T, reliable bool) (*Manager, *Manager, *Link, *Link) {
	t.Helper()
	m1, err := New(Options{PoolSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	m2, err := New(Options{PoolSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { m1.Close(); m2.Close() })
	a, b := m1.NewLink(), m2.NewLink()
	b.AcceptInitSyn(a.InitSyn())
	a.InitAck(b.AcceptInitAck())
	if err := b.AcceptOpenSyn(a.OpenSyn(reliable)); err != nil {
		t.Fatal(err)
	}
	if err := a.OpenAck(b.AcceptOpenAck(reliable)); err != nil {
		t.Fatal(err)
	}
	if !a.Active() || !b.Active() {
		t.Fatal("SHM not negotiated")
	}
	return m1, m2, a, b
}

func TestWrapResolve(t *testing.T) {
	for _, reliable := range []bool{true, false} {
		m1, _, a, b := pair(t, reliable)
		data := bytes.Repeat([]byte("zenoh-shm"), 10000)
		w, err := a.Wrap(data, protocol.PriorityData)
		if err != nil {
			t.Fatal(err)
		}
		w.Sent()
		info := w.Parts[0].Shm
		got, err := b.Resolve(w.Parts, protocol.PriorityData)
		if err != nil || !bytes.Equal(got, data) {
			t.Fatalf("resolve: %v (len %d)", err, len(got))
		}
		// A second read of the same reference must fail once it is reclaimed.
		deadline := time.Now().Add(2 * time.Second)
		for {
			g.mu.Lock()
			n := len(m1.busy)
			g.mu.Unlock()
			if n == 0 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("chunk never reclaimed")
			}
			time.Sleep(20 * time.Millisecond)
		}
		if reliable {
			g.mu.Lock()
			if len(a.pending[protocol.PriorityData]) != 0 ||
				atomic.LoadUint32(m1.counter(a.txIDs[protocol.PriorityData])) != 0 {
				t.Error("handoff not acknowledged")
			}
			g.mu.Unlock()
		}
		if _, err := readShm(info); err == nil {
			t.Error("stale reference resolved")
		}
	}
}

func TestChallengeMismatch(t *testing.T) {
	m1, _ := New(Options{PoolSize: 1 << 16})
	m2, _ := New(Options{PoolSize: 1 << 16})
	defer m1.Close()
	defer m2.Close()
	a, b := m1.NewLink(), m2.NewLink()
	defer a.Close()
	defer b.Close()
	b.AcceptInitSyn(a.InitSyn())
	ack := b.AcceptInitAck()
	ack.Buf[0] ^= 1 // corrupt the echoed challenge
	a.InitAck(ack)
	if a.OpenSyn(true) != nil {
		t.Error("initiator went on after a bad challenge")
	}
}

func TestPoolExhaustion(t *testing.T) {
	_, _, a, _ := pair(t, true)
	var ws []*Wrapped
	for {
		w, err := a.Wrap(make([]byte, 100<<10), protocol.PriorityData)
		if err == ErrNoMemory {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		ws = append(ws, w)
	}
	if len(ws) != 10 {
		t.Errorf("fit %d 100KiB buffers in 1MiB", len(ws))
	}
}

// waitFreed waits until m has no chunk allocated any more.
func waitFreed(t *testing.T, m *Manager) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		g.mu.Lock()
		n := len(m.busy)
		g.mu.Unlock()
		if n == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d chunk(s) never reclaimed", n)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// A message the transport dropped gives its references back: the handoff
// counter must not stay up (it would keep later buffers held for good) and
// the chunk must be freed.
func TestDiscard(t *testing.T) {
	for _, reliable := range []bool{true, false} {
		m1, _, a, b := pair(t, reliable)
		prio := protocol.PriorityData
		data := bytes.Repeat([]byte("x"), 8<<10)
		var ws []*Wrapped
		for range 3 {
			w, err := a.Wrap(data, prio)
			if err != nil {
				t.Fatal(err)
			}
			ws = append(ws, w)
		}
		ws[0].Sent()
		ws[1].Discard() // dropped on congestion: never reaches b
		ws[1].Sent()    // no-op once discarded
		ws[2].Sent()
		for _, i := range []int{0, 2} {
			if _, err := b.Resolve(ws[i].Parts, prio); err != nil {
				t.Fatal(err)
			}
		}

		// Zero-copy: the application's buffer, given to a dropped message.
		buf, err := m1.Alloc(4 << 10)
		if err != nil {
			t.Fatal(err)
		}
		wb, err := a.WrapBuf(buf, prio)
		if err != nil {
			t.Fatal(err)
		}
		wb.Discard()
		if n := atomic.LoadUint32(buf.h.refcount()); n != 1 {
			t.Errorf("refcount after discarding a zero-copy handoff = %d, want 1", n)
		}
		buf.Close()

		waitFreed(t, m1)
		g.mu.Lock()
		if reliable && (len(a.pending[prio]) != 0 || atomic.LoadUint32(m1.counter(a.txIDs[prio])) != 0) {
			t.Errorf("handoff state left: %d pending, counter %d",
				len(a.pending[prio]), atomic.LoadUint32(m1.counter(a.txIDs[prio])))
		}
		if !reliable && len(a.timed) != 2 { // the two delivered ones keep theirs
			t.Errorf("%d timed holds, want 2", len(a.timed))
		}
		g.mu.Unlock()
	}
}

// Discarding after the link closed only drops the receiver's reference.
func TestDiscardAfterClose(t *testing.T) {
	m1, _, a, _ := pair(t, true)
	w, err := a.Wrap(make([]byte, 8<<10), protocol.PriorityData)
	if err != nil {
		t.Fatal(err)
	}
	a.Close()
	w.Discard()
	waitFreed(t, m1)
}
