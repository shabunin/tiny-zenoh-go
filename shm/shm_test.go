//go:build linux && !tinygo

package shm

import (
	"bytes"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shabunin/gozenoh/protocol"
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
