package transport

import (
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/shabunin/tiny-zenoh-go/protocol"
)

// gatedConn holds writes while its gate is shut: a link that the peer
// stops reading, as far as the sender can tell.
type gatedConn struct {
	net.Conn
	mu      sync.Mutex
	cond    sync.Cond
	shut    bool
	waiting int
	writes  int
}

func newGatedConn(c net.Conn) *gatedConn {
	g := &gatedConn{Conn: c}
	g.cond.L = &g.mu
	return g
}

func (g *gatedConn) Write(b []byte) (int, error) {
	g.mu.Lock()
	for g.shut {
		g.waiting++
		g.cond.Broadcast()
		g.cond.Wait()
		g.waiting--
	}
	g.writes++
	g.mu.Unlock()
	return g.Conn.Write(b)
}

func (g *gatedConn) setShut(shut bool) {
	g.mu.Lock()
	g.shut = shut
	g.cond.Broadcast()
	g.mu.Unlock()
}

// awaitBlocked waits until a write is held at the gate.
func (g *gatedConn) awaitBlocked(t *testing.T) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		g.mu.Lock()
		for g.waiting == 0 {
			g.cond.Wait()
		}
		g.mu.Unlock()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("no write reached the gate")
	}
}

type congestionPair struct {
	o, a *Unicast
	gate *gatedConn
	got  chan string
	oerr chan error
}

// newCongestionPair connects a sender whose writes can be held to a
// receiver that reports the keys of the PUSHes it gets.
func newCongestionPair(t *testing.T, q TxQueue, batch uint16) *congestionPair {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	p := &congestionPair{got: make(chan string, 64), oerr: make(chan error, 1)}
	ach := make(chan *Unicast, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			ach <- nil
			return
		}
		a, _ := Accept(c, Config{ZID: testZID(t), WhatAmI: protocol.Peer, QoS: true},
			Callbacks{OnMessage: func(m protocol.Message, _ bool) error {
				p.got <- m.(*protocol.Push).WireExpr.Suffix
				return nil
			}})
		ach <- a
	}()
	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	p.gate = newGatedConn(c)
	q.Mode = TxQueued
	p.o, err = Open(p.gate, Config{ZID: testZID(t), WhatAmI: protocol.Peer, QoS: true, BatchSize: batch,
		Lease: 2 * time.Second, TxQueue: q}, Callbacks{OnClose: func(err error) { p.oerr <- err }})
	if err != nil {
		t.Fatal(err)
	}
	if p.a = <-ach; p.a == nil {
		t.Fatal("accept failed")
	}
	t.Cleanup(func() {
		p.gate.setShut(false)
		_ = p.o.Close()
		_ = p.a.Close()
	})
	return p
}

func (p *congestionPair) expect(t *testing.T, want ...string) {
	t.Helper()
	for _, w := range want {
		select {
		case k := <-p.got:
			if k != w {
				t.Errorf("got %s, want %s", k, w)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("missing %s", w)
		}
	}
}

func (p *congestionPair) expectNothing(t *testing.T) {
	t.Helper()
	select {
	case k := <-p.got:
		t.Errorf("unexpected %s", k)
	case <-time.After(100 * time.Millisecond):
	}
}

func ccPush(key string, p protocol.Priority, block bool) *protocol.Push {
	return &protocol.Push{WireExpr: protocol.WireExpr{Suffix: key}, QoS: protocol.NewQoS(p, block, false),
		Body: &protocol.Put{Payload: []byte("x")}}
}

func dropped(t *testing.T, err error) int {
	t.Helper()
	var ce *CongestionError
	if !errors.As(err, &ce) || !errors.Is(err, ErrCongested) {
		t.Fatalf("Send = %v, want a congestion error", err)
	}
	return len(ce.Dropped)
}

func oneBatch() TxQueue {
	var q TxQueue
	for i := range q.Size {
		q.Size[i] = 1
	}
	return q
}

// A full queue drops droppable messages (at once once congested) but keeps
// blocking ones waiting; the queue drains when the link moves again.
func TestCongestionDrop(t *testing.T) {
	q := oneBatch()
	q.WaitBeforeDrop = 5 * time.Millisecond
	p := newCongestionPair(t, q, 0)
	p.gate.setShut(true)
	if err := p.o.Send(true, ccPush("m1", protocol.PriorityData, false)); err != nil {
		t.Fatal(err)
	}
	p.gate.awaitBlocked(t) // m1's batch is being written: the queue is full
	t0 := time.Now()
	if n := dropped(t, p.o.Send(true, ccPush("m2", protocol.PriorityData, false))); n != 1 {
		t.Errorf("dropped %d", n)
	}
	if d := time.Since(t0); d < 5*time.Millisecond {
		t.Errorf("dropped after %v, before wait_before_drop", d)
	}
	// Best effort is always droppable; congested: dropped straight away.
	if n := dropped(t, p.o.Send(false, ccPush("m3", protocol.PriorityData, true))); n != 1 {
		t.Errorf("dropped %d", n)
	}
	// Another priority has its own queue.
	if err := p.o.Send(true, ccPush("rt", protocol.PriorityRealTime, false)); err != nil {
		t.Fatal(err)
	}
	blocked := make(chan error, 1)
	go func() { blocked <- p.o.Send(true, ccPush("m4", protocol.PriorityData, true)) }()
	select {
	case err := <-blocked:
		t.Fatalf("blocking message returned %v while the link is stuck", err)
	case <-time.After(50 * time.Millisecond):
	}
	p.gate.setShut(false)
	if err := <-blocked; err != nil {
		t.Fatal(err)
	}
	p.expect(t, "m1", "rt", "m4")
	if err := p.o.Send(true, ccPush("m5", protocol.PriorityData, false)); err != nil {
		t.Fatal(err)
	}
	p.expect(t, "m5")
	p.expectNothing(t)
}

// A blocking message that finds no room for WaitBeforeClose closes the
// transport.
func TestCongestionBlockClose(t *testing.T) {
	q := oneBatch()
	q.WaitBeforeClose = 100 * time.Millisecond
	p := newCongestionPair(t, q, 0)
	p.gate.setShut(true)
	if err := p.o.Send(true, ccPush("m1", protocol.PriorityData, true)); err != nil {
		t.Fatal(err)
	}
	p.gate.awaitBlocked(t)
	t0 := time.Now()
	if err := p.o.Send(true, ccPush("m2", protocol.PriorityData, true)); !errors.Is(err, ErrClosed) {
		t.Fatalf("Send = %v, want ErrClosed", err)
	}
	if d := time.Since(t0); d < 100*time.Millisecond {
		t.Errorf("gave up after %v", d)
	}
	select {
	case err := <-p.oerr:
		if !errors.Is(err, ErrBlocked) {
			t.Errorf("closed with %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("transport not closed")
	}
}

// BlockFirst: the first message waits in the background, the next one is
// dropped while it does.
func TestCongestionBlockFirst(t *testing.T) {
	p := newCongestionPair(t, oneBatch(), 0)
	p.gate.setShut(true)
	if err := p.o.Send(true, ccPush("m1", protocol.PriorityData, false)); err != nil {
		t.Fatal(err)
	}
	p.gate.awaitBlocked(t)
	bf := func(key string) *protocol.Push {
		m := ccPush(key, protocol.PriorityData, false)
		m.QoS = m.QoS.WithBlockFirst()
		return m
	}
	if q := bf("x").QoS; !q.BlockFirst() || q.Block() || q.Priority() != protocol.PriorityData {
		t.Fatalf("QoS %#x", q)
	}
	t0 := time.Now()
	if err := p.o.Send(true, bf("bf1")); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(t0); d > 50*time.Millisecond {
		t.Errorf("BlockFirst Send took %v", d)
	}
	if n := dropped(t, p.o.Send(true, bf("bf2"))); n != 1 {
		t.Errorf("dropped %d", n)
	}
	p.gate.setShut(false)
	p.expect(t, "m1", "bf1")
	p.expectNothing(t)
}

// Held up, the queues go out highest priority first.
func TestCongestionPriorityOrder(t *testing.T) {
	p := newCongestionPair(t, TxQueue{}, 0)
	p.gate.setShut(true)
	if err := p.o.Send(true, ccPush("bg1", protocol.PriorityBackground, true)); err != nil {
		t.Fatal(err)
	}
	p.gate.awaitBlocked(t)
	for _, m := range []*protocol.Push{ccPush("bg2", protocol.PriorityBackground, true),
		ccPush("d", protocol.PriorityData, true), ccPush("c", protocol.PriorityControl, true)} {
		if err := p.o.Send(true, m); err != nil {
			t.Fatal(err)
		}
	}
	p.gate.setShut(false)
	p.expect(t, "bg1", "c", "d", "bg2")
}

// A droppable message given up after its first fragment is announced by a
// drop FRAGMENT: the receiver discards it and goes on.
func TestCongestionFragmentsAbandoned(t *testing.T) {
	var q TxQueue
	for i := range q.Size {
		q.Size[i] = 2
	}
	p := newCongestionPair(t, q, 512)
	p.gate.setShut(true)
	if err := p.o.Send(true, ccPush("m0", protocol.PriorityData, false)); err != nil {
		t.Fatal(err)
	}
	p.gate.awaitBlocked(t) // one batch taken; room for one fragment only
	big := ccPush("big", protocol.PriorityData, false)
	big.Body = &protocol.Put{Payload: make([]byte, 3000)}
	if n := dropped(t, p.o.Send(true, big)); n != 1 {
		t.Errorf("dropped %d", n)
	}
	p.gate.setShut(false)
	p.expect(t, "m0")
	if err := p.o.Send(true, ccPush("after", protocol.PriorityData, false)); err != nil {
		t.Fatal(err)
	}
	p.expect(t, "after")
	p.expectNothing(t)
}

// Messages of one Send share a batch, and later ones join a queued frame.
func TestTxQueueBatching(t *testing.T) {
	p := newCongestionPair(t, TxQueue{}, 0)
	p.gate.setShut(true)
	if err := p.o.Send(true, ccPush("a", protocol.PriorityData, true)); err != nil {
		t.Fatal(err)
	}
	p.gate.awaitBlocked(t)
	for _, k := range []string{"b", "c", "d"} {
		if err := p.o.Send(true, ccPush(k, protocol.PriorityData, true)); err != nil {
			t.Fatal(err)
		}
	}
	g := p.gate
	g.mu.Lock()
	before := g.writes
	g.mu.Unlock()
	p.gate.setShut(false)
	p.expect(t, "a", "b", "c", "d")
	g.mu.Lock()
	n := g.writes - before
	g.mu.Unlock()
	if n != 2 { // a's batch, then b, c and d in one
		t.Errorf("%d writes, want 2", n)
	}
}
