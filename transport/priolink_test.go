package transport

import (
	"io"
	"net"
	"testing"

	"github.com/shabunin/gozenoh/protocol"
)

// memPrioLink is an in-memory PriorityLink: a pipe for Control and one pipe
// per other priority each way.
type memPrioLink struct {
	net.Conn
	w [numPrio]io.Writer
	r [numPrio]io.Reader
}

func (m *memPrioLink) PriorityStreams() bool                        { return true }
func (m *memPrioLink) PriorityWriter(p protocol.Priority) io.Writer { return m.w[p] }
func (m *memPrioLink) PriorityReader(p protocol.Priority) io.Reader { return m.r[p] }

// stuckReader never returns: a stream whose receiver stopped reading.
type stuckReader struct{ ch chan struct{} }

func (s stuckReader) Read([]byte) (int, error) { <-s.ch; return 0, io.EOF }

func memPrioPair(stalled protocol.Priority, unblock chan struct{}) (*memPrioLink, *memPrioLink) {
	c1, c2 := net.Pipe()
	a, b := &memPrioLink{Conn: c1}, &memPrioLink{Conn: c2}
	for p := 1; p < numPrio; p++ {
		x1, x2 := net.Pipe()
		y1, y2 := net.Pipe()
		a.w[p], b.r[p] = x1, x2
		b.w[p], a.r[p] = y1, y2
	}
	b.r[stalled] = stuckReader{unblock}
	return a, b
}

// A priority whose stream stalls holds up only itself.
func TestPriorityStreamsIndependent(t *testing.T) {
	unblock := make(chan struct{})
	defer close(unblock)
	la, lb := memPrioPair(protocol.PriorityBackground, unblock)
	got := newCollector()
	type res struct {
		u   *Unicast
		err error
	}
	ch := make(chan res, 1)
	go func() {
		u, err := Accept(lb, Config{ZID: testZID(t), WhatAmI: protocol.Peer, QoS: true}, got.cb())
		ch <- res{u, err}
	}()
	a, err := Open(la, Config{ZID: testZID(t), WhatAmI: protocol.Peer, QoS: true}, Callbacks{})
	if err != nil {
		t.Fatal(err)
	}
	r := <-ch
	if r.err != nil {
		t.Fatal(r.err)
	}
	defer a.Close()
	defer r.u.Close()
	if !a.PriorityStreams() || !r.u.PriorityStreams() {
		t.Fatal("priority streams not in use")
	}
	// The first Background batch blocks in its stream; a few more queue up.
	for range 4 {
		if err := a.Send(true, pushPrio("bg", protocol.PriorityBackground)); err != nil {
			t.Fatal(err)
		}
	}
	for _, p := range []protocol.Priority{protocol.PriorityRealTime, protocol.PriorityData} {
		if err := a.Send(true, pushPrio("p", p)); err != nil {
			t.Fatal(err)
		}
	}
	if msgs := got.wait(t, 2); len(msgs) != 2 || msgs[0] != "p=x" || msgs[1] != "p=x" {
		t.Fatalf("got %v", msgs)
	}
}
