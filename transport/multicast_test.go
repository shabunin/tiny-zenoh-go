package transport

import (
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/shabunin/gozenoh/protocol"
)

// hub is an in-memory multicast group.
type hub struct {
	mu      sync.Mutex
	members map[string]*hubLink
	drop    func(src string, b []byte) bool
}

type hubLink struct {
	h    *hub
	name string
	rx   chan [2]any
	done chan struct{}
	once sync.Once
}

func (h *hub) join(name string) *hubLink {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.members == nil {
		h.members = map[string]*hubLink{}
	}
	l := &hubLink{h: h, name: name, rx: make(chan [2]any, 1024), done: make(chan struct{})}
	h.members[name] = l
	return l
}

func (l *hubLink) Write(b []byte) (int, error) {
	l.h.mu.Lock()
	defer l.h.mu.Unlock()
	if l.h.drop != nil && l.h.drop(l.name, b) {
		return len(b), nil
	}
	for n, m := range l.h.members {
		if n != l.name {
			select {
			case m.rx <- [2]any{append([]byte(nil), b...), l.name}:
			default:
			}
		}
	}
	return len(b), nil
}

func (l *hubLink) ReadFrom(b []byte) (int, string, error) {
	select {
	case d := <-l.rx:
		return copy(b, d[0].([]byte)), d[1].(string), nil
	case <-l.done:
		return 0, "", io.EOF
	}
}

func (l *hubLink) Close() error {
	l.once.Do(func() {
		close(l.done)
		l.h.mu.Lock()
		delete(l.h.members, l.name)
		l.h.mu.Unlock()
	})
	return nil
}

func (l *hubLink) MTU() int { return 0xffff }

type mcNode struct {
	m     *Multicast
	msgs  chan string
	peers chan string
	gone  chan error
}

func newMCNode(t *testing.T, h *hub, name string, qos bool) *mcNode {
	n := &mcNode{msgs: make(chan string, 64), peers: make(chan string, 8), gone: make(chan error, 8)}
	m, err := NewMulticast(h.join(name), MulticastConfig{ZID: testZID(t), WhatAmI: protocol.Peer, QoS: qos,
		JoinInterval: 50 * time.Millisecond, Lease: 300 * time.Millisecond, BatchSize: 1024},
		MulticastCallbacks{
			OnPeer: func(p *MulticastPeer) { n.peers <- p.Addr },
			OnMessage: func(p *MulticastPeer, m protocol.Message, _ bool) {
				n.msgs <- p.Addr + ":" + m.(*protocol.Push).WireExpr.Suffix
			},
			OnPeerGone: func(p *MulticastPeer, err error) { n.gone <- err },
		})
	if err != nil {
		t.Fatal(err)
	}
	n.m = m
	t.Cleanup(func() { _ = m.Close() })
	return n
}

func recv[T any](t *testing.T, c chan T) T {
	t.Helper()
	select {
	case v := <-c:
		return v
	case <-time.After(2 * time.Second):
		t.Fatal("timeout")
	}
	var z T
	return z
}

func TestMulticastGroup(t *testing.T) {
	for _, qos := range []bool{false, true} {
		h := &hub{}
		a := newMCNode(t, h, "a", qos)
		b := newMCNode(t, h, "b", qos)
		c := newMCNode(t, h, "c", qos)
		for _, n := range []*mcNode{a, b, c} {
			recv(t, n.peers)
			recv(t, n.peers)
		}
		big := pushPrio("big", protocol.PriorityInteractiveHigh)
		big.Body = &protocol.Put{Payload: make([]byte, 5000)} // fragmented at batch 1024
		if err := a.m.Send(true, pushPrio("x", protocol.PriorityData), big, pushPrio("y", protocol.PriorityRealTime)); err != nil {
			t.Fatal(err)
		}
		for _, n := range []*mcNode{b, c} {
			for _, w := range []string{"a:x", "a:big", "a:y"} {
				if got := recv(t, n.msgs); got != w {
					t.Errorf("qos=%v: got %s, want %s", qos, got, w)
				}
			}
		}
		if len(b.m.Peers()) != 2 {
			t.Errorf("b knows %d peers", len(b.m.Peers()))
		}
		// a leaves: the others drop it at once (CLOSE).
		_ = a.m.Close()
		if err := recv(t, b.gone); !errors.Is(err, ErrPeerClosed) {
			t.Errorf("gone: %v", err)
		}
		_ = b.m.Close()
		_ = c.m.Close()
	}
}

func TestMulticastLateJoinerAndLease(t *testing.T) {
	h := &hub{}
	a := newMCNode(t, h, "a", false)
	// a sends before b exists: SNs advance.
	for range 5 {
		_ = a.m.Send(false, pushPrio("early", protocol.PriorityData))
	}
	b := newMCNode(t, h, "b", false)
	recv(t, b.peers) // a's next JOIN tells b where a's SNs are
	if err := a.m.Send(false, pushPrio("late", protocol.PriorityData)); err != nil {
		t.Fatal(err)
	}
	if got := recv(t, b.msgs); got != "a:late" {
		t.Errorf("got %s", got)
	}
	// a goes silent (but does not say CLOSE): b expires it after its lease.
	h.mu.Lock()
	h.drop = func(src string, _ []byte) bool { return src == "a" }
	h.mu.Unlock()
	if err := recv(t, b.gone); !errors.Is(err, ErrLeaseExpired) {
		t.Errorf("gone: %v", err)
	}
}

func TestMulticastIgnoresMismatch(t *testing.T) {
	h := &hub{}
	a := newMCNode(t, h, "a", true) // QoS
	b := newMCNode(t, h, "b", false)
	recv(t, a.peers) // a (QoS) accepts b
	select {
	case p := <-b.peers:
		t.Errorf("b without QoS accepted QoS node %s", p)
	case <-time.After(200 * time.Millisecond):
	}
}
