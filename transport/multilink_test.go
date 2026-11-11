package transport

import (
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shabunin/tiny-zenoh-go/protocol"
)

func TestMultiLinkRSA(t *testing.T) {
	k, err := NewMultiLinkKey()
	if err != nil {
		t.Fatal(err)
	}
	if k.n.BitLen() != 512 {
		t.Fatalf("modulus has %d bits", k.n.BitLen())
	}
	pub := rsaPub{k.n, k.e}
	for _, msg := range [][]byte{{}, {1, 2, 3, 4, 5, 6, 7, 8}, make([]byte, 53)} {
		ct, err := pub.encrypt(msg)
		if err != nil {
			t.Fatal(err)
		}
		got, err := k.decrypt(ct)
		if err != nil || string(got) != string(msg) {
			t.Fatalf("round trip of %d bytes: %v", len(msg), err)
		}
	}
	if _, err := pub.encrypt(make([]byte, 54)); err == nil {
		t.Error("message longer than k-11 accepted")
	}
}

// countingConn counts bytes written; reliable=false makes the transport treat
// it like a datagram link for link selection.
type countingConn struct {
	net.Conn
	reliable bool
	written  atomic.Int64
}

func (c *countingConn) Write(b []byte) (int, error) {
	c.written.Add(int64(len(b)))
	return c.Conn.Write(b)
}
func (c *countingConn) Reliable() bool { return c.reliable }

// mlNode is one side of a multilink test: its transport to the other side.
type mlNode struct {
	cfg  Config
	mu   sync.Mutex
	tr   *Unicast
	msgs chan string
}

func newMLNode(t *testing.T, maxLinks int) *mlNode {
	key, err := NewMultiLinkKey()
	if err != nil {
		t.Fatal(err)
	}
	n := &mlNode{msgs: make(chan string, 64)}
	n.cfg = Config{ZID: testZID(t), WhatAmI: protocol.Peer, QoS: true, MaxLinks: maxLinks, MultiLinkKey: key,
		Join: func(protocol.ZID) *Unicast { n.mu.Lock(); defer n.mu.Unlock(); return n.tr }}
	return n
}

func (n *mlNode) cb() Callbacks {
	return Callbacks{OnMessage: func(m protocol.Message, _ bool) error {
		n.msgs <- m.(*protocol.Push).WireExpr.Suffix
		return nil
	}}
}

func (n *mlNode) set(t *testing.T, tr *Unicast, err error) *Unicast {
	t.Helper()
	if err != nil {
		return nil
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.tr == nil {
		n.tr = tr
		t.Cleanup(func() { _ = tr.Close() })
	} else if n.tr != tr {
		t.Errorf("second link made a new transport instead of joining")
	}
	return tr
}

// connect opens one more link from a to b.
func connect(t *testing.T, a, b *mlNode, aConn func(net.Conn) Link) (error, error) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	errc := make(chan error, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			errc <- err
			return
		}
		tr, err := Accept(c, b.cfg, b.cb())
		b.set(t, tr, err)
		errc <- err
	}()
	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	var l Link = c
	if aConn != nil {
		l = aConn(c)
	}
	tr, oerr := Open(l, a.cfg, a.cb())
	a.set(t, tr, oerr)
	return oerr, <-errc
}

func expectMsgs(t *testing.T, n *mlNode, want ...string) {
	t.Helper()
	for _, w := range want {
		select {
		case got := <-n.msgs:
			if got != w {
				t.Errorf("got %s, want %s", got, w)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("missing %s", w)
		}
	}
}

func TestMultiLinkJoin(t *testing.T) {
	a, b := newMLNode(t, 2), newMLNode(t, 2)
	if oerr, aerr := connect(t, a, b, nil); oerr != nil || aerr != nil {
		t.Fatalf("first link: %v / %v", oerr, aerr)
	}
	if !a.tr.MultiLink() || !b.tr.MultiLink() {
		t.Fatal("multilink not negotiated")
	}
	var lossy *countingConn
	if oerr, aerr := connect(t, a, b, func(c net.Conn) Link { lossy = &countingConn{Conn: c}; return lossy }); oerr != nil || aerr != nil {
		t.Fatalf("second link: %v / %v", oerr, aerr)
	}
	if a.tr.Links() != 2 || b.tr.Links() != 2 {
		t.Fatalf("links a=%d b=%d", a.tr.Links(), b.tr.Links())
	}

	// Reliable traffic stays on the reliable (first) link; best-effort goes
	// on the one that says it is unreliable.
	before := lossy.written.Load()
	if err := a.tr.Send(true, pushPrio("r1", protocol.PriorityData)); err != nil {
		t.Fatal(err)
	}
	expectMsgs(t, b, "r1")
	if lossy.written.Load() != before {
		t.Error("reliable message went over the unreliable link")
	}
	if err := a.tr.Send(false, pushPrio("b1", protocol.PriorityData), pushPrio("b2", protocol.PriorityRealTime)); err != nil {
		t.Fatal(err)
	}
	expectMsgs(t, b, "b1", "b2")
	if lossy.written.Load() == before {
		t.Error("best-effort messages did not use the unreliable link")
	}
	// Both directions share one SN space per channel across links.
	if err := b.tr.Send(true, pushPrio("back", protocol.PriorityControl)); err != nil {
		t.Fatal(err)
	}
	expectMsgs(t, a, "back")

	// A third link exceeds MaxLinks.
	oerr, aerr := connect(t, a, b, nil)
	if !errors.Is(aerr, ErrDuplicate) || oerr == nil {
		t.Errorf("third link: open=%v accept=%v", oerr, aerr)
	}

	// Losing the second link leaves the transport up on the first.
	_ = lossy.Conn.Close()
	deadline := time.Now().Add(2 * time.Second)
	for a.tr.Links() != 1 || b.tr.Links() != 1 {
		if time.Now().After(deadline) {
			t.Fatalf("links after loss a=%d b=%d", a.tr.Links(), b.tr.Links())
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := a.tr.Send(false, pushPrio("after", protocol.PriorityData)); err != nil {
		t.Fatal(err)
	}
	expectMsgs(t, b, "after")
	select {
	case <-a.tr.Done():
		t.Fatal("transport closed with a link left")
	default:
	}
}

func TestMultiLinkImposter(t *testing.T) {
	a, b := newMLNode(t, 2), newMLNode(t, 2)
	if oerr, aerr := connect(t, a, b, nil); oerr != nil || aerr != nil {
		t.Fatalf("first link: %v / %v", oerr, aerr)
	}
	// Same ZID, different key: may not join.
	other, _ := NewMultiLinkKey()
	imp := newMLNode(t, 2)
	imp.cfg.ZID, imp.cfg.MultiLinkKey = a.cfg.ZID, other
	_, aerr := connect(t, imp, b, nil)
	if !errors.Is(aerr, errInvalid) {
		t.Errorf("imposter link: %v", aerr)
	}
	if b.tr.Links() != 1 {
		t.Errorf("b has %d links", b.tr.Links())
	}
}

func TestMultiLinkOneSideOnly(t *testing.T) {
	a, b := newMLNode(t, 2), newMLNode(t, 1)
	b.cfg.MultiLinkKey = nil
	if oerr, aerr := connect(t, a, b, nil); oerr != nil || aerr != nil {
		t.Fatalf("first link: %v / %v", oerr, aerr)
	}
	if a.tr.MultiLink() || b.tr.MultiLink() {
		t.Error("multilink negotiated with a node without it")
	}
	// Without multilink the dialer itself refuses the duplicate, right after
	// INIT ACK (before OPEN SYN), and the acceptor sees its CLOSE.
	if oerr, aerr := connect(t, a, b, nil); !errors.Is(oerr, ErrDuplicate) || aerr == nil {
		t.Errorf("second link: %v / %v", oerr, aerr)
	}
}
