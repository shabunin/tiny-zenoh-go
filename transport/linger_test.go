package transport

import (
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shabunin/tiny-zenoh-go/protocol"
)

// TestCloseLinger: what a sender wrote before closing reaches a slow
// receiver, though most of it still sat in the socket buffers when Close was
// called. Closing outright loses it: the receiver's next keep-alive meets a
// closed socket, which resets the connection. Seen in the lab, where a
// publisher closing right after a burst lost half of it.
func TestCloseLinger(t *testing.T) {
	for _, tc := range []struct {
		name   string
		linger time.Duration
		all    bool
	}{
		{"linger", 0, true},     // default: the lease
		{"outright", -1, false}, // what Close did before
	} {
		t.Run(tc.name, func(t *testing.T) {
			const n = 1500
			got := lingerBurst(t, n, tc.linger)
			if tc.all && got != n {
				t.Fatalf("received %d of %d", got, n)
			}
			if !tc.all && got == n {
				t.Logf("received all %d without lingering (no reset this time)", n)
			}
			t.Logf("received %d of %d", got, n)
		})
	}
}

// lingerBurst sends n 2 KB messages to a receiver that takes a millisecond
// over each, closes at once, and returns how many the receiver got.
func lingerBurst(t *testing.T, n int, linger time.Duration) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	var got atomic.Int32
	closed := make(chan struct{})
	ach := make(chan *Unicast, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			ach <- nil
			return
		}
		// A short lease: keep-alives every 100 ms.
		a, _ := Accept(c, Config{ZID: testZID(t), WhatAmI: protocol.Peer, QoS: true, Lease: 400 * time.Millisecond},
			Callbacks{
				OnMessage: func(protocol.Message, bool) error {
					time.Sleep(time.Millisecond)
					got.Add(1)
					return nil
				},
				OnClose: func(error) { close(closed) },
			})
		ach <- a
	}()
	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	o, err := Open(c, Config{ZID: testZID(t), WhatAmI: protocol.Peer, QoS: true, CloseLinger: linger,
		TxQueue: TxQueue{Mode: TxQueued}}, Callbacks{})
	if err != nil {
		t.Fatal(err)
	}
	a := <-ach
	if a == nil {
		t.Fatal("accept failed")
	}
	defer a.Close()
	payload := make([]byte, 2048)
	for i := range n {
		m := &protocol.Push{WireExpr: protocol.WireExpr{Suffix: "k"}, QoS: protocol.NewQoS(protocol.PriorityData, true, false),
			Body: &protocol.Put{Payload: payload}}
		if err := o.Send(true, m); err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
	}
	start := time.Now()
	_ = o.Close()
	t.Logf("Close took %v", time.Since(start).Round(time.Millisecond))
	select {
	case <-closed:
	case <-time.After(15 * time.Second):
		t.Fatal("receiver never closed")
	}
	return int(got.Load())
}

// TestCloseLingerDeadPeer: a peer that went silent (here behind a proxy
// that stopped forwarding) does not hold Close up much past half its lease.
func TestCloseLingerDeadPeer(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	pln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer pln.Close()
	var cut atomic.Bool
	pipe := func(dst, src net.Conn) {
		buf := make([]byte, 4096)
		for {
			k, err := src.Read(buf)
			if err != nil {
				return
			}
			if !cut.Load() {
				if _, err := dst.Write(buf[:k]); err != nil {
					return
				}
			}
		}
	}
	go func() { // the proxy
		in, err := pln.Accept()
		if err != nil {
			return
		}
		out, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			return
		}
		go pipe(out, in)
		pipe(in, out)
	}()
	ach := make(chan *Unicast, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			ach <- nil
			return
		}
		a, _ := Accept(c, Config{ZID: testZID(t), WhatAmI: protocol.Peer, Lease: time.Second}, Callbacks{})
		ach <- a
	}()
	c, err := net.Dial("tcp", pln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	o, err := Open(c, Config{ZID: testZID(t), WhatAmI: protocol.Peer, TxQueue: TxQueue{Mode: TxQueued}}, Callbacks{})
	if err != nil {
		t.Fatal(err)
	}
	a := <-ach
	if a == nil {
		t.Fatal("accept failed")
	}
	defer a.Close()
	cut.Store(true)
	start := time.Now()
	_ = o.Close()
	if d := time.Since(start); d > 1500*time.Millisecond {
		t.Fatalf("Close took %v with a silent peer of lease 1s", d)
	} else {
		t.Logf("Close took %v", d.Round(time.Millisecond))
	}
}
