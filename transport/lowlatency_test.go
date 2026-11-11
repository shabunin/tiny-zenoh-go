package transport

import (
	"encoding/binary"
	"errors"
	"net"
	"sync"
	"testing"

	"github.com/shabunin/gozenoh/protocol"
)

func llNode(t *testing.T, ll bool) *mlNode {
	n := &mlNode{msgs: make(chan string, 64)}
	n.cfg = Config{ZID: testZID(t), WhatAmI: protocol.Peer, LowLatency: ll, BatchSize: 1024}
	return n
}

// recConn records what is written after recording starts.
type recConn struct {
	net.Conn
	mu  sync.Mutex
	on  bool
	buf []byte
}

func (c *recConn) Write(b []byte) (int, error) {
	c.mu.Lock()
	if c.on {
		c.buf = append(c.buf, b...)
	}
	c.mu.Unlock()
	return c.Conn.Write(b)
}

func TestLowLatency(t *testing.T) {
	a, b := llNode(t, true), llNode(t, true)
	var rec *recConn
	if oerr, aerr := connect(t, a, b, func(c net.Conn) Link { rec = &recConn{Conn: c}; return rec }); oerr != nil || aerr != nil {
		t.Fatalf("handshake: %v / %v", oerr, aerr)
	}
	if !a.tr.LowLatency() || !b.tr.LowLatency() {
		t.Fatal("low latency not negotiated")
	}
	rec.mu.Lock()
	rec.on = true
	rec.mu.Unlock()
	if err := a.tr.Send(true, pushPrio("k1", protocol.PriorityData), pushPrio("k2", protocol.PriorityData)); err != nil {
		t.Fatal(err)
	}
	expectMsgs(t, b, "k1", "k2")
	if err := b.tr.Send(false, pushPrio("back", protocol.PriorityData)); err != nil {
		t.Fatal(err)
	}
	expectMsgs(t, a, "back")

	// On the wire: two separate messages, each a u32 LE length then a bare
	// PUSH (no FRAME).
	rec.mu.Lock()
	w := append([]byte(nil), rec.buf...)
	rec.mu.Unlock()
	for i := range 2 {
		if len(w) < 5 {
			t.Fatalf("message %d: only %d bytes left", i, len(w))
		}
		n := int(binary.LittleEndian.Uint32(w))
		if id := w[4] & 0x1f; id != protocol.IDPush {
			t.Fatalf("message %d starts with id %#x, want PUSH", i, id)
		}
		w = w[4+n:]
	}
	if len(w) != 0 {
		t.Fatalf("%d trailing bytes", len(w))
	}

	// Too big for the batch size: refused, the transport lives on.
	big := &protocol.Push{WireExpr: protocol.WireExpr{Suffix: "big"}, Body: &protocol.Put{Payload: make([]byte, 2000)}}
	if err := a.tr.Send(true, big); !errors.Is(err, ErrBatchSize) {
		t.Fatalf("oversized message: %v", err)
	}
	if err := a.tr.Send(true, pushPrio("after", protocol.PriorityData)); err != nil {
		t.Fatal(err)
	}
	expectMsgs(t, b, "after")
}

func TestLowLatencyOneSided(t *testing.T) {
	a, b := llNode(t, true), llNode(t, false)
	if oerr, aerr := connect(t, a, b, nil); oerr != nil || aerr != nil {
		t.Fatalf("handshake: %v / %v", oerr, aerr)
	}
	if a.tr.LowLatency() || b.tr.LowLatency() {
		t.Fatal("low latency used though only one side enables it")
	}
	if err := a.tr.Send(true, pushPrio("k", protocol.PriorityData)); err != nil {
		t.Fatal(err)
	}
	expectMsgs(t, b, "k")
}

func TestLowLatencyNoQoS(t *testing.T) {
	a, b := llNode(t, true), llNode(t, true)
	a.cfg.QoS = true
	if oerr, _ := connect(t, a, b, nil); oerr == nil {
		t.Fatal("QoS with LowLatency accepted")
	}
}
