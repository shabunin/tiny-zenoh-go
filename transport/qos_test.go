package transport

import (
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/shabunin/tiny-zenoh-go/internal/codec"
	"github.com/shabunin/tiny-zenoh-go/protocol"
)

// setupQoS is setup with a router that answers INIT with ackExts.
func setupQoS(t *testing.T, ackExts []protocol.Extension, cb Callbacks) (*Unicast, *fakeRouter, *protocol.Init) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	type res struct {
		f   *fakeRouter
		syn *protocol.Init
	}
	ch := make(chan res, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		f := &fakeRouter{t: t, conn: c, buf: make([]byte, 65535)}
		syn := f.read().(*protocol.Init)
		f.write(&protocol.Init{
			IsAck: true, Version: protocol.ProtocolVersion, WhatAmI: protocol.Router,
			ZID: testZID(t), HasParams: true, Resolution: protocol.DefaultResolution,
			BatchSize: 1024, Cookie: []byte("c"), Exts: ackExts,
		})
		open := f.read().(*protocol.Open)
		f.clientISN = open.InitialSN
		f.write(&protocol.Open{IsAck: true, Lease: 10000, InitialSN: 100})
		ch <- res{f, syn}
	}()
	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	tr, err := Open(c, Config{ZID: testZID(t), WhatAmI: protocol.Client, QoS: true}, cb)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	r := <-ch
	t.Cleanup(func() { _ = tr.Close(); _ = r.f.conn.Close() })
	return tr, r.f, r.syn
}

func pushPrio(key string, p protocol.Priority) *protocol.Push {
	return &protocol.Push{WireExpr: protocol.WireExpr{Suffix: key}, QoS: protocol.NewQoS(p, false, false),
		Body: &protocol.Put{Payload: []byte("x")}}
}

func framePrio(t *testing.T, fr *protocol.Frame) protocol.Priority {
	t.Helper()
	e := findExt(fr.Exts, extQoS)
	if e == nil {
		return protocol.PriorityData
	}
	if !e.Mandatory || e.Enc != protocol.ExtZ64 {
		t.Errorf("frame QoS ext = %+v, want mandatory Z64", e)
	}
	return protocol.Priority(e.U64)
}

func TestQoSFramesPerPriority(t *testing.T) {
	c := newCollector()
	tr, f, syn := setupQoS(t, []protocol.Extension{{ID: extQoS, Enc: protocol.ExtUnit}}, c.cb())
	if e := findExt(syn.Exts, extQoS); e == nil || e.Enc != protocol.ExtUnit {
		t.Fatalf("INIT SYN QoS ext = %+v", e)
	}
	if !tr.QoS() {
		t.Fatal("QoS not negotiated")
	}
	err := tr.Send(true, pushPrio("d1", protocol.PriorityData), pushPrio("c1", protocol.PriorityControl),
		pushPrio("c2", protocol.PriorityControl), pushPrio("r1", protocol.PriorityRealTime),
		pushPrio("d2", protocol.PriorityData))
	if err != nil {
		t.Fatal(err)
	}
	// Queued, highest priority first; messages of a channel join its frame.
	want := []struct {
		prio protocol.Priority
		sn   uint64
		n    int
	}{
		{protocol.PriorityControl, f.clientISN, 2}, // own channel: same initial SN
		{protocol.PriorityRealTime, f.clientISN, 1},
		{protocol.PriorityData, f.clientISN, 2},
	}
	for i, w := range want {
		fr := f.read().(*protocol.Frame)
		if p := framePrio(t, fr); p != w.prio || fr.SN != w.sn&snMask(32) {
			t.Errorf("frame %d: prio %d sn %d, want prio %d sn %d", i, p, fr.SN, w.prio, w.sn)
		}
		if n := countMsgs(t, fr.Payload); n != w.n {
			t.Errorf("frame %d: %d messages, want %d", i, n, w.n)
		}
	}

	// Inbound: each priority has its own SN window.
	qos := func(p protocol.Priority) []protocol.Extension { return qosExts(p) }
	f.write(&protocol.Frame{Reliable: true, SN: 100, Payload: pushBytes("a", "1")})
	f.write(&protocol.Frame{Reliable: true, SN: 100, Exts: qos(protocol.PriorityInteractiveHigh), Payload: pushBytes("b", "2")})
	f.write(&protocol.Frame{Reliable: true, SN: 100, Exts: qos(protocol.PriorityInteractiveHigh), Payload: pushBytes("dup", "x")})
	big := pushBytes("c", "333")
	f.write(&protocol.Fragment{Reliable: true, More: true, SN: 100, Exts: qos(protocol.PriorityControl), Payload: big[:4]})
	f.write(&protocol.Frame{Reliable: true, SN: 101, Payload: pushBytes("d", "4")})
	f.write(&protocol.Fragment{Reliable: true, SN: 101, Exts: qos(protocol.PriorityControl), Payload: big[4:]})
	got := c.wait(t, 4)
	if w := []string{"a=1", "b=2", "d=4", "c=333"}; !reflect.DeepEqual(got, w) {
		t.Errorf("got %v, want %v", got, w)
	}
}

func TestQoSLinkPriorityRange(t *testing.T) {
	// QoSLink: priorities 4..6 present, reliability absent. As in zenoh-rust,
	// the range steers link selection; a message outside it still goes out
	// with its own priority (on the only link).
	v := uint64(0b010) | uint64(protocol.PriorityDataHigh)<<3 | uint64(protocol.PriorityDataLow)<<11
	tr, f, _ := setupQoS(t, []protocol.Extension{{ID: extQoS, Enc: protocol.ExtZ64, U64: v}}, Callbacks{})
	if !tr.QoS() {
		t.Fatal("QoS not negotiated")
	}
	if l := tr.firstLink(); !l.hasRange || l.prioLo != 4 || l.prioHi != 6 {
		t.Errorf("link range = %v %d..%d", l.hasRange, l.prioLo, l.prioHi)
	}
	if err := tr.Send(false, pushPrio("c", protocol.PriorityControl), pushPrio("b", protocol.PriorityBackground)); err != nil {
		t.Fatal(err)
	}
	if p := framePrio(t, f.read().(*protocol.Frame)); p != protocol.PriorityControl {
		t.Errorf("Control sent at %d", p)
	}
	if p := framePrio(t, f.read().(*protocol.Frame)); p != protocol.PriorityBackground {
		t.Errorf("Background sent at %d", p)
	}
}

func TestNoQoSRejectsPriorityFrames(t *testing.T) {
	closed := make(chan error, 1)
	tr, f, _ := setupQoS(t, nil, Callbacks{OnClose: func(err error) { closed <- err }})
	if tr.QoS() {
		t.Fatal("QoS on although the peer did not answer it")
	}
	if err := tr.Send(true, pushPrio("c", protocol.PriorityControl)); err != nil {
		t.Fatal(err)
	}
	if fr := f.read().(*protocol.Frame); len(fr.Exts) != 0 {
		t.Errorf("frame without QoS carries %+v", fr.Exts)
	}
	f.write(&protocol.Frame{Reliable: true, SN: 100, Exts: qosExts(protocol.PriorityControl), Payload: pushBytes("a", "1")})
	select {
	case err := <-closed:
		if err == nil {
			t.Error("closed without error")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("priority frame on a non-QoS transport was accepted")
	}
}

func TestQoSAcceptPair(t *testing.T) {
	for _, c := range []struct{ o, a, want bool }{{true, true, true}, {true, false, false}, {false, true, false}} {
		got := make(chan string, 8)
		o, a, oerr, aerr := acceptPair(t,
			Config{ZID: testZID(t), WhatAmI: protocol.Peer, QoS: c.o, BatchSize: 512},
			Config{ZID: testZID(t), WhatAmI: protocol.Peer, QoS: c.a},
			Callbacks{},
			Callbacks{OnMessage: func(m protocol.Message, _ bool) error { got <- m.(*protocol.Push).WireExpr.Suffix; return nil }})
		if oerr != nil || aerr != nil {
			t.Fatalf("open=%v accept=%v", oerr, aerr)
		}
		if o.QoS() != c.want || a.QoS() != c.want {
			t.Errorf("%+v: QoS o=%v a=%v", c, o.QoS(), a.QoS())
		}
		bigPush := pushPrio("big", protocol.PriorityInteractiveLow)
		bigPush.Body = &protocol.Put{Payload: make([]byte, 3000)} // fragmented
		if err := o.Send(true, pushPrio("1", protocol.PriorityBackground), bigPush, pushPrio("2", protocol.PriorityControl)); err != nil {
			t.Fatal(err)
		}
		// Priorities are queued apart: any order.
		seen := map[string]bool{}
		for range 3 {
			select {
			case k := <-got:
				seen[k] = true
			case <-time.After(2 * time.Second):
				t.Fatalf("%+v: missing messages (got %v)", c, seen)
			}
		}
		if !seen["1"] || !seen["big"] || !seen["2"] {
			t.Errorf("%+v: got %v", c, seen)
		}
		_ = o.Close()
		_ = a.Close()
	}
}

// countMsgs counts the network messages in a frame payload.
func countMsgs(t *testing.T, b []byte) int {
	t.Helper()
	r := codec.NewReader(b)
	n := 0
	for r.Remaining() > 0 {
		if _, err := protocol.DecodeNetwork(r); err != nil {
			t.Fatalf("frame payload: %v", err)
		}
		n++
	}
	return n
}
