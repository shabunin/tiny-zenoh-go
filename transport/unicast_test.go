package transport

import (
	"bytes"
	"crypto/rand"
	"errors"
	"net"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/shabunin/gozenoh/internal/codec"
	"github.com/shabunin/gozenoh/protocol"
)

func testZID(t *testing.T) protocol.ZID {
	t.Helper()
	var b [16]byte
	_, _ = rand.Read(b[:])
	z, err := protocol.NewZID(b[:])
	if err != nil {
		t.Fatal(err)
	}
	return z
}

// rawMsg is a pre-encoded "network message" for exercising framing.
type rawMsg []byte

func (m rawMsg) Encode(w *codec.Writer) { w.WriteRaw(m) }

// fakeRouter is the accepting side of a handshake, driven by the test.
type fakeRouter struct {
	t         *testing.T
	conn      net.Conn
	buf       []byte
	clientISN uint64
	maxBody   int // largest batch body received
}

func (f *fakeRouter) read() protocol.Message {
	f.t.Helper()
	_ = f.conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	b, err := ReadBatch(f.conn, f.buf)
	if err != nil {
		f.t.Fatalf("router read: %v", err)
	}
	f.maxBody = max(f.maxBody, len(b))
	m, err := protocol.DecodeTransport(codec.NewReader(b))
	if err != nil {
		f.t.Fatalf("router decode: %v", err)
	}
	return m
}

func (f *fakeRouter) write(m protocol.Message) {
	f.t.Helper()
	w := codec.NewWriter()
	w.WriteRaw([]byte{0, 0})
	m.Encode(w)
	b := w.Bytes()
	n := len(b) - 2
	b[0], b[1] = byte(n), byte(n>>8)
	if _, err := f.conn.Write(b); err != nil {
		f.t.Fatalf("router write: %v", err)
	}
}

// accept performs the router side of INIT/OPEN.
func (f *fakeRouter) accept(batch uint16, leaseMS uint64) {
	syn, ok := f.read().(*protocol.Init)
	if !ok || syn.IsAck {
		f.t.Fatalf("expected INIT SYN, got %+v", syn)
	}
	f.write(&protocol.Init{
		IsAck: true, Version: protocol.ProtocolVersion, WhatAmI: protocol.Router,
		ZID: testZID(f.t), HasParams: true, Resolution: protocol.DefaultResolution,
		BatchSize: batch, Cookie: []byte("cookie!"),
	})
	open, ok := f.read().(*protocol.Open)
	if !ok || open.IsAck || string(open.Cookie) != "cookie!" {
		f.t.Fatalf("expected OPEN SYN with cookie, got %+v", open)
	}
	f.clientISN = open.InitialSN
	f.write(&protocol.Open{IsAck: true, Lease: leaseMS, InitialSN: 100})
}

// setup starts a fake router on loopback and opens a client transport to it.
func setup(t *testing.T, batch uint16, leaseMS uint64, cfg Config, cb Callbacks) (*Unicast, *fakeRouter) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	routerCh := make(chan *fakeRouter, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		f := &fakeRouter{t: t, conn: c, buf: make([]byte, 65535)}
		f.accept(batch, leaseMS)
		routerCh <- f
	}()
	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	cfg.ZID = testZID(t)
	cfg.WhatAmI = protocol.Client
	tr, err := Open(c, cfg, cb)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	f := <-routerCh
	t.Cleanup(func() { _ = tr.Close(); _ = f.conn.Close() })
	return tr, f
}

func TestHandshakeNegotiation(t *testing.T) {
	tr, _ := setup(t, 1024, 7000, Config{}, Callbacks{})
	if tr.BatchSize() != 1024 {
		t.Errorf("BatchSize = %d, want 1024 (min of both sides)", tr.BatchSize())
	}
	if tr.PeerLease() != 7*time.Second {
		t.Errorf("PeerLease = %v, want 7s", tr.PeerLease())
	}
	if tr.PeerWhatAmI() != protocol.Router {
		t.Errorf("PeerWhatAmI = %v", tr.PeerWhatAmI())
	}
}

func TestSendFramesAndSN(t *testing.T) {
	tr, f := setup(t, 1024, 10000, Config{}, Callbacks{})
	if err := tr.Send(true, rawMsg{0x1d, 1}, rawMsg{0x1d, 2}); err != nil {
		t.Fatal(err)
	}
	if err := tr.Send(true, rawMsg{0x1d, 3}); err != nil {
		t.Fatal(err)
	}
	fr1 := f.read().(*protocol.Frame)
	fr2 := f.read().(*protocol.Frame)
	if !fr1.Reliable || fr1.SN != f.clientISN || !bytes.Equal(fr1.Payload, []byte{0x1d, 1, 0x1d, 2}) {
		t.Errorf("frame1 = %+v (isn %d)", fr1, f.clientISN)
	}
	if f.clientISN > snMask(32) {
		t.Errorf("initial SN %#x exceeds 28-bit SN space", f.clientISN)
	}
	if fr2.SN != (f.clientISN+1)&snMask(32) || !bytes.Equal(fr2.Payload, []byte{0x1d, 3}) {
		t.Errorf("frame2 = %+v", fr2)
	}
}

func TestSendFragments(t *testing.T) {
	tr, f := setup(t, 256, 10000, Config{}, Callbacks{})
	big := make([]byte, 1000)
	for i := range big {
		big[i] = byte(i)
	}
	errc := make(chan error, 1)
	go func() { errc <- tr.Send(true, rawMsg(big)) }()

	var got []byte
	sn := f.clientISN
	for {
		fr, ok := f.read().(*protocol.Fragment)
		if !ok {
			t.Fatalf("expected FRAGMENT")
		}
		if fr.SN != sn {
			t.Fatalf("fragment SN = %d, want %d", fr.SN, sn)
		}
		sn++
		got = append(got, fr.Payload...)
		if !fr.More {
			break
		}
	}
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, big) {
		t.Errorf("reassembled %d bytes, mismatch", len(got))
	}
	// zenoh-rust counts the 2-byte stream prefix inside the batch size.
	if f.maxBody > 256-2 {
		t.Errorf("sent a %d-byte batch body; limit with batch size 256 is 254", f.maxBody)
	}
	if f.maxBody < 250 {
		t.Errorf("fragments only %d bytes: not filling batches", f.maxBody)
	}
}

func pushBytes(key, payload string) []byte {
	w := codec.NewWriter()
	(&protocol.Push{WireExpr: protocol.WireExpr{Suffix: key}, Body: &protocol.Put{Payload: []byte(payload)}}).Encode(w)
	return w.Bytes()
}

// collector gathers the PUT payloads delivered via OnMessage.
type collector struct {
	mu  sync.Mutex
	got []string
	ch  chan struct{}
}

func newCollector() *collector { return &collector{ch: make(chan struct{}, 16)} }

func (c *collector) cb() Callbacks {
	return Callbacks{OnMessage: func(m protocol.Message, rel bool) error {
		p := m.(*protocol.Push)
		c.mu.Lock()
		c.got = append(c.got, p.WireExpr.Suffix+"="+string(p.Body.(*protocol.Put).Payload))
		c.mu.Unlock()
		c.ch <- struct{}{}
		return nil
	}}
}

func (c *collector) wait(t *testing.T, n int) []string {
	t.Helper()
	for range n {
		select {
		case <-c.ch:
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for OnMessage")
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.got...)
}

func TestReceiveFrameAndFragments(t *testing.T) {
	c := newCollector()
	_, f := setup(t, 1024, 10000, Config{}, c.cb())
	// Two network messages in one FRAME.
	f.write(&protocol.Frame{Reliable: true, SN: 100, Payload: append(pushBytes("a", "1"), pushBytes("b", "2")...)})
	// One message split across two FRAGMENTs.
	big := pushBytes("c", "333")
	f.write(&protocol.Fragment{Reliable: true, More: true, SN: 101, Payload: big[:4]})
	f.write(&protocol.Fragment{Reliable: true, More: false, SN: 102, Payload: big[4:]})
	got := c.wait(t, 3)
	want := []string{"a=1", "b=2", "c=333"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestDuplicateSNDropped(t *testing.T) {
	c := newCollector()
	_, f := setup(t, 1024, 10000, Config{}, c.cb())
	f.write(&protocol.Frame{Reliable: true, SN: 100, Payload: pushBytes("a", "1")})
	f.write(&protocol.Frame{Reliable: true, SN: 100, Payload: pushBytes("dup", "x")}) // duplicate
	f.write(&protocol.Frame{Reliable: true, SN: 50, Payload: pushBytes("old", "x")})  // stale
	f.write(&protocol.Frame{Reliable: true, SN: 101, Payload: pushBytes("b", "2")})
	// Best-effort has an independent SN space starting at the same initial SN.
	f.write(&protocol.Frame{Reliable: false, SN: 100, Payload: pushBytes("be", "3")})
	got := c.wait(t, 3)
	want := []string{"a=1", "b=2", "be=3"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestPeerCloseAndKeepAlive(t *testing.T) {
	closed := make(chan error, 1)
	cb := Callbacks{OnClose: func(err error) { closed <- err }}
	// Our lease 200ms => keep-alive every 50ms when idle.
	_, f := setup(t, 1024, 10000, Config{Lease: 200 * time.Millisecond}, cb)
	if _, ok := f.read().(*protocol.KeepAlive); !ok {
		t.Fatal("expected KEEP_ALIVE from idle client")
	}
	f.write(&protocol.Close{Session: true, Reason: 0})
	select {
	case err := <-closed:
		if !errors.Is(err, ErrPeerClosed) {
			t.Errorf("OnClose err = %v, want ErrPeerClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("transport did not close")
	}
}

func TestLeaseExpiry(t *testing.T) {
	closed := make(chan error, 1)
	cb := Callbacks{OnClose: func(err error) { closed <- err }}
	// Peer announces a 150ms lease and then goes silent.
	_, f := setup(t, 1024, 150, Config{Lease: 100 * time.Millisecond}, cb)
	go func() { // drain our keep-alives so writes don't block
		buf := make([]byte, 65535)
		for {
			if _, err := ReadBatch(f.conn, buf); err != nil {
				return
			}
		}
	}()
	select {
	case err := <-closed:
		if !errors.Is(err, ErrLeaseExpired) {
			t.Errorf("OnClose err = %v, want ErrLeaseExpired", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("lease did not expire")
	}
}

// TestInteropZenohd runs against a real router when ZENOH_ROUTER is set, e.g.
// ZENOH_ROUTER=127.0.0.1:7447. It uses a short lease so that only working
// keep-alives keep the session up.
func TestInteropZenohd(t *testing.T) {
	// Accept both "host:port" and the locator form "tcp/host:port".
	addr := strings.TrimPrefix(os.Getenv("ZENOH_ROUTER"), "tcp/")
	if addr == "" || addr == "go" {
		t.Skip("set ZENOH_ROUTER=tcp/host:port to run against a real zenohd")
	}
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	closed := make(chan error, 1)
	tr, err := Open(c, Config{ZID: testZID(t), WhatAmI: protocol.Client, Lease: time.Second},
		Callbacks{OnClose: func(err error) { closed <- err }})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Logf("connected to %s %s, batch=%d lease=%v", tr.PeerWhatAmI(), tr.PeerZID(), tr.BatchSize(), tr.PeerLease())
	select {
	case err := <-closed:
		t.Fatalf("session closed early: %v", err)
	case <-time.After(3 * time.Second): // 3x our lease
	}
	if err := tr.Close(); err != nil {
		t.Fatal(err)
	}
}

// acceptPair connects an Open-side and an Accept-side transport over loopback.
func acceptPair(t *testing.T, ocfg, acfg Config, ocb, acb Callbacks) (*Unicast, *Unicast, error, error) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	type res struct {
		u   *Unicast
		err error
	}
	ch := make(chan res, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			ch <- res{nil, err}
			return
		}
		u, err := Accept(c, acfg, acb)
		ch <- res{u, err}
	}()
	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	o, oerr := Open(c, ocfg, ocb)
	a := <-ch
	return o, a.u, oerr, a.err
}

func TestAcceptHandshakeAndTraffic(t *testing.T) {
	za, _ := protocol.NewZID([]byte{1, 2, 3})
	zb, _ := protocol.NewZID([]byte{4, 5})
	got := make(chan protocol.Message, 4)
	var admitted protocol.ZID
	o, a, oerr, aerr := acceptPair(t,
		Config{ZID: za, WhatAmI: protocol.Peer, BatchSize: 1024, Lease: 3 * time.Second},
		Config{ZID: zb, WhatAmI: protocol.Peer, BatchSize: 4096, Lease: 1500 * time.Millisecond,
			Admit: func(z protocol.ZID, w protocol.WhatAmI) error { admitted = z; return nil }},
		Callbacks{},
		Callbacks{OnMessage: func(m protocol.Message, _ bool) error { got <- m; return nil }})
	if oerr != nil || aerr != nil {
		t.Fatalf("open=%v accept=%v", oerr, aerr)
	}
	defer o.Close()
	defer a.Close()
	if !admitted.Equal(za) || !a.PeerZID().Equal(za) || !o.PeerZID().Equal(zb) {
		t.Errorf("zids: admitted %s, a.peer %s, o.peer %s", admitted, a.PeerZID(), o.PeerZID())
	}
	if o.BatchSize() != 1024 || a.BatchSize() != 1024 {
		t.Errorf("batch o=%d a=%d", o.BatchSize(), a.BatchSize())
	}
	if a.PeerLease() != 3*time.Second || o.PeerLease() != 1500*time.Millisecond {
		t.Errorf("lease a.peer=%v o.peer=%v", a.PeerLease(), o.PeerLease())
	}
	big := make([]byte, 5000) // forces fragments at batch 1024
	for i := range big {
		big[i] = byte(i)
	}
	push := &protocol.Push{WireExpr: protocol.WireExpr{Suffix: "a/b"}, Body: &protocol.Put{Payload: big}}
	if err := o.Send(true, push); err != nil {
		t.Fatal(err)
	}
	select {
	case m := <-got:
		p, ok := m.(*protocol.Push)
		if !ok || string(p.Body.(*protocol.Put).Payload) != string(big) {
			t.Fatalf("got %#v", m)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no message")
	}
}

func TestAcceptRejectsDuplicate(t *testing.T) {
	za, _ := protocol.NewZID([]byte{1})
	zb, _ := protocol.NewZID([]byte{2})
	_, _, oerr, aerr := acceptPair(t,
		Config{ZID: za, WhatAmI: protocol.Peer},
		Config{ZID: zb, WhatAmI: protocol.Peer,
			Admit: func(protocol.ZID, protocol.WhatAmI) error { return ErrDuplicate }},
		Callbacks{}, Callbacks{})
	if !errors.Is(aerr, ErrDuplicate) {
		t.Errorf("accept err = %v", aerr)
	}
	if oerr == nil || !strings.Contains(oerr.Error(), "reason 0x4") {
		t.Errorf("open err = %v, want CLOSE MAX_LINKS", oerr)
	}
}

func TestAcceptRejectsSelf(t *testing.T) {
	z, _ := protocol.NewZID([]byte{7})
	_, _, oerr, aerr := acceptPair(t, Config{ZID: z}, Config{ZID: z}, Callbacks{}, Callbacks{})
	if !errors.Is(aerr, ErrSelf) || oerr == nil {
		t.Errorf("accept=%v open=%v", aerr, oerr)
	}
}
