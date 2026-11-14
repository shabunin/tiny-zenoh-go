package transport

import (
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/shabunin/tiny-zenoh-go/protocol"
)

// The handshake refuses what a misbehaving or hostile peer sends: each case
// scripts the other side message by message and checks the error, and that
// a refusal over an established exchange tells the peer with CLOSE.

// tcpPair returns the two ends of a loopback TCP connection (buffered, unlike
// net.Pipe, so a refusal's CLOSE doesn't block on a peer that stopped reading).
func tcpPair(t *testing.T) (a, b net.Conn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	ch := make(chan net.Conn, 1)
	go func() {
		c, _ := ln.Accept()
		ch <- c
	}()
	a, err = net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	b = <-ch
	if b == nil {
		t.Fatal("accept failed")
	}
	t.Cleanup(func() { _ = a.Close(); _ = b.Close() })
	return a, b
}

type hsCase struct {
	name      string
	cfg       func(t *testing.T, c *Config)
	peer      func(f *fakeRouter) // the scripted other side
	want      string              // in the error
	wantClose bool                // the peer gets CLOSE (invalid)
}

func ext(id byte, enc protocol.ExtEnc, u uint64, buf []byte) protocol.Extension {
	return protocol.Extension{ID: id, Enc: enc, U64: u, Buf: buf}
}

func goodAck(f *fakeRouter, exts ...protocol.Extension) *protocol.Init {
	return &protocol.Init{IsAck: true, Version: protocol.ProtocolVersion, WhatAmI: protocol.Router,
		ZID: testZID(f.t), HasParams: true, Resolution: protocol.DefaultResolution, BatchSize: 2048,
		Cookie: []byte("cookie!"), Exts: exts}
}

func goodSyn(f *fakeRouter, exts ...protocol.Extension) *protocol.Init {
	return &protocol.Init{Version: protocol.ProtocolVersion, WhatAmI: protocol.Peer, ZID: testZID(f.t),
		HasParams: true, Resolution: protocol.DefaultResolution, BatchSize: 2048, Exts: exts}
}

// toOpenSyn plays the acceptor up to (and including) reading OPEN SYN.
func toOpenSyn(f *fakeRouter, exts ...protocol.Extension) {
	f.read()
	f.write(goodAck(f, exts...))
	f.read()
}

// toOpenSynAccept plays the dialer up to sending OPEN SYN built by open.
func toOpenSynAccept(f *fakeRouter, open func(cookie []byte) protocol.Message) {
	f.write(goodSyn(f))
	ack, ok := f.read().(*protocol.Init)
	if !ok || !ack.IsAck {
		f.t.Fatalf("expected INIT ACK, got %+v", ack)
	}
	f.write(open(ack.Cookie))
}

func multiLink(t *testing.T, c *Config) {
	k, err := NewMultiLinkKey()
	if err != nil {
		t.Fatal(err)
	}
	c.MaxLinks, c.MultiLinkKey = 2, k
}

var badSN = uint64(1) << 62 // outside the default 32-bit resolution

func runHandshake(t *testing.T, dial bool, c hsCase) {
	t.Helper()
	ours, theirs := tcpPair(t)
	f := &fakeRouter{t: t, conn: theirs, buf: make([]byte, 65535)}
	cfg := Config{ZID: testZID(t), WhatAmI: protocol.Router, HandshakeTimeout: 2 * time.Second}
	if c.cfg != nil {
		c.cfg(t, &cfg)
	}
	done := make(chan error, 1)
	go func() {
		var tr *Unicast
		var err error
		if dial {
			tr, err = Open(ours, cfg, Callbacks{})
		} else {
			tr, err = Accept(ours, cfg, Callbacks{})
		}
		if tr != nil {
			_ = tr.Close()
		}
		done <- err
	}()
	c.peer(f)
	var err error
	select {
	case err = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handshake still running")
	}
	if err == nil || !errors.Is(err, ErrHandshake) || !strings.Contains(err.Error(), c.want) {
		t.Fatalf("err %v, want ErrHandshake with %q", err, c.want)
	}
	if c.wantClose {
		cl, ok := f.read().(*protocol.Close)
		if !ok || cl.Reason != protocol.CloseInvalid {
			t.Fatalf("peer got %+v, want CLOSE invalid", cl)
		}
	}
}

func TestOpenHandshakeRejects(t *testing.T) {
	cases := []hsCase{
		{name: "ack version", peer: func(f *fakeRouter) {
			f.read()
			a := goodAck(f)
			a.Version = protocol.ProtocolVersion - 1
			f.write(a)
		}, want: "protocol version"},
		{name: "not an ack", peer: func(f *fakeRouter) {
			f.read()
			f.write(&protocol.Open{IsAck: true, Lease: 1000})
		}, want: "expected INIT ACK"},
		{name: "a syn instead", peer: func(f *fakeRouter) {
			f.read()
			f.write(goodSyn(f))
		}, want: "expected INIT ACK"},
		{name: "closed before ack", peer: func(f *fakeRouter) {
			f.read()
			_ = f.conn.Close()
		}, want: "read INIT ACK"},
		{name: "region name not a buffer", peer: func(f *fakeRouter) {
			f.read()
			f.write(goodAck(f, ext(extRegionName, protocol.ExtZ64, 1, nil)))
		}, want: "malformed region name", wantClose: true},
		{name: "region name empty", peer: func(f *fakeRouter) {
			f.read()
			f.write(goodAck(f, ext(extRegionName, protocol.ExtZBuf, 0, []byte{})))
		}, want: "region name is empty", wantClose: true},
		{name: "qos twice", cfg: func(_ *testing.T, c *Config) { c.QoS = true }, peer: func(f *fakeRouter) {
			f.read()
			f.write(goodAck(f, ext(extQoS, protocol.ExtUnit, 0, nil), ext(extQoS, protocol.ExtZ64, 1, nil)))
		}, want: "both QoS and QoSLink", wantClose: true},
		{name: "multilink garbage", cfg: multiLink, peer: func(f *fakeRouter) {
			f.read()
			f.write(goodAck(f, ext(extMultiLink, protocol.ExtZBuf, 0, []byte{0xff, 0xff})))
		}, want: "INIT ACK", wantClose: true},
		{name: "closed before open ack", peer: func(f *fakeRouter) {
			toOpenSyn(f)
			_ = f.conn.Close()
		}, want: "read OPEN ACK"},
		{name: "open ack is a keepalive", peer: func(f *fakeRouter) {
			toOpenSyn(f)
			f.write(&protocol.KeepAlive{})
		}, want: "expected OPEN ACK"},
		{name: "open ack sn", peer: func(f *fakeRouter) {
			toOpenSyn(f)
			f.write(&protocol.Open{IsAck: true, Lease: 1000, InitialSN: badSN})
		}, want: "initial SN"},
		{name: "open ack remote bound", peer: func(f *fakeRouter) {
			toOpenSyn(f)
			f.write(&protocol.Open{IsAck: true, Lease: 1000, Exts: []protocol.Extension{ext(extRemoteBound, protocol.ExtZ64, 7, nil)}})
		}, want: "remote bound", wantClose: true},
		{name: "open ack lacks usrpwd", cfg: func(_ *testing.T, c *Config) {
			c.Auth = &Auth{User: "u", Password: "p"}
		}, peer: func(f *fakeRouter) {
			f.read()
			// usrpwd nonce sub-extension (id 2, z64) inside the auth extension
			f.write(goodAck(f, ext(extAuth, protocol.ExtZBuf, 0, []byte{0x22, 0x07})))
			f.read()
			f.write(&protocol.Open{IsAck: true, Lease: 1000})
		}, want: "usrpwd confirmation", wantClose: true},
		{name: "ack without usrpwd", cfg: func(_ *testing.T, c *Config) {
			c.Auth = &Auth{User: "u", Password: "p"}
		}, peer: func(f *fakeRouter) {
			f.read()
			f.write(goodAck(f))
		}, want: "usrpwd"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) { runHandshake(t, true, c) })
	}
}

func TestAcceptHandshakeRejects(t *testing.T) {
	open := func(isn uint64, exts ...protocol.Extension) func([]byte) protocol.Message {
		return func(cookie []byte) protocol.Message {
			return &protocol.Open{Lease: 1000, InitialSN: isn, Cookie: cookie, Exts: exts}
		}
	}
	cases := []hsCase{
		{name: "open first", peer: func(f *fakeRouter) {
			f.write(&protocol.Open{Lease: 1000})
		}, want: "expected INIT SYN"},
		{name: "an ack first", peer: func(f *fakeRouter) {
			f.write(goodAck(f))
		}, want: "expected INIT SYN"},
		{name: "closed at once", peer: func(f *fakeRouter) {
			_ = f.conn.Close()
		}, want: "read INIT SYN"},
		{name: "syn version", peer: func(f *fakeRouter) {
			s := goodSyn(f)
			s.Version = protocol.ProtocolVersion + 1
			f.write(s)
		}, want: "protocol version", wantClose: true},
		{name: "region name not a buffer", peer: func(f *fakeRouter) {
			f.write(goodSyn(f, ext(extRegionName, protocol.ExtUnit, 0, nil)))
		}, want: "malformed region name", wantClose: true},
		{name: "qos twice", cfg: func(_ *testing.T, c *Config) { c.QoS = true }, peer: func(f *fakeRouter) {
			f.write(goodSyn(f, ext(extQoS, protocol.ExtUnit, 0, nil), ext(extQoS, protocol.ExtZ64, 1, nil)))
		}, want: "both QoS and QoSLink", wantClose: true},
		{name: "multilink garbage", cfg: multiLink, peer: func(f *fakeRouter) {
			f.write(goodSyn(f, ext(extMultiLink, protocol.ExtZBuf, 0, []byte{0xff})))
		}, want: "INIT SYN", wantClose: true},
		{name: "closed after init ack", peer: func(f *fakeRouter) {
			f.write(goodSyn(f))
			f.read()
			_ = f.conn.Close()
		}, want: "read OPEN SYN"},
		{name: "wrong cookie", peer: func(f *fakeRouter) {
			toOpenSynAccept(f, func([]byte) protocol.Message { return &protocol.Open{Lease: 1000, Cookie: []byte("forged")} })
		}, want: "unknown cookie", wantClose: true},
		{name: "not an open syn", peer: func(f *fakeRouter) {
			toOpenSynAccept(f, func([]byte) protocol.Message { return &protocol.KeepAlive{} })
		}, want: "expected OPEN SYN"},
		{name: "open syn sn", peer: func(f *fakeRouter) {
			toOpenSynAccept(f, open(badSN))
		}, want: "initial SN", wantClose: true},
		{name: "open syn remote bound", peer: func(f *fakeRouter) {
			toOpenSynAccept(f, open(0, ext(extRemoteBound, protocol.ExtUnit, 0, nil)))
		}, want: "remote bound", wantClose: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) { runHandshake(t, false, c) })
	}
}

func TestEstablishConfigErrors(t *testing.T) {
	a, _ := tcpPair(t)
	if _, err := Open(a, Config{}, Callbacks{}); err == nil || !strings.Contains(err.Error(), "ZID is required") {
		t.Fatalf("no ZID: %v", err)
	}
	b, _ := tcpPair(t)
	if _, err := Accept(b, Config{ZID: testZID(t), MaxLinks: 2}, Callbacks{}); err == nil || !strings.Contains(err.Error(), "MultiLinkKey") {
		t.Fatalf("multilink without key: %v", err)
	}
}
