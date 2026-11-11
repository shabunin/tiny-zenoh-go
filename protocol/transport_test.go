package protocol

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/shabunin/tiny-zenoh-go/internal/codec"
)

func zid(t *testing.T, b ...byte) ZID {
	t.Helper()
	z, err := NewZID(b)
	if err != nil {
		t.Fatalf("NewZID: %v", err)
	}
	return z
}

// roundTrip encodes m, decodes it back, and checks equality.
func roundTrip(t *testing.T, m Message) Message {
	t.Helper()
	w := codec.NewWriter()
	m.Encode(w)
	got, err := DecodeTransport(codec.NewReader(w.Bytes()))
	if err != nil {
		t.Fatalf("DecodeTransport(%T): %v", m, err)
	}
	if !reflect.DeepEqual(m, got) {
		t.Errorf("round-trip %T:\n got  %+v\n want %+v", m, got, m)
	}
	return got
}

func TestInitByteLayout(t *testing.T) {
	m := &Init{
		Version:    ProtocolVersion,
		WhatAmI:    Client,
		ZID:        zid(t, 0x01, 0x02, 0x03),
		HasParams:  true,
		Resolution: DefaultResolution,
		BatchSize:  65535,
	}
	w := codec.NewWriter()
	m.Encode(w)
	// header=0x41 (INIT, S set), version, packed=(3-1)<<4|Client=0x22, zid, res=0x0A, batch LE.
	want := []byte{0x41, 0x09, 0x22, 0x01, 0x02, 0x03, 0x0a, 0xff, 0xff}
	if !bytes.Equal(w.Bytes(), want) {
		t.Fatalf("INIT bytes = % x, want % x", w.Bytes(), want)
	}
}

func TestInitRoundTrip(t *testing.T) {
	// SYN without params or exts.
	roundTrip(t, &Init{Version: ProtocolVersion, WhatAmI: Peer, ZID: zid(t, 0xaa)})
	// ACK with params, cookie, and extensions.
	roundTrip(t, &Init{
		IsAck:      true,
		Version:    ProtocolVersion,
		WhatAmI:    Router,
		ZID:        zid(t, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16),
		HasParams:  true,
		Resolution: DefaultResolution,
		BatchSize:  4096,
		Cookie:     []byte{0xde, 0xad, 0xbe, 0xef},
		Exts: []Extension{
			{ID: 0x1, Enc: ExtUnit},
			{ID: 0x5, Enc: ExtZ64, U64: 42, Mandatory: true},
		},
	})
}

func TestOpenRoundTrip(t *testing.T) {
	roundTrip(t, &Open{Lease: 10000, InitialSN: 0, Cookie: []byte{1, 2, 3}})
	roundTrip(t, &Open{
		IsAck:        true,
		LeaseSeconds: true,
		Lease:        10,
		InitialSN:    12345,
		Exts:         []Extension{{ID: 0x1, Enc: ExtUnit}},
	})
}

func TestCloseKeepAliveRoundTrip(t *testing.T) {
	roundTrip(t, &Close{Session: true, Reason: 0})
	roundTrip(t, &Close{Session: false, Reason: 3, Exts: []Extension{{ID: 2, Enc: ExtZBuf, Buf: []byte{9, 9}}}})
	roundTrip(t, &KeepAlive{})
	roundTrip(t, &KeepAlive{Exts: []Extension{{ID: 1, Enc: ExtUnit}}})
}

func TestFrameRoundTrip(t *testing.T) {
	roundTrip(t, &Frame{Reliable: true, SN: 7, Payload: []byte{0x1d, 0x00, 0x11}})
	roundTrip(t, &Frame{
		Reliable: false,
		SN:       1 << 20,
		Exts:     []Extension{{ID: 1, Enc: ExtZ64, U64: 5}},
		Payload:  []byte{1, 2, 3, 4, 5},
	})
	// Empty payload is valid (decodes to nil, semantically identical).
	roundTrip(t, &Frame{SN: 0, Payload: nil})
}

func TestFragmentRoundTrip(t *testing.T) {
	roundTrip(t, &Fragment{Reliable: true, More: true, SN: 3, Payload: []byte{0xaa, 0xbb}})
	roundTrip(t, &Fragment{Reliable: true, More: false, SN: 4, Payload: []byte{0xcc}})
}

func TestExtensionChainOrder(t *testing.T) {
	exts := []Extension{
		{ID: 1, Enc: ExtUnit},
		{ID: 2, Enc: ExtZ64, U64: 300},
		{ID: 3, Enc: ExtZBuf, Buf: []byte{1, 2, 3}, Mandatory: true},
	}
	w := codec.NewWriter()
	WriteExtensions(w, exts)
	got, err := ReadExtensions(codec.NewReader(w.Bytes()))
	if err != nil {
		t.Fatalf("ReadExtensions: %v", err)
	}
	if !reflect.DeepEqual(exts, got) {
		t.Errorf("extensions:\n got  %+v\n want %+v", got, exts)
	}
}

func TestZIDString(t *testing.T) {
	// Wire bytes are little-endian; String prints big-endian hex.
	z := zid(t, 0x01, 0x02, 0x03)
	if got := z.String(); got != "30201" {
		t.Errorf("ZID.String() = %q, want %q", got, "30201")
	}
}

func TestResolutionBits(t *testing.T) {
	if DefaultResolution.FrameSNBits() != 32 {
		t.Errorf("default FrameSNBits = %d, want 32", DefaultResolution.FrameSNBits())
	}
}

func TestJoinRoundTrip(t *testing.T) {
	zid, _ := NewZID([]byte{1, 2, 3, 4})
	q := [8]PrioritySN{{1, 2}, {3, 4}, {5, 6}, {7, 8}, {9, 10}, {11, 12}, {13, 14}, {200, 300}}
	for _, j := range []*Join{
		{Version: ProtocolVersion, WhatAmI: Peer, ZID: zid, BatchSize: MulticastBatchSize, LeaseMillis: 10000, NextSN: PrioritySN{5, 7}},
		{Version: ProtocolVersion, WhatAmI: Router, ZID: zid, BatchSize: 1500, LeaseMillis: 2500, QoS: &q,
			Exts: []Extension{{ID: JoinExtPatch, Enc: ExtZ64, U64: 1}}},
	} {
		w := codec.NewWriter()
		j.Encode(w)
		m, err := DecodeTransport(codec.NewReader(w.Bytes()))
		if err != nil {
			t.Fatal(err)
		}
		got := m.(*Join)
		if j.Resolution == 0 {
			j.Resolution = DefaultResolution
		}
		if !reflect.DeepEqual(got, j) {
			t.Errorf("got %+v\nwant %+v", got, j)
		}
	}
	// zenoh-rust's encoding of a default JOIN (T flag, no S): header 0x27|0x20.
	w := codec.NewWriter()
	(&Join{Version: 9, WhatAmI: Peer, ZID: zid, BatchSize: MulticastBatchSize, LeaseMillis: 10000}).Encode(w)
	if b := w.Bytes(); b[0] != 0x27 {
		t.Errorf("header %#x, want 0x27 (JOIN with T)", b[0])
	}
}
