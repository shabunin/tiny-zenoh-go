package protocol

import (
	"bytes"
	"testing"

	"github.com/shabunin/tiny-zenoh-go/internal/codec"
)

// Fuzzing the decoders of everything a remote node sends. A decoder must
// reject bad input with an error, never panic; and what it accepts must
// survive re-encoding: encode(decode(x)) decodes again and re-encodes to
// the same bytes.

func encode(m Message) []byte {
	var w codec.Writer
	m.Encode(&w)
	return w.Bytes()
}

func seedNetwork() []Message {
	zid := ZID{b: []byte{1, 2, 3, 4}}
	ts := &Timestamp{Time: 0x1234567890, ID: zid}
	src := &SourceInfo{ZID: zid, EID: 7, SN: 99}
	parts := []PayloadPart{{Raw: []byte("head")}, {Shm: &ShmInfo{DataLen: 1 << 20, MetaID: 3, MetaIndex: 700, Generation: 9}}}
	ms := []Message{
		&Push{WireExpr: WireExpr{Suffix: "a/b"}, Body: &Put{Payload: []byte("hi")}},
		&Push{WireExpr: WireExpr{Scope: 3, Suffix: "/x", Mapping: true},
			QoS: NewQoS(PriorityRealTime, true, true), Timestamp: ts, NodeID: 4,
			Body: &Put{Timestamp: ts, Encoding: Encoding{ID: 5, Schema: []byte("s")},
				SourceInfo: src, Attachment: []byte("att"), Payload: []byte("payload")}},
		&Push{WireExpr: WireExpr{Suffix: "a"}, Body: &Del{Timestamp: ts, SourceInfo: src, Attachment: []byte{1}}},
		&Push{WireExpr: WireExpr{Suffix: "a/b"}, Body: &Put{Parts: parts, Attachment: []byte{1}}},
		&Request{ID: 42, WireExpr: WireExpr{Suffix: "q/**"}, QoS: QoSRequest,
			Target: TargetAll, Budget: 3, TimeoutMS: 10000, NodeID: 2,
			Query: &Query{Consolidation: ConsolidationLatest, Parameters: "a=1;b=2", SourceInfo: src,
				Body: &QueryBody{Encoding: Encoding{ID: 1}, Payload: []byte("body")}, Attachment: []byte("att")}},
		&Request{ID: 1, WireExpr: WireExpr{Suffix: "q"}, Query: &Query{Body: &QueryBody{Parts: parts}}},
		&Response{RequestID: 42, WireExpr: WireExpr{Suffix: "q/1"},
			Responder: &EntityGlobalID{ZID: zid, EID: 9}, Body: &Reply{Body: &Put{Payload: []byte("v")}}},
		&Response{RequestID: 1, WireExpr: WireExpr{Suffix: "q"}, Body: &Reply{Consolidation: ConsolidationNone, Body: &Del{}}},
		&Response{RequestID: 2, WireExpr: WireExpr{Suffix: "q"}, Body: &Err{Encoding: Encoding{ID: 1}, Payload: []byte("boom")}},
		&ResponseFinal{RequestID: 42, QoS: QoSRequest},
		&Interest{ID: 5, Mode: InterestCurrentFuture, Options: InterestTokens | InterestAggregate, WireExpr: &WireExpr{Suffix: "alive/**"}},
		&Interest{ID: 6, Mode: InterestCurrent, Options: InterestKeyExprs | InterestSubscribers},
		&Interest{ID: 5, Mode: InterestFinal},
		&OAM{ID: 1, Enc: ExtZBuf, Buf: []byte{1, 2}},
		&OAM{ID: 2, Enc: ExtZ64, U64: 300, QoS: QoSDeclare},
	}
	for _, d := range []Declaration{
		&DeclareKeyExpr{ID: 3, WireExpr: WireExpr{Suffix: "demo/ex"}},
		&UndeclareKeyExpr{ID: 3},
		&DeclareSubscriber{ID: 10, WireExpr: WireExpr{Scope: 3, Suffix: "/**", Mapping: true}},
		&UndeclareSubscriber{ID: 10, WireExpr: WireExpr{Suffix: "demo/**"}},
		&DeclareQueryable{ID: 11, WireExpr: WireExpr{Suffix: "q"}, Complete: true, Distance: 2},
		&UndeclareQueryable{ID: 11, WireExpr: WireExpr{Suffix: "q", Mapping: true}},
		&DeclareToken{ID: 13, WireExpr: WireExpr{Suffix: "alive/me"}},
		&UndeclareToken{ID: 13},
		&DeclareFinal{},
	} {
		ms = append(ms, &Declare{HasInterestID: true, InterestID: 77, QoS: QoSDeclare, NodeID: 1, Body: d})
	}
	return ms
}

func seedTransport() []Message {
	z := ZID{b: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}}
	return []Message{
		&Init{Version: ProtocolVersion, WhatAmI: Peer, ZID: ZID{b: []byte{0xaa}}},
		&Init{IsAck: true, Version: ProtocolVersion, WhatAmI: Router, ZID: z, HasParams: true,
			Resolution: DefaultResolution, BatchSize: 4096, Cookie: []byte{0xde, 0xad},
			Exts: []Extension{{ID: 0x1, Enc: ExtUnit}, {ID: 0x5, Enc: ExtZ64, U64: 42, Mandatory: true},
				{ID: 0x3, Enc: ExtZBuf, Buf: []byte{1, 2, 3}}}},
		&Open{Lease: 10000, InitialSN: 0, Cookie: []byte{1, 2, 3}},
		&Open{IsAck: true, LeaseSeconds: true, Lease: 10, InitialSN: 12345, Exts: []Extension{{ID: 0x1, Enc: ExtUnit}}},
		&Close{Session: true, Reason: 0},
		&Close{Reason: 3, Exts: []Extension{{ID: 2, Enc: ExtZBuf, Buf: []byte{9, 9}}}},
		&KeepAlive{},
		&Frame{Reliable: true, SN: 7, Payload: encode(&Push{WireExpr: WireExpr{Suffix: "a/b"}, Body: &Put{Payload: []byte("hi")}})},
		&Frame{SN: 1 << 20, Exts: []Extension{{ID: 1, Enc: ExtZ64, U64: 5}}, Payload: []byte{1, 2, 3}},
		&Fragment{Reliable: true, More: true, SN: 3, Payload: []byte{0xaa, 0xbb}},
		&Join{Version: ProtocolVersion, WhatAmI: Peer, ZID: z, Resolution: DefaultResolution, BatchSize: 8192, LeaseMillis: 10000, QoS: &[8]PrioritySN{{Reliable: 5}}},
	}
}

func FuzzDecodeNetwork(f *testing.F) {
	for _, m := range seedNetwork() {
		f.Add(encode(m))
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		r := codec.NewReader(b)
		for r.Remaining() > 0 {
			before := r.Remaining()
			m, err := DecodeNetwork(r)
			if err != nil {
				return
			}
			if r.Remaining() >= before {
				t.Fatalf("decoder made no progress on % x", b)
			}
			stable(t, m, func(r *codec.Reader) (Message, error) { return DecodeNetwork(r) })
		}
	})
}

func FuzzDecodeTransport(f *testing.F) {
	for _, m := range seedTransport() {
		f.Add(encode(m))
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		r := codec.NewReader(b)
		for r.Remaining() > 0 {
			before := r.Remaining()
			m, err := DecodeTransport(r)
			if err != nil {
				return
			}
			if r.Remaining() >= before {
				t.Fatalf("decoder made no progress on % x", b)
			}
			stable(t, m, func(r *codec.Reader) (Message, error) { return DecodeTransport(r) })
		}
	})
}

func FuzzDecodeScouting(f *testing.F) {
	z := ZID{b: []byte{0xaa, 0xbb}}
	for _, m := range []Message{
		&Scout{Version: 9, What: MatchRouter | MatchPeer},
		&Scout{Version: 9, What: MatchPeer, ZID: z},
		&Hello{Version: 9, WhatAmI: Peer, ZID: z, Locators: []string{"tcp/1.2.3.4:5", "udp/[::1]:7447"}},
		&Hello{Version: 9, WhatAmI: Router, ZID: z},
	} {
		f.Add(encode(m))
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		m, err := DecodeScouting(b)
		if err != nil {
			return
		}
		e1 := encode(m)
		m2, err := DecodeScouting(e1)
		if err != nil {
			t.Fatalf("re-encoded %T % x does not decode: %v", m, e1, err)
		}
		if e2 := encode(m2); !bytes.Equal(e1, e2) {
			t.Fatalf("%T unstable:\n % x\n % x", m, e1, e2)
		}
	})
}

// stable checks that m re-encodes into bytes that decode to the same
// encoding.
func stable(t *testing.T, m Message, dec func(*codec.Reader) (Message, error)) {
	t.Helper()
	e1 := encode(m)
	r := codec.NewReader(e1)
	m2, err := dec(r)
	if err != nil {
		t.Fatalf("re-encoded %T % x does not decode: %v", m, e1, err)
	}
	if e2 := encode(m2); !bytes.Equal(e1, e2) {
		t.Fatalf("%T unstable:\n % x\n % x", m, e1, e2)
	}
}
