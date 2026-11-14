package protocol

import (
	"fmt"
	"testing"

	"github.com/shabunin/tiny-zenoh-go/internal/codec"
)

// TestDecodeSweep feeds the decoders every prefix of every corpus message
// and every single-bit (and whole-byte) corruption of it: what a remote
// node, a broken link or a bug on the other side can produce. The decoders
// must return errors, never panic, and whatever they still accept must
// re-encode stably. Unlike the fuzz targets' seeds, this reaches the error
// return after each field in the normal test run.
func TestDecodeSweep(t *testing.T) {
	type decoder func([]byte) (Message, error)
	stream := func(dec func(*codec.Reader) (Message, error)) decoder {
		return func(b []byte) (Message, error) { return dec(codec.NewReader(b)) }
	}
	sets := []struct {
		name string
		ms   []Message
		dec  decoder
	}{
		{"network", append(seedNetwork(), sweepNetwork()...), stream(DecodeNetwork)},
		{"transport", seedTransport(), stream(DecodeTransport)},
		{"scouting", sweepScouting(), DecodeScouting},
	}
	for _, set := range sets {
		t.Run(set.name, func(t *testing.T) {
			var prefixes, rejected, flips, accepted int
			for _, m := range set.ms {
				b := encode(m)
				if _, err := set.dec(b); err != nil {
					t.Fatalf("%T % x: %v", m, b, err)
				}
				try := func(what string, in []byte) bool {
					defer func() {
						if p := recover(); p != nil {
							t.Fatalf("%T %s: % x: panic %v", m, what, in, p)
						}
					}()
					got, err := set.dec(in)
					if err != nil {
						return false
					}
					e1 := encode(got)
					again, err := set.dec(e1)
					if err != nil {
						t.Fatalf("%T %s: accepted % x as %T whose encoding % x does not decode: %v", m, what, in, got, e1, err)
					}
					if e2 := encode(again); string(e1) != string(e2) {
						t.Fatalf("%T %s: unstable % x / % x", m, what, e1, e2)
					}
					return true
				}
				// Frame and Fragment payloads run to the end of the batch, so
				// a cut one is a shorter one: only their header can't be cut.
				tail, allowed := 0, -1
				switch m := m.(type) {
				case *Frame:
					tail = len(m.Payload)
				case *Fragment:
					tail = len(m.Payload)
				case *Init:
					// zenoh-pico 1.10 sends INIT ACKs flagged with extensions
					// but without any: accepted where the extensions start.
					c := *m
					c.Exts = nil
					allowed = len(encode(&c))
				}
				for i := range len(b) {
					prefixes++
					if !try(fmt.Sprintf("prefix %d", i), b[:i]) {
						rejected++
					} else if i < len(b)-tail && i != allowed {
						t.Errorf("%T: prefix % x of % x accepted", m, b[:i], b)
					}
				}
				in := make([]byte, len(b))
				for i := range b {
					for _, mask := range []byte{1, 2, 4, 8, 16, 32, 64, 128, 0xff} {
						copy(in, b)
						in[i] ^= mask
						flips++
						if try(fmt.Sprintf("byte %d ^ %#x", i, mask), in) {
							accepted++
						}
					}
				}
			}
			t.Logf("%d messages: %d/%d prefixes rejected; %d of %d corruptions still decode", len(set.ms), rejected, prefixes, accepted, flips)
		})
	}
}

// sweepNetwork adds the variants the fuzz seeds lack: reply bodies of
// every kind, shared-memory parts, every consolidation, declarations with
// their optional parts, interests with every option.
func sweepNetwork() []Message {
	zid := ZID{b: []byte{9, 8, 7}}
	ts := &Timestamp{Time: 1 << 40, ID: zid}
	src := &SourceInfo{ZID: zid, EID: 1, SN: 2}
	shm := []PayloadPart{{Shm: &ShmInfo{DataLen: 64, MetaID: 1, MetaIndex: 2, Generation: 3}}}
	var ms []Message
	for _, c := range []Consolidation{ConsolidationAuto, ConsolidationNone, ConsolidationMonotonic, ConsolidationLatest} {
		ms = append(ms,
			&Request{ID: 3, WireExpr: WireExpr{Suffix: "c"}, Query: &Query{Consolidation: c}},
			&Response{RequestID: 3, WireExpr: WireExpr{Suffix: "c"}, Body: &Reply{Consolidation: c,
				Body: &Put{Timestamp: ts, SourceInfo: src, Encoding: Encoding{ID: 7, Schema: []byte("x")}, Attachment: []byte("a"), Parts: shm}}})
	}
	ms = append(ms,
		&Response{RequestID: 4, WireExpr: WireExpr{Scope: 2, Suffix: "/r", Mapping: true}, QoS: QoSRequest, Timestamp: ts,
			Responder: &EntityGlobalID{ZID: zid, EID: 1},
			Body:      &Reply{Body: &Del{Timestamp: ts, SourceInfo: src, Attachment: []byte("d")}}},
		&Response{RequestID: 5, WireExpr: WireExpr{Suffix: "e"},
			Body: &Err{Encoding: Encoding{ID: 3, Schema: []byte("s")}, SourceInfo: src, Parts: shm}},
		&ResponseFinal{RequestID: 6},
		&Push{WireExpr: WireExpr{Suffix: "p"}, Body: &Put{Payload: []byte{}}},
		&Interest{ID: 7, Mode: InterestFuture, Options: InterestKeyExprs | InterestSubscribers | InterestQueryables | InterestTokens | InterestAggregate,
			WireExpr: &WireExpr{Scope: 4, Suffix: "/x", Mapping: true}, QoS: QoSDeclare, NodeID: 3},
		&OAM{ID: 3, Enc: ExtUnit},
	)
	for _, d := range []Declaration{
		&DeclareKeyExpr{ID: 4, WireExpr: WireExpr{Scope: 1, Suffix: "/k", Mapping: true}},
		&UndeclareSubscriber{ID: 10},
		&UndeclareQueryable{ID: 11},
		&UndeclareToken{ID: 13, WireExpr: WireExpr{Scope: 2, Suffix: "/t", Mapping: true}},
		&DeclareQueryable{ID: 12, WireExpr: WireExpr{Suffix: "q"}},
		&DeclareToken{ID: 14, WireExpr: WireExpr{Scope: 1}},
	} {
		ms = append(ms, &Declare{Body: d})
	}
	return ms
}

func sweepScouting() []Message {
	z := ZID{b: []byte{0xaa, 0xbb}}
	return []Message{
		&Scout{Version: ProtocolVersion, What: MatchRouter | MatchPeer | MatchClient},
		&Scout{Version: ProtocolVersion, What: MatchPeer, ZID: z},
		&Hello{Version: ProtocolVersion, WhatAmI: Peer, ZID: z, Locators: []string{"tcp/1.2.3.4:5", "udp/[::1]:7447"}},
		&Hello{Version: ProtocolVersion, WhatAmI: Client, ZID: z},
	}
}
