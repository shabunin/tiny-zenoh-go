package protocol

import (
	"testing"

	"github.com/shabunin/tiny-zenoh-go/internal/codec"
)

func benchMessages() map[string]Message {
	zid := ZID{b: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}}
	ts := &Timestamp{Time: 0x1234567890, ID: zid}
	return map[string]Message{
		"Push8B":   &Push{WireExpr: WireExpr{Scope: 1}, Body: &Put{Payload: make([]byte, 8)}},
		"Push1KiB": &Push{WireExpr: WireExpr{Suffix: "demo/example/key"}, Body: &Put{Timestamp: ts, Encoding: Encoding{ID: 1}, Payload: make([]byte, 1024)}},
		"Declare":  &Declare{QoS: QoSDeclare, Body: &DeclareSubscriber{ID: 7, WireExpr: WireExpr{Suffix: "demo/**"}}},
		"Request": &Request{ID: 42, WireExpr: WireExpr{Suffix: "q/**"}, Target: TargetAll,
			Query: &Query{Consolidation: ConsolidationLatest, Parameters: "a=1;b=2"}},
	}
}

func BenchmarkEncode(b *testing.B) {
	for name, m := range benchMessages() {
		b.Run(name, func(b *testing.B) {
			var w codec.Writer
			b.ReportAllocs()
			for range b.N {
				w.Reset()
				m.Encode(&w)
			}
			b.SetBytes(int64(w.Len()))
		})
	}
}

func BenchmarkDecode(b *testing.B) {
	for name, m := range benchMessages() {
		b.Run(name, func(b *testing.B) {
			var w codec.Writer
			m.Encode(&w)
			buf := w.Bytes()
			b.SetBytes(int64(len(buf)))
			b.ReportAllocs()
			for range b.N {
				if _, err := DecodeNetwork(codec.NewReader(buf)); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
