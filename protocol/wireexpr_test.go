package protocol

import (
	"bytes"
	"testing"

	"github.com/shabunin/tiny-zenoh-go/internal/codec"
)

func TestWireExprRoundTrip(t *testing.T) {
	cases := []WireExpr{
		{Scope: 0, Suffix: "demo/example/**"},     // full keyexpr, no mapping
		{Scope: 42},                               // pure declared id
		{Scope: 7, Suffix: "/sub", Mapping: true}, // prefix + suffix, sender's space
	}
	for _, we := range cases {
		w := codec.NewWriter()
		we.Encode(w)
		f := we.Flags()
		got, err := DecodeWireExpr(codec.NewReader(w.Bytes()), f&flagWireN != 0, f&flagWireM != 0)
		if err != nil {
			t.Fatalf("DecodeWireExpr(%+v): %v", we, err)
		}
		if got != we {
			t.Errorf("round-trip = %+v, want %+v", got, we)
		}
	}
}

func TestWireExprBytes(t *testing.T) {
	we := WireExpr{Scope: 0, Suffix: "a/b"}
	w := codec.NewWriter()
	we.Encode(w)
	want := []byte{0x00, 0x03, 'a', '/', 'b'}
	if !bytes.Equal(w.Bytes(), want) {
		t.Errorf("WireExpr bytes = % x, want % x", w.Bytes(), want)
	}
	if we.Flags() != flagWireN {
		t.Errorf("Flags = %#x, want N only", we.Flags())
	}
}
