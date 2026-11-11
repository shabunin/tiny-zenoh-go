package transport

import (
	"bytes"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/shabunin/tiny-zenoh-go/protocol"
)

func TestCompressionPair(t *testing.T) {
	for _, c := range []struct{ o, a, want bool }{{true, true, true}, {true, false, false}, {false, true, false}} {
		got := make(chan []byte, 8)
		o, a, oerr, aerr := acceptPair(t,
			Config{ZID: testZID(t), WhatAmI: protocol.Peer, Compression: c.o, QoS: true, BatchSize: 2048},
			Config{ZID: testZID(t), WhatAmI: protocol.Peer, Compression: c.a, QoS: true},
			Callbacks{},
			Callbacks{OnMessage: func(m protocol.Message, _ bool) error {
				got <- m.(*protocol.Push).Body.(*protocol.Put).Payload
				return nil
			}})
		if oerr != nil || aerr != nil {
			t.Fatalf("open=%v accept=%v", oerr, aerr)
		}
		if o.Compression() != c.want || a.Compression() != c.want {
			t.Errorf("%+v: compression o=%v a=%v", c, o.Compression(), a.Compression())
		}
		text := bytes.Repeat([]byte("compress me please "), 600) // 11400 bytes: fragmented
		noise := make([]byte, 5000)
		for i := range noise {
			noise[i] = byte(rand.Uint32())
		}
		small := []byte("hi")
		for _, p := range [][]byte{text, noise, small} {
			push := &protocol.Push{WireExpr: protocol.WireExpr{Suffix: "k"}, Body: &protocol.Put{Payload: p}}
			if err := o.Send(true, push); err != nil {
				t.Fatal(err)
			}
			select {
			case b := <-got:
				if !bytes.Equal(b, p) {
					t.Fatalf("%+v: payload of %d bytes corrupted", c, len(p))
				}
			case <-time.After(2 * time.Second):
				t.Fatalf("%+v: missing payload", c)
			}
		}
		tx, _ := o.CompressedBatches()
		_, rx := a.CompressedBatches()
		if c.want && (tx == 0 || tx != rx) {
			t.Errorf("compressed batches tx=%d rx=%d", tx, rx)
		}
		if !c.want && tx+rx != 0 {
			t.Errorf("compressed without negotiation")
		}
		// Keep-alives and CLOSE go through the header path too.
		_ = o.Close()
		select {
		case <-a.Done():
		case <-time.After(2 * time.Second):
			t.Fatal("peer did not see CLOSE")
		}
	}
}
