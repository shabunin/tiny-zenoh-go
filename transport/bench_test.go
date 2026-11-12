package transport

import (
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shabunin/tiny-zenoh-go/protocol"
)

// BenchmarkUnicast: one transport over loopback TCP, messages of several
// sizes (64 KiB ones are fragmented), with and without compression.
func BenchmarkUnicast(b *testing.B) {
	for _, compress := range []bool{false, true} {
		for _, size := range []int{8, 1024, 64 << 10} {
			name := fmt.Sprintf("%dB", size)
			if compress {
				name += "/lz4"
			}
			b.Run(name, func(b *testing.B) { benchUnicast(b, size, compress) })
		}
	}
}

func benchUnicast(b *testing.B, size int, compress bool) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	defer ln.Close()
	za, _ := protocol.NewZID([]byte{1})
	zb, _ := protocol.NewZID([]byte{2})
	var got atomic.Int64
	want := int64(b.N)
	done := make(chan struct{})
	acc := make(chan *Unicast, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			acc <- nil
			return
		}
		u, err := Accept(c, Config{ZID: zb, WhatAmI: protocol.Peer, Compression: compress}, Callbacks{
			OnMessage: func(protocol.Message, bool) error {
				if got.Add(1) == want {
					close(done)
				}
				return nil
			}})
		if err != nil {
			u = nil
		}
		acc <- u
	}()
	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		b.Fatal(err)
	}
	o, err := Open(c, Config{ZID: za, WhatAmI: protocol.Peer, Compression: compress}, Callbacks{})
	if err != nil {
		b.Fatal(err)
	}
	defer o.Close()
	a := <-acc
	if a == nil {
		b.Fatal("accept failed")
	}
	defer a.Close()
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte(i % 7) // compressible
	}
	msg := &protocol.Push{WireExpr: protocol.WireExpr{Scope: 1}, Body: &protocol.Put{Payload: payload},
		QoS: protocol.NewQoS(protocol.PriorityData, true, false)}
	b.SetBytes(int64(size))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := o.Send(true, msg); err != nil {
			b.Fatal(err)
		}
	}
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		b.Fatalf("received %d of %d", got.Load(), b.N)
	}
	b.StopTimer()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "msg/s")
}
