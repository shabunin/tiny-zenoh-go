package transport

import (
	"testing"
	"time"

	"github.com/shabunin/tiny-zenoh-go/protocol"
)

// Both ends of a link count the same bytes and messages; a fragmented
// message counts once as a network message.
func TestLinkStats(t *testing.T) {
	za, _ := protocol.NewZID([]byte{1, 2, 3})
	zb, _ := protocol.NewZID([]byte{4, 5})
	got := make(chan protocol.Message, 4)
	o, a, oerr, aerr := acceptPair(t,
		Config{ZID: za, WhatAmI: protocol.Peer, BatchSize: 1024, QoS: true, Stats: true},
		Config{ZID: zb, WhatAmI: protocol.Peer, BatchSize: 1024, QoS: true, Stats: true},
		Callbacks{},
		Callbacks{OnMessage: func(m protocol.Message, _ bool) error { got <- m; return nil }})
	if oerr != nil || aerr != nil {
		t.Fatalf("open=%v accept=%v", oerr, aerr)
	}
	defer o.Close()
	defer a.Close()
	big := &protocol.Push{WireExpr: protocol.WireExpr{Suffix: "a/b"}, QoS: protocol.NewQoS(protocol.PriorityRealTime, false, false),
		Body: &protocol.Put{Payload: make([]byte, 5000)}}
	del := &protocol.Push{WireExpr: protocol.WireExpr{Suffix: "a/b"}, QoS: protocol.NewQoS(protocol.PriorityData, false, false), Body: &protocol.Del{}}
	if err := o.Send(true, big, del); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		select {
		case <-got:
		case <-time.After(2 * time.Second):
			t.Fatal("no message")
		}
	}
	oc, ac := o.LinkInfos()[0].Stats.Snapshot(), a.LinkInfos()[0].Stats.Snapshot()
	if oc.Bytes[Tx] == 0 || oc.Bytes[Tx] != ac.Bytes[Rx] || oc.TransportMessages[Tx] != ac.TransportMessages[Rx] {
		t.Errorf("tx %+v / rx %+v", oc, ac)
	}
	if oc.TransportMessages[Tx] < 6 { // 5000 bytes in 1024-byte batches, then a frame
		t.Errorf("transport messages %d", oc.TransportMessages[Tx])
	}
	for _, c := range []*LinkCounts{&oc, &ac} {
		d := Tx
		if c == &ac {
			d = Rx
		}
		if c.Network(d, protocol.PriorityRealTime, KindPut, 0) != 1 || c.Network(d, protocol.PriorityData, KindDel, 0) != 1 ||
			c.NetworkTotal(d, -1) != 2 {
			t.Errorf("dir %d: network %v", d, c.NetworkMessages)
		}
	}
	if (&LinkCounts{}).NetworkTotal(Tx, -1) != 0 || (*LinkStats)(nil).Snapshot().Bytes != [2]uint64{} {
		t.Error("zero counts")
	}
}
