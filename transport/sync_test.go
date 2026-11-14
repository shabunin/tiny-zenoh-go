package transport

import (
	"bytes"
	"testing"

	"github.com/shabunin/tiny-zenoh-go/protocol"
)

// TxSync is what TinyGo builds run by default (no queue goroutines): Send
// packs and writes the batches itself, in the order of the messages.

var syncCfg = Config{TxQueue: TxQueue{Mode: TxSync}}

func filled(n int, b byte) rawMsg {
	m := make(rawMsg, n)
	m[0] = 0x1d
	for i := 1; i < n; i++ {
		m[i] = b
	}
	return m
}

func TestSyncFrames(t *testing.T) {
	tr, f := setup(t, 256, 10000, syncCfg, Callbacks{})
	isn := f.clientISN
	next := func(reliable bool, sn uint64, payload ...rawMsg) {
		t.Helper()
		fr, ok := f.read().(*protocol.Frame)
		if !ok {
			t.Fatal("expected FRAME")
		}
		want := bytes.Join(func() [][]byte {
			var bs [][]byte
			for _, p := range payload {
				bs = append(bs, p)
			}
			return bs
		}(), nil)
		if fr.Reliable != reliable || fr.SN != sn&snMask(32) || !bytes.Equal(fr.Payload, want) {
			t.Fatalf("frame reliable %v sn %d payload %d bytes, want %v %d %d bytes", fr.Reliable, fr.SN, len(fr.Payload), reliable, sn, len(want))
		}
	}

	// Messages that fit share a frame.
	a, b := rawMsg{0x1d, 1}, rawMsg{0x1d, 2}
	if err := tr.Send(true, a, b); err != nil {
		t.Fatal(err)
	}
	next(true, isn, a, b)
	// Best effort is its own channel with its own SN.
	if err := tr.Send(false, rawMsg{0x1d, 3}); err != nil {
		t.Fatal(err)
	}
	next(false, isn, rawMsg{0x1d, 3})
	// A message that overflows the frame starts the next one.
	m1, m2 := filled(150, 0xa1), filled(150, 0xa2)
	if err := tr.Send(true, m1, m2); err != nil {
		t.Fatal(err)
	}
	next(true, isn+1, m1)
	next(true, isn+2, m2)
	// A small message, then one larger than a batch: a frame, then fragments
	// (more SNs), then what follows in a frame again.
	big := filled(700, 0xbb)
	small, after := rawMsg{0x1d, 4}, rawMsg{0x1d, 5}
	errc := make(chan error, 1)
	go func() { errc <- tr.Send(true, small, big, after) }()
	next(true, isn+3, small)
	var got []byte
	sn := isn + 4
	for {
		fr, ok := f.read().(*protocol.Fragment)
		if !ok {
			t.Fatal("expected FRAGMENT")
		}
		if fr.SN != sn&snMask(32) || !fr.Reliable {
			t.Fatalf("fragment sn %d reliable %v, want %d", fr.SN, fr.Reliable, sn)
		}
		sn++
		got = append(got, fr.Payload...)
		if !fr.More {
			break
		}
	}
	if !bytes.Equal(got, big) {
		t.Fatalf("fragments carry %d bytes, want the %d of the message", len(got), len(big))
	}
	next(true, sn, after)
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
	if f.maxBody > 254 {
		t.Errorf("%d-byte batch body over the 256 batch size", f.maxBody)
	}

	_ = f.conn.Close()
	_ = tr.Close()
	if err := tr.Send(true, a); err != ErrClosed {
		t.Fatalf("Send after Close: %v", err)
	}
}

// With QoS, a sync Send keeps the messages' order and starts a frame at
// each change of priority (each priority with its own SN).
func TestSyncQoSFrames(t *testing.T) {
	tr, f, _ := setupQoSCfg(t, []protocol.Extension{{ID: extQoS, Enc: protocol.ExtUnit}}, Callbacks{}, syncCfg)
	if !tr.QoS() {
		t.Fatal("QoS not negotiated")
	}
	err := tr.Send(true, pushPrio("d1", protocol.PriorityData), pushPrio("c1", protocol.PriorityControl),
		pushPrio("c2", protocol.PriorityControl), pushPrio("r1", protocol.PriorityRealTime),
		pushPrio("d2", protocol.PriorityData))
	if err != nil {
		t.Fatal(err)
	}
	isn := f.clientISN
	for i, w := range []struct {
		prio protocol.Priority
		sn   uint64
		n    int
	}{
		{protocol.PriorityData, isn, 1},
		{protocol.PriorityControl, isn, 2},
		{protocol.PriorityRealTime, isn, 1},
		{protocol.PriorityData, isn + 1, 1},
	} {
		fr := f.read().(*protocol.Frame)
		if p := framePrio(t, fr); p != w.prio || fr.SN != w.sn&snMask(32) {
			t.Errorf("frame %d: prio %d sn %d, want prio %d sn %d", i, p, fr.SN, w.prio, w.sn)
		}
		if n := countMsgs(t, fr.Payload); n != w.n {
			t.Errorf("frame %d: %d messages, want %d", i, n, w.n)
		}
	}
}
