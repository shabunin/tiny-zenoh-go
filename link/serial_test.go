package link

import (
	"bytes"
	"io"
	"testing"
	"time"
)

// Vectors from zenoh-rust's z-serial (src/lib.rs tests).
func TestSerialFrameVectors(t *testing.T) {
	cases := []struct{ data, wire []byte }{
		{[]byte{0x00, 0x11, 0x00}, []byte{0x01, 0x02, 0x03, 0x01, 0x02, 0x11, 0x05, 0x73, 0xEC, 0x75, 0xF9, 0x00}},
		{[]byte{0x11, 0x22, 0x00, 0x33}, []byte{0x01, 0x02, 0x04, 0x03, 0x11, 0x22, 0x06, 0x33, 0x8D, 0x03, 0x6D, 0xFB, 0x00}},
	}
	for _, c := range cases {
		got := appendSerialFrame(nil, 0, c.data)
		if !bytes.Equal(got, c.wire) {
			t.Errorf("encode %x = %x, want %x", c.data, got, c.wire)
		}
		raw, err := cobsDecode(nil, c.wire[:len(c.wire)-1])
		if err != nil {
			t.Fatal(err)
		}
		flags, payload, err := parseSerialFrame(raw)
		if err != nil || flags != 0 || !bytes.Equal(payload, c.data) {
			t.Errorf("decode %x: %v %x", c.wire, err, payload)
		}
	}
}

func TestCOBSRoundTrip(t *testing.T) {
	for _, n := range []int{0, 1, 253, 254, 255, 256, 600, 1507} {
		for _, fill := range []byte{0, 1, 0xff} {
			src := bytes.Repeat([]byte{fill}, n)
			if n > 3 {
				src[n/2] = 0
			}
			enc := cobsEncode(nil, src)
			if bytes.IndexByte(enc, 0) >= 0 {
				t.Fatalf("n=%d fill=%d: zero in encoding", n, fill)
			}
			dec, err := cobsDecode(nil, enc)
			if err != nil || !bytes.Equal(dec, src) {
				t.Fatalf("n=%d fill=%d: round trip failed (%v)", n, fill, err)
			}
		}
	}
	if len(cobsEncode(nil, make([]byte, SerialMTU+serialOverhead))) > serialMaxFrame {
		t.Error("an MTU-sized frame exceeds the max COBS frame")
	}
}

type pipeRWC struct {
	io.Reader
	io.WriteCloser
}

func serialPair() (*Serial, *Serial) {
	ar, bw := io.Pipe()
	br, aw := io.Pipe()
	return NewSerial(pipeRWC{ar, aw}), NewSerial(pipeRWC{br, bw})
}

func TestSerialHandshakeAndData(t *testing.T) {
	a, b := serialPair()
	defer a.Close()
	defer b.Close()
	// Garbage before the handshake (like an ESP32 boot log) is skipped.
	go func() { _, _ = b.rw.Write([]byte("rst:0x1 (POWERON)\r\n\x00")); _ = b.Accept() }()
	if err := a.Connect(2 * time.Second); err != nil {
		t.Fatal(err)
	}
	msg := bytes.Repeat([]byte{0, 1, 2, 0xff}, 375)
	go func() { _, _ = a.Write(msg) }()
	buf := make([]byte, 2000)
	n, err := b.Read(buf)
	if err != nil || !bytes.Equal(buf[:n], msg) {
		t.Fatalf("read %d %v", n, err)
	}
	if _, err := a.Write(make([]byte, SerialMTU+1)); err == nil {
		t.Error("oversized write accepted")
	}
	// The initiator restarts: the responder answers R and drops the session.
	go func() { _ = a.writeFrame(serialInit, nil) }()
	if _, err := b.Read(buf); err != ErrSerialReset {
		t.Errorf("err = %v, want reset", err)
	}
	f, _ := a.next(nil)
	if f.flags != serialReset {
		t.Errorf("flags %x, want R", f.flags)
	}
}
