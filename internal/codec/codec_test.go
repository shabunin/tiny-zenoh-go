package codec

import (
	"bytes"
	"testing"
)

func TestZIntVectors(t *testing.T) {
	// Worked examples from the Zenoh 1.0.0 primitives spec.
	cases := []struct {
		v   uint64
		enc []byte
	}{
		{0, []byte{0x00}},
		{127, []byte{0x7f}},
		{128, []byte{0x80, 0x01}},
		{300, []byte{0xac, 0x02}},
		{16383, []byte{0xff, 0x7f}},
		{16384, []byte{0x80, 0x80, 0x01}},
		{0xffffffff, []byte{0xff, 0xff, 0xff, 0xff, 0x0f}},
		// Zenoh's 9-byte tweak: 2^64-1 is nine 0xFF bytes, not ten.
		{0xffffffffffffffff, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
	}
	for _, c := range cases {
		w := NewWriter()
		w.WriteZInt(c.v)
		if !bytes.Equal(w.Bytes(), c.enc) {
			t.Errorf("WriteZInt(%d) = % x, want % x", c.v, w.Bytes(), c.enc)
		}
		got, err := NewReader(c.enc).ReadZInt()
		if err != nil {
			t.Errorf("ReadZInt(% x): %v", c.enc, err)
		}
		if got != c.v {
			t.Errorf("ReadZInt(% x) = %d, want %d", c.enc, got, c.v)
		}
	}
}

func TestZIntRoundTrip(t *testing.T) {
	vals := []uint64{1, 2, 63, 64, 65, 255, 256, 1 << 20, 1 << 35, 1 << 56, 1<<56 + 1, 1 << 63}
	w := NewWriter()
	for _, v := range vals {
		w.WriteZInt(v)
	}
	r := NewReader(w.Bytes())
	for _, want := range vals {
		got, err := r.ReadZInt()
		if err != nil {
			t.Fatalf("ReadZInt: %v", err)
		}
		if got != want {
			t.Errorf("round-trip = %d, want %d", got, want)
		}
	}
	if r.Remaining() != 0 {
		t.Errorf("Remaining = %d, want 0", r.Remaining())
	}
}

func TestZBytesAndString(t *testing.T) {
	w := NewWriter()
	w.WriteZBytes([]byte("hello"))
	w.WriteString("world!")
	w.WriteZBytes(nil)
	r := NewReader(w.Bytes())

	b, err := r.ReadZBytes()
	if err != nil || string(b) != "hello" {
		t.Fatalf("ReadZBytes = %q, %v", b, err)
	}
	s, err := r.ReadString()
	if err != nil || s != "world!" {
		t.Fatalf("ReadString = %q, %v", s, err)
	}
	empty, err := r.ReadZBytes()
	if err != nil || len(empty) != 0 {
		t.Fatalf("ReadZBytes(empty) = %q, %v", empty, err)
	}
}

func TestShortBuffer(t *testing.T) {
	if _, err := NewReader(nil).ReadByte(); err != ErrShortBuffer {
		t.Errorf("ReadByte on empty: want ErrShortBuffer")
	}
	// Length prefix says 5 bytes but only 2 present.
	if _, err := NewReader([]byte{0x05, 0x01, 0x02}).ReadZBytes(); err != ErrShortBuffer {
		t.Errorf("ReadZBytes truncated: %v, want ErrShortBuffer", err)
	}
}

func TestHeader(t *testing.T) {
	h := Header(0x1d, FlagZ|Flag1) // PUSH with Z and FL1
	if MsgID(h) != 0x1d {
		t.Errorf("MsgID = %#x, want 0x1d", MsgID(h))
	}
	if !HasZ(h) {
		t.Errorf("HasZ = false, want true")
	}
	if h&Flag1 == 0 {
		t.Errorf("Flag1 not set")
	}
	if h&Flag2 != 0 {
		t.Errorf("Flag2 set unexpectedly")
	}
}
