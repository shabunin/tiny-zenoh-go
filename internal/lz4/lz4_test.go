package lz4

import (
	"bytes"
	"math/rand/v2"
	"testing"
)

func roundTrip(t *testing.T, c *Compressor, src []byte) []byte {
	t.Helper()
	enc := c.Compress(nil, src)
	if len(enc) > Bound(len(src)) {
		t.Fatalf("%d bytes compressed to %d > bound %d", len(src), len(enc), Bound(len(src)))
	}
	dec, err := Decompress(nil, enc, len(src))
	if err != nil {
		t.Fatalf("decompress %d bytes: %v", len(src), err)
	}
	if !bytes.Equal(dec, src) {
		t.Fatalf("round trip of %d bytes differs", len(src))
	}
	if len(src) > 0 {
		if _, err := Decompress(nil, enc, len(src)-1); err == nil {
			t.Fatal("limit not enforced")
		}
	}
	return enc
}

func TestRoundTrip(t *testing.T) {
	var c Compressor
	r := rand.New(rand.NewPCG(1, 2))
	for _, n := range []int{0, 1, 4, 11, 12, 13, 16, 100, 1000, 4096, 65535} {
		rnd := make([]byte, n)
		for i := range rnd {
			rnd[i] = byte(r.Uint32())
		}
		roundTrip(t, &c, rnd)
		rep := bytes.Repeat([]byte("zenoh"), n/5+1)[:n]
		enc := roundTrip(t, &c, rep)
		if n >= 1000 && len(enc) > n/20 {
			t.Errorf("repetitive %d bytes only compressed to %d", n, len(enc))
		}
		zeros := make([]byte, n)
		roundTrip(t, &c, zeros)
		mixed := make([]byte, n)
		for i := range mixed {
			if r.IntN(4) == 0 {
				mixed[i] = byte(r.IntN(3))
			} else if i > 300 {
				mixed[i] = mixed[i-r.IntN(300)-1]
			}
		}
		roundTrip(t, &c, mixed)
	}
}

// A block produced by the reference implementation (lz4 -B4 --no-frame-crc,
// data "abcabcabcabcabcabcabcabcabcabc..."): one literal run, an overlapping
// match, and trailing literals.
func TestDecompressReference(t *testing.T) {
	// literals "abc", match offset 3 length 4+15+6=25, literals "xyzzy"
	block := []byte{0x3f, 'a', 'b', 'c', 3, 0, 6, 0x50, 'x', 'y', 'z', 'z', 'y'}
	want := append(bytes.Repeat([]byte("abc"), 10)[:28], "xyzzy"...)
	got, err := Decompress(nil, block, 100)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("got %q, %v", got, err)
	}
}

func TestDecompressCorrupt(t *testing.T) {
	for _, b := range [][]byte{
		{},
		{0x10},                  // literal run past the end
		{0x10, 'a', 1},          // truncated offset
		{0x10, 'a', 2, 0, 0x00}, // offset beyond output
		{0x10, 'a', 0, 0, 0x00}, // zero offset
		{0x1f, 'a', 1, 0},       // missing match length byte
	} {
		if _, err := Decompress(nil, b, 1000); err == nil {
			t.Errorf("%x decoded without error", b)
		}
	}
}

func FuzzDecompress(f *testing.F) {
	var c Compressor
	f.Add(c.Compress(nil, []byte("hello hello hello hello hello")))
	f.Fuzz(func(t *testing.T, b []byte) {
		out, err := Decompress(nil, b, 4096)
		if err == nil && len(out) > 4096 {
			t.Fatal("limit exceeded")
		}
	})
}
