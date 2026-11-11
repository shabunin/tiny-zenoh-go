package xxh3

import (
	"encoding/binary"
	"testing"
)

func input(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte((i*31 + 7) % 251)
	}
	return b
}

// Vectors from xxhash-rust 0.8.19 (xxh3_64), every length branch.
var vectors = []struct {
	n    int
	want uint64
}{
	{0, 0x2d06800538d394c2},
	{1, 0x4c5cca45d0f4811f},
	{2, 0xa7e250c97710ff27},
	{3, 0x15f7093b173d005c},
	{4, 0xdca012f95811b6b9},
	{5, 0xb290cafc7b254345},
	{8, 0xdec6a9a43575982e},
	{9, 0x15e553b97e27735d},
	{15, 0x66026768a0fe1a85},
	{16, 0xa7683b861e585aa6},
	{17, 0x637c1aa907698945},
	{31, 0x9a20d1569e8ff426},
	{32, 0xfb0a38fb3af32306},
	{33, 0xdbd8ff66fdf7b97e},
	{63, 0x6555dc2e9918d3f3},
	{64, 0xa1688ef0a48a39d4},
	{65, 0x67dc5b5cae64c652},
	{96, 0xd1a35000054a80d5},
	{97, 0xd628cd723fed4570},
	{127, 0x207564f9ca6034f8},
	{128, 0x6d0f64c82ddaad27},
	{129, 0xeaf3fc97c05f44f3},
	{200, 0xf4b54cdc82f20685},
	{239, 0x1e645342febb13c1},
	{240, 0x22f28cbbfaf0447f},
	{241, 0x07525dbc14902c7f},
	{255, 0x3e64256196c9377b},
	{256, 0xa49b06aa88ab05e1},
	{500, 0x821bfabe31bf88ff},
	{1023, 0xfe4f1eaedb87b59b},
	{1024, 0xe2898655db7bc9ee},
	{1025, 0x134c652ba3d6fb9e},
	{2048, 0x63a78a59658d80f4},
	{4000, 0x9a8433b5aef4c537},
}

func TestHash(t *testing.T) {
	for _, v := range vectors {
		if got := Hash(input(v.n)); got != v.want {
			t.Errorf("len %d: %016x, want %016x", v.n, got, v.want)
		}
	}
}

// TestZenohFingerprints: zenoh's configuration and event fingerprints,
// hashed by Xxh3::default() updates in zenoh-plugin-storage-manager.
func TestZenohFingerprints(t *testing.T) {
	le := func(v uint64, n int) []byte { b := make([]byte, n); binary.LittleEndian.PutUint64(b, v); return b }
	cfg := Sum([]byte("demo/**"), []byte("demo"), le(1000, 16), le(5, 8), le(6, 8), le(30, 8), le(250, 16))
	if cfg != 0x02aa2c88cfcfcfe4 {
		t.Errorf("config fingerprint %016x", cfg)
	}
	id := make([]byte, 16)
	copy(id, []byte{0xab, 0xcd, 0x01})
	if ev := Sum([]byte("a/b"), le(0x0123456789abcdef, 8), id); ev != 0x67777d48e0079a2c {
		t.Errorf("event fingerprint %016x", ev)
	}
}
