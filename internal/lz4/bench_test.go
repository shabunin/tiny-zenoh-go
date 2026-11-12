package lz4

import (
	"bytes"
	"testing"
)

// A batch of small, similar messages: what zenoh compresses.
func benchBatch() []byte {
	var b bytes.Buffer
	for i := 0; b.Len() < 60000; i++ {
		b.WriteString("\x3d\x00\x10demo/example/key\x01\x08temp=21.")
		b.WriteByte(byte('0' + i%10))
	}
	return b.Bytes()
}

func BenchmarkCompress(b *testing.B) {
	src := benchBatch()
	var c Compressor
	dst := make([]byte, 0, Bound(len(src)))
	b.SetBytes(int64(len(src)))
	b.ReportAllocs()
	for range b.N {
		dst = c.Compress(dst[:0], src)
	}
	b.ReportMetric(float64(len(dst))/float64(len(src)), "ratio")
}

func BenchmarkDecompress(b *testing.B) {
	src := benchBatch()
	var c Compressor
	z := c.Compress(nil, src)
	dst := make([]byte, 0, len(src))
	b.SetBytes(int64(len(src)))
	b.ReportAllocs()
	for range b.N {
		var err error
		if dst, err = Decompress(dst[:0], z, len(src)); err != nil {
			b.Fatal(err)
		}
	}
}
