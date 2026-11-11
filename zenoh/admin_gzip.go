//go:build !tinygo

package zenoh

import (
	"bytes"
	"compress/gzip"
)

func gzipBytes(b []byte) ([]byte, bool) {
	var buf bytes.Buffer
	w, err := gzip.NewWriterLevel(&buf, gzip.BestSpeed)
	if err != nil {
		return nil, false
	}
	if _, err := w.Write(b); err != nil || w.Close() != nil {
		return nil, false
	}
	return buf.Bytes(), true
}
