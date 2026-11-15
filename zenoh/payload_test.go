package zenoh

import (
	"crypto/sha256"
	"encoding/hex"
)

func pattern(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte((i*7 + 3) % 251)
	}
	return b
}

func sum(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
