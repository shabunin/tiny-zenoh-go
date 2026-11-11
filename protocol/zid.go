package protocol

import (
	"errors"

	"github.com/shabunin/tiny-zenoh-go/internal/codec"
)

// ErrZIDLength is returned when a ZID is not between 1 and 16 bytes, or is all
// zeros.
var ErrZIDLength = errors.New("protocol: ZID must be a non-zero 1..16 byte value")

// ZID is a Zenoh node identifier. zenoh-rust models it as a non-zero u128
// stored little-endian on the wire using only its significant bytes (1..16).
// String renders it as lower-case hex without leading zeros, exactly like
// zenohd prints it.
type ZID struct {
	b []byte
}

// NewZID copies b (little-endian, 1..16 bytes) into a ZID. Trailing zero bytes
// (the most-significant end) are trimmed so the ZID uses the same significant
// size zenoh-rust would; an all-zero value is rejected.
func NewZID(b []byte) (ZID, error) {
	if len(b) < 1 || len(b) > 16 {
		return ZID{}, ErrZIDLength
	}
	n := len(b)
	for n > 0 && b[n-1] == 0 {
		n--
	}
	if n == 0 {
		return ZID{}, ErrZIDLength
	}
	cp := make([]byte, n)
	copy(cp, b[:n])
	return ZID{cp}, nil
}

// ParseZID parses the hex form produced by String.
func ParseZID(s string) (ZID, error) {
	if len(s) == 0 || len(s) > 32 {
		return ZID{}, ErrZIDLength
	}
	if len(s)%2 == 1 {
		s = "0" + s
	}
	b := make([]byte, len(s)/2)
	for i := range b {
		hi, lo := unhex(s[2*i]), unhex(s[2*i+1])
		if hi < 0 || lo < 0 {
			return ZID{}, errors.New("protocol: invalid ZID hex")
		}
		b[len(b)-1-i] = byte(hi<<4 | lo)
	}
	return NewZID(b)
}

func unhex(c byte) int {
	switch {
	case c >= '0' && c <= '9':
		return int(c - '0')
	case c >= 'a' && c <= 'f':
		return int(c-'a') + 10
	case c >= 'A' && c <= 'F':
		return int(c-'A') + 10
	}
	return -1
}

// Bytes returns the ZID's wire bytes (little-endian). Do not mutate.
func (z ZID) Bytes() []byte { return z.b }

// Len returns the ZID length in bytes (1..16), or 0 for the zero value.
func (z ZID) Len() int { return len(z.b) }

// IsZero reports whether z is unset.
func (z ZID) IsZero() bool { return len(z.b) == 0 }

const hexdigits = "0123456789abcdef"

// String renders the ZID as zenohd does: hex of the u128 value, no leading zeros.
func (z ZID) String() string {
	if len(z.b) == 0 {
		return ""
	}
	out := make([]byte, 0, len(z.b)*2)
	for i := len(z.b) - 1; i >= 0; i-- {
		out = append(out, hexdigits[z.b[i]>>4], hexdigits[z.b[i]&0x0f])
	}
	if out[0] == '0' {
		out = out[1:]
	}
	return string(out)
}

// Equal reports whether two ZIDs are the same value.
func (z ZID) Equal(o ZID) bool { return string(z.b) == string(o.b) }

// writeZIDNibble writes the compact ZID form used by INIT, JOIN, SourceInfo and
// EntityGlobalId: one byte whose high nibble is len-1 and whose low nibble is
// caller-defined (WhatAmI for INIT, zero elsewhere), then the raw ZID bytes.
func writeZIDNibble(w *codec.Writer, z ZID, low byte) {
	_ = w.WriteByte(byte((z.Len()-1)<<4) | (low & 0x0f))
	w.WriteRaw(z.b)
}

// readZIDNibble reads the compact ZID form, returning the low nibble too.
func readZIDNibble(r *codec.Reader) (ZID, byte, error) {
	p, err := r.ReadByte()
	if err != nil {
		return ZID{}, 0, err
	}
	raw, err := r.ReadRaw(int(p>>4) + 1)
	if err != nil {
		return ZID{}, 0, err
	}
	z, err := NewZID(raw)
	return z, p & 0x0f, err
}

func writePackedZID(w *codec.Writer, z ZID, wai WhatAmI) {
	writeZIDNibble(w, z, byte(wai)&0x03)
}

func readPackedZID(r *codec.Reader) (ZID, WhatAmI, error) {
	z, low, err := readZIDNibble(r)
	return z, WhatAmI(low & 0x03), err
}
