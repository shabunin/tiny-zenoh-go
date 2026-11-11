// Package lz4 implements the LZ4 block format (no frame, no size prefix),
// which zenoh uses to compress transport batches (zenoh-rust: lz4_flex::block).
//
// The compressor is a small greedy one meant for batches of at most 64 KiB:
// its hash table holds 16-bit positions (8 KiB) and is reused between calls.
package lz4

import (
	"encoding/binary"
	"errors"
)

// Errors returned by Decompress.
var (
	ErrCorrupt  = errors.New("lz4: corrupt block")
	ErrTooLarge = errors.New("lz4: decompressed block exceeds the limit")
)

const (
	minMatch     = 4
	lastLiterals = 5  // the last 5 bytes of a block are always literals
	mfLimit      = 12 // the last match starts at least 12 bytes before the end
	hashLog      = 12
	maxInput     = 0xffff // positions are 16-bit
)

// Compressor holds the match-finder state. The zero value is ready to use; it
// is not safe for concurrent use.
type Compressor struct {
	table [1 << hashLog]uint16
}

// Bound returns the largest compressed size of n input bytes.
func Bound(n int) int { return n + n/255 + 16 }

func hash(v uint32) uint32 { return (v * 2654435761) >> (32 - hashLog) }

// Compress appends the LZ4 block encoding of src (at most 65535 bytes) to dst.
func (c *Compressor) Compress(dst, src []byte) []byte {
	n := len(src)
	if n > maxInput {
		panic("lz4: input larger than 64 KiB")
	}
	anchor := 0
	if n >= mfLimit+1 {
		matchLimit := n - lastLiterals
		for i := 0; i <= n-mfLimit; {
			seq := binary.LittleEndian.Uint32(src[i:])
			h := hash(seq)
			ref := int(c.table[h])
			c.table[h] = uint16(i)
			// Stale entries (from earlier calls) are harmless: the bytes are
			// compared.
			if ref >= i || binary.LittleEndian.Uint32(src[ref:]) != seq {
				i++
				continue
			}
			// Extend backwards over pending literals, then forwards.
			for i > anchor && ref > 0 && src[i-1] == src[ref-1] {
				i--
				ref--
			}
			ml := minMatch
			for i+ml < matchLimit && src[ref+ml] == src[i+ml] {
				ml++
			}
			dst = appendSequence(dst, src[anchor:i], i-ref, ml)
			i += ml
			anchor = i
			if i <= n-mfLimit && i >= 2 {
				p := i - 2
				c.table[hash(binary.LittleEndian.Uint32(src[p:]))] = uint16(p)
			}
		}
	}
	// Last literals.
	lit := src[anchor:]
	dst = appendLen(append(dst, byte(min(len(lit), 15))<<4), len(lit))
	return append(dst, lit...)
}

func appendSequence(dst, lit []byte, offset, ml int) []byte {
	m := ml - minMatch
	dst = append(dst, byte(min(len(lit), 15))<<4|byte(min(m, 15)))
	dst = appendLen(dst, len(lit))
	dst = append(dst, lit...)
	dst = binary.LittleEndian.AppendUint16(dst, uint16(offset))
	return appendLen(dst, m)
}

// appendLen writes the extension bytes of a 4-bit length field holding n.
func appendLen(dst []byte, n int) []byte {
	if n < 15 {
		return dst
	}
	n -= 15
	for n >= 255 {
		dst = append(dst, 255)
		n -= 255
	}
	return append(dst, byte(n))
}

// Decompress appends the decoding of the block src to dst; the decoded block
// may be at most limit bytes.
func Decompress(dst, src []byte, limit int) ([]byte, error) {
	base := len(dst)
	readLen := func(i *int, n int) (int, error) {
		if n != 15 {
			return n, nil
		}
		for {
			if *i >= len(src) {
				return 0, ErrCorrupt
			}
			b := src[*i]
			*i++
			n += int(b)
			if n > limit {
				return 0, ErrTooLarge
			}
			if b != 255 {
				return n, nil
			}
		}
	}
	for i := 0; ; {
		if i >= len(src) {
			return nil, ErrCorrupt
		}
		token := src[i]
		i++
		ll, err := readLen(&i, int(token>>4))
		if err != nil {
			return nil, err
		}
		if ll > len(src)-i {
			return nil, ErrCorrupt
		}
		if len(dst)-base+ll > limit {
			return nil, ErrTooLarge
		}
		dst = append(dst, src[i:i+ll]...)
		i += ll
		if i == len(src) {
			return dst, nil // the last sequence has no match
		}
		if len(src)-i < 2 {
			return nil, ErrCorrupt
		}
		off := int(binary.LittleEndian.Uint16(src[i:]))
		i += 2
		if off == 0 || off > len(dst)-base {
			return nil, ErrCorrupt
		}
		ml, err := readLen(&i, int(token&15))
		if err != nil {
			return nil, err
		}
		ml += minMatch
		if len(dst)-base+ml > limit {
			return nil, ErrTooLarge
		}
		start := len(dst) - off
		if off >= ml {
			dst = append(dst, dst[start:start+ml]...)
		} else {
			for k := range ml { // overlapping copy
				dst = append(dst, dst[start+k])
			}
		}
	}
}
