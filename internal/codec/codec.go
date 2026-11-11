// Package codec implements the low-level Zenoh wire primitives: variable-length
// integers (VLE, a.k.a. "zint"), length-prefixed byte slices and strings, the
// common message-header byte, and extension headers, as defined by the Zenoh
// 1.0.0 protocol specification.
//
//	https://spec.zenoh.io/spec/1.0.0/wire/primitives.html
//
// The package is dependency-free (only the standard "errors" package) and
// TinyGo-compatible: no reflection, no encoding/*, no fmt in the hot path.
package codec

import "errors"

// Errors returned by Reader.
var (
	// ErrShortBuffer means a read ran past the end of the buffer.
	ErrShortBuffer = errors.New("gozenoh/codec: buffer underrun")
	// ErrVarintTooLong means a VLE value did not terminate within 9 bytes.
	ErrVarintTooLong = errors.New("gozenoh/codec: varint too long")
)

// Common message-header byte layout (primitives §"Message Header Byte"):
//
//	bits 0:4  message ID
//	bit  5    FL1 (message-specific flag)
//	bit  6    FL2 (message-specific flag)
//	bit  7    Z   (extensions follow)
const (
	// FlagZ is bit 7 of a message header: when set, extension blocks follow
	// the message's base fields.
	FlagZ byte = 1 << 7
	// Flag1 is bit 5, the first message-specific flag.
	Flag1 byte = 1 << 5
	// Flag2 is bit 6, the second message-specific flag.
	Flag2 byte = 1 << 6

	idMask byte = 0x1f
)

// MsgID returns the 5-bit message ID encoded in a header byte.
func MsgID(h byte) byte { return h & idMask }

// HasZ reports whether the extensions (Z) flag is set in a header byte.
func HasZ(h byte) bool { return h&FlagZ != 0 }

// Header builds a header byte from a 5-bit message ID and a set of flag bits
// (any of Flag1, Flag2, FlagZ OR'd together).
func Header(id byte, flags byte) byte { return (id & idMask) | flags }

// -----------------------------------------------------------------------------
// Writer
// -----------------------------------------------------------------------------

// Writer is an append-only byte buffer used to build wire messages.
// The zero value is ready to use; NewWriter is provided for symmetry.
type Writer struct {
	buf []byte
}

// NewWriter returns an empty Writer.
func NewWriter() *Writer { return &Writer{} }

// Wrap returns a Writer that appends to (and may grow) the given slice.
func Wrap(b []byte) *Writer { return &Writer{buf: b} }

// Bytes returns the bytes written so far. The slice aliases the Writer's
// internal buffer; copy it if you need to retain it across further writes.
func (w *Writer) Bytes() []byte { return w.buf }

// Len returns the number of bytes written so far.
func (w *Writer) Len() int { return len(w.buf) }

// Reset truncates the buffer to zero length, retaining capacity.
func (w *Writer) Reset() { w.buf = w.buf[:0] }

// Truncate discards all but the first n bytes. It panics if n is out of range.
func (w *Writer) Truncate(n int) { w.buf = w.buf[:n] }

// WriteByte appends a single byte.
func (w *Writer) WriteByte(b byte) error { w.buf = append(w.buf, b); return nil }

// WriteRaw appends raw bytes with no length prefix.
func (w *Writer) WriteRaw(p []byte) { w.buf = append(w.buf, p...) }

// WriteZInt writes v using Zenoh's VLE (variable-length encoding).
//
// This is little-endian base-128 (LEB128) with one Zenoh-specific tweak: the
// 9th byte, when reached, carries a full 8 value bits with no continuation bit.
// A uint64 therefore never needs more than 9 bytes (textbook LEB128 would use
// 10). Worked examples from the spec:
//
//	0        -> 0x00
//	127      -> 0x7F
//	128      -> 0x80 0x01
//	300      -> 0xAC 0x02
//	2^32-1   -> 0xFF 0xFF 0xFF 0xFF 0x0F
//	2^64-1   -> 0xFF x9
func (w *Writer) WriteZInt(v uint64) {
	for range 8 {
		if v < 0x80 {
			w.buf = append(w.buf, byte(v))
			return
		}
		w.buf = append(w.buf, byte(v)|0x80)
		v >>= 7
	}
	// 9th byte carries the remaining (at most 8) bits in full.
	w.buf = append(w.buf, byte(v))
}

// WriteZBytes writes a byte slice prefixed by its length as a VLE integer.
func (w *Writer) WriteZBytes(p []byte) {
	w.WriteZInt(uint64(len(p)))
	w.buf = append(w.buf, p...)
}

// WriteString writes a string prefixed by its byte length as a VLE integer.
func (w *Writer) WriteString(s string) {
	w.WriteZInt(uint64(len(s)))
	w.buf = append(w.buf, s...)
}

// -----------------------------------------------------------------------------
// Reader
// -----------------------------------------------------------------------------

// Reader consumes a byte slice. It does not copy; slices returned by ReadZBytes
// and ReadRaw alias the underlying buffer.
type Reader struct {
	buf []byte
	pos int
}

// NewReader returns a Reader over b.
func NewReader(b []byte) *Reader { return &Reader{buf: b} }

// Remaining returns the number of unread bytes.
func (r *Reader) Remaining() int { return len(r.buf) - r.pos }

// Pos returns the current read offset.
func (r *Reader) Pos() int { return r.pos }

// ReadByte reads a single byte.
func (r *Reader) ReadByte() (byte, error) {
	if r.pos >= len(r.buf) {
		return 0, ErrShortBuffer
	}
	b := r.buf[r.pos]
	r.pos++
	return b, nil
}

// PeekByte returns the next byte without consuming it.
func (r *Reader) PeekByte() (byte, error) {
	if r.pos >= len(r.buf) {
		return 0, ErrShortBuffer
	}
	return r.buf[r.pos], nil
}

// ReadRaw reads exactly n bytes, returning a sub-slice that aliases the buffer.
func (r *Reader) ReadRaw(n int) ([]byte, error) {
	if n < 0 || r.pos+n > len(r.buf) {
		return nil, ErrShortBuffer
	}
	p := r.buf[r.pos : r.pos+n]
	r.pos += n
	return p, nil
}

// ReadZInt reads a VLE-encoded unsigned integer (see WriteZInt).
func (r *Reader) ReadZInt() (uint64, error) {
	var v uint64
	var shift uint
	for i := range 9 {
		b, err := r.ReadByte()
		if err != nil {
			return 0, err
		}
		if i == 8 {
			// 9th byte: full 8 value bits, no continuation.
			v |= uint64(b) << shift
			return v, nil
		}
		v |= uint64(b&0x7f) << shift
		if b&0x80 == 0 {
			return v, nil
		}
		shift += 7
	}
	return 0, ErrVarintTooLong
}

// ReadZBytes reads a VLE-length-prefixed byte slice. The returned slice aliases
// the underlying buffer.
func (r *Reader) ReadZBytes() ([]byte, error) {
	n, err := r.ReadZInt()
	if err != nil {
		return nil, err
	}
	return r.ReadRaw(int(n))
}

// ReadString reads a VLE-length-prefixed UTF-8 string.
func (r *Reader) ReadString() (string, error) {
	b, err := r.ReadZBytes()
	if err != nil {
		return "", err
	}
	return string(b), nil
}
