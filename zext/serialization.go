// Package zext implements zenoh-ext's serialization format (zenoh-ext
// serialization.rs, RFC "Serialization"), the one z_serialize /
// z_deserialize use in every zenoh binding, so that typed payloads can be
// exchanged with Rust, Python, C and other zenoh applications.
//
// The format has no type tags: both sides must agree on the types.
//
//   - Integers and floats are fixed-width little-endian (i8..i64, u8..u64,
//     i128/u128, f32, f64); bool is one byte, 0 or 1.
//   - Sequences (strings, byte slices, slices, arrays, sets) are a LEB128
//     length followed by their elements; maps are a LEB128 length followed
//     by key/value pairs.
//   - A tuple (or struct) is its fields one after the other, so writing
//     several values in a row is the same as writing a tuple of them.
//
// Go's int and uint have no zenoh counterpart (their size varies) and are
// refused by Serialize; use a sized type, or WriteVarInt for zenoh's
// VarInt<usize>.
//
// The package uses no reflection and works with TinyGo.
package zext

import (
	"encoding/binary"
	"errors"
	"math"
	"unicode/utf8"
)

// ErrDeserialize is returned when data doesn't decode as the requested
// types (too short, too long, invalid bool or UTF-8, bad length).
var ErrDeserialize = errors.New("zext: deserialization error")

// Serializer appends values in zenoh's serialization format.
type Serializer struct {
	buf []byte
}

// NewSerializer returns an empty serializer.
func NewSerializer() *Serializer { return &Serializer{} }

// Bytes returns the serialized data.
func (s *Serializer) Bytes() []byte { return s.buf }

// Reset empties the serializer, keeping its buffer.
func (s *Serializer) Reset() { s.buf = s.buf[:0] }

func (s *Serializer) WriteUint8(v uint8)   { s.buf = append(s.buf, v) }
func (s *Serializer) WriteUint16(v uint16) { s.buf = binary.LittleEndian.AppendUint16(s.buf, v) }
func (s *Serializer) WriteUint32(v uint32) { s.buf = binary.LittleEndian.AppendUint32(s.buf, v) }
func (s *Serializer) WriteUint64(v uint64) { s.buf = binary.LittleEndian.AppendUint64(s.buf, v) }
func (s *Serializer) WriteInt8(v int8)     { s.WriteUint8(uint8(v)) }
func (s *Serializer) WriteInt16(v int16)   { s.WriteUint16(uint16(v)) }
func (s *Serializer) WriteInt32(v int32)   { s.WriteUint32(uint32(v)) }
func (s *Serializer) WriteInt64(v int64)   { s.WriteUint64(uint64(v)) }

// WriteUint128 writes a u128 (or, with the bits of a two's complement
// value, an i128) given as its high and low 64 bits.
func (s *Serializer) WriteUint128(hi, lo uint64) {
	s.WriteUint64(lo)
	s.WriteUint64(hi)
}

func (s *Serializer) WriteFloat32(v float32) { s.WriteUint32(math.Float32bits(v)) }
func (s *Serializer) WriteFloat64(v float64) { s.WriteUint64(math.Float64bits(v)) }

// WriteBool writes a bool as one byte.
func (s *Serializer) WriteBool(v bool) {
	var b uint8
	if v {
		b = 1
	}
	s.WriteUint8(b)
}

// WriteVarInt writes an unsigned LEB128 integer (zenoh's VarInt<usize>).
func (s *Serializer) WriteVarInt(v uint64) {
	for v >= 0x80 {
		s.buf = append(s.buf, byte(v)|0x80)
		v >>= 7
	}
	s.buf = append(s.buf, byte(v))
}

// WriteLen writes the length that starts a sequence or map.
func (s *Serializer) WriteLen(n int) { s.WriteVarInt(uint64(n)) }

// WriteBytes writes a byte sequence (Vec<u8>, ZBytes).
func (s *Serializer) WriteBytes(b []byte) {
	s.WriteLen(len(b))
	s.buf = append(s.buf, b...)
}

// WriteString writes a UTF-8 string.
func (s *Serializer) WriteString(v string) {
	s.WriteLen(len(v))
	s.buf = append(s.buf, v...)
}

// WriteSlice writes a sequence of values with write.
func WriteSlice[T any](s *Serializer, v []T, write func(*Serializer, T)) {
	s.WriteLen(len(v))
	for _, e := range v {
		write(s, e)
	}
}

// WriteMap writes a map with writeKey and writeValue (in Go's map order:
// zenoh maps are unordered).
func WriteMap[K comparable, V any](s *Serializer, m map[K]V, writeKey func(*Serializer, K), writeValue func(*Serializer, V)) {
	s.WriteLen(len(m))
	for k, v := range m {
		writeKey(s, k)
		writeValue(s, v)
	}
}

// Deserializer reads values in zenoh's serialization format.
type Deserializer struct {
	b   []byte
	off int
}

// NewDeserializer reads from b.
func NewDeserializer(b []byte) *Deserializer { return &Deserializer{b: b} }

// Done reports whether all the data was read.
func (d *Deserializer) Done() bool { return d.off == len(d.b) }

// Remaining returns the number of bytes left.
func (d *Deserializer) Remaining() int { return len(d.b) - d.off }

func (d *Deserializer) take(n int) ([]byte, error) {
	if n < 0 || n > len(d.b)-d.off {
		return nil, ErrDeserialize
	}
	p := d.b[d.off : d.off+n]
	d.off += n
	return p, nil
}

func (d *Deserializer) ReadUint8() (uint8, error) {
	p, err := d.take(1)
	if err != nil {
		return 0, err
	}
	return p[0], nil
}

func (d *Deserializer) ReadUint16() (uint16, error) {
	p, err := d.take(2)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint16(p), nil
}

func (d *Deserializer) ReadUint32() (uint32, error) {
	p, err := d.take(4)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(p), nil
}

func (d *Deserializer) ReadUint64() (uint64, error) {
	p, err := d.take(8)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(p), nil
}

func (d *Deserializer) ReadInt8() (int8, error) {
	v, err := d.ReadUint8()
	return int8(v), err
}

func (d *Deserializer) ReadInt16() (int16, error) {
	v, err := d.ReadUint16()
	return int16(v), err
}

func (d *Deserializer) ReadInt32() (int32, error) {
	v, err := d.ReadUint32()
	return int32(v), err
}

func (d *Deserializer) ReadInt64() (int64, error) {
	v, err := d.ReadUint64()
	return int64(v), err
}

// ReadUint128 reads a u128 (or i128) as its high and low 64 bits.
func (d *Deserializer) ReadUint128() (hi, lo uint64, err error) {
	if lo, err = d.ReadUint64(); err != nil {
		return 0, 0, err
	}
	hi, err = d.ReadUint64()
	return hi, lo, err
}

func (d *Deserializer) ReadFloat32() (float32, error) {
	v, err := d.ReadUint32()
	return math.Float32frombits(v), err
}

func (d *Deserializer) ReadFloat64() (float64, error) {
	v, err := d.ReadUint64()
	return math.Float64frombits(v), err
}

// ReadBool reads a bool (a byte that must be 0 or 1).
func (d *Deserializer) ReadBool() (bool, error) {
	v, err := d.ReadUint8()
	if err != nil {
		return false, err
	}
	switch v {
	case 0:
		return false, nil
	case 1:
		return true, nil
	}
	return false, ErrDeserialize
}

// ReadVarInt reads an unsigned LEB128 integer (zenoh's VarInt<usize>).
func (d *Deserializer) ReadVarInt() (uint64, error) {
	var v uint64
	for shift := uint(0); shift < 64; shift += 7 {
		b, err := d.ReadUint8()
		if err != nil {
			return 0, err
		}
		if shift == 63 && b > 1 {
			return 0, ErrDeserialize // overflows 64 bits
		}
		v |= uint64(b&0x7f) << shift
		if b < 0x80 {
			return v, nil
		}
	}
	return 0, ErrDeserialize
}

// ReadLen reads the length that starts a sequence or map. A length larger
// than the bytes left is refused (every element Go can serialize takes at
// least a byte), so corrupted data fails here instead of allocating.
func (d *Deserializer) ReadLen() (int, error) {
	n, err := d.ReadVarInt()
	if err != nil {
		return 0, err
	}
	if n > uint64(d.Remaining()) {
		return 0, ErrDeserialize
	}
	return int(n), nil
}

// ReadBytes reads a byte sequence (a copy).
func (d *Deserializer) ReadBytes() ([]byte, error) {
	n, err := d.ReadLen()
	if err != nil {
		return nil, err
	}
	p, err := d.take(n)
	if err != nil {
		return nil, err
	}
	return append([]byte(nil), p...), nil
}

// ReadString reads a UTF-8 string.
func (d *Deserializer) ReadString() (string, error) {
	n, err := d.ReadLen()
	if err != nil {
		return "", err
	}
	p, err := d.take(n)
	if err != nil || !utf8.Valid(p) {
		return "", ErrDeserialize
	}
	return string(p), nil
}

// ReadSlice reads a sequence of values with read.
func ReadSlice[T any](d *Deserializer, read func(*Deserializer) (T, error)) ([]T, error) {
	n, err := d.ReadLen()
	if err != nil {
		return nil, err
	}
	out := make([]T, 0, n)
	for range n {
		v, err := read(d)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}

// ReadMap reads a map with readKey and readValue.
func ReadMap[K comparable, V any](d *Deserializer, readKey func(*Deserializer) (K, error), readValue func(*Deserializer) (V, error)) (map[K]V, error) {
	n, err := d.ReadLen()
	if err != nil {
		return nil, err
	}
	out := make(map[K]V, n)
	for range n {
		k, err := readKey(d)
		if err != nil {
			return nil, err
		}
		v, err := readValue(d)
		if err != nil {
			return nil, err
		}
		out[k] = v
	}
	return out, nil
}
