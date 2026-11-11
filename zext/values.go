package zext

import (
	"errors"
	"fmt"
)

// Marshaler is a type that writes itself (e.g. a struct, as the tuple of
// its fields).
type Marshaler interface {
	MarshalZ(*Serializer)
}

// Unmarshaler is a type that reads itself.
type Unmarshaler interface {
	UnmarshalZ(*Deserializer) error
}

// Serialize serializes vals one after the other, i.e. as a tuple
// (z_serialize of a tuple; a single value for a single argument). It takes
// the sized integers, float32/64, bool, string, []byte, slices of those,
// map[string]string, map[string]int64, map[string]float64, and Marshalers;
// anything else (int, uint, structs...) is an error: write it with a
// Serializer, or make it a Marshaler.
func Serialize(vals ...any) ([]byte, error) {
	s := NewSerializer()
	for _, v := range vals {
		if err := s.Write(v); err != nil {
			return nil, err
		}
	}
	return s.Bytes(), nil
}

// Deserialize reads a tuple from b into ptrs, pointers to the types
// Serialize takes (or Unmarshalers), and checks that nothing is left
// (z_deserialize).
func Deserialize(b []byte, ptrs ...any) error {
	d := NewDeserializer(b)
	for _, p := range ptrs {
		if err := d.Read(p); err != nil {
			return err
		}
	}
	if !d.Done() {
		return ErrDeserialize
	}
	return nil
}

var errType = errors.New("zext: unsupported type")

// Write writes one value of a type Serialize takes.
func (s *Serializer) Write(v any) error {
	switch v := v.(type) {
	case Marshaler:
		v.MarshalZ(s)
	case uint8:
		s.WriteUint8(v)
	case uint16:
		s.WriteUint16(v)
	case uint32:
		s.WriteUint32(v)
	case uint64:
		s.WriteUint64(v)
	case int8:
		s.WriteInt8(v)
	case int16:
		s.WriteInt16(v)
	case int32:
		s.WriteInt32(v)
	case int64:
		s.WriteInt64(v)
	case float32:
		s.WriteFloat32(v)
	case float64:
		s.WriteFloat64(v)
	case bool:
		s.WriteBool(v)
	case string:
		s.WriteString(v)
	case []byte:
		s.WriteBytes(v)
	case []int8:
		WriteSlice(s, v, (*Serializer).WriteInt8)
	case []uint16:
		WriteSlice(s, v, (*Serializer).WriteUint16)
	case []int16:
		WriteSlice(s, v, (*Serializer).WriteInt16)
	case []uint32:
		WriteSlice(s, v, (*Serializer).WriteUint32)
	case []int32:
		WriteSlice(s, v, (*Serializer).WriteInt32)
	case []uint64:
		WriteSlice(s, v, (*Serializer).WriteUint64)
	case []int64:
		WriteSlice(s, v, (*Serializer).WriteInt64)
	case []float32:
		WriteSlice(s, v, (*Serializer).WriteFloat32)
	case []float64:
		WriteSlice(s, v, (*Serializer).WriteFloat64)
	case []bool:
		WriteSlice(s, v, (*Serializer).WriteBool)
	case []string:
		WriteSlice(s, v, (*Serializer).WriteString)
	case [][]byte:
		WriteSlice(s, v, (*Serializer).WriteBytes)
	case map[string]string:
		WriteMap(s, v, (*Serializer).WriteString, (*Serializer).WriteString)
	case map[string]int64:
		WriteMap(s, v, (*Serializer).WriteString, (*Serializer).WriteInt64)
	case map[string]float64:
		WriteMap(s, v, (*Serializer).WriteString, (*Serializer).WriteFloat64)
	default:
		return fmt.Errorf("%w %T", errType, v)
	}
	return nil
}

// Read reads one value into p, a pointer to a type Serialize takes (or an
// Unmarshaler).
func (d *Deserializer) Read(p any) error {
	var err error
	switch p := p.(type) {
	case Unmarshaler:
		return p.UnmarshalZ(d)
	case *uint8:
		*p, err = d.ReadUint8()
	case *uint16:
		*p, err = d.ReadUint16()
	case *uint32:
		*p, err = d.ReadUint32()
	case *uint64:
		*p, err = d.ReadUint64()
	case *int8:
		*p, err = d.ReadInt8()
	case *int16:
		*p, err = d.ReadInt16()
	case *int32:
		*p, err = d.ReadInt32()
	case *int64:
		*p, err = d.ReadInt64()
	case *float32:
		*p, err = d.ReadFloat32()
	case *float64:
		*p, err = d.ReadFloat64()
	case *bool:
		*p, err = d.ReadBool()
	case *string:
		*p, err = d.ReadString()
	case *[]byte:
		*p, err = d.ReadBytes()
	case *[]int8:
		*p, err = ReadSlice(d, (*Deserializer).ReadInt8)
	case *[]uint16:
		*p, err = ReadSlice(d, (*Deserializer).ReadUint16)
	case *[]int16:
		*p, err = ReadSlice(d, (*Deserializer).ReadInt16)
	case *[]uint32:
		*p, err = ReadSlice(d, (*Deserializer).ReadUint32)
	case *[]int32:
		*p, err = ReadSlice(d, (*Deserializer).ReadInt32)
	case *[]uint64:
		*p, err = ReadSlice(d, (*Deserializer).ReadUint64)
	case *[]int64:
		*p, err = ReadSlice(d, (*Deserializer).ReadInt64)
	case *[]float32:
		*p, err = ReadSlice(d, (*Deserializer).ReadFloat32)
	case *[]float64:
		*p, err = ReadSlice(d, (*Deserializer).ReadFloat64)
	case *[]bool:
		*p, err = ReadSlice(d, (*Deserializer).ReadBool)
	case *[]string:
		*p, err = ReadSlice(d, (*Deserializer).ReadString)
	case *[][]byte:
		*p, err = ReadSlice(d, (*Deserializer).ReadBytes)
	case *map[string]string:
		*p, err = ReadMap(d, (*Deserializer).ReadString, (*Deserializer).ReadString)
	case *map[string]int64:
		*p, err = ReadMap(d, (*Deserializer).ReadString, (*Deserializer).ReadInt64)
	case *map[string]float64:
		*p, err = ReadMap(d, (*Deserializer).ReadString, (*Deserializer).ReadFloat64)
	default:
		return fmt.Errorf("%w %T", errType, p)
	}
	return err
}
