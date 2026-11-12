package zext

import (
	"bytes"
	"reflect"
	"testing"
)

// fuzzTypes are the types Deserialize takes, picked by the first byte of
// the fuzzer's input.
var fuzzTypes = []any{
	uint8(0), uint16(0), uint32(0), uint64(0), int8(0), int16(0), int32(0), int64(0), float32(0), float64(0),
	false, "", []byte(nil), []int8(nil), []uint16(nil), []int16(nil), []uint32(nil), []int32(nil), []uint64(nil),
	[]int64(nil), []float32(nil), []float64(nil), []bool(nil), []string(nil), [][]byte(nil),
	map[string]string(nil), map[string]int64(nil), map[string]float64(nil),
}

// FuzzDeserialize reads arbitrary bytes as a value of each type (and as a
// tuple of two), and checks that what decodes re-serializes to bytes that
// decode to the same thing: serialize∘deserialize is stable.
func FuzzDeserialize(f *testing.F) {
	for i, v := range []any{uint8(255), uint16(500), int32(1234566), int64(-20000000), float32(1234.0),
		float64(1.5), true, "test", []byte("xy"), []int8{-1}, []uint16{1, 2}, []int16{-3}, []uint32{7},
		[]int32{1, 2}, []uint64{1 << 40}, []int64{-100, 500, 100000, -20000000}, []float32{0.5}, []float64{0.25, -2},
		[]bool{true, false}, []string{"a", "", "b"}, [][]byte{{1}, {}}, map[string]string{"k": "v"},
		map[string]int64{"n": -7}, map[string]float64{"a": 1.5}} {
		b, err := Serialize(v)
		if err != nil {
			f.Fatal(err)
		}
		for j, t := range fuzzTypes {
			if reflect.TypeOf(t) == reflect.TypeOf(v) {
				f.Add(append([]byte{byte(j)}, b...))
			}
		}
		f.Add(append([]byte{byte(i), 0xff, 0xff, 0xff, 0xff, 0x0f}, b...))
	}
	f.Add([]byte{25, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) == 0 {
			return
		}
		sel, data := int(data[0]), data[1:]
		typ := reflect.TypeOf(fuzzTypes[sel%len(fuzzTypes)])
		typ2 := reflect.TypeOf(fuzzTypes[sel/len(fuzzTypes)%len(fuzzTypes)])

		p := reflect.New(typ)
		if err := Deserialize(data, p.Interface()); err == nil {
			roundTrip(t, data, p)
		}
		// A tuple: the first value must leave the rest to the second.
		p1, p2 := reflect.New(typ), reflect.New(typ2)
		if err := Deserialize(data, p1.Interface(), p2.Interface()); err == nil {
			roundTrip(t, data, p1, p2)
		}
		// The primitive readers, in turn, on what is left.
		d := NewDeserializer(data)
		for d.Remaining() > 0 {
			before := d.Remaining()
			switch op := data[len(data)-d.Remaining()] % 6; op {
			case 0:
				_, _ = d.ReadVarInt()
			case 1:
				_, _ = d.ReadLen()
			case 2:
				_, _ = d.ReadString()
			case 3:
				_, _ = d.ReadBytes()
			case 4:
				_, _, _ = d.ReadUint128()
			default:
				_, _ = d.ReadBool()
			}
			if d.Remaining() > before || d.Remaining() < 0 {
				t.Fatalf("Remaining went from %d to %d", before, d.Remaining())
			}
			if d.Remaining() == before {
				d.off++ // a failed read: skip a byte
			}
		}
	})
}

func roundTrip(t *testing.T, data []byte, ptrs ...reflect.Value) {
	t.Helper()
	vals := make([]any, len(ptrs))
	for i, p := range ptrs {
		vals[i] = p.Elem().Interface()
	}
	b, err := Serialize(vals...)
	if err != nil {
		t.Fatalf("Serialize(%#v): %v", vals, err)
	}
	if len(b) > len(data) {
		t.Fatalf("%x decodes to %#v, which serializes longer: %x", data, vals, b)
	}
	back := make([]any, len(ptrs))
	for i, p := range ptrs {
		back[i] = reflect.New(p.Elem().Type()).Interface()
	}
	if err := Deserialize(b, back...); err != nil {
		t.Fatalf("%#v serialized to %x, which doesn't deserialize: %v", vals, b, err)
	}
	vals2 := make([]any, len(back))
	for i, p := range back {
		vals2[i] = reflect.ValueOf(p).Elem().Interface()
	}
	b2, err := Serialize(vals2...)
	if err != nil || !bytes.Equal(b, b2) {
		t.Fatalf("%x: serialization not stable: %x then %x (%v)", data, b, b2, err)
	}
	// Maps have no order and floats may be NaN: compare those by bytes.
	if len(vals) == 1 && reflect.TypeOf(vals[0]).Kind() != reflect.Map && !bytes.Equal(b, data) &&
		!hasLongVarInt(data) {
		t.Fatalf("%x decodes to %#v, which serializes to %x", data, vals, b)
	}
}

// hasLongVarInt reports whether data may hold a non-minimal LEB128 (a
// continuation byte followed by 0x00), which decodes but serializes shorter.
func hasLongVarInt(data []byte) bool {
	for i := 0; i+1 < len(data); i++ {
		if data[i]&0x80 != 0 && data[i+1] == 0 {
			return true
		}
	}
	return false
}
