package zext

import (
	"bytes"
	"errors"
	"math"
	"testing"
)

func mustSerialize(t *testing.T, vals ...any) []byte {
	t.Helper()
	b, err := Serialize(vals...)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// zenoh-ext serialization.rs binary_format test vectors.
func TestRustBinaryFormat(t *testing.T) {
	type pair struct {
		s string
		v int16
	}
	cases := []struct {
		name string
		got  []byte
		want []byte
	}{
		{"i32", mustSerialize(t, int32(1234566)), []byte{134, 214, 18, 0}},
		{"negative i32", mustSerialize(t, int32(-49245)), []byte{163, 63, 255, 255}},
		{"str", mustSerialize(t, "test"), []byte{4, 116, 101, 115, 116}},
		{"tuple", mustSerialize(t, uint16(500), float32(1234.0), "test"), []byte{244, 1, 0, 64, 154, 68, 4, 116, 101, 115, 116}},
		{"vec i64", mustSerialize(t, []int64{-100, 500, 100000, -20000000}), []byte{
			4, 156, 255, 255, 255, 255, 255, 255, 255, 244, 1, 0, 0, 0, 0, 0, 0, 160, 134, 1,
			0, 0, 0, 0, 0, 0, 211, 206, 254, 255, 255, 255, 255}},
		{"vec of tuples", func() []byte {
			s := NewSerializer()
			WriteSlice(s, []pair{{"s1", 10}, {"s2", -10000}}, func(s *Serializer, p pair) {
				s.WriteString(p.s)
				s.WriteInt16(p.v)
			})
			return s.Bytes()
		}(), []byte{2, 2, 115, 49, 10, 0, 2, 115, 50, 240, 216}},
		// From the Python binding (eclipse-zenoh 1.10): ints default to i32,
		// floats to f64.
		{"python list[int]", mustSerialize(t, []int32{1, 2}), []byte{2, 1, 0, 0, 0, 2, 0, 0, 0}},
		{"python dict[str,float]", mustSerialize(t, map[string]float64{"a": 1.5}), []byte{1, 1, 'a', 0, 0, 0, 0, 0, 0, 0xf8, 0x3f}},
		{"python bool", mustSerialize(t, true), []byte{1}},
		{"python bytes", mustSerialize(t, []byte("xy")), []byte{2, 'x', 'y'}},
	}
	for _, c := range cases {
		if !bytes.Equal(c.got, c.want) {
			t.Errorf("%s: got %v, want %v", c.name, c.got, c.want)
		}
	}
}

func TestRoundTrip(t *testing.T) {
	in := []any{uint8(255), int8(-128), uint16(65535), int16(-32768), uint32(math.MaxUint32), int32(math.MinInt32),
		uint64(math.MaxUint64), int64(math.MinInt64), float32(-1.5), math.Inf(1), false, true, "héllo", []byte{0, 1, 2},
		[]string{"a", "", "b"}, []float64{0.25, -2}, [][]byte{{1}, {}}, map[string]string{"k": "v", "x": ""},
		map[string]int64{"n": -7}, []bool{true, false}}
	b := mustSerialize(t, in...)
	var (
		u8   uint8
		i8   int8
		u16  uint16
		i16  int16
		u32  uint32
		i32  int32
		u64  uint64
		i64  int64
		f32  float32
		f64  float64
		b1   bool
		b2   bool
		str  string
		raw  []byte
		strs []string
		fs   []float64
		bss  [][]byte
		mss  map[string]string
		msi  map[string]int64
		bs   []bool
	)
	if err := Deserialize(b, &u8, &i8, &u16, &i16, &u32, &i32, &u64, &i64, &f32, &f64, &b1, &b2, &str, &raw,
		&strs, &fs, &bss, &mss, &msi, &bs); err != nil {
		t.Fatal(err)
	}
	if u8 != 255 || i8 != -128 || u16 != 65535 || i16 != -32768 || u32 != math.MaxUint32 || i32 != math.MinInt32 ||
		u64 != math.MaxUint64 || i64 != math.MinInt64 || f32 != -1.5 || !math.IsInf(f64, 1) || b1 || !b2 ||
		str != "héllo" || !bytes.Equal(raw, []byte{0, 1, 2}) || len(strs) != 3 || strs[2] != "b" ||
		fs[1] != -2 || len(bss) != 2 || len(bss[1]) != 0 || mss["k"] != "v" || mss["x"] != "" || msi["n"] != -7 ||
		len(bs) != 2 || !bs[0] {
		t.Fatalf("round trip mismatch")
	}
}

func TestVarInt(t *testing.T) {
	for _, v := range []uint64{0, 1, 127, 128, 300, 1 << 35, math.MaxUint64} {
		s := NewSerializer()
		s.WriteVarInt(v)
		d := NewDeserializer(s.Bytes())
		got, err := d.ReadVarInt()
		if err != nil || got != v || !d.Done() {
			t.Fatalf("varint %d: got %d, %v", v, got, err)
		}
	}
	s := NewSerializer()
	s.WriteVarInt(300)
	if !bytes.Equal(s.Bytes(), []byte{0xac, 0x02}) {
		t.Fatalf("LEB128 300 = %x", s.Bytes())
	}
	// 10 bytes whose last one overflows 64 bits.
	if _, err := NewDeserializer([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x02}).ReadVarInt(); err == nil {
		t.Fatal("overflowing varint accepted")
	}
}

func TestUint128(t *testing.T) {
	s := NewSerializer()
	s.WriteUint128(1, 2)
	want := []byte{2, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0}
	if !bytes.Equal(s.Bytes(), want) {
		t.Fatalf("got %v", s.Bytes())
	}
	hi, lo, err := NewDeserializer(want).ReadUint128()
	if err != nil || hi != 1 || lo != 2 {
		t.Fatalf("hi %d lo %d %v", hi, lo, err)
	}
}

type point struct {
	x, y float64
	tag  string
}

func (p point) MarshalZ(s *Serializer) {
	s.WriteFloat64(p.x)
	s.WriteFloat64(p.y)
	s.WriteString(p.tag)
}

func (p *point) UnmarshalZ(d *Deserializer) (err error) {
	if p.x, err = d.ReadFloat64(); err != nil {
		return err
	}
	if p.y, err = d.ReadFloat64(); err != nil {
		return err
	}
	p.tag, err = d.ReadString()
	return err
}

func TestMarshaler(t *testing.T) {
	b := mustSerialize(t, point{1, 2, "p"}, int32(3))
	// A struct is the tuple of its fields.
	if want := mustSerialize(t, 1.0, 2.0, "p", int32(3)); !bytes.Equal(b, want) {
		t.Fatalf("got %v want %v", b, want)
	}
	var p point
	var n int32
	if err := Deserialize(b, &p, &n); err != nil || p != (point{1, 2, "p"}) || n != 3 {
		t.Fatalf("%+v %d %v", p, n, err)
	}
}

func TestErrors(t *testing.T) {
	var b bool
	if err := Deserialize([]byte{2}, &b); !errors.Is(err, ErrDeserialize) {
		t.Fatalf("bool 2: %v", err)
	}
	var i int32
	if err := Deserialize([]byte{1, 2, 3}, &i); !errors.Is(err, ErrDeserialize) {
		t.Fatalf("short: %v", err)
	}
	if err := Deserialize([]byte{1, 2, 3, 4, 5}, &i); !errors.Is(err, ErrDeserialize) {
		t.Fatalf("trailing: %v", err)
	}
	var s string
	if err := Deserialize([]byte{1, 0xff}, &s); !errors.Is(err, ErrDeserialize) {
		t.Fatalf("bad utf-8: %v", err)
	}
	var xs []int64
	if err := Deserialize([]byte{0xff, 0xff, 0xff, 0xff, 0x0f}, &xs); !errors.Is(err, ErrDeserialize) {
		t.Fatalf("huge length: %v", err)
	}
	if _, err := Serialize(1); err == nil {
		t.Fatal("int accepted")
	}
	var u uint
	if err := Deserialize([]byte{1}, &u); err == nil {
		t.Fatal("*uint accepted")
	}
}
