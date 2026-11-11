package protocol

import "testing"

func TestParseZID(t *testing.T) {
	for _, b := range [][]byte{{1}, {0x0f, 0xa0}, {0xde, 0xad, 0xbe, 0xef, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 0x12}} {
		z, err := NewZID(b)
		if err != nil {
			t.Fatal(err)
		}
		p, err := ParseZID(z.String())
		if err != nil || !p.Equal(z) {
			t.Fatalf("%s: parsed %v (%v)", z, p, err)
		}
	}
	for _, s := range []string{"", "0", "xyz", "123456789012345678901234567890123"} {
		if _, err := ParseZID(s); err == nil {
			t.Errorf("ParseZID(%q) accepted", s)
		}
	}
}
