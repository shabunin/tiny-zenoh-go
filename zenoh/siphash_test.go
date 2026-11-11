package zenoh

import "testing"

// Expected values from CPython's hash() of the same bytes with
// PYTHONHASHSEED=0 (SipHash-1-3, zero key), masked to 64 bits.
func TestSipHash13(t *testing.T) {
	cases := []struct {
		in   string
		want uint64
	}{
		{"a", 0x407448d2b89b1813},
		{"abcdefgh", 0x3f7b849c0b8e35ea},
		{"0123456789abcdef0123456789abcdef", 0x508d775b6fd5229b},
		{"zenoh!", 0xd10a06c73286f729},
		{"\x00\x01\x02\x03\x04\x05\x06\x07\x08\t\n\x0b\x0c\r\x0e\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f", 0x31ef8061c910629b},
		{"xxxxxxxxxxxxxxx", 0xd9623e714b290b4b},
	}
	for _, c := range cases {
		if got := sipHash13([]byte(c.in)); got != c.want {
			t.Errorf("sipHash13(%q) = %#x, want %#x", c.in, got, c.want)
		}
	}
}
