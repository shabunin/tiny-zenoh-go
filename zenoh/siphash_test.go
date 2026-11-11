package zenoh

import (
	"testing"

	"github.com/shabunin/tiny-zenoh-go/protocol"
)

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

// Edge weights zenoh-rust 1.10 reported (admin space linkstate) for pairs
// of routers; they depend on the order of the two zids in the hash, which
// is uhlc's (lexicographic over the little-endian bytes).
func TestEdgeWeightRust(t *testing.T) {
	for _, c := range []struct{ a, b, want string }{
		{"34c338cb3af481ff3205708fce05f125", "846256c00172d1f3647fbffd7f756525", "100.08821998026414"},
		{"d559f47c331cccbd1303962086e65824", "7482e55c98f9ae6fbac2d38a535c1c41", "100.24086273653452"},
		{"b4d236908b4218b269a2bd370fd8e181", "b0021d9f48778fd90732a9fd4b53c7bf", "100.63278123890814"},
		{"e12bbe199f06201ffadbd459869c155e", "65089cf9376e2ae1cfb5cd8b19dcee24", "100.06468053070472"},
	} {
		a, err := protocol.ParseZID(c.a)
		if err != nil {
			t.Fatal(err)
		}
		b, err := protocol.ParseZID(c.b)
		if err != nil {
			t.Fatal(err)
		}
		for _, n := range []*lsNetwork{newNetwork(a), newNetwork(b)} {
			other := b
			if n.nodes[0].zid.Equal(b) {
				other = a
			}
			n.addNode(&lsNode{zid: other, links: map[string]uint16{}})
			if got := rustFloat(n.edgeWeight(0, 1)); got != c.want {
				t.Errorf("%s-%s: %s, want %s", c.a, c.b, got, c.want)
			}
		}
	}
}
