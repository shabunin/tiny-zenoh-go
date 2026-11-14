package zenoh

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/shabunin/tiny-zenoh-go/protocol"
)

// Link states come from other routers and peers (OAM link-state messages):
// every prefix of a valid list must be refused, and every corruption must
// be refused or decode into something that encodes and decodes again.
func TestLinkStateDecodeSweep(t *testing.T) {
	z1, _ := protocol.NewZID([]byte{1, 2, 3, 4, 5, 6, 7, 8})
	z2, _ := protocol.NewZID([]byte{0xee})
	lists := [][]linkState{
		{{psid: 0, sn: 7, zid: z1, whatami: protocol.Router, hasWAI: true,
			locators: []string{"tcp/10.0.0.1:7447", "udp/[::1]:7447"}, hasLoc: true,
			links: []uint64{1, 2, 300}, weights: []uint16{10, 0, 65535}, gateway: true}},
		{{psid: 1, sn: 1 << 40, zid: z2, whatami: protocol.Peer, hasWAI: true, links: []uint64{0}},
			{psid: 2, sn: 3, whatami: protocol.Client, hasWAI: true, hasLoc: true},
			{psid: 3, sn: 4}},
		{},
	}
	for li, list := range lists {
		b := encodeLinkStates(list)
		got, err := decodeLinkStates(b)
		if err != nil || len(got) != len(list) {
			t.Fatalf("list %d: %v (%d states)", li, err, len(got))
		}
		if e := encodeLinkStates(got); !bytes.Equal(e, b) {
			t.Fatalf("list %d: re-encoded % x, want % x", li, e, b)
		}
		try := func(what string, in []byte) bool {
			defer func() {
				if p := recover(); p != nil {
					t.Fatalf("list %d %s: % x: panic %v", li, what, in, p)
				}
			}()
			got, err := decodeLinkStates(in)
			if err != nil {
				return false
			}
			if _, err := decodeLinkStates(encodeLinkStates(got)); err != nil {
				t.Fatalf("list %d %s: accepted % x, its re-encoding fails: %v", li, what, in, err)
			}
			return true
		}
		for i := range len(b) {
			if try(fmt.Sprintf("prefix %d", i), b[:i]) {
				t.Errorf("list %d: prefix % x of % x accepted", li, b[:i], b)
			}
		}
		in := make([]byte, len(b))
		for i := range b {
			for _, m := range []byte{1, 2, 4, 8, 16, 32, 64, 128, 0xff} {
				copy(in, b)
				in[i] ^= m
				try(fmt.Sprintf("byte %d ^ %#x", i, m), in)
			}
		}
	}
	for _, bad := range [][]byte{
		{0xff, 0xff, 0xff, 0x0f},        // count larger than the input
		{1, lsWAI, 0, 0, 3},             // whatami bits 3: not one role
		{1, lsLOC, 0, 0, 0x7f},          // locator count larger than the rest
		{1, 0, 0, 0, 0x7f},              // link count larger than the rest
		{1, lsPID, 0, 0, 17, 1, 2, 3},   // zid longer than 16
		{1, lsWGT, 0, 0, 2, 1, 2, 0x80}, // weight cut short
	} {
		if _, err := decodeLinkStates(bad); err == nil {
			t.Errorf("% x accepted", bad)
		}
	}
}
