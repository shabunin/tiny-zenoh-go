package protocol

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/shabunin/gozenoh/internal/codec"
)

func TestScoutingVectors(t *testing.T) {
	zid, _ := NewZID([]byte{0xaa, 0xbb})
	cases := []struct {
		m    Message
		want []byte
	}{
		// header SCOUT, version 9, what = router|peer, no zid
		{&Scout{Version: 9, What: MatchRouter | MatchPeer}, []byte{0x01, 0x09, 0x03}},
		// with zid: flags = (len-1)<<4 | I | what
		{&Scout{Version: 9, What: MatchPeer, ZID: zid}, []byte{0x01, 0x09, 0x1a, 0xaa, 0xbb}},
		// HELLO from a peer with one locator: header L flag, flags = (len-1)<<4 | peer
		{&Hello{Version: 9, WhatAmI: Peer, ZID: zid, Locators: []string{"tcp/1.2.3.4:5"}},
			append([]byte{0x22, 0x09, 0x11, 0xaa, 0xbb, 0x01, 13}, "tcp/1.2.3.4:5"...)},
		{&Hello{Version: 9, WhatAmI: Router, ZID: zid}, []byte{0x02, 0x09, 0x10, 0xaa, 0xbb}},
	}
	for _, c := range cases {
		var w codec.Writer
		c.m.Encode(&w)
		if !bytes.Equal(w.Bytes(), c.want) {
			t.Errorf("%T encode = % x, want % x", c.m, w.Bytes(), c.want)
		}
		got, err := DecodeScouting(c.want)
		if err != nil {
			t.Fatalf("decode % x: %v", c.want, err)
		}
		if !reflect.DeepEqual(got, c.m) {
			t.Errorf("decode = %#v, want %#v", got, c.m)
		}
	}
	if !(MatchRouter | MatchPeer).Matches(Peer) || MatchRouter.Matches(Client) {
		t.Error("matcher")
	}
}
