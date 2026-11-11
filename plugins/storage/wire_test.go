package storage

import (
	"encoding/hex"
	"reflect"
	"testing"

	"github.com/shabunin/tiny-zenoh-go/zenoh"
)

// Encodings by zenoh-rust's serde types under bincode 1.3.3 (mirrors of
// zenoh-plugin-storage-manager's Digest, AlignmentQuery, AlignmentReply).
var wireVectors = map[string]string{
	"digest":         "010000000000000002000000000000000100000000000000030000000000000004000000000000000200000000000000050000000000000001000000000000000600000000000000070000000000000008000000000000000000000000000000",
	"q_discovery":    "00000000",
	"q_all":          "01000000",
	"q_diff":         "02000000010100000000000000090000000000000001000000000000000a0000000000000002000000000000000b000000000000000c00000000000000",
	"q_intervals":    "0300000001000000000000000d00000000000000",
	"q_subintervals": "0400000001000000000000000e0000000000000001000000000000000f00000000000000",
	"q_events":       "050000000200000000000000010300000000000000612f62efcdab8967452301abcd010000000000000000000000000001efcdab8967452301abcd01000000000000000000000000000000000000efcdab8967452301abcd010000000000000000000000000000030000000400000000000000782f2a2a",
	"r_discovery":    "000000000600000000000000616263643031",
	"r_intervals":    "01000000010000000000000001000000000000000200000000000000",
	"r_subintervals": "0200000001000000000000000300000000000000010000000000000004000000000000000500000000000000",
	"r_events":       "0300000001000000000000000101000000000000006befcdab8967452301abcd010000000000000000000000000001efcdab8967452301abcd010000000000000000000000000001000000",
	"r_retrieval":    "040000000101000000000000006befcdab8967452301abcd0100000000000000000000000000000200000003000000000000006b2f2a",
}

func vecTS() zenoh.Timestamp {
	var id [16]byte
	copy(id[:], []byte{0xab, 0xcd, 0x01})
	return zenoh.Timestamp{Time: 0x0123456789abcdef, ID: idFromBytes(id)}
}

func vecEvent(key string, has bool, a action, last bool) eventMeta {
	m := eventMeta{key: key, hasKey: has, ts: vecTS(), act: a}
	if last {
		t := vecTS()
		m.lastNonWild = &t
	}
	return m
}

func TestWireVectors(t *testing.T) {
	check := func(name string, got []byte) {
		t.Helper()
		if want := wireVectors[name]; hex.EncodeToString(got) != want {
			t.Errorf("%s:\n got %x\nwant %s", name, got, want)
		}
	}
	dg := digest{cfgFP: 1, cold: 2, warm: map[uint64]uint64{3: 4},
		hot: map[uint64]map[uint64]uint64{5: {6: 7}, 8: {}}}
	check("digest", encodeDigest(dg))
	if back, err := decodeDigest(encodeDigest(dg)); err != nil || !reflect.DeepEqual(back, dg) {
		t.Errorf("digest round trip: %+v %v", back, err)
	}
	queries := map[string]*alignQuery{
		"q_discovery":    {kind: qDiscovery},
		"q_all":          {kind: qAll},
		"q_diff":         {kind: qDiff, diff: digestDiff{coldDiffer: true, warm: []uint64{9}, hot: map[uint64][]uint64{10: {11, 12}}}},
		"q_intervals":    {kind: qIntervals, intervals: []uint64{13}},
		"q_subintervals": {kind: qSubIntervals, subs: map[uint64][]uint64{14: {15}}},
		"q_events": {kind: qEvents, events: []eventMeta{vecEvent("a/b", true, action{kind: actPut}, true),
			vecEvent("", false, action{kind: actWildDel, wild: "x/**"}, false)}},
	}
	for name, q := range queries {
		check(name, encodeQuery(q))
		b, _ := hex.DecodeString(wireVectors[name])
		back, err := decodeQuery(b)
		if err != nil || hex.EncodeToString(encodeQuery(back)) != wireVectors[name] {
			t.Errorf("%s decode: %+v %v", name, back, err)
		}
	}
	replies := map[string]*alignReply{
		"r_discovery":    {kind: rDiscovery, zid: "abcd01"},
		"r_intervals":    {kind: rIntervals, intervals: map[uint64]uint64{1: 2}},
		"r_subintervals": {kind: rSubIntervals, subs: map[uint64]map[uint64]uint64{3: {4: 5}}},
		"r_events":       {kind: rEventsMetadata, events: []eventMeta{vecEvent("k", true, action{kind: actDelete}, true)}},
		"r_retrieval":    {kind: rRetrieval, event: vecEvent("k", true, action{kind: actWildPut, wild: "k/*"}, false)},
	}
	for name, r := range replies {
		check(name, encodeReply(r))
		b, _ := hex.DecodeString(wireVectors[name])
		back, err := decodeReply(b)
		if err != nil || hex.EncodeToString(encodeReply(back)) != wireVectors[name] {
			t.Errorf("%s decode: %+v %v", name, back, err)
		}
	}
	for _, bad := range []string{"", "06000000", "0500000001000000000000", "0000000006000000000000006162"} {
		b, _ := hex.DecodeString(bad)
		if _, err := decodeQuery(b); err == nil {
			t.Errorf("query %q decoded", bad)
		}
	}
}

func TestEventFingerprint(t *testing.T) {
	if fp := fingerprint("a/b", true, vecTS()); fp != 0x67777d48e0079a2c {
		t.Errorf("event fingerprint %016x", fp)
	}
	l := newReplLog("demo/**", "demo", ReplicationConfig{Interval: 1000 * 1e6, SubIntervals: 5, Hot: 6, Warm: 30, PropagationDelay: 250 * 1e6})
	if l.cfgFP != 0x02aa2c88cfcfcfe4 {
		t.Errorf("configuration fingerprint %016x", l.cfgFP)
	}
}
