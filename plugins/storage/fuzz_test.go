package storage

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"reflect"
	"strings"
	"testing"
	"time"
)

// Replication messages come from other replicas: decoding must fail
// cleanly on anything, and what decodes must re-encode to bytes that decode
// to the same thing.

func addWireSeeds(f *testing.F, prefix string) {
	for name, h := range wireVectors {
		if strings.HasPrefix(name, prefix) {
			b, _ := hex.DecodeString(h)
			f.Add(b)
		}
	}
	// Lengths whose product with the element size overflows 64 bits.
	for _, kind := range []uint32{0, 1, 2, 3, 4, 5} {
		for _, n := range []uint64{1 << 60, 1 << 61, 1<<63 + 1, 1<<64 - 1} {
			b := binary.LittleEndian.AppendUint32(nil, kind)
			f.Add(binary.LittleEndian.AppendUint64(b, n))
			b = append(binary.LittleEndian.AppendUint32(nil, kind), 1)
			f.Add(binary.LittleEndian.AppendUint64(b, n))
			f.Add(binary.LittleEndian.AppendUint64(make([]byte, 16), n))
			f.Add(binary.LittleEndian.AppendUint64(make([]byte, 32), n))
		}
	}
}

// fuzzLog is a replication log holding a few events, for decoded messages
// to be processed against.
func fuzzLog() *replLog {
	l := newReplLog("demo/**", "demo", ReplicationConfig{Interval: time.Second, SubIntervals: 5, Hot: 6, Warm: 30,
		PropagationDelay: 250 * time.Millisecond})
	ts := vecTS()
	for i, k := range []string{"a/b", "k", "x/y"} {
		ts.Time += uint64(i) << 32
		l.insertEvent(newEvent(k, true, ts, action{kind: actPut}))
	}
	l.insertEvent(newEvent("x/**", true, ts, action{kind: actWildPut, wild: "x/**"}))
	return l
}

// process runs replica events through a log, as alignment does.
func process(l *replLog, evs []eventMeta) {
	for _, ev := range evs {
		l.lookupNewer(&ev)
		switch ev.act.kind {
		case actWildPut, actWildDel:
			l.removeOverridden(func(e *event) string { return e.key }, ev.act.wild, ev.ts, ev.act.sampleKind())
		}
		l.insertEvent(eventFrom(ev))
		l.removeEvent(&ev)
	}
	l.digestFrom(ntpMillis(vecTS().Time) / l.intervalMs)
}

func FuzzDecodeDigest(f *testing.F) {
	addWireSeeds(f, "digest")
	f.Add(encodeDigest(fuzzLog().digestFrom(ntpMillis(vecTS().Time) / 1000)))
	f.Fuzz(func(t *testing.T, b []byte) {
		dg, err := decodeDigest(b)
		if err != nil {
			return
		}
		e := encodeDigest(dg)
		if len(e) > len(b) {
			t.Fatalf("%x re-encodes longer: %x", b, e)
		}
		back, err := decodeDigest(e)
		if err != nil || !reflect.DeepEqual(back, dg) {
			t.Fatalf("%x: %+v re-encodes to %x, decoding to %+v, %v", b, dg, e, back, err)
		}
		l := fuzzLog()
		l.cfgFP = dg.cfgFP
		l.digestFrom(ntpMillis(vecTS().Time) / 1000).diff(dg)
	})
}

func FuzzDecodeQuery(f *testing.F) {
	addWireSeeds(f, "q_")
	f.Fuzz(func(t *testing.T, b []byte) {
		q, err := decodeQuery(b)
		if err != nil {
			return
		}
		e := encodeQuery(q)
		if len(e) > len(b) {
			t.Fatalf("%x re-encodes longer: %x", b, e)
		}
		back, err := decodeQuery(e)
		if err != nil || !reflect.DeepEqual(back, q) {
			t.Fatalf("%x: %+v re-encodes to %x, decoding to %+v, %v", b, q, e, back, err)
		}
		if again := encodeQuery(back); !bytes.Equal(again, e) {
			t.Fatalf("%x: encoding not stable: %x then %x", b, e, again)
		}
		process(fuzzLog(), q.events)
	})
}

func FuzzDecodeReply(f *testing.F) {
	addWireSeeds(f, "r_")
	f.Fuzz(func(t *testing.T, b []byte) {
		r, err := decodeReply(b)
		if err != nil {
			return
		}
		e := encodeReply(r)
		if len(e) > len(b) {
			t.Fatalf("%x re-encodes longer: %x", b, e)
		}
		back, err := decodeReply(e)
		if err != nil || !reflect.DeepEqual(back, r) {
			t.Fatalf("%x: %+v re-encodes to %x, decoding to %+v, %v", b, r, e, back, err)
		}
		if again := encodeReply(back); !bytes.Equal(again, e) {
			t.Fatalf("%x: encoding not stable: %x then %x", b, e, again)
		}
		l := fuzzLog()
		process(l, r.events)
		if r.kind == rRetrieval {
			process(l, []eventMeta{r.event})
		}
	})
}
