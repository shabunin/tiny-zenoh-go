package storage

import (
	"testing"
	"time"

	"github.com/shabunin/tiny-zenoh-go/protocol"
	"github.com/shabunin/tiny-zenoh-go/zenoh"
)

func testZID(b byte) zenoh.ZID {
	z, err := protocol.NewZID([]byte{b})
	if err != nil {
		panic(err)
	}
	return z
}

func TestReplLogHardening(t *testing.T) {
	rc := ReplicationConfig{
		Interval:         time.Second,
		SubIntervals:     5,
		Hot:              6,
		Warm:             30,
		PropagationDelay: 200 * time.Millisecond,
	}
	l := newReplLog("demo/**", "", rc)

	// Zero/small upper must not underflow hotLow or warmLow.
	if got := l.warmLow(0); got != 0 {
		t.Fatalf("warmLow(0) = %d, want 0", got)
	}
	if got := l.warmLow(5); got != 0 {
		t.Fatalf("warmLow(5) = %d, want 0", got)
	}

	// digestFrom with a small upper must not underflow.
	dZero := l.digestFrom(0)
	if dZero.cold != 0 {
		t.Fatalf("digestFrom(0).cold = %d, want 0", dZero.cold)
	}
}

func TestWireZeroZIDRejection(t *testing.T) {
	var e benc
	e.u32(rRetrieval)
	e.meta(&eventMeta{
		key:    "demo/a",
		hasKey: true,
		ts:     zenoh.Timestamp{Time: zenohNTP(time.Now()), ID: zenoh.ZID{}},
		act:    action{kind: actPut},
	})
	if _, err := decodeReply(e.b); err == nil {
		t.Fatal("decodeReply accepted zero Timestamp.ID")
	}

	var eq benc
	eq.u32(qEvents)
	eq.u64(1)
	eq.meta(&eventMeta{
		key:    "demo/a",
		hasKey: true,
		ts:     zenoh.Timestamp{Time: zenohNTP(time.Now()), ID: zenoh.ZID{}},
		act:    action{kind: actPut},
	})
	if _, err := decodeQuery(eq.b); err == nil {
		t.Fatal("decodeQuery accepted zero Timestamp.ID")
	}
}

func TestStorageHardening(t *testing.T) {
	// Zero GCPeriod in gcLoop must exit cleanly without panicking.
	done := make(chan struct{})
	zeroGC := &Storage{stop: make(chan struct{})}
	go func() {
		zeroGC.gcLoop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("gcLoop with GCPeriod <= 0 did not return immediately")
	}

	r := router(t)
	s := client(t, r)
	m, err := NewMemory(s, Config{KeyExpr: "st/**", GCLifespan: 1000000 * time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	now := time.Now()
	older := zenoh.Timestamp{Time: zenohNTP(now.Add(-10 * time.Second)), ID: testZID(1)}
	newer := zenoh.Timestamp{Time: zenohNTP(now), ID: testZID(2)}

	m.mu.Lock()
	m.processLocked("st/*", zenoh.SampleKindPut, []byte("new"), zenoh.EncodingTextPlain, newer)
	m.processLocked("st/*", zenoh.SampleKindPut, []byte("old"), zenoh.EncodingTextPlain, older)
	gotPut := m.wildPuts["st/*"]

	m.processLocked("st/del/*", zenoh.SampleKindDelete, nil, zenoh.Encoding{}, newer)
	m.processLocked("st/del/*", zenoh.SampleKindDelete, nil, zenoh.Encoding{}, older)
	gotDel := m.wildDels["st/del/*"]
	m.mu.Unlock()

	if gotPut == nil || string(gotPut.payload) != "new" || !tsEqual(gotPut.ts, newer) {
		t.Fatalf("stale wildPut overwrote newer entry: %+v", gotPut)
	}
	if gotDel == nil || !tsEqual(gotDel.ts, newer) {
		t.Fatalf("stale wildDel overwrote newer entry: %+v", gotDel)
	}

	// gc with huge GCLifespan (larger than Unix epoch time) must clamp limit to 0, not underflow uint64.
	m.gc(now)
	m.mu.Lock()
	_, kept := m.wildPuts["st/*"]
	m.mu.Unlock()
	if !kept {
		t.Fatal("gc underflowed limit and dropped recent wildcard entry")
	}
}

func TestReplicationHardening(t *testing.T) {
	r := router(t)
	s := client(t, r)
	m, err := NewMemory(s, Config{
		KeyExpr:     "rh/**",
		Replication: fastReplication(),
	})
	if err != nil {
		t.Fatal(err)
	}

	repl := m.repl

	// validEvent guards.
	now := time.Now()
	ts1 := zenoh.Timestamp{Time: zenohNTP(now), ID: testZID(1)}
	badCases := []eventMeta{
		{key: "", hasKey: false, ts: ts1, act: action{kind: actPut}},                              // missing StripPrefix
		{key: "rh/*", hasKey: true, ts: ts1, act: action{kind: actPut}},                           // non-concrete put
		{key: "other/x", hasKey: true, ts: ts1, act: action{kind: actPut}},                        // out of key_expr scope
		{key: "rh/x", hasKey: true, ts: ts1, act: action{kind: actWildPut, wild: "rh/concrete"}},  // concrete wildcard
		{key: "other/**", hasKey: true, ts: ts1, act: action{kind: actWildDel, wild: "other/**"}}, // non-intersecting wildcard
		{key: "rh/a", hasKey: true, ts: ts1, act: action{kind: actionKind(99)}},                   // unknown kind
	}
	for i := range badCases {
		if repl.validEvent(&badCases[i]) {
			t.Fatalf("validEvent(%+v) = true, want false", badCases[i])
		}
	}
	if !repl.validEvent(&eventMeta{key: "rh/a", hasKey: true, ts: ts1, act: action{kind: actPut}}) {
		t.Fatal("validEvent rejected valid actPut")
	}

	// onDigest must ignore wildcard / malformed responder IDs in sample keys without panicking.
	digestPayload := encodeDigest(digest{cfgFP: m.log.cfgFP})
	repl.onDigest(zenoh.Sample{KeyExpr: "@-digest/*/1", Payload: digestPayload})
	repl.onDigest(zenoh.Sample{KeyExpr: "@-digest//1", Payload: digestPayload})
	repl.onDigest(zenoh.Sample{KeyExpr: "@-digest/a$b/1", Payload: digestPayload})

	// spawnAligner + Close must cancel active queries via r.ct and wait for wg.
	repl.spawnAligner(alignerKey("11223344", repl.fp), &alignQuery{kind: qAll})
	done := make(chan error, 1)
	go func() {
		done <- m.Close()
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Close returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close hung waiting for active aligner")
	}

	// Spawning after Close must be a safe no-op.
	repl.spawnAligner(alignerKey("11223344", repl.fp), &alignQuery{kind: qAll})
}
