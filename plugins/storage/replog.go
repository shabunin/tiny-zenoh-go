package storage

import (
	"encoding/binary"
	"sort"
	"time"

	"github.com/shabunin/tiny-zenoh-go/internal/xxh3"
	"github.com/shabunin/tiny-zenoh-go/keyexpr"
	"github.com/shabunin/tiny-zenoh-go/protocol"
	"github.com/shabunin/tiny-zenoh-go/zenoh"
)

// The replication log of zenoh-plugin-storage-manager (replication/log.rs,
// classification.rs, digest.rs): the latest event of every key, filed by
// time into intervals and sub-intervals, each with the XOR of its events'
// fingerprints.

type actionKind uint32 // also the bincode variant index of zenoh's Action

const (
	actPut actionKind = iota
	actDelete
	actWildPut
	actWildDel
)

type action struct {
	kind actionKind
	wild string // wildcard key expression of actWildPut / actWildDel
}

func actionOf(k zenoh.SampleKind) action {
	if k == zenoh.SampleKindDelete {
		return action{kind: actDelete}
	}
	return action{kind: actPut}
}

func (a action) sampleKind() zenoh.SampleKind {
	if a.kind == actDelete || a.kind == actWildDel {
		return zenoh.SampleKindDelete
	}
	return zenoh.SampleKindPut
}

// eventMeta is zenoh's EventMetadata.
type eventMeta struct {
	key         string // stripped key (or, for wildcard updates, the wildcard)
	hasKey      bool   // false: the key is the strip prefix itself
	ts          zenoh.Timestamp
	lastNonWild *zenoh.Timestamp // timestamp_last_non_wildcard_update
	act         action
}

type event struct {
	eventMeta
	fp uint64
}

// logKey is zenoh's LogLatestKey: the key and whether the event is a
// put/delete, a wildcard put or a wildcard delete.
type logKey struct {
	key    string
	hasKey bool
	kind   uint8
}

func (e *eventMeta) logKey() logKey {
	k := logKey{key: e.key, hasKey: e.hasKey}
	switch e.act.kind {
	case actWildPut:
		k.kind = 1
	case actWildDel:
		k.kind = 2
	}
	return k
}

// determineAction turns a wildcard action into a plain put or delete for
// keys other than the wildcard itself.
func determineAction(key string, has bool, a action) action {
	if (a.kind == actWildPut || a.kind == actWildDel) && has && key != a.wild {
		if a.kind == actWildPut {
			return action{kind: actPut}
		}
		return action{kind: actDelete}
	}
	return a
}

func newEvent(key string, has bool, ts zenoh.Timestamp, a action) *event {
	m := eventMeta{key: key, hasKey: has, ts: ts, act: determineAction(key, has, a)}
	if a.kind == actPut || a.kind == actDelete {
		t := ts
		m.lastNonWild = &t
	}
	return eventFrom(m)
}

func eventFrom(m eventMeta) *event {
	return &event{eventMeta: m, fp: fingerprint(m.key, m.hasKey, m.ts)}
}

// fingerprint is xxh3(key, time u64 LE, id u128 LE).
func fingerprint(key string, has bool, ts zenoh.Timestamp) uint64 {
	var b []byte
	if has {
		b = append(b, key...)
	}
	b = binary.LittleEndian.AppendUint64(b, ts.Time)
	id := idBytes(ts.ID)
	return xxh3.Hash(append(b, id[:]...))
}

func (e *event) setTimestampAndAction(ts zenoh.Timestamp, a action) {
	if a.kind == actPut || a.kind == actDelete {
		t := ts
		e.lastNonWild = &t
	}
	e.ts = ts
	e.act = determineAction(e.key, e.hasKey, a)
	e.fp = fingerprint(e.key, e.hasKey, e.ts)
}

func idFromBytes(b [16]byte) zenoh.ZID {
	n := 16
	for n > 1 && b[n-1] == 0 {
		n--
	}
	z, _ := protocol.NewZID(b[:n])
	return z
}

// ntpMillis is uhlc's NTP64 → milliseconds since the epoch (sub-second
// nanoseconds rounded up, as in NTP64::subsec_nanos).
func ntpMillis(t uint64) uint64 {
	nanos := ((t&0xFFFFFFFF)*1_000_000_000 + (1<<32 - 1)) >> 32
	return (t>>32)*1000 + nanos/1_000_000
}

func zenohNTP(t time.Time) uint64 { return protocol.NTP64FromTime(t) }

type subInterval struct {
	fp     uint64
	events map[logKey]*event
}

type interval struct {
	fp   uint64
	subs map[uint64]*subInterval
}

func (iv *interval) sortedSubs() []uint64 {
	idx := make([]uint64, 0, len(iv.subs))
	for k := range iv.subs {
		idx = append(idx, k)
	}
	sort.Slice(idx, func(i, j int) bool { return idx[i] < idx[j] })
	return idx
}

// subFingerprints are the non-zero sub-interval fingerprints.
func (iv *interval) subFingerprints() map[uint64]uint64 {
	out := map[uint64]uint64{}
	for k, s := range iv.subs {
		if s.fp != 0 {
			out[k] = s.fp
		}
	}
	return out
}

type replLog struct {
	cfgFP      uint64
	intervalMs uint64
	subMs      uint64
	hot, warm  uint64
	prefix     string
	intervals  map[uint64]*interval
}

func newReplLog(keyExpr, prefix string, rc ReplicationConfig) *replLog {
	le := func(v uint64, n int) []byte {
		b := make([]byte, n)
		binary.LittleEndian.PutUint64(b, v)
		return b
	}
	ims := uint64(rc.Interval.Milliseconds())
	parts := [][]byte{[]byte(keyExpr)}
	if prefix != "" {
		parts = append(parts, []byte(prefix))
	}
	parts = append(parts, le(ims, 16), le(uint64(rc.SubIntervals), 8), le(rc.Hot, 8), le(rc.Warm, 8),
		le(uint64(rc.PropagationDelay.Milliseconds()), 16))
	return &replLog{cfgFP: xxh3.Sum(parts...), intervalMs: ims, subMs: ims / uint64(rc.SubIntervals),
		hot: rc.Hot, warm: rc.Warm, prefix: prefix, intervals: map[uint64]*interval{}}
}

// classify is zenoh's get_time_classification.
func (l *replLog) classify(ts zenoh.Timestamp) (uint64, uint64) {
	ms := ntpMillis(ts.Time)
	iv := ms / l.intervalMs
	return iv, (ms - iv*l.intervalMs) / l.subMs
}

func (l *replLog) sortedIntervals() []uint64 {
	idx := make([]uint64, 0, len(l.intervals))
	for k := range l.intervals {
		idx = append(idx, k)
	}
	sort.Slice(idx, func(i, j int) bool { return idx[i] < idx[j] })
	return idx
}

// lookupNewer returns the logged event of meta's key if it is not older
// than meta (searching from meta's time on).
func (l *replLog) lookupNewer(meta *eventMeta) *event {
	iv0, sub0 := l.classify(meta.ts)
	k := meta.logKey()
	for _, ivIdx := range l.sortedIntervals() {
		if ivIdx < iv0 {
			continue
		}
		iv := l.intervals[ivIdx]
		for _, subIdx := range iv.sortedSubs() {
			if ivIdx == iv0 && subIdx < sub0 {
				continue
			}
			if ev, ok := iv.subs[subIdx].events[k]; ok {
				if !tsLess(ev.ts, meta.ts) {
					return ev
				}
				return nil
			}
		}
	}
	return nil
}

func (l *replLog) removeEvent(meta *eventMeta) *event {
	ivIdx, subIdx := l.classify(meta.ts)
	iv := l.intervals[ivIdx]
	if iv == nil || iv.subs[subIdx] == nil {
		return nil
	}
	sub := iv.subs[subIdx]
	ev, ok := sub.events[meta.logKey()]
	if !ok {
		return nil
	}
	delete(sub.events, meta.logKey())
	sub.fp ^= ev.fp
	iv.fp ^= ev.fp
	return ev
}

type removal int

const (
	notFound removal = iota
	keptNewer
	removedOlder
)

// removeOlder removes meta's key's event if it is older than meta.
func (l *replLog) removeOlder(meta *eventMeta) (removal, *event) {
	k := meta.logKey()
	idx := l.sortedIntervals()
	for i := len(idx) - 1; i >= 0; i-- {
		iv := l.intervals[idx[i]]
		for _, subIdx := range iv.sortedSubs() {
			sub := iv.subs[subIdx]
			ev, ok := sub.events[k]
			if !ok {
				continue
			}
			if !tsLess(ev.ts, meta.ts) {
				return keptNewer, nil
			}
			delete(sub.events, k)
			sub.fp ^= ev.fp
			iv.fp ^= ev.fp
			if len(sub.events) == 0 {
				delete(iv.subs, subIdx)
			}
			return removedOlder, ev
		}
	}
	return notFound, nil
}

// insertEvent logs ev unless a newer event of its key is logged.
func (l *replLog) insertEvent(ev *event) {
	if r, _ := l.removeOlder(&ev.eventMeta); r == keptNewer {
		return
	}
	l.insertUnchecked(ev)
}

func (l *replLog) insertUnchecked(ev *event) {
	ivIdx, subIdx := l.classify(ev.ts)
	iv := l.intervals[ivIdx]
	if iv == nil {
		iv = &interval{subs: map[uint64]*subInterval{}}
		l.intervals[ivIdx] = iv
	}
	iv.fp ^= ev.fp
	sub := iv.subs[subIdx]
	if sub == nil {
		sub = &subInterval{events: map[logKey]*event{}}
		iv.subs[subIdx] = sub
	}
	sub.fp ^= ev.fp
	if old, ok := sub.events[ev.logKey()]; ok {
		sub.fp ^= old.fp // as zenoh: the interval's fingerprint keeps it
	}
	sub.events[ev.logKey()] = ev
}

// digest is zenoh's Digest for the intervals elapsed at now.
type digest struct {
	cfgFP, cold uint64
	warm        map[uint64]uint64
	hot         map[uint64]map[uint64]uint64
}

func (l *replLog) lastElapsed(now time.Time) uint64 {
	return uint64(now.UnixMilli()) / l.intervalMs
}

func (l *replLog) digest(now time.Time) digest { return l.digestFrom(l.lastElapsed(now)) }

// digestFrom is the digest whose hot era ends with interval upper.
func (l *replLog) digestFrom(upper uint64) digest {
	hotLow := upper - l.hot + 1
	warmLow := upper - l.hot - l.warm + 1
	d := digest{cfgFP: l.cfgFP, warm: map[uint64]uint64{}, hot: map[uint64]map[uint64]uint64{}}
	for idx, iv := range l.intervals {
		switch {
		case idx > upper:
		case idx < warmLow:
			d.cold ^= iv.fp
		case idx < hotLow:
			if iv.fp != 0 {
				d.warm[idx] = iv.fp
			}
		default:
			d.hot[idx] = iv.subFingerprints()
		}
	}
	return d
}

// digestDiff is what differs in another replica's digest.
type digestDiff struct {
	coldDiffer bool
	warm       []uint64
	hot        map[uint64][]uint64
}

// diff is zenoh's Digest::diff: the parts of other that differ from d
// (nil if none, or if the configurations differ).
func (d digest) diff(other digest) *digestDiff {
	if d.cfgFP != other.cfgFP {
		return nil
	}
	out := &digestDiff{coldDiffer: d.cold != other.cold, hot: map[uint64][]uint64{}}
	for idx, subs := range other.hot {
		mine := d.hot[idx]
		for sub, fp := range subs {
			if f, ok := mine[sub]; !ok || f != fp {
				out.hot[idx] = append(out.hot[idx], sub)
			}
		}
	}
	for idx, fp := range other.warm {
		if f, ok := d.warm[idx]; !ok || f != fp {
			out.warm = append(out.warm, idx)
		}
	}
	if len(out.hot) > 0 || len(out.warm) > 0 {
		return out
	}
	if out.coldDiffer {
		return &digestDiff{coldDiffer: true, hot: map[uint64][]uint64{}}
	}
	return nil
}

// removeOverridden is zenoh's remove_events_overridden_by_wildcard_update
// over one map of events: it takes out the events a wildcard update at
// wildTs on wildKe overrides.
func removeOverridden(events map[logKey]*event, full func(*event) string, wildKe string,
	wildTs zenoh.Timestamp, wildKind zenoh.SampleKind) []*event {
	var out []*event
	for k, ev := range events {
		if !tsLess(ev.ts, wildTs) {
			if wildKind == zenoh.SampleKindDelete && ev.act.kind == actPut {
				if ev.lastNonWild != nil && tsLess(wildTs, *ev.lastNonWild) {
					continue
				}
			} else {
				continue
			}
		}
		var fullKe string
		switch ev.act.kind {
		case actDelete:
			continue
		case actPut:
			fullKe = full(ev)
		default:
			fullKe = ev.act.wild
		}
		if keyexpr.Include(wildKe, fullKe) {
			if wildKind == zenoh.SampleKindPut && ev.act.kind == actWildDel {
				continue
			}
			out = append(out, ev)
			delete(events, k)
		}
	}
	return out
}

func (l *replLog) removeOverridden(full func(*event) string, wildKe string, wildTs zenoh.Timestamp,
	wildKind zenoh.SampleKind) []*event {
	var out []*event
	for _, iv := range l.intervals {
		for _, sub := range iv.subs {
			removed := removeOverridden(sub.events, full, wildKe, wildTs, wildKind)
			for _, ev := range removed {
				sub.fp ^= ev.fp
				iv.fp ^= ev.fp
			}
			out = append(out, removed...)
		}
	}
	return out
}
