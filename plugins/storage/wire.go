package storage

import (
	"encoding/binary"
	"errors"
	"sort"

	"github.com/shabunin/tiny-zenoh-go/zenoh"
)

// bincode 1.x (default options: little endian, fixed-width integers, u64
// lengths, u32 enum variant indexes, u8 Option tags) encodings of the
// replication messages, as zenoh-rust serializes them.

type benc struct{ b []byte }

func (e *benc) u8(v uint8)   { e.b = append(e.b, v) }
func (e *benc) u32(v uint32) { e.b = binary.LittleEndian.AppendUint32(e.b, v) }
func (e *benc) u64(v uint64) { e.b = binary.LittleEndian.AppendUint64(e.b, v) }
func (e *benc) str(s string) { e.u64(uint64(len(s))); e.b = append(e.b, s...) }
func (e *benc) ts(t zenoh.Timestamp) {
	e.u64(t.Time)
	id := idBytes(t.ID)
	e.b = append(e.b, id[:]...)
}

func sortedU64[V any](m map[uint64]V) []uint64 {
	ks := make([]uint64, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Slice(ks, func(i, j int) bool { return ks[i] < ks[j] })
	return ks
}

func (e *benc) set(s []uint64) {
	e.u64(uint64(len(s)))
	for _, v := range s {
		e.u64(v)
	}
}

func (e *benc) fpMap(m map[uint64]uint64) {
	e.u64(uint64(len(m)))
	for _, k := range sortedU64(m) {
		e.u64(k)
		e.u64(m[k])
	}
}

func (e *benc) fpMap2(m map[uint64]map[uint64]uint64) {
	e.u64(uint64(len(m)))
	for _, k := range sortedU64(m) {
		e.u64(k)
		e.fpMap(m[k])
	}
}

func (e *benc) setMap(m map[uint64][]uint64) {
	e.u64(uint64(len(m)))
	for _, k := range sortedU64(m) {
		e.u64(k)
		e.set(m[k])
	}
}

func (e *benc) meta(m *eventMeta) {
	if m.hasKey {
		e.u8(1)
		e.str(m.key)
	} else {
		e.u8(0)
	}
	e.ts(m.ts)
	if m.lastNonWild != nil {
		e.u8(1)
		e.ts(*m.lastNonWild)
	} else {
		e.u8(0)
	}
	e.u32(uint32(m.act.kind))
	if m.act.kind == actWildPut || m.act.kind == actWildDel {
		e.str(m.act.wild)
	}
}

var errShort = errors.New("storage: truncated replication message")

type bdec struct {
	b   []byte
	err error
}

func (d *bdec) take(n uint64) []byte {
	if d.err != nil || uint64(len(d.b)) < n {
		d.err = errShort
		return nil
	}
	v := d.b[:n]
	d.b = d.b[n:]
	return v
}

func (d *bdec) u8() uint8 {
	if b := d.take(1); b != nil {
		return b[0]
	}
	return 0
}

func (d *bdec) u32() uint32 {
	if b := d.take(4); b != nil {
		return binary.LittleEndian.Uint32(b)
	}
	return 0
}

func (d *bdec) u64() uint64 {
	if b := d.take(8); b != nil {
		return binary.LittleEndian.Uint64(b)
	}
	return 0
}

// count reads a length, refusing ones that can't fit in what is left
// (dividing: n*elem overflows for n near 2^64/elem).
func (d *bdec) count(elem uint64) uint64 {
	n := d.u64()
	if d.err == nil && n > uint64(len(d.b))/elem {
		d.err = errShort
		return 0
	}
	return n
}

func (d *bdec) str() string { return string(d.take(d.count(1))) }

func (d *bdec) ts() zenoh.Timestamp {
	t := d.u64()
	var id [16]byte
	copy(id[:], d.take(16))
	return zenoh.Timestamp{Time: t, ID: idFromBytes(id)}
}

func (d *bdec) set() []uint64 {
	n := d.count(8)
	out := make([]uint64, 0, n)
	for i := uint64(0); i < n && d.err == nil; i++ {
		out = append(out, d.u64())
	}
	return out
}

func (d *bdec) fpMap() map[uint64]uint64 {
	n := d.count(16)
	out := make(map[uint64]uint64, n)
	for i := uint64(0); i < n && d.err == nil; i++ {
		k := d.u64()
		out[k] = d.u64()
	}
	return out
}

func (d *bdec) fpMap2() map[uint64]map[uint64]uint64 {
	n := d.count(16)
	out := make(map[uint64]map[uint64]uint64, n)
	for i := uint64(0); i < n && d.err == nil; i++ {
		k := d.u64()
		out[k] = d.fpMap()
	}
	return out
}

func (d *bdec) setMap() map[uint64][]uint64 {
	n := d.count(16)
	out := make(map[uint64][]uint64, n)
	for i := uint64(0); i < n && d.err == nil; i++ {
		k := d.u64()
		out[k] = d.set()
	}
	return out
}

func (d *bdec) meta() eventMeta {
	var m eventMeta
	switch d.u8() {
	case 0:
	case 1:
		m.key, m.hasKey = d.str(), true
	default:
		d.fail()
	}
	m.ts = d.ts()
	switch d.u8() {
	case 0:
	case 1:
		t := d.ts()
		m.lastNonWild = &t
	default:
		d.fail()
	}
	m.act.kind = actionKind(d.u32())
	switch m.act.kind {
	case actPut, actDelete:
	case actWildPut, actWildDel:
		m.act.wild = d.str()
	default:
		d.fail()
	}
	return m
}

func (d *bdec) fail() {
	if d.err == nil {
		d.err = errors.New("storage: bad replication message")
	}
}

func (d *bdec) metas() []eventMeta {
	n := d.count(26) // smallest EventMetadata
	out := make([]eventMeta, 0, n)
	for i := uint64(0); i < n && d.err == nil; i++ {
		out = append(out, d.meta())
	}
	return out
}

func (d *bdec) done() error {
	if d.err == nil && len(d.b) != 0 {
		d.fail()
	}
	return d.err
}

func encodeDigest(dg digest) []byte {
	var e benc
	e.u64(dg.cfgFP)
	e.u64(dg.cold)
	e.fpMap(dg.warm)
	e.fpMap2(dg.hot)
	return e.b
}

func decodeDigest(b []byte) (digest, error) {
	d := bdec{b: b}
	dg := digest{cfgFP: d.u64(), cold: d.u64()}
	dg.warm = d.fpMap()
	dg.hot = d.fpMap2()
	return dg, d.done()
}

// AlignmentQuery variants.
const (
	qDiscovery uint32 = iota
	qAll
	qDiff
	qIntervals
	qSubIntervals
	qEvents
)

type alignQuery struct {
	kind      uint32
	diff      digestDiff
	intervals []uint64
	subs      map[uint64][]uint64
	events    []eventMeta
}

func encodeQuery(q *alignQuery) []byte {
	var e benc
	e.u32(q.kind)
	switch q.kind {
	case qDiff:
		if q.diff.coldDiffer {
			e.u8(1)
		} else {
			e.u8(0)
		}
		e.set(q.diff.warm)
		e.setMap(q.diff.hot)
	case qIntervals:
		e.set(q.intervals)
	case qSubIntervals:
		e.setMap(q.subs)
	case qEvents:
		e.u64(uint64(len(q.events)))
		for i := range q.events {
			e.meta(&q.events[i])
		}
	}
	return e.b
}

func decodeQuery(b []byte) (*alignQuery, error) {
	d := bdec{b: b}
	q := &alignQuery{kind: d.u32()}
	switch q.kind {
	case qDiscovery, qAll:
	case qDiff:
		q.diff.coldDiffer = d.u8() != 0
		q.diff.warm = d.set()
		q.diff.hot = d.setMap()
	case qIntervals:
		q.intervals = d.set()
	case qSubIntervals:
		q.subs = d.setMap()
	case qEvents:
		q.events = d.metas()
	default:
		d.fail()
	}
	return q, d.done()
}

// AlignmentReply variants.
const (
	rDiscovery uint32 = iota
	rIntervals
	rSubIntervals
	rEventsMetadata
	rRetrieval
)

type alignReply struct {
	kind      uint32
	zid       string
	intervals map[uint64]uint64
	subs      map[uint64]map[uint64]uint64
	events    []eventMeta
	event     eventMeta
}

func encodeReply(r *alignReply) []byte {
	var e benc
	e.u32(r.kind)
	switch r.kind {
	case rDiscovery:
		e.str(r.zid)
	case rIntervals:
		e.fpMap(r.intervals)
	case rSubIntervals:
		e.fpMap2(r.subs)
	case rEventsMetadata:
		e.u64(uint64(len(r.events)))
		for i := range r.events {
			e.meta(&r.events[i])
		}
	case rRetrieval:
		e.meta(&r.event)
	}
	return e.b
}

func decodeReply(b []byte) (*alignReply, error) {
	d := bdec{b: b}
	r := &alignReply{kind: d.u32()}
	switch r.kind {
	case rDiscovery:
		r.zid = d.str()
	case rIntervals:
		r.intervals = d.fpMap()
	case rSubIntervals:
		r.subs = d.fpMap2()
	case rEventsMetadata:
		r.events = d.metas()
	case rRetrieval:
		r.event = d.meta()
	default:
		d.fail()
	}
	return r, d.done()
}
