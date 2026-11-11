// Package storage is zenoh's storage manager with its "memory" volume
// (zenoh-rust plugins/zenoh-plugin-storage-manager): a storage subscribes
// to a key expression, keeps the latest value of every key under it, and
// answers queries on it with those values and their timestamps. With
// Config.Replication, storages on the same key expression and settings
// align with each other, wire-compatible with zenoh-rust's replicas (see
// replication.go).
//
// As in zenoh-rust: samples without a timestamp get one from the session;
// an older sample never replaces a newer one (deletes included: their
// events are remembered, in the replication log or, without replication,
// until GCLifespan); puts and deletes on wildcard keys apply to every
// stored key they cover and also override older puts that arrive later on
// such keys; wildcard updates are forgotten after GCLifespan.
package storage

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/shabunin/tiny-zenoh-go/keyexpr"
	"github.com/shabunin/tiny-zenoh-go/zenoh"
)

// Config is one entry of zenoh's plugins/storage_manager/storages section.
type Config struct {
	// KeyExpr is the key expression the storage subscribes to and answers
	// queries on.
	KeyExpr string `json:"key_expr"`
	// StripPrefix, if set, must be a non-wildcard prefix of KeyExpr; keys
	// are stored (and replicated) without it.
	StripPrefix string `json:"strip_prefix,omitempty"`
	// Complete declares the queryable complete (it has every key under
	// KeyExpr).
	Complete bool `json:"complete,omitempty"`
	// GCPeriod and GCLifespan are zenoh's garbage_collection settings:
	// default 30s and 24h.
	GCPeriod   time.Duration `json:"-"`
	GCLifespan time.Duration `json:"-"`
	// Replication, if set, aligns this storage with the other replicas of
	// it (same KeyExpr, StripPrefix and replication settings).
	Replication *ReplicationConfig `json:"-"`
}

// ReplicationConfig is zenoh's storage "replication" section.
type ReplicationConfig struct {
	// Interval is how often digests are published (default 10s); time is
	// cut into intervals of this length to compare replicas.
	Interval time.Duration
	// SubIntervals per interval (default 5).
	SubIntervals int
	// Hot and Warm are how many intervals the hot era (compared per
	// sub-interval) and warm era (compared per interval) span: default 6
	// and 30. Anything older is the cold era, compared as one fingerprint.
	Hot, Warm uint64
	// PropagationDelay is waited before each digest so that updates in
	// flight reach every replica first (default 250ms); at most half the
	// interval.
	PropagationDelay time.Duration
}

func (r *ReplicationConfig) defaults() error {
	if r.Interval <= 0 {
		r.Interval = 10 * time.Second
	}
	if r.SubIntervals <= 0 {
		r.SubIntervals = 5
	}
	if r.Hot == 0 {
		r.Hot = 6
	}
	if r.Warm == 0 {
		r.Warm = 30
	}
	if r.PropagationDelay <= 0 {
		r.PropagationDelay = 250 * time.Millisecond
	}
	if r.Interval.Milliseconds() < int64(r.SubIntervals) {
		return fmt.Errorf("storage: replication interval %v is shorter than %d ms", r.Interval, r.SubIntervals)
	}
	if r.Interval-r.PropagationDelay < r.PropagationDelay {
		return fmt.Errorf("storage: replication propagation_delay %v is more than half the interval %v",
			r.PropagationDelay, r.Interval)
	}
	return nil
}

type stored struct {
	payload  []byte
	encoding zenoh.Encoding
	ts       zenoh.Timestamp
}

type update struct {
	kind zenoh.SampleKind
	stored
}

// Memory is a running in-memory storage.
type Memory struct {
	s    *zenoh.Session
	cfg  Config
	sub  *zenoh.Subscriber
	qbl  *zenoh.Queryable
	repl *replicator
	stop chan struct{}
	once sync.Once

	mu       sync.Mutex
	data     map[string]*stored // by stripped key ("" is the strip prefix itself)
	wildPuts map[string]*update // by wildcard key expression
	wildDels map[string]*update
	latest   map[logKey]*event // recent events (drained into the log with replication)
	log      *replLog          // nil without replication
}

// NewMemory starts a storage on s.
func NewMemory(s *zenoh.Session, cfg Config) (*Memory, error) {
	if err := keyexpr.Validate(cfg.KeyExpr); err != nil {
		return nil, fmt.Errorf("storage: key_expr: %w", err)
	}
	if p := cfg.StripPrefix; p != "" {
		if !keyexpr.IsConcrete(p) {
			return nil, fmt.Errorf("storage: strip_prefix %q contains a wildcard", p)
		}
		if !strings.HasPrefix(cfg.KeyExpr, p) {
			return nil, fmt.Errorf("storage: strip_prefix %q is not a prefix of key_expr %q", p, cfg.KeyExpr)
		}
	}
	if cfg.GCPeriod <= 0 {
		cfg.GCPeriod = 30 * time.Second
	}
	if cfg.GCLifespan <= 0 {
		cfg.GCLifespan = 24 * time.Hour
	}
	m := &Memory{s: s, cfg: cfg, stop: make(chan struct{}),
		data: map[string]*stored{}, wildPuts: map[string]*update{}, wildDels: map[string]*update{},
		latest: map[logKey]*event{}}
	if cfg.Replication != nil {
		rc := *cfg.Replication
		if err := rc.defaults(); err != nil {
			return nil, err
		}
		m.cfg.Replication = &rc
		m.log = newReplLog(cfg.KeyExpr, cfg.StripPrefix, rc)
	}
	var err error
	if m.sub, err = s.DeclareSubscriber(cfg.KeyExpr, m.onSample); err != nil {
		return nil, err
	}
	if m.qbl, err = s.DeclareQueryable(cfg.KeyExpr, m.onQuery, zenoh.WithComplete(cfg.Complete)); err != nil {
		_ = m.sub.Undeclare()
		return nil, err
	}
	if m.log != nil {
		if m.repl, err = startReplication(m); err != nil {
			_ = m.sub.Undeclare()
			_ = m.qbl.Undeclare()
			return nil, err
		}
	}
	go m.gcLoop()
	return m, nil
}

// Close stops the storage; its content is lost.
func (m *Memory) Close() error {
	var err error
	m.once.Do(func() {
		close(m.stop)
		err = errors.Join(m.sub.Undeclare(), m.qbl.Undeclare())
		if m.repl != nil {
			err = errors.Join(err, m.repl.close())
		}
	})
	return err
}

// Len reports how many keys hold a value.
func (m *Memory) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.data)
}

// tsLess is uhlc's timestamp order: time, then the ID's 16 little-endian
// bytes.
func tsLess(a, b zenoh.Timestamp) bool {
	if a.Time != b.Time {
		return a.Time < b.Time
	}
	x, y := idBytes(a.ID), idBytes(b.ID)
	return string(x[:]) < string(y[:])
}

func tsEqual(a, b zenoh.Timestamp) bool { return a.Time == b.Time && idBytes(a.ID) == idBytes(b.ID) }

// idBytes is a uhlc ID: the ZID's little-endian bytes, zero padded to 16.
func idBytes(z zenoh.ZID) [16]byte {
	var k [16]byte
	copy(k[:], z.Bytes())
	return k
}

// strip removes the strip prefix from a full key (zenoh's strip_prefix):
// has is false when the key is the prefix itself.
func (m *Memory) strip(full string) (key string, has bool, err error) {
	p := m.cfg.StripPrefix
	switch {
	case p == "":
		return full, true, nil
	case full == p:
		return "", false, nil
	case strings.HasPrefix(full, p+"/"):
		return full[len(p)+1:], true, nil
	}
	return "", false, fmt.Errorf("storage: %q does not start with strip_prefix %q", full, p)
}

// full puts the strip prefix back (zenoh's prefix).
func (m *Memory) full(key string, has bool) string {
	switch p := m.cfg.StripPrefix; {
	case p == "":
		return key
	case !has:
		return p
	default:
		return p + "/" + key
	}
}

func (m *Memory) onSample(smp zenoh.Sample) {
	ts := m.s.NewTimestamp()
	if smp.Timestamp != nil {
		ts = *smp.Timestamp
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.processLocked(smp.KeyExpr, smp.Kind, smp.Payload, smp.Encoding, ts)
}

// processLocked is zenoh's StorageService::process_sample.
func (m *Memory) processLocked(ke string, kind zenoh.SampleKind, payload []byte, enc zenoh.Encoding, ts zenoh.Timestamp) {
	act := actionOf(kind)
	var keys []string
	if !keyexpr.IsConcrete(ke) {
		m.registerWildLocked(ke, kind, ts, payload, enc)
		act = action{kind: actWildPut, wild: ke}
		if kind == zenoh.SampleKindDelete {
			act.kind = actWildDel
		}
		ev := newEvent(ke, true, ts, act)
		m.latest[ev.logKey()] = ev
		for k := range m.data {
			if full := m.full(k, k != ""); keyexpr.Intersect(ke, full) {
				keys = append(keys, full)
			}
		}
		sort.Strings(keys)
	} else {
		keys = []string{ke}
	}
	for _, k := range keys {
		toStore := update{kind: kind, stored: stored{payload, enc, ts}}
		if _, u := m.overridingLocked(k, ts, nil, actionOf(kind)); u != nil {
			toStore = *u
		}
		sk, has, err := m.strip(k)
		if err != nil {
			continue
		}
		evAct := act
		if keyexpr.IsConcrete(ke) {
			evAct = actionOf(toStore.kind)
		}
		ev := newEvent(sk, has, toStore.ts, evAct)
		if !m.isLatestLocked(ev) {
			continue
		}
		if toStore.kind == zenoh.SampleKindPut {
			st := toStore.stored
			m.data[sk] = &st
		} else {
			delete(m.data, sk)
		}
		m.latest[ev.logKey()] = ev
	}
}

// isLatestLocked reports whether nothing newer than ev is known for its
// key (zenoh's guard_cache_if_latest, with a newer cached event, deletes
// included, winning).
func (m *Memory) isLatestLocked(ev *event) bool {
	if cached, ok := m.latest[ev.logKey()]; ok {
		return tsLess(cached.ts, ev.ts)
	}
	if m.log != nil {
		return m.log.lookupNewer(&ev.eventMeta) == nil
	}
	if d, ok := m.data[ev.key]; ok {
		return !tsLess(ev.ts, d.ts)
	}
	return true
}

func (m *Memory) registerWildLocked(ke string, kind zenoh.SampleKind, ts zenoh.Timestamp, payload []byte, enc zenoh.Encoding) {
	u := &update{kind: kind, stored: stored{payload, enc, ts}}
	if kind == zenoh.SampleKindPut {
		m.wildPuts[ke] = u
	} else {
		m.wildDels[ke] = u
	}
}

// overridingLocked is zenoh's overriding_wild_update: for a put, delete or
// wildcard delete, the earliest wildcard delete on an intersecting key not
// older than the event (or than its last non-wildcard update); else, for a
// put or wildcard put, the latest wildcard put not older than it.
func (m *Memory) overridingLocked(key string, ts zenoh.Timestamp, lastNonWild *zenoh.Timestamp, act action) (string, *update) {
	if act.kind == actPut || act.kind == actDelete || act.kind == actWildDel {
		lowest := ts
		if lastNonWild != nil {
			lowest = *lastNonWild
		}
		var best *update
		var bestKe string
		for _, k := range sortedKeys(m.wildDels) {
			d := m.wildDels[k]
			if keyexpr.Intersect(k, key) && !tsLess(d.ts, lowest) && (best == nil || tsLess(d.ts, best.ts)) {
				best, bestKe = d, k
			}
		}
		if best != nil {
			return bestKe, best
		}
	}
	if act.kind == actPut || act.kind == actWildPut {
		latest := ts
		var best *update
		var bestKe string
		for _, k := range sortedKeys(m.wildPuts) {
			p := m.wildPuts[k]
			if keyexpr.Intersect(k, key) && !tsLess(p.ts, latest) {
				latest, best, bestKe = p.ts, p, k
			}
		}
		if best != nil {
			return bestKe, best
		}
	}
	return "", nil
}

func sortedKeys[V any](m map[string]V) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

func (m *Memory) onQuery(q *zenoh.Query) {
	type hit struct {
		key string
		st  stored
	}
	var hits []hit
	m.mu.Lock()
	if keyexpr.IsConcrete(q.KeyExpr()) {
		if sk, _, err := m.strip(q.KeyExpr()); err == nil {
			if st, ok := m.data[sk]; ok {
				hits = append(hits, hit{q.KeyExpr(), *st})
			}
		}
	} else {
		for k, st := range m.data {
			if full := m.full(k, k != ""); keyexpr.Intersect(q.KeyExpr(), full) {
				hits = append(hits, hit{full, *st})
			}
		}
	}
	m.mu.Unlock()
	for _, h := range hits {
		_ = q.Reply(h.key, h.st.payload, zenoh.WithEncoding(h.st.encoding), zenoh.WithTimestamp(h.st.ts))
	}
}

func (m *Memory) gcLoop() {
	t := time.NewTicker(m.cfg.GCPeriod)
	defer t.Stop()
	for {
		select {
		case <-m.stop:
			return
		case now := <-t.C:
			m.gc(now)
		}
	}
}

// gc forgets wildcard updates and (without replication) cached events
// older than GCLifespan, by their timestamps.
func (m *Memory) gc(now time.Time) {
	limit := ntpMillis(zenohNTP(now)) - uint64(m.cfg.GCLifespan.Milliseconds())
	old := func(ts zenoh.Timestamp) bool { return ntpMillis(ts.Time) < limit }
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, w := range []map[string]*update{m.wildPuts, m.wildDels} {
		for k, u := range w {
			if old(u.ts) {
				delete(w, k)
			}
		}
	}
	if m.log == nil {
		for k, ev := range m.latest {
			if old(ev.ts) {
				delete(m.latest, k)
			}
		}
	}
}
