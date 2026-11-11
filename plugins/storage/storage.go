// Package storage is zenoh's storage manager with its "memory" volume
// (zenoh-rust plugins/zenoh-plugin-storage-manager): a storage subscribes
// to a key expression, keeps the latest value of every key under it, and
// answers queries on it with those values and their timestamps.
//
// As in zenoh-rust: samples without a timestamp get one from the session;
// an older sample never replaces a newer one; deletes leave a tombstone so
// that late older puts stay deleted; puts and deletes on wildcard keys
// apply to every stored key they cover and also override older puts that
// arrive later on such keys; tombstones and wildcard updates are forgotten
// after GCLifespan. Replication (alignment between storages) is not
// implemented.
package storage

import (
	"bytes"
	"errors"
	"fmt"
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
	// StripPrefix, if set, must be a non-wildcard prefix of KeyExpr; zenoh's
	// backends store keys without it. It changes nothing seen from outside.
	StripPrefix string `json:"strip_prefix,omitempty"`
	// Complete declares the queryable complete (it has every key under
	// KeyExpr).
	Complete bool `json:"complete,omitempty"`
	// GCPeriod and GCLifespan are zenoh's garbage_collection settings:
	// default 30s and 24h.
	GCPeriod   time.Duration `json:"-"`
	GCLifespan time.Duration `json:"-"`
}

type update struct {
	kind     zenoh.SampleKind
	payload  []byte
	encoding zenoh.Encoding
	ts       zenoh.Timestamp
	at       time.Time // when it arrived (garbage collection)
}

// Memory is a running in-memory storage.
type Memory struct {
	s    *zenoh.Session
	cfg  Config
	sub  *zenoh.Subscriber
	qbl  *zenoh.Queryable
	stop chan struct{}
	once sync.Once

	mu       sync.Mutex
	data     map[string]*update // concrete keys, deletes kept as tombstones
	wildPuts map[string]*update
	wildDels map[string]*update
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
		if cfg.KeyExpr != p && !strings.HasPrefix(cfg.KeyExpr, p+"/") {
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
		data: map[string]*update{}, wildPuts: map[string]*update{}, wildDels: map[string]*update{}}
	var err error
	if m.sub, err = s.DeclareSubscriber(cfg.KeyExpr, m.onSample); err != nil {
		return nil, err
	}
	if m.qbl, err = s.DeclareQueryable(cfg.KeyExpr, m.onQuery, zenoh.WithComplete(cfg.Complete)); err != nil {
		_ = m.sub.Undeclare()
		return nil, err
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
	})
	return err
}

// Len reports how many keys hold a value (tombstones excluded).
func (m *Memory) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, u := range m.data {
		if u.kind == zenoh.SampleKindPut {
			n++
		}
	}
	return n
}

// newer reports whether a is a later timestamp than b (uhlc order: time,
// then id).
func newer(a, b zenoh.Timestamp) bool {
	if a.Time != b.Time {
		return a.Time > b.Time
	}
	return bytes.Compare(zidKey(a.ID), zidKey(b.ID)) > 0
}

// zidKey is uhlc's ID order: the 16 little-endian bytes, zero padded.
func zidKey(z zenoh.ZID) []byte {
	var k [16]byte
	copy(k[:], z.Bytes())
	return k[:]
}

func (m *Memory) onSample(smp zenoh.Sample) {
	u := &update{kind: smp.Kind, payload: smp.Payload, encoding: smp.Encoding, at: time.Now()}
	if smp.Timestamp != nil {
		u.ts = *smp.Timestamp
	} else {
		u.ts = m.s.NewTimestamp()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if !keyexpr.IsConcrete(smp.KeyExpr) {
		wild := m.wildPuts
		if u.kind == zenoh.SampleKindDelete {
			wild = m.wildDels
		}
		if prev := wild[smp.KeyExpr]; prev == nil || newer(u.ts, prev.ts) {
			wild[smp.KeyExpr] = u
		}
		for key := range m.data {
			if keyexpr.Intersect(smp.KeyExpr, key) {
				m.applyLocked(key, u)
			}
		}
		return
	}
	if w := m.overridingLocked(smp.KeyExpr, u.ts); w != nil {
		u = w
	}
	m.applyLocked(smp.KeyExpr, u)
}

func (m *Memory) applyLocked(key string, u *update) {
	if prev := m.data[key]; prev != nil && !newer(u.ts, prev.ts) {
		return
	}
	m.data[key] = u
}

// overridingLocked finds the wildcard update that wins over a sample on
// key at ts: the earliest wildcard delete not older than it, else the
// latest wildcard put not older than it.
func (m *Memory) overridingLocked(key string, ts zenoh.Timestamp) *update {
	var best *update
	for k, d := range m.wildDels {
		if keyexpr.Intersect(k, key) && !newer(ts, d.ts) && (best == nil || newer(best.ts, d.ts)) {
			best = d
		}
	}
	if best != nil {
		return best
	}
	for k, p := range m.wildPuts {
		if keyexpr.Intersect(k, key) && !newer(ts, p.ts) && (best == nil || newer(p.ts, best.ts)) {
			best = p
		}
	}
	return best
}

func (m *Memory) onQuery(q *zenoh.Query) {
	type hit struct {
		key string
		u   *update
	}
	var hits []hit
	m.mu.Lock()
	for key, u := range m.data {
		if u.kind == zenoh.SampleKindPut && keyexpr.Intersect(q.KeyExpr(), key) {
			hits = append(hits, hit{key, u})
		}
	}
	m.mu.Unlock()
	for _, h := range hits {
		_ = q.Reply(h.key, h.u.payload, zenoh.WithEncoding(h.u.encoding), zenoh.WithTimestamp(h.u.ts))
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

func (m *Memory) gc(now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	old := func(u *update) bool { return now.Sub(u.at) > m.cfg.GCLifespan }
	for k, u := range m.data {
		if u.kind == zenoh.SampleKindDelete && old(u) {
			delete(m.data, k)
		}
	}
	for _, w := range []map[string]*update{m.wildPuts, m.wildDels} {
		for k, u := range w {
			if old(u) {
				delete(w, k)
			}
		}
	}
}
