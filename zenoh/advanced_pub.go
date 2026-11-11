package zenoh

import (
	"encoding/binary"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Advanced pub/sub, wire-compatible with zenoh-rust's zenoh-ext
// (advanced_publisher.rs, advanced_cache.rs, advanced_subscriber.rs).
//
// An advanced publisher on key K uses the suffix
// S = "@adv/pub/<zid>/<eid>/<meta>" ("uhlc" instead of <eid> when samples are
// ordered by timestamp rather than sequence number, "_" when no metadata):
//   - with miss detection every sample carries a SourceInfo {zid, eid, sn},
//     sn counting from 0, and an optional heartbeat publishes the last sn
//     (u32 LE) on K/S;
//   - with a cache it keeps the last samples and answers queries on K/S,
//     honoring the "_sn=a..b", "_max=n" and "_time=[..]" parameters;
//   - with publisher detection it holds the liveliness token K/S.
// "@adv" is a verbatim chunk, so subscribers on K never see that traffic.

const (
	advPrefix = "@adv"
	advEmpty  = "_"
	advUHLC   = "uhlc"
)

// CacheConfig sizes an advanced publisher's cache.
type CacheConfig struct {
	MaxSamples int // samples kept (default 1)
}

// MissDetectionConfig turns on sequence numbers so subscribers can detect
// and recover lost samples.
type MissDetectionConfig struct {
	// Heartbeat, if set, publishes the last sequence number periodically so
	// subscribers learn about lost last samples.
	Heartbeat time.Duration
	// Sporadic heartbeats are sent only when something was published since
	// the previous one.
	Sporadic bool
}

// AdvancedPublisherConfig selects the advanced features of a publisher.
type AdvancedPublisherConfig struct {
	Cache              *CacheConfig
	MissDetection      *MissDetectionConfig
	PublisherDetection bool   // declare a liveliness token
	Metadata           string // key expression appended to the advanced suffix
}

// AdvancedPublisher is a publisher with a cache for late joiners, sequence
// numbers for miss detection and liveliness for publisher detection.
type AdvancedPublisher struct {
	*Publisher
	eid     uint32
	suffix  string
	seq     bool
	mu      sync.Mutex
	sn      uint32 // next sequence number
	cache   []Sample
	max     int
	qbl     *Queryable
	token   *LivelinessToken
	stopHB  chan struct{}
	stopped sync.Once
}

func joinKey(parts ...string) string { return strings.Join(parts, "/") }

// DeclareAdvancedPublisher declares an advanced publisher on key; opts become
// the defaults of its puts.
func (s *Session) DeclareAdvancedPublisher(key string, conf AdvancedPublisherConfig, opts ...PutOption) (*AdvancedPublisher, error) {
	if conf.Cache != nil && conf.Cache.MaxSamples < 0 {
		return nil, errAdv("cache max samples must be positive")
	}
	meta := advEmpty
	if conf.Metadata != "" {
		k, err := validKey(conf.Metadata)
		if err != nil {
			return nil, err
		}
		meta = k.String()
	}
	pub, err := s.DeclarePublisher(key, opts...)
	if err != nil {
		return nil, err
	}
	ap := &AdvancedPublisher{Publisher: pub, eid: s.entityID(), seq: conf.MissDetection != nil}
	id := advUHLC
	if ap.seq {
		id = strconv.FormatUint(uint64(ap.eid), 10)
	}
	ap.suffix = joinKey(advPrefix, "pub", s.zid.String(), id, meta)
	full := joinKey(pub.key, ap.suffix)
	fail := func(err error) (*AdvancedPublisher, error) {
		_ = ap.Undeclare()
		return nil, err
	}
	if conf.Cache != nil {
		ap.max = conf.Cache.MaxSamples
		if ap.max == 0 {
			ap.max = 1
		}
		if ap.qbl, err = s.DeclareQueryable(full, ap.answer); err != nil {
			return fail(err)
		}
	}
	if conf.PublisherDetection {
		if ap.token, err = s.Liveliness().DeclareToken(full); err != nil {
			return fail(err)
		}
	}
	if md := conf.MissDetection; md != nil && md.Heartbeat > 0 {
		hb, err := s.DeclarePublisher(full)
		if err != nil {
			return fail(err)
		}
		ap.stopHB = make(chan struct{})
		go ap.heartbeat(hb, md.Heartbeat, md.Sporadic)
	}
	return ap, nil
}

type errAdv string

func (e errAdv) Error() string { return "zenoh: " + string(e) }

func (ap *AdvancedPublisher) heartbeat(hb *Publisher, period time.Duration, sporadic bool) {
	defer hb.Undeclare()
	t := time.NewTicker(period)
	defer t.Stop()
	var last uint32
	for {
		select {
		case <-ap.stopHB:
			return
		case <-ap.s.done:
			return
		case <-t.C:
		}
		ap.mu.Lock()
		sn := ap.sn
		ap.mu.Unlock()
		if sn == 0 || (sporadic && sn <= last) {
			continue
		}
		last = sn
		var b [4]byte
		binary.LittleEndian.PutUint32(b[:], sn-1)
		var o []PutOption
		if sporadic {
			o = append(o, WithCongestionControl(CongestionBlock))
		}
		_ = hb.Put(b[:], o...)
	}
}

// Put publishes payload.
func (ap *AdvancedPublisher) Put(payload []byte, opts ...PutOption) error {
	return ap.publishAdv(SampleKindPut, payload, opts)
}

// Delete publishes a deletion.
func (ap *AdvancedPublisher) Delete(opts ...PutOption) error {
	return ap.publishAdv(SampleKindDelete, nil, opts)
}

func (ap *AdvancedPublisher) publishAdv(kind SampleKind, payload []byte, opts []PutOption) error {
	o := ap.prepare(kind, payload, opts)
	return ap.s.publish(ap.key, ap.wire(), kind, payload, &o)
}

// prepare stamps a publication, numbers it and caches it.
func (ap *AdvancedPublisher) prepare(kind SampleKind, payload []byte, opts []PutOption) putOptions {
	o := ap.options(opts)
	if o.timestamp == nil {
		ts := ap.s.NewTimestamp() // as zenoh-rust does when the session has an HLC
		o.timestamp = &ts
	}
	ap.mu.Lock()
	if ap.seq {
		o.sourceInfo = &SourceInfo{ZID: ap.s.zid, EID: ap.eid, SN: ap.sn}
		ap.sn++
	}
	if ap.qbl != nil {
		smp := Sample{KeyExpr: ap.key, Kind: kind, Payload: payload, Encoding: o.encoding,
			Timestamp: o.timestamp, Attachment: o.attachment, SourceInfo: o.sourceInfo,
			Priority: o.priority, Congestion: o.congestion, Express: o.express}
		if len(ap.cache) >= ap.max {
			ap.cache = append(ap.cache[:0], ap.cache[1:]...)
		}
		ap.cache = append(ap.cache, smp)
	}
	ap.mu.Unlock()
	return o
}

// snLess orders wrapping u32 sequence numbers as zenoh-ext's WrappingSn.
func snLess(a, b uint32) bool { return a != b && b-a <= math.MaxUint32/2 }

// snRange is a parsed "_sn=a..b" (a single "a" means a..a).
type snRange struct {
	from, to       uint32
	hasFrom, hasTo bool
}

func parseSnRange(v string) snRange {
	var r snRange
	a, b, two := strings.Cut(v, "..")
	if n, err := strconv.ParseUint(a, 10, 32); err == nil {
		r.from, r.hasFrom = uint32(n), true
	}
	if !two {
		r.to, r.hasTo = r.from, r.hasFrom
		return r
	}
	if n, err := strconv.ParseUint(b, 10, 32); err == nil {
		r.to, r.hasTo = uint32(n), true
	}
	return r
}

func (r snRange) contains(sn uint32) bool {
	return (!r.hasFrom || !snLess(sn, r.from)) && (!r.hasTo || !snLess(r.to, sn))
}

// answer replies to a query on the cache, as zenoh-ext's AdvancedCache.
func (ap *AdvancedPublisher) answer(q *Query) {
	params := q.Parameters()
	v, _ := Param(params, "_sn")
	rng := parseSnRange(v)
	bounded := rng.hasFrom || rng.hasTo
	var tr *TimeRange
	if v, ok := Param(params, "_time"); ok {
		if r, err := ParseTimeRange(v); err == nil {
			tr = &r
		}
	}
	max := -1
	if v, ok := Param(params, "_max"); ok {
		if n, err := strconv.ParseUint(v, 10, 32); err == nil {
			max = int(n)
		}
	}
	ap.mu.Lock()
	var out []Sample
	for _, smp := range ap.cache {
		if bounded && (smp.SourceInfo == nil || !rng.contains(smp.SourceInfo.SN)) {
			continue
		}
		if tr != nil && smp.Timestamp != nil && !tr.Contains(smp.Timestamp.GoTime()) {
			continue
		}
		out = append(out, smp)
	}
	ap.mu.Unlock()
	if max >= 0 && len(out) > max {
		out = out[len(out)-max:] // the most recent ones
	}
	for _, smp := range out {
		opts := []PutOption{WithEncoding(smp.Encoding), WithAttachment(smp.Attachment),
			WithPriority(PriorityData), WithCongestionControl(CongestionBlock)}
		if smp.Timestamp != nil {
			opts = append(opts, WithTimestamp(*smp.Timestamp))
		}
		if smp.SourceInfo != nil {
			opts = append(opts, WithSourceInfo(*smp.SourceInfo))
		}
		if smp.Kind == SampleKindDelete {
			_ = q.ReplyDelete(smp.KeyExpr, opts...)
		} else {
			_ = q.Reply(smp.KeyExpr, smp.Payload, opts...)
		}
	}
}

// ID returns the publisher's global entity id.
func (ap *AdvancedPublisher) ID() EntityGlobalID { return EntityGlobalID{ZID: ap.s.zid, EID: ap.eid} }

// Undeclare stops the publisher, its cache, heartbeat and token.
func (ap *AdvancedPublisher) Undeclare() error {
	ap.stopped.Do(func() {
		if ap.stopHB != nil {
			close(ap.stopHB)
		}
		if ap.qbl != nil {
			_ = ap.qbl.Undeclare()
		}
		if ap.token != nil {
			_ = ap.token.Undeclare()
		}
	})
	return ap.Publisher.Undeclare()
}
