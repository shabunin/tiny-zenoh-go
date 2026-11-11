package zenoh

import (
	"encoding/binary"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/shabunin/tiny-zenoh-go/keyexpr"
	"github.com/shabunin/tiny-zenoh-go/protocol"
)

// HistoryConfig asks advanced publishers' caches for past samples.
type HistoryConfig struct {
	// DetectLatePublishers also queries publishers that appear later (needs
	// their PublisherDetection).
	DetectLatePublishers bool
	MaxSamples           int           // per publisher, 0 = all cached
	MaxAge               time.Duration // 0 = any age
}

// RecoveryConfig retransmits lost samples from the publishers' caches (needs
// their MissDetection and a cache).
type RecoveryConfig struct {
	// PeriodicQueries asks every publisher for newer samples at this period;
	// Heartbeat instead relies on the publishers' heartbeats. With neither,
	// only gaps in received sequence numbers trigger queries.
	PeriodicQueries time.Duration
	Heartbeat       bool
	// RetentionPeriod is how long the state of a silent publisher is kept
	// (default 1h).
	RetentionPeriod time.Duration
}

// AdvancedSubscriberConfig selects the advanced features of a subscriber.
type AdvancedSubscriberConfig struct {
	History             *HistoryConfig
	Recovery            *RecoveryConfig
	QueryTimeout        time.Duration // default 10s
	QueryTarget         QueryTarget   // default TargetAll
	SubscriberDetection bool          // declare a liveliness token
	Metadata            string        // appended to the token's key
	targetSet           bool
}

// WithQueryTarget sets the target of the subscriber's queries.
func (c AdvancedSubscriberConfig) WithQueryTarget(t QueryTarget) AdvancedSubscriberConfig {
	c.QueryTarget, c.targetSet = t, true
	return c
}

// Miss reports samples from a source that were lost for good.
type Miss struct {
	Source EntityGlobalID
	Count  uint32
}

type srcID struct {
	zid string // ZID bytes
	eid uint32
}

type seqSource struct {
	zid      ZID
	last     uint32
	hasLast  bool
	queries  int
	pending  map[uint32]Sample
	access   time.Time
	alive    bool
	periodic chan struct{} // closes the periodic query loop
}

type tsSource struct {
	last    *Timestamp
	queries int
	pending []Sample // ordered by timestamp
	access  time.Time
	alive   bool
}

// AdvancedSubscriber delivers samples from advanced publishers in order,
// with history and recovery of lost samples. Its callbacks run with the
// subscriber's lock held: they must not call back into it.
type AdvancedSubscriber struct {
	s       *Session
	key     string
	eid     uint32
	sub     *Subscriber
	liv     *LivelinessSubscriber
	hb      *Subscriber
	token   *LivelinessToken
	gcStop  chan struct{}
	timeout time.Duration
	target  QueryTarget
	hist    *HistoryConfig
	recov   *RecoveryConfig

	mu       sync.Mutex
	cb       func(Sample)
	global   int // initial queries still running
	maxDepth int
	seq      map[srcID]*seqSource
	ts       map[string]*tsSource
	misses   map[*MissListener]func(Miss)
}

// MissListener is notified of lost samples.
type MissListener struct{ as *AdvancedSubscriber }

// DeclareAdvancedSubscriber subscribes to key with the advanced features of
// conf.
func (s *Session) DeclareAdvancedSubscriber(key string, handler func(Sample), conf AdvancedSubscriberConfig) (*AdvancedSubscriber, error) {
	k, err := validKey(key)
	if err != nil {
		return nil, err
	}
	if h := conf.History; h != nil && (h.MaxSamples < 0 || h.MaxAge < 0) {
		return nil, errAdv("history limits must be positive")
	}
	meta := advEmpty
	if conf.Metadata != "" {
		m, err := validKey(conf.Metadata)
		if err != nil {
			return nil, err
		}
		meta = m.String()
	}
	as := &AdvancedSubscriber{
		s: s, key: k.String(), eid: s.entityID(), cb: handler,
		timeout: conf.QueryTimeout, target: TargetAll, hist: conf.History, recov: conf.Recovery,
		maxDepth: int(^uint(0) >> 1),
		seq:      map[srcID]*seqSource{}, ts: map[string]*tsSource{},
		misses: map[*MissListener]func(Miss){}, gcStop: make(chan struct{}),
	}
	if as.timeout == 0 {
		as.timeout = 10 * time.Second
	}
	if conf.targetSet {
		as.target = conf.QueryTarget
	}
	if conf.History != nil {
		as.global = 1
		if conf.History.MaxSamples > 0 {
			as.maxDepth = conf.History.MaxSamples
		}
	}
	fail := func(err error) (*AdvancedSubscriber, error) {
		_ = as.Undeclare()
		return nil, err
	}
	if as.sub, err = s.DeclareSubscriber(as.key, as.onSample); err != nil {
		return nil, err
	}
	pubs := joinKey(as.key, advPrefix, "pub", "**")
	if conf.History != nil {
		as.query(joinKey(as.key, advPrefix, "**"), as.histParams(), as.initialDone)
		if conf.History.DetectLatePublishers {
			if as.liv, err = s.Liveliness().DeclareSubscriber(pubs, as.onPublisher, true); err != nil {
				return fail(err)
			}
		}
	}
	if conf.Recovery != nil && conf.Recovery.Heartbeat {
		if as.hb, err = s.DeclareSubscriber(pubs, as.onHeartbeat); err != nil {
			return fail(err)
		}
	}
	if conf.SubscriberDetection {
		tok := joinKey(as.key, advPrefix, "sub", s.zid.String(), strconv.FormatUint(uint64(as.eid), 10), meta)
		if as.token, err = s.Liveliness().DeclareToken(tok); err != nil {
			return fail(err)
		}
	}
	retention := time.Hour
	if conf.Recovery != nil && conf.Recovery.RetentionPeriod > 0 {
		retention = conf.Recovery.RetentionPeriod
	}
	go as.gc(retention)
	return as, nil
}

// KeyExpr returns the subscriber's key expression.
func (as *AdvancedSubscriber) KeyExpr() string { return as.key }

// ID returns the subscriber's global entity id.
func (as *AdvancedSubscriber) ID() EntityGlobalID { return EntityGlobalID{ZID: as.s.zid, EID: as.eid} }

// DeclareMissListener calls cb when samples are lost for good.
func (as *AdvancedSubscriber) DeclareMissListener(cb func(Miss)) *MissListener {
	l := &MissListener{as: as}
	as.mu.Lock()
	as.misses[l] = cb
	as.mu.Unlock()
	return l
}

// Undeclare stops the listener.
func (l *MissListener) Undeclare() {
	l.as.mu.Lock()
	delete(l.as.misses, l)
	l.as.mu.Unlock()
}

// DetectPublishers subscribes to the liveliness of the advanced publishers
// matching the subscriber (those with PublisherDetection).
func (as *AdvancedSubscriber) DetectPublishers(handler func(Sample), history bool) (*LivelinessSubscriber, error) {
	return as.s.Liveliness().DeclareSubscriber(joinKey(as.key, advPrefix, "pub", "**"), handler, history)
}

// Undeclare stops the subscriber and everything it declared.
func (as *AdvancedSubscriber) Undeclare() error {
	as.mu.Lock()
	if as.cb == nil {
		as.mu.Unlock()
		return nil
	}
	as.cb = nil
	as.misses = map[*MissListener]func(Miss){}
	for _, st := range as.seq {
		st.stopPeriodic()
	}
	as.mu.Unlock()
	close(as.gcStop)
	var err error
	if as.sub != nil {
		err = as.sub.Undeclare()
	}
	if as.liv != nil {
		_ = as.liv.Undeclare()
	}
	if as.hb != nil {
		_ = as.hb.Undeclare()
	}
	if as.token != nil {
		_ = as.token.Undeclare()
	}
	return err
}

// -----------------------------------------------------------------------------
// Ordering (zenoh-ext handle_sample)
// -----------------------------------------------------------------------------

func (as *AdvancedSubscriber) seqSourceLocked(si *SourceInfo) (*seqSource, bool) {
	id := srcID{string(si.ZID.Bytes()), si.EID}
	if st := as.seq[id]; st != nil {
		return st, false
	}
	st := &seqSource{zid: si.ZID, pending: map[uint32]Sample{}, access: time.Now()}
	as.seq[id] = st
	return st, true
}

func (as *AdvancedSubscriber) missLocked(si *SourceInfo, n uint32) {
	m := Miss{Source: EntityGlobalID{ZID: si.ZID, EID: si.EID}, Count: n}
	for _, cb := range as.misses {
		cb(m)
	}
}

func (as *AdvancedSubscriber) deliverAndFlushLocked(smp Sample, sn uint32, st *seqSource) {
	as.cb(smp)
	st.last, st.hasLast = sn, true
	for {
		next, ok := st.pending[st.last+1]
		if !ok {
			return
		}
		delete(st.pending, st.last+1)
		as.cb(next)
		st.last++
	}
}

// sortedSNs lists the pending sequence numbers in wrapping order.
func (st *seqSource) sortedSNs() []uint32 {
	sns := make([]uint32, 0, len(st.pending))
	for sn := range st.pending {
		sns = append(sns, sn)
	}
	sort.Slice(sns, func(i, j int) bool { return snLess(sns[i], sns[j]) })
	return sns
}

// handleLocked processes a sample from the subscription or a reply; it
// reports whether its source was new.
func (as *AdvancedSubscriber) handleLocked(smp Sample) bool {
	if as.cb == nil {
		return false
	}
	if si := smp.SourceInfo; si != nil {
		st, isNew := as.seqSourceLocked(si)
		sn := si.SN
		switch {
		case !st.hasLast && as.global != 0:
			if as.maxDepth == 1 {
				st.last, st.hasLast = sn, true
				as.cb(smp)
			} else {
				st.pending[sn] = smp
				if len(st.pending) >= as.maxDepth {
					first := st.sortedSNs()[0]
					s0 := st.pending[first]
					delete(st.pending, first)
					as.deliverAndFlushLocked(s0, first, st)
				}
			}
		case st.hasLast && sn != st.last+1:
			if snLess(st.last, sn) {
				if as.recov != nil {
					st.pending[sn] = smp
				} else {
					as.missLocked(si, sn-st.last-1)
					as.cb(smp)
					st.last = sn
				}
			}
		default:
			as.deliverAndFlushLocked(smp, sn, st)
		}
		st.access = time.Now()
		return isNew
	}
	if ts := smp.Timestamp; ts != nil {
		id := string(ts.ID.Bytes())
		st := as.ts[id]
		if st == nil {
			st = &tsSource{}
			as.ts[id] = st
		}
		if st.last == nil || tsAfter(ts, st.last) {
			if (as.global == 0 && st.queries == 0) || as.maxDepth == 1 {
				st.last = ts
				as.cb(smp)
			} else {
				st.addPending(smp)
				if len(st.pending) >= as.maxDepth {
					as.flushTimestampedLocked(st)
				}
			}
		}
		st.access = time.Now()
		return false
	}
	as.cb(smp)
	return false
}

func (st *tsSource) addPending(smp Sample) {
	i := sort.Search(len(st.pending), func(i int) bool { return !tsAfter(smp.Timestamp, st.pending[i].Timestamp) })
	if i < len(st.pending) && !tsAfter(st.pending[i].Timestamp, smp.Timestamp) {
		return // same timestamp already pending
	}
	st.pending = append(st.pending, Sample{})
	copy(st.pending[i+1:], st.pending[i:])
	st.pending[i] = smp
}

func (as *AdvancedSubscriber) flushSequencedLocked(st *seqSource) {
	if as.cb == nil || st.queries != 0 || len(st.pending) == 0 {
		return
	}
	for _, sn := range st.sortedSNs() {
		smp := st.pending[sn]
		delete(st.pending, sn)
		switch {
		case !st.hasLast, sn == st.last+1:
			st.last, st.hasLast = sn, true
			as.cb(smp)
		case snLess(st.last+1, sn):
			as.missLocked(smp.SourceInfo, sn-st.last-1)
			st.last = sn
			as.cb(smp)
		}
	}
}

func (as *AdvancedSubscriber) flushTimestampedLocked(st *tsSource) {
	if as.cb == nil || st.queries != 0 || len(st.pending) == 0 {
		return
	}
	pending := st.pending
	st.pending = nil
	for _, smp := range pending {
		if st.last == nil || tsAfter(smp.Timestamp, st.last) {
			st.last = smp.Timestamp
			as.cb(smp)
		}
	}
}

// -----------------------------------------------------------------------------
// Queries
// -----------------------------------------------------------------------------

func (as *AdvancedSubscriber) histParams() []string {
	var p []string
	if h := as.hist; h != nil {
		if h.MaxSamples > 0 {
			p = append(p, "_max="+strconv.Itoa(h.MaxSamples))
		}
		if h.MaxAge > 0 {
			p = append(p, "_time="+SinceNow(h.MaxAge).String())
		}
	}
	return p
}

func snRangeParam(from *uint32, to *uint32) string {
	var a, b string
	if from != nil {
		a = strconv.FormatUint(uint64(*from), 10)
	}
	if to != nil {
		b = strconv.FormatUint(uint64(*to), 10)
	}
	return "_sn=" + a + ".." + b
}

// query sends a get whose matching replies go through handleLocked; done runs
// (with the lock held) when it completes. It must be called without the lock.
func (as *AdvancedSubscriber) query(key string, params []string, done func()) {
	params = append(params, anyKeyParam)
	sel := key + "?" + strings.Join(params, ";")
	cb := func(r Reply) {
		if r.Sample == nil || !intersects(as.key, r.Sample.KeyExpr) {
			return
		}
		as.mu.Lock()
		as.handleLocked(*r.Sample)
		as.mu.Unlock()
	}
	var once sync.Once // an error may follow a completion when the session closes
	fin := func() {
		once.Do(func() {
			as.mu.Lock()
			done()
			as.mu.Unlock()
		})
	}
	if err := as.s.GetFunc(sel, cb, fin, WithConsolidation(ConsolidationNone),
		WithTarget(as.target), WithTimeout(as.timeout)); err != nil {
		fin()
	}
}

// initialDone ends one of the history queries not tied to one source.
func (as *AdvancedSubscriber) initialDone() {
	if as.global > 0 {
		as.global--
	}
	if as.global != 0 {
		return
	}
	for id, st := range as.seq {
		as.flushSequencedLocked(st)
		as.startPeriodicLocked(id, st)
	}
	for _, st := range as.ts {
		as.flushTimestampedLocked(st)
	}
}

func (as *AdvancedSubscriber) seqDone(id srcID) func() {
	return func() {
		st := as.seq[id]
		if st == nil {
			return
		}
		if st.queries > 0 {
			st.queries--
		}
		if as.global == 0 {
			as.flushSequencedLocked(st)
		}
	}
}

func (as *AdvancedSubscriber) tsDone(id string) func() {
	return func() {
		st := as.ts[id]
		if st == nil {
			return
		}
		if st.queries > 0 {
			st.queries--
		}
		if as.global == 0 {
			as.flushTimestampedLocked(st)
		}
	}
}

// sourceKey is K/@adv/*/<zid>/<eid>/**: every advanced key of one source.
func (as *AdvancedSubscriber) sourceKey(st *seqSource, eid uint32) string {
	return joinKey(as.key, advPrefix, "*", st.zid.String(), strconv.FormatUint(uint64(eid), 10), "**")
}

func (st *seqSource) nextSN() *uint32 {
	if !st.hasLast {
		return nil
	}
	n := st.last + 1
	return &n
}

func (st *seqSource) stopPeriodic() {
	if st.periodic != nil {
		close(st.periodic)
		st.periodic = nil
	}
}

func (as *AdvancedSubscriber) startPeriodicLocked(id srcID, st *seqSource) {
	if as.recov == nil || as.recov.PeriodicQueries <= 0 || as.cb == nil {
		return
	}
	st.stopPeriodic()
	stop := make(chan struct{})
	st.periodic = stop
	go func() {
		t := time.NewTicker(as.recov.PeriodicQueries)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-as.s.done:
				return
			case <-t.C:
			}
			as.mu.Lock()
			st := as.seq[id]
			if st == nil || as.cb == nil {
				as.mu.Unlock()
				return
			}
			st.queries++
			key, params := as.sourceKey(st, id.eid), snRangeParam(st.nextSN(), nil)
			as.mu.Unlock()
			as.query(key, []string{params}, as.seqDone(id))
		}
	}()
}

// onSample handles the main subscription.
func (as *AdvancedSubscriber) onSample(smp Sample) {
	as.mu.Lock()
	isNew := as.handleLocked(smp)
	si := smp.SourceInfo
	if si == nil || as.cb == nil {
		as.mu.Unlock()
		return
	}
	id := srcID{string(si.ZID.Bytes()), si.EID}
	st := as.seq[id]
	if st == nil {
		as.mu.Unlock()
		return
	}
	if isNew {
		as.startPeriodicLocked(id, st)
	}
	if as.recov != nil && st.queries == 0 && len(st.pending) > 0 {
		st.queries++
		key, params := as.sourceKey(st, si.EID), snRangeParam(st.nextSN(), nil)
		as.mu.Unlock()
		as.query(key, []string{params}, as.seqDone(id))
		return
	}
	as.mu.Unlock()
}

// advToken is a parsed K/@adv/<entity>/<zid>/<eid>/<meta...> key.
type advToken struct {
	zid    string
	eid    string
	entity string
}

func parseAdvKey(key string) (advToken, bool) {
	parts := strings.Split(key, "/")
	for i, p := range parts {
		if p == advPrefix && len(parts) >= i+5 {
			return advToken{entity: parts[i+1], zid: parts[i+2], eid: parts[i+3]}, true
		}
	}
	return advToken{}, false
}

// onPublisher handles the liveliness of advanced publishers (late joiners).
func (as *AdvancedSubscriber) onPublisher(smp Sample) {
	tk, ok := parseAdvKey(smp.KeyExpr)
	if !ok {
		return
	}
	put := smp.Kind == SampleKindPut
	params := as.histParams()
	zid, err := protocol.ParseZID(tk.zid)
	as.mu.Lock()
	if as.cb == nil {
		as.mu.Unlock()
		return
	}
	switch {
	case err == nil && tk.eid == advUHLC:
		id := string(zid.Bytes())
		st := as.ts[id]
		if !put {
			if st != nil {
				st.alive = false
			}
			as.mu.Unlock()
			return
		}
		if st == nil {
			st = &tsSource{}
			as.ts[id] = st
		}
		st.queries++
		st.alive, st.access = true, time.Now()
		as.mu.Unlock()
		as.query(smp.KeyExpr, params, as.tsDone(id))
	case err == nil:
		eid, perr := strconv.ParseUint(tk.eid, 10, 32)
		if perr != nil {
			as.mu.Unlock()
			return
		}
		id := srcID{string(zid.Bytes()), uint32(eid)}
		if !put {
			if st := as.seq[id]; st != nil {
				st.alive = false
			}
			as.mu.Unlock()
			return
		}
		st, isNew := as.seqSourceLocked(&SourceInfo{ZID: zid, EID: uint32(eid)})
		if isNew {
			as.startPeriodicLocked(id, st)
		}
		st.queries++
		st.alive, st.access = true, time.Now()
		as.mu.Unlock()
		as.query(smp.KeyExpr, params, as.seqDone(id))
	case put:
		as.global++
		as.mu.Unlock()
		as.query(smp.KeyExpr, params, as.initialDone)
	default:
		as.mu.Unlock()
	}
}

// onHeartbeat handles a publisher's last sequence number.
func (as *AdvancedSubscriber) onHeartbeat(smp Sample) {
	if smp.Kind != SampleKindPut || len(smp.Payload) != 4 {
		return
	}
	tk, ok := parseAdvKey(smp.KeyExpr)
	if !ok {
		return
	}
	zid, err := protocol.ParseZID(tk.zid)
	if err != nil {
		return
	}
	eid, err := strconv.ParseUint(tk.eid, 10, 32)
	if err != nil {
		return
	}
	hbSN := binary.LittleEndian.Uint32(smp.Payload)
	id := srcID{string(zid.Bytes()), uint32(eid)}
	as.mu.Lock()
	if as.cb == nil {
		as.mu.Unlock()
		return
	}
	st, isNew := as.seqSourceLocked(&SourceInfo{ZID: zid, EID: uint32(eid)})
	st.access = time.Now()
	if isNew {
		as.startPeriodicLocked(id, st)
		if as.global > 0 {
			as.mu.Unlock()
			return
		}
	}
	if (!st.hasLast || snLess(st.last, hbSN)) && st.queries == 0 {
		params := snRangeParam(st.nextSN(), &hbSN)
		st.queries++
		as.mu.Unlock()
		as.query(smp.KeyExpr, []string{params}, as.seqDone(id))
		return
	}
	as.mu.Unlock()
}

// gc forgets sources silent for longer than the retention period, unless
// their publisher is known alive.
func (as *AdvancedSubscriber) gc(retention time.Duration) {
	t := time.NewTicker(retention)
	defer t.Stop()
	for {
		select {
		case <-as.gcStop:
			return
		case <-as.s.done:
			return
		case <-t.C:
		}
		now := time.Now()
		as.mu.Lock()
		for id, st := range as.seq {
			if now.Sub(st.access) > retention {
				if st.alive {
					st.access = now
				} else {
					st.stopPeriodic()
					delete(as.seq, id)
				}
			}
		}
		for id, st := range as.ts {
			if now.Sub(st.access) > retention {
				if st.alive {
					st.access = now
				} else {
					delete(as.ts, id)
				}
			}
		}
		as.mu.Unlock()
	}
}

func intersects(a, b string) bool { return keyexpr.Intersect(a, b) }
