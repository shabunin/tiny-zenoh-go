package storage

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"time"

	"github.com/shabunin/tiny-zenoh-go/zenoh"
)

// Replication between storages, wire-compatible with zenoh-rust's
// storage manager (replication/core.rs, aligner_query.rs,
// aligner_reply.rs).
//
// Every Interval each replica publishes a Digest of its replication log on
// "@-digest/<zid>/<configuration fingerprint>": the fingerprint of its cold
// era, of every warm-era interval and of every hot-era sub-interval. A
// replica receiving a digest that differs from its own asks the sender's
// aligner queryable "@zid/<zid>/<configuration fingerprint>/aligner" (the
// question and the answers travel bincode-encoded in attachments) and
// narrows down from intervals to sub-intervals to events, then retrieves
// the events it lacks with their payloads. A new, empty replica first asks
// any replica for everything (Discovery, then All).

type replicator struct {
	m          *Storage
	rc         ReplicationConfig
	fp         string // configuration fingerprint, decimal
	digestKey  string
	alignerKey string
	sub        *zenoh.Subscriber
	qbl        *zenoh.Queryable
	stop       chan struct{}
	wg         sync.WaitGroup
}

func alignerKey(zid, fp string) string { return "@zid/" + zid + "/" + fp + "/aligner" }

func startReplication(m *Storage) (*replicator, error) {
	r := &replicator{m: m, rc: *m.cfg.Replication, fp: fmt.Sprint(m.log.cfgFP), stop: make(chan struct{})}
	zid := m.s.ZID().String()
	r.digestKey = "@-digest/" + zid + "/" + r.fp
	r.alignerKey = alignerKey(zid, r.fp)
	var err error
	r.sub, err = m.s.DeclareSubscriber("@-digest/*/"+r.fp, r.onDigest, zenoh.WithAllowedOrigin(zenoh.LocalityRemote))
	if err != nil {
		return nil, err
	}
	r.qbl, err = m.s.DeclareQueryable(r.alignerKey, r.onAlignQuery, zenoh.WithAllowedOrigin(zenoh.LocalityRemote))
	if err != nil {
		_ = r.sub.Undeclare()
		return nil, err
	}
	r.wg.Add(2)
	go r.initialAlignment()
	go r.publishDigests()
	return r, nil
}

func (r *replicator) close() error {
	close(r.stop)
	err := errors.Join(r.sub.Undeclare(), r.qbl.Undeclare())
	r.wg.Wait()
	return err
}

func (r *replicator) sleep(d time.Duration) bool {
	if d <= 0 {
		return true
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-r.stop:
		return false
	case <-t.C:
		return true
	}
}

// initialAlignment: an empty replica asks any other for all its content.
func (r *replicator) initialAlignment() {
	defer r.wg.Done()
	if !r.sleep(500 * time.Millisecond) { // zenoh's scouting/delay default
		return
	}
	r.m.mu.Lock()
	empty := len(r.m.log.intervals) == 0
	r.m.mu.Unlock()
	if empty {
		r.queryAligner(alignerKey("*", r.fp), &alignQuery{kind: qDiscovery})
	}
}

// publishDigests publishes the digest every interval, at a random point
// in its first third, after moving the latest updates into the log.
func (r *replicator) publishDigests() {
	defer r.wg.Done()
	ims := r.rc.Interval.Milliseconds()
	if !r.sleep(time.Duration(ims-time.Now().UnixMilli()%ims) * time.Millisecond) {
		return
	}
	for {
		start := time.Now()
		if !r.sleep(r.rc.PropagationDelay) {
			return
		}
		m := r.m
		m.mu.Lock()
		for k, ev := range m.latest {
			m.log.insertEvent(ev)
			delete(m.latest, k)
		}
		payload := encodeDigest(m.log.digest(time.Now()))
		m.mu.Unlock()
		if !r.sleep(time.Duration(rand.Int64N(max(ims/3, 1))) * time.Millisecond) {
			return
		}
		_ = m.s.Put(r.digestKey, payload)
		if !r.sleep(r.rc.Interval - time.Since(start)) {
			return
		}
	}
}

func (r *replicator) onDigest(s zenoh.Sample) {
	parts := strings.Split(s.KeyExpr, "/")
	if len(parts) != 3 {
		return
	}
	other, err := decodeDigest(s.Payload)
	if err != nil {
		return
	}
	r.m.mu.Lock()
	mine := r.m.log.digest(time.Now())
	r.m.mu.Unlock()
	if d := mine.diff(other); d != nil {
		go r.queryAligner(alignerKey(parts[1], r.fp), &alignQuery{kind: qDiff, diff: *d})
	}
}

// queryAligner sends an alignment query and processes the replies (only
// the first one for a discovery).
func (r *replicator) queryAligner(key string, q *alignQuery) {
	select {
	case <-r.stop:
		return
	default:
	}
	cons := zenoh.ConsolidationNone
	if q.kind == qDiscovery {
		cons = zenoh.ConsolidationMonotonic
	}
	replies, err := r.m.s.Get(key, zenoh.WithQueryAttachment(encodeQuery(q)), zenoh.WithConsolidation(cons))
	if err != nil {
		return
	}
	defer func() {
		for range replies {
		}
	}()
	for rep := range replies {
		if rep.Sample == nil || rep.Sample.Attachment == nil {
			continue
		}
		ar, err := decodeReply(rep.Sample.Attachment)
		if err != nil {
			continue
		}
		r.processReply(key, ar, rep.Sample)
		if q.kind == qDiscovery {
			return
		}
	}
}

// --- answering alignment queries (aligner_query.rs) ---

func (r *replicator) onAlignQuery(q *zenoh.Query) {
	if q.Attachment() == nil {
		return
	}
	aq, err := decodeQuery(q.Attachment())
	if err != nil {
		return
	}
	m := r.m
	reply := func(ar *alignReply, payload []byte, enc *zenoh.Encoding) {
		opts := []zenoh.PutOption{zenoh.WithAttachment(encodeReply(ar))}
		if enc != nil {
			opts = append(opts, zenoh.WithEncoding(*enc))
		}
		_ = q.Reply(q.KeyExpr(), payload, opts...)
	}
	switch aq.kind {
	case qDiscovery:
		reply(&alignReply{kind: rDiscovery, zid: m.s.ZID().String()}, nil, nil)
	case qAll:
		m.mu.Lock()
		var metas []eventMeta
		for _, idx := range m.log.sortedIntervals() {
			for _, sub := range m.log.intervals[idx].subs {
				for _, ev := range sub.events {
					metas = append(metas, ev.eventMeta)
				}
			}
		}
		m.mu.Unlock()
		for i := range metas {
			r.replyRetrieval(metas[i], reply)
		}
	case qDiff:
		if aq.diff.coldDiffer {
			m.mu.Lock()
			warmLow := m.log.lastElapsed(time.Now()) - m.log.hot - m.log.warm + 1
			ivs := map[uint64]uint64{}
			for idx, iv := range m.log.intervals {
				if idx < warmLow {
					ivs[idx] = iv.fp
				}
			}
			m.mu.Unlock()
			reply(&alignReply{kind: rIntervals, intervals: ivs}, nil, nil)
		}
		if len(aq.diff.warm) > 0 {
			r.replySubIntervals(aq.diff.warm, reply)
		}
		if len(aq.diff.hot) > 0 {
			r.replyEventsMetadata(aq.diff.hot, reply)
		}
	case qIntervals:
		if len(aq.intervals) > 0 {
			r.replySubIntervals(aq.intervals, reply)
		}
	case qSubIntervals:
		if len(aq.subs) > 0 {
			r.replyEventsMetadata(aq.subs, reply)
		}
	case qEvents:
		for _, ev := range aq.events {
			r.replyRetrieval(ev, reply)
		}
	}
}

type replyFunc func(*alignReply, []byte, *zenoh.Encoding)

func (r *replicator) replySubIntervals(intervals []uint64, reply replyFunc) {
	out := map[uint64]map[uint64]uint64{}
	r.m.mu.Lock()
	for _, idx := range intervals {
		if iv := r.m.log.intervals[idx]; iv != nil {
			out[idx] = iv.subFingerprints()
		}
	}
	r.m.mu.Unlock()
	reply(&alignReply{kind: rSubIntervals, subs: out}, nil, nil)
}

func (r *replicator) replyEventsMetadata(subs map[uint64][]uint64, reply replyFunc) {
	var metas []eventMeta
	r.m.mu.Lock()
	for idx, sl := range subs {
		iv := r.m.log.intervals[idx]
		if iv == nil {
			continue
		}
		for _, s := range sl {
			if sub := iv.subs[s]; sub != nil {
				for _, ev := range sub.events {
					metas = append(metas, ev.eventMeta)
				}
			}
		}
	}
	r.m.mu.Unlock()
	reply(&alignReply{kind: rEventsMetadata, events: metas}, nil, nil)
}

// replyRetrieval sends one event with its value (zenoh's
// reply_event_retrieval).
func (r *replicator) replyRetrieval(ev eventMeta, reply replyFunc) {
	m := r.m
	var payload []byte
	var enc *zenoh.Encoding
	switch ev.act.kind {
	case actPut:
		m.mu.Lock()
		st, ok := m.lookupLocked(ev.key)
		m.mu.Unlock()
		if !ok || !tsEqual(st.ts, ev.ts) {
			return
		}
		payload, enc = st.payload, &st.encoding
	case actWildPut:
		m.mu.Lock()
		u, ok := m.wildPuts[ev.act.wild]
		m.mu.Unlock()
		if !ok {
			return
		}
		payload, enc = u.payload, &u.encoding
	}
	reply(&alignReply{kind: rRetrieval, event: ev}, payload, enc)
}

// --- processing alignment replies (aligner_reply.rs) ---

func (r *replicator) processReply(alignerKe string, ar *alignReply, smp *zenoh.Sample) {
	m := r.m
	switch ar.kind {
	case rDiscovery:
		go r.queryAligner(alignerKey(ar.zid, r.fp), &alignQuery{kind: qAll})
	case rIntervals:
		var diff []uint64
		m.mu.Lock()
		for idx, fp := range ar.intervals {
			if iv := m.log.intervals[idx]; iv == nil || iv.fp != fp {
				diff = append(diff, idx)
			}
		}
		m.mu.Unlock()
		if len(diff) > 0 {
			go r.queryAligner(alignerKe, &alignQuery{kind: qIntervals, intervals: diff})
		}
	case rSubIntervals:
		diff := map[uint64][]uint64{}
		m.mu.Lock()
		for idx, subs := range ar.subs {
			iv := m.log.intervals[idx]
			var d []uint64
			for s, fp := range subs {
				if iv == nil || iv.subs[s] == nil || iv.subs[s].fp != fp {
					d = append(d, s)
				}
			}
			diff[idx] = d
		}
		m.mu.Unlock()
		if len(diff) > 0 {
			go r.queryAligner(alignerKe, &alignQuery{kind: qSubIntervals, subs: diff})
		}
	case rEventsMetadata:
		var missing []eventMeta
		m.mu.Lock()
		for _, ev := range ar.events {
			if r.processEventMetaLocked(ev) {
				missing = append(missing, ev)
			}
		}
		m.mu.Unlock()
		if len(missing) > 0 {
			go r.queryAligner(alignerKe, &alignQuery{kind: qEvents, events: missing})
		}
	case rRetrieval:
		m.mu.Lock()
		r.processRetrievalLocked(ar.event, smp.Payload, smp.Encoding)
		m.mu.Unlock()
	}
}

// cachedNewerLocked: a not-yet-logged update at least as recent is known.
func (r *replicator) cachedNewerLocked(ev *eventMeta) bool {
	c, ok := r.m.latest[ev.logKey()]
	return ok && !tsLess(c.ts, ev.ts)
}

// processEventMetaLocked handles an event another replica has, reporting
// whether its value must be retrieved (zenoh's process_event_metadata).
func (r *replicator) processEventMetaLocked(ev eventMeta) bool {
	m := r.m
	if r.cachedNewerLocked(&ev) || !r.needsProcessingLocked(&ev) {
		return false
	}
	switch ev.act.kind {
	case actPut, actWildPut:
		return true
	case actDelete:
		switch res, old := m.log.removeOlder(&ev); res {
		case keptNewer:
			return false
		case removedOlder:
			if old.act.kind == actPut {
				m.removeLocked(ev.key, ev.ts)
			}
		}
	case actWildDel:
		r.applyWildcardLocked(&ev, nil, zenoh.Encoding{})
	}
	m.log.insertUnchecked(eventFrom(ev))
	return false
}

// processRetrievalLocked stores a retrieved event (zenoh's
// process_event_retrieval).
func (r *replicator) processRetrievalLocked(ev eventMeta, payload []byte, enc zenoh.Encoding) {
	m := r.m
	if r.cachedNewerLocked(&ev) || !r.needsProcessingLocked(&ev) {
		return
	}
	m.log.removeOlder(&ev)
	switch ev.act.kind {
	case actWildDel:
		m.registerWildLocked(ev.act.wild, zenoh.SampleKindDelete, ev.ts, nil, zenoh.Encoding{})
	case actPut:
		m.storeLocked(ev.key, stored{payload: payload, encoding: enc, ts: ev.ts})
	case actWildPut:
		r.applyWildcardLocked(&ev, payload, enc)
	}
	m.log.insertUnchecked(eventFrom(ev))
}

func (r *replicator) fullKey(ev *event) string { return r.m.full(ev.key, ev.hasKey) }

// needsProcessingLocked is zenoh's needs_further_processing: whether the
// replica's event is newer than what the log and the wildcard updates
// already say about its key (handling the cases where a wildcard update
// overrides it).
func (r *replicator) needsProcessingLocked(ev *eventMeta) bool {
	m := r.m
	full := ev.act.wild
	if ev.act.kind == actPut || ev.act.kind == actDelete {
		if !ev.hasKey && m.cfg.StripPrefix == "" {
			return false
		}
		full = m.full(ev.key, ev.hasKey)
	}
	wildKe, wild := m.overridingLocked(full, ev.ts, ev.lastNonWild, ev.act)
	logged := m.log.lookupNewer(ev)
	switch {
	case wild == nil && logged == nil:
		return true
	case wild == nil:
		return false
	case logged == nil:
		r.storeOverriddenLocked(ev, wildKe, wild)
		return false
	case tsEqual(wild.ts, logged.ts) && ev.act.kind == actDelete && logged.lastNonWild != nil &&
		tsLess(*logged.lastNonWild, ev.ts):
		if logged.act.kind == actPut {
			m.removeLocked(ev.key, ev.ts)
		}
		if removed := m.log.removeEvent(&logged.eventMeta); removed != nil {
			removed.setTimestampAndAction(ev.ts, ev.act)
			m.log.insertUnchecked(removed)
			return false
		}
		return true
	default:
		return false
	}
}

// applyWildcardLocked applies a replica's wildcard update to the stored
// keys and logged events it overrides (zenoh's apply_wildcard_update).
func (r *replicator) applyWildcardLocked(ev *eventMeta, payload []byte, enc zenoh.Encoding) {
	m := r.m
	kind := ev.act.sampleKind()
	if res, _ := m.log.removeOlder(ev); res == keptNewer {
		return
	}
	overridden := removeOverridden(m.latest, r.fullKey, ev.act.wild, ev.ts, kind)
	overridden = append(overridden, m.log.removeOverridden(r.fullKey, ev.act.wild, ev.ts, kind)...)
	for _, ov := range overridden {
		switch ov.act.kind {
		case actPut:
			if kind == zenoh.SampleKindPut {
				m.storeLocked(ov.key, stored{payload: payload, encoding: enc, ts: ev.ts})
			} else {
				m.removeLocked(ov.key, ev.ts)
			}
		case actWildPut, actWildDel:
			delete(m.wildPuts, ov.act.wild) // as zenoh does, for both kinds
			continue
		}
		ov.setTimestampAndAction(ev.ts, ev.act)
		m.log.insertUnchecked(ov)
	}
	m.registerWildLocked(ev.act.wild, kind, ev.ts, payload, enc)
}

// storeOverriddenLocked records a replica's put or delete that a wildcard
// update we know overrides, as that wildcard update.
func (r *replicator) storeOverriddenLocked(ev *eventMeta, wildKe string, u *update) {
	if ev.act.kind == actWildPut || ev.act.kind == actWildDel {
		return
	}
	act := action{kind: actWildPut, wild: wildKe}
	if u.kind == zenoh.SampleKindDelete {
		act.kind = actWildDel
	} else {
		r.m.storeLocked(ev.key, stored{payload: u.payload, encoding: u.encoding, ts: u.ts})
	}
	e := eventFrom(*ev)
	e.setTimestampAndAction(u.ts, act)
	r.m.log.insertEvent(e)
}
