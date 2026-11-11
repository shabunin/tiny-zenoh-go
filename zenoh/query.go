package zenoh

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shabunin/tiny-zenoh-go/keyexpr"
	"github.com/shabunin/tiny-zenoh-go/protocol"
)

// -----------------------------------------------------------------------------
// Replies
// -----------------------------------------------------------------------------

// ReplyError is an error answer to a query (or the local "Timeout" error).
type ReplyError struct {
	Payload  []byte
	Encoding Encoding
}

func (e *ReplyError) Error() string { return "zenoh: query replied with error: " + string(e.Payload) }

// Reply is one answer to a Get: exactly one of Sample and Err is set.
type Reply struct {
	Sample    *Sample
	Err       *ReplyError
	ReplierID *EntityGlobalID
}

// Result returns the sample or the error.
func (r Reply) Result() (Sample, error) {
	if r.Err != nil {
		return Sample{}, r.Err
	}
	return *r.Sample, nil
}

// -----------------------------------------------------------------------------
// Get
// -----------------------------------------------------------------------------

type getOptions struct {
	target        QueryTarget
	consolidation ConsolidationMode
	timeout       time.Duration
	body          *protocol.QueryBody
	attachment    []byte
}

// GetOption customizes a Get.
type GetOption func(*getOptions)

// WithTarget selects which queryables are queried.
func WithTarget(t QueryTarget) GetOption { return func(o *getOptions) { o.target = t } }

// WithConsolidation selects reply consolidation.
func WithConsolidation(c ConsolidationMode) GetOption {
	return func(o *getOptions) { o.consolidation = c }
}

// WithTimeout bounds the query.
func WithTimeout(d time.Duration) GetOption { return func(o *getOptions) { o.timeout = d } }

// WithPayload sends a value along with the query.
func WithPayload(payload []byte, enc Encoding) GetOption {
	return func(o *getOptions) { o.body = &protocol.QueryBody{Encoding: enc.wire(), Payload: payload} }
}

// WithQueryAttachment attaches opaque bytes to the query.
func WithQueryAttachment(b []byte) GetOption { return func(o *getOptions) { o.attachment = b } }

type pendingQuery struct {
	key     string
	querier uint32 // the querier's interest id, 0 for Session.Get
	anyKey  bool   // "_anyke": accept replies on any key
	mode    ConsolidationMode
	timer   *time.Timer
	// Guarded by Session.mu: the faces the request went to, and whether the
	// session's own queryables are still working on it.
	faces map[*face]bool
	local bool

	mu       sync.Mutex
	cb       func(Reply)
	onDone   func()
	finished bool
	latest   map[string]Reply
	order    []string
	mono     map[string]*Timestamp
}

func (q *pendingQuery) add(r Reply) {
	q.mu.Lock()
	if q.finished {
		q.mu.Unlock()
		return
	}
	if r.Err != nil || q.mode == ConsolidationNone {
		q.mu.Unlock()
		q.cb(r)
		return
	}
	key, ts := r.Sample.KeyExpr, r.Sample.Timestamp
	switch q.mode {
	case ConsolidationMonotonic:
		if prev, ok := q.mono[key]; ok && !tsAfter(ts, prev) {
			q.mu.Unlock()
			return
		}
		q.mono[key] = ts
		q.mu.Unlock()
		q.cb(r)
	default: // Latest
		if prev, ok := q.latest[key]; ok {
			if !tsAfter(ts, prev.Sample.Timestamp) {
				q.mu.Unlock()
				return
			}
		} else {
			q.order = append(q.order, key)
		}
		q.latest[key] = r
		q.mu.Unlock()
	}
}

// finish flushes Latest replies, optionally appends extra (e.g. a timeout
// error) and signals completion. Safe to call more than once.
func (q *pendingQuery) finish(extra ...Reply) {
	q.mu.Lock()
	if q.finished {
		q.mu.Unlock()
		return
	}
	q.finished = true
	var flush []Reply
	for _, k := range q.order {
		flush = append(flush, q.latest[k])
	}
	timer := q.timer
	q.mu.Unlock()
	if timer != nil {
		timer.Stop()
	}
	for _, r := range append(flush, extra...) {
		q.cb(r)
	}
	if q.onDone != nil {
		q.onDone()
	}
}

// hasTimeRange reports whether selector parameters carry a "_time" range,
// which makes Auto consolidation resolve to None (as in zenoh-rust).
func hasTimeRange(params string) bool {
	_, ok := Param(params, "_time")
	return ok
}

const anyKeyParam = "_anyke"

// Param returns the value of name in selector parameters "k1=v1;k2;..." (a
// name without '=' has an empty value).
func Param(params, name string) (string, bool) {
	for params != "" {
		var kv string
		kv, params, _ = strings.Cut(params, ";")
		if k, v, _ := strings.Cut(kv, "="); k == name {
			return v, true
		}
	}
	return "", false
}

// Get queries "key[?parameters]" and returns replies on a channel that is
// closed when the query completes (all finals received, or timeout).
func (s *Session) Get(selector string, opts ...GetOption) (<-chan Reply, error) {
	f := newFifo[Reply]()
	if err := s.GetFunc(selector, f.push, f.close, opts...); err != nil {
		f.close()
		return nil, err
	}
	return f.out, nil
}

// GetFunc is the callback form of Get: cb receives each reply and done is
// called once when the query completes.
func (s *Session) GetFunc(selector string, cb func(Reply), done func(), opts ...GetOption) error {
	key, params := splitSelector(selector)
	k, err := validKey(key)
	if err != nil {
		return err
	}
	o := getOptions{timeout: s.cfg.QueryTimeout}
	for _, opt := range opts {
		opt(&o)
	}
	return s.get(k, params, &o, nil, cb, done)
}

func (s *Session) get(k *keyexpr.Key, params string, o *getOptions, qr *Querier, cb func(Reply), done func()) error {
	mode := o.consolidation
	if mode == ConsolidationAuto {
		mode = ConsolidationLatest
		if hasTimeRange(params) {
			mode = ConsolidationNone
		}
	}
	pq := &pendingQuery{
		key: k.String(), mode: mode, cb: cb, onDone: done,
		latest: map[string]Reply{}, mono: map[string]*Timestamp{},
		faces: map[*face]bool{}, local: true,
	}
	s.mu.Lock()
	if s.closed.Load() {
		s.mu.Unlock()
		return ErrSessionClosed
	}
	_, pq.anyKey = Param(params, anyKeyParam)
	if qr != nil {
		if qr.m.gone {
			s.mu.Unlock()
			return ErrUndeclared
		}
		pq.querier = qr.m.id
	}
	qid := s.nextQID
	s.nextQID++
	for _, f := range s.faces {
		if f.wantsQueryLocked(k.String(), o.target) {
			pq.faces[f] = true
		}
	}
	routers, _ := s.meshRouteLocked(kindQbl, nil, 0, k.String(), o.target)
	for f := range routers {
		pq.faces[f] = true
	}
	dst := make([]*face, 0, len(pq.faces))
	for f := range pq.faces {
		dst = append(dst, f)
	}
	s.queries[qid] = pq
	s.mu.Unlock()
	pq.mu.Lock()
	if !pq.finished {
		pq.timer = time.AfterFunc(o.timeout, func() { s.queryTimeout(qid) })
	}
	pq.mu.Unlock()

	req := &protocol.Request{
		ID: qid, WireExpr: protocol.WireExpr{Suffix: k.String()}, QoS: protocol.QoSRequest,
		Target: o.target, TimeoutMS: uint64(o.timeout / time.Millisecond),
		Query: &protocol.Query{Consolidation: mode, Parameters: params, Body: o.body, Attachment: o.attachment},
	}
	for _, f := range dst {
		if err := f.send(true, req); err != nil {
			if s.cfg.Mode == ModeClient {
				s.mu.Lock()
				delete(s.queries, qid)
				s.mu.Unlock()
				pq.mu.Lock()
				if pq.timer != nil {
					pq.timer.Stop()
				}
				pq.mu.Unlock()
				if s.closed.Load() {
					return ErrSessionClosed
				}
				return err
			}
			s.queryFinal(qid, f) // that peer is going away: don't wait for it
		}
	}
	// Also ask this session's own queryables.
	s.dispatchQuery(qid, nil, k.String(), params, o.body, o.attachment, o.target)
	return nil
}

func (s *Session) queryTimeout(qid uint32) {
	s.mu.Lock()
	pq := s.queries[qid]
	delete(s.queries, qid)
	s.mu.Unlock()
	if pq != nil {
		pq.finish(Reply{Err: &ReplyError{Payload: []byte("Timeout"), Encoding: EncodingZenohString}})
	}
}

// queryFinal records that face f (nil: our own queryables) is done with query
// qid, and completes the query when nobody is left.
func (s *Session) queryFinal(qid uint32, f *face) {
	s.mu.Lock()
	pq := s.queries[qid]
	if pq == nil {
		s.mu.Unlock()
		return
	}
	if f == nil {
		pq.local = false
	} else {
		delete(pq.faces, f)
	}
	if pq.local || len(pq.faces) > 0 {
		s.mu.Unlock()
		return
	}
	delete(s.queries, qid)
	s.mu.Unlock()
	pq.finish()
}

func (s *Session) deliverReply(qid uint32, r Reply) {
	s.mu.Lock()
	pq := s.queries[qid]
	s.mu.Unlock()
	if pq == nil {
		return
	}
	// Like zenoh-rust, drop replies on keys outside the query's key expression
	// unless the query says "_anyke".
	if r.Sample != nil && !pq.anyKey && !keyexpr.Intersect(pq.key, r.Sample.KeyExpr) {
		return
	}
	pq.add(r)
}

func (s *Session) handleResponse(f *face, m *protocol.Response) {
	key, ok := f.resolve(m.WireExpr)
	if !ok {
		return
	}
	r := Reply{ReplierID: m.Responder}
	switch b := m.Body.(type) {
	case *protocol.Reply:
		smp := sampleFromBody(key, b.Body, m.QoS)
		r.Sample = &smp
	case *protocol.Err:
		r.Err = &ReplyError{Payload: b.Payload, Encoding: encodingFromWire(b.Encoding)}
	}
	s.deliverReply(m.RequestID, r)
}

// -----------------------------------------------------------------------------
// Queryable
// -----------------------------------------------------------------------------

// Queryable answers queries on a key expression.
type Queryable struct {
	s        *Session
	id       uint32
	key      *keyexpr.Key
	complete bool
	handler  func(*Query)
}

// QueryableOption customizes a queryable.
type QueryableOption func(*Queryable)

// WithComplete declares that the queryable answers for every key it matches.
func WithComplete(c bool) QueryableOption { return func(q *Queryable) { q.complete = c } }

// KeyExpr returns the queryable's key expression.
func (q *Queryable) KeyExpr() string { return q.key.String() }

// DeclareQueryable calls handler for each query intersecting key. Replies may
// be sent from the handler; when it returns the query is finalized, unless the
// handler called Query.Detach (then call Query.Finish when done).
func (s *Session) DeclareQueryable(key string, handler func(*Query), opts ...QueryableOption) (*Queryable, error) {
	k, err := validKey(key)
	if err != nil {
		return nil, err
	}
	q := &Queryable{s: s, id: s.entityID(), key: k, handler: handler}
	for _, opt := range opts {
		opt(q)
	}
	s.mu.Lock()
	s.queryables[q.id] = q
	err = s.declareLocalLocked(kindQbl, k.String(), &protocol.Declare{QoS: protocol.QoSDeclare, Body: &protocol.DeclareQueryable{
		ID: q.id, WireExpr: protocol.WireExpr{Suffix: k.String()}, Complete: q.complete}})
	if err != nil {
		s.mu.Lock()
		delete(s.queryables, q.id)
		s.mu.Unlock()
		return nil, err
	}
	return q, nil
}

// Undeclare removes the queryable.
func (q *Queryable) Undeclare() error {
	q.s.mu.Lock()
	_, ok := q.s.queryables[q.id]
	delete(q.s.queryables, q.id)
	if !ok {
		q.s.mu.Unlock()
		return nil
	}
	return q.s.declareLocalLocked(kindQbl, q.key.String(), &protocol.Declare{QoS: protocol.QoSDeclare,
		Body: &protocol.UndeclareQueryable{ID: q.id}})
}

// queryState tracks the local handlers working on one incoming request; the
// final is emitted when the last one is done.
// queryState tracks the local handlers working on one request; the final is
// emitted when the last one is done. origin is the face the request came
// from, or nil for the session's own Get.
type queryState struct {
	s         *Session
	rid       uint32
	origin    *face
	onFinal   func() // if set, called instead of sending/recording the final
	remaining atomic.Int32
}

func (st *queryState) release() {
	if st.remaining.Add(-1) != 0 {
		return
	}
	if st.onFinal != nil {
		st.onFinal()
	} else if st.origin == nil {
		st.s.queryFinal(st.rid, nil)
	} else {
		_ = st.origin.send(true, &protocol.ResponseFinal{RequestID: st.rid, QoS: protocol.QoSRequest})
	}
}

func (s *Session) handleRequest(f *face, m *protocol.Request) {
	key, ok := f.resolve(m.WireExpr)
	if !ok || m.Query == nil {
		_ = f.send(true, &protocol.ResponseFinal{RequestID: m.ID, QoS: protocol.QoSRequest})
		return
	}
	q := m.Query
	s.dispatchQuery(m.ID, f, key, q.Parameters, q.Body, q.Attachment, m.Target)
}

func (s *Session) dispatchQuery(rid uint32, origin *face, key, params string,
	body *protocol.QueryBody, att []byte, target QueryTarget) {
	s.dispatchQueryWith(rid, origin, nil, key, params, body, att, target)
}

func (s *Session) dispatchQueryWith(rid uint32, origin *face, onFinal func(), key, params string,
	body *protocol.QueryBody, att []byte, target QueryTarget) {
	s.mu.Lock()
	var qs []*Queryable
	for _, q := range s.queryables {
		if keyexpr.Intersect(q.key.String(), key) && (target != TargetAllComplete || q.complete) {
			qs = append(qs, q)
		}
	}
	s.mu.Unlock()
	st := &queryState{s: s, rid: rid, origin: origin, onFinal: onFinal}
	st.remaining.Store(int32(len(qs) + 1))
	for _, qbl := range qs {
		query := &Query{st: st, qbl: qbl, key: key, params: params, attachment: att}
		if body != nil {
			query.payload, query.encoding = body.Payload, encodingFromWire(body.Encoding)
		}
		qbl.handler(query)
		if !query.detached.Load() {
			query.Finish()
		}
	}
	st.release()
}

// Query is a query received by a queryable.
type Query struct {
	st         *queryState
	qbl        *Queryable
	key        string
	params     string
	payload    []byte
	encoding   Encoding
	attachment []byte
	detached   atomic.Bool
	finished   atomic.Bool
}

// KeyExpr returns the queried key expression.
func (q *Query) KeyExpr() string { return q.key }

// Parameters returns the selector parameters (after '?').
func (q *Query) Parameters() string { return q.params }

// Selector returns "key?parameters" (or just key).
func (q *Query) Selector() string {
	if q.params == "" {
		return q.key
	}
	return q.key + "?" + q.params
}

// AcceptsAnyKey reports whether the querier accepts replies on keys outside
// the query's key expression (the "_anyke" selector parameter).
func (q *Query) AcceptsAnyKey() bool {
	_, ok := Param(q.params, anyKeyParam)
	return ok
}

// Payload returns the value sent with the query, if any.
func (q *Query) Payload() []byte { return q.payload }

// Encoding returns the encoding of the query's payload.
func (q *Query) Encoding() Encoding { return q.encoding }

// Attachment returns the query's attachment, if any.
func (q *Query) Attachment() []byte { return q.attachment }

// Detach keeps the query open after the handler returns; call Finish when all
// replies have been sent.
func (q *Query) Detach() { q.detached.Store(true) }

// Finish signals that no more replies will be sent for this queryable.
func (q *Query) Finish() {
	if q.finished.CompareAndSwap(false, true) {
		q.st.release()
	}
}

func (q *Query) respond(key string, body protocol.ResponseBody, r Reply) error {
	if q.finished.Load() {
		return fmt.Errorf("zenoh: reply after query finished")
	}
	if q.st.origin == nil {
		q.st.s.deliverReply(q.st.rid, r)
		return nil
	}
	return q.st.origin.send(true, &protocol.Response{
		RequestID: q.st.rid,
		WireExpr:  protocol.WireExpr{Suffix: key},
		Responder: &protocol.EntityGlobalID{ZID: q.st.s.zid, EID: q.qbl.id},
		Body:      body,
	})
}

func (q *Query) replyData(key string, kind SampleKind, payload []byte, opts []PutOption) error {
	k, err := validKey(key)
	if err != nil {
		return err
	}
	if !keyexpr.Intersect(q.key, k.String()) && !q.AcceptsAnyKey() {
		return fmt.Errorf("zenoh: reply key %q does not match query %q", key, q.key)
	}
	o := defaultPutOptions()
	for _, opt := range opts {
		opt(&o)
	}
	data := q.st.s.buildData(kind, &o)
	if p, ok := data.(*protocol.Put); ok {
		p.Payload = payload
	}
	smp := sampleFromBody(k.String(), data, o.qos())
	return q.respond(k.String(), &protocol.Reply{Body: data},
		Reply{Sample: &smp, ReplierID: &EntityGlobalID{ZID: q.st.s.zid, EID: q.qbl.id}})
}

// Reply answers the query with a value on key (which must intersect the
// query's key expression).
func (q *Query) Reply(key string, payload []byte, opts ...PutOption) error {
	return q.replyData(key, SampleKindPut, payload, opts)
}

// ReplyDelete answers the query with a deletion on key.
func (q *Query) ReplyDelete(key string, opts ...PutOption) error {
	return q.replyData(key, SampleKindDelete, nil, opts)
}

// ReplyErr answers the query with an error.
func (q *Query) ReplyErr(payload []byte, enc Encoding) error {
	return q.respond(q.key, &protocol.Err{Encoding: enc.wire(), Payload: payload},
		Reply{Err: &ReplyError{Payload: payload, Encoding: enc}, ReplierID: &EntityGlobalID{ZID: q.st.s.zid, EID: q.qbl.id}})
}
