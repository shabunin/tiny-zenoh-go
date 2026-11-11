package zenoh

import (
	"github.com/shabunin/tiny-zenoh-go/keyexpr"
)

// Querier sends queries repeatedly on one key expression, with fixed target,
// consolidation and timeout (zenoh-rust api/querier.rs). It knows whether
// some queryable matches it.
type Querier struct {
	s    *Session
	key  *keyexpr.Key
	opts []GetOption
	m    *matcher
}

// DeclareQuerier declares a querier on key; opts (target, consolidation,
// timeout, ...) become the defaults of its queries.
func (s *Session) DeclareQuerier(key string, opts ...GetOption) (*Querier, error) {
	k, err := validKey(key)
	if err != nil {
		return nil, err
	}
	var o getOptions
	for _, opt := range opts {
		opt(&o)
	}
	m, err := s.newMatcher(kindQbl, k.String(), o.target == TargetAllComplete)
	if err != nil {
		return nil, err
	}
	return &Querier{s: s, key: k, opts: opts, m: m}, nil
}

// KeyExpr returns the querier's key expression.
func (q *Querier) KeyExpr() string { return q.key.String() }

// Get queries the querier's key with selector parameters params (may be
// empty); replies arrive on a channel closed when the query completes.
func (q *Querier) Get(params string, opts ...GetOption) (<-chan Reply, error) {
	f := newFifo[Reply]()
	if err := q.GetFunc(params, f.push, f.close, opts...); err != nil {
		f.close()
		return nil, err
	}
	return f.out, nil
}

// GetFunc is the callback form of Get.
func (q *Querier) GetFunc(params string, cb func(Reply), done func(), opts ...GetOption) error {
	o := getOptions{timeout: q.s.cfg.QueryTimeout}
	for _, opt := range q.opts {
		opt(&o)
	}
	for _, opt := range opts {
		opt(&o)
	}
	return q.s.get(q.key, params, &o, q, cb, done)
}

// MatchingStatus reports whether some queryable matches the querier.
func (q *Querier) MatchingStatus() (MatchingStatus, error) { return q.m.status() }

// DeclareMatchingListener calls cb when queryables start or stop matching
// the querier (and at once if some match already).
func (q *Querier) DeclareMatchingListener(cb func(MatchingStatus)) (*MatchingListener, error) {
	return q.m.listen(cb)
}

// Undeclare ends the querier: like zenoh-rust, its queries still running
// are dropped (they complete without further replies).
func (q *Querier) Undeclare() error {
	ok, err := q.m.close()
	if !ok {
		return err
	}
	s := q.s
	s.mu.Lock()
	var pending []*pendingQuery
	for qid, pq := range s.queries {
		if pq.querier == q.m.id {
			delete(s.queries, qid)
			pending = append(pending, pq)
		}
	}
	s.mu.Unlock()
	for _, pq := range pending {
		pq.finish()
	}
	return err
}
