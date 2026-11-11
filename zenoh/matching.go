package zenoh

import (
	"errors"

	"github.com/shabunin/tiny-zenoh-go/keyexpr"
	"github.com/shabunin/tiny-zenoh-go/protocol"
)

// Matching status (zenoh-rust api/matching.rs). A publisher matches when some
// subscriber anywhere intersects its key; a querier matches when some
// queryable intersects its key (for TargetAllComplete: a complete queryable
// whose key includes it). Like zenoh-rust, publishers and queriers send their
// routed faces an INTEREST CurrentFuture on their key, so the router keeps us
// told about the subscribers / queryables we could reach; push faces and the
// router mesh tell us everything anyway.

// MatchingStatus says whether a publisher or querier currently has anybody to
// talk to.
type MatchingStatus struct {
	Matching bool
}

// ErrUndeclared is returned when using an undeclared publisher or querier.
var ErrUndeclared = errors.New("zenoh: undeclared")

// matcher tracks the matching status of one publisher or querier. All of its
// fields, and its listeners', are guarded by Session.mu.
type matcher struct {
	s        *Session
	id       uint32 // interest id
	kind     declKind
	key      string
	complete bool // queryables: complete ones including key only
	lsn      map[*MatchingListener]struct{}
	running  bool // a goroutine is delivering notifications
	dirty    bool // re-check requested while running
	gone     bool
}

// MatchingListener is notified when the matching status changes.
type MatchingListener struct {
	m   *matcher
	cb  func(MatchingStatus)
	cur bool
}

func (m *matcher) interest(mode protocol.InterestMode) *protocol.Interest {
	opts := protocol.InterestKeyExprs | m.kind.option()
	return &protocol.Interest{ID: m.id, Mode: mode, QoS: protocol.QoSDeclare, Options: opts,
		WireExpr: &protocol.WireExpr{Suffix: m.key}}
}

// newMatcher registers a matcher and asks the routed faces about key.
func (s *Session) newMatcher(k declKind, key string, complete bool) (*matcher, error) {
	s.mu.Lock()
	if s.closed.Load() {
		s.mu.Unlock()
		return nil, ErrSessionClosed
	}
	s.nextInterest++
	m := &matcher{s: s, id: s.nextInterest, kind: k, key: key, complete: complete,
		lsn: map[*MatchingListener]struct{}{}}
	s.matchers[m.id] = m
	routed := s.routedFacesLocked()
	s.mu.Unlock()
	if err := s.broadcast(routed, m.interest(protocol.InterestCurrentFuture)); err != nil {
		s.mu.Lock()
		delete(s.matchers, m.id)
		s.mu.Unlock()
		return nil, err
	}
	return m, nil
}

// close forgets the matcher; it reports false if it was already gone.
func (m *matcher) close() (bool, error) {
	s := m.s
	s.mu.Lock()
	if m.gone {
		s.mu.Unlock()
		return false, nil
	}
	m.gone = true
	delete(s.matchers, m.id)
	m.lsn = map[*MatchingListener]struct{}{}
	routed := s.routedFacesLocked()
	s.mu.Unlock()
	return true, s.broadcast(routed, &protocol.Interest{ID: m.id, Mode: protocol.InterestFinal,
		QoS: protocol.QoSDeclare})
}

func (m *matcher) matches(key string, complete bool) bool {
	if m.complete {
		return complete && keyexpr.Include(key, m.key)
	}
	return keyexpr.Intersect(key, m.key)
}

// statusLocked computes the matching status from what the session knows:
// its own entities, those declared by its faces, and the router mesh.
func (m *matcher) statusLocked() bool {
	s := m.s
	switch m.kind {
	case kindSub:
		for _, sub := range s.subs {
			if m.matches(sub.key.String(), false) {
				return true
			}
		}
		for _, f := range s.faces {
			for _, key := range f.subs {
				if m.matches(key, false) {
					return true
				}
			}
		}
	case kindQbl:
		for _, q := range s.queryables {
			if m.matches(q.key.String(), q.complete) {
				return true
			}
		}
		for _, f := range s.faces {
			for _, q := range f.qbls {
				if m.matches(q.key, q.complete) {
					return true
				}
			}
		}
	}
	for _, mesh := range s.meshes {
		for key, set := range mesh.remote[m.kind] {
			for _, info := range set {
				if m.matches(key, info.complete) {
					return true
				}
			}
		}
	}
	return false
}

func (m *matcher) status() (MatchingStatus, error) {
	m.s.mu.Lock()
	defer m.s.mu.Unlock()
	if m.gone {
		return MatchingStatus{}, ErrUndeclared
	}
	return MatchingStatus{Matching: m.statusLocked()}, nil
}

// listen adds a listener; like zenoh-rust it hears "matching" right away if
// that is the current status.
func (m *matcher) listen(cb func(MatchingStatus)) (*MatchingListener, error) {
	s := m.s
	l := &MatchingListener{m: m, cb: cb}
	s.mu.Lock()
	if m.gone {
		s.mu.Unlock()
		return nil, ErrUndeclared
	}
	m.lsn[l] = struct{}{}
	s.mu.Unlock()
	m.notify()
	return l, nil
}

// Undeclare stops the listener. It may be called from its callback.
func (l *MatchingListener) Undeclare() error {
	s := l.m.s
	s.mu.Lock()
	delete(l.m.lsn, l)
	s.mu.Unlock()
	return nil
}

// notify brings every listener up to date. One goroutine at a time does the
// work, so callbacks see changes in order; callers that find it busy leave
// it a note to look again.
func (m *matcher) notify() {
	s := m.s
	s.mu.Lock()
	if m.running {
		m.dirty = true
		s.mu.Unlock()
		return
	}
	m.running = true
	for {
		m.dirty = false
		var calls []*MatchingListener
		if len(m.lsn) > 0 {
			st := m.statusLocked()
			for l := range m.lsn {
				if l.cur != st {
					l.cur = st
					calls = append(calls, l)
				}
			}
		}
		s.mu.Unlock()
		for _, l := range calls {
			l.cb(MatchingStatus{Matching: l.cur})
		}
		s.mu.Lock()
		if !m.dirty {
			m.running = false
			s.mu.Unlock()
			return
		}
	}
}

// checkMatching re-evaluates every matcher with listeners; it is called after
// anything that may change a matching status.
func (s *Session) checkMatching() {
	s.mu.Lock()
	var ms []*matcher
	for _, m := range s.matchers {
		if len(m.lsn) > 0 {
			ms = append(ms, m)
		}
	}
	s.mu.Unlock()
	for _, m := range ms {
		m.notify()
	}
}

// MatchingStatus reports whether some subscriber matches the publisher.
func (p *Publisher) MatchingStatus() (MatchingStatus, error) { return p.m.status() }

// DeclareMatchingListener calls cb when subscribers start or stop matching
// the publisher (and at once if some match already).
func (p *Publisher) DeclareMatchingListener(cb func(MatchingStatus)) (*MatchingListener, error) {
	return p.m.listen(cb)
}
