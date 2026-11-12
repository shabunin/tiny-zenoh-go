package zenoh

import (
	"sync"
	"time"

	"github.com/shabunin/tiny-zenoh-go/keyexpr"
	"github.com/shabunin/tiny-zenoh-go/protocol"
)

// Liveliness gives access to liveliness tokens: lightweight presence markers
// that disappear automatically when their session goes away.
type Liveliness struct{ s *Session }

// Liveliness returns the session's liveliness API.
func (s *Session) Liveliness() Liveliness { return Liveliness{s} }

// LivelinessToken is a declared liveliness token.
type LivelinessToken struct {
	s   *Session
	id  uint32
	key string
}

// DeclareToken declares a liveliness token on key.
func (l Liveliness) DeclareToken(key string) (*LivelinessToken, error) {
	k, err := l.s.nsKey(key)
	if err != nil {
		return nil, err
	}
	t := &LivelinessToken{s: l.s, id: l.s.entityID(), key: k.String()}
	l.s.mu.Lock()
	l.s.localTokens[t.id] = t
	err = l.s.declareLocalLocked(kindToken, t.key, &protocol.Declare{QoS: protocol.QoSDeclare,
		Body: &protocol.DeclareToken{ID: t.id, WireExpr: protocol.WireExpr{Suffix: t.key}}})
	if err != nil {
		l.s.mu.Lock()
		delete(l.s.localTokens, t.id)
		l.s.mu.Unlock()
		return nil, err
	}
	if l.s.seesOwnTokens() {
		l.s.livelinessEvent(nil, t.key, SampleKindPut)
	}
	return t, nil
}

// KeyExpr returns the token's key expression.
func (t *LivelinessToken) KeyExpr() string { return t.s.userKey(t.key) }

// Undeclare withdraws the token.
func (t *LivelinessToken) Undeclare() error {
	t.s.mu.Lock()
	_, ok := t.s.localTokens[t.id]
	delete(t.s.localTokens, t.id)
	if !ok {
		t.s.mu.Unlock()
		return nil
	}
	err := t.s.declareLocalLocked(kindToken, t.key, &protocol.Declare{QoS: protocol.QoSDeclare,
		Body: &protocol.UndeclareToken{ID: t.id}})
	if t.s.seesOwnTokens() {
		t.s.livelinessEvent(nil, t.key, SampleKindDelete)
	}
	return err
}

// seesOwnTokens reports whether the session tells its own liveliness
// subscribers about its own tokens. Peers and routers do, as zenoh-rust's;
// a client hears of them from its router, which echoes them (zenohd, and
// tzd likewise), and would otherwise see them twice.
func (s *Session) seesOwnTokens() bool { return s.cfg.Mode != ModeClient }

// LivelinessSubscriber receives a PUT sample when a matching token appears and
// a DELETE sample when it disappears.
type LivelinessSubscriber struct {
	s       *Session
	id      uint32 // interest id
	key     *keyexpr.Key
	handler func(Sample)
}

func (l Liveliness) interestID() uint32 {
	l.s.mu.Lock()
	defer l.s.mu.Unlock()
	l.s.nextInterest++
	return l.s.nextInterest
}

func (ls *LivelinessSubscriber) interest(id uint32, mode protocol.InterestMode) *protocol.Interest {
	return &protocol.Interest{
		ID: id, Mode: mode, QoS: protocol.QoSDeclare,
		Options:  protocol.InterestKeyExprs | protocol.InterestTokens,
		WireExpr: &protocol.WireExpr{Suffix: ls.key.String()},
	}
}

// routedFacesLocked returns the faces that must be asked about remote
// entities with INTERESTs (push faces tell us everything unprompted).
func (s *Session) routedFacesLocked() []*face {
	var out []*face
	for _, f := range s.faces {
		if f.routed {
			out = append(out, f)
		}
	}
	return out
}

// pushTokensLocked lists the tokens known from push faces that match key.
func (s *Session) pushTokensLocked(key string) []string {
	seen := map[string]bool{}
	var out []string
	for _, f := range s.faces {
		if f.routed {
			continue
		}
		for _, k := range f.tokens {
			if !seen[k] && keyexpr.Intersect(key, k) {
				seen[k] = true
				out = append(out, k)
			}
		}
	}
	for _, m := range s.meshes {
		for k := range m.remote[kindToken] {
			if !seen[k] && keyexpr.Intersect(key, k) {
				seen[k] = true
				out = append(out, k)
			}
		}
	}
	return out
}

// DeclareSubscriber subscribes to liveliness changes on key. With history, the
// tokens alive at declaration time are reported first.
func (l Liveliness) DeclareSubscriber(key string, handler func(Sample), history bool) (*LivelinessSubscriber, error) {
	k, err := l.s.nsKey(key)
	if err != nil {
		return nil, err
	}
	s := l.s
	ls := &LivelinessSubscriber{s: s, id: l.interestID(), key: k, handler: handler}
	mode := protocol.InterestFuture
	if history {
		mode = protocol.InterestCurrentFuture
	}
	s.mu.Lock()
	s.livSubs[ls.id] = ls
	routed := s.routedFacesLocked()
	var known []string
	if history {
		known = s.pushTokensLocked(k.String())
		if s.seesOwnTokens() {
			for _, t := range s.localTokens {
				if keyexpr.Intersect(k.String(), t.key) {
					known = append(known, t.key)
				}
			}
		}
	}
	s.mu.Unlock()
	for _, key := range known {
		if user, ok := s.nsStrip(key); ok {
			handler(Sample{KeyExpr: user, Kind: SampleKindPut})
		}
	}
	if err := s.broadcast(routed, ls.interest(ls.id, mode)); err != nil {
		s.mu.Lock()
		delete(s.livSubs, ls.id)
		s.mu.Unlock()
		return nil, err
	}
	return ls, nil
}

// Undeclare stops the liveliness subscriber.
func (ls *LivelinessSubscriber) Undeclare() error {
	s := ls.s
	s.mu.Lock()
	_, ok := s.livSubs[ls.id]
	delete(s.livSubs, ls.id)
	routed := s.routedFacesLocked()
	s.mu.Unlock()
	if !ok {
		return nil
	}
	return s.broadcast(routed, &protocol.Interest{ID: ls.id, Mode: protocol.InterestFinal})
}

type livGet struct {
	key   string
	once  sync.Once
	cb    func(Reply)
	done  func()
	timer *time.Timer
	faces map[*face]bool // routed faces yet to answer; guarded by Session.mu
}

func (g *livGet) finish() {
	g.once.Do(func() {
		if g.timer != nil {
			g.timer.Stop()
		}
		g.done()
	})
}

// Get returns the liveliness tokens currently alive on key. The channel is
// closed when every router has answered or the timeout expires.
func (l Liveliness) Get(key string, opts ...GetOption) (<-chan Reply, error) {
	k, err := l.s.nsKey(key)
	if err != nil {
		return nil, err
	}
	s := l.s
	o := getOptions{timeout: s.cfg.QueryTimeout}
	for _, opt := range opts {
		opt.applyGet(&o)
	}
	f := newFifo[Reply]()
	id := l.interestID()
	g := &livGet{key: k.String(), cb: f.push, done: f.close, faces: map[*face]bool{}}
	if o.cancel != nil {
		var ok bool
		g.cb, g.done, ok = o.cancel.bind(f.push, f.close, func() {
			s.mu.Lock()
			if s.livGets[id] == g {
				delete(s.livGets, id)
			}
			s.mu.Unlock()
			g.finish()
		})
		if !ok {
			f.close()
			return nil, ErrCancelled
		}
	}
	s.mu.Lock()
	if s.closed.Load() {
		s.mu.Unlock()
		return nil, ErrSessionClosed
	}
	routed := s.routedFacesLocked()
	for _, rf := range routed {
		g.faces[rf] = true
	}
	known := s.pushTokensLocked(k.String())
	for _, t := range s.localTokens {
		if keyexpr.Intersect(k.String(), t.key) {
			known = append(known, t.key)
		}
	}
	if len(routed) > 0 {
		s.livGets[id] = g
	}
	s.mu.Unlock()
	for _, key := range known {
		if user, ok := s.nsStrip(key); ok {
			smp := Sample{KeyExpr: user, Kind: SampleKindPut}
			g.cb(Reply{Sample: &smp})
		}
	}
	if len(routed) == 0 {
		g.finish()
		return f.out, nil
	}
	g.timer = time.AfterFunc(o.timeout, func() {
		s.mu.Lock()
		delete(s.livGets, id)
		s.mu.Unlock()
		g.finish()
	})
	in := &protocol.Interest{
		ID: id, Mode: protocol.InterestCurrent,
		Options:  protocol.InterestKeyExprs | protocol.InterestTokens,
		WireExpr: &protocol.WireExpr{Suffix: k.String()},
	}
	for _, rf := range routed {
		if err := rf.send(true, in); err != nil {
			s.livGetFinal(id, rf)
		}
	}
	return f.out, nil
}

// livGetFinal records face f's D_FINAL for liveliness get id.
func (s *Session) livGetFinal(id uint32, f *face) {
	s.mu.Lock()
	g := s.livGets[id]
	if g == nil {
		s.mu.Unlock()
		return
	}
	delete(g.faces, f)
	if len(g.faces) > 0 {
		s.mu.Unlock()
		return
	}
	delete(s.livGets, id)
	s.mu.Unlock()
	g.finish()
}

// livelinessEvent routes a token declaration/undeclaration from the router.
// Declarations tagged with an interest id answer that interest only.
func (s *Session) livelinessEvent(m *protocol.Declare, key string, kind SampleKind) {
	user, ok := s.nsStrip(key)
	if !ok {
		return // outside our namespace
	}
	smp := Sample{KeyExpr: user, Kind: kind}
	s.mu.Lock()
	var handlers []func(Sample)
	if m != nil && m.HasInterestID {
		if g := s.livGets[m.InterestID]; g != nil {
			s.mu.Unlock()
			if kind == SampleKindPut && keyexpr.Intersect(g.key, key) {
				g.cb(Reply{Sample: &smp})
			}
			return
		}
		if ls := s.livSubs[m.InterestID]; ls != nil {
			handlers = append(handlers, ls.handler)
		}
	} else {
		for _, ls := range s.livSubs {
			if keyexpr.Intersect(ls.key.String(), key) {
				handlers = append(handlers, ls.handler)
			}
		}
	}
	s.mu.Unlock()
	for _, h := range handlers {
		h(smp)
	}
}
