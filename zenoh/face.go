package zenoh

import (
	"errors"
	"sync"

	"github.com/shabunin/tiny-zenoh-go/keyexpr"
	"github.com/shabunin/tiny-zenoh-go/protocol"
	"github.com/shabunin/tiny-zenoh-go/shm"
	"github.com/shabunin/tiny-zenoh-go/transport"
)

// initialInterestID is the implicit interest under which zenoh peers push all
// their declarations to each other on connect, ending with a D_FINAL that
// carries this id (zenoh/src/net/routing/hat/peer: INITIAL_INTEREST_ID).
const initialInterestID = 0

// face is one established connection to another node.
//
// A face is either routed (client mode, or a router seen from a peer): we send
// it all our traffic and ask it about remote entities with INTERESTs; or push
// (a peer or client seen from a peer): both sides push their declarations, and
// we send it only data its declarations ask for.
type face struct {
	s       *Session
	tr      *transport.Unicast
	shm     *shm.Link // negotiated shared memory, nil if none
	zid     ZID
	whatami protocol.WhatAmI
	// routed: the node routes for us (we are its client, or a peer and it is
	// a router): we send it all our traffic and ask it about remote entities
	// with INTERESTs. Otherwise it tells us its entities unprompted and only
	// gets data and queries its declarations ask for.
	routed bool
	// push: peer-to-peer face; we also push all our declarations to it.
	push bool
	// router: per-face routing state when the face is in one of our south
	// regions (we are its gateway): it learns about entities through
	// INTERESTs.
	router *routerState
	// relay: the entities of our south regions we declared to a north face
	// (a peer, or our gateway), on top of our session's own.
	relay *routerState
	// region is the face's region; remoteBound the side of it we stand on
	// (BoundSouth: it is our gateway); regionName what it announced.
	region      Region
	remoteBound Bound
	regionName  string
	transient   *Region  // region by the custom gateway config, set during OPEN
	ifaces      []string // local network interfaces of its link
	live        bool     // handshake done and role assigned (guarded by s.mu)
	// mesh: we are a router and the face is another router.
	mesh bool
	// proto is the protocol of the link the face was made on ("tcp", ...).
	proto string
	// acl is the face's access control state (nil: no access control).
	acl *faceACL
	// icpt are the face's interceptors (nil: none).
	icpt *faceInterceptors
	// dialed is the locator we dialed (empty for accepted links).
	dialed string
	// joined: protocol and dialed locator of the links that joined the
	// transport (multilink; guarded by s.mu).
	joined map[transport.Link][2]string
	// published are the links the session admin space announced (guarded
	// by s.mu).
	published []adminLink
	// stats are the face's traffic statistics (nil if off).
	stats *statsTransport

	ready     chan struct{}   // closed when tr is usable (or the handshake failed)
	linkDone  <-chan struct{} // closed when the link this face was made on is gone
	joinTo    *face           // multilink: the face whose transport the link joined
	closed    chan struct{}   // closed when the face is gone
	closeOnce sync.Once
	// lifeMu guards setUp (Session.established is done with the face) and
	// closePending (the transport closed before that).
	lifeMu       sync.Mutex
	setUp        bool
	closePending bool
	initial      []transport.Encoder // declarations to send right after the handshake

	// Guarded by s.mu.
	remoteExprs map[uint16]string // key-expr ids it declared
	subs        map[uint32]string // its subscribers (push faces)
	qbls        map[uint32]remoteQueryable
	tokens      map[uint32]string // its liveliness tokens
}

type remoteQueryable struct {
	key      string
	complete bool
}

func newFace(s *Session) *face {
	return &face{
		s:           s,
		ready:       make(chan struct{}),
		closed:      make(chan struct{}),
		remoteExprs: map[uint16]string{},
		subs:        map[uint32]string{},
		qbls:        map[uint32]remoteQueryable{},
		tokens:      map[uint32]string{},
	}
}

func (f *face) String() string { return f.whatami.String() + ":" + f.zid.String() }

// send transmits on the face once its handshake and initial declarations are
// done (so a later declaration never overtakes the initial snapshot).
func (f *face) send(reliable bool, msgs ...transport.Encoder) error {
	<-f.ready
	if f.tr == nil {
		return transport.ErrClosed
	}
	if f.acl != nil || f.icpt != nil {
		if msgs = f.interceptEgress(msgs, reliable); len(msgs) == 0 {
			return nil
		}
	}
	if f.shm != nil {
		var sent func()
		msgs, sent = f.toShm(msgs)
		defer sent()
	}
	err := f.tr.Send(reliable, msgs...)
	var ce *transport.CongestionError
	if errors.As(err, &ce) {
		// Dropped on congestion, as zenoh does: not an error for the sender.
		err = nil
	}
	if f.stats == nil {
		return err
	}
	switch {
	case ce != nil:
		sent := make([]transport.Encoder, 0, len(msgs)-len(ce.Dropped))
		d := ce.Dropped
		for _, m := range msgs {
			if len(d) > 0 && m == d[0] {
				f.statsDrop(transport.Tx, dropCongestion, m)
				d = d[1:]
			} else {
				sent = append(sent, m)
			}
		}
		f.observeTx(sent)
	case errors.Is(err, transport.ErrClosed):
		for _, m := range msgs {
			f.statsDrop(transport.Tx, dropNoLink, m)
		}
	default:
		f.observeTx(msgs)
	}
	return err
}

func (f *face) close() {
	<-f.ready
	if f.tr != nil {
		_ = f.tr.Close()
	}
}

// snapshotLocked lists what a new face must learn about us: our key-expr
// ids; for north faces, every entity of our session and of our south
// regions, closed (peers) by the initial-interest D_FINAL; for our gateways
// also our liveliness and matching interests. South faces ask with
// INTERESTs instead.
func (s *Session) snapshotLocked(f *face) []transport.Encoder {
	var out []transport.Encoder
	decl := func(d protocol.Declaration) {
		out = append(out, &protocol.Declare{QoS: protocol.QoSDeclare, Body: d})
	}
	for id, key := range s.localExprs {
		decl(&protocol.DeclareKeyExpr{ID: id, WireExpr: protocol.WireExpr{Suffix: key}})
	}
	if !f.push && !f.routed {
		return out // south faces learn about us through their interests
	}
	for id, sub := range s.subs {
		if sub.remote() {
			decl(&protocol.DeclareSubscriber{ID: id, WireExpr: protocol.WireExpr{Suffix: sub.key.String()}})
		}
	}
	for id, q := range s.queryables {
		if q.remote() {
			decl(&protocol.DeclareQueryable{ID: id, WireExpr: protocol.WireExpr{Suffix: q.key.String()}, Complete: q.complete})
		}
	}
	for id, t := range s.localTokens {
		decl(&protocol.DeclareToken{ID: id, WireExpr: protocol.WireExpr{Suffix: t.key}})
	}
	if f.relay != nil {
		out = append(out, s.relayedLocked(f, nil)...)
	}
	if f.routed {
		for id, ls := range s.livSubs {
			out = append(out, ls.interest(id, protocol.InterestCurrentFuture))
		}
		for _, m := range s.matchers {
			out = append(out, m.interest(protocol.InterestCurrentFuture))
		}
		return append(out, s.upstreamSnapshotLocked(f)...)
	}
	out = append(out, &protocol.Declare{HasInterestID: true, InterestID: initialInterestID,
		QoS: protocol.QoSDeclare, Body: &protocol.DeclareFinal{}})
	return out
}

// onClose forgets the face: its pending query finals and liveliness interests
// are resolved, its tokens are reported gone. In client mode losing the face
// ends the session.
// onClose is the transport's close callback. It may come while
// Session.established is still setting the face up (the link died right
// after the handshake), even from inside it: the teardown then waits for
// the setup to finish, so it undoes all of it.
func (f *face) onClose(error) {
	f.closeOnce.Do(func() { close(f.closed) })
	f.lifeMu.Lock()
	if !f.setUp {
		f.closePending = true
		f.lifeMu.Unlock()
		return
	}
	f.lifeMu.Unlock()
	f.teardown()
}

// setupFinished ends Session.established's setup of a face whose transport
// started, running the teardown if the transport closed meanwhile.
func (f *face) setupFinished() {
	f.lifeMu.Lock()
	f.setUp = true
	pending := f.closePending
	f.lifeMu.Unlock()
	if pending {
		f.teardown()
	}
}

// teardown forgets a closed face: its routes, entities and admin state.
func (f *face) teardown() {
	s := f.s
	s.mu.Lock()
	if s.faces[string(f.zid.Bytes())] == f {
		delete(s.faces, string(f.zid.Bytes()))
	}
	var finals []uint32
	for qid, pq := range s.queries {
		if pq.faces[f] {
			finals = append(finals, qid)
		}
	}
	var gets []*livGet
	for _, g := range s.livGets {
		if g.faces[f] {
			delete(g.faces, f)
			if len(g.faces) == 0 {
				gets = append(gets, g)
			}
		}
	}
	var routes []uint32
	for rid, rq := range s.routes {
		if rq.origin == f {
			delete(s.routes, rid) // nobody to answer any more
			rq.timer.Stop()
		} else if rq.pending[f] {
			routes = append(routes, rid)
		}
	}
	out := outbox{}
	wasLive := f.live
	f.live = false
	published := f.published
	f.published = nil
	if wasLive && s.cfg.Mode != ModeClient && !f.mesh {
		s.routerFaceGoneLocked(f, out)
	}
	if f.mesh {
		s.meshLinkDown(f, out)
	}
	if s.gossip != nil {
		s.gossipLinkDownLocked(f)
	}
	if s.gossipNet != nil {
		s.gossipNetLinkDownLocked(f, out)
	}
	s.southLinkDownLocked(f)
	var gone []string
	for _, key := range f.tokens {
		gone = append(gone, key)
	}
	f.tokens = map[uint32]string{}
	s.mu.Unlock()

	out.send()
	if f.routed {
		s.upstreamDone(f)
	}
	s.checkMatching()
	for _, rid := range routes {
		s.routeDone(rid, f, false)
	}
	for _, qid := range finals {
		s.queryFinal(qid, f)
	}
	for _, g := range gets {
		g.finish()
	}
	if wasLive {
		s.adminFaceDown(f, published)
	}
	if s.closed.Load() {
		return // the session is shutting down (and may be the caller)
	}
	if s.cfg.Mode == ModeClient {
		s.shutdown()
		return
	}
	{
		for _, key := range gone {
			s.livelinessEvent(nil, key, SampleKindDelete)
		}
	}
}

// resolve turns a received WireExpr into a full key expression. M=1 means the
// scope id was declared by the sender; M=0 means by us.
func (f *face) resolve(we protocol.WireExpr) (string, bool) {
	if we.Scope == 0 {
		return we.Suffix, true
	}
	f.s.mu.Lock()
	defer f.s.mu.Unlock()
	var prefix string
	var ok bool
	if we.Mapping {
		prefix, ok = f.remoteExprs[we.Scope]
	} else {
		prefix, ok = f.s.localExprs[we.Scope]
	}
	return prefix + we.Suffix, ok
}

// wantsData reports whether data on key should be sent to the face.
func (f *face) wantsDataLocked(key string) bool {
	if f.routed {
		return true
	}
	for _, k := range f.subs {
		if keyexpr.Intersect(k, key) {
			return true
		}
	}
	return false
}

// wantsQuery reports whether a query on key should be sent to the face.
func (f *face) wantsQueryLocked(key string, target QueryTarget) bool {
	if f.routed {
		return true
	}
	for _, q := range f.qbls {
		if keyexpr.Intersect(q.key, key) && (target != TargetAllComplete || q.complete) {
			return true
		}
	}
	return false
}

// -----------------------------------------------------------------------------
// Receive dispatch
// -----------------------------------------------------------------------------

func (f *face) onMessage(m protocol.Message, reliable bool) error {
	<-f.ready
	shm := f.stats != nil && transport.IsShm(m)
	if err := f.unshm(m); err != nil {
		return nil // unreadable shared-memory payload: drop, as zenoh-rust does
	}
	if f.acl != nil || f.icpt != nil {
		var ok bool
		if m, ok = f.interceptIngress(m, reliable); !ok {
			return nil // access control or an interceptor: dropped
		}
	}
	if f.stats != nil {
		f.observeRx(m, shm)
	}
	return f.dispatch(m, reliable)
}

// dispatch processes a message from f that may be processed.
func (f *face) dispatch(m protocol.Message, reliable bool) error {
	s := f.s
	switch m := m.(type) {
	case *protocol.Push:
		key, ok := f.resolve(m.WireExpr)
		if !ok {
			return nil // unknown mapping: drop, as zenoh-rust does
		}
		if p, isPut := m.Body.(*protocol.Put); isPut && s.tsOn && !s.stampIngress(p) {
			return nil // timestamp from the future
		}
		s.dispatchSample(sampleFromBody(key, m.Body, m.QoS), false)
		if s.cfg.Mode != ModeClient {
			s.routerForward(f, key, m, reliable)
		}
	case *protocol.OAM:
		if f.mesh {
			s.meshOAM(f, m)
			s.checkMatching()
		} else if f.region.South {
			if f.region.Mode == protocol.Peer {
				s.southOAM(f, m)
			}
		} else if s.gossip != nil {
			s.gossipOAM(f, m)
		} else if s.gossipNet != nil {
			s.gossipNetOAM(f, m)
		}
	case *protocol.Request:
		if s.cfg.Mode != ModeClient {
			if key, ok := f.resolve(m.WireExpr); ok {
				s.routerRequest(f, key, m)
			} else {
				_ = f.send(true, &protocol.ResponseFinal{RequestID: m.ID, QoS: protocol.QoSRequest})
			}
			return nil
		}
		s.handleRequest(f, m)
	case *protocol.Response:
		if !s.routerResponse(f, m) {
			s.handleResponse(f, m)
		}
	case *protocol.ResponseFinal:
		s.mu.Lock()
		routed := s.routes[m.RequestID] != nil
		s.mu.Unlock()
		if routed {
			s.routeDone(m.RequestID, f, false)
		} else {
			s.queryFinal(m.RequestID, f)
		}
	case *protocol.Declare:
		if f.mesh {
			switch m.Body.(type) {
			case *protocol.DeclareKeyExpr, *protocol.UndeclareKeyExpr:
			default:
				s.meshDeclare(f, m)
				s.checkMatching()
				return nil
			}
		}
		f.handleDeclare(m)
		s.checkMatching()
	case *protocol.Interest:
		switch {
		case f.mesh: // routers don't send each other interests
		case f.router != nil:
			s.routerInterest(f, m)
		default:
			f.handleInterest(m)
		}
	}
	return nil
}

func (f *face) handleDeclare(m *protocol.Declare) {
	s := f.s
	switch d := m.Body.(type) {
	case *protocol.DeclareKeyExpr:
		// The scope (if any) of a D_KEYEXPR refers to the declarer's own mappings.
		we := d.WireExpr
		we.Mapping = true
		if key, ok := f.resolve(we); ok {
			s.mu.Lock()
			f.remoteExprs[d.ID] = key
			s.mu.Unlock()
		}
	case *protocol.UndeclareKeyExpr:
		s.mu.Lock()
		delete(f.remoteExprs, d.ID)
		s.mu.Unlock()
	case *protocol.DeclareSubscriber:
		if key, ok := f.resolve(d.WireExpr); ok {
			s.mu.Lock()
			f.subs[d.ID] = key
			f.propagateLocked(kindSub, key)
		}
	case *protocol.UndeclareSubscriber:
		s.mu.Lock()
		key, ok := f.subs[d.ID]
		delete(f.subs, d.ID)
		if ok {
			f.propagateLocked(kindSub, key)
		} else {
			s.mu.Unlock()
		}
	case *protocol.DeclareQueryable:
		if key, ok := f.resolve(d.WireExpr); ok {
			s.mu.Lock()
			f.qbls[d.ID] = remoteQueryable{key: key, complete: d.Complete}
			f.propagateLocked(kindQbl, key)
		}
	case *protocol.UndeclareQueryable:
		s.mu.Lock()
		q, ok := f.qbls[d.ID]
		delete(f.qbls, d.ID)
		if ok {
			f.propagateLocked(kindQbl, q.key)
		} else {
			s.mu.Unlock()
		}
	case *protocol.DeclareToken:
		if key, ok := f.resolve(d.WireExpr); ok {
			s.mu.Lock()
			_, known := f.tokens[d.ID]
			f.tokens[d.ID] = key
			f.propagateLocked(kindToken, key)
			// A token re-declared to answer an interest is news only to that
			// interest; livelinessEvent sorts it out.
			if !known || m.HasInterestID {
				s.livelinessEvent(m, key, SampleKindPut)
			}
		}
	case *protocol.UndeclareToken:
		s.mu.Lock()
		key, ok := f.tokens[d.ID]
		delete(f.tokens, d.ID)
		if ok {
			f.propagateLocked(kindToken, key)
		} else {
			s.mu.Unlock()
			key, ok = f.resolve(d.WireExpr)
		}
		if ok && key != "" {
			s.livelinessEvent(m, key, SampleKindDelete)
		}
	case *protocol.DeclareFinal:
		if m.HasInterestID && m.InterestID != initialInterestID {
			s.mu.Lock()
			up := s.upstream != nil && s.upstream.id == m.InterestID
			s.mu.Unlock()
			if up {
				s.upstreamDone(f)
			} else {
				s.livGetFinal(m.InterestID, f)
			}
		}
	}
}

// propagateLocked is called with Session.mu held after f's entity (k, key)
// changed; it unlocks, and in router mode tells interested faces.
func (f *face) propagateLocked(k declKind, key string) {
	s := f.s
	if s.cfg.Mode == ModeClient || !f.live {
		s.mu.Unlock()
		return
	}
	out := outbox{}
	s.routerUpdateLocked(k, key, out)
	s.mu.Unlock()
	out.send()
}

// handleInterest answers a node's interest in our declarations: for Current
// modes we (re)declare matching entities tagged with the interest id, then
// send D_FINAL. Future declarations are pushed to every face anyway.
func (f *face) handleInterest(m *protocol.Interest) {
	if m.Mode != protocol.InterestCurrent && m.Mode != protocol.InterestCurrentFuture {
		return
	}
	s := f.s
	var filter string
	if m.WireExpr != nil {
		if k, ok := f.resolve(*m.WireExpr); ok {
			filter = k
		}
	}
	match := func(k string) bool { return filter == "" || keyexpr.Intersect(filter, k) }
	var msgs []transport.Encoder
	decl := func(d protocol.Declaration) {
		msgs = append(msgs, &protocol.Declare{HasInterestID: true, InterestID: m.ID, QoS: protocol.QoSDeclare, Body: d})
	}
	s.mu.Lock()
	if m.Options&protocol.InterestSubscribers != 0 {
		for id, sub := range s.subs {
			if sub.remote() && match(sub.key.String()) {
				decl(&protocol.DeclareSubscriber{ID: id, WireExpr: protocol.WireExpr{Suffix: sub.key.String()}})
			}
		}
	}
	if m.Options&protocol.InterestQueryables != 0 {
		for id, q := range s.queryables {
			if q.remote() && match(q.key.String()) {
				decl(&protocol.DeclareQueryable{ID: id, WireExpr: protocol.WireExpr{Suffix: q.key.String()}, Complete: q.complete})
			}
		}
	}
	if m.Options&protocol.InterestTokens != 0 {
		for id, t := range s.localTokens {
			if match(t.key) {
				decl(&protocol.DeclareToken{ID: id, WireExpr: protocol.WireExpr{Suffix: t.key}})
			}
		}
	}
	if f.relay != nil { // and those of our south regions
		for k := declKind(0); k < nKinds; k++ {
			if m.Options&k.option() == 0 {
				continue
			}
			for _, key := range s.keysLocked(k, filter, f) {
				if d := f.relay.declared[k][key]; d != nil {
					id := m.ID
					msgs = append(msgs, declMsg(k, d.id, key, d.complete, &id))
				}
			}
		}
	}
	s.mu.Unlock()
	decl(&protocol.DeclareFinal{})
	_ = f.send(true, msgs...)
}
