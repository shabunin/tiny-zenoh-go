package zenoh

import (
	"sort"
	"time"

	"github.com/shabunin/tiny-zenoh-go/keyexpr"
	"github.com/shabunin/tiny-zenoh-go/protocol"
	"github.com/shabunin/tiny-zenoh-go/transport"
)

// Router mode.
//
// A router serves the clients and peers attached to it (zenoh-rust's "broker"
// hat, zenoh/src/net/routing/hat/broker). Verified against zenoh-rust 1.10:
// such nodes push their subscribers, queryables and tokens to the router
// unprompted, and learn about everyone else's through INTERESTs (a publisher
// sends INTEREST(subscribers) and only sends data once a matching subscriber
// is declared to it; liveliness subscribers and queriers likewise). So the
// router:
//
//   - stores what each face declares;
//   - answers INTERESTs with the matching entities of all other faces and of
//     its own session, aggregated per key expression, and keeps faces with a
//     Future interest up to date;
//   - forwards PUSHes to the faces whose subscribers match, and REQUESTs to
//     the faces whose queryables match, relaying their responses back under
//     the origin's request id.

// declKind indexes the three kinds of declared entity.
type declKind int

const (
	kindSub declKind = iota
	kindQbl
	kindToken
	nKinds
)

func (k declKind) option() protocol.InterestOptions {
	switch k {
	case kindSub:
		return protocol.InterestSubscribers
	case kindQbl:
		return protocol.InterestQueryables
	default:
		return protocol.InterestTokens
	}
}

// remoteInterest is an interest a face registered with us.
type remoteInterest struct {
	mode protocol.InterestMode
	opts protocol.InterestOptions
	key  string // "" = every key

	// aggregate: the face wants one declaration on key itself (with the
	// interest's id) while anything matching it exists, instead of the
	// matching entities (zenoh-pico's publishers and queriers ask so, and
	// count only declarations whose key equals theirs). Not for tokens.
	aggregate bool
	agg       [nKinds]*declared // what we declared for it (future mode)
}

func (i *remoteInterest) matches(k declKind, key string) bool {
	return i.opts&k.option() != 0 && (i.key == "" || keyexpr.Intersect(i.key, key))
}

func (i *remoteInterest) future() bool {
	return i.mode == protocol.InterestFuture || i.mode == protocol.InterestCurrentFuture
}

// declared is an aggregated entity we declared to a face.
type declared struct {
	id       uint32
	complete bool // queryables
}

// routerState is per-face state kept in router mode (guarded by Session.mu).
type routerState struct {
	interests map[uint32]*remoteInterest
	declared  [nKinds]map[string]*declared
	nextID    uint32
}

func newRelayState() *routerState { return newRouterState() }

func newRouterState() *routerState {
	rs := &routerState{interests: map[uint32]*remoteInterest{}}
	for k := range rs.declared {
		rs.declared[k] = map[string]*declared{}
	}
	return rs
}

// sourceVisibleLocked reports whether the entities of face g are told to
// dst: by region (crossesLocked), or, for dst nil (our announcement as a
// source to the router mesh), all of our faces but routers.
func (s *Session) sourceVisibleLocked(g, dst *face) bool {
	if g == dst || !g.live || g.mesh {
		return false
	}
	return dst == nil || s.crossesLocked(g, dst)
}

// echoesOwnLocked reports whether face g's own entities of kind k are told
// back to it: a client's liveliness tokens are, as zenohd does it (a client
// session sees its own tokens only that way).
func echoesOwnLocked(k declKind, g, dst *face) bool {
	return k == kindToken && g == dst && g.live && g.whatami == protocol.Client
}

// hasTokenLocked reports whether the face itself declared a token on key.
func (f *face) hasTokenLocked(key string) bool {
	for _, k := range f.tokens {
		if k == key {
			return true
		}
	}
	return false
}

// sourcesLocked reports whether any source of (k, key) visible to dst
// exists (our session, our faces by region, other routers), and for
// queryables whether any of them is complete. A north face (relay) is told
// only about our south regions: our session declares its own entities
// itself.
func (s *Session) sourcesLocked(k declKind, key string, dst *face) (present, complete bool) {
	present, complete = s.localSourcesLocked(k, key, dst)
	if dst != nil && dst.relay == nil {
		for _, m := range s.meshes {
			for _, info := range m.remote[k][key] {
				present, complete = true, complete || info.complete
			}
		}
	}
	return present, complete
}

// localSourcesLocked is sourcesLocked without other routers.
func (s *Session) localSourcesLocked(k declKind, key string, dst *face) (present, complete bool) {
	if dst == nil || dst.relay == nil {
		switch k {
		case kindSub:
			for _, sub := range s.subs {
				if sub.remote() && sub.key.String() == key {
					return true, false
				}
			}
		case kindQbl:
			for _, q := range s.queryables {
				if q.remote() && q.key.String() == key {
					present, complete = true, complete || q.complete
				}
			}
		case kindToken:
			for _, t := range s.localTokens {
				if t.key == key {
					return true, false
				}
			}
		}
	}
	for _, f := range s.faces {
		if !s.sourceVisibleLocked(f, dst) && !echoesOwnLocked(k, f, dst) {
			continue
		}
		switch k {
		case kindSub:
			for _, fk := range f.subs {
				if fk == key {
					return true, false
				}
			}
		case kindQbl:
			for _, q := range f.qbls {
				if q.key == key {
					present, complete = true, complete || q.complete
				}
			}
		case kindToken:
			for _, fk := range f.tokens {
				if fk == key {
					return true, false
				}
			}
		}
	}
	return present, complete
}

// keysLocked lists the distinct keys of kind k known from sources visible
// to dst that intersect filter ("" = all).
func (s *Session) keysLocked(k declKind, filter string, dst *face) []string {
	set := map[string]bool{}
	add := func(key string) {
		if filter == "" || keyexpr.Intersect(filter, key) {
			set[key] = true
		}
	}
	if dst == nil || dst.relay == nil {
		switch k {
		case kindSub:
			for _, sub := range s.subs {
				if sub.remote() {
					add(sub.key.String())
				}
			}
		case kindQbl:
			for _, q := range s.queryables {
				if q.remote() {
					add(q.key.String())
				}
			}
		case kindToken:
			for _, t := range s.localTokens {
				add(t.key)
			}
		}
		if dst != nil {
			for _, m := range s.meshes {
				for key := range m.remote[k] {
					add(key)
				}
			}
		}
	}
	for _, f := range s.faces {
		if !s.sourceVisibleLocked(f, dst) && !echoesOwnLocked(k, f, dst) {
			continue
		}
		switch k {
		case kindSub:
			for _, key := range f.subs {
				add(key)
			}
		case kindQbl:
			for _, q := range f.qbls {
				add(q.key)
			}
		case kindToken:
			for _, key := range f.tokens {
				add(key)
			}
		}
	}
	out := make([]string, 0, len(set))
	for key := range set {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

func declMsg(k declKind, id uint32, key string, complete bool, interest *uint32) *protocol.Declare {
	d := &protocol.Declare{QoS: protocol.QoSDeclare}
	if interest != nil {
		d.HasInterestID, d.InterestID = true, *interest
	}
	we := protocol.WireExpr{Suffix: key}
	switch k {
	case kindSub:
		d.Body = &protocol.DeclareSubscriber{ID: id, WireExpr: we}
	case kindQbl:
		d.Body = &protocol.DeclareQueryable{ID: id, WireExpr: we, Complete: complete}
	default:
		d.Body = &protocol.DeclareToken{ID: id, WireExpr: we}
	}
	return d
}

func undeclMsg(k declKind, id uint32, key string) *protocol.Declare {
	d := &protocol.Declare{QoS: protocol.QoSDeclare}
	we := protocol.WireExpr{Suffix: key}
	switch k {
	case kindSub:
		d.Body = &protocol.UndeclareSubscriber{ID: id, WireExpr: we}
	case kindQbl:
		d.Body = &protocol.UndeclareQueryable{ID: id, WireExpr: we}
	default:
		d.Body = &protocol.UndeclareToken{ID: id, WireExpr: we}
	}
	return d
}

// outbox queues messages to faces while Session.mu is held, and sends them
// once it is released. The messages go into each face's own queue at once
// (face.enqueue), so they leave in the order the changes they tell of were
// made, whichever goroutine sends first: link states and declarations built
// under the lock by two goroutines used to go out in the order the
// goroutines got to send. A link state then could come before the one that
// maps a router's NodeId, and the neighbour dropped that link from the
// state, kept it as the newest and passed it on (seen in the lab: routers
// whose graphs disagreed computed trees that looped data for good).
type outbox map[*face]struct{}

func (o outbox) add(f *face, m transport.Encoder) {
	f.enqueue(m)
	o[f] = struct{}{}
}

func (o outbox) send() {
	for f := range o {
		f.flush()
	}
}

// routerUpdateLocked re-evaluates entity (k, key) for every face after its
// sources changed (because of face src, if not nil): south faces with a
// matching Future interest get it declared once it exists (or re-declared
// when a queryable's completeness changes), and undeclared when its last
// source is gone; north faces (relay) are kept told about every entity of
// our south regions.
func (s *Session) routerUpdateLocked(k declKind, key string, src *face, out outbox) {
	s.meshOwnUpdateLocked(k, key, out)
	for _, f := range s.faces {
		rs, relay := f.router, false
		if rs == nil {
			rs, relay = f.relay, true
		}
		if rs == nil || !f.live {
			continue
		}
		present, complete := s.sourcesLocked(k, key, f)
		have := rs.declared[k][key]
		interested := relay || f.futureInterestLocked(k, key)
		switch {
		case have != nil && !present:
			delete(rs.declared[k], key)
			out.add(f, undeclMsg(k, have.id, key))
		case have == nil && !present && !relay && k == kindToken && interested && f != src && !f.hasTokenLocked(key):
			// Like zenoh-rust: a token the face was never told about (its
			// interest is future-only) still gets a one-shot undeclaration,
			// so liveliness subscribers without history see it go. Not the
			// face that declared or undeclared it: a peer does not hear of
			// its own tokens (a client hears of them by the echo above).
			out.add(f, undeclMsg(k, 0, key))
		case present && have == nil && interested:
			d := &declared{id: s.declIDLocked(rs, relay), complete: complete}
			rs.declared[k][key] = d
			out.add(f, declMsg(k, d.id, key, complete, nil))
		case present && have != nil && k == kindQbl && have.complete != complete:
			have.complete = complete
			out.add(f, declMsg(k, have.id, key, complete, nil))
		}
		if !relay {
			s.aggregateUpdateLocked(f, k, key, out)
		}
	}
}

// aggregateLocked reports whether any entity of kind k visible to f
// intersects aggKey, and for queryables whether one of them is complete and
// includes aggKey (zenoh-rust's QueryableInfoType::aggregate).
func (s *Session) aggregateLocked(k declKind, aggKey string, f *face) (present, complete bool) {
	for _, key := range s.keysLocked(k, aggKey, f) {
		present = true
		if k != kindQbl {
			break
		}
		if _, c := s.sourcesLocked(k, key, f); c && keyexpr.Include(key, aggKey) {
			complete = true
			break
		}
	}
	return present, complete
}

// aggregateUpdateLocked keeps f's future aggregate interests matching
// (k, key) up to date: their key is declared while a match exists.
func (s *Session) aggregateUpdateLocked(f *face, k declKind, key string, out outbox) {
	for id, in := range f.router.interests {
		if !in.aggregate || !in.future() || !in.matches(k, key) {
			continue
		}
		present, complete := s.aggregateLocked(k, in.key, f)
		have := in.agg[k]
		switch {
		case have != nil && !present:
			in.agg[k] = nil
			out.add(f, undeclMsg(k, have.id, in.key))
		case present && have == nil:
			f.router.nextID++
			in.agg[k] = &declared{id: f.router.nextID, complete: complete}
			iid := id
			out.add(f, declMsg(k, in.agg[k].id, in.key, complete, &iid))
		case present && k == kindQbl && have.complete != complete:
			have.complete = complete
			iid := id
			out.add(f, declMsg(k, have.id, in.key, complete, &iid))
		}
	}
}

// declIDLocked picks the id of an entity we declare to a face: per face for
// south faces, from the session's entity ids for north faces, which also
// get our own entities under those ids.
func (s *Session) declIDLocked(rs *routerState, relay bool) uint32 {
	if relay {
		s.nextEntity++
		return s.nextEntity
	}
	rs.nextID++
	return rs.nextID
}

// relayedLocked lists the declarations of our south regions' entities a
// new north face must get, recording them in its relay state.
func (s *Session) relayedLocked(f *face, interest *uint32) []transport.Encoder {
	var out []transport.Encoder
	for k := declKind(0); k < nKinds; k++ {
		for _, key := range s.keysLocked(k, "", f) {
			_, complete := s.sourcesLocked(k, key, f)
			d := f.relay.declared[k][key]
			if d == nil {
				d = &declared{id: s.declIDLocked(f.relay, true), complete: complete}
				f.relay.declared[k][key] = d
			}
			out = append(out, declMsg(k, d.id, key, complete, interest))
		}
	}
	return out
}

func (f *face) futureInterestLocked(k declKind, key string) bool {
	for _, i := range f.router.interests {
		if i.future() && !i.aggregate && i.matches(k, key) {
			return true
		}
	}
	return false
}

// routerInterest handles an INTEREST from face f.
func (s *Session) routerInterest(f *face, m *protocol.Interest) {
	if m.Mode == protocol.InterestFinal {
		s.mu.Lock()
		delete(f.router.interests, m.ID)
		s.mu.Unlock()
		return
	}
	in := &remoteInterest{mode: m.Mode, opts: m.Options}
	if m.WireExpr != nil {
		key, ok := f.resolve(*m.WireExpr)
		if !ok {
			return
		}
		in.key = key
		in.aggregate = m.Options&protocol.InterestAggregate != 0
	}
	future := m.Mode == protocol.InterestFuture || m.Mode == protocol.InterestCurrentFuture
	current := m.Mode == protocol.InterestCurrent || m.Mode == protocol.InterestCurrentFuture
	if future {
		s.mu.Lock()
		f.router.interests[m.ID] = in
		s.mu.Unlock()
	}
	if current {
		// Answer once our own gateways (if any) told us what they know.
		s.afterUpstream(func() { s.answerInterest(f, m, in, future) })
	}
}

// answerInterest sends the current part of south face f's interest m: the
// matching entities visible to it, then D_FINAL.
func (s *Session) answerInterest(f *face, m *protocol.Interest, in *remoteInterest, future bool) {
	var msgs []transport.Encoder
	s.mu.Lock()
	rs := f.router
	{
		id := m.ID
		for k := declKind(0); k < nKinds; k++ {
			if in.opts&k.option() == 0 {
				continue
			}
			if in.aggregate {
				if k == kindToken {
					continue // illegal, zenoh-rust ignores it too
				}
				present, complete := s.aggregateLocked(k, in.key, f)
				if !present {
					continue
				}
				var did uint32 // zenoh-rust: id 0 for a current-only aggregate
				if future {
					if in.agg[k] == nil {
						rs.nextID++
						in.agg[k] = &declared{id: rs.nextID, complete: complete}
					}
					did = in.agg[k].id
				}
				msgs = append(msgs, declMsg(k, did, in.key, complete, &id))
				continue
			}
			for _, key := range s.keysLocked(k, in.key, f) {
				_, complete := s.sourcesLocked(k, key, f)
				d := rs.declared[k][key]
				if d == nil {
					rs.nextID++
					d = &declared{id: rs.nextID, complete: complete}
					if future {
						rs.declared[k][key] = d // it will be kept up to date
					}
				}
				msgs = append(msgs, declMsg(k, d.id, key, complete, &id))
			}
		}
	}
	msgs = append(msgs, &protocol.Declare{HasInterestID: true, InterestID: m.ID,
		QoS: protocol.QoSDeclare, Body: &protocol.DeclareFinal{}})
	f.enqueue(msgs...) // in order with the updates queued after (outbox)
	s.mu.Unlock()
	f.flush()
}

// routerFaceGoneLocked propagates the removal of everything face f declared;
// f must already be out of s.faces.
func (s *Session) routerFaceGoneLocked(f *face, out outbox) {
	for _, key := range f.subs {
		s.routerUpdateLocked(kindSub, key, f, out)
	}
	for _, q := range f.qbls {
		s.routerUpdateLocked(kindQbl, q.key, f, out)
	}
	for _, key := range f.tokens {
		s.routerUpdateLocked(kindToken, key, f, out)
	}
}

// -----------------------------------------------------------------------------
// Data and query forwarding
// -----------------------------------------------------------------------------

// routerForward sends a PUSH received from src to every other face whose
// subscribers match.
func (s *Session) routerForward(src *face, key string, m *protocol.Push, reliable bool) {
	r := s.forwardRoute(src, m.NodeID, key)
	if r.drop {
		return
	}
	// m is ours (just decoded, and only its body went to our subscribers),
	// and sending encodes it at once: it is forwarded as it is.
	m.WireExpr = protocol.WireExpr{Suffix: key}
	m.NodeID = 0
	for _, f := range r.faces {
		_ = f.send(reliable, m)
	}
	for _, h := range r.routers {
		m.NodeID = h.node
		_ = h.f.send(reliable, m)
	}
}

// routedQuery is a request we forwarded on behalf of another face.
type routedQuery struct {
	origin    *face
	originRID uint32
	pending   map[*face]bool // faces we forwarded to (nil key: our queryables)
	local     bool
	timer     *time.Timer
}

// routerRequest forwards a REQUEST from src to the faces (and local
// queryables) that can answer it.
func (s *Session) routerRequest(src *face, key string, m *protocol.Request) {
	s.mu.Lock()
	rq := &routedQuery{origin: src, originRID: m.ID, pending: map[*face]bool{}}
	tree, ok := s.meshSourceLocked(src, m.NodeID)
	if !ok {
		tree = 0
	}
	origin := s.originOf(src, tree)
	routers, _ := s.meshRouteLocked(kindQbl, src, m.NodeID, key, m.Target)
	for f := range routers {
		rq.pending[f] = true
	}
	for _, f := range s.faces {
		if f.live && f.wantsQueryLocked(key, m.Target) && s.forwardsLocked(src, f, origin) {
			rq.pending[f] = true
		}
	}
	if len(rq.pending) == 0 && s.cfg.Mode != ModeRouter {
		s.mu.Unlock()
		s.handleRequest(src, m) // only ours to answer
		return
	}
	for _, q := range s.queryables {
		if q.remote() && keyexpr.Intersect(q.key.String(), key) && (m.Target != TargetAllComplete || q.complete) {
			rq.local = true
			break
		}
	}
	if len(rq.pending) == 0 && !rq.local {
		s.mu.Unlock()
		_ = src.send(true, &protocol.ResponseFinal{RequestID: m.ID, QoS: protocol.QoSRequest})
		return
	}
	rid := s.nextQID
	s.nextQID++
	s.routes[rid] = rq
	dst := make([]*face, 0, len(rq.pending))
	for f := range rq.pending {
		dst = append(dst, f)
	}
	s.mu.Unlock()

	timeout := s.cfg.QueryTimeout
	if m.TimeoutMS > 0 {
		timeout = time.Duration(m.TimeoutMS) * time.Millisecond
	}
	rq.timer = time.AfterFunc(timeout, func() { s.routeDone(rid, nil, true) })

	fwd := *m
	fwd.ID = rid
	fwd.WireExpr = protocol.WireExpr{Suffix: key}
	fwd.NodeID = 0
	for _, f := range dst {
		msg := &fwd
		if node, ok := routers[f]; ok {
			meshFwd := fwd
			meshFwd.NodeID = node
			msg = &meshFwd
		}
		if err := f.send(true, msg); err != nil {
			s.routeDone(rid, f, false)
		}
	}
	if rq.local {
		q := m.Query
		if q == nil {
			q = &protocol.Query{}
		}
		s.dispatchRoutedQuery(rid, src, m.ID, key, q, m.Target)
	}
}

// routeDone records that dst (nil: the local queryables) is done with routed
// request rid; with all set, the whole route is finished (timeout). When
// nobody is left, the origin gets its RESPONSE_FINAL.
func (s *Session) routeDone(rid uint32, dst *face, all bool) {
	s.mu.Lock()
	rq := s.routes[rid]
	if rq == nil {
		s.mu.Unlock()
		return
	}
	switch {
	case all:
		rq.pending, rq.local = nil, false
	case dst == nil:
		rq.local = false
	default:
		delete(rq.pending, dst)
	}
	if len(rq.pending) > 0 || rq.local {
		s.mu.Unlock()
		return
	}
	delete(s.routes, rid)
	s.mu.Unlock()
	rq.timer.Stop()
	_ = rq.origin.send(true, &protocol.ResponseFinal{RequestID: rq.originRID, QoS: protocol.QoSRequest})
}

// routerResponse relays a RESPONSE for a routed request back to its origin.
// It reports false if rid is not a routed request.
func (s *Session) routerResponse(src *face, m *protocol.Response) bool {
	s.mu.Lock()
	rq := s.routes[m.RequestID]
	s.mu.Unlock()
	if rq == nil {
		return false
	}
	key, ok := src.resolve(m.WireExpr)
	if !ok {
		return true
	}
	fwd := *m
	fwd.RequestID = rq.originRID
	fwd.WireExpr = protocol.WireExpr{Suffix: key}
	_ = rq.origin.send(true, &fwd)
	return true
}

// dispatchRoutedQuery runs the router's own queryables for a routed request:
// replies go straight to the origin, the final counts toward the route.
func (s *Session) dispatchRoutedQuery(rid uint32, origin *face, originRID uint32, key string, q *protocol.Query, target QueryTarget) {
	s.dispatchQueryWith(originRID, origin, func() { s.routeDone(rid, nil, false) },
		key, q.Parameters, q.Body, q.Attachment, target)
}
