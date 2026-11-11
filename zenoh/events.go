package zenoh

import (
	"errors"
	"slices"

	"github.com/shabunin/tiny-zenoh-go/link"
	"github.com/shabunin/tiny-zenoh-go/protocol"
	"github.com/shabunin/tiny-zenoh-go/transport"
)

// Connectivity events (zenoh-rust: session.info().transport_events_listener()
// and link_events_listener(), api/connectivity.rs): a callback hears every
// transport to another node open (SampleKindPut) and close (SampleKindDelete),
// or every link of those transports come and go. Multicast group members
// count as transports too (TransportInfo.Multicast), with the group link.

// Link describes one link of a transport (zenoh-rust: info::Link).
type Link struct {
	ZID        ZID    // the node at the other end
	Multicast  bool   // a link of a multicast group member
	Src, Dst   string // locators: ours and theirs (the group, for multicast)
	Group      string // the multicast group, "" for unicast
	MTU        int
	Streamed   bool     // a byte stream (TCP, TLS, ...), not datagrams
	Interfaces []string // local network interfaces the link uses
	// AuthIdentifier is the peer's certificate common name (TLS, QUIC).
	AuthIdentifier string
	// Priorities is the range the link carries, highest first; set only
	// when the transport has QoS.
	Priorities  *PriorityRange
	Reliability Reliability
}

// PriorityRange is a range of priorities, Start the highest (lowest value).
type PriorityRange struct{ Start, End Priority }

// TransportEvent reports a transport opened (Kind SampleKindPut) or closed
// (SampleKindDelete).
type TransportEvent struct {
	Kind      SampleKind
	Transport TransportInfo
}

// LinkEvent reports a link added (Kind SampleKindPut) or removed
// (SampleKindDelete).
type LinkEvent struct {
	Kind SampleKind
	Link Link
}

// EventsOption configures an events listener.
type EventsOption interface{ applyEv(*EventsListener) }

type evFn func(*EventsListener)

func (f evFn) applyEv(l *EventsListener) { f(l) }

// WithHistory first reports, as SampleKindPut events, the transports (or
// links) that are already up.
func WithHistory(on bool) EventsOption {
	return evFn(func(l *EventsListener) { l.history = on })
}

// WithLinkTransport limits a link events listener to the links of one
// transport (as given by Transports or a TransportEvent).
func WithLinkTransport(t TransportInfo) EventsOption {
	return evFn(func(l *EventsListener) { l.filter, l.filterZID, l.filterMcast = true, t.ZID, t.Multicast })
}

// EventsListener is a declared transport or link events listener.
type EventsListener struct {
	s           *Session
	onTransport func(TransportEvent)
	onLink      func(LinkEvent)
	history     bool
	filter      bool
	filterZID   ZID
	filterMcast bool
	closed      bool // guarded by Session.mu
}

var errEventsUndeclared = errors.New("zenoh: events listener already undeclared")

// DeclareTransportEventsListener calls cb as transports to other nodes open
// and close. Callbacks run on the session's goroutines, one at a time and in
// order; they must not block for long, nor declare events listeners (they
// may undeclare them, and call Transports and Links).
func (s *Session) DeclareTransportEventsListener(cb func(TransportEvent), opts ...EventsOption) (*EventsListener, error) {
	if cb == nil {
		return nil, errors.New("zenoh: nil callback")
	}
	return s.declareEvents(&EventsListener{onTransport: cb}, opts)
}

// DeclareLinkEventsListener calls cb as links of the transports to other
// nodes are added and removed (see DeclareTransportEventsListener). A
// transport's links are added after it opens and removed before it closes
// (zenoh-rust reports only the transport closing).
func (s *Session) DeclareLinkEventsListener(cb func(LinkEvent), opts ...EventsOption) (*EventsListener, error) {
	if cb == nil {
		return nil, errors.New("zenoh: nil callback")
	}
	return s.declareEvents(&EventsListener{onLink: cb}, opts)
}

func (s *Session) declareEvents(l *EventsListener, opts []EventsOption) (*EventsListener, error) {
	for _, o := range opts {
		o.applyEv(l)
	}
	l.s = s
	// Holding evMu, no event is emitted between the history and the
	// registration: nothing is missed or reported twice.
	s.evMu.Lock()
	defer s.evMu.Unlock()
	if s.closed.Load() {
		return nil, ErrSessionClosed
	}
	s.evStart()
	s.mu.Lock()
	s.evListeners = append(s.evListeners, l)
	entries := s.evEntriesLocked()
	s.mu.Unlock()
	if l.history {
		for _, e := range entries {
			if l.onTransport != nil {
				l.onTransport(TransportEvent{Kind: SampleKindPut, Transport: e.ti})
			} else {
				for _, k := range e.links {
					l.deliverLink(LinkEvent{Kind: SampleKindPut, Link: k})
				}
			}
		}
	}
	return l, nil
}

// Undeclare stops the listener; no callback starts after it returns
// (except when called from the listener's own callback, which may still be
// handed the events of the same change).
func (l *EventsListener) Undeclare() error {
	s := l.s
	s.mu.Lock()
	defer s.mu.Unlock()
	if l.closed {
		return errEventsUndeclared
	}
	l.closed = true
	if i := slices.Index(s.evListeners, l); i >= 0 {
		s.evListeners = slices.Delete(s.evListeners, i, i+1)
	}
	return nil
}

func (l *EventsListener) deliverLink(ev LinkEvent) {
	if l.filter && (!ev.Link.ZID.Equal(l.filterZID) || ev.Link.Multicast != l.filterMcast) {
		return
	}
	l.onLink(ev)
}

// Links describes the links of the transports to other nodes (zenoh-rust:
// session.info().links()).
func (s *Session) Links() []Link {
	if s.evHooks.Load() == nil {
		s.evMu.Lock()
		s.evStart()
		s.evMu.Unlock()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []Link
	for _, e := range s.evEntriesLocked() {
		out = append(out, e.links...)
	}
	return out
}

// evHooks connect the transports to the events machinery. They are set by
// the first events listener (or Links call), so that programs which use
// neither don't link it in (TinyGo builds).
type evHooks struct {
	faceUp    func(f *face, links []adminLink)
	faceLinks func(f *face, links []adminLink)
	faceDown  func(f *face)
	member    func(g *mcastGroup, p *transport.MulticastPeer, up bool)
	groupDown func(g *mcastGroup)
}

// evStart sets the hooks and takes stock of the transports already up;
// evMu held.
func (s *Session) evStart() {
	if s.evHooks.Load() != nil {
		return
	}
	s.evHooks.Store(&evHooks{
		faceUp: func(f *face, links []adminLink) {
			s.evUp(f, f, f.transportInfo(), f.linksOf(links))
		},
		faceLinks: func(f *face, links []adminLink) { s.evLinks(f, f.linksOf(links)) },
		faceDown:  func(f *face) { s.evDown(f) },
		member: func(g *mcastGroup, p *transport.MulticastPeer, up bool) {
			if up {
				s.mcastMemberUp(g, p)
			} else {
				s.mcastMemberDown(g, p)
			}
		},
		groupDown: s.mcastGroupDown,
	})
	// From now on the hooks report changes, but wait for evMu: transports
	// already up are recorded first. One that goes down meanwhile is
	// skipped (not live) or recorded and then withdrawn by its hook.
	add := func(key any, f *face, ti TransportInfo, links []Link) {
		s.mu.Lock()
		defer s.mu.Unlock()
		if (f == nil || f.live) && s.evFindLocked(key) < 0 {
			s.evState = append(s.evState, &evEntry{key: key, ti: ti, links: links})
		}
	}
	for _, f := range s.faceList() {
		select {
		case <-f.ready:
		default:
			continue // its faceUp hook will come
		}
		if f.tr != nil {
			add(f, f, f.transportInfo(), f.linksOf(f.adminLinks()))
		}
	}
	s.mu.Lock()
	groups := s.mcast
	s.mu.Unlock()
	for _, g := range groups {
		for _, p := range g.tr.Peers() {
			ti, k := g.memberInfo(&p)
			add(mcastKey{g, p.Addr}, nil, ti, []Link{k})
		}
	}
}

// -----------------------------------------------------------------------------
// State and emission. evMu serializes changes and their callbacks, so the
// transports and links announced so far (evState, guarded by Session.mu)
// match what listeners were told, in order.
// -----------------------------------------------------------------------------

type evEntry struct {
	key   any // the face, or mcastKey
	ti    TransportInfo
	links []Link
}

// evEntriesLocked lists (copies of) the announced transports in the order
// they came up; s.mu held.
func (s *Session) evEntriesLocked() []evEntry {
	out := make([]evEntry, len(s.evState))
	for i, e := range s.evState {
		out[i] = evEntry{ti: e.ti, links: append([]Link(nil), e.links...)}
	}
	return out
}

// evFindLocked returns the index of key's entry, or -1; s.mu held.
func (s *Session) evFindLocked(key any) int {
	for i, e := range s.evState {
		if e.key == key {
			return i
		}
	}
	return -1
}

// evListenersLocked lists the listeners in declaration order; s.mu held.
func (s *Session) evListenersLocked() []*EventsListener {
	return slices.Clone(s.evListeners)
}

func (s *Session) evEmitTransport(ls []*EventsListener, ev TransportEvent) {
	for _, l := range ls {
		if l.onTransport != nil && l.active() {
			l.onTransport(ev)
		}
	}
}

func (s *Session) evEmitLink(ls []*EventsListener, ev LinkEvent) {
	for _, l := range ls {
		if l.onLink != nil && l.active() {
			l.deliverLink(ev)
		}
	}
}

func (l *EventsListener) active() bool {
	l.s.mu.Lock()
	defer l.s.mu.Unlock()
	return !l.closed
}

// evUp announces a transport (key: its face f, or its multicast group
// member) and its links. f.live is checked under evMu, so that an up racing
// with the transport's own down is dropped.
func (s *Session) evUp(key any, f *face, ti TransportInfo, links []Link) {
	s.evMu.Lock()
	defer s.evMu.Unlock()
	s.mu.Lock()
	if (f != nil && !f.live) || s.evFindLocked(key) >= 0 || s.closed.Load() {
		s.mu.Unlock()
		return
	}
	s.evState = append(s.evState, &evEntry{key: key, ti: ti, links: links})
	ls := s.evListenersLocked()
	s.mu.Unlock()
	s.evEmitTransport(ls, TransportEvent{Kind: SampleKindPut, Transport: ti})
	for _, k := range links {
		s.evEmitLink(ls, LinkEvent{Kind: SampleKindPut, Link: k})
	}
}

// evDown withdraws a transport and its links.
func (s *Session) evDown(key any) {
	s.evMu.Lock()
	defer s.evMu.Unlock()
	s.mu.Lock()
	i := s.evFindLocked(key)
	if i < 0 {
		s.mu.Unlock()
		return
	}
	e := s.evState[i]
	s.evState = slices.Delete(s.evState, i, i+1)
	ls := s.evListenersLocked()
	s.mu.Unlock()
	for _, k := range e.links {
		s.evEmitLink(ls, LinkEvent{Kind: SampleKindDelete, Link: k})
	}
	s.evEmitTransport(ls, TransportEvent{Kind: SampleKindDelete, Transport: e.ti})
}

// evLinks replaces a transport's links, announcing the difference.
func (s *Session) evLinks(key any, now []Link) {
	s.evMu.Lock()
	defer s.evMu.Unlock()
	s.mu.Lock()
	i := s.evFindLocked(key)
	if i < 0 {
		s.mu.Unlock()
		return
	}
	e := s.evState[i]
	old := e.links
	e.links = now
	e.ti.Links = len(now)
	ls := s.evListenersLocked()
	s.mu.Unlock()
	has := func(ls []Link, k Link) bool {
		for _, x := range ls {
			if x.Src == k.Src && x.Dst == k.Dst {
				return true
			}
		}
		return false
	}
	for _, k := range old {
		if !has(now, k) {
			s.evEmitLink(ls, LinkEvent{Kind: SampleKindDelete, Link: k})
		}
	}
	for _, k := range now {
		if !has(old, k) {
			s.evEmitLink(ls, LinkEvent{Kind: SampleKindPut, Link: k})
		}
	}
}

// -----------------------------------------------------------------------------
// Unicast transports (faces) and multicast group members.
// -----------------------------------------------------------------------------

// transportInfo describes f's transport (f.tr set).
func (f *face) transportInfo() TransportInfo {
	t := f.tr
	return TransportInfo{ZID: f.zid, WhatAmI: f.whatami, Proto: f.proto,
		Links: t.Links(), BatchSize: t.BatchSize(), QoS: t.QoS(),
		LowLatency: t.LowLatency(), Compression: t.Compression(),
		MultiLink: t.MultiLink(), SharedMem: f.shm != nil, AuthUser: t.AuthUser(),
		PriorityStreams: t.PriorityStreams(), Region: f.region,
		Gateway: f.remoteBound == BoundSouth, RegionName: f.regionName}
}

// linkOf describes one of f's links.
func (f *face) linkOf(l adminLink) Link {
	k := Link{ZID: f.zid, Src: l.src, Dst: l.dst, MTU: l.info.MTU, Streamed: l.info.Streamed,
		Interfaces: link.LocalInterfaces(l.info.Link), AuthIdentifier: link.PeerCommonName(l.info.Link),
		Reliability: BestEffort}
	if k.MTU == 0 {
		k.MTU = 65480 // zenoh-rust's TCP link MTU
	}
	if k.Interfaces == nil {
		k.Interfaces = []string{}
	}
	if l.info.Reliable {
		k.Reliability = Reliable
	}
	if f.tr != nil && f.tr.QoS() {
		r := PriorityRange{protocol.PriorityControl, protocol.PriorityBackground}
		if l.info.HasRange {
			r = PriorityRange{l.info.PrioLo, l.info.PrioHi}
		}
		k.Priorities = &r
	}
	return k
}

func (f *face) linksOf(ls []adminLink) []Link {
	out := make([]Link, len(ls))
	for i, l := range ls {
		out[i] = f.linkOf(l)
	}
	return out
}

// mcastKey identifies a group member.
type mcastKey struct {
	g    *mcastGroup
	addr string
}

func (g *mcastGroup) memberInfo(p *transport.MulticastPeer) (TransportInfo, Link) {
	ti := TransportInfo{ZID: p.ZID, WhatAmI: p.WhatAmI, Proto: "udp", Links: 1,
		BatchSize: g.batch, QoS: p.QoS, Multicast: true}
	k := Link{ZID: p.ZID, Multicast: true, Src: g.src, Dst: g.locator, Group: g.locator,
		MTU: g.mtu, Interfaces: []string{}, Reliability: BestEffort}
	if p.QoS {
		k.Priorities = &PriorityRange{protocol.PriorityControl, protocol.PriorityBackground}
	}
	return ti, k
}

func (s *Session) mcastMemberUp(g *mcastGroup, p *transport.MulticastPeer) {
	ti, k := g.memberInfo(p)
	s.evUp(mcastKey{g, p.Addr}, nil, ti, []Link{k})
}

func (s *Session) mcastMemberDown(g *mcastGroup, p *transport.MulticastPeer) {
	s.evDown(mcastKey{g, p.Addr})
}

// mcastGroupDown withdraws the members of a group that stopped.
func (s *Session) mcastGroupDown(g *mcastGroup) {
	s.mu.Lock()
	var keys []mcastKey
	for _, e := range s.evState {
		if mk, ok := e.key.(mcastKey); ok && mk.g == g {
			keys = append(keys, mk)
		}
	}
	s.mu.Unlock()
	for _, k := range keys {
		s.evDown(k)
	}
}

// mcastTransports describes the group members (for Transports).
func (s *Session) mcastTransports() []TransportInfo {
	s.mu.Lock()
	groups := s.mcast
	s.mu.Unlock()
	var out []TransportInfo
	for _, g := range groups {
		for _, p := range g.tr.Peers() {
			ti, _ := g.memberInfo(&p)
			out = append(out, ti)
		}
	}
	return out
}
