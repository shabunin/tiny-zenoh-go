package zenoh

import (
	"github.com/shabunin/tiny-zenoh-go/protocol"
)

// Gossip scouting between peers (zenoh-rust zenoh/src/net/protocol/gossip.rs,
// the default non-multihop flavour used by peer hats).
//
// Each peer keeps a small graph: itself (psid 0) plus one node per peer or
// router it has a transport to. Over OAM link-state messages (the same
// LinkStateList codec as the router mesh) it tells its neighbours where it
// listens and which nodes it knows. A peer that learns of a node with
// locators connects to it, so a peer that was configured to reach only one
// other peer ends up linked to all of that peer's neighbours. Information
// travels one hop: a neighbour's own state is forwarded to the other
// neighbours, nothing further.
//
// Directions (zenoh-rust compute_auto_region): between two peers each side
// sees the other as "north" and sends no link lists; a peer sees a router as
// "south" (a gateway) and also sends it its links.

type gNode struct {
	zid      ZID
	whatami  WhatAmI
	locators []string
	hasLoc   bool
	sn       uint64
	gateway  bool
}

type gLink struct {
	f        *face
	south    bool              // the remote is "south" of us (a router)
	mappings map[uint64]string // the remote's psid -> zid key
}

type gossipState struct {
	nodes []*gNode // index is our psid; nil marks a free slot
	links []*gLink
}

func newGossip(self ZID, w WhatAmI) *gossipState {
	return &gossipState{nodes: []*gNode{{zid: self, whatami: w, sn: 1}}}
}

func (g *gossipState) idx(z ZID) int {
	for i, n := range g.nodes {
		if n != nil && n.zid.Equal(z) {
			return i
		}
	}
	return -1
}

func (g *gossipState) addNode(n *gNode) int {
	for i, m := range g.nodes {
		if m == nil {
			g.nodes[i] = n
			return i
		}
	}
	g.nodes = append(g.nodes, n)
	return len(g.nodes) - 1
}

func (g *gossipState) link(f *face) *gLink {
	for _, l := range g.links {
		if l.f == f {
			return l
		}
	}
	return nil
}

// entry is zenoh-rust make_link_state; selfLocs are our own locators.
func (g *gossipState) entry(i int, withZID, withLoc, withLinks bool, selfLocs []string) linkState {
	n := g.nodes[i]
	ls := linkState{psid: uint64(i), sn: n.sn, whatami: n.whatami, hasWAI: true, gateway: n.gateway}
	if withZID {
		ls.zid = n.zid
	}
	if withLoc {
		if i == 0 {
			ls.locators, ls.hasLoc = selfLocs, true
		} else {
			ls.locators, ls.hasLoc = n.locators, n.hasLoc
		}
	}
	if withLinks && i == 0 {
		ls.links = []uint64{}
		for _, l := range g.links {
			if j := g.idx(l.f.zid); j >= 0 {
				ls.links = append(ls.links, uint64(j))
			}
		}
	}
	return ls
}

// gossipTarget reports whether link states are sent to a node of role w
// (zenoh-rust default scouting/gossip/target: router|peer).
func gossipTarget(w WhatAmI) bool { return w == protocol.Router || w == protocol.Peer }

// gossipLocators are the locators we advertise: our listeners', unspecified
// addresses expanded without loopback (zenoh-rust get_locators_noloopback).
func (s *Session) gossipLocators() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.gossipLocatorsLocked()
}

func (s *Session) gossipLocatorsLocked() []string {
	out := []string{}
	for _, ln := range s.listeners {
		out = append(out, expandLocator(ln.Locator(), false)...)
	}
	return out
}

func (s *Session) queueGossip(out outbox, f *face, list []linkState) {
	if gossipTarget(f.whatami) {
		out.add(f, &protocol.OAM{ID: oamLinkState, Enc: protocol.ExtZBuf,
			Buf: encodeLinkStates(list), QoS: protocol.QoSDeclare})
	}
}

// gossipLinkUp is zenoh-rust Gossip::add_link.
func (s *Session) gossipLinkUp(f *face) {
	locs := s.gossipLocators()
	out := outbox{}
	s.mu.Lock()
	g := s.gossip
	if g == nil || g.link(f) != nil || s.faces[string(f.zid.Bytes())] != f {
		s.mu.Unlock()
		return
	}
	south := f.remoteBound == BoundSouth // our gateway: it also gets our links
	g.links = append(g.links, &gLink{f: f, south: south, mappings: map[uint64]string{}})
	i, isNew := g.idx(f.zid), false
	if i < 0 {
		i = g.addNode(&gNode{zid: f.zid, whatami: f.whatami, gateway: south})
		isNew = true
	}
	g.nodes[0].sn++
	for _, l := range g.links {
		if l.f == f || !l.south {
			continue
		}
		var list []linkState
		if isNew {
			list = append(list, g.entry(i, true, false, false, locs))
		}
		list = append(list, g.entry(0, false, true, true, locs))
		s.queueGossip(out, l.f, list)
	}
	var all []linkState
	for j, n := range g.nodes {
		if n != nil {
			all = append(all, g.entry(j, true, true, south, locs))
		}
	}
	s.queueGossip(out, f, all)
	s.mu.Unlock()
	out.send()
}

// gossipLinkDownLocked is zenoh-rust Gossip::remove_link, and then tells
// our gateways our links without it, as gossipLinkUp does with a new one.
// zenoh-rust (1.10) does not: its gateways keep counting a gateway we lost
// among ours, and when that one was the primary, none forwards for us any
// more (seen in the lab: no failover between two gateways of a peer
// subregion). Gateways take the update like any newer link state.
func (s *Session) gossipLinkDownLocked(f *face, out outbox) {
	g := s.gossip
	if g == nil || g.link(f) == nil {
		return
	}
	for i, l := range g.links {
		if l.f == f {
			g.links = append(g.links[:i], g.links[i+1:]...)
			break
		}
	}
	// Another live transport may already exist for that node (e.g. after a
	// duplicate was resolved): keep its node then.
	for _, l := range g.links {
		if l.f.zid.Equal(f.zid) {
			return
		}
	}
	if i := g.idx(f.zid); i > 0 {
		g.nodes[i] = nil
	}
	g.nodes[0].sn++
	var locs []string
	for _, l := range g.links {
		if !l.south {
			continue
		}
		if locs == nil {
			locs = s.gossipLocatorsLocked()
		}
		s.queueGossip(out, l.f, []linkState{g.entry(0, false, true, true, locs)})
	}
}

// gossipOAM is zenoh-rust Gossip::link_states: a neighbour's link states.
func (s *Session) gossipOAM(f *face, m *protocol.OAM) {
	if m.ID != oamLinkState || m.Enc != protocol.ExtZBuf {
		return
	}
	list, err := decodeLinkStates(m.Buf)
	if err != nil {
		return
	}
	if gossipTarget(f.whatami) {
		s.gossipLinkUp(f) // its link states may come before established() adds the link
	}
	type cand struct {
		zid  ZID
		locs []string
	}
	var connect []cand
	out := outbox{}
	s.mu.Lock()
	g := s.gossip
	src := g.link(f)
	if src == nil {
		s.mu.Unlock()
		return
	}
	for _, ls := range list {
		z := ls.zid
		if z.Len() > 0 {
			src.mappings[ls.psid] = zkey(z)
		} else {
			k, ok := src.mappings[ls.psid]
			if !ok {
				continue // unknown mapping
			}
			if z, err = protocol.NewZID([]byte(k)); err != nil {
				continue
			}
		}
		w := ls.whatami
		if !ls.hasWAI {
			w = protocol.Router
		}
		if z.Equal(f.zid) {
			fwd := -1
			if i := g.idx(z); i < 0 {
				i = g.addNode(&gNode{zid: z, whatami: w, locators: ls.locators, hasLoc: ls.hasLoc,
					sn: ls.sn, gateway: ls.gateway})
				if ls.hasLoc {
					fwd = i
				}
			} else if n := g.nodes[i]; n.sn < ls.sn {
				n.sn = ls.sn
				if ls.hasLoc && !(n.hasLoc && equalStrings(n.locators, ls.locators)) {
					n.locators, n.hasLoc = ls.locators, true
					fwd = i
				}
			}
			if fwd >= 0 {
				e := g.entry(fwd, true, true, false, nil)
				for _, l := range g.links {
					if !l.f.zid.Equal(z) {
						s.queueGossip(out, l.f, []linkState{e})
					}
				}
			}
		}
		if ls.hasLoc && len(ls.locators) > 0 && s.cfg.Gossip.Autoconnect.Matches(w) && !z.Equal(s.zid) &&
			s.faces[string(z.Bytes())] == nil {
			connect = append(connect, cand{z, ls.locators})
		}
	}
	s.mu.Unlock()
	out.send()
	for _, c := range connect {
		go s.connectScouted(c.zid, c.locs)
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// GossipConfig configures gossip scouting (peer mode): peers tell their
// neighbours where they listen, so peers that know only some of the others
// still end up connected to all of them.
type GossipConfig struct {
	// Disable turns gossip off (link states neither sent nor processed).
	Disable bool
	// Autoconnect lists the roles to connect to when gossiped. Default
	// routers, peers and clients (zenoh-rust's peer default).
	Autoconnect WhatAmIMatcher
	// NoAutoconnect: learn and relay gossip but connect to nobody
	// (zenoh-rust autoconnect "").
	NoAutoconnect bool
	// Multihop relays every node's link state (with its locators) to all
	// neighbours, not just the next hop (zenoh-rust
	// scouting/gossip/multihop), so peers learn about peers they have no
	// common neighbour with. More traffic; meant for peer subsystems that
	// aren't fully reachable from each other.
	Multihop bool
}
