package zenoh

import (
	"github.com/shabunin/gozenoh/keyexpr"
	"github.com/shabunin/gozenoh/protocol"
)

// Router-to-router routing (zenoh-rust's router hat,
// zenoh/src/net/routing/hat/router), verified against zenohd 1.10.2:
//
//   - A router that has subscribers/queryables/tokens of its own (its session's
//     or its clients' and peers') on a key declares itself as their source:
//     D_SUBSCRIBER etc. with id 0 and NodeId = its psid, sent to its children
//     in its own tree. Each router forwards a sourced declaration to the
//     children of the source's tree, re-tagging the NodeId with its own psid
//     for the source. Routers never send each other INTERESTs.
//   - Data and queries carry the NodeId of the router where they entered the
//     router network; each hop sends them toward every router with a matching
//     sourced entity via the next hop on the entry router's tree.

// qInfo is a queryable's routing info.
type qInfo struct {
	complete bool
	distance uint16
}

// mergeQInfo is zenoh-rust's merge_qabl_infos.
func mergeQInfo(a, b qInfo) qInfo {
	switch {
	case a.complete == b.complete:
		a.distance = min(a.distance, b.distance)
	case b.complete:
		a.distance = b.distance
	}
	a.complete = a.complete || b.complete
	return a
}

type meshState struct {
	net    *lsNetwork
	remote [nKinds]map[string]map[string]qInfo // key -> source router zid -> info
	own    [nKinds]map[string]qInfo            // entities we announced as source
}

func newMesh(self ZID) *meshState {
	m := &meshState{net: newNetwork(self)}
	for k := range m.remote {
		m.remote[k] = map[string]map[string]qInfo{}
		m.own[k] = map[string]qInfo{}
	}
	return m
}

func sourcedDecl(k declKind, key string, node int, info qInfo, remove bool) *protocol.Declare {
	d := &protocol.Declare{QoS: protocol.QoSDeclare, NodeID: uint16(node)}
	we := protocol.WireExpr{Suffix: key}
	switch {
	case k == kindSub && !remove:
		d.Body = &protocol.DeclareSubscriber{WireExpr: we}
	case k == kindSub:
		d.Body = &protocol.UndeclareSubscriber{WireExpr: we}
	case k == kindQbl && !remove:
		d.Body = &protocol.DeclareQueryable{WireExpr: we, Complete: info.complete, Distance: info.distance}
	case k == kindQbl:
		d.Body = &protocol.UndeclareQueryable{WireExpr: we}
	case !remove:
		d.Body = &protocol.DeclareToken{WireExpr: we}
	default:
		d.Body = &protocol.UndeclareToken{WireExpr: we}
	}
	return d
}

// sendToChildrenLocked sends a sourced (un)declaration for tree root to that
// tree's children, except skip.
func (s *Session) sendToChildrenLocked(root int, k declKind, key string, info qInfo, remove bool, skip *face, out outbox) {
	net := s.mesh.net
	if root >= len(net.trees) {
		return
	}
	for _, c := range net.trees[root].children {
		if f := net.faceOf(c); f != nil && f != skip {
			out.add(f, sourcedDecl(k, key, root, info, remove))
		}
	}
}

// ownSourceLocked reports whether our session or our clients/peers have an
// entity (k, key), i.e. whether we must announce ourselves as its source.
func (s *Session) ownSourceLocked(k declKind, key string) (bool, qInfo) {
	present, complete := s.localSourcesLocked(k, key, nil)
	return present, qInfo{complete: complete}
}

// meshOwnUpdateLocked (re)announces or withdraws us as a source of (k, key).
func (s *Session) meshOwnUpdateLocked(k declKind, key string, out outbox) {
	if s.mesh == nil {
		return
	}
	want, info := s.ownSourceLocked(k, key)
	have, had := s.mesh.own[k][key]
	switch {
	case want && (!had || (k == kindQbl && have != info)):
		s.mesh.own[k][key] = info
		s.sendToChildrenLocked(0, k, key, info, false, nil, out)
	case !want && had:
		delete(s.mesh.own[k], key)
		s.sendToChildrenLocked(0, k, key, info, true, nil, out)
	}
}

// meshDeclare handles a sourced (un)declaration from neighbour router f.
func (s *Session) meshDeclare(f *face, m *protocol.Declare) {
	var k declKind
	var we protocol.WireExpr
	var info qInfo
	remove := false
	switch d := m.Body.(type) {
	case *protocol.DeclareSubscriber:
		k, we = kindSub, d.WireExpr
	case *protocol.UndeclareSubscriber:
		k, we, remove = kindSub, d.WireExpr, true
	case *protocol.DeclareQueryable:
		k, we, info = kindQbl, d.WireExpr, qInfo{complete: d.Complete, distance: d.Distance}
	case *protocol.UndeclareQueryable:
		k, we, remove = kindQbl, d.WireExpr, true
	case *protocol.DeclareToken:
		k, we = kindToken, d.WireExpr
	case *protocol.UndeclareToken:
		k, we, remove = kindToken, d.WireExpr, true
	default:
		return
	}
	key, ok := f.resolve(we)
	if !ok || key == "" {
		return
	}
	out := outbox{}
	s.mu.Lock()
	src := s.mesh.net.localPSID(f, uint64(m.NodeID))
	if src <= 0 { // unknown, or claims to be us
		s.mu.Unlock()
		return
	}
	router := zkey(s.mesh.net.nodes[src].zid)
	set := s.mesh.remote[k][key]
	_, existed := set[router]
	if remove {
		if !existed {
			s.mu.Unlock()
			return
		}
		delete(set, router)
		if len(set) == 0 {
			delete(s.mesh.remote[k], key)
		}
	} else {
		if set == nil {
			set = map[string]qInfo{}
			s.mesh.remote[k][key] = set
		}
		set[router] = info
	}
	s.sendToChildrenLocked(src, k, key, info, remove, f, out)
	newcomer := k == kindToken && !remove && !existed && !s.tokenKnownElsewhereLocked(key, router)
	gone := k == kindToken && remove && !s.tokenKnownElsewhereLocked(key, "")
	s.routerUpdateLocked(k, key, out)
	s.mu.Unlock()
	out.send()
	if newcomer {
		s.livelinessEvent(nil, key, SampleKindPut)
	} else if gone {
		s.livelinessEvent(nil, key, SampleKindDelete)
	}
}

// tokenKnownElsewhereLocked reports whether another router than except is a
// source of token key.
func (s *Session) tokenKnownElsewhereLocked(key, except string) bool {
	for r := range s.mesh.remote[kindToken][key] {
		if r != except {
			return true
		}
	}
	return false
}

// meshLinkUp starts the link-state exchange with a new neighbour router.
func (s *Session) meshLinkUp(f *face) {
	out := outbox{}
	s.mu.Lock()
	sends := s.mesh.net.addLink(f)
	s.queueLinkStates(sends, out)
	s.meshTreesChangedLocked(out)
	s.mu.Unlock()
	out.send()
}

// meshLinkDown removes a neighbour router.
func (s *Session) meshLinkDown(f *face, out outbox) {
	removed, sends := s.mesh.net.removeLink(f)
	s.queueLinkStates(sends, out)
	s.meshRoutersGoneLocked(removed, out)
	s.meshTreesChangedLocked(out)
}

// meshOAM handles a link-state message from neighbour router f.
func (s *Session) meshOAM(f *face, m *protocol.OAM) {
	if m.ID != oamLinkState || m.Enc != protocol.ExtZBuf {
		return
	}
	list, err := decodeLinkStates(m.Buf)
	if err != nil {
		return
	}
	out := outbox{}
	s.mu.Lock()
	removed, sends := s.mesh.net.linkStates(list, f)
	s.queueLinkStates(sends, out)
	s.meshRoutersGoneLocked(removed, out)
	s.meshTreesChangedLocked(out)
	s.mu.Unlock()
	out.send()
}

func (s *Session) queueLinkStates(sends []lsSend, out outbox) {
	for _, snd := range sends {
		out.add(snd.f, &protocol.OAM{ID: oamLinkState, Enc: protocol.ExtZBuf,
			Buf: encodeLinkStates(snd.list), QoS: protocol.QoSDeclare})
	}
}

// meshRoutersGoneLocked forgets the entities sourced by unreachable routers.
func (s *Session) meshRoutersGoneLocked(removed []ZID, out outbox) {
	if len(removed) == 0 {
		return
	}
	gone := map[string]bool{}
	for _, z := range removed {
		gone[zkey(z)] = true
	}
	type change struct {
		k   declKind
		key string
	}
	var changes []change
	for k := range s.mesh.remote {
		for key, set := range s.mesh.remote[k] {
			hit := false
			for r := range set {
				if gone[r] {
					delete(set, r)
					hit = true
				}
			}
			if len(set) == 0 {
				delete(s.mesh.remote[k], key)
			}
			if hit {
				changes = append(changes, change{declKind(k), key})
			}
		}
	}
	for _, c := range changes {
		s.routerUpdateLocked(c.k, c.key, out)
	}
}

// meshTreesChangedLocked recomputes trees and sends the sourced declarations
// of each tree's root to that tree's new children.
func (s *Session) meshTreesChangedLocked(out outbox) {
	net := s.mesh.net
	for root, children := range net.computeTrees() {
		if len(children) == 0 || net.nodes[root] == nil {
			continue
		}
		send := func(k declKind, key string, info qInfo) {
			for _, c := range children {
				if f := net.faceOf(c); f != nil {
					out.add(f, sourcedDecl(k, key, root, info, false))
				}
			}
		}
		for k := declKind(0); k < nKinds; k++ {
			if root == 0 {
				for key, info := range s.mesh.own[k] {
					send(k, key, info)
				}
				continue
			}
			rz := zkey(net.nodes[root].zid)
			for key, set := range s.mesh.remote[k] {
				if info, ok := set[rz]; ok {
					send(k, key, info)
				}
			}
		}
	}
}

// meshSource returns the tree to route a message from face f along: the
// entry router's psid, or 0 (us) for traffic from clients, peers or our
// session (f nil). ok is false for an unknown NodeId.
func (s *Session) meshSourceLocked(f *face, node uint16) (int, bool) {
	if s.mesh == nil || f == nil || !f.mesh {
		return 0, true
	}
	src := s.mesh.net.localPSID(f, uint64(node))
	return src, src >= 0
}

// meshTargetsLocked lists the neighbour routers to send to so that traffic on
// key entering at router src reaches every router with a matching sourced
// entity of kind k.
func (s *Session) meshTargetsLocked(k declKind, src int, key string, target QueryTarget, skip *face) map[*face]bool {
	out := map[*face]bool{}
	if s.mesh == nil {
		return out
	}
	net := s.mesh.net
	if src >= len(net.trees) {
		return out
	}
	dirs := net.trees[src].directions
	for rk, set := range s.mesh.remote[k] {
		if !keyexpr.Intersect(rk, key) {
			continue
		}
		for r, info := range set {
			if k == kindQbl && target == TargetAllComplete && !(info.complete && keyexpr.Include(rk, key)) {
				continue
			}
			i := -1
			for j, nd := range net.nodes {
				if nd != nil && zkey(nd.zid) == r {
					i = j
					break
				}
			}
			if i <= 0 || i >= len(dirs) || dirs[i] < 0 {
				continue
			}
			if f := net.faceOf(dirs[i]); f != nil && f != skip {
				out[f] = true
			}
		}
	}
	return out
}
