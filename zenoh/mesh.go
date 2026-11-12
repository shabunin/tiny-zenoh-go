package zenoh

import (
	"time"

	"slices"

	"github.com/shabunin/tiny-zenoh-go/keyexpr"
	"github.com/shabunin/tiny-zenoh-go/protocol"
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
//
// A router that is the gateway of router subregions (Config.Gateway) runs one
// such mesh per region, each with its own link-state graph (zenoh-rust: a
// router hat per region). In a south mesh it flags itself as a gateway. Into
// each mesh it announces, as their source, the entities it knows from outside
// that mesh: its session, its clients and peers, and the other meshes.

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
	region Region
	net    *lsNetwork
	remote [nKinds]map[string]map[string]qInfo // key -> source router zid -> info
	own    [nKinds]map[string]qInfo            // entities we announced as source

	treesPending bool // a recomputation of the trees is scheduled
}

func newMesh(self ZID, r Region) *meshState {
	m := &meshState{region: r, net: newNetwork(self)}
	m.net.nodes[0].gateway = r.South
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
func (s *Session) sendToChildrenLocked(m *meshState, root int, k declKind, key string, info qInfo, remove bool, skip *face, out outbox) {
	net := m.net
	if root >= len(net.trees) {
		return
	}
	for _, c := range net.trees[root].children {
		if f := net.faceOf(c); f != nil && f != skip {
			out.add(f, sourcedDecl(k, key, root, info, remove))
		}
	}
}

// ownSourceLocked reports whether our session, our clients/peers or another
// of our meshes have an entity (k, key), i.e. whether we must announce
// ourselves in mesh m as its source.
func (s *Session) ownSourceLocked(m *meshState, k declKind, key string) (bool, qInfo) {
	present, complete := s.localSourcesLocked(k, key, nil)
	for _, o := range s.meshes {
		if o == m {
			continue
		}
		for r, info := range o.remote[k][key] {
			if m.net.knows(r) {
				// Another gateway between both meshes: it announces
				// itself in m (re-announcing it would loop).
				continue
			}
			present, complete = true, complete || info.complete
		}
	}
	return present, qInfo{complete: complete}
}

// meshOf returns the mesh of router face f (nil if f is not a router of ours).
func (s *Session) meshOf(f *face) *meshState {
	if f == nil || !f.mesh {
		return nil
	}
	return s.meshes[f.region]
}

// meshForLocked returns the mesh of region r, made on first use.
func (s *Session) meshForLocked(r Region) *meshState {
	m := s.meshes[r]
	if m == nil {
		m = newMesh(s.zid, r)
		s.meshes[r] = m
		// Announce what we already know from outside it.
		for k := declKind(0); k < nKinds; k++ {
			keys := s.keysLocked(k, "", nil)
			for _, o := range s.meshes {
				for key := range o.remote[k] {
					keys = append(keys, key)
				}
			}
			for _, key := range keys {
				if want, info := s.ownSourceLocked(m, k, key); want {
					m.own[k][key] = info
				}
			}
		}
	}
	return m
}

// meshOwnUpdateLocked (re)announces or withdraws us as a source of (k, key).
func (s *Session) meshOwnUpdateLocked(k declKind, key string, out outbox) {
	for _, m := range s.meshes {
		s.meshOwnUpdateInLocked(m, k, key, out)
	}
}

func (s *Session) meshOwnUpdateInLocked(m *meshState, k declKind, key string, out outbox) {
	want, info := s.ownSourceLocked(m, k, key)
	have, had := m.own[k][key]
	switch {
	case want && (!had || (k == kindQbl && have != info)):
		m.own[k][key] = info
		s.sendToChildrenLocked(m, 0, k, key, info, false, nil, out)
	case !want && had:
		delete(m.own[k], key)
		s.sendToChildrenLocked(m, 0, k, key, info, true, nil, out)
	}
}

// meshReownLocked re-evaluates what we announce in m from our other meshes
// (which of their sources m knows itself changes with m's graph).
func (s *Session) meshReownLocked(m *meshState, out outbox) {
	if len(s.meshes) < 2 {
		return
	}
	for k := declKind(0); k < nKinds; k++ {
		keys := map[string]bool{}
		for key := range m.own[k] {
			keys[key] = true
		}
		for _, o := range s.meshes {
			if o != m {
				for key := range o.remote[k] {
					keys[key] = true
				}
			}
		}
		for key := range keys {
			s.meshOwnUpdateInLocked(m, k, key, out)
		}
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
	mesh := s.meshOf(f)
	if mesh == nil {
		s.mu.Unlock()
		return
	}
	src := mesh.net.localPSID(f, uint64(m.NodeID))
	if src <= 0 { // unknown, or claims to be us
		s.mu.Unlock()
		return
	}
	router := zkey(mesh.net.nodes[src].zid)
	set := mesh.remote[k][key]
	_, existed := set[router]
	if remove {
		if !existed {
			s.mu.Unlock()
			return
		}
		delete(set, router)
		if len(set) == 0 {
			delete(mesh.remote[k], key)
		}
	} else {
		if set == nil {
			set = map[string]qInfo{}
			mesh.remote[k][key] = set
		}
		set[router] = info
	}
	s.sendToChildrenLocked(mesh, src, k, key, info, remove, f, out)
	newcomer := k == kindToken && !remove && !existed && !s.tokenKnownElsewhereLocked(key, router)
	gone := k == kindToken && remove && !s.tokenKnownElsewhereLocked(key, "")
	s.routerUpdateLocked(k, key, nil, out)
	s.mu.Unlock()
	out.send()
	if newcomer {
		s.livelinessEvent(nil, key, SampleKindPut)
	} else if gone {
		s.livelinessEvent(nil, key, SampleKindDelete)
	}
}

// tokenKnownElsewhereLocked reports whether another router than except (in
// any of our meshes) is a source of token key.
func (s *Session) tokenKnownElsewhereLocked(key, except string) bool {
	for _, m := range s.meshes {
		for r := range m.remote[kindToken][key] {
			if r != except {
				return true
			}
		}
	}
	return false
}

// meshLinkUp starts the link-state exchange with a new neighbour router.
func (s *Session) meshLinkUp(f *face) {
	out := outbox{}
	s.mu.Lock()
	if !f.live || s.faces[string(f.zid.Bytes())] != f {
		s.mu.Unlock()
		return
	}
	m := s.meshForLocked(f.region)
	if m.net.links[f] != nil {
		s.mu.Unlock()
		return
	}
	sends := m.net.addLink(f)
	s.queueLinkStates(sends, out)
	s.meshTreesChangedLocked(m, out)
	s.mu.Unlock()
	out.send()
}

// meshLinkDown removes a neighbour router.
func (s *Session) meshLinkDown(f *face, out outbox) {
	m := s.meshOf(f)
	if m == nil || m.net.links[f] == nil {
		return
	}
	removed, sends := m.net.removeLink(f)
	s.queueLinkStates(sends, out)
	s.meshRoutersGoneLocked(m, removed, out)
	s.meshTreesChangedLocked(m, out)
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
	s.meshLinkUp(f) // its link states may come before established() adds the link
	out := outbox{}
	s.mu.Lock()
	mesh := s.meshOf(f)
	if mesh == nil {
		s.mu.Unlock()
		return
	}
	removed, sends := mesh.net.linkStates(list, f)
	s.queueLinkStates(sends, out)
	s.meshRoutersGoneLocked(mesh, removed, out)
	if len(mesh.net.changed) > 0 || len(removed) > 0 {
		s.meshTreesChangedLocked(mesh, out)
	}
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
func (s *Session) meshRoutersGoneLocked(m *meshState, removed []ZID, out outbox) {
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
	for k := range m.remote {
		for key, set := range m.remote[k] {
			hit := false
			for r := range set {
				if gone[r] {
					delete(set, r)
					hit = true
				}
			}
			if len(set) == 0 {
				delete(m.remote[k], key)
			}
			if hit {
				changes = append(changes, change{declKind(k), key})
			}
		}
	}
	for _, c := range changes {
		s.routerUpdateLocked(c.k, c.key, nil, out)
	}
}

// treesDelay is how long after a change of the graph the trees are
// recomputed (zenoh-rust TREES_COMPUTATION_DELAY_MS): the changes of a burst
// of link states, such as a whole mesh reconnecting, cost one computation
// instead of one each. Routing meanwhile follows the previous trees.
var treesDelay = 100 * time.Millisecond

// meshTreesChangedLocked schedules the recomputation of m's trees.
func (s *Session) meshTreesChangedLocked(m *meshState, _ outbox) {
	if m.treesPending {
		return
	}
	m.treesPending = true
	time.AfterFunc(treesDelay, func() {
		out := outbox{}
		s.mu.Lock()
		m.treesPending = false
		if s.closed.Load() {
			s.mu.Unlock()
			return
		}
		s.meshComputeTreesLocked(m, out)
		s.mu.Unlock()
		s.routesChanged()
		out.send()
		s.checkMatching()
	})
}

// meshComputeTreesLocked recomputes trees and sends the sourced declarations
// of each tree's root to that tree's new children.
func (s *Session) meshComputeTreesLocked(m *meshState, out outbox) {
	net := m.net
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
				for key, info := range m.own[k] {
					send(k, key, info)
				}
				continue
			}
			rz := zkey(net.nodes[root].zid)
			for key, set := range m.remote[k] {
				if info, ok := set[rz]; ok {
					send(k, key, info)
				}
			}
		}
	}
	s.meshReownLocked(m, out)
}

// meshSourceLocked returns the tree of f's mesh to route a message from
// router face f along (the entry router's psid). ok is false for an unknown
// NodeId. Traffic from anything else starts at us (tree 0) in every mesh.
func (s *Session) meshSourceLocked(f *face, node uint16) (int, bool) {
	m := s.meshOf(f)
	if m == nil {
		return 0, true
	}
	src := m.net.localPSID(f, uint64(node))
	return src, src >= 0
}

// meshRouteLocked lists, over all our meshes, the neighbour routers to send
// traffic of kind k on key to, with the NodeId (tree) to tag it with:
// traffic from src's own mesh continues on tree (the entry router's), and
// traffic from elsewhere enters every other mesh at us, through the region
// gateway rule. ok is false if src's NodeId is unknown.
func (s *Session) meshRouteLocked(k declKind, src *face, node uint16, key string, target QueryTarget) (map[*face]uint16, bool) {
	tree, ok := s.meshSourceLocked(src, node)
	if !ok {
		return nil, false
	}
	remote := false
	for _, m := range s.meshes {
		if len(m.remote[k]) > 0 {
			remote = true
			break
		}
	}
	if !remote {
		return nil, true // no other router has any: nothing to compute
	}
	out := map[*face]uint16{}
	own := s.meshOf(src)
	origin := s.originOf(src, tree)
	// A router in several of our meshes (another gateway) is reached through
	// one only: src's own mesh first, which reaches it anyway.
	seen := map[string]bool{}
	for _, m := range s.meshOrderLocked(own) {
		root := 0
		if m == own {
			root = tree
		}
		for f := range s.meshTargetsLocked(m, k, root, key, target, src, seen) {
			if m != own && !s.gatewayAllowsLocked(src, f, origin) {
				continue
			}
			out[f] = uint16(root)
		}
	}
	return out, true
}

// meshOrderLocked lists our meshes, first (when not nil), then north, then
// the south ones by region id.
func (s *Session) meshOrderLocked(first *meshState) []*meshState {
	out := make([]*meshState, 0, len(s.meshes))
	if first != nil {
		out = append(out, first)
	}
	if m := s.meshes[North]; m != nil && m != first {
		out = append(out, m)
	}
	var south []*meshState
	for r, m := range s.meshes {
		if r.South && m != first {
			south = append(south, m)
		}
	}
	slices.SortFunc(south, func(a, b *meshState) int { return a.region.ID - b.region.ID })
	return append(out, south...)
}

// meshTargetsLocked lists the neighbour routers of mesh m to send to so that
// traffic on key entering at router src reaches every router with a matching
// sourced entity of kind k, except those in seen; it adds the ones it covers
// to seen.
func (s *Session) meshTargetsLocked(m *meshState, k declKind, src int, key string, target QueryTarget, skip *face, seen map[string]bool) map[*face]bool {
	var out map[*face]bool
	net := m.net
	if src >= len(net.trees) {
		return out
	}
	dirs := net.trees[src].directions
	for rk, set := range m.remote[k] {
		if !keyexpr.Intersect(rk, key) {
			continue
		}
		for r, info := range set {
			if k == kindQbl && target == TargetAllComplete && !(info.complete && keyexpr.Include(rk, key)) {
				continue
			}
			if seen[r] {
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
			seen[r] = true
			if f := net.faceOf(dirs[i]); f != nil && f != skip {
				if out == nil {
					out = map[*face]bool{}
				}
				out[f] = true
			}
		}
	}
	return out
}
