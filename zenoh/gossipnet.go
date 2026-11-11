package zenoh

import "github.com/shabunin/gozenoh/protocol"

// Multihop gossip (zenoh-rust scouting/gossip/multihop: true). A peer runs
// the same link-state graph as the router mesh (protocol/network.rs with
// full_linkstate off, gossip_multihop on), but only for discovery: every
// node's link state, locators included, is relayed to all neighbours, so a
// peer learns where every peer of the connected subsystem listens, however
// many hops away, and connects to it. Data still flows peer to peer.
//
// It replaces zenoh-rust's former peer "linkstate" routing mode, which
// zenoh 1.10 deprecated (routing.peer.mode has no effect any more).

// gossipNetLinkUp is Network::add_link for a new neighbour.
func (s *Session) gossipNetLinkUp(f *face) {
	locs := s.gossipLocators()
	out := outbox{}
	s.mu.Lock()
	n := s.gossipNet
	if n == nil || n.links[f] != nil || s.faces[string(f.zid.Bytes())] != f {
		s.mu.Unlock()
		return
	}
	n.selfLocs = locs
	s.queueLinkStates(n.addLink(f), out)
	s.mu.Unlock()
	out.send()
}

// gossipNetLinkDownLocked is Network::remove_link.
func (s *Session) gossipNetLinkDownLocked(f *face, out outbox) {
	if _, ok := s.gossipNet.links[f]; !ok {
		return
	}
	_, sends := s.gossipNet.removeLink(f)
	s.queueLinkStates(sends, out)
}

// gossipNetOAM is Network::link_states: it updates the graph, relays what
// is new and connects to newly learnt peers.
func (s *Session) gossipNetOAM(f *face, m *protocol.OAM) {
	if m.ID != oamLinkState || m.Enc != protocol.ExtZBuf {
		return
	}
	list, err := decodeLinkStates(m.Buf)
	if err != nil {
		return
	}
	if gossipTarget(f.whatami) {
		s.gossipNetLinkUp(f) // its link states may come before established() adds the link
	}
	locs := s.gossipLocators()
	type cand struct {
		zid  ZID
		locs []string
	}
	var connect []cand
	out := outbox{}
	s.mu.Lock()
	n := s.gossipNet
	n.selfLocs = locs
	_, sends := n.linkStates(list, f)
	s.queueLinkStates(sends, out)
	for _, i := range n.changed {
		nd := n.nodes[i]
		if i == 0 || nd == nil || !nd.hasLoc || len(nd.locators) == 0 || !nd.hasWAI ||
			!s.cfg.Gossip.Autoconnect.Matches(nd.whatami) || s.faces[string(nd.zid.Bytes())] != nil {
			continue
		}
		connect = append(connect, cand{nd.zid, nd.locators})
	}
	s.mu.Unlock()
	out.send()
	for _, c := range connect {
		go s.connectScouted(c.zid, c.locs)
	}
}
