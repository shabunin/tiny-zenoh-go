package zenoh

import (
	"fmt"

	"github.com/shabunin/tiny-zenoh-go/link"
	"github.com/shabunin/tiny-zenoh-go/protocol"
	"github.com/shabunin/tiny-zenoh-go/transport"
)

// Multicast groups. A multicast udp/ locator in Config.Listen (e.g.
// "udp/224.0.0.225:7447", optionally "#iface=eth0") makes the session join
// that group with a multicast transport, as zenoh-rust does: every
// publication of this session is sent to the group, whatever is subscribed,
// and data other nodes send to the group reaches this session's
// subscribers. Queries, liveliness and routing do not use groups.

type mcastGroup struct {
	tr *transport.Multicast
	// Key-expression ids declared by each sender (by address); guarded by
	// Session.mu.
	exprs map[string]map[uint16]string
}

func (s *Session) openMulticast(ep string) error {
	l, err := link.ListenMulticast(ep)
	if err != nil {
		return fmt.Errorf("zenoh: multicast %s: %w", ep, err)
	}
	g := &mcastGroup{exprs: map[string]map[uint16]string{}}
	tr, err := transport.NewMulticast(l, transport.MulticastConfig{
		ZID:     s.zid,
		WhatAmI: s.cfg.Mode.whatAmI(),
		Lease:   s.cfg.Lease,
		// Like zenoh-rust: min(batch size, 8192). Group members ignore the
		// JOINs of nodes with another batch size (zenoh-pico uses 2048).
		BatchSize: min(s.cfg.BatchSize, protocol.MulticastBatchSize),
	}, transport.MulticastCallbacks{
		OnMessage: func(p *transport.MulticastPeer, m protocol.Message, _ bool) { s.mcastMessage(g, p, m) },
		OnPeerGone: func(p *transport.MulticastPeer, _ error) {
			s.mu.Lock()
			delete(g.exprs, p.Addr)
			s.mu.Unlock()
		},
	})
	if err != nil {
		return fmt.Errorf("zenoh: multicast %s: %w", ep, err)
	}
	g.tr = tr
	s.mu.Lock()
	s.mcast = append(s.mcast, g)
	s.mu.Unlock()
	return nil
}

func (s *Session) mcastMessage(g *mcastGroup, p *transport.MulticastPeer, m protocol.Message) {
	switch m := m.(type) {
	case *protocol.Push:
		key, ok := s.mcastResolve(g, p.Addr, m.WireExpr)
		if put, isPut := m.Body.(*protocol.Put); ok && isPut && s.tsOn && !s.stampIngress(put) {
			return
		}
		if ok {
			s.dispatchSample(sampleFromBody(key, m.Body, m.QoS), false)
		}
	case *protocol.Declare:
		switch d := m.Body.(type) {
		case *protocol.DeclareKeyExpr:
			if key, ok := s.mcastResolve(g, p.Addr, d.WireExpr); ok {
				s.mu.Lock()
				if g.exprs[p.Addr] == nil {
					g.exprs[p.Addr] = map[uint16]string{}
				}
				g.exprs[p.Addr][d.ID] = key
				s.mu.Unlock()
			}
		case *protocol.UndeclareKeyExpr:
			s.mu.Lock()
			delete(g.exprs[p.Addr], d.ID)
			s.mu.Unlock()
		}
	}
}

func (s *Session) mcastResolve(g *mcastGroup, addr string, we protocol.WireExpr) (string, bool) {
	if we.Scope == 0 {
		return we.Suffix, true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	prefix, ok := g.exprs[addr][we.Scope]
	return prefix + we.Suffix, ok
}

// mcastPublish sends a publication to every group.
func (s *Session) mcastPublish(key string, qos protocol.QoS, body protocol.DataBody, reliable bool) {
	s.mu.Lock()
	groups := s.mcast
	s.mu.Unlock()
	if len(groups) == 0 {
		return
	}
	msg := &protocol.Push{WireExpr: protocol.WireExpr{Suffix: key}, QoS: qos, Body: body}
	for _, g := range groups {
		_ = g.tr.Send(reliable, msg)
	}
}

// MulticastPeers lists the nodes this session hears on its multicast groups.
func (s *Session) MulticastPeers() []ZID {
	s.mu.Lock()
	groups := s.mcast
	s.mu.Unlock()
	var out []ZID
	for _, g := range groups {
		for _, p := range g.tr.Peers() {
			out = append(out, p.ZID)
		}
	}
	return out
}
