package zenoh

import (
	"errors"
	"fmt"

	"github.com/shabunin/tiny-zenoh-go/protocol"
	"github.com/shabunin/tiny-zenoh-go/transport"
)

// Regions (zenoh 1.10, zenoh/src/net/runtime/region.rs and routing/gateway.rs).
//
// A node sorts every node it has a transport to into a region: the "north"
// region of its equals (routers for a router, peers for a peer), or one of
// the "south" subregions it is the gateway of (by default: a router's peers,
// a router's clients, a peer's clients). Routing follows the regions:
//
//   - toward a node north of us that says we are south of it (our gateway:
//     a peer's router, a client's router or peer), we send all our traffic
//     and ask about remote entities with INTERESTs;
//   - a node in one of our south regions tells us its entities unprompted
//     and learns about everyone else's through INTERESTs, as a client does;
//   - north peers push their entities to each other; north routers run the
//     link-state mesh.
//
// Traffic crosses regions only through gateways: data or a query from a
// region is never sent back into that region, except within a region of
// clients (who cannot reach each other otherwise). Peers of one subregion
// talk to each other directly; the gateway relays its peers' link states
// (single-hop gossip) so that they find each other. With several gateways
// for one peer, only the one with the highest ZID among those the peer is
// linked to forwards its traffic across (zenoh-rust InterRegionFilter).
//
// With Config.Gateway.South set, the south subregions are user-defined:
// nodes are matched against filters (role, ZID, network interface, the
// region name the node announces), so that e.g. a peer can be the gateway
// of a group of peers, or a router can keep groups of peers apart.

// Bound is the side of a node another node stands on (see transport.Bound).
type Bound = transport.Bound

// Bound values.
const (
	BoundNone  = transport.BoundNone
	BoundNorth = transport.BoundNorth
	BoundSouth = transport.BoundSouth
)

// Region identifies one of this node's regions.
type Region struct {
	// South is false for the north region.
	South bool
	// ID numbers the south subregion: 0 for the default ones, the index in
	// Config.Gateway.South for custom ones.
	ID int
	// Mode is the role of the nodes of a south subregion.
	Mode WhatAmI
}

// North is the north region.
var North = Region{}

func defaultSouth(w WhatAmI) Region { return Region{South: true, Mode: w} }

func (r Region) bound() Bound {
	if r.South {
		return BoundSouth
	}
	return BoundNorth
}

func (r Region) String() string {
	if !r.South {
		return "north"
	}
	return fmt.Sprintf("south(%d,%s)", r.ID, r.Mode)
}

// GatewayConfig configures the south subregions (zenoh-rust gateway.south).
type GatewayConfig struct {
	// South lists custom subregions; nil keeps zenoh's "auto" preset
	// (peers and clients south of routers, clients south of peers). A node
	// goes to the first subregion with a matching filter, otherwise north.
	South []Subregion
}

// Subregion is one custom south subregion.
type Subregion struct {
	// Filters select the nodes of the subregion; a node matches if any
	// filter does. nil matches every node.
	Filters []GatewayFilter
}

// GatewayFilter matches nodes by role, ZID, network interface and region
// name; an unset field matches anything, and a filter matches if all its set
// fields do.
type GatewayFilter struct {
	// Modes the node may have (0: any).
	Modes WhatAmIMatcher
	// ZIDs the node may have.
	ZIDs []ZID
	// Interfaces: every local network interface of the link to the node
	// must be one of these.
	Interfaces []string
	// RegionNames the node may announce (Config.RegionName); a node that
	// announces none does not match.
	RegionNames []string
	// Negated inverts the filter.
	Negated bool
}

// regionPeer is what region computation knows about a remote node.
type regionPeer struct {
	zid        ZID
	whatami    WhatAmI
	regionName string
	ifaces     []string
}

func (f *GatewayFilter) matches(p regionPeer) bool {
	ok := true
	if f.ZIDs != nil {
		found := false
		for _, z := range f.ZIDs {
			found = found || z.Equal(p.zid)
		}
		ok = ok && found
	}
	if f.Interfaces != nil {
		for _, i := range p.ifaces {
			found := false
			for _, j := range f.Interfaces {
				found = found || i == j
			}
			ok = ok && found
		}
	}
	if f.Modes != 0 {
		ok = ok && f.Modes.Matches(p.whatami)
	}
	if f.RegionNames != nil {
		found := false
		for _, n := range f.RegionNames {
			found = found || (p.regionName != "" && n == p.regionName)
		}
		ok = ok && found
	}
	return ok != f.Negated
}

func (sr *Subregion) matches(p regionPeer) bool {
	if sr.Filters == nil {
		return true
	}
	for i := range sr.Filters {
		if sr.Filters[i].matches(p) {
			return true
		}
	}
	return false
}

func (c *Config) validateRegions() error {
	if c.RegionName != "" {
		if err := transport.ValidateRegionName(c.RegionName); err != nil {
			return fmt.Errorf("zenoh: %w", err)
		}
	}
	for _, sr := range c.Gateway.South {
		for _, f := range sr.Filters {
			for _, n := range f.RegionNames {
				if err := transport.ValidateRegionName(n); err != nil {
					return fmt.Errorf("zenoh: gateway filter: %w", err)
				}
			}
		}
	}
	if c.Gateway.South != nil && c.Mode == ModeClient {
		return errors.New("zenoh: a client cannot be a region gateway")
	}
	return nil
}

// transientRegion is compute_transient_region_of: the region the custom
// gateway configuration puts p in (nil with the auto preset).
func (s *Session) transientRegion(p regionPeer) (*Region, error) {
	if s.cfg.Gateway.South == nil {
		return nil, nil
	}
	for i := range s.cfg.Gateway.South {
		if s.cfg.Gateway.South[i].matches(p) {
			if p.whatami == protocol.Router && s.cfg.Mode != ModeRouter {
				return nil, errors.New("router regions cannot be subregions of non-router regions")
			}
			return &Region{South: true, ID: i, Mode: p.whatami}, nil
		}
	}
	return &North, nil
}

// autoRegion is compute_auto_region: the region of a node of role remote
// and the side of it we stand on, with the auto preset.
func autoRegion(mode, remote WhatAmI) (Region, Bound, error) {
	switch {
	case mode == protocol.Router && remote != protocol.Router,
		mode == protocol.Peer && remote == protocol.Client:
		return defaultSouth(remote), BoundNorth, nil
	case mode == remote && mode != protocol.Client:
		return North, BoundNorth, nil
	case mode == protocol.Client && remote == protocol.Client:
		return North, BoundNone, errors.New("north-north client-client configuration (invalid)")
	default: // a router seen from a peer or client, a peer seen from a client
		return North, BoundSouth, nil
	}
}

// regionOf is compute_region_of: the region of p and the side of p we stand
// on, given the side the custom configuration puts it on (ours) and the side
// p told us we stand on (theirs).
func (s *Session) regionOf(p regionPeer, ours *Region, theirs Bound) (Region, Bound, error) {
	mode := s.cfg.Mode.whatAmI()
	switch {
	case ours == nil && theirs == BoundNone:
		return autoRegion(mode, p.whatami)
	case ours == nil && theirs == BoundSouth:
		return North, BoundSouth, nil
	case ours != nil && ours.South && theirs == BoundNone:
		return *ours, BoundNorth, nil
	case ours == nil && theirs == BoundNorth:
		r, rb, err := autoRegion(mode, p.whatami)
		if err != nil {
			return r, rb, err
		}
		if rb != BoundNorth {
			return r, rb, errors.New("remote's custom configuration conflicts with auto preset")
		}
		return r, BoundNorth, nil
	case ours != nil && !ours.South && theirs == BoundNone:
		r, rb, err := autoRegion(mode, p.whatami)
		if err != nil {
			return r, rb, err
		}
		if r.South {
			return r, rb, errors.New("remote's auto preset conflicts with custom configuration")
		}
		return North, rb, nil
	case ours != nil && !ours.South && theirs == BoundNorth:
		if mode != p.whatami {
			return North, BoundNorth, fmt.Errorf("north-north %s-%s configuration (invalid)", mode, p.whatami)
		}
		return North, BoundNorth, nil
	case ours != nil && ours.South && theirs == BoundSouth:
		return North, BoundNone, errors.New("south-south configuration (invalid)")
	default: // one side north, the other south
		return *ours, theirs, nil
	}
}

// boundFor is the transport's Config.Bound for face f: it records the
// transient region and tells the peer which side of us it stands on.
func (s *Session) boundFor(f *face, p transport.PeerInfo) (Bound, error) {
	rp := regionPeer{zid: p.ZID, whatami: p.WhatAmI, regionName: p.RegionName, ifaces: f.ifaces}
	r, err := s.transientRegion(rp)
	if err != nil {
		return BoundNone, err
	}
	f.transient = r
	if r == nil {
		return BoundNone, nil
	}
	return r.bound(), nil
}

// assignRegionLocked computes the face's region once its handshake is done
// and gives it its routing role. Called with s.mu held.
func (s *Session) assignRegionLocked(f *face, tr *transport.Unicast) error {
	rp := regionPeer{zid: f.zid, whatami: f.whatami, regionName: tr.PeerRegionName(), ifaces: f.ifaces}
	if f.transient == nil && s.cfg.Gateway.South != nil {
		// The transport skipped Config.Bound (e.g. a joined link).
		r, err := s.transientRegion(rp)
		if err != nil {
			return err
		}
		f.transient = r
	}
	region, rb, err := s.regionOf(rp, f.transient, tr.PeerBound())
	if err != nil {
		return err
	}
	f.region, f.remoteBound, f.regionName = region, rb, tr.PeerRegionName()
	f.routed, f.push, f.mesh, f.router = false, false, false, nil
	switch {
	case s.cfg.Mode == ModeRouter && f.whatami == protocol.Router:
		// Routers mesh whatever the region: a south router is in the mesh
		// of its subregion, a gateway is in our (north) mesh.
		f.mesh = true
	case rb == BoundSouth: // our gateway
		f.routed = true
		if s.cfg.Mode != ModeClient {
			f.relay = newRelayState()
		}
	case region.South: // we are its gateway
		f.router = newRouterState()
	default: // north peers
		f.push = true
		f.relay = newRelayState()
	}
	return nil
}

// -----------------------------------------------------------------------------
// Crossing regions
// -----------------------------------------------------------------------------

// crossesLocked reports whether traffic from src (nil: our own session) may
// be sent to dst by region: never back into its own region, except within a
// region of clients. Router-to-router traffic follows the mesh instead.
func (s *Session) crossesLocked(src, dst *face) bool {
	if src == dst || dst.mesh {
		return false
	}
	if src == nil {
		return true
	}
	if dst.router != nil { // a south face
		return src.region != dst.region || dst.region.Mode == protocol.Client
	}
	// North faces (peers, our gateways) only get what comes from the south.
	return src.region.South
}

// gatewayAllowsLocked is zenoh-rust's InterRegionFilter: with several
// gateways between a subregion and the rest, only one of them forwards a
// given message across. origin is the node the message comes from (the mesh
// tree root for traffic from other routers).
func (s *Session) gatewayAllowsLocked(src, dst *face, origin ZID) bool {
	if src == nil || src.region.South == dst.region.South {
		return true // our own traffic, or not crossing a north/south bound
	}
	if src.region.South {
		return s.primaryGatewayLocked(src, src.region, src.zid, origin)
	}
	return s.primaryGatewayLocked(src, dst.region, dst.zid, origin)
}

// primaryGatewayLocked reports whether we are the gateway of south region r
// to forward traffic from src (to or from node of r) across it.
//
// In a peer region it is the highest ZID among the gateways linked to node.
// In a router region, traffic that enters our mesh network here (from our
// session, clients or peers) crosses here; traffic from another router
// crosses at the highest-ZID gateway of the region's mesh, unless it comes
// from one of the region's gateways itself, which does it directly.
func (s *Session) primaryGatewayLocked(src *face, r Region, node, origin ZID) bool {
	switch r.Mode {
	case protocol.Router:
		if !src.mesh {
			return true
		}
		m := s.meshes[r]
		if m == nil {
			return true
		}
		primary := s.zid
		for i, nd := range m.net.nodes {
			if nd == nil || !nd.gateway || i >= len(m.net.trees[0].directions) || (i > 0 && m.net.trees[0].directions[i] < 0) {
				continue
			}
			if nd.zid.Equal(origin) {
				return false
			}
			if zidLess(primary, nd.zid) {
				primary = nd.zid
			}
		}
		return primary.Equal(s.zid)
	case protocol.Peer:
		net := s.southNets[r]
		if net == nil {
			return true
		}
		gwys := net.gatewaysOf(node)
		if len(gwys) == 0 {
			return true
		}
		primary := gwys[0]
		for _, g := range gwys {
			if g.Equal(origin) {
				return false // the origin is a gateway itself: it delivers directly
			}
			if zidLess(primary, g) {
				primary = g
			}
		}
		return primary.Equal(s.zid)
	}
	return true
}

// originOf is the node traffic from src comes from: the mesh tree root for
// traffic from other routers, the face's node otherwise, us for our own.
func (s *Session) originOf(src *face, tree int) ZID {
	if src == nil {
		return s.zid
	}
	if m := s.meshOf(src); m != nil && tree > 0 && tree < len(m.net.nodes) && m.net.nodes[tree] != nil {
		return m.net.nodes[tree].zid
	}
	return src.zid
}

// forwardsLocked combines crossesLocked and gatewayAllowsLocked.
func (s *Session) forwardsLocked(src, dst *face, origin ZID) bool {
	return s.crossesLocked(src, dst) && s.gatewayAllowsLocked(src, dst, origin)
}

// -----------------------------------------------------------------------------
// South peer regions: single-hop gossip at the gateway
// -----------------------------------------------------------------------------

// southNetLocked returns the link-state graph of f's peer subregion (made on
// first use), or nil if f is not in one.
func (s *Session) southNetLocked(f *face) *lsNetwork {
	if !f.region.South || f.region.Mode != protocol.Peer {
		return nil
	}
	n := s.southNets[f.region]
	if n == nil {
		n = newNetworkAs(s.zid, s.cfg.Mode.whatAmI())
		n.singlehop = true
		n.gossip = !s.cfg.Gossip.Disable
		n.nodes[0].gateway = true
		s.southNets[f.region] = n
	}
	return n
}

func (s *Session) southLinkUp(f *face) {
	locs := s.gossipLocators()
	out := outbox{}
	s.mu.Lock()
	n := s.southNetLocked(f)
	if n == nil || n.links[f] != nil || s.faces[string(f.zid.Bytes())] != f {
		s.mu.Unlock()
		return
	}
	n.selfLocs = locs
	s.queueLinkStates(n.addLink(f), out)
	s.mu.Unlock()
	out.send()
}

func (s *Session) southLinkDownLocked(f *face) {
	if n := s.southNets[f.region]; n != nil && n.links[f] != nil {
		n.removeLink(f)
	}
}

func (s *Session) southOAM(f *face, m *protocol.OAM) {
	if m.ID != oamLinkState || m.Enc != protocol.ExtZBuf {
		return
	}
	list, err := decodeLinkStates(m.Buf)
	if err != nil {
		return
	}
	s.southLinkUp(f) // its link states may come before established() adds the link
	locs := s.gossipLocators()
	out := outbox{}
	s.mu.Lock()
	if n := s.southNets[f.region]; n != nil && n.links[f] != nil {
		n.selfLocs = locs
		_, sends := n.linkStates(list, f)
		s.queueLinkStates(sends, out)
	}
	s.mu.Unlock()
	out.send()
}
