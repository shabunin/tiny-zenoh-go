package zenoh

import (
	"bytes"
	"errors"
	"math"

	"github.com/shabunin/tiny-zenoh-go/internal/codec"
	"github.com/shabunin/tiny-zenoh-go/protocol"
)

// Router link-state network, mirroring zenoh-rust's
// zenoh/src/net/protocol/network.rs in its full-linkstate (router) flavour.
//
// Every router floods the list of routers it is linked to (OAM id 1, a
// LinkStateList). Each node numbers the graph with its own indices ("psids");
// a receiver maps a sender's psids through the zids that sender announced.
// From the graph each router computes, for every node, the shortest-path tree
// rooted there; declarations and data originating at a node travel along its
// tree, tagged with the origin's psid in the NodeId extension.

const oamLinkState = 1

// Link-state option bits.
const (
	lsPID = 1 << 0 // zid present
	lsWAI = 1 << 1 // whatami present
	lsLOC = 1 << 2 // locators present
	lsWGT = 1 << 3 // link weights present
	lsGWY = 1 << 4 // node is a gateway
)

type linkState struct {
	psid, sn uint64
	zid      ZID // zero when absent
	whatami  WhatAmI
	hasWAI   bool
	locators []string
	hasLoc   bool
	links    []uint64
	weights  []uint16 // nil when absent
	gateway  bool
}

// whatAmIBits converts to/from the bitmask form used in link states.
func whatAmIBits(w WhatAmI) byte {
	switch w {
	case protocol.Router:
		return 1
	case protocol.Peer:
		return 2
	}
	return 4
}

func whatAmIFromBits(b byte) (WhatAmI, bool) {
	switch b {
	case 1:
		return protocol.Router, true
	case 2:
		return protocol.Peer, true
	case 4:
		return protocol.Client, true
	}
	return 0, false
}

func encodeLinkStates(list []linkState) []byte {
	var w codec.Writer
	w.WriteZInt(uint64(len(list)))
	for _, ls := range list {
		var opts uint64
		if !ls.zid.IsZero() {
			opts |= lsPID
		}
		if ls.hasWAI {
			opts |= lsWAI
		}
		if ls.hasLoc {
			opts |= lsLOC
		}
		if ls.weights != nil {
			opts |= lsWGT
		}
		if ls.gateway {
			opts |= lsGWY
		}
		w.WriteZInt(opts)
		w.WriteZInt(ls.psid)
		w.WriteZInt(ls.sn)
		if !ls.zid.IsZero() {
			w.WriteZBytes(ls.zid.Bytes())
		}
		if ls.hasWAI {
			_ = w.WriteByte(whatAmIBits(ls.whatami))
		}
		if ls.hasLoc {
			w.WriteZInt(uint64(len(ls.locators)))
			for _, l := range ls.locators {
				w.WriteString(l)
			}
		}
		w.WriteZInt(uint64(len(ls.links)))
		for _, l := range ls.links {
			w.WriteZInt(l)
		}
		for _, wt := range ls.weights {
			w.WriteZInt(uint64(wt))
		}
	}
	return w.Bytes()
}

var errBadLinkState = errors.New("zenoh: malformed link state")

func decodeLinkStates(b []byte) ([]linkState, error) {
	r := codec.NewReader(b)
	n, err := r.ReadZInt()
	if err != nil || n > uint64(len(b)) {
		return nil, errBadLinkState
	}
	out := make([]linkState, 0, n)
	for range n {
		var ls linkState
		opts, err := r.ReadZInt()
		if err != nil {
			return nil, err
		}
		if ls.psid, err = r.ReadZInt(); err != nil {
			return nil, err
		}
		if ls.sn, err = r.ReadZInt(); err != nil {
			return nil, err
		}
		if opts&lsPID != 0 {
			raw, err := r.ReadZBytes()
			if err != nil {
				return nil, err
			}
			if ls.zid, err = protocol.NewZID(raw); err != nil {
				return nil, err
			}
		}
		if opts&lsWAI != 0 {
			bt, err := r.ReadByte()
			if err != nil {
				return nil, err
			}
			if ls.whatami, ls.hasWAI = whatAmIFromBits(bt); !ls.hasWAI {
				return nil, errBadLinkState
			}
		}
		if opts&lsLOC != 0 {
			ls.hasLoc = true
			nl, err := r.ReadZInt()
			if err != nil || nl > uint64(r.Remaining()) {
				return nil, errBadLinkState
			}
			for range nl {
				l, err := r.ReadString()
				if err != nil {
					return nil, err
				}
				ls.locators = append(ls.locators, l)
			}
		}
		nl, err := r.ReadZInt()
		if err != nil || nl > uint64(r.Remaining()) {
			return nil, errBadLinkState
		}
		for range nl {
			l, err := r.ReadZInt()
			if err != nil {
				return nil, err
			}
			ls.links = append(ls.links, l)
		}
		if opts&lsWGT != 0 {
			for range nl {
				wt, err := r.ReadZInt()
				if err != nil {
					return nil, err
				}
				ls.weights = append(ls.weights, uint16(wt))
			}
		}
		ls.gateway = opts&lsGWY != 0
		out = append(out, ls)
	}
	return out, nil
}

// -----------------------------------------------------------------------------
// Graph
// -----------------------------------------------------------------------------

type lsNode struct {
	zid      ZID
	whatami  WhatAmI
	hasWAI   bool
	locators []string
	hasLoc   bool
	sn       uint64
	links    map[string]uint16 // neighbour zid -> raw weight (0 = default)
	gateway  bool
}

// lsLink is a direct link to a neighbouring router.
type lsLink struct {
	f        *face
	mappings map[uint64]ZID // the neighbour's psids
}

type lsTree struct {
	parent     int   // -1: none
	children   []int // nodes whose path to the root goes through us
	directions []int // per destination: our next hop toward it on this tree, -1 if none
	// root and kids are the zkeys of the root and children when computed:
	// node slots are reused, so indices alone do not say whether a child
	// is new to a tree.
	root string
	kids []string
}

type lsNetwork struct {
	nodes []*lsNode // index = our psid; self is 0; nil = removed
	// byZID indexes nodes by zkey (live ones only): a mesh of a few hundred
	// routers looked nodes up by scanning them all, several times per link
	// of every link state received, and fell behind while forming.
	byZID map[string]int
	links map[*face]*lsLink
	trees []lsTree

	// gossip: link states carry locators (zenoh-rust propagate_locators with
	// multihop gossip): ours are selfLocs, set before each operation.
	gossip   bool
	selfLocs []string
	// singlehop: a region gateway's graph of its south peers (zenoh-rust
	// Network with neither full link state nor multihop gossip): nodes come
	// only from received link states, and a neighbour's own state is relayed
	// to the other neighbours, nothing further.
	singlehop bool
	// changed lists the nodes the last linkStates call added or updated.
	changed []int
}

type lsSend struct {
	f    *face
	list []linkState
}

func newNetwork(self ZID) *lsNetwork { return newNetworkAs(self, protocol.Router) }

// newNetworkAs makes a link-state graph rooted at a node of role w.
func newNetworkAs(self ZID, w WhatAmI) *lsNetwork {
	n := &lsNetwork{links: map[*face]*lsLink{}, byZID: map[string]int{zkey(self): 0}}
	n.nodes = []*lsNode{{zid: self, whatami: w, hasWAI: true, sn: 1, links: map[string]uint16{}}}
	n.trees = []lsTree{{parent: -1, directions: []int{-1}}}
	return n
}

func zkey(z ZID) string { return string(z.Bytes()) }

// knows reports whether the node with zkey r is in the graph (other than us).
func (n *lsNetwork) knows(r string) bool {
	i, ok := n.byZID[r]
	return ok && i > 0
}

func (n *lsNetwork) idx(z ZID) int { return n.idxKey(zkey(z)) }

// idxKey is idx by zkey.
func (n *lsNetwork) idxKey(k string) int {
	if i, ok := n.byZID[k]; ok {
		return i
	}
	return -1
}

// addNode puts nd in the first free slot (as zenoh-rust's StableGraph
// does): otherwise every router that drops out of the mesh and comes back
// takes a new one, and the trees, N x N, grew with each mass reconnect
// (38 MB on a 225-router lab after three).
func (n *lsNetwork) addNode(nd *lsNode) int {
	i := len(n.nodes)
	for j := 1; j < len(n.nodes); j++ {
		if n.nodes[j] == nil {
			i = j
			break
		}
	}
	if i == len(n.nodes) {
		n.nodes = append(n.nodes, nd)
	} else {
		n.nodes[i] = nd
	}
	n.byZID[zkey(nd.zid)] = i
	return i
}

// dropNode removes node i from the graph.
func (n *lsNetwork) dropNode(i int) {
	if nd := n.nodes[i]; nd != nil {
		delete(n.byZID, zkey(nd.zid))
		n.nodes[i] = nil
	}
}

func (n *lsNetwork) entry(i int, withZID, withLinks bool) linkState {
	nd := n.nodes[i]
	ls := linkState{psid: uint64(i), sn: nd.sn, whatami: nd.whatami, hasWAI: nd.hasWAI, gateway: nd.gateway}
	if withZID {
		ls.zid = nd.zid
	}
	if n.gossip {
		if i == 0 {
			ls.locators, ls.hasLoc = n.selfLocs, true
		} else {
			ls.locators, ls.hasLoc = nd.locators, nd.hasLoc
		}
	}
	if withLinks {
		var weights []uint16
		custom := false
		for z, w := range nd.links {
			j := n.idxKey(z)
			if j < 0 {
				continue
			}
			ls.links = append(ls.links, uint64(j))
			weights = append(weights, w)
			custom = custom || w != 0
		}
		if custom {
			ls.weights = weights
		}
	}
	return ls
}

// addLink registers a new neighbour router and returns the link-state
// messages to send.
func (n *lsNetwork) addLink(f *face) []lsSend {
	n.links[f] = &lsLink{f: f, mappings: map[uint64]ZID{}}
	if n.singlehop {
		self := n.nodes[0]
		self.links[zkey(f.zid)] = 0
		self.sn++
		// Tell the new neighbour about the others, so it can connect to them.
		var list []linkState
		for j, nd := range n.nodes {
			if j > 0 && nd != nil && n.isNeighbour(nd.zid) {
				list = append(list, n.entry(j, true, false))
			}
		}
		if len(list) == 0 {
			return nil
		}
		return []lsSend{{f, list}}
	}
	i := n.idx(f.zid)
	isNew := i < 0
	if isNew {
		i = n.addNode(&lsNode{zid: f.zid, whatami: f.whatami, hasWAI: true, links: map[string]uint16{}})
	}
	self := n.nodes[0]
	self.links[zkey(f.zid)] = 0
	self.sn++
	var out []lsSend
	for g := range n.links {
		if g == f {
			continue
		}
		if isNew {
			out = append(out, lsSend{g, []linkState{n.entry(i, true, false), n.entry(0, false, true)}})
		} else {
			out = append(out, lsSend{g, []linkState{n.entry(0, false, true)}})
		}
	}
	var all []linkState
	for j, nd := range n.nodes {
		if nd != nil {
			all = append(all, n.entry(j, true, true))
		}
	}
	return append(out, lsSend{f, all})
}

// removeLink drops a neighbour; it returns the routers no longer reachable
// and the messages to send.
func (n *lsNetwork) removeLink(f *face) ([]ZID, []lsSend) {
	if _, ok := n.links[f]; !ok {
		return nil, nil
	}
	delete(n.links, f)
	delete(n.nodes[0].links, zkey(f.zid))
	if n.singlehop {
		if !n.isNeighbour(f.zid) {
			if i := n.idx(f.zid); i > 0 {
				n.dropNode(i)
			}
		}
		return nil, nil
	}
	removed := n.removeDetached()
	n.nodes[0].sn++
	var out []lsSend
	for g := range n.links {
		out = append(out, lsSend{g, []linkState{n.entry(0, false, true)}})
	}
	return removed, out
}

// removeDetached deletes nodes no longer reachable from us through
// announced links.
func (n *lsNetwork) removeDetached() []ZID {
	seen := make([]bool, len(n.nodes))
	stack := []int{0}
	for len(stack) > 0 {
		i := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if seen[i] {
			continue
		}
		seen[i] = true
		for z := range n.nodes[i].links {
			if j := n.idxKey(z); j >= 0 && !seen[j] {
				stack = append(stack, j)
			}
		}
	}
	var removed []ZID
	for i, nd := range n.nodes {
		if nd != nil && !seen[i] {
			removed = append(removed, nd.zid)
			n.dropNode(i)
		}
	}
	return removed
}

// linkStates applies a LinkStateList received from neighbour src. It returns
// the routers that disappeared and the messages to send.
func (n *lsNetwork) linkStates(list []linkState, src *face) ([]ZID, []lsSend) {
	n.changed = n.changed[:0]
	link := n.links[src]
	if link == nil {
		return nil, nil
	}
	var states []lsLocal
	for _, ls := range list {
		z := ls.zid
		if !z.IsZero() {
			link.mappings[ls.psid] = z
		} else if z = link.mappings[ls.psid]; z.IsZero() {
			continue // unknown psid
		}
		states = append(states, lsLocal{zid: z, ls: ls})
	}
	for i := range states {
		st := &states[i]
		st.links = map[string]uint16{}
		for k, psid := range st.ls.links {
			z, ok := link.mappings[psid]
			if !ok {
				continue
			}
			var w uint16
			if st.ls.weights != nil && k < len(st.ls.weights) {
				w = st.ls.weights[k]
			}
			st.links[zkey(z)] = w
		}
	}

	if n.singlehop {
		return nil, n.singlehopStates(states)
	}

	var newNodes []int
	updated := map[int]bool{}
	for _, st := range states {
		i := n.idx(st.zid)
		whatami, hasWAI := st.ls.whatami, st.ls.hasWAI
		if !hasWAI {
			whatami, hasWAI = protocol.Router, true
		}
		if i >= 0 {
			nd := n.nodes[i]
			if nd.sn >= st.ls.sn {
				continue
			}
			old := nd.sn
			nd.sn, nd.whatami, nd.hasWAI = st.ls.sn, whatami, hasWAI
			if st.ls.hasLoc {
				nd.locators, nd.hasLoc = st.ls.locators, true
			}
			nd.links, nd.gateway = st.links, st.ls.gateway
			if old == 0 {
				newNodes = append(newNodes, i)
			} else {
				updated[i] = true
			}
		} else {
			i = n.addNode(&lsNode{zid: st.zid, whatami: whatami, hasWAI: hasWAI, locators: st.ls.locators,
				hasLoc: st.ls.hasLoc, sn: st.ls.sn, links: st.links, gateway: st.ls.gateway})
			newNodes = append(newNodes, i)
		}
		for z := range st.links {
			if n.idxKey(z) < 0 {
				zz, _ := protocol.NewZID([]byte(z))
				newNodes = append(newNodes, n.addNode(&lsNode{zid: zz, links: map[string]uint16{}}))
			}
		}
	}
	if len(newNodes) == 0 && len(updated) == 0 {
		return nil, nil // all known already (the same states come over every link)
	}
	removed := n.removeDetached()
	keep := func(i int) bool { return n.nodes[i] != nil }
	n.changed = n.changed[:0]
	for _, i := range newNodes {
		if keep(i) {
			n.changed = append(n.changed, i)
		}
	}
	for i := range updated {
		if keep(i) {
			n.changed = append(n.changed, i)
		}
	}

	// Propagate: new nodes in full to every link; updated nodes' links to
	// every link but the sender and the node itself.
	var out []lsSend
	for g := range n.links {
		var l []linkState
		for _, i := range newNodes {
			if keep(i) {
				l = append(l, n.entry(i, true, true))
			}
		}
		if g != src {
			for i := range updated {
				if keep(i) && !n.nodes[i].zid.Equal(g.zid) {
					l = append(l, n.entry(i, false, true))
				}
			}
		}
		if len(l) > 0 {
			out = append(out, lsSend{g, l})
		}
	}
	return removed, out
}

// lsLocal is a received link state with its ids mapped to ours.
type lsLocal struct {
	zid   ZID
	ls    linkState
	links map[string]uint16
}

func (n *lsNetwork) isNeighbour(z ZID) bool {
	for f := range n.links {
		if f.zid.Equal(z) {
			return true
		}
	}
	return false
}

// singlehopStates is zenoh-rust process_singlehop_gossip_linkstate.
func (n *lsNetwork) singlehopStates(states []lsLocal) []lsSend {
	var out []lsSend
	for _, st := range states {
		whatami, hasWAI := st.ls.whatami, st.ls.hasWAI
		if !hasWAI {
			whatami, hasWAI = protocol.Router, true
		}
		i := n.idx(st.zid)
		if i == 0 {
			continue
		}
		if i < 0 {
			i = n.addNode(&lsNode{zid: st.zid, whatami: whatami, hasWAI: hasWAI, locators: st.ls.locators,
				hasLoc: st.ls.hasLoc, sn: st.ls.sn, links: st.links, gateway: st.ls.gateway})
			if !st.ls.hasLoc {
				continue
			}
		} else {
			nd := n.nodes[i]
			if nd.sn >= st.ls.sn {
				continue
			}
			nd.sn = st.ls.sn
			// Gossip sends a node's links only to its gateways, and states
			// of other nodes without them: keep what we know then.
			if len(st.links) > 0 {
				nd.links = st.links
			}
			if !st.ls.hasLoc || (nd.hasLoc && equalStrings(nd.locators, st.ls.locators)) {
				continue
			}
			nd.locators, nd.hasLoc = st.ls.locators, true
		}
		if n.gossip && n.isNeighbour(st.zid) {
			e := n.entry(i, true, false)
			for g := range n.links {
				if !g.zid.Equal(st.zid) {
					out = append(out, lsSend{g, []linkState{e}})
				}
			}
		}
	}
	return out
}

// gatewaysOf lists the gateways (us included) that node z is linked to,
// or nil if z is unknown.
func (n *lsNetwork) gatewaysOf(z ZID) []ZID {
	i := n.idx(z)
	if i < 0 {
		return nil
	}
	var out []ZID
	for _, nd := range n.nodes {
		if nd == nil || !nd.gateway {
			continue
		}
		if _, ok := n.nodes[i].links[zkey(nd.zid)]; ok {
			out = append(out, nd.zid)
		}
	}
	return out
}

// localPSID maps a psid announced by neighbour f to our index.
func (n *lsNetwork) localPSID(f *face, psid uint64) int {
	link := n.links[f]
	if link == nil {
		return -1
	}
	z, ok := link.mappings[psid]
	if !ok {
		return -1
	}
	return n.idx(z)
}

// faceOf returns the neighbour face for node i, if directly linked.
func (n *lsNetwork) faceOf(i int) *face {
	if i < 0 || i >= len(n.nodes) || n.nodes[i] == nil {
		return nil
	}
	for f := range n.links {
		if f.zid.Equal(n.nodes[i].zid) {
			return f
		}
	}
	return nil
}

// zidLess orders zids as zenoh-rust does: uhlc's ID derives Ord on its
// 16 little-endian bytes, so the comparison is lexicographic over the wire
// bytes (zero-padded), *not* numeric. Checked against zenoh-rust's edge
// weights, which hash the smaller zid first.
func zidLess(a, b ZID) bool {
	var x, y [16]byte
	copy(x[:], a.Bytes())
	copy(y[:], b.Bytes())
	return bytes.Compare(x[:], y[:]) < 0
}

// edgeWeight reproduces zenoh-rust's update_edge: the larger configured
// weight (default 100), times a tiny deterministic jitter derived from both
// zids with SipHash-1-3, so all routers break ties identically.
func (n *lsNetwork) edgeWeight(i, j int) float64 {
	a, b := n.nodes[i], n.nodes[j]
	lo, hi := a.zid, b.zid
	if zidLess(hi, lo) {
		lo, hi = hi, lo
	}
	var msg [32]byte
	copy(msg[:16], lo.Bytes())
	copy(msg[16:], hi.Bytes())
	h := uint32(sipHash13(msg[:]))
	w1, w2 := a.links[zkey(b.zid)], b.links[zkey(a.zid)]
	w := uint16(100)
	switch {
	case w1 != 0 && w2 != 0:
		w = max(w1, w2)
	case w1 != 0:
		w = w1
	case w2 != 0:
		w = w2
	}
	return float64(w) * (1.0 + 0.01*(float64(h)/float64(math.MaxUint32)))
}

// lsEdge is a neighbour in the graph, with the weight of the edge to it.
type lsEdge struct {
	to int
	w  float64
}

// adjacency lists, per node, the nodes sharing an edge with it (an edge
// exists when both ends announce the link), with the edges' weights. It is
// built once per tree computation: the trees take a Dijkstra per node, and
// looking neighbours and weights up anew at every step made a 49-router
// mesh spend seconds per batch of link states (seen in the lab).
func (n *lsNetwork) adjacency() [][]lsEdge {
	idx := make(map[string]int, len(n.nodes))
	for j, nd := range n.nodes {
		if nd != nil {
			idx[zkey(nd.zid)] = j
		}
	}
	adj := make([][]lsEdge, len(n.nodes))
	for i, nd := range n.nodes {
		if nd == nil {
			continue
		}
		zi := zkey(nd.zid)
		for z := range nd.links {
			j, ok := idx[z]
			if !ok || j == i {
				continue
			}
			if _, back := n.nodes[j].links[zi]; back {
				adj[i] = append(adj[i], lsEdge{j, n.edgeWeight(i, j)})
			}
		}
	}
	return adj
}

type pqItem struct {
	node int
	dist float64
}

// pq is a binary min-heap on dist (container/heap boxed every item).
type pq []pqItem

func (p *pq) push(it pqItem) {
	h := append(*p, it)
	for i := len(h) - 1; i > 0; {
		up := (i - 1) / 2
		if h[up].dist <= h[i].dist {
			break
		}
		h[up], h[i] = h[i], h[up]
		i = up
	}
	*p = h
}

func (p *pq) pop() pqItem {
	h := *p
	top := h[0]
	last := len(h) - 1
	h[0] = h[last]
	h = h[:last]
	for i := 0; ; {
		l, r, m := 2*i+1, 2*i+2, i
		if l < len(h) && h[l].dist < h[m].dist {
			m = l
		}
		if r < len(h) && h[r].dist < h[m].dist {
			m = r
		}
		if m == i {
			break
		}
		h[i], h[m] = h[m], h[i]
		i = m
	}
	*p = h
	return top
}

// shortestPaths returns the predecessor of every node on its shortest path
// from root (-1 for the root and unreachable nodes), over adj, and the
// reachable nodes in the order they were settled (each after its
// predecessor). dist, pred, order and h are scratch space, reused.
func (n *lsNetwork) shortestPaths(root int, adj [][]lsEdge) []int {
	var sc spScratch
	pred, _ := sc.run(len(n.nodes), root, adj)
	return pred
}

type spScratch struct {
	dist  []float64
	pred  []int
	order []int
	h     pq
}

func (sc *spScratch) run(nn, root int, adj [][]lsEdge) (pred, order []int) {
	if cap(sc.dist) < nn {
		sc.dist, sc.pred = make([]float64, nn), make([]int, nn)
	}
	dist, pred := sc.dist[:nn], sc.pred[:nn]
	for i := range dist {
		dist[i], pred[i] = math.Inf(1), -1
	}
	order = sc.order[:0]
	dist[root] = 0
	h := append(sc.h[:0], pqItem{root, 0})
	for len(h) > 0 {
		it := h.pop()
		if it.dist > dist[it.node] {
			continue
		}
		order = append(order, it.node)
		for _, e := range adj[it.node] {
			d := it.dist + e.w
			if d < dist[e.to] {
				dist[e.to], pred[e.to] = d, it.node
				h.push(pqItem{e.to, d})
			}
		}
	}
	sc.order, sc.h = order, h
	return pred, order
}

// computeTrees rebuilds the per-root trees and returns, per root, the
// children that are new compared with the previous trees.
func (n *lsNetwork) computeTrees() [][]int {
	old := n.trees
	nn := len(n.nodes)
	n.trees = make([]lsTree, nn)
	adj := n.adjacency()
	var sc spScratch
	reach := make([]bool, nn) // reachable from us
	_, order := sc.run(nn, 0, adj)
	for _, i := range order {
		reach[i] = true
	}
	newChildren := make([][]int, nn)
	dirs := make([]int, nn*nn) // the trees' directions, one backing array
	for r, nd := range n.nodes {
		t := &n.trees[r]
		t.parent = -1
		t.directions = dirs[r*nn : (r+1)*nn : (r+1)*nn]
		for i := range t.directions {
			t.directions[i] = -1
		}
		if nd == nil {
			continue
		}
		pred, order := sc.run(nn, r, adj)
		t.parent = pred[0]
		t.root = zkey(nd.zid)
		// Our next hop toward d on this tree is the node right below us on
		// the root's path to d, when we are on it: hop[d] = d if pred[d] is
		// us, else hop[pred[d]]; settle order puts pred[d] first. Otherwise
		// it is our parent.
		hop := t.directions // filled in place, then the -1s get the parent
		for _, d := range order {
			switch p := pred[d]; {
			case p < 0:
			case p == 0:
				hop[d] = d
				t.children = append(t.children, d)
				t.kids = append(t.kids, zkey(n.nodes[d].zid))
			default:
				hop[d] = hop[p]
			}
		}
		for d := range hop {
			switch {
			case d == 0 || n.nodes[d] == nil || !reach[d]:
				hop[d] = -1
			case hop[d] < 0:
				hop[d] = t.parent
			}
		}
		for k, c := range t.children {
			isNew := true
			if r < len(old) && old[r].root == t.root {
				for _, ok := range old[r].kids {
					if ok == t.kids[k] {
						isNew = false
						break
					}
				}
			}
			if isNew {
				newChildren[r] = append(newChildren[r], c)
			}
		}
	}
	return newChildren
}
