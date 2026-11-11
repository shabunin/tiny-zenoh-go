package zenoh

import (
	"container/heap"
	"errors"
	"math"

	"github.com/shabunin/gozenoh/internal/codec"
	"github.com/shabunin/gozenoh/protocol"
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
}

type lsNetwork struct {
	nodes []*lsNode // index = our psid; self is 0; nil = removed
	links map[*face]*lsLink
	trees []lsTree

	// gossip: link states carry locators (zenoh-rust propagate_locators with
	// multihop gossip): ours are selfLocs, set before each operation.
	gossip   bool
	selfLocs []string
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
	n := &lsNetwork{links: map[*face]*lsLink{}}
	n.nodes = []*lsNode{{zid: self, whatami: w, hasWAI: true, sn: 1, links: map[string]uint16{}}}
	n.trees = []lsTree{{parent: -1, directions: []int{-1}}}
	return n
}

func zkey(z ZID) string { return string(z.Bytes()) }

func (n *lsNetwork) idx(z ZID) int {
	for i, nd := range n.nodes {
		if nd != nil && nd.zid.Equal(z) {
			return i
		}
	}
	return -1
}

func (n *lsNetwork) addNode(nd *lsNode) int {
	n.nodes = append(n.nodes, nd)
	return len(n.nodes) - 1
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
			j := -1
			for k, other := range n.nodes {
				if other != nil && zkey(other.zid) == z {
					j = k
					break
				}
			}
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
			for j, nd := range n.nodes {
				if nd != nil && !seen[j] && zkey(nd.zid) == z {
					stack = append(stack, j)
				}
			}
		}
	}
	var removed []ZID
	for i, nd := range n.nodes {
		if nd != nil && !seen[i] {
			removed = append(removed, nd.zid)
			n.nodes[i] = nil
		}
	}
	return removed
}

// linkStates applies a LinkStateList received from neighbour src. It returns
// the routers that disappeared and the messages to send.
func (n *lsNetwork) linkStates(list []linkState, src *face) ([]ZID, []lsSend) {
	link := n.links[src]
	if link == nil {
		return nil, nil
	}
	type local struct {
		zid   ZID
		ls    linkState
		links map[string]uint16
	}
	var states []local
	for _, ls := range list {
		z := ls.zid
		if !z.IsZero() {
			link.mappings[ls.psid] = z
		} else if z = link.mappings[ls.psid]; z.IsZero() {
			continue // unknown psid
		}
		states = append(states, local{zid: z, ls: ls})
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
			found := false
			for _, nd := range n.nodes {
				if nd != nil && zkey(nd.zid) == z {
					found = true
					break
				}
			}
			if !found {
				zz, _ := protocol.NewZID([]byte(z))
				newNodes = append(newNodes, n.addNode(&lsNode{zid: zz, links: map[string]uint16{}}))
			}
		}
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

// zidLess orders zids as zenoh-rust does (as little-endian u128 values).
func zidLess(a, b ZID) bool {
	var x, y [16]byte
	copy(x[:], a.Bytes())
	copy(y[:], b.Bytes())
	for i := 15; i >= 0; i-- {
		if x[i] != y[i] {
			return x[i] < y[i]
		}
	}
	return false
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

// neighbours lists the nodes sharing an edge with i: an edge exists when both
// ends announce the link.
func (n *lsNetwork) neighbours(i int) []int {
	var out []int
	for z := range n.nodes[i].links {
		for j, nd := range n.nodes {
			if nd != nil && j != i && zkey(nd.zid) == z {
				if _, back := nd.links[zkey(n.nodes[i].zid)]; back {
					out = append(out, j)
				}
			}
		}
	}
	return out
}

type pqItem struct {
	node int
	dist float64
}
type pq []pqItem

func (p pq) Len() int           { return len(p) }
func (p pq) Less(i, j int) bool { return p[i].dist < p[j].dist }
func (p pq) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p *pq) Push(x any)        { *p = append(*p, x.(pqItem)) }
func (p *pq) Pop() any          { o := *p; x := o[len(o)-1]; *p = o[:len(o)-1]; return x }

// shortestPaths returns the predecessor of every node on its shortest path
// from root (-1 for the root and unreachable nodes).
func (n *lsNetwork) shortestPaths(root int) []int {
	dist := make([]float64, len(n.nodes))
	pred := make([]int, len(n.nodes))
	for i := range dist {
		dist[i], pred[i] = math.Inf(1), -1
	}
	dist[root] = 0
	h := &pq{{root, 0}}
	for h.Len() > 0 {
		it := heap.Pop(h).(pqItem)
		if it.dist > dist[it.node] {
			continue
		}
		for _, j := range n.neighbours(it.node) {
			d := it.dist + n.edgeWeight(it.node, j)
			if d < dist[j] {
				dist[j], pred[j] = d, it.node
				heap.Push(h, pqItem{j, d})
			}
		}
	}
	return pred
}

// computeTrees rebuilds the per-root trees and returns, per root, the
// children that are new compared with the previous trees.
func (n *lsNetwork) computeTrees() [][]int {
	old := n.trees
	n.trees = make([]lsTree, len(n.nodes))
	reach := n.shortestPaths(0) // reachability from us
	newChildren := make([][]int, len(n.nodes))
	for r, nd := range n.nodes {
		t := &n.trees[r]
		t.parent = -1
		t.directions = make([]int, len(n.nodes))
		for i := range t.directions {
			t.directions[i] = -1
		}
		if nd == nil {
			continue
		}
		pred := n.shortestPaths(r)
		t.parent = pred[0]
		for i, p := range pred {
			if p == 0 {
				t.children = append(t.children, i)
			}
		}
		for d := range n.nodes {
			if d == 0 || n.nodes[d] == nil || reach[d] < 0 {
				continue
			}
			dir := -1
			for cur := d; pred[cur] >= 0; cur = pred[cur] {
				if pred[cur] == 0 {
					dir = cur
					break
				}
			}
			if dir < 0 {
				dir = t.parent
			}
			t.directions[d] = dir
		}
		for _, c := range t.children {
			isNew := true
			if r < len(old) {
				for _, oc := range old[r].children {
					if oc == c {
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
