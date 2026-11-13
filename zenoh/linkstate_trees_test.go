package zenoh

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/shabunin/tiny-zenoh-go/protocol"
)

// TestComputeTreesDirections checks the trees' next hops against walking
// each destination's path up to us, on random graphs with removed nodes.
func TestComputeTreesDirections(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	for round := 0; round < 30; round++ {
		nn := 5 + rng.Intn(60)
		zids := make([]ZID, nn)
		for i := range zids {
			z, err := protocol.ParseZID(fmt.Sprintf("%02x%02x%02x", round+1, 0x10+i/256, i%256))
			if err != nil {
				t.Fatal(err)
			}
			zids[i] = z
		}
		n := newNetworkAs(zids[0], protocol.Router)
		for i := 1; i < nn; i++ {
			n.addNode(&lsNode{zid: zids[i], whatami: protocol.Router, links: map[string]uint16{}})
		}
		for e := 0; e < nn*2; e++ {
			i, j := rng.Intn(nn), rng.Intn(nn)
			if i == j {
				continue
			}
			n.nodes[i].links[zkey(zids[j])] = uint16(rng.Intn(3) * 50)
			if rng.Intn(10) > 0 { // some links one-sided: no edge
				n.nodes[j].links[zkey(zids[i])] = 0
			}
		}
		for k := 0; k < nn/10; k++ {
			if i := 1 + rng.Intn(nn-1); n.nodes[i] != nil {
				n.dropNode(i)
			}
		}
		n.computeTrees()
		adj := n.adjacency()
		reach := n.shortestPaths(0, adj)
		for r, nd := range n.nodes {
			if nd == nil {
				continue
			}
			pred := n.shortestPaths(r, adj)
			tr := n.trees[r]
			if tr.parent != pred[0] {
				t.Fatalf("round %d tree %d: parent %d, want %d", round, r, tr.parent, pred[0])
			}
			for d := range n.nodes {
				want := -1
				if d != 0 && n.nodes[d] != nil && (reach[d] >= 0 || d == 0) {
					for cur := d; pred[cur] >= 0; cur = pred[cur] {
						if pred[cur] == 0 {
							want = cur
							break
						}
					}
					if want < 0 {
						want = tr.parent
					}
				}
				if tr.directions[d] != want {
					t.Fatalf("round %d tree %d dest %d: next hop %d, want %d", round, r, d, tr.directions[d], want)
				}
			}
		}
	}
}

// TestNodeSlotsReused: routers that leave and come back reuse the graph's
// slots, and a router in a reused slot is a new child for the trees (so
// declarations are sent to it).
func TestNodeSlotsReused(t *testing.T) {
	zid := func(i int) ZID {
		z, err := protocol.ParseZID(fmt.Sprintf("%04x", 0x1000+i))
		if err != nil {
			t.Fatal(err)
		}
		return z
	}
	self := zid(0)
	n := newNetworkAs(self, protocol.Router)
	join := func(i int) int {
		k := n.addNode(&lsNode{zid: zid(i), whatami: protocol.Router, links: map[string]uint16{zkey(self): 0}})
		n.nodes[0].links[zkey(zid(i))] = 0
		return k
	}
	leave := func(k int) {
		delete(n.nodes[0].links, zkey(n.nodes[k].zid))
		n.dropNode(k)
	}
	a := join(1)
	if nc := n.computeTrees(); len(nc[0]) != 1 || nc[0][0] != a {
		t.Fatalf("first child: new children of our tree %v", nc[0])
	}
	for round := 2; round < 50; round++ {
		leave(a)         // and another router takes its place before the trees are
		b := join(round) // recomputed (they are 100 ms after a change)
		if b != a {
			t.Fatalf("round %d: slot %d, want the freed %d", round, b, a)
		}
		if nc := n.computeTrees(); len(nc[0]) != 1 || nc[0][0] != b {
			t.Fatalf("round %d: a router in a reused slot is not a new child: %v", round, nc[0])
		}
	}
	if len(n.nodes) != 2 {
		t.Fatalf("%d node slots for 2 nodes", len(n.nodes))
	}
}

// TestChildLinkFlap: a neighbour whose link went down and came back between
// two computations of the trees (a new face, same router) has lost the
// declarations it had from us: it is a new child again.
func TestChildLinkFlap(t *testing.T) {
	self, _ := protocol.ParseZID("1000")
	other, _ := protocol.ParseZID("1001")
	n := newNetworkAs(self, protocol.Router)
	connect := func() *face {
		f := &face{zid: other}
		n.links[f] = &lsLink{f: f, mappings: map[uint64]ZID{}}
		n.nodes[0].links[zkey(other)] = 0
		return f
	}
	f1 := connect()
	k := n.addNode(&lsNode{zid: other, whatami: protocol.Router, links: map[string]uint16{zkey(self): 0}})
	if nc := n.computeTrees(); len(nc[0]) != 1 || nc[0][0] != k {
		t.Fatalf("first link: new children %v", nc[0])
	}
	if nc := n.computeTrees(); len(nc[0]) != 0 {
		t.Fatalf("nothing changed: new children %v", nc[0])
	}
	delete(n.links, f1) // down and up again before the next computation
	connect()
	if nc := n.computeTrees(); len(nc[0]) != 1 || nc[0][0] != k {
		t.Fatalf("after the flap: new children %v, want [%d]", nc[0], k)
	}
}
