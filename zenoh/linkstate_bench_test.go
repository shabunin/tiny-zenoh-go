package zenoh

import (
	"fmt"
	"testing"

	"github.com/shabunin/tiny-zenoh-go/protocol"
)

// gridNetwork is the link-state graph of a rows x cols grid of routers, as
// router 0 (a corner) sees it.
func gridNetwork(b testing.TB, rows, cols int) *lsNetwork {
	zids := make([]ZID, rows*cols)
	for i := range zids {
		z, err := protocol.ParseZID(fmt.Sprintf("%02x%02x", 0x10+i/256, i%256))
		if err != nil {
			b.Fatal(err)
		}
		zids[i] = z
	}
	n := newNetworkAs(zids[0], protocol.Router)
	for i := 1; i < len(zids); i++ {
		n.addNode(&lsNode{zid: zids[i], whatami: protocol.Router, links: map[string]uint16{}})
	}
	link := func(i, j int) {
		n.nodes[i].links[zkey(zids[j])] = 0
		n.nodes[j].links[zkey(zids[i])] = 0
	}
	for i := range zids {
		if (i+1)%cols != 0 {
			link(i, i+1)
		}
		if i+cols < len(zids) {
			link(i, i+cols)
		}
	}
	return n
}

func BenchmarkComputeTrees(b *testing.B) {
	for _, size := range []int{7, 10, 15, 20} {
		b.Run(fmt.Sprintf("grid%dx%d", size, size), func(b *testing.B) {
			n := gridNetwork(b, size, size)
			b.ReportAllocs()
			for b.Loop() {
				n.computeTrees()
			}
		})
	}
}

// BenchmarkLinkStateUpdate: router 0 of a grid gets one neighbour's new
// link state (a link of a far router changed), as each router does many
// times over while a mesh forms.
func BenchmarkLinkStateUpdate(b *testing.B) {
	for _, size := range []int{10, 15, 20} {
		b.Run(fmt.Sprintf("grid%dx%d", size, size), func(b *testing.B) {
			n := gridNetwork(b, size, size)
			nb := &face{zid: n.nodes[1].zid}
			n.links[nb] = &lsLink{f: nb, mappings: map[uint64]ZID{}}
			far := len(n.nodes) - 1
			// the neighbour names nodes by its own psids: here, ours
			for i, nd := range n.nodes {
				n.links[nb].mappings[uint64(i)] = nd.zid
			}
			st := n.entry(far, true, true)
			b.ReportAllocs()
			for b.Loop() {
				st.sn++
				n.linkStates([]linkState{st}, nb)
			}
		})
	}
}
