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
		n.nodes = append(n.nodes, &lsNode{zid: zids[i], whatami: protocol.Router, links: map[string]uint16{}})
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
	for _, size := range []int{7, 10} {
		b.Run(fmt.Sprintf("grid%dx%d", size, size), func(b *testing.B) {
			n := gridNetwork(b, size, size)
			b.ReportAllocs()
			for b.Loop() {
				n.computeTrees()
			}
		})
	}
}
