// Command tinypeer is a peer-mode smoke test that also builds with TinyGo:
// it connects directly to another peer (no router) and publishes.
//
//	tinypeer tcp/127.0.0.1:7448 demo/hello
package main

import (
	"os"
	"time"

	"github.com/shabunin/gozenoh/zenoh"
)

func main() {
	if len(os.Args) < 3 {
		println("usage: tinypeer <peer-locator> <key>")
		os.Exit(2)
	}
	s, err := zenoh.Open(zenoh.Config{
		Mode:     zenoh.ModePeer,
		Connect:  []string{os.Args[1]},
		Listen:   []string{"tcp/127.0.0.1:0"},
		Scouting: zenoh.ScoutingConfig{Disable: true},
	})
	if err != nil {
		println(err.Error())
		os.Exit(1)
	}
	defer s.Close()
	time.Sleep(300 * time.Millisecond) // let the peer's declarations arrive
	for i := 0; i < 3; i++ {
		_ = s.Put(os.Args[2], []byte("tiny-peer"))
	}
	time.Sleep(200 * time.Millisecond)
	println("peers:", len(s.PeersZID()))
}
