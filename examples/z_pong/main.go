// Command z_pong echoes test/ping onto test/pong (see z_ping).
//
//	go run ./examples/z_pong
package main

import (
	"flag"
	"fmt"

	"github.com/shabunin/tiny-zenoh-go/examples/internal/zargs"
	"github.com/shabunin/tiny-zenoh-go/zenoh"
)

func main() {
	common := zargs.Flags()
	common.ShmFlag()
	noExpress := flag.Bool("no-express", false, "let the messages be batched")
	flag.Parse()

	s := common.Open()
	defer s.Close()

	pub, err := s.DeclarePublisher("test/pong",
		zenoh.WithCongestionControl(zenoh.CongestionBlock), zenoh.WithExpress(!*noExpress))
	if err != nil {
		zargs.Fatal(err)
	}
	defer pub.Undeclare()
	sub, err := s.DeclareSubscriber("test/ping", func(smp zenoh.Sample) { _ = pub.Put(smp.Payload) })
	if err != nil {
		zargs.Fatal(err)
	}
	defer sub.Undeclare()
	fmt.Println("Press CTRL-C to quit...")
	zargs.WaitForCtrlC()
}
