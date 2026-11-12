// Command z_sub subscribes to a key expression and prints what it receives.
//
//	go run ./examples/z_sub -k 'demo/example/**'
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
	key := flag.String("k", "demo/example/**", "the key expression to subscribe to")
	flag.Parse()

	s := common.Open()
	defer s.Close()

	fmt.Printf("Declaring Subscriber on '%s'...\n", *key)
	sub, err := s.DeclareSubscriber(*key, func(smp zenoh.Sample) {
		fmt.Printf(">> [Subscriber] Received %s ('%s': '%s')", zargs.Kind(smp.Kind), smp.KeyExpr, zargs.Payload(smp.Payload))
		if smp.Attachment != nil {
			fmt.Printf(" (%s)", zargs.Payload(smp.Attachment))
		}
		fmt.Println()
	})
	if err != nil {
		zargs.Fatal(err)
	}
	defer sub.Undeclare()
	fmt.Println("Press CTRL-C to quit...")
	zargs.WaitForCtrlC()
}
