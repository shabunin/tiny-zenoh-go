// Command z_sub_liveliness reports liveliness tokens as they appear and
// disappear.
//
//	go run ./examples/z_sub_liveliness -k 'group1/**' --history
package main

import (
	"flag"
	"fmt"

	"github.com/shabunin/tiny-zenoh-go/examples/internal/zargs"
	"github.com/shabunin/tiny-zenoh-go/zenoh"
)

func main() {
	common := zargs.Flags()
	key := flag.String("k", "group1/**", "the key expression of the tokens")
	history := flag.Bool("history", false, "also report the tokens already alive")
	flag.Parse()

	s := common.Open()
	defer s.Close()

	fmt.Printf("Declaring Liveliness Subscriber on '%s'...\n", *key)
	sub, err := s.Liveliness().DeclareSubscriber(*key, func(smp zenoh.Sample) {
		if smp.Kind == zenoh.SampleKindPut {
			fmt.Printf(">> [LivelinessSubscriber] New alive token ('%s')\n", smp.KeyExpr)
		} else {
			fmt.Printf(">> [LivelinessSubscriber] Dropped token ('%s')\n", smp.KeyExpr)
		}
	}, *history)
	if err != nil {
		zargs.Fatal(err)
	}
	defer sub.Undeclare()
	fmt.Println("Press CTRL-C to quit...")
	zargs.WaitForCtrlC()
}
