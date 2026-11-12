// Command z_get_liveliness lists the liveliness tokens alive now.
//
//	go run ./examples/z_get_liveliness -k 'group1/**'
package main

import (
	"flag"
	"fmt"
	"time"

	"github.com/shabunin/tiny-zenoh-go/examples/internal/zargs"
	"github.com/shabunin/tiny-zenoh-go/zenoh"
)

func main() {
	common := zargs.Flags()
	key := flag.String("k", "group1/**", "the key expression of the tokens")
	timeout := flag.Int("o", 10000, "the query timeout in milliseconds")
	flag.Parse()

	s := common.Open()
	defer s.Close()

	fmt.Printf("Sending Liveliness Query '%s'...\n", *key)
	replies, err := s.Liveliness().Get(*key, zenoh.WithTimeout(time.Duration(*timeout)*time.Millisecond))
	if err != nil {
		zargs.Fatal(err)
	}
	for r := range replies {
		if r.Sample != nil {
			fmt.Printf(">> Alive token ('%s')\n", r.Sample.KeyExpr)
		} else if r.Err != nil {
			fmt.Printf(">> Received (ERROR: '%s')\n", zargs.Payload(r.Err.Payload))
		}
	}
}
