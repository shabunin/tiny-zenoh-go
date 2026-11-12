// Command z_get queries a selector and prints the replies.
//
//	go run ./examples/z_get -s 'demo/example/**'
//	go run ./examples/z_get -s 'demo/example/**?arg=1' -p 'query payload' -t ALL
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
	selector := flag.String("s", "demo/example/**", "the selector: a key expression and ?parameters")
	payload := flag.String("p", "", "a payload to send with the query")
	target := flag.String("t", "BEST_MATCHING", "the query target: BEST_MATCHING, ALL or ALL_COMPLETE")
	timeout := flag.Int("o", 10000, "the query timeout in milliseconds")
	flag.Parse()

	s := common.Open()
	defer s.Close()

	fmt.Printf("Sending Query '%s'...\n", *selector)
	opts := []zenoh.GetOption{zenoh.WithTarget(zargs.Target(*target)),
		zenoh.WithTimeout(time.Duration(*timeout) * time.Millisecond)}
	if *payload != "" {
		opts = append(opts, zenoh.WithPayload([]byte(*payload), zenoh.EncodingTextPlain))
	}
	replies, err := s.Get(*selector, opts...)
	if err != nil {
		zargs.Fatal(err)
	}
	for r := range replies {
		zargs.PrintReply(r)
	}
}
