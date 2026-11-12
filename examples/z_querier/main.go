// Command z_querier declares a querier and queries through it every second.
//
//	go run ./examples/z_querier -s 'demo/example/**' -p 'Querier Get from Go!'
package main

import (
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/shabunin/tiny-zenoh-go/examples/internal/zargs"
	"github.com/shabunin/tiny-zenoh-go/zenoh"
)

func main() {
	common := zargs.Flags()
	selector := flag.String("s", "demo/example/**", "the selector: a key expression and ?parameters")
	payload := flag.String("p", "", "a payload to send with each query")
	target := flag.String("t", "BEST_MATCHING", "the query target: BEST_MATCHING, ALL or ALL_COMPLETE")
	timeout := flag.Int("o", 10000, "the query timeout in milliseconds")
	flag.Parse()

	s := common.Open()
	defer s.Close()

	key, params, _ := strings.Cut(*selector, "?")
	fmt.Printf("Declaring Querier on '%s'...\n", key)
	q, err := s.DeclareQuerier(key, zenoh.WithTarget(zargs.Target(*target)),
		zenoh.WithTimeout(time.Duration(*timeout)*time.Millisecond))
	if err != nil {
		zargs.Fatal(err)
	}
	defer q.Undeclare()
	fmt.Println("Press CTRL-C to quit...")
	for idx := 0; ; idx++ {
		time.Sleep(time.Second)
		buf := fmt.Sprintf("[%4d] %s", idx, *payload)
		fmt.Printf("Querying '%s' with payload: '%s'...\n", *selector, buf)
		replies, err := q.Get(params, zenoh.WithPayload([]byte(buf), zenoh.EncodingTextPlain))
		if err != nil {
			zargs.Fatal(err)
		}
		for r := range replies {
			zargs.PrintReply(r)
		}
	}
}
