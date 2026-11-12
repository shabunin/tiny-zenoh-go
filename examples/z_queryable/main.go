// Command z_queryable answers queries on a key expression.
//
//	go run ./examples/z_queryable -k demo/example/queryable -p 'Hi from Go!'
package main

import (
	"flag"
	"fmt"

	"github.com/shabunin/tiny-zenoh-go/examples/internal/zargs"
	"github.com/shabunin/tiny-zenoh-go/zenoh"
)

func main() {
	common := zargs.Flags()
	key := flag.String("k", "demo/example/zenoh-go-queryable", "the key expression to answer on")
	payload := flag.String("p", "Queryable from Go!", "the payload of the replies")
	complete := flag.Bool("complete", false, "declare the queryable complete")
	flag.Parse()

	s := common.Open()
	defer s.Close()

	fmt.Printf("Declaring Queryable on '%s'...\n", *key)
	q, err := s.DeclareQueryable(*key, func(q *zenoh.Query) {
		if p := q.Payload(); len(p) > 0 {
			fmt.Printf(">> [Queryable ] Received Query '%s' with payload '%s'\n", q.Selector(), zargs.Payload(p))
		} else {
			fmt.Printf(">> [Queryable ] Received Query '%s'\n", q.Selector())
		}
		fmt.Printf(">> [Queryable ] Responding ('%s': '%s')\n", *key, *payload)
		if err := q.Reply(*key, []byte(*payload)); err != nil {
			fmt.Println(">> [Queryable ] Reply failed:", err)
		}
	}, zenoh.WithComplete(*complete))
	if err != nil {
		zargs.Fatal(err)
	}
	defer q.Undeclare()
	fmt.Println("Press CTRL-C to quit...")
	zargs.WaitForCtrlC()
}
