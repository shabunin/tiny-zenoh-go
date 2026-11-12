// Command z_put puts a value on a key, once.
//
//	go run ./examples/z_put -k demo/example/hello -p 'Hello!'
package main

import (
	"flag"
	"fmt"

	"github.com/shabunin/tiny-zenoh-go/examples/internal/zargs"
	"github.com/shabunin/tiny-zenoh-go/zenoh"
)

func main() {
	common := zargs.Flags()
	key := flag.String("k", "demo/example/zenoh-go-put", "the key expression to put on")
	payload := flag.String("p", "Put from Go!", "the payload")
	attach := flag.String("a", "", "an attachment to add")
	flag.Parse()

	s := common.Open()
	defer s.Close()

	fmt.Printf("Putting Data ('%s': '%s')...\n", *key, *payload)
	var opts []zenoh.PutOption
	if *attach != "" {
		opts = append(opts, zenoh.WithAttachment([]byte(*attach)))
	}
	if err := s.Put(*key, []byte(*payload), opts...); err != nil {
		zargs.Fatal(err)
	}
}
