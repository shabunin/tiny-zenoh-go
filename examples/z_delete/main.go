// Command z_delete deletes a key (storages forget its value).
//
//	go run ./examples/z_delete -k demo/example/hello
package main

import (
	"flag"
	"fmt"

	"github.com/shabunin/tiny-zenoh-go/examples/internal/zargs"
)

func main() {
	common := zargs.Flags()
	key := flag.String("k", "demo/example/zenoh-go-put", "the key expression to delete")
	flag.Parse()

	s := common.Open()
	defer s.Close()

	fmt.Printf("Deleting resources matching '%s'...\n", *key)
	if err := s.Delete(*key); err != nil {
		zargs.Fatal(err)
	}
}
