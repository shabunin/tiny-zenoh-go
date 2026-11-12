// Command z_liveliness declares a liveliness token: others see it alive
// until the program exits (or the connection is lost).
//
//	go run ./examples/z_liveliness -k group1/zenoh-go
package main

import (
	"flag"
	"fmt"

	"github.com/shabunin/tiny-zenoh-go/examples/internal/zargs"
)

func main() {
	common := zargs.Flags()
	key := flag.String("k", "group1/zenoh-go", "the key expression of the token")
	flag.Parse()

	s := common.Open()
	defer s.Close()

	fmt.Printf("Declaring LivelinessToken on '%s'...\n", *key)
	tok, err := s.Liveliness().DeclareToken(*key)
	if err != nil {
		zargs.Fatal(err)
	}
	fmt.Println("Press CTRL-C to undeclare LivelinessToken and quit...")
	zargs.WaitForCtrlC()
	fmt.Println("Undeclaring LivelinessToken...")
	_ = tok.Undeclare()
}
