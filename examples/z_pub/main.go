// Command z_pub declares a publisher and publishes a counter every second.
//
//	go run ./examples/z_pub -k demo/example/hello -p 'Hello!' --add-matching-listener
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
	common.ShmFlag()
	key := flag.String("k", "demo/example/zenoh-go-pub", "the key expression to publish on")
	payload := flag.String("p", "Pub from Go!", "the payload")
	attach := flag.String("a", "", "an attachment to add to each publication")
	matching := flag.Bool("add-matching-listener", false, "report whether subscribers match the publisher")
	flag.Parse()

	s := common.Open()
	defer s.Close()

	fmt.Printf("Declaring Publisher on '%s'...\n", *key)
	pub, err := s.DeclarePublisher(*key)
	if err != nil {
		zargs.Fatal(err)
	}
	defer pub.Undeclare()
	if *matching {
		_, err := pub.DeclareMatchingListener(func(m zenoh.MatchingStatus) {
			if m.Matching {
				fmt.Println("Publisher has matching subscribers.")
			} else {
				fmt.Println("Publisher has NO MORE matching subscribers.")
			}
		})
		if err != nil {
			zargs.Fatal(err)
		}
	}
	fmt.Println("Press CTRL-C to quit...")
	for idx := 0; ; idx++ {
		time.Sleep(time.Second)
		buf := fmt.Sprintf("[%4d] %s", idx, *payload)
		fmt.Printf("Putting Data ('%s': '%s')...\n", *key, buf)
		var opts []zenoh.PutOption
		if *attach != "" {
			opts = append(opts, zenoh.WithAttachment([]byte(*attach)))
		}
		if err := pub.Put([]byte(buf), opts...); err != nil {
			zargs.Fatal(err)
		}
	}
}
