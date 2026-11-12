// Command z_advanced_pub publishes with a cache for late joiners, sequence
// numbers with heartbeats (so subscribers recover lost samples) and a
// liveliness token (so subscribers detect it). Works with zenoh's
// zenoh-ext AdvancedSubscriber, and z_advanced_sub.
//
//	go run ./examples/z_advanced_pub -k demo/example/zenoh-go-pub --history 10
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
	key := flag.String("k", "demo/example/zenoh-go-pub", "the key expression to publish onto")
	payload := flag.String("p", "Pub from Go!", "the payload to publish")
	history := flag.Int("history", 1, "the samples kept for late joiners")
	flag.Parse()

	s := common.Open()
	defer s.Close()

	fmt.Printf("Declaring AdvancedPublisher on '%s'...\n", *key)
	pub, err := s.DeclareAdvancedPublisher(*key, zenoh.AdvancedPublisherConfig{
		Cache:              &zenoh.CacheConfig{MaxSamples: *history},
		MissDetection:      &zenoh.MissDetectionConfig{Heartbeat: 500 * time.Millisecond},
		PublisherDetection: true,
	})
	if err != nil {
		zargs.Fatal(err)
	}
	defer pub.Undeclare()

	fmt.Println("Press CTRL-C to quit...")
	go func() {
		for idx := 0; ; idx++ {
			time.Sleep(time.Second)
			msg := fmt.Sprintf("[%4d] %s", idx, *payload)
			fmt.Printf("Putting Data ('%s': '%s')...\n", *key, msg)
			if err := pub.Put([]byte(msg)); err != nil {
				fmt.Println("put:", err)
			}
		}
	}()
	zargs.WaitForCtrlC()
}
