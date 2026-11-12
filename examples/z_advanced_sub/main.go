// Command z_advanced_sub subscribes to advanced publishers: it gets their
// cached history (also from publishers that appear later), recovers lost
// samples and reports the ones lost for good.
//
//	go run ./examples/z_advanced_sub -k 'demo/example/**'
package main

import (
	"flag"
	"fmt"

	"github.com/shabunin/tiny-zenoh-go/examples/internal/zargs"
	"github.com/shabunin/tiny-zenoh-go/zenoh"
)

func main() {
	common := zargs.Flags()
	key := flag.String("k", "demo/example/**", "the key expression to subscribe to")
	flag.Parse()

	s := common.Open()
	defer s.Close()

	fmt.Printf("Declaring AdvancedSubscriber on '%s'...\n", *key)
	sub, err := s.DeclareAdvancedSubscriber(*key, func(smp zenoh.Sample) {
		fmt.Printf(">> [Subscriber] Received %s ('%s': '%s')\n", zargs.Kind(smp.Kind), smp.KeyExpr, zargs.Payload(smp.Payload))
	}, zenoh.AdvancedSubscriberConfig{
		History:             &zenoh.HistoryConfig{DetectLatePublishers: true},
		Recovery:            &zenoh.RecoveryConfig{Heartbeat: true},
		SubscriberDetection: true,
	})
	if err != nil {
		zargs.Fatal(err)
	}
	defer sub.Undeclare()
	sub.DeclareMissListener(func(m zenoh.Miss) {
		fmt.Printf(">> [Subscriber] Missed %d samples from %v !!!\n", m.Count, m.Source)
	})
	fmt.Println("Press CTRL-C to quit...")
	zargs.WaitForCtrlC()
}
