// Command z_sub_thr counts the messages on test/thr and prints the rate
// every N of them (see z_pub_thr).
//
//	go run ./examples/z_sub_thr -n 100000 -s 10
package main

import (
	"flag"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/shabunin/tiny-zenoh-go/examples/internal/zargs"
	"github.com/shabunin/tiny-zenoh-go/zenoh"
)

func main() {
	common := zargs.Flags()
	common.ShmFlag()
	number := flag.Int("n", 100000, "the messages in a measurement")
	samples := flag.Int("s", 10, "the measurements to take before exiting")
	flag.Parse()

	s := common.Open()
	defer s.Close()

	var (
		mu      sync.Mutex
		count   int
		start   time.Time
		rounds  int
		elapsed time.Duration
		done    = make(chan struct{})
	)
	sub, err := s.DeclareSubscriber("test/thr", func(zenoh.Sample) {
		mu.Lock()
		defer mu.Unlock()
		if rounds >= *samples {
			return
		}
		if count == 0 {
			start = time.Now()
			count++
			return
		}
		count++
		if count < *number {
			return
		}
		d := time.Since(start)
		elapsed += d
		fmt.Printf("%.1f msg/s\n", float64(*number)/d.Seconds())
		count = 0
		rounds++
		if rounds == *samples {
			close(done)
		}
	})
	if err != nil {
		zargs.Fatal(err)
	}
	defer sub.Undeclare()
	fmt.Println("Press CTRL-C to quit...")
	go func() { zargs.WaitForCtrlC(); s.Close(); os.Exit(0) }()
	<-done
	fmt.Printf("average: %.1f msg/s\n", float64(*number**samples)/elapsed.Seconds())
}
