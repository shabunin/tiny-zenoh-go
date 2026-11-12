// Command z_ping measures round-trip time with z_pong: it publishes on
// test/ping, z_pong echoes on test/pong.
//
//	go run ./examples/z_pong &
//	go run ./examples/z_ping 64
package main

import (
	"flag"
	"fmt"
	"strconv"
	"time"

	"github.com/shabunin/tiny-zenoh-go/examples/internal/zargs"
	"github.com/shabunin/tiny-zenoh-go/zenoh"
)

func main() {
	common := zargs.Flags()
	common.ShmFlag()
	warmup := flag.Float64("w", 1, "the warmup time in seconds")
	samples := flag.Int("n", 100, "the number of round trips to measure")
	noExpress := flag.Bool("no-express", false, "let the messages be batched")
	flag.Usage = func() {
		fmt.Fprintln(flag.CommandLine.Output(), "usage: z_ping [options] PAYLOAD_SIZE")
		flag.PrintDefaults()
	}
	flag.Parse()
	size := 64
	if flag.NArg() > 0 {
		n, err := strconv.Atoi(flag.Arg(0))
		if err != nil || n < 0 {
			zargs.Fatal(fmt.Errorf("bad payload size %q", flag.Arg(0)))
		}
		size = n
	}

	s := common.Open()
	defer s.Close()

	pong := make(chan struct{}, 1)
	sub, err := s.DeclareSubscriber("test/pong", func(zenoh.Sample) { pong <- struct{}{} })
	if err != nil {
		zargs.Fatal(err)
	}
	defer sub.Undeclare()
	pub, err := s.DeclarePublisher("test/ping",
		zenoh.WithCongestionControl(zenoh.CongestionBlock), zenoh.WithExpress(!*noExpress))
	if err != nil {
		zargs.Fatal(err)
	}
	defer pub.Undeclare()

	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 10)
	}
	ping := func() bool {
		if err := pub.Put(data); err != nil {
			zargs.Fatal(err)
		}
		select {
		case <-pong:
			return true
		case <-time.After(5 * time.Second):
			return false
		}
	}

	// Wait for z_pong's subscriber to be known: a ping published before
	// that goes nowhere.
	for wait := time.Now().Add(10 * time.Second); ; {
		if m, err := pub.MatchingStatus(); err == nil && m.Matching {
			break
		}
		if time.Now().After(wait) {
			zargs.Fatal(fmt.Errorf("nobody subscribes to test/ping: is z_pong running?"))
		}
		time.Sleep(10 * time.Millisecond)
	}

	fmt.Printf("Warming up for %vs...\n", *warmup)
	end := time.Now().Add(time.Duration(*warmup * float64(time.Second)))
	for time.Now().Before(end) {
		if !ping() {
			zargs.Fatal(fmt.Errorf("no pong in 5s: is z_pong running?"))
		}
	}
	for i := 0; i < *samples; i++ {
		start := time.Now()
		if !ping() {
			zargs.Fatal(fmt.Errorf("no pong in 5s"))
		}
		rtt := time.Since(start).Microseconds()
		fmt.Printf("%d bytes: seq=%d rtt=%dµs lat=%dµs\n", size, i, rtt, rtt/2)
	}
}
