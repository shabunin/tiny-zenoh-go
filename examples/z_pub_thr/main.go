// Command z_pub_thr publishes on test/thr as fast as it can, for z_sub_thr
// to measure the throughput.
//
//	go run ./examples/z_sub_thr &
//	go run ./examples/z_pub_thr 8
package main

import (
	"flag"
	"fmt"
	"strconv"

	"github.com/shabunin/tiny-zenoh-go/examples/internal/zargs"
	"github.com/shabunin/tiny-zenoh-go/zenoh"
)

func main() {
	common := zargs.Flags()
	common.ShmFlag()
	prio := flag.Int("p", int(zenoh.PriorityData), "the priority (1 real time .. 7 background)")
	express := flag.Bool("express", false, "send every message at once instead of batching")
	number := flag.Int("n", 0, "stop after this many messages (0: never)")
	flag.Usage = func() {
		fmt.Fprintln(flag.CommandLine.Output(), "usage: z_pub_thr [options] PAYLOAD_SIZE")
		flag.PrintDefaults()
	}
	flag.Parse()
	size := 8
	if flag.NArg() > 0 {
		n, err := strconv.Atoi(flag.Arg(0))
		if err != nil || n < 0 {
			zargs.Fatal(fmt.Errorf("bad payload size %q", flag.Arg(0)))
		}
		size = n
	}

	s := common.Open()
	defer s.Close()

	pub, err := s.DeclarePublisher("test/thr",
		zenoh.WithCongestionControl(zenoh.CongestionBlock),
		zenoh.WithPriority(zenoh.Priority(*prio)),
		zenoh.WithExpress(*express))
	if err != nil {
		zargs.Fatal(err)
	}
	defer pub.Undeclare()

	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 10)
	}
	fmt.Println("Press CTRL-C to quit...")
	for i := 0; *number == 0 || i < *number; i++ {
		if err := pub.Put(data); err != nil {
			zargs.Fatal(err)
		}
	}
}
