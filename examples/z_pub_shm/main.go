// Command z_pub_shm publishes from shared memory: same-host nodes that
// enabled shared memory (e.g. z_sub --enable-shm, or zenohd with
// transport/shared_memory/enabled) receive a reference, not a copy. Linux.
//
//	go run ./examples/z_pub_shm -k demo/example/zenoh-go-pub-shm
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
	key := flag.String("k", "demo/example/zenoh-go-pub-shm", "the key expression to publish onto")
	payload := flag.String("p", "Pub from Go SHM!", "the payload to publish")
	flag.Parse()
	common.EnableShm()

	s := common.Open()
	defer s.Close()
	provider := s.ShmProvider()
	if provider == nil {
		zargs.Fatal(fmt.Errorf("shared memory is not available on this system"))
	}

	fmt.Printf("Declaring Publisher on '%s'...\n", *key)
	pub, err := s.DeclarePublisher(*key)
	if err != nil {
		zargs.Fatal(err)
	}
	defer pub.Undeclare()

	fmt.Println("Press CTRL-C to quit...")
	go func() {
		for idx := 0; ; idx++ {
			time.Sleep(time.Second)
			msg := fmt.Sprintf("[%4d] %s", idx, *payload)
			buf, err := provider.Alloc(len(msg))
			if err != nil {
				fmt.Println("alloc:", err)
				continue
			}
			copy(buf.Bytes(), msg)
			fmt.Printf("Putting Data ('%s': '%s')...\n", *key, msg)
			if err := pub.Put(nil, zenoh.WithShmBuf(buf)); err != nil {
				fmt.Println("put:", err)
			}
			buf.Close()
		}
	}()
	zargs.WaitForCtrlC()
}
