// Command z_storage is a minimal storage: it keeps the latest value of
// every key it hears on a key expression and answers queries with them.
// (For a full storage manager, run tzd with plugins/storage_manager.)
//
//	go run ./examples/z_storage -k 'demo/example/**'
package main

import (
	"flag"
	"fmt"
	"sync"

	"github.com/shabunin/tiny-zenoh-go/examples/internal/zargs"
	"github.com/shabunin/tiny-zenoh-go/keyexpr"
	"github.com/shabunin/tiny-zenoh-go/zenoh"
)

func main() {
	common := zargs.Flags()
	key := flag.String("k", "demo/example/**", "the key expression to store")
	complete := flag.Bool("complete", false, "declare the storage complete")
	flag.Parse()

	s := common.Open()
	defer s.Close()

	var mu sync.Mutex
	stored := map[string]zenoh.Sample{}

	fmt.Printf("Declaring Subscriber on '%s'...\n", *key)
	sub, err := s.DeclareSubscriber(*key, func(smp zenoh.Sample) {
		fmt.Printf(">> [Subscriber] Received %s ('%s': '%s')\n", zargs.Kind(smp.Kind), smp.KeyExpr, zargs.Payload(smp.Payload))
		mu.Lock()
		defer mu.Unlock()
		if smp.Kind == zenoh.SampleKindDelete {
			delete(stored, smp.KeyExpr)
		} else {
			stored[smp.KeyExpr] = smp
		}
	})
	if err != nil {
		zargs.Fatal(err)
	}
	defer sub.Undeclare()

	fmt.Printf("Declaring Queryable on '%s'...\n", *key)
	q, err := s.DeclareQueryable(*key, func(q *zenoh.Query) {
		fmt.Printf(">> [Queryable ] Received Query '%s'\n", q.Selector())
		mu.Lock()
		var hits []zenoh.Sample
		for k, smp := range stored {
			if keyexpr.Intersect(q.KeyExpr(), k) {
				hits = append(hits, smp)
			}
		}
		mu.Unlock()
		for _, smp := range hits {
			opts := []zenoh.PutOption{zenoh.WithEncoding(smp.Encoding)}
			if smp.Timestamp != nil {
				opts = append(opts, zenoh.WithTimestamp(*smp.Timestamp))
			}
			_ = q.Reply(smp.KeyExpr, smp.Payload, opts...)
		}
	}, zenoh.WithComplete(*complete))
	if err != nil {
		zargs.Fatal(err)
	}
	defer q.Undeclare()
	fmt.Println("Press CTRL-C to quit...")
	zargs.WaitForCtrlC()
}
