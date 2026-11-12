// Command z_info prints the session's id and the routers and peers it is
// connected to.
//
//	go run ./examples/z_info -e tcp/127.0.0.1:7447
package main

import (
	"flag"
	"fmt"
	"time"

	"github.com/shabunin/tiny-zenoh-go/examples/internal/zargs"
)

func main() {
	common := zargs.Flags()
	common.ShmFlag()
	flag.Parse()

	s := common.Open()
	defer s.Close()
	time.Sleep(500 * time.Millisecond) // let scouting find the others

	fmt.Println("zid:", s.ZID())
	fmt.Println("routers zid:", s.RoutersZID())
	fmt.Println("peers zid:", s.PeersZID())
	for _, t := range s.Transports() {
		fmt.Printf("transport: %s %s over %s (links %d, qos %v, shm %v)\n", t.ZID, t.WhatAmI, t.Proto, t.Links, t.QoS, t.SharedMem)
	}
}
