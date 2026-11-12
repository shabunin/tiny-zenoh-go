// Command z_scout looks for zenoh routers and peers on the local network
// (multicast scouting) for a second and prints what answers.
//
//	go run ./examples/z_scout
package main

import (
	"fmt"
	"time"

	"github.com/shabunin/tiny-zenoh-go/examples/internal/zargs"
	"github.com/shabunin/tiny-zenoh-go/zenoh"
)

func main() {
	fmt.Println("Scouting...")
	sc, err := zenoh.Scout(zenoh.MatchRouter|zenoh.MatchPeer, zenoh.ScoutingConfig{}, func(h zenoh.Hello) {
		fmt.Printf("Hello { zid: %s, whatami: %s, locators: %v }\n", h.ZID, h.WhatAmI, h.Locators)
	})
	if err != nil {
		zargs.Fatal(err)
	}
	time.Sleep(time.Second)
	sc.Stop()
}
