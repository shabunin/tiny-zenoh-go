// Command sim runs the board-test agent on a PC, to check the harness and
// the run itself before flashing a device:
//
//	sim tcp/127.0.0.1:7447 board/sim/plain [compression|lowlatency|user:password]
package main

import (
	"os"
	"strings"

	"github.com/shabunin/gozenoh/examples/boardtest/agent"
	"github.com/shabunin/gozenoh/transport"
	"github.com/shabunin/gozenoh/zenoh"
)

func main() {
	if len(os.Args) < 3 {
		println("usage: sim <router-locator> <prefix> [compression|lowlatency|user:password]...")
		os.Exit(2)
	}
	cfg := zenoh.Config{Connect: []string{os.Args[1]}}
	for _, opt := range os.Args[3:] {
		switch {
		case opt == "compression":
			cfg.Compression = true
		case opt == "lowlatency":
			cfg.LowLatency = true
		case strings.Contains(opt, ":"):
			u, p, _ := strings.Cut(opt, ":")
			cfg.Auth = &transport.Auth{User: u, Password: p}
		}
	}
	s, err := zenoh.Open(cfg)
	if err != nil {
		println(err.Error())
		os.Exit(1)
	}
	defer s.Close()
	n, err := agent.Run(s, os.Args[2], func(m string) { println(m) })
	if err != nil {
		println("failed after", n, "steps:", err.Error())
		os.Exit(1)
	}
}
