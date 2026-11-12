// Package zargs holds what the examples share: the command-line options
// of zenoh's examples (mode, connect and listen endpoints, multicast
// scouting) turned into a zenoh.Config, and small printing helpers.
package zargs

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/shabunin/tiny-zenoh-go/protocol"
	"github.com/shabunin/tiny-zenoh-go/shm"
	"github.com/shabunin/tiny-zenoh-go/zenoh"
)

var shmOptions shm.Options

type list []string

func (l *list) String() string     { return strings.Join(*l, ",") }
func (l *list) Set(v string) error { *l = append(*l, v); return nil }

// Common are the options every example takes.
type Common struct {
	mode        string
	connect     list
	listen      list
	noMulticast bool
	shm         bool
}

// Flags registers the common options on the command line; call flag.Parse
// afterwards, then Config.
func Flags() *Common {
	c := &Common{}
	for _, n := range []string{"m", "mode"} {
		flag.StringVar(&c.mode, n, "peer", "the session mode: peer, client or router")
	}
	for _, n := range []string{"e", "connect"} {
		flag.Var(&c.connect, n, "an endpoint to connect to, e.g. tcp/127.0.0.1:7447 (repeatable)")
	}
	for _, n := range []string{"l", "listen"} {
		flag.Var(&c.listen, n, "an endpoint to listen on, e.g. tcp/0.0.0.0:7448 (repeatable)")
	}
	flag.BoolVar(&c.noMulticast, "no-multicast-scouting", false, "disable multicast scouting")
	return c
}

// ShmFlag adds --enable-shm (shared memory with same-host nodes; Linux).
func (c *Common) ShmFlag() {
	flag.BoolVar(&c.shm, "enable-shm", false, "enable shared-memory transport with nodes on this host (Linux)")
}

// EnableShm turns shared memory on regardless of the flag.
func (c *Common) EnableShm() { c.shm = true }

// Config is the session configuration the options describe.
func (c *Common) Config() zenoh.Config {
	var cfg zenoh.Config
	switch c.mode {
	case "peer":
		cfg.Mode = zenoh.ModePeer
	case "client":
		cfg.Mode = zenoh.ModeClient
	case "router":
		cfg.Mode = zenoh.ModeRouter
	default:
		Fatal(fmt.Errorf("unknown mode %q (peer, client or router)", c.mode))
	}
	cfg.Connect = c.connect
	if len(c.listen) > 0 {
		cfg.Listen = c.listen
	}
	cfg.Scouting.Disable = c.noMulticast
	if c.shm {
		cfg.SharedMemory = &shmOptions
	}
	return cfg
}

// Open opens a session with the options' configuration.
func (c *Common) Open() *zenoh.Session {
	fmt.Println("Opening session...")
	s, err := zenoh.Open(c.Config())
	if err != nil {
		Fatal(err)
	}
	return s
}

// Fatal reports err and exits.
func Fatal(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}

// WaitForCtrlC blocks until the process is interrupted.
func WaitForCtrlC() {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	<-ch
}

// Payload renders a payload for printing: as text when it is printable.
func Payload(b []byte) string {
	for _, r := range string(b) {
		if r == 0xfffd || (r < 0x20 && r != '\n' && r != '\t' && r != '\r') {
			return fmt.Sprintf("<%d bytes>", len(b))
		}
	}
	return string(b)
}

// Kind renders a sample kind as zenoh's examples do.
func Kind(k zenoh.SampleKind) string {
	if k == zenoh.SampleKindDelete {
		return "DELETE"
	}
	return "PUT"
}

// Target parses a query target: BEST_MATCHING, ALL or ALL_COMPLETE.
func Target(s string) zenoh.QueryTarget {
	switch strings.ToUpper(s) {
	case "", "BEST_MATCHING":
		return protocol.TargetBestMatching
	case "ALL":
		return protocol.TargetAll
	case "ALL_COMPLETE":
		return protocol.TargetAllComplete
	}
	Fatal(fmt.Errorf("unknown query target %q (BEST_MATCHING, ALL or ALL_COMPLETE)", s))
	return 0
}

// PrintReply prints a query reply as zenoh's examples do.
func PrintReply(r zenoh.Reply) {
	if r.Err != nil {
		fmt.Printf(">> Received (ERROR: '%s')\n", Payload(r.Err.Payload))
		return
	}
	if r.Sample != nil {
		fmt.Printf(">> Received ('%s': '%s')\n", r.Sample.KeyExpr, Payload(r.Sample.Payload))
	}
}
