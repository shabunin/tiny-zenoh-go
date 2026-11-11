// Command zprobe is a low-level debugging tool: it opens a TCP connection to a
// Zenoh router, sends an INIT SYN, and hex-dumps + decodes whatever comes back.
//
//	go run ./examples/zprobe [host:port]
package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/shabunin/gozenoh/internal/codec"
	"github.com/shabunin/gozenoh/protocol"
)

func main() {
	addr := "127.0.0.1:7447"
	if len(os.Args) > 1 {
		addr = os.Args[1]
	}
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		fmt.Println("dial:", err)
		os.Exit(1)
	}
	defer conn.Close()

	var idb [16]byte
	_, _ = rand.Read(idb[:])
	zid, _ := protocol.NewZID(idb[:])

	syn := &protocol.Init{
		Version:    protocol.ProtocolVersion,
		WhatAmI:    protocol.Client,
		ZID:        zid,
		HasParams:  true,
		Resolution: protocol.DefaultResolution,
		BatchSize:  65535,
	}
	w := codec.NewWriter()
	syn.Encode(w)
	send(conn, w.Bytes())

	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		msg, err := recv(conn)
		if err != nil {
			fmt.Println("recv:", err)
			return
		}
		fmt.Printf("<- %d bytes: %s\n", len(msg), hex.EncodeToString(msg))
		// A batch may hold several transport messages back to back.
		r := codec.NewReader(msg)
		for r.Remaining() > 0 {
			m, err := protocol.DecodeTransport(r)
			if err != nil {
				fmt.Println("   decode error:", err)
				break
			}
			fmt.Printf("   decoded %T: %+v\n", m, m)
			if ack, ok := m.(*protocol.Init); ok && ack.IsAck {
				fmt.Printf("   peer zid=%s whatami=%s\n", ack.ZID, ack.WhatAmI)
				var isn uint64
				if s := os.Getenv("ZISN"); s != "" {
					isn, _ = strconv.ParseUint(s, 0, 64)
				}
				open := &protocol.Open{Lease: 10000, InitialSN: isn, Cookie: ack.Cookie}
				ow := codec.NewWriter()
				open.Encode(ow)
				send(conn, ow.Bytes())
			}
		}
	}
}

func send(c net.Conn, b []byte) {
	fmt.Printf("-> %d bytes: %s\n", len(b), hex.EncodeToString(b))
	hdr := []byte{byte(len(b)), byte(len(b) >> 8)}
	_, _ = c.Write(append(hdr, b...))
}

func recv(c net.Conn) ([]byte, error) {
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	var l [2]byte
	if _, err := io.ReadFull(c, l[:]); err != nil {
		return nil, err
	}
	n := int(l[0]) | int(l[1])<<8
	buf := make([]byte, n)
	_, err := io.ReadFull(c, buf)
	return buf, err
}
