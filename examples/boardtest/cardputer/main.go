//go:build tinygo

// Command cardputer runs the board tests (examples/boardtest/agent) on an
// M5Stack Cardputer (ESP32-S3) over Wi-Fi, once per profile:
//
//	plain     tcp to the host's port 7447 (zenohd, the Rust router)
//	auth-lz4  tcp to 7460: a gozenohd with --compression --usrpwd-dict
//	lowlat    tcp to 7461: a gozenohd with --lowlatency
//
// Wi-Fi settings come from tiny-knx-go's settings record (TKC1) in the last
// flash block, as its Cardputer example stores them. The router host is set
// at build time; with several, the one on the device's /24 is used:
//
//	tinygo flash -target=m5stamp-s3a -port /dev/ttyACM0 \
//	    -ldflags '-X main.hosts=192.168.1.10,192.168.2.10' .
//
// Progress goes to the USB console.
package main

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"machine"
	"net"
	"strings"
	"time"

	"tinygo.org/x/drivers/netdev"
	nl "tinygo.org/x/drivers/netlink"
	espnet "tinygo.org/x/espradio/netlink"

	"github.com/shabunin/gozenoh/examples/boardtest/agent"
	"github.com/shabunin/gozenoh/link"
	"github.com/shabunin/gozenoh/transport"
	"github.com/shabunin/gozenoh/zenoh"
)

var hosts = "192.168.1.10" // comma-separated, set with -ldflags -X main.hosts=

type profile struct {
	name string
	port string
	conf func(*zenoh.Config)
}

var profiles = []profile{
	{"plain", "7447", func(*zenoh.Config) {}},
	{"auth-lz4", "7460", func(c *zenoh.Config) {
		c.Compression = true
		c.Auth = &transport.Auth{User: "board", Password: "board-test-pw"}
	}},
	{"lowlat", "7461", func(c *zenoh.Config) { c.LowLatency = true }},
}

var wifi espnet.Esplink

// loadWiFi reads the SSID and passphrase from tiny-knx-go's TKC1 record.
func loadWiFi() (string, string, error) {
	b := make([]byte, 256)
	if _, err := machine.Flash.ReadAt(b, machine.Flash.Size()-machine.Flash.EraseBlockSize()); err != nil {
		return "", "", err
	}
	if string(b[:4]) != "TKC1" {
		return "", "", errors.New("no TKC1 settings")
	}
	p := b[4:]
	short := errors.New("short settings")
	str := func() string {
		if len(p) < 1 || len(p) < 1+int(p[0]) {
			p = nil
			return ""
		}
		v := string(p[1 : 1+p[0]])
		p = p[1+p[0]:]
		return v
	}
	ssid, pass, _ := str(), str(), str()
	if len(p) < 1 {
		return "", "", short
	}
	p = p[1:]
	for range 2 {
		if len(p) < 1 || p[0] == 1 && len(p) < 17 {
			return "", "", short
		}
		if p[0] == 0 {
			p = p[1:]
		} else {
			p = p[17:]
		}
	}
	if len(p) < 12 {
		return "", "", short
	}
	p = p[8:]
	if n := len(b) - len(p); binary.LittleEndian.Uint32(p) != crc32.ChecksumIEEE(b[:n]) {
		return "", "", errors.New("settings CRC mismatch")
	}
	return ssid, pass, nil
}

func subnet(ip string) string { return ip[:strings.LastIndexByte(ip, '.')] }

func main() {
	time.Sleep(2 * time.Second)
	println("boardtest: starting")
	ssid, pass, err := loadWiFi()
	if err != nil {
		println("settings:", err.Error())
		select {}
	}
	netdev.UseNetdev(&wifi)
	for try := 0; ; try++ {
		println("wifi: connecting to", ssid)
		if err := wifi.NetConnect(&nl.ConnectParams{Ssid: ssid, Passphrase: pass}); err == nil {
			break
		} else {
			println("wifi:", err.Error())
		}
		if try > 0 {
			time.Sleep(3 * time.Second)
		}
	}
	addr, _ := wifi.Addr()
	ip := addr.String()
	println("wifi: ip", ip)
	list := strings.Split(hosts, ",")
	host := list[0]
	for _, h := range list {
		if subnet(h) == subnet(ip) {
			host = h
		}
	}

	results := make([]string, len(profiles))
	for i, pr := range profiles {
		prefix := "board/cardputer/" + pr.name
		for try := 0; try < 5; try++ {
			cfg := zenoh.Config{
				Connect:   []string{"tcp/" + host + ":" + pr.port},
				BatchSize: 4096,
				Dial: func(loc string, _ time.Duration) (transport.Link, error) {
					l, err := link.Parse(loc)
					if err != nil {
						return nil, err
					}
					return net.Dial("tcp", l.Address)
				},
			}
			pr.conf(&cfg)
			println("zenoh:", pr.name, "connecting to", cfg.Connect[0])
			s, err := zenoh.Open(cfg)
			if err != nil {
				println("zenoh:", err.Error())
				results[i] = "FAIL open: " + err.Error()
				time.Sleep(3 * time.Second)
				continue
			}
			n, err := agent.Run(s, prefix, func(m string) { println(pr.name+":", m) })
			_ = s.Close()
			if err == nil {
				results[i] = "PASS"
				break
			}
			results[i] = "FAIL after " + string(rune('0'+n)) + " steps: " + err.Error()
			if err != agent.ErrNoHarness {
				break
			}
		}
	}
	for {
		for i, pr := range profiles {
			println("result", pr.name, results[i])
		}
		time.Sleep(10 * time.Second)
	}
}
