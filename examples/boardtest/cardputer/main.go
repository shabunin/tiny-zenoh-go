//go:build tinygo

// Command cardputer runs the board tests (examples/boardtest/agent) on an
// M5Stack Cardputer (ESP32-S3) over Wi-Fi, once per profile:
//
//	plain     tcp to the host's port 7447 (zenohd, the Rust router)
//	auth-lz4  tcp to 7460: a tzd with --compression --usrpwd-dict
//	lowlat    tcp to 7461: a tzd with --lowlatency
//	tls-rust  tls to 7463: zenohd (rustls), the server certificate signed by tlsCA
//	mtls-go   tls to 7464: a tzd requiring a client certificate (tlsCert, tlsKey)
//
// The TLS profiles run when the CA (and, for mtls-go, the client
// certificate and key) are given as base64 PEM:
//
//	-ldflags '-X main.tlsCA=... -X main.tlsCert=... -X main.tlsKey=...'
//
// -X main.only=tls-rust,mtls-go picks profiles, and -X main.debugMem=1
// prints goroutines and heap after each session closes.
//
// Memory: the heap is ~300 KB, the Wi-Fi driver takes ~134 KB of it and a
// session peaks at 200–230 KB in use. Run the TLS profiles on their own boot:
// five sessions in a row can run out of memory. Two causes:
//
//   - lneto v0.4.0 (the TCP/IP stack under espradio) keeps closed
//     connections, with 5 KB of buffers each, reachable from its port table.
//     lneto-release-closed-conns.patch here fixes that. With it, all five
//     profiles have passed in one boot; without it, the fifth runs out.
//   - Fragmentation: every goroutine needs one contiguous 8 KB stack. After a
//     few sessions free memory (~100 KB) is split into pieces under 8 KB, and
//     starting a goroutine fails. This happens with or without the patch.
//
// To build against the patched lneto (v0.4.0):
//
//	cp -r $(go env GOMODCACHE)/github.com/soypat/lneto@v0.4.0 /tmp/lneto
//	chmod -R u+w /tmp/lneto && git -C /tmp/lneto apply "$PWD/lneto-release-closed-conns.patch"
//	go mod edit -replace github.com/soypat/lneto=/tmp/lneto
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
	"encoding/base64"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"machine"
	"net"
	"runtime"
	"strings"
	"time"

	"tinygo.org/x/drivers/netdev"
	nl "tinygo.org/x/drivers/netlink"
	espnet "tinygo.org/x/espradio/netlink"

	"github.com/shabunin/tiny-zenoh-go/examples/boardtest/agent"
	"github.com/shabunin/tiny-zenoh-go/link"
	"github.com/shabunin/tiny-zenoh-go/transport"
	"github.com/shabunin/tiny-zenoh-go/zenoh"
)

var hosts = "192.168.1.10" // comma-separated, set with -ldflags -X main.hosts=

// Set with -ldflags -X (see above).
var tlsCA, tlsCert, tlsKey, only, debugMem string

type profile struct {
	name string
	port string // proto/port
	conf func(*zenoh.Config)
}

var profiles = []profile{
	{"plain", "tcp/7447", func(*zenoh.Config) {}},
	{"auth-lz4", "tcp/7460", func(c *zenoh.Config) {
		c.Compression = true
		c.Auth = &transport.Auth{User: "board", Password: "board-test-pw"}
	}},
	{"lowlat", "tcp/7461", func(c *zenoh.Config) { c.LowLatency = true }},
	{"tls-rust", "tls/7463", func(*zenoh.Config) {}},
	{"mtls-go", "tls/7464", func(*zenoh.Config) {}},
}

func unb64(s string) []byte {
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		println("bad base64 build setting:", err.Error())
	}
	return b
}

// dial opens tcp/ and tls/ links (TLS 1.3 by package tls13), reporting the
// handshake's time and memory.
func dial(loc string, mtls bool) (transport.Link, error) {
	l, err := link.Parse(loc)
	if err != nil {
		return nil, err
	}
	c, err := net.Dial("tcp", l.Address)
	if err != nil || l.Protocol != "tls" {
		return c, err
	}
	cfg := &link.TLSConfig{RootCACertificatePEM: unb64(tlsCA)}
	if mtls {
		cfg.ConnectCertificatePEM, cfg.ConnectPrivateKeyPEM = unb64(tlsCert), unb64(tlsKey)
	}
	var m0, m1 runtime.MemStats
	runtime.ReadMemStats(&m0)
	start := time.Now()
	tc, err := link.TLS13Client(c, loc, cfg)
	if err != nil {
		c.Close()
		return nil, err
	}
	runtime.ReadMemStats(&m1)
	println("tls: handshake", time.Since(start).Milliseconds(), "ms, allocated", m1.TotalAlloc-m0.TotalAlloc,
		"bytes, heap in use", m1.HeapInuse)
	return tc, nil
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
		if only != "" && !strings.Contains(","+only+",", ","+pr.name+",") ||
			strings.HasPrefix(pr.port, "tls/") && tlsCA == "" {
			results[i] = "skipped"
			continue
		}
		prefix := "board/cardputer/" + pr.name
		proto, port, _ := strings.Cut(pr.port, "/")
		mtls := pr.name == "mtls-go"
		for try := 0; try < 5; try++ {
			cfg := zenoh.Config{
				Connect:   []string{proto + "/" + host + ":" + port},
				BatchSize: 4096,
				Dial: func(loc string, _ time.Duration) (transport.Link, error) {
					return dial(loc, mtls)
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
			if debugMem != "" {
				for k := 0; k < 4; k++ {
					time.Sleep(time.Duration(1+15*k) * time.Second)
					runtime.GC()
					var ms runtime.MemStats
					runtime.ReadMemStats(&ms)
					println("closed:", pr.name, "goroutines", runtime.NumGoroutine(), "heap in use", ms.HeapInuse)
				}
			}
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
