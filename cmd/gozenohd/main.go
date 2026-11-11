// Command gozenohd is a pure-Go Zenoh router, a stand-in for zenohd.
//
//	gozenohd                                  # listen on tcp/0.0.0.0:7447
//	gozenohd -l tcp/0.0.0.0:7447 -l tcp/[::]:7448 -l udp/0.0.0.0:7447
//	gozenohd --no-multicast-scouting
//	gozenohd -l quic/0.0.0.0:7447 --tls-cert srv.pem --tls-key srv.key
//	gozenohd -l tls/0.0.0.0:7447 --tls-cert srv.pem --tls-key srv.key [--tls-ca ca.pem --tls-mtls]
//	gozenohd -e tcp/10.0.0.5:7447            # join another router (gozenohd or zenohd)
//
// Clients and peers attach to it; it routes publications, queries and
// liveliness between them, answers multicast scouting so they can find it,
// and serves its admin space at @/<zid>/router.
package main

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/shabunin/gozenoh/link"
	_ "github.com/shabunin/gozenoh/link/quic" // quic/ locators (not in TinyGo builds)
	"github.com/shabunin/gozenoh/protocol"
	"github.com/shabunin/gozenoh/shm"
	"github.com/shabunin/gozenoh/transport"
	"github.com/shabunin/gozenoh/zenoh"
)

type list []string

func (l *list) String() string     { return strings.Join(*l, ",") }
func (l *list) Set(v string) error { *l = append(*l, v); return nil }

func main() {
	var listen, connect list
	flag.Var(&listen, "l", "locator to listen on (repeatable; default tcp/0.0.0.0:7447)")
	flag.Var(&connect, "e", "locator to connect to (repeatable)")
	noScout := flag.Bool("no-multicast-scouting", false, "don't answer multicast SCOUTs")
	scoutAddr := flag.String("scouting-address", "224.0.0.224:7446", "multicast scouting group")
	id := flag.String("i", "", "router ZID in hex (default random)")
	verbose := flag.Bool("v", false, "log connected sessions every 10s")
	shmOn := flag.Bool("shm", false, "enable shared memory with same-host nodes (Linux)")
	compress := flag.Bool("compression", false, "enable LZ4 batch compression with nodes that enable it")
	maxLinks := flag.Int("max-links", 1, "links per peer transport (>1 enables multilink)")
	var tlsCfg link.TLSConfig
	flag.StringVar(&tlsCfg.ListenCertificate, "tls-cert", "", "PEM certificate chain for tls/ listeners")
	flag.StringVar(&tlsCfg.ListenPrivateKey, "tls-key", "", "PEM private key for tls/ listeners")
	flag.StringVar(&tlsCfg.RootCACertificate, "tls-ca", "", "PEM CA to verify tls/ peers (default: system roots)")
	flag.StringVar(&tlsCfg.ConnectCertificate, "tls-client-cert", "", "PEM client certificate for -e tls/ (mutual TLS)")
	flag.StringVar(&tlsCfg.ConnectPrivateKey, "tls-client-key", "", "PEM client key for -e tls/ (mutual TLS)")
	flag.BoolVar(&tlsCfg.EnableMTLS, "tls-mtls", false, "require client certificates signed by --tls-ca")
	usrpwdDict := flag.String("usrpwd-dict", "", "require user/password authentication: file of user:password lines")
	user := flag.String("user", "", "user name to present to -e nodes that require authentication")
	password := flag.String("password", "", "password to present with --user")
	authKey := flag.String("auth-key", "", "PKCS#1 PEM RSA private key: enables pubkey authentication")
	var knownKeys list
	flag.Var(&knownKeys, "auth-known-key", "PKCS#1 PEM RSA public key accepted from nodes (repeatable)")
	lowLatency := flag.Bool("lowlatency", false, "low-latency transport (no batching, no QoS) with nodes that enable it")
	aclFile := flag.String("acl", "", "access control: JSON file with zenoh's access_control section")
	flag.Parse()

	cfg := zenoh.Config{
		Mode:        zenoh.ModeRouter,
		Listen:      listen,
		Connect:     connect,
		Scouting:    zenoh.ScoutingConfig{Disable: *noScout, Address: *scoutAddr},
		TLS:         &tlsCfg,
		Compression: *compress,
		MaxLinks:    *maxLinks,
		LowLatency:  *lowLatency,
	}
	auth, err := authConfig(*usrpwdDict, *user, *password, *authKey, knownKeys)
	if err != nil {
		fmt.Fprintln(os.Stderr, "gozenohd:", err)
		os.Exit(2)
	}
	cfg.Auth = auth
	if *aclFile != "" {
		if cfg.ACL, err = loadACL(*aclFile); err != nil {
			fmt.Fprintln(os.Stderr, "gozenohd:", err)
			os.Exit(2)
		}
	}
	if *shmOn {
		cfg.SharedMemory = &shm.Options{}
	}
	if *id != "" {
		h := *id
		if len(h)%2 == 1 {
			h = "0" + h
		}
		b, err := hex.DecodeString(h)
		if err == nil {
			// The ZID's text form is the little-endian u128 printed big-endian.
			for i, j := 0, len(b)-1; i < j; i, j = i+1, j-1 {
				b[i], b[j] = b[j], b[i]
			}
			cfg.ZID, err = protocol.NewZID(b)
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, "gozenohd: bad -i:", err)
			os.Exit(2)
		}
	}
	r, err := zenoh.Open(cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, "gozenohd:", err)
		os.Exit(1)
	}
	fmt.Printf("gozenohd %s listening on %s\n", r.ZID(), strings.Join(r.Locators(), " "))

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	tick := time.NewTicker(10 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-sig:
			_ = r.Close()
			return
		case <-tick.C:
			if *verbose {
				fmt.Printf("clients=%d peers=%d routers=%d\n",
					len(r.ClientsZID()), len(r.PeersZID()), len(r.RoutersZID()))
			}
		}
	}
}

// authConfig builds the handshake authentication settings from the flags
// (nil when none is set).
func authConfig(dict, user, password, keyFile string, known []string) (*transport.Auth, error) {
	a := &transport.Auth{User: user, Password: password}
	if (user == "") != (password == "") {
		return nil, fmt.Errorf("--user and --password go together")
	}
	if dict != "" {
		b, err := os.ReadFile(dict)
		if err != nil {
			return nil, err
		}
		if a.Users, err = transport.ParseUsrPwdDict(b); err != nil {
			return nil, err
		}
	}
	if keyFile != "" {
		b, err := os.ReadFile(keyFile)
		if err != nil {
			return nil, err
		}
		if a.Key, err = transport.ParseRSAKeyPEM(b); err != nil {
			return nil, err
		}
	}
	for _, f := range known {
		b, err := os.ReadFile(f)
		if err != nil {
			return nil, err
		}
		k, err := transport.ParseRSAPublicKeyPEM(b)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", f, err)
		}
		a.KnownKeys = append(a.KnownKeys, k)
	}
	if a.User == "" && a.Users == nil && a.Key == nil {
		return nil, nil
	}
	return a, nil
}

// loadACL reads a JSON copy of zenoh's access_control config section (nil
// when it says enabled: false).
func loadACL(file string) (*zenoh.ACLConfig, error) {
	b, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}
	var c struct {
		Enabled *bool `json:"enabled"`
		zenoh.ACLConfig
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&c); err != nil {
		return nil, fmt.Errorf("%s: %w", file, err)
	}
	if c.Enabled != nil && !*c.Enabled {
		return nil, nil
	}
	return &c.ACLConfig, nil
}
