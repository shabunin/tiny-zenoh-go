// Command gozenohd is a pure-Go Zenoh router, a stand-in for zenohd.
//
//	gozenohd                                  # listen on tcp/0.0.0.0:7447
//	gozenohd -l tcp/0.0.0.0:7447 -l tcp/[::]:7448 -l udp/0.0.0.0:7447
//	gozenohd --no-multicast-scouting
//	gozenohd -l quic/0.0.0.0:7447 --tls-cert srv.pem --tls-key srv.key
//	gozenohd -l tls/0.0.0.0:7447 --tls-cert srv.pem --tls-key srv.key [--tls-ca ca.pem --tls-mtls]
//	gozenohd -e tcp/10.0.0.5:7447            # join another router (gozenohd or zenohd)
//	gozenohd -config zenoh.json              # downsampling, low_pass_filter, qos/network, timestamping, ...
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

	"github.com/shabunin/tiny-zenoh-go/link"
	_ "github.com/shabunin/tiny-zenoh-go/link/quic" // quic/ locators (not in TinyGo builds)
	"github.com/shabunin/tiny-zenoh-go/protocol"
	"github.com/shabunin/tiny-zenoh-go/shm"
	"github.com/shabunin/tiny-zenoh-go/transport"
	"github.com/shabunin/tiny-zenoh-go/zenoh"
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
	regionName := flag.String("region-name", "", "region name announced to the nodes we connect to")
	gatewayFile := flag.String("gateway", "", "south subregions: JSON file with zenoh's gateway section")
	configFile := flag.String("config", "", "JSON zenoh config file: its downsampling, low_pass_filter, qos/network, "+
		"timestamping, access_control and gateway sections")
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
	cfg.RegionName = *regionName
	if *gatewayFile != "" {
		if cfg.Gateway, err = loadGateway(*gatewayFile); err != nil {
			fmt.Fprintln(os.Stderr, "gozenohd:", err)
			os.Exit(2)
		}
	}
	if *configFile != "" {
		if err = loadConfig(*configFile, &cfg); err != nil {
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
	acl, err := decodeACL(b)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", file, err)
	}
	return acl, nil
}

func decodeACL(b []byte) (*zenoh.ACLConfig, error) {
	var c struct {
		Enabled *bool `json:"enabled"`
		zenoh.ACLConfig
	}
	if err := decodeStrict(b, &c); err != nil {
		return nil, err
	}
	if c.Enabled != nil && !*c.Enabled {
		return nil, nil
	}
	return &c.ACLConfig, nil
}

// loadGateway reads a JSON copy of zenoh's gateway config section:
// {"south": "auto"} or {"south": [{"filters": [{"modes": [...], "zids":
// [...], "interfaces": [...], "region_names": [...], "negated": bool}]}]}.
func loadGateway(file string) (zenoh.GatewayConfig, error) {
	b, err := os.ReadFile(file)
	if err != nil {
		return zenoh.GatewayConfig{}, err
	}
	gc, err := decodeGateway(b)
	if err != nil {
		return gc, fmt.Errorf("%s: %w", file, err)
	}
	return gc, nil
}

func decodeGateway(b []byte) (zenoh.GatewayConfig, error) {
	var gc zenoh.GatewayConfig
	var c struct {
		South json.RawMessage `json:"south"`
	}
	if err := decodeStrict(b, &c); err != nil {
		return gc, err
	}
	var preset string
	if len(c.South) == 0 || json.Unmarshal(c.South, &preset) == nil {
		if preset != "" && preset != "auto" {
			return gc, fmt.Errorf("unknown gateway preset %q", preset)
		}
		return gc, nil
	}
	var subs []struct {
		Filters *[]struct {
			Modes       []string `json:"modes"`
			ZIDs        []string `json:"zids"`
			Interfaces  []string `json:"interfaces"`
			RegionNames []string `json:"region_names"`
			Negated     bool     `json:"negated"`
		} `json:"filters"`
	}
	if err := decodeStrict(c.South, &subs); err != nil {
		return gc, fmt.Errorf("south: %w", err)
	}
	gc.South = []zenoh.Subregion{}
	for _, sr := range subs {
		var out zenoh.Subregion
		if sr.Filters != nil {
			out.Filters = []zenoh.GatewayFilter{}
			for _, f := range *sr.Filters {
				gf := zenoh.GatewayFilter{Interfaces: f.Interfaces, RegionNames: f.RegionNames, Negated: f.Negated}
				for _, m := range f.Modes {
					switch m {
					case "router":
						gf.Modes |= zenoh.MatchRouter
					case "peer":
						gf.Modes |= zenoh.MatchPeer
					case "client":
						gf.Modes |= zenoh.MatchClient
					default:
						return gc, fmt.Errorf("unknown mode %q", m)
					}
				}
				for _, z := range f.ZIDs {
					zid, err := protocol.ParseZID(z)
					if err != nil {
						return gc, fmt.Errorf("zid %q: %w", z, err)
					}
					gf.ZIDs = append(gf.ZIDs, zid)
				}
				out.Filters = append(out.Filters, gf)
			}
		}
		gc.South = append(gc.South, out)
	}
	return gc, nil
}

func decodeStrict(b []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}

// loadConfig reads a zenoh configuration file (plain JSON, not JSON5) and
// applies the sections gozenohd understands; it warns about the others.
func loadConfig(file string, cfg *zenoh.Config) error {
	b, err := os.ReadFile(file)
	if err != nil {
		return err
	}
	var sections map[string]json.RawMessage
	if err := json.Unmarshal(b, &sections); err != nil {
		return fmt.Errorf("%s: %w", file, err)
	}
	for name, v := range sections {
		switch name {
		case "downsampling":
			err = decodeStrict(v, &cfg.Downsampling)
		case "low_pass_filter":
			err = decodeStrict(v, &cfg.LowPassFilter)
		case "qos":
			var q struct {
				Network     []zenoh.QoSOverwriteConfig `json:"network"`
				Publication json.RawMessage            `json:"publication"`
			}
			if err = decodeStrict(v, &q); err == nil {
				cfg.QoSOverwrite = q.Network
				if len(q.Publication) > 0 && string(q.Publication) != "[]" {
					fmt.Fprintf(os.Stderr, "gozenohd: %s: ignoring qos/publication\n", file)
				}
			}
		case "timestamping":
			var ts struct {
				Enabled             json.RawMessage `json:"enabled"`
				DropFutureTimestamp bool            `json:"drop_future_timestamp"`
			}
			if err = decodeStrict(v, &ts); err == nil {
				cfg.Timestamping.DropFutureTimestamp = ts.DropFutureTimestamp
				cfg.Timestamping.Enabled, err = routerValue(ts.Enabled)
			}
		case "access_control":
			cfg.ACL, err = decodeACL(v)
		case "gateway":
			cfg.Gateway, err = decodeGateway(v)
		default:
			fmt.Fprintf(os.Stderr, "gozenohd: %s: ignoring section %q\n", file, name)
		}
		if err != nil {
			return fmt.Errorf("%s: %s: %w", file, name, err)
		}
	}
	return nil
}

// routerValue reads a zenoh mode-dependent bool (a bool, or {"router":
// bool, "peer": bool, "client": bool}) for router mode (nil: unset).
func routerValue(v json.RawMessage) (*bool, error) {
	if len(v) == 0 || string(v) == "null" {
		return nil, nil
	}
	var b bool
	if json.Unmarshal(v, &b) == nil {
		return &b, nil
	}
	var m struct {
		Router *bool `json:"router"`
		Peer   *bool `json:"peer"`
		Client *bool `json:"client"`
	}
	if err := decodeStrict(v, &m); err != nil {
		return nil, err
	}
	return m.Router, nil
}
