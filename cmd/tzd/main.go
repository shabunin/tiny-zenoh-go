// Command tzd is a pure-Go Zenoh router, a stand-in for zenohd.
//
//	tzd                                  # listen on tcp/0.0.0.0:7447
//	tzd -l tcp/0.0.0.0:7447 -l tcp/[::]:7448 -l udp/0.0.0.0:7447
//	tzd --no-multicast-scouting
//	tzd -l quic/0.0.0.0:7447 --tls-cert srv.pem --tls-key srv.key
//	tzd -l tls/0.0.0.0:7447 --tls-cert srv.pem --tls-key srv.key [--tls-ca ca.pem --tls-mtls]
//	tzd -e tcp/10.0.0.5:7447             # join another router (tzd or zenohd)
//	tzd -c zenoh.json5                   # zenoh JSON5 config: listen, connect, scouting, downsampling, qos, ...
//	tzd --rest-http-port 8000            # REST API (plugins/rest)
//	tzd --cfg 'plugins/storage_manager/storages/demo:{key_expr:"demo/**",volume:"memory"}'
//	tzd --cfg 'plugins/storage_manager/volumes/fs:{}' \
//	  --cfg 'plugins/storage_manager/storages/files:{key_expr:"files/**",strip_prefix:"files",volume:{id:"fs",dir:"files"}}'
//
// Clients and peers attach to it; it routes publications, queries and
// liveliness between them, answers multicast scouting so they can find it,
// and serves its admin space under @/<zid>/router (info, metrics, link-state
// graphs, entities, route successors). Built-in plugins: the REST API and
// the storage manager with the memory volume and the fs, influxdb,
// influxdb2 and s3 backends (TinyGo builds: memory and fs).
package main

import (
	"bytes"
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
	scoutAddr := flag.String("scouting-address", defaultScoutAddr, "multicast scouting group")
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
	configFile := flag.String("config", "", "zenoh config file (JSON5): its mode, id, listen, connect, scouting/multicast, "+
		"downsampling, low_pass_filter, qos/network, timestamping, access_control, gateway, adminspace, stats, metadata "+
		"and plugins (rest, storage_manager with memory, fs, influxdb, influxdb2 and s3 volumes) sections")
	flag.StringVar(configFile, "c", "", "same as -config")
	restPort := flag.String("rest-http-port", "", "enable the REST API: a port, <ip>:<port>, or none")
	var cfgOverrides list
	flag.Var(&cfgOverrides, "cfg", "config change KEY:VALUE, VALUE in JSON5, KEY a /-separated path or empty for the whole config (repeatable)")
	noTimestamp := flag.Bool("no-timestamp", false, "don't timestamp routed data that has no timestamp")
	adminPerms := flag.String("adminspace-permissions", "", "admin space permissions: r, rw or none (rw: plugins/... config writes at @/<zid>/router/config/**)")
	flag.Parse()

	cfg := zenoh.Config{
		Mode:     zenoh.ModeRouter,
		Listen:   listen,
		Connect:  connect,
		Scouting: zenoh.ScoutingConfig{Address: defaultScoutAddr},
	}
	var err error
	if *aclFile != "" {
		if cfg.ACL, err = loadACL(*aclFile); err != nil {
			fmt.Fprintln(os.Stderr, "tzd:", err)
			os.Exit(2)
		}
	}
	if *gatewayFile != "" {
		if cfg.Gateway, err = loadGateway(*gatewayFile); err != nil {
			fmt.Fprintln(os.Stderr, "tzd:", err)
			os.Exit(2)
		}
	}
	tree, err := configTree(*configFile, *restPort, cfgOverrides)
	if err != nil {
		fmt.Fprintln(os.Stderr, "tzd:", err)
		os.Exit(2)
	}
	// Flags given explicitly win over the configuration, as in zenohd.
	flag.Visit(func(f *flag.Flag) {
		set := func(path string, v any) { _ = insertPath(tree, path, v) }
		switch f.Name {
		case "no-multicast-scouting":
			set("scouting/multicast/enabled", !*noScout)
		case "scouting-address":
			set("scouting/multicast/address", *scoutAddr)
		case "shm":
			set("transport/shared_memory/enabled", *shmOn)
		case "compression":
			set("transport/unicast/compression/enabled", *compress)
		case "max-links":
			set("transport/unicast/max_links", *maxLinks)
		case "lowlatency":
			set("transport/unicast/lowlatency", *lowLatency)
		case "tls-cert":
			set("transport/link/tls/listen_certificate", tlsCfg.ListenCertificate)
		case "tls-key":
			set("transport/link/tls/listen_private_key", tlsCfg.ListenPrivateKey)
		case "tls-ca":
			set("transport/link/tls/root_ca_certificate", tlsCfg.RootCACertificate)
		case "tls-client-cert":
			set("transport/link/tls/connect_certificate", tlsCfg.ConnectCertificate)
		case "tls-client-key":
			set("transport/link/tls/connect_private_key", tlsCfg.ConnectPrivateKey)
		case "tls-mtls":
			set("transport/link/tls/enable_mtls", tlsCfg.EnableMTLS)
		case "usrpwd-dict":
			set("transport/auth/usrpwd/dictionary_file", *usrpwdDict)
		case "user":
			set("transport/auth/usrpwd/user", *user)
		case "password":
			set("transport/auth/usrpwd/password", *password)
		case "region-name":
			set("region_name", *regionName)
		case "no-timestamp":
			set("timestamping/enabled", !*noTimestamp)
		}
	})
	switch *adminPerms {
	case "":
	case "r", "rw", "none":
		insertPath(tree, "adminspace/permissions", map[string]any{"read": *adminPerms != "none", "write": *adminPerms == "rw"})
	default:
		fmt.Fprintln(os.Stderr, "tzd: --adminspace-permissions: r, rw or none")
		os.Exit(2)
	}
	plugins, err := applyConfig(tree, &cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, "tzd: config:", err)
		os.Exit(2)
	}
	if err = pubkeyAuth(&cfg, *authKey, knownKeys); err != nil {
		fmt.Fprintln(os.Stderr, "tzd:", err)
		os.Exit(2)
	}
	if *id != "" {
		if cfg.ZID, err = parseZIDHex(*id); err != nil {
			fmt.Fprintln(os.Stderr, "tzd: bad -i:", err)
			os.Exit(2)
		}
	}
	pluginTree, _ := tree["plugins"].(map[string]any)
	runner := newPluginRunner(pluginTree)
	cfg.AdminSpace.OnConfig = runner.onConfig
	cfg.AdminSpace.OnConfigError = func(key string, err error) { warnf("config %s: %v", key, err) }
	r, err := zenoh.Open(cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, "tzd:", err)
		os.Exit(1)
	}
	fmt.Printf("tzd %s listening on %s\n", r.ZID(), strings.Join(r.Locators(), " "))
	stopPlugins := runner.stop
	err = runner.start(r, plugins)
	if err != nil {
		fmt.Fprintln(os.Stderr, "tzd:", err)
		_ = r.Close()
		os.Exit(1)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	tick := time.NewTicker(10 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-sig:
			stopPlugins()
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

// pubkeyAuth adds the --auth-key/--auth-known-key public key
// authentication to cfg.
func pubkeyAuth(cfg *zenoh.Config, keyFile string, known []string) error {
	if keyFile == "" {
		if len(known) > 0 {
			return fmt.Errorf("--auth-known-key needs --auth-key")
		}
		return nil
	}
	if cfg.Auth == nil {
		cfg.Auth = &transport.Auth{}
	}
	a := cfg.Auth
	b, err := os.ReadFile(keyFile)
	if err != nil {
		return err
	}
	if a.Key, err = transport.ParseRSAKeyPEM(b); err != nil {
		return err
	}
	for _, f := range known {
		b, err := os.ReadFile(f)
		if err != nil {
			return err
		}
		k, err := transport.ParseRSAPublicKeyPEM(b)
		if err != nil {
			return fmt.Errorf("%s: %w", f, err)
		}
		a.KnownKeys = append(a.KnownKeys, k)
	}
	return nil
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
