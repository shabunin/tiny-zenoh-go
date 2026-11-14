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
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"runtime"
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

// debugSignals, if set (sigdump.go), installs the stack/memory dump
// signal handlers.
var debugSignals func()

// statusEvery is how often -v prints the connected sessions.
var statusEvery = 10 * time.Second

func main() {
	if debugSignals != nil {
		debugSignals()
	}
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr, stop))
}

// options is what the command line and the configuration ask for.
type options struct {
	version bool
	verbose bool
	cfg     zenoh.Config
	plugins *pluginsConfig
	tree    map[string]any // the whole configuration (its plugins section is live)
}

// usageError is a bad command line or configuration: exit status 2.
type usageError struct{ error }

// run is tzd: it routes until stop delivers, then closes everything and
// returns the exit status (0 ok, 1 runtime failure, 2 bad usage or
// configuration).
func run(args []string, stdout, stderr io.Writer, stop <-chan os.Signal) int {
	defer setConsole(setConsole(stdout, stderr))
	o, err := parseArgs(args, stderr)
	switch {
	case errors.Is(err, flag.ErrHelp):
		return 0
	case err != nil:
		var ue usageError
		if errors.As(err, &ue) && ue.error == nil {
			return 2 // the flag package already said why
		}
		errorf("%v", err)
		return 2
	case o.version:
		printf("tzd %s %s %s/%s\n", zenoh.Version, runtime.Version(), runtime.GOOS, runtime.GOARCH)
		return 0
	}

	pluginTree, _ := o.tree["plugins"].(map[string]any)
	runner := newPluginRunner(pluginTree)
	cfg := o.cfg
	cfg.AdminSpace.OnConfig = runner.onConfig
	cfg.AdminSpace.OnConfigError = func(key string, err error) { warnf("config %s: %v", key, err) }
	r, err := zenoh.Open(cfg)
	if err != nil {
		errorf("%v", err)
		return 1
	}
	printf("tzd %s listening on %s\n", r.ZID(), strings.Join(r.Locators(), " "))
	if err = runner.start(r, o.plugins); err != nil {
		errorf("%v", err)
		_ = r.Close()
		return 1
	}

	tick := time.NewTicker(statusEvery)
	defer tick.Stop()
	for {
		select {
		case <-stop:
			runner.stop()
			_ = r.Close()
			return 0
		case <-tick.C:
			if o.verbose {
				printf("clients=%d peers=%d routers=%d\n",
					len(r.ClientsZID()), len(r.PeersZID()), len(r.RoutersZID()))
			}
		}
	}
}

// parseArgs reads the command line and the configuration it names into
// options. Flag errors and -h come back as usageError{nil} and
// flag.ErrHelp (the flag package has printed them to stderr), other
// problems as usageError.
func parseArgs(args []string, stderr io.Writer) (*options, error) {
	fs := flag.NewFlagSet("tzd", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var listen, connect list
	fs.Var(&listen, "l", "locator to listen on (repeatable; default tcp/0.0.0.0:7447)")
	fs.Var(&connect, "e", "locator to connect to (repeatable)")
	noScout := fs.Bool("no-multicast-scouting", false, "don't answer multicast SCOUTs")
	scoutAddr := fs.String("scouting-address", defaultScoutAddr, "multicast scouting group")
	id := fs.String("i", "", "router ZID in hex (default random)")
	verbose := fs.Bool("v", false, "log connected sessions every 10s")
	shmOn := fs.Bool("shm", false, "enable shared memory with same-host nodes (Linux)")
	compress := fs.Bool("compression", false, "enable LZ4 batch compression with nodes that enable it")
	maxLinks := fs.Int("max-links", 1, "links per peer transport (>1 enables multilink)")
	var tlsCfg link.TLSConfig
	fs.StringVar(&tlsCfg.ListenCertificate, "tls-cert", "", "PEM certificate chain for tls/ listeners")
	fs.StringVar(&tlsCfg.ListenPrivateKey, "tls-key", "", "PEM private key for tls/ listeners")
	fs.StringVar(&tlsCfg.RootCACertificate, "tls-ca", "", "PEM CA to verify tls/ peers (default: system roots)")
	fs.StringVar(&tlsCfg.ConnectCertificate, "tls-client-cert", "", "PEM client certificate for -e tls/ (mutual TLS)")
	fs.StringVar(&tlsCfg.ConnectPrivateKey, "tls-client-key", "", "PEM client key for -e tls/ (mutual TLS)")
	fs.BoolVar(&tlsCfg.EnableMTLS, "tls-mtls", false, "require client certificates signed by --tls-ca")
	usrpwdDict := fs.String("usrpwd-dict", "", "require user/password authentication: file of user:password lines")
	user := fs.String("user", "", "user name to present to -e nodes that require authentication")
	password := fs.String("password", "", "password to present with --user")
	authKey := fs.String("auth-key", "", "PKCS#1 PEM RSA private key: enables pubkey authentication")
	var knownKeys list
	fs.Var(&knownKeys, "auth-known-key", "PKCS#1 PEM RSA public key accepted from nodes (repeatable)")
	lowLatency := fs.Bool("lowlatency", false, "low-latency transport (no batching, no QoS) with nodes that enable it")
	aclFile := fs.String("acl", "", "access control: JSON file with zenoh's access_control section")
	regionName := fs.String("region-name", "", "region name announced to the nodes we connect to")
	gatewayFile := fs.String("gateway", "", "south subregions: JSON file with zenoh's gateway section")
	configFile := fs.String("config", "", "zenoh config file (JSON5): its mode, id, listen, connect, scouting/multicast, "+
		"downsampling, low_pass_filter, qos/network, timestamping, access_control, gateway, adminspace, stats, metadata "+
		"and plugins (rest, storage_manager with memory, fs, influxdb, influxdb2 and s3 volumes) sections")
	fs.StringVar(configFile, "c", "", "same as -config")
	restPort := fs.String("rest-http-port", "", "enable the REST API: a port, <ip>:<port>, or none")
	var cfgOverrides list
	fs.Var(&cfgOverrides, "cfg", "config change KEY:VALUE, VALUE in JSON5, KEY a /-separated path or empty for the whole config (repeatable)")
	noTimestamp := fs.Bool("no-timestamp", false, "don't timestamp routed data that has no timestamp")
	adminPerms := fs.String("adminspace-permissions", "", "admin space permissions: r, rw or none (rw: plugins/... config writes at @/<zid>/router/config/**)")
	showVersion := fs.Bool("version", false, "print the version and exit")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil, err
		}
		return nil, usageError{}
	}
	if fs.NArg() > 0 {
		return nil, usageError{fmt.Errorf("unexpected arguments: %s", strings.Join(fs.Args(), " "))}
	}
	o := &options{version: *showVersion, verbose: *verbose}
	if o.version {
		return o, nil
	}

	o.cfg = zenoh.Config{
		Mode:     zenoh.ModeRouter,
		Listen:   listen,
		Connect:  connect,
		Scouting: zenoh.ScoutingConfig{Address: defaultScoutAddr},
	}
	cfg := &o.cfg
	var err error
	if *aclFile != "" {
		if cfg.ACL, err = loadACL(*aclFile); err != nil {
			return nil, usageError{err}
		}
	}
	if *gatewayFile != "" {
		if cfg.Gateway, err = loadGateway(*gatewayFile); err != nil {
			return nil, usageError{err}
		}
	}
	tree, err := configTree(*configFile, *restPort, cfgOverrides)
	if err != nil {
		return nil, usageError{err}
	}
	// Flags given explicitly win over the configuration, as in zenohd.
	fs.Visit(func(f *flag.Flag) {
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
		_ = insertPath(tree, "adminspace/permissions", map[string]any{"read": *adminPerms != "none", "write": *adminPerms == "rw"})
	default:
		return nil, usageError{errors.New("--adminspace-permissions: r, rw or none")}
	}
	if o.plugins, err = applyConfig(tree, cfg); err != nil {
		return nil, usageError{fmt.Errorf("config: %w", err)}
	}
	if err = pubkeyAuth(cfg, *authKey, knownKeys); err != nil {
		return nil, usageError{err}
	}
	if *id != "" {
		if cfg.ZID, err = parseZIDHex(*id); err != nil {
			return nil, usageError{fmt.Errorf("bad -i: %w", err)}
		}
	}
	o.tree = tree
	return o, nil
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
