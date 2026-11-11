package main

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/shabunin/tiny-zenoh-go/internal/json5"
	"github.com/shabunin/tiny-zenoh-go/link"
	"github.com/shabunin/tiny-zenoh-go/plugins/storage"
	"github.com/shabunin/tiny-zenoh-go/protocol"
	"github.com/shabunin/tiny-zenoh-go/shm"
	"github.com/shabunin/tiny-zenoh-go/transport"
	"github.com/shabunin/tiny-zenoh-go/zenoh"
)

// configTree builds the configuration the way zenohd does: the file (JSON5,
// or the whole configuration given as --cfg ':<json5>'), then
// --rest-http-port, then each --cfg 'path/to/key:<json5 value>'.
func configTree(file, restPort string, overrides []string) (map[string]any, error) {
	tree := map[string]any{}
	var whole *string
	for _, o := range overrides {
		k, v, ok := strings.Cut(o, ":")
		if !ok {
			return nil, fmt.Errorf("--cfg accepts KEY:VALUE pairs; %q is not one", o)
		}
		if k == "" {
			whole = &v
		}
	}
	var src []byte
	switch {
	case whole != nil:
		src = []byte(*whole)
	case file != "":
		if ext := strings.ToLower(filepath.Ext(file)); ext == ".yaml" || ext == ".yml" {
			return nil, fmt.Errorf("%s: YAML configuration files are not supported (use JSON5)", file)
		}
		b, err := os.ReadFile(file)
		if err != nil {
			return nil, err
		}
		src = b
	}
	if src != nil {
		v, err := parseJSON5(src)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", file, err)
		}
		m, ok := v.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("%s: the configuration is not an object", file)
		}
		tree = m
	}
	if restPort != "" && !strings.EqualFold(restPort, "none") {
		if err := insertPath(tree, "plugins/rest/http_port", restPort); err != nil {
			return nil, err
		}
	}
	for _, o := range overrides {
		k, v, _ := strings.Cut(o, ":")
		if k == "" {
			continue
		}
		val, err := parseJSON5([]byte(v))
		if err != nil {
			return nil, fmt.Errorf("--cfg %s: %w", k, err)
		}
		if err := insertPath(tree, strings.TrimPrefix(k, "/"), val); err != nil {
			return nil, fmt.Errorf("--cfg %s: %w", k, err)
		}
	}
	return tree, nil
}

func parseJSON5(b []byte) (any, error) {
	j, err := json5.ToJSON(b)
	if err != nil {
		return nil, err
	}
	dec := json.NewDecoder(bytes.NewReader(j))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	return v, nil
}

// insertPath sets tree[a][b][c] = v for path "a/b/c", creating objects on
// the way.
func insertPath(tree map[string]any, path string, v any) error {
	parts := strings.Split(path, "/")
	m := tree
	for i, p := range parts[:len(parts)-1] {
		next, ok := m[p].(map[string]any)
		if !ok {
			if _, exists := m[p]; exists && m[p] != nil {
				return fmt.Errorf("%s is not an object", strings.Join(parts[:i+1], "/"))
			}
			next = map[string]any{}
			m[p] = next
		}
		m = next
	}
	m[parts[len(parts)-1]] = v
	return nil
}

// applyTransport applies zenoh's transport section: unicast lowlatency,
// max_links, qos and compression; link/tx lease and batch_size; link/tls
// files; shared_memory/enabled; auth/usrpwd.
func applyTransport(v json.RawMessage, cfg *zenoh.Config) error {
	var tr struct {
		Unicast struct {
			Lowlatency  *bool                   `json:"lowlatency"`
			MaxLinks    *int                    `json:"max_links"`
			QoS         struct{ Enabled *bool } `json:"qos"`
			Compression struct{ Enabled *bool } `json:"compression"`
		} `json:"unicast"`
		Link struct {
			Tx struct {
				Lease     *int64  `json:"lease"`
				BatchSize *uint16 `json:"batch_size"`
			} `json:"tx"`
			TLS *struct {
				RootCACertificate   *string `json:"root_ca_certificate"`
				ListenCertificate   *string `json:"listen_certificate"`
				ListenPrivateKey    *string `json:"listen_private_key"`
				ConnectCertificate  *string `json:"connect_certificate"`
				ConnectPrivateKey   *string `json:"connect_private_key"`
				EnableMTLS          *bool   `json:"enable_mtls"`
				VerifyNameOnConnect *bool   `json:"verify_name_on_connect"`
			} `json:"tls"`
		} `json:"link"`
		SharedMemory struct{ Enabled *bool } `json:"shared_memory"`
		Auth         struct {
			Usrpwd struct {
				User           *string `json:"user"`
				Password       *string `json:"password"`
				DictionaryFile *string `json:"dictionary_file"`
			} `json:"usrpwd"`
			Pubkey map[string]any `json:"pubkey"`
		} `json:"auth"`
	}
	if err := json.Unmarshal(v, &tr); err != nil {
		return err
	}
	u := tr.Unicast
	if u.Lowlatency != nil {
		cfg.LowLatency = *u.Lowlatency
	}
	if u.MaxLinks != nil {
		cfg.MaxLinks = *u.MaxLinks
	}
	if u.QoS.Enabled != nil {
		cfg.DisableQoS = !*u.QoS.Enabled
	}
	if u.Compression.Enabled != nil {
		cfg.Compression = *u.Compression.Enabled
	}
	if l := tr.Link.Tx.Lease; l != nil {
		cfg.Lease = time.Duration(*l) * time.Millisecond
	}
	if b := tr.Link.Tx.BatchSize; b != nil {
		cfg.BatchSize = *b
	}
	if t := tr.Link.TLS; t != nil {
		if cfg.TLS == nil {
			cfg.TLS = &link.TLSConfig{}
		}
		for _, f := range []struct {
			src *string
			dst *string
		}{{t.RootCACertificate, &cfg.TLS.RootCACertificate}, {t.ListenCertificate, &cfg.TLS.ListenCertificate},
			{t.ListenPrivateKey, &cfg.TLS.ListenPrivateKey}, {t.ConnectCertificate, &cfg.TLS.ConnectCertificate},
			{t.ConnectPrivateKey, &cfg.TLS.ConnectPrivateKey}} {
			if f.src != nil {
				*f.dst = *f.src
			}
		}
		if t.EnableMTLS != nil {
			cfg.TLS.EnableMTLS = *t.EnableMTLS
		}
		if t.VerifyNameOnConnect != nil {
			cfg.TLS.SkipNameVerification = !*t.VerifyNameOnConnect
		}
	}
	if e := tr.SharedMemory.Enabled; e != nil {
		cfg.SharedMemory = nil
		if *e {
			cfg.SharedMemory = &shm.Options{}
		}
	}
	up := tr.Auth.Usrpwd
	str := func(p *string) string {
		if p == nil {
			return ""
		}
		return *p
	}
	user, password, dict := str(up.User), str(up.Password), str(up.DictionaryFile)
	if (user == "") != (password == "") {
		return fmt.Errorf("auth/usrpwd: user and password go together")
	}
	if user != "" || dict != "" {
		if cfg.Auth == nil {
			cfg.Auth = &transport.Auth{}
		}
		cfg.Auth.User, cfg.Auth.Password = user, password
		if dict != "" {
			b, err := os.ReadFile(dict)
			if err != nil {
				return fmt.Errorf("auth/usrpwd: %w", err)
			}
			if cfg.Auth.Users, err = transport.ParseUsrPwdDict(b); err != nil {
				return fmt.Errorf("auth/usrpwd: %s: %w", dict, err)
			}
		}
	}
	for k, val := range tr.Auth.Pubkey {
		if val != nil {
			warnf("transport/auth/pubkey/%s: not supported here, use --auth-key and --auth-known-key", k)
		}
	}
	return nil
}

// pluginsConfig is what gozenohd runs of zenoh's plugins section.
type pluginsConfig struct {
	restAddr string // plugins/rest/http_port
	storages []namedStorage
}

type namedStorage struct {
	name string
	cfg  storage.Config
}

// applyConfig applies the sections gozenohd understands to cfg and warns
// about the others. Endpoints given as flags (already in cfg) win over the
// file's.
func applyConfig(tree map[string]any, cfg *zenoh.Config) (*pluginsConfig, error) {
	plugins := &pluginsConfig{}
	names := make([]string, 0, len(tree))
	for name := range tree {
		names = append(names, name)
	}
	sort.Slice(names, func(i, j int) bool { return names[i] == "mode" || (names[j] != "mode" && names[i] < names[j]) })
	for _, name := range names {
		v, err := json.Marshal(tree[name])
		if err != nil {
			return nil, err
		}
		switch name {
		case "mode":
			var m string
			if err = json.Unmarshal(v, &m); err == nil {
				switch m {
				case "router":
					cfg.Mode = zenoh.ModeRouter
				case "peer":
					cfg.Mode = zenoh.ModePeer
				case "client":
					cfg.Mode = zenoh.ModeClient
				default:
					err = fmt.Errorf("unknown mode %q", m)
				}
			}
		case "id":
			var id string
			if err = json.Unmarshal(v, &id); err == nil {
				cfg.ZID, err = parseZIDHex(id)
			}
		case "listen", "connect":
			var e struct {
				Endpoints json.RawMessage `json:"endpoints"`
			}
			if err = json.Unmarshal(v, &e); err == nil && len(e.Endpoints) > 0 {
				var eps []string
				if err = modeValue(e.Endpoints, cfg.Mode, &eps); err == nil {
					if name == "listen" && len(cfg.Listen) == 0 {
						cfg.Listen = eps
					} else if name == "connect" && len(cfg.Connect) == 0 {
						cfg.Connect = eps
					}
				}
			}
		case "scouting":
			var s struct {
				Multicast struct {
					Enabled json.RawMessage `json:"enabled"`
					Address string          `json:"address"`
				} `json:"multicast"`
				Gossip struct {
					Enabled  json.RawMessage `json:"enabled"`
					Multihop json.RawMessage `json:"multihop"`
				} `json:"gossip"`
			}
			if err = json.Unmarshal(v, &s); err == nil {
				var on bool
				if len(s.Multicast.Enabled) > 0 && modeValue(s.Multicast.Enabled, cfg.Mode, &on) == nil {
					cfg.Scouting.Disable = !on
				}
				if s.Multicast.Address != "" {
					cfg.Scouting.Address = s.Multicast.Address
				}
				on = true
				if len(s.Gossip.Enabled) > 0 && modeValue(s.Gossip.Enabled, cfg.Mode, &on) == nil {
					cfg.Gossip.Disable = !on
				}
				on = false
				if len(s.Gossip.Multihop) > 0 && modeValue(s.Gossip.Multihop, cfg.Mode, &on) == nil {
					cfg.Gossip.Multihop = on
				}
			}
		case "region_name":
			err = json.Unmarshal(v, &cfg.RegionName)
		case "namespace":
			err = json.Unmarshal(v, &cfg.Namespace)
		case "queries_default_timeout":
			var ms *int64
			if err = json.Unmarshal(v, &ms); err == nil && ms != nil {
				cfg.QueryTimeout = time.Duration(*ms) * time.Millisecond
			}
		case "transport":
			err = applyTransport(v, cfg)
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
					warnf("ignoring qos/publication")
				}
			}
		case "timestamping":
			var ts struct {
				Enabled             json.RawMessage `json:"enabled"`
				DropFutureTimestamp bool            `json:"drop_future_timestamp"`
			}
			if err = decodeStrict(v, &ts); err == nil {
				cfg.Timestamping.DropFutureTimestamp = ts.DropFutureTimestamp
				if len(ts.Enabled) > 0 && string(ts.Enabled) != "null" {
					var b bool
					if err = modeValue(ts.Enabled, cfg.Mode, &b); err == nil {
						cfg.Timestamping.Enabled = &b
					}
				}
			}
		case "adminspace":
			var a struct {
				Enabled     *bool `json:"enabled"`
				Permissions *struct {
					Read  *bool `json:"read"`
					Write bool  `json:"write"`
				} `json:"permissions"`
			}
			if err = decodeStrict(v, &a); err == nil {
				cfg.AdminSpace.Enabled = a.Enabled
				if p := a.Permissions; p != nil {
					cfg.AdminSpace.DenyRead = p.Read != nil && !*p.Read
					if p.Write {
						warnf("adminspace writes are not supported")
					}
				}
			}
		case "metadata":
			// Re-encoded: zenoh-rust shows objects with sorted keys.
			var m any
			if err = json.Unmarshal(v, &m); err == nil {
				cfg.Metadata, err = json.Marshal(m)
			}
		case "access_control":
			cfg.ACL, err = decodeACL(v)
		case "gateway":
			cfg.Gateway, err = decodeGateway(v)
		case "plugins":
			err = decodePlugins(v, plugins)
		case "plugins_loading":
			// Plugins are built in.
		default:
			warnf("ignoring section %q", name)
		}
		if err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
	}
	return plugins, nil
}

const defaultScoutAddr = "224.0.0.224:7446"

func warnf(f string, a ...any) { fmt.Fprintf(os.Stderr, "gozenohd: config: "+f+"\n", a...) }

// modeValue decodes a zenoh mode-dependent value: the value itself, or
// {"router": v, "peer": v, "client": v} (the entry for mode is used).
func modeValue(raw json.RawMessage, mode zenoh.Mode, out any) error {
	if json.Unmarshal(raw, out) == nil {
		return nil
	}
	var m map[string]json.RawMessage
	if err := decodeStrict(raw, &m); err != nil {
		return err
	}
	for k := range m {
		if k != "router" && k != "peer" && k != "client" {
			return fmt.Errorf("unknown mode %q", k)
		}
	}
	if v, ok := m[mode.String()]; ok {
		return json.Unmarshal(v, out)
	}
	return nil
}

// parseZIDHex reads a ZID the way zenohd's -i and "id" do: hex text of the
// little-endian u128, printed big-endian.
func parseZIDHex(h string) (protocol.ZID, error) {
	if len(h)%2 == 1 {
		h = "0" + h
	}
	b, err := hex.DecodeString(h)
	if err != nil {
		return protocol.ZID{}, err
	}
	for i, j := 0, len(b)-1; i < j; i, j = i+1, j-1 {
		b[i], b[j] = b[j], b[i]
	}
	return protocol.NewZID(b)
}

func decodePlugins(raw json.RawMessage, p *pluginsConfig) error {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		return err
	}
	for name, v := range m {
		switch name {
		case "rest":
			var r struct {
				HTTPPort json.RawMessage `json:"http_port"`
			}
			if err := json.Unmarshal(v, &r); err != nil {
				return fmt.Errorf("rest: %w", err)
			}
			var s string
			var n json.Number
			switch {
			case json.Unmarshal(r.HTTPPort, &s) == nil && s != "":
				p.restAddr = s
			case json.Unmarshal(r.HTTPPort, &n) == nil && n != "":
				p.restAddr = n.String()
			default:
				return fmt.Errorf("rest: http_port must be a port number or \"<ip>:<port>\"")
			}
		case "storage_manager":
			var sm struct {
				Volumes  map[string]json.RawMessage `json:"volumes"`
				Storages map[string]json.RawMessage `json:"storages"`
			}
			if err := json.Unmarshal(v, &sm); err != nil {
				return fmt.Errorf("storage_manager: %w", err)
			}
			for vol := range sm.Volumes {
				warnf("storage_manager: volume %q is not available (only the built-in \"memory\" is)", vol)
			}
			names := make([]string, 0, len(sm.Storages))
			for n := range sm.Storages {
				names = append(names, n)
			}
			sort.Strings(names)
			for _, n := range names {
				sc, err := decodeStorage(n, sm.Storages[n])
				if err != nil {
					return fmt.Errorf("storage_manager: storages: %s: %w", n, err)
				}
				p.storages = append(p.storages, namedStorage{n, sc})
			}
		default:
			warnf("plugin %q is not available in gozenohd (built in: rest, storage_manager)", name)
		}
	}
	return nil
}

// decodeStorage reads one storage of zenoh-backend-traits' config.
func decodeStorage(name string, raw json.RawMessage) (storage.Config, error) {
	var s struct {
		KeyExpr           string          `json:"key_expr"`
		StripPrefix       string          `json:"strip_prefix"`
		Complete          json.RawMessage `json:"complete"`
		Volume            json.RawMessage `json:"volume"`
		GarbageCollection *struct {
			Period   *uint64 `json:"period"`
			Lifespan *uint64 `json:"lifespan"`
		} `json:"garbage_collection"`
		Replication json.RawMessage `json:"replication"`
	}
	var c storage.Config
	if err := json.Unmarshal(raw, &s); err != nil {
		return c, err
	}
	if s.KeyExpr == "" {
		return c, fmt.Errorf("key_expr is missing")
	}
	c.KeyExpr, c.StripPrefix = s.KeyExpr, s.StripPrefix
	if len(s.Complete) > 0 {
		switch string(s.Complete) {
		case "true", `"true"`:
			c.Complete = true
		case "false", `"false"`:
		default:
			return c, fmt.Errorf("complete must be true or false")
		}
	}
	var vol string
	if json.Unmarshal(s.Volume, &vol) != nil {
		var o struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(s.Volume, &o); err != nil || o.ID == "" {
			return c, fmt.Errorf("volume must be a string or an object with an \"id\"")
		}
		vol = o.ID
	}
	if vol != "memory" {
		return c, fmt.Errorf("volume %q is not available (only \"memory\" is)", vol)
	}
	if g := s.GarbageCollection; g != nil {
		if g.Period != nil {
			c.GCPeriod = time.Duration(*g.Period) * time.Second
		}
		if g.Lifespan != nil {
			c.GCLifespan = time.Duration(*g.Lifespan) * time.Second
		}
	}
	if len(s.Replication) > 0 && string(s.Replication) != "null" {
		// zenoh-backend-traits: interval in (fractional) seconds,
		// propagation_delay in milliseconds.
		var r struct {
			Interval         *float64 `json:"interval"`
			SubIntervals     *int     `json:"sub_intervals"`
			Hot              *uint64  `json:"hot"`
			Warm             *uint64  `json:"warm"`
			PropagationDelay *uint64  `json:"propagation_delay"`
		}
		if err := json.Unmarshal(s.Replication, &r); err != nil {
			return c, fmt.Errorf("replication: %w", err)
		}
		rc := &storage.ReplicationConfig{}
		if r.Interval != nil {
			rc.Interval = time.Duration(*r.Interval * float64(time.Second))
		}
		if r.SubIntervals != nil {
			rc.SubIntervals = *r.SubIntervals
		}
		if r.Hot != nil {
			rc.Hot = *r.Hot
		}
		if r.Warm != nil {
			rc.Warm = *r.Warm
		}
		if r.PropagationDelay != nil {
			rc.PropagationDelay = time.Duration(*r.PropagationDelay) * time.Millisecond
		}
		c.Replication = rc
	}
	return c, nil
}

// startPlugins starts the configured storages and REST server on s; the
// returned function stops them.
func startPlugins(s *zenoh.Session, p *pluginsConfig) (func(), error) {
	var stops []func() error
	stop := func() {
		for i := len(stops) - 1; i >= 0; i-- {
			_ = stops[i]()
		}
	}
	for _, ns := range p.storages {
		m, err := storage.NewMemory(s, ns.cfg)
		if err != nil {
			stop()
			return nil, fmt.Errorf("storage %s: %w", ns.name, err)
		}
		stops = append(stops, m.Close)
		fmt.Printf("storage %s on %s (memory)\n", ns.name, ns.cfg.KeyExpr)
	}
	if p.restAddr != "" {
		closeREST, addr, err := startREST(s, p.restAddr)
		if err != nil {
			stop()
			return nil, err
		}
		stops = append(stops, closeREST)
		fmt.Printf("REST API on http://%s\n", addr)
	}
	return stop, nil
}
