// Package zenoh is the user-facing API of tiny-zenoh-go: a pure-Go Zenoh client and
// peer.
//
//	s, err := zenoh.Open(zenoh.Config{Connect: []string{"tcp/127.0.0.1:7447"}})
//	sub, _ := s.DeclareSubscriber("demo/**", func(smp zenoh.Sample) { ... })
//	s.Put("demo/hello", []byte("world"))
//	replies, _ := s.Get("demo/**")
//
// In client mode (the default) the session talks to one router. In peer mode
// it listens for and connects to other peers directly, discovering them by
// multicast scouting, and routers too. A peer routes for the clients
// attached to it, and with Config.Gateway for subregions of peers (zenoh
// 1.10 regions, see region.go).
//
// Callbacks (subscriber, queryable, liveliness) run on a receive goroutine:
// keep them short and never block in them waiting for other Zenoh traffic.
// Channel-based results (Get) are buffered without bound and never block the
// receive path.
package zenoh

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shabunin/tiny-zenoh-go/keyexpr"
	"github.com/shabunin/tiny-zenoh-go/link"
	"github.com/shabunin/tiny-zenoh-go/protocol"
	"github.com/shabunin/tiny-zenoh-go/shm"
	"github.com/shabunin/tiny-zenoh-go/transport"
)

// ErrSessionClosed is returned by operations on a closed session.
var ErrSessionClosed = errors.New("zenoh: session closed")

// Mode is the role a session plays in the Zenoh network.
type Mode int

const (
	// ModeClient connects to a single router (or peer) that routes for it.
	ModeClient Mode = iota
	// ModePeer listens for and connects to other nodes directly.
	ModePeer
	// ModeRouter routes for the clients and peers attached to it.
	ModeRouter
)

func (m Mode) String() string {
	switch m {
	case ModePeer:
		return "peer"
	case ModeRouter:
		return "router"
	}
	return "client"
}

func (m Mode) whatAmI() protocol.WhatAmI {
	switch m {
	case ModePeer:
		return protocol.Peer
	case ModeRouter:
		return protocol.Router
	}
	return protocol.Client
}

// WhatAmIMatcher is a set of node roles (for scouting and autoconnect).
type WhatAmIMatcher = protocol.WhatAmIMatcher

// Role sets.
const (
	MatchRouter = protocol.MatchRouter
	MatchPeer   = protocol.MatchPeer
	MatchClient = protocol.MatchClient
)

// ScoutingConfig configures multicast scouting (peer mode).
type ScoutingConfig struct {
	// Disable turns multicast scouting off (no SCOUT sent, none answered).
	Disable bool
	// Address is the multicast group. Default "224.0.0.224:7446".
	Address string
	// Interface names the network interface to scout on. Default: all
	// up, multicast-capable, non-loopback IPv4 interfaces.
	Interface string
	// Autoconnect lists the roles to connect to when discovered. Default
	// routers and peers.
	Autoconnect WhatAmIMatcher
}

// Config configures a session. Zero fields take defaults.
type Config struct {
	// Mode is ModeClient (default) or ModePeer.
	Mode Mode
	// Connect lists locators to connect to. In client mode they are tried in
	// order until one works; default ["tcp/127.0.0.1:7447"]. In peer mode all
	// are connected, and reconnected in the background when lost.
	Connect []string
	// Listen lists locators to accept connections on (peer and router mode).
	// Default (nil) ["tcp/0.0.0.0:0"] for a peer, ["tcp/0.0.0.0:7447"] for a
	// router; an empty non-nil list listens nowhere.
	Listen []string
	// Scouting configures multicast discovery: a peer scouts and answers, a
	// router only answers (so clients and peers can find it).
	Scouting ScoutingConfig
	// Gossip configures gossip scouting between peers (peer mode).
	Gossip GossipConfig
	// RegionName is announced to the nodes we connect to (zenoh-rust
	// region_name, 1..32 bytes), so that a gateway can sort us into one of
	// its subregions by name.
	RegionName string
	// Gateway configures the subregions we are the gateway of (zenoh-rust
	// gateway.south); the zero value keeps zenoh's auto preset.
	Gateway GatewayConfig
	// Namespace, if set (a key expression without wildcards), prefixes every
	// key the session is given with "<Namespace>/" and strips it from every
	// key it gives back; traffic on keys outside the namespace doesn't reach
	// the session (zenoh-rust config "namespace").
	Namespace string
	// ZID is this session's id. Default: random.
	ZID ZID
	// Lease is the session lease announced to other nodes. Default 10s.
	Lease time.Duration
	// BatchSize is the largest batch we accept. Default 65535; lower it on
	// memory-constrained targets. Multicast groups use min(BatchSize, 8192),
	// and all members of a group must agree on it (2048 to join zenoh-pico
	// nodes with their default build).
	BatchSize uint16
	// QueryTimeout is the default Get timeout. Default 10s.
	QueryTimeout time.Duration
	// ConnectTimeout bounds dialing each locator. Default 5s.
	ConnectTimeout time.Duration
	// TLS configures "tls/" locators (certificates, CA, mutual TLS). Locators
	// can also carry these settings in their "#..." config part.
	TLS *link.TLSConfig
	// Dial opens a link to a locator. Default: package link (tcp, udp, tls).
	// Override it to supply your own transport (e.g. a TinyGo netdev socket).
	Dial func(locator string, timeout time.Duration) (transport.Link, error)
	// ListenFunc opens a listener on a locator. Default: package link.
	ListenFunc func(locator string) (link.Listener, error)
	// SharedMemory, if set, enables zenoh's shared-memory transport with
	// same-host nodes that enable it too (Linux only; Open fails elsewhere):
	// payloads of at least Threshold bytes are passed through /dev/shm.
	SharedMemory *shm.Options
	// DisableQoS turns off zenoh's QoS transport extension, which is proposed
	// by default as zenoh-rust does: with it, each message priority (see
	// WithPriority) has its own sequence-number channels on a link, and
	// frames tell the peer the priority of what they carry.
	DisableQoS bool
	// LowLatency enables zenoh's low-latency transport with nodes that
	// enable it too (zenoh-rust: transport/unicast/lowlatency, which needs
	// transport/unicast/qos/enabled false): every message is written at once
	// on its own, with no batching, sequence numbers or fragmentation, so
	// payloads must fit in the batch size. It turns QoS off.
	LowLatency bool
	// Compression enables zenoh's LZ4 batch compression on links to nodes
	// that enable it too (zenoh-rust: transport/unicast/compression/enabled).
	// It trades CPU for bandwidth: useful on slow links with compressible
	// payloads.
	Compression bool
	// TxQueue configures each link's transmission queue and congestion
	// control (zenoh-rust transport/link/tx/queue): how many batches each
	// priority may hold, how long a CongestionDrop message waits for room
	// before it is dropped and a CongestionBlock one before the link is
	// given up. On TinyGo, Send writes batches itself (blocking, dropping
	// nothing) unless Mode is transport.TxQueued.
	TxQueue transport.TxQueue
	// MaxLinks > 1 enables multilink (zenoh-rust: transport/unicast/max_links):
	// up to MaxLinks links to the same node, e.g. Connect listing its tcp/
	// and udp/ locators, then make one transport. Reliable traffic uses the
	// reliable links and best-effort traffic prefers unreliable (datagram)
	// ones; the transport survives as long as one link is up, and lost links
	// are redialed. Both nodes must enable it.
	MaxLinks int
	// Auth enables handshake authentication (zenoh-rust transport/auth):
	// User+Password are presented to the nodes we connect to; Users and/or
	// an RSA Key (with KnownKeys) are required from nodes connecting to us.
	Auth *transport.Auth
	// ACL, if set, enables access control (zenoh-rust access_control): which
	// messages may be received from and sent to which nodes.
	ACL *ACLConfig
	// Downsampling, LowPassFilter and QoSOverwrite configure interceptors
	// on the data crossing the node's links (zenoh-rust "downsampling",
	// "low_pass_filter" and "qos/network").
	Downsampling  []DownsamplingConfig
	LowPassFilter []LowPassFilterConfig
	QoSOverwrite  []QoSOverwriteConfig
	// Timestamping configures timestamping (by default enabled in router
	// mode only).
	Timestamping TimestampingConfig
	// AdminSpace configures the admin space (zenoh-rust "adminspace").
	AdminSpace AdminSpaceConfig
	// Metadata is a JSON value the admin space shows (zenoh-rust
	// "metadata"), e.g. []byte(`{"name":"strawberry"}`); nil shows null.
	Metadata []byte
	// Stats configures traffic statistics (zenoh-rust's "stats" feature
	// and config section): see Session.Stats and Session.Metrics.
	Stats StatsConfig
}

func (c *Config) setDefaults() {
	if len(c.Connect) == 0 && c.Mode == ModeClient {
		c.Connect = []string{"tcp/127.0.0.1:7447"}
	}
	if c.Listen == nil && c.Mode == ModePeer {
		c.Listen = []string{"tcp/0.0.0.0:0"}
	}
	if c.Listen == nil && c.Mode == ModeRouter {
		c.Listen = []string{"tcp/0.0.0.0:7447"}
	}
	if c.Scouting.Address == "" {
		c.Scouting.Address = "224.0.0.224:7446"
	}
	if c.Scouting.Autoconnect == 0 {
		c.Scouting.Autoconnect = MatchRouter | MatchPeer
	}
	if c.Gossip.Autoconnect == 0 && !c.Gossip.NoAutoconnect {
		c.Gossip.Autoconnect = MatchRouter | MatchPeer | MatchClient
	}
	if c.Lease == 0 {
		c.Lease = 10 * time.Second
	}
	if c.QueryTimeout == 0 {
		c.QueryTimeout = 10 * time.Second
	}
	if c.ConnectTimeout == 0 {
		c.ConnectTimeout = 5 * time.Second
	}
	opts := link.Options{TLS: c.TLS}
	if c.Dial == nil {
		c.Dial = func(loc string, timeout time.Duration) (transport.Link, error) {
			return link.DialWith(loc, timeout, opts)
		}
	}
	if c.ListenFunc == nil {
		c.ListenFunc = func(loc string) (link.Listener, error) { return link.ListenWith(loc, opts) }
	}
}

// NewZID returns a random ZID.
func NewZID() ZID {
	for {
		var b [16]byte
		// Uniqueness, not secrecy, matters here; math/rand/v2 is also
		// available on TinyGo targets without a crypto RNG.
		binary.LittleEndian.PutUint64(b[:8], rand.Uint64())
		binary.LittleEndian.PutUint64(b[8:], rand.Uint64())
		if z, err := protocol.NewZID(b[:]); err == nil {
			return z
		}
	}
}

// Session is a connection to a Zenoh network.
type Session struct {
	cfg  Config
	zid  ZID
	done chan struct{}

	closed    atomic.Bool
	closeOnce sync.Once
	clock     hlc

	mu           sync.Mutex
	faces        map[string]*face // by peer ZID (registered at handshake)
	connecting   map[string]bool  // ZIDs being dialed after scouting
	listeners    []link.Listener
	scout        *scouter
	nextEntity   uint32
	nextQID      uint32
	nextInterest uint32
	nextExpr     uint16
	localExprs   map[uint16]string // key-expr ids we declared
	// exprMu also guards localExprs and the faces' remoteExprs: writers
	// hold it and mu, so readers may hold either (resolving a message's
	// key needs only this one).
	exprMu       sync.RWMutex
	subs         map[uint32]*Subscriber
	queryables   map[uint32]*Queryable
	queries      map[uint32]*pendingQuery
	routes       map[uint32]*routedQuery          // router mode: requests forwarded for faces
	meshes       map[Region]*meshState            // router mode: the router networks, per region
	gossip       *gossipState                     // peer mode: gossip scouting
	gossipNet    *lsNetwork                       // peer mode: multihop gossip
	southNets    map[Region]*lsNetwork            // our south peer subregions
	upstream     *upstreamInterest                // what our gateways tell us, for our south faces
	acl          *aclEnforcer                     // access control, nil if off
	icpt         *interceptors                    // downsampling, low-pass, QoS overwrite; nil if none
	tsOn, tsDrop bool                             // timestamping, and dropping future timestamps
	livSubs      map[uint32]*LivelinessSubscriber // by interest id
	livGets      map[uint32]*livGet               // by interest id
	matchers     map[uint32]*matcher              // publishers/queriers by interest id
	localTokens  map[uint32]*LivelinessToken
	shm          *shm.Manager // nil without Config.SharedMemory
	mlKey        *transport.MultiLinkKey
	mcast        []*mcastGroup  // multicast groups joined (Listen)
	stats        *statsRegistry // traffic statistics, nil if off

	// Connectivity events (events.go). evMu orders announcements; evState
	// and evListeners are guarded by mu.
	evMu        sync.Mutex
	evHooks     atomic.Pointer[evHooks]
	evListeners []*EventsListener // in declaration order
	evState     []*evEntry        // announced transports, in order
}

// Open starts a session. In client mode it connects to the first reachable
// locator of cfg.Connect. In peer and router mode it starts listening,
// connects to cfg.Connect (retrying in the background), and starts scouting.
func Open(cfg Config) (*Session, error) {
	cfg.setDefaults()
	s := &Session{
		cfg:         cfg,
		zid:         cfg.ZID,
		done:        make(chan struct{}),
		faces:       map[string]*face{},
		connecting:  map[string]bool{},
		localExprs:  map[uint16]string{},
		subs:        map[uint32]*Subscriber{},
		queryables:  map[uint32]*Queryable{},
		queries:     map[uint32]*pendingQuery{},
		routes:      map[uint32]*routedQuery{},
		livSubs:     map[uint32]*LivelinessSubscriber{},
		livGets:     map[uint32]*livGet{},
		matchers:    map[uint32]*matcher{},
		localTokens: map[uint32]*LivelinessToken{},
		southNets:   map[Region]*lsNetwork{},
	}
	if err := cfg.validateRegions(); err != nil {
		return nil, err
	}
	if err := validNamespace(cfg.Namespace); err != nil {
		return nil, err
	}
	if s.zid.IsZero() {
		s.zid = NewZID()
	}
	s.meshes = map[Region]*meshState{}
	if cfg.Mode == ModeRouter {
		s.meshForLocked(North)
	}
	icpt, err := newInterceptors(cfg.Downsampling, cfg.LowPassFilter, cfg.QoSOverwrite)
	if err != nil {
		return nil, err
	}
	s.icpt = icpt
	s.tsOn = cfg.Mode == ModeRouter
	if cfg.Timestamping.Enabled != nil {
		s.tsOn = *cfg.Timestamping.Enabled
	}
	s.tsDrop = cfg.Timestamping.DropFutureTimestamp
	if on := statsDefault; cfg.Stats.Enabled == nil && on || cfg.Stats.Enabled != nil && *cfg.Stats.Enabled {
		r, err := newStatsRegistry(cfg.Stats)
		if err != nil {
			return nil, fmt.Errorf("zenoh: stats: %w", err)
		}
		s.stats = r
	}
	if cfg.ACL != nil {
		acl, err := newACL(cfg.ACL)
		if err != nil {
			return nil, err
		}
		s.acl = acl
	}
	if cfg.Mode == ModePeer && !cfg.Gossip.Disable {
		if cfg.Gossip.Multihop {
			s.gossipNet = newNetworkAs(s.zid, protocol.Peer)
			s.gossipNet.gossip = true
		} else {
			s.gossip = newGossip(s.zid, protocol.Peer)
		}
	}
	if cfg.SharedMemory != nil {
		m, err := shm.New(*cfg.SharedMemory)
		if err != nil {
			return nil, fmt.Errorf("zenoh: shared memory: %w", err)
		}
		s.shm = m
	}
	if cfg.MaxLinks > 1 {
		k, err := transport.NewMultiLinkKey()
		if err != nil {
			return nil, fmt.Errorf("zenoh: multilink key: %w", err)
		}
		s.mlKey = k
	}
	if err := s.startAdmin(); err != nil {
		if s.shm != nil {
			_ = s.shm.Close()
		}
		return nil, err
	}
	if cfg.Mode == ModeClient {
		if err := s.openClient(); err != nil {
			if s.shm != nil {
				_ = s.shm.Close()
			}
			return nil, err
		}
		return s, nil
	}
	if err := s.openPeer(); err != nil {
		_ = s.Close()
		return nil, err
	}
	return s, nil
}

func (s *Session) openClient() error {
	var errs []error
	for i, ep := range s.cfg.Connect {
		if _, err := s.dial(ep); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", ep, err))
			continue
		}
		if s.mlKey != nil {
			// Multilink: the router's other locators add links (other
			// routers are refused as duplicates).
			for _, ep := range s.cfg.Connect[i+1:] {
				_, _ = s.dial(ep)
			}
		}
		return nil
	}
	return fmt.Errorf("zenoh: could not connect: %w", errors.Join(errs...))
}

func (s *Session) openPeer() error {
	for _, ep := range s.cfg.Listen {
		if link.IsMulticast(ep) {
			if err := s.openMulticast(ep); err != nil {
				return err
			}
			continue
		}
		ln, err := s.cfg.ListenFunc(ep)
		if err != nil {
			return fmt.Errorf("zenoh: listen on %s: %w", ep, err)
		}
		s.mu.Lock()
		s.listeners = append(s.listeners, ln)
		s.mu.Unlock()
		go s.acceptLoop(ln)
	}
	for _, ep := range s.cfg.Connect {
		f, err := s.dial(ep) // first attempt synchronously, so Open returns connected
		go s.connector(ep, f, err)
	}
	if !s.cfg.Scouting.Disable {
		sc, err := startScouter(s)
		if err != nil {
			return fmt.Errorf("zenoh: scouting: %w", err)
		}
		s.mu.Lock()
		s.scout = sc
		s.mu.Unlock()
	}
	return nil
}

// ZID returns this session's id.
func (s *Session) ZID() ZID { return s.zid }

// Mode returns the session's mode.
func (s *Session) Mode() Mode { return s.cfg.Mode }

// RouterZID returns the id of a router the session is connected to (in client
// mode: the node it connected to, whatever its role), or a zero ZID.
//
// ClientsZID, PeersZID and RoutersZID list the connected nodes by role.
func (s *Session) RouterZID() ZID {
	for _, f := range s.faceList() {
		if s.cfg.Mode == ModeClient || f.whatami == protocol.Router {
			return f.zid
		}
	}
	return ZID{}
}

// RoutersZID returns the ids of the connected routers.
func (s *Session) RoutersZID() []ZID { return s.zidsOf(protocol.Router) }

// PeersZID returns the ids of the connected peers.
func (s *Session) PeersZID() []ZID { return s.zidsOf(protocol.Peer) }

// ClientsZID returns the ids of the connected clients (peer and router mode).
func (s *Session) ClientsZID() []ZID { return s.zidsOf(protocol.Client) }

func (s *Session) zidsOf(w protocol.WhatAmI) []ZID {
	var out []ZID
	for _, f := range s.faceList() {
		if f.whatami == w {
			out = append(out, f.zid)
		}
	}
	return out
}

// TransportInfo describes what was negotiated with a connected node.
type TransportInfo struct {
	ZID         ZID
	WhatAmI     WhatAmI
	Proto       string // protocol of the link the transport was made on
	Links       int
	BatchSize   int
	QoS         bool
	LowLatency  bool
	Compression bool
	MultiLink   bool
	SharedMem   bool
	// PriorityStreams: each priority has its own stream (QUIC multistream).
	PriorityStreams bool
	AuthUser        string // the user the node authenticated as, if any
	// Region is the node's region; Gateway reports that it is our region
	// gateway (we are south of it); RegionName is the name it announced.
	Region     Region
	Gateway    bool
	RegionName string
	// Multicast: a node heard on one of our multicast groups (zenoh-rust
	// Transport::is_multicast); only ZID, WhatAmI, Proto, Links, BatchSize
	// and QoS are set.
	Multicast bool
}

// Transports describes the transports to the nodes we are connected to:
// unicast ones, then the members of our multicast groups (zenoh-rust:
// session.info().transports()).
func (s *Session) Transports() []TransportInfo {
	var out []TransportInfo
	for _, f := range s.faceList() {
		select {
		case <-f.ready:
		default:
			continue
		}
		if f.tr == nil {
			continue
		}
		out = append(out, f.transportInfo())
	}
	return append(out, s.mcastTransports()...)
}

// Locators returns the locators this session listens on (peer mode), with
// unspecified addresses expanded to the host's interface addresses.
func (s *Session) Locators() []string {
	s.mu.Lock()
	lns := append([]link.Listener(nil), s.listeners...)
	s.mu.Unlock()
	var out []string
	for _, ln := range lns {
		out = append(out, expandLocator(ln.Locator(), true)...)
	}
	return out
}

// Done is closed when the session ends (Close; in client mode also when the
// router connection is lost).
func (s *Session) Done() <-chan struct{} { return s.done }

// NewTimestamp returns a fresh timestamp from the session's HLC.
func (s *Session) NewTimestamp() Timestamp { return Timestamp{Time: s.clock.now(), ID: s.zid} }

// Close ends the session.
func (s *Session) Close() error {
	s.shutdown()
	return nil
}

func (s *Session) shutdown() {
	s.closeOnce.Do(func() {
		s.closed.Store(true)
		s.mu.Lock()
		lns, sc, groups := s.listeners, s.scout, s.mcast
		s.listeners, s.scout, s.mcast = nil, nil, nil
		faces := make([]*face, 0, len(s.faces))
		for _, f := range s.faces {
			faces = append(faces, f)
		}
		queries := s.queries
		s.queries = map[uint32]*pendingQuery{}
		gets := s.livGets
		s.livGets = map[uint32]*livGet{}
		s.mu.Unlock()
		for _, ln := range lns {
			_ = ln.Close()
		}
		for _, g := range groups {
			_ = g.tr.Close()
		}
		if sc != nil {
			sc.close()
		}
		for _, f := range faces {
			f.close()
		}
		for _, q := range queries {
			q.finish()
		}
		for _, g := range gets {
			g.finish()
		}
		if s.shm != nil {
			_ = s.shm.Close()
		}
		close(s.done)
	})
}

// faceList returns the established faces.
func (s *Session) faceList() []*face {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.faceListLocked()
}

func (s *Session) faceListLocked() []*face {
	out := make([]*face, 0, len(s.faces))
	for _, f := range s.faces {
		if f.live {
			out = append(out, f)
		}
	}
	return out
}

// broadcast sends declarations-style messages to every face. In client mode a
// failure is reported (the router is the only face); in peer mode a dying face
// is simply skipped.
func (s *Session) broadcast(faces []*face, msgs ...transport.Encoder) error {
	if s.closed.Load() {
		return ErrSessionClosed
	}
	var first error
	for _, f := range faces {
		if err := f.send(true, msgs...); err != nil && first == nil {
			first = err
		}
	}
	if s.cfg.Mode == ModeClient {
		if first != nil && s.closed.Load() {
			return ErrSessionClosed
		}
		return first
	}
	return nil
}

// declareLocalLocked announces a change to one of the session's own entities.
// It is called with s.mu held and unlocks it. North faces (peers, our
// gateways) get msg; south faces whose interests cover the entity, and the
// router mesh, are told through routerUpdateLocked.
func (s *Session) declareLocalLocked(k declKind, key string, msg transport.Encoder) error {
	defer s.checkMatching()
	out := outbox{}
	if s.cfg.Mode != ModeClient {
		s.routerUpdateLocked(k, key, out)
	}
	var faces []*face
	for _, f := range s.faceListLocked() {
		if f.push || f.routed {
			faces = append(faces, f)
		}
	}
	s.mu.Unlock()
	if s.closed.Load() {
		return ErrSessionClosed
	}
	out.send()
	return s.broadcast(faces, msg)
}

func (s *Session) entityID() uint32 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextEntity++
	return s.nextEntity
}

func validKey(key string) (*keyexpr.Key, error) {
	k, err := keyexpr.New(key)
	if err != nil {
		return nil, fmt.Errorf("zenoh: invalid key expression %q: %w", key, err)
	}
	return k, nil
}

// splitSelector splits "key?params" into its parts.
func splitSelector(sel string) (string, string) {
	key, params, _ := strings.Cut(sel, "?")
	return key, params
}

// -----------------------------------------------------------------------------
// Connections
// -----------------------------------------------------------------------------

func (s *Session) transportConfig(f *face) transport.Config {
	return transport.Config{
		ZID:          s.zid,
		WhatAmI:      s.cfg.Mode.whatAmI(),
		Lease:        s.cfg.Lease,
		BatchSize:    s.cfg.BatchSize,
		Admit:        func(z protocol.ZID, w protocol.WhatAmI) error { return s.admit(f, z, w) },
		Ext:          s.shmExt(),
		QoS:          !s.cfg.DisableQoS && !s.cfg.LowLatency,
		LowLatency:   s.cfg.LowLatency,
		Compression:  s.cfg.Compression,
		MaxLinks:     s.cfg.MaxLinks,
		MultiLinkKey: s.mlKey,
		Join:         func(z protocol.ZID) *transport.Unicast { return s.join(f, z) },
		Auth:         s.cfg.Auth,
		RegionName:   s.cfg.RegionName,
		Bound:        func(p transport.PeerInfo) (transport.Bound, error) { return s.boundFor(f, p) },
		Stats:        s.stats != nil,
		TxQueue:      s.cfg.TxQueue,
	}
}

// join returns the transport of the established face to node z, if any, for
// a new link to join (multilink); f remembers it.
func (s *Session) join(f *face, z protocol.ZID) *transport.Unicast {
	if s.mlKey == nil {
		return nil
	}
	s.mu.Lock()
	ef := s.faces[string(z.Bytes())]
	s.mu.Unlock()
	if ef == nil {
		return nil
	}
	select {
	case <-ef.ready:
	default:
		return nil // still in its handshake
	}
	if ef.tr == nil {
		return nil
	}
	f.joinTo = ef
	return ef.tr
}

func (s *Session) shmExt() func() transport.HandshakeExt {
	if s.shm == nil {
		return nil
	}
	return func() transport.HandshakeExt { return s.shm.NewLink() }
}

// admit registers f under the peer's ZID unless a face to that peer exists.
// It also snapshots the declarations f must be told about; any declaration
// made afterwards finds f in s.faces and is sent by its declarer.
func (s *Session) admit(f *face, z protocol.ZID, w protocol.WhatAmI) error {
	if z.Equal(s.zid) {
		return transport.ErrSelf
	}
	f.zid, f.whatami = z, w // also on rejection: the connector looks up duplicates by it
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed.Load() {
		return ErrSessionClosed
	}
	if s.cfg.Mode == ModeClient && len(s.faces) > 0 {
		return transport.ErrDuplicate
	}
	if _, dup := s.faces[string(z.Bytes())]; dup {
		return transport.ErrDuplicate
	}
	// The face's region, hence its role, is known once OPEN is done
	// (established); until then declarations skip it (it is not live).
	s.faces[string(z.Bytes())] = f
	return nil
}

func (s *Session) callbacks(f *face) transport.Callbacks {
	return transport.Callbacks{OnMessage: f.onMessage, OnClose: f.onClose}
}

// dial connects to a locator and returns the established face.
func (s *Session) dial(ep string) (*face, error) {
	l, err := s.cfg.Dial(ep, s.cfg.ConnectTimeout)
	if err != nil {
		return nil, err
	}
	f := newFace(s)
	f.ifaces = link.LocalInterfaces(l)
	f.dialed = ep
	if loc, err := link.Parse(ep); err == nil {
		f.proto = loc.Protocol
	}
	tr, err := transport.Open(l, s.transportConfig(f), s.callbacks(f))
	return f, s.established(f, l, tr, err)
}

// accept runs the responder handshake on an inbound link.
func (s *Session) accept(l transport.Link, proto string) {
	f := newFace(s)
	f.proto = proto
	f.ifaces = link.LocalInterfaces(l)
	tr, err := transport.Accept(l, s.transportConfig(f), s.callbacks(f))
	_ = s.established(f, l, tr, err)
}

// established finishes face setup after a handshake: on success it sends the
// initial declarations and opens the face for traffic. If the link joined the
// transport of an existing face instead, f only tracks that link.
func (s *Session) established(f *face, l transport.Link, tr *transport.Unicast, err error) error {
	if err == nil && f.joinTo != nil && tr == f.joinTo.tr {
		f.linkDone = tr.LinkDone(l)
		close(f.ready)
		owner := f.joinTo
		s.mu.Lock()
		if owner.joined == nil {
			owner.joined = map[transport.Link][2]string{}
		}
		owner.joined[l] = [2]string{f.proto, f.dialed}
		s.mu.Unlock()
		s.adminLinksChanged(owner)
		go func() {
			select {
			case <-f.linkDone:
				s.adminLinksChanged(owner)
				s.mu.Lock()
				delete(owner.joined, l)
				s.mu.Unlock()
			case <-owner.closed:
			}
		}()
		return nil
	}
	f.joinTo = nil
	if err != nil {
		s.mu.Lock()
		if f.zid.Len() > 0 && s.faces[string(f.zid.Bytes())] == f {
			delete(s.faces, string(f.zid.Bytes())) // admitted, then the handshake failed
		}
		s.mu.Unlock()
		close(f.ready)
		return err
	}
	// The transport runs: it may close at any time from here on, and the
	// face's teardown waits until this setup is over.
	defer f.setupFinished()
	s.mu.Lock()
	if err := s.assignRegionLocked(f, tr); err != nil {
		if s.faces[string(f.zid.Bytes())] == f {
			delete(s.faces, string(f.zid.Bytes()))
		}
		s.mu.Unlock()
		_ = tr.Close()
		close(f.ready)
		return fmt.Errorf("zenoh: region of %s: %w", f.zid, err)
	}
	f.live = true
	f.initial = s.snapshotLocked(f)
	upstream := f.router != nil && s.needUpstreamLocked()
	s.mu.Unlock()
	f.tr = tr
	f.linkDone = tr.LinkDone(l)
	f.shm = shmLink(tr)
	s.statsUp(f)
	s.setupACL(f, l, tr)
	s.setupInterceptors(f)
	if f.acl != nil || f.icpt != nil {
		f.initial = f.interceptEgress(f.initial, true)
	}
	if len(f.initial) > 0 {
		_ = tr.Send(true, f.initial...)
	}
	f.initial = nil
	if upstream {
		s.startUpstream() // before f's first INTEREST can be read
	}
	close(f.ready)
	if s.closed.Load() {
		f.close()
		return ErrSessionClosed
	}
	s.adminFaceUp(f)
	if f.mesh {
		s.meshLinkUp(f)
	}
	if !f.region.South {
		if s.gossip != nil && gossipTarget(f.whatami) {
			s.gossipLinkUp(f)
		}
		if s.gossipNet != nil && gossipTarget(f.whatami) {
			s.gossipNetLinkUp(f)
		}
	} else if f.region.Mode == protocol.Peer {
		s.southLinkUp(f)
	}
	return nil
}

func (s *Session) acceptLoop(ln link.Listener) {
	for {
		l, err := ln.Accept()
		if err != nil {
			if s.closed.Load() {
				return
			}
			time.Sleep(100 * time.Millisecond) // e.g. too many open files
			continue
		}
		go s.accept(l, ln.Locator().Protocol)
	}
}

// connector keeps a configured peer-mode endpoint connected.
func (s *Session) connector(ep string, f *face, err error) {
	backoff := time.Second
	for !s.closed.Load() {
		switch {
		case err == nil:
			backoff = time.Second
			select {
			case <-f.linkDone: // this link (or the whole transport) is gone
			case <-s.done:
				return
			}
		case errors.Is(err, transport.ErrDuplicate):
			// Already connected to that node (e.g. it connected to us, or was
			// scouted first): watch that connection instead.
			if existing := s.faceByZID(f.zid); existing != nil {
				select {
				case <-existing.closed:
				case <-s.done:
					return
				}
				break
			}
			fallthrough
		default:
			select {
			case <-time.After(backoff):
			case <-s.done:
				return
			}
			backoff = min(backoff*2, 8*time.Second)
		}
		f, err = s.dial(ep)
	}
}

func (s *Session) faceByZID(z ZID) *face {
	if z.Len() == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.faces[string(z.Bytes())]
}

// connectScouted connects to a node found by scouting, unless already
// connected or connecting to it.
//
// Two nodes usually discover each other at the same time. If both dialed at
// once, each could keep its own outgoing handshake and refuse the other's,
// leaving no link at all. So the node with the greater ZID dials at once and
// the other waits a little, dialing only if still not connected (in case the
// greater one cannot reach it). zenoh-rust peers always dial at once; that is
// fine, a Go node then just accepts.
func (s *Session) connectScouted(z ZID, locators []string) {
	key := string(z.Bytes())
	s.mu.Lock()
	if s.closed.Load() || s.faces[key] != nil || s.connecting[key] || z.Equal(s.zid) {
		s.mu.Unlock()
		return
	}
	s.connecting[key] = true
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.connecting, key)
		s.mu.Unlock()
	}()
	if zidLess(s.zid, z) {
		select {
		case <-time.After(time.Second + rand.N(500*time.Millisecond)):
		case <-s.done:
			return
		}
		if s.faceByZID(z) != nil {
			return
		}
	}
	// TCP first: it is what nodes listen on by default and dials cheaply.
	for _, tcp := range []bool{true, false} {
		for _, loc := range locators {
			l, err := link.Parse(loc)
			if err != nil || (l.Protocol == "tcp") != tcp || link.IsMulticast(loc) {
				continue
			}
			if _, err := s.dial(loc); err == nil || errors.Is(err, transport.ErrDuplicate) {
				return
			}
		}
	}
}
