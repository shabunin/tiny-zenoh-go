package transport

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shabunin/tiny-zenoh-go/internal/codec"
	"github.com/shabunin/tiny-zenoh-go/internal/lz4"
	"github.com/shabunin/tiny-zenoh-go/protocol"
)

// Errors reported by the transport (via Open or Callbacks.OnClose).
var (
	ErrClosed       = errors.New("transport: closed")
	ErrHandshake    = errors.New("transport: handshake failed")
	ErrLeaseExpired = errors.New("transport: peer lease expired")
	ErrPeerClosed   = errors.New("transport: closed by peer")
	ErrBatchSize    = errors.New("transport: batch exceeds negotiated size")
	// ErrDuplicate may be returned by Config.Admit when a transport to the peer
	// already exists; the new link is closed with reason MAX_LINKS.
	ErrDuplicate = errors.New("transport: already connected to peer")
	// ErrSelf may be returned by Config.Admit when the peer is this node.
	ErrSelf = errors.New("transport: connection to self")

	errInvalid    = errors.New("transport: link does not match the peer's transport")
	errLinkClosed = errors.New("transport: link closed by peer")
)

// Config parameterizes a unicast transport. Zero fields take defaults.
type Config struct {
	ZID     protocol.ZID     // required: this node's id
	WhatAmI protocol.WhatAmI // role announced in INIT; note the zero value is Router

	// Lease is how long the peer may go without hearing from us before it
	// declares us dead. Default 10s.
	Lease time.Duration
	// KeepAliveDivisor: keep-alives are sent every Lease/KeepAliveDivisor when
	// otherwise idle. Default 4 (Zenoh's default).
	KeepAliveDivisor int
	// BatchSize is the largest batch we can receive. Default 65535. Lower it on
	// memory-constrained (TinyGo) targets; the receive buffer is this size.
	BatchSize uint16
	// Resolution proposes the SN/request-id resolution. Default 32-bit/32-bit.
	Resolution protocol.Resolution
	// MaxMessageSize bounds a message reassembled from FRAGMENTs. Default 16 MiB.
	MaxMessageSize int
	// HandshakeTimeout bounds INIT/OPEN when the Link supports deadlines.
	// Default 10s.
	HandshakeTimeout time.Duration
	// Admit, if set, is called once the peer's identity is known and before
	// the transport is established (on the accepting side: before OPEN ACK is
	// sent). A non-nil error aborts the handshake and closes the link with a
	// matching CLOSE reason (see ErrDuplicate, ErrSelf).
	Admit func(zid protocol.ZID, whatami protocol.WhatAmI) error
	// Ext, if set, makes the optional handshake extension exchange run for
	// this transport (see HandshakeExt); Unicast.Ext returns it afterwards.
	Ext func() HandshakeExt
	// Auth, if set, enables handshake authentication (usrpwd, pubkey).
	Auth *Auth
	// Compression proposes zenoh's batch compression (LZ4): used only if the
	// peer enables it too (zenoh-rust: transport/unicast/compression/enabled,
	// off by default). Batches are compressed when that makes them smaller.
	Compression bool
	// QoS proposes zenoh's QoS extension: with it, each of the 8 message
	// priorities gets its own reliable and best-effort sequence-number
	// channels and frames carry their priority. It is used only if the peer
	// agrees (zenoh-rust does by default).
	QoS bool
	// LowLatency proposes zenoh's low-latency transport: each message is
	// written on its own as soon as it is sent, without batching, sequence
	// numbers or fragmentation (so it must fit in the batch size). Used only
	// if the peer enables it too (zenoh-rust: transport/unicast/lowlatency).
	// It excludes QoS, compression and multilink.
	LowLatency bool

	// MaxLinks > 1 enables multilink (zenoh-rust: transport/unicast/max_links):
	// further links to a peer join its existing transport instead of being
	// refused, if both nodes enable it. MultiLinkKey must then be set, the same
	// for every transport of this node.
	MaxLinks     int
	MultiLinkKey *MultiLinkKey
	// Join, if set, is asked (instead of Admit) whether a transport to the
	// peer exists; the new link joins the one it returns, if the multilink
	// handshake proved it is the same peer. Open and Accept then return that
	// transport. Returning nil makes the handshake call Admit.
	Join func(zid protocol.ZID) *Unicast

	// RegionName, if set, is announced to the peer in INIT (zenoh-rust
	// region_name) so that it can sort us into one of its subregions.
	RegionName string
	// Bound, if set, is called during OPEN to decide which side of us the
	// peer stands on; anything but BoundNone is told to the peer
	// (open::ext::RemoteBound). An error aborts the handshake.
	Bound func(PeerInfo) (Bound, error)

	// Stats keeps traffic counters per link (see LinkStats, LinkInfo.Stats).
	Stats bool

	// TxQueue configures the links' transmission queues and congestion
	// control (zenoh-rust transport/link/tx/queue).
	TxQueue TxQueue
}

func (c *Config) setDefaults() {
	if c.Lease == 0 {
		c.Lease = 10 * time.Second
	}
	if c.KeepAliveDivisor <= 0 {
		c.KeepAliveDivisor = 4
	}
	if c.BatchSize == 0 {
		c.BatchSize = 65535
	}
	if c.Resolution == 0 {
		c.Resolution = protocol.DefaultResolution
	}
	if c.MaxMessageSize == 0 {
		c.MaxMessageSize = 16 << 20
	}
	if c.HandshakeTimeout == 0 {
		c.HandshakeTimeout = 10 * time.Second
	}
	c.TxQueue.setDefaults()
}

// Encoder is a message that can serialize itself (network messages).
type Encoder interface {
	Encode(w *codec.Writer)
}

// Callbacks receive inbound traffic and lifecycle events. They are invoked from
// the transport's receive goroutine and must not block for long.
type Callbacks struct {
	// OnMessage is called for every network message received in an accepted
	// FRAME or reassembled from FRAGMENTs, in order. Messages from frames whose
	// sequence number is a duplicate or stale are decoded (to find the batch
	// boundary) but not delivered. Returning an error closes the transport.
	OnMessage func(m protocol.Message, reliable bool) error
	// OnClose is called exactly once when the transport closes. err is nil for a
	// local Close.
	OnClose func(err error)
}

type deadliner interface {
	SetDeadline(t time.Time) error
}

type fragState struct {
	buf    []byte
	nextSN uint64
	active bool
	skip   bool // a fragment was lost/oversized: drop until the message ends
}

// tlink is one link of a transport, with what was negotiated on it.
type tlink struct {
	t         *Unicast // owner; changes once, before the link starts, on a join
	link      Link
	prefix    int    // length-prefix bytes per batch: 2 on streams, 0 on packet links
	propose   uint16 // batch size we propose for this link
	batchSize int    // negotiated
	reliable  bool   // what the link guarantees (or the peer's QoSLink says)
	peerLease time.Duration

	// QoSLink restrictions from the peer.
	hasRange       bool
	prioLo, prioHi protocol.Priority
	qosLink        uint64 // the peer's QoSLink value, echoed when accepting

	// compress is set once compression was negotiated: from then on every
	// batch carries a header byte after the length prefix.
	compress bool
	lz       *compressState
	// lowLatency: after the handshake, messages travel one by one (with a
	// u32 length prefix on streams).
	lowLatency bool

	rxBuf          []byte
	lastRx, lastTx atomic.Int64
	stats          *LinkStats // nil: not counting (set before the link starts)

	// prioW: per-priority streams (PriorityLink), nil when not in use.
	prioW []io.Writer
	// txq is the link's transmission queue (nil: batches are written by
	// Send itself, see TxSync).
	txq *txQueue

	done      chan struct{}
	closeOnce sync.Once
}

func newTLink(link Link, batch uint16) *tlink {
	l := &tlink{link: link, prefix: streamPrefix, propose: batch, done: make(chan struct{})}
	rxSize := int(batch)
	if pl, ok := link.(PacketLink); ok {
		l.prefix = 0
		if mtu := pl.MTU(); mtu > 0 && mtu < int(l.propose) {
			l.propose = uint16(mtu)
		}
		// A packet must never be truncated: size for the largest the link
		// can carry, whatever batch size we ask for.
		rxSize = 0xffff
		if mtu := pl.MTU(); mtu > 0 && mtu < rxSize {
			rxSize = mtu
		}
	}
	l.rxBuf = make([]byte, rxSize)
	l.reliable = linkReliable(link)
	return l
}

func (l *tlink) close() {
	l.closeOnce.Do(func() {
		close(l.done)
		if l.txq != nil {
			l.txq.stop(ErrClosed)
		}
		_ = l.link.Close()
	})
}

// sendClose queues (or writes) CLOSE on l and waits, at most d, for what
// is queued on l to be written, before l is closed.
func (t *Unicast) sendClose(l *tlink, c *protocol.Close, d time.Duration) {
	if l.txq == nil {
		_ = t.writeTransport(l, c)
		return
	}
	deadline := time.Now().Add(d)
	// What is queued goes first: CLOSE takes the Control queue, which the
	// writers serve before the others, and the peer drops what follows it
	// (zenoh-rust likewise drains its pipeline before closing).
	l.txq.flush(d)
	_, _ = l.txq.pushTransport(c, time.Until(deadline))
	l.txq.flush(time.Until(deadline))
}

// closeWait bounds how long closing waits for queued batches.
func (t *Unicast) closeWait() time.Duration {
	return t.cfg.Lease / time.Duration(t.cfg.KeepAliveDivisor)
}

// Unicast is an established point-to-point Zenoh transport over one or more
// links (see Config.MaxLinks).
type Unicast struct {
	cfg Config
	cb  Callbacks

	peerZID     protocol.ZID
	peerWhatAmI protocol.WhatAmI
	peerRegion  string // region name the peer announced
	peerBound   Bound  // side of the peer we stand on, by its OPEN
	ourBound    Bound  // side of us the peer stands on, as we told it
	snMask      uint64
	// qos is set when the QoS extension was negotiated.
	qos bool
	// peerMLKey is the peer's multilink public key, proven during the
	// handshake (nil without multilink).
	peerMLKey []byte

	lmu   sync.Mutex
	links []*tlink

	// txMu serializes Send without transmission queues (and low latency).
	txMu sync.Mutex
	// txSN: per priority [0] best-effort, [1] reliable; guarded by txMu, or
	// with queues by the priority's txc lock.
	txSN      [numPrio][2]uint64
	txc       [numPrio]txChan
	txBatch   codec.Writer
	txPrio    protocol.Priority // priority of txBatch, for per-priority streams
	txPayload codec.Writer

	rxMu   sync.Mutex // links receive concurrently
	rxLink *tlink     // the link whose batch is being handled (guarded by rxMu)
	rx     rxState

	ext HandshakeExt
	// patch is the negotiated zenoh patch level (INIT ext 0x7).
	patch uint64

	// Handshake state.
	hs *tlink    // the link the handshake runs on
	ml mlState   // multilink exchange
	au authState // authentication exchange

	authUser string // accepted: the peer's usrpwd user

	lowLatency bool     // negotiated low-latency mode
	join       *Unicast // the transport the new link joins, if any

	closed    atomic.Bool
	closeOnce sync.Once
	done      chan struct{}
}

// Open performs the INIT/OPEN handshake as the initiating side over link and,
// on success, starts the receive and keep-alive goroutines. On failure the link
// is closed. With multilink, the result may be an existing transport that the
// link joined (see Config.Join).
func Open(link Link, cfg Config, cb Callbacks) (*Unicast, error) {
	return establish(link, cfg, cb, (*Unicast).openHandshake)
}

// Accept performs the INIT/OPEN handshake as the responding side over an
// inbound link (e.g. from a listener) and, on success, starts the transport.
// On failure the link is closed.
func Accept(link Link, cfg Config, cb Callbacks) (*Unicast, error) {
	return establish(link, cfg, cb, (*Unicast).acceptHandshake)
}

func establish(link Link, cfg Config, cb Callbacks, handshake func(*Unicast) error) (*Unicast, error) {
	cfg.setDefaults()
	if cfg.ZID.IsZero() {
		_ = link.Close()
		return nil, errors.New("transport: Config.ZID is required")
	}
	if cfg.MaxLinks > 1 && cfg.MultiLinkKey == nil {
		_ = link.Close()
		return nil, errors.New("transport: Config.MaxLinks > 1 needs a MultiLinkKey")
	}
	if cfg.LowLatency && cfg.QoS {
		_ = link.Close()
		return nil, errors.New("transport: QoS and LowLatency are incompatible (as in zenoh-rust)")
	}
	t := &Unicast{cfg: cfg, cb: cb, done: make(chan struct{})}
	l := newTLink(link, cfg.BatchSize)
	l.t = t
	t.hs = l
	if cfg.Ext != nil {
		t.ext = cfg.Ext()
	}
	if err := handshake(t); err != nil {
		_ = link.Close()
		if t.ext != nil {
			t.ext.Close()
		}
		return nil, err
	}
	t.hs = nil
	if ex := t.join; ex != nil {
		if t.ext != nil {
			t.ext.Close() // the transport keeps the state of its first link
		}
		if err := ex.addLink(l); err != nil {
			_ = link.Close()
			return nil, err
		}
		return ex, nil
	}
	t.prepare(l)
	t.links = []*tlink{l}
	if c := bestEffortCompanion(l); c != nil {
		c.t = t
		t.prepare(c)
		t.links = append(t.links, c)
		t.start(c)
	}
	t.start(l)
	return t, nil
}

// bestEffortCompanion returns the unreliable link that comes with l (see
// MixedReliabilityLink), set up like l, or nil.
func bestEffortCompanion(l *tlink) *tlink {
	ml, ok := l.link.(MixedReliabilityLink)
	if !ok || l.lowLatency {
		return nil
	}
	be := ml.BestEffort()
	if be == nil {
		return nil
	}
	c := newTLink(be, uint16(l.batchSize))
	c.batchSize = int(c.propose)
	c.reliable = false
	c.peerLease = l.peerLease
	c.enableCompression(l.compress)
	return c
}

// prepare sets up an established link's sending side, before Send can
// pick it.
func (t *Unicast) prepare(l *tlink) {
	if t.cfg.Stats {
		l.stats = &LinkStats{}
	}
	now := time.Now().UnixNano()
	l.lastRx.Store(now)
	l.lastTx.Store(now)
	if pl, ok := l.link.(PriorityLink); ok && pl.PriorityStreams() && !l.lowLatency {
		l.prioW = make([]io.Writer, numPrio)
		l.prioW[0] = l.link
		for p := 1; p < numPrio; p++ {
			l.prioW[p] = pl.PriorityWriter(protocol.Priority(p))
		}
	}
	if t.cfg.TxQueue.Mode == TxQueued && !l.lowLatency {
		l.txq = newTxQueue(l, t.qos, &t.cfg.TxQueue)
	}
}

// start runs a prepared link's goroutines.
func (t *Unicast) start(l *tlink) {
	if l.txq != nil {
		l.txq.start()
	}
	if pl, ok := l.link.(PriorityLink); ok && l.prioW != nil {
		for p := 1; p < numPrio; p++ {
			go t.rxLoop(l, pl.PriorityReader(protocol.Priority(p)), make([]byte, len(l.rxBuf)))
		}
	}
	go t.rxLoop(l, l.link, l.rxBuf)
	go t.keepAliveLoop(l)
}

// addLink attaches an established link to t.
func (t *Unicast) addLink(l *tlink) error {
	t.lmu.Lock()
	if t.closed.Load() || len(t.links) == 0 {
		t.lmu.Unlock()
		return ErrClosed
	}
	l.t = t
	t.prepare(l)
	t.links = append(t.links, l)
	t.lmu.Unlock()
	t.start(l)
	return nil
}

// linkList returns the current links. The slice is shared, not copied: it
// must not be modified (t.links is only ever replaced or appended to past
// what earlier callers see).
func (t *Unicast) linkList() []*tlink {
	t.lmu.Lock()
	defer t.lmu.Unlock()
	return t.links
}

func (t *Unicast) firstLink() *tlink {
	t.lmu.Lock()
	defer t.lmu.Unlock()
	if len(t.links) == 0 {
		return nil
	}
	return t.links[0]
}

// PeerZID returns the remote node's id.
func (t *Unicast) PeerZID() protocol.ZID { return t.peerZID }

// PeerWhatAmI returns the remote node's role.
func (t *Unicast) PeerWhatAmI() protocol.WhatAmI { return t.peerWhatAmI }

// PeerLease returns the lease the remote node announced (on the first link).
func (t *Unicast) PeerLease() time.Duration {
	if l := t.firstLink(); l != nil {
		return l.peerLease
	}
	return 0
}

// BatchSize returns the batch size negotiated on the first link.
func (t *Unicast) BatchSize() int {
	if l := t.firstLink(); l != nil {
		return l.batchSize
	}
	return 0
}

// Links returns the number of links the transport currently runs over.
func (t *Unicast) Links() int {
	t.lmu.Lock()
	defer t.lmu.Unlock()
	return len(t.links)
}

// LinkDone returns a channel closed when link (one given to Open or Accept
// that ended up in this transport) is no longer part of it. Link values are
// compared with ==.
func (t *Unicast) LinkDone(link Link) <-chan struct{} {
	t.lmu.Lock()
	defer t.lmu.Unlock()
	for _, l := range t.links {
		if l.link == link {
			return l.done
		}
	}
	c := make(chan struct{})
	close(c)
	return c
}

// PriorityStreams reports whether the first link carries each priority on
// its own stream (see PriorityLink).
func (t *Unicast) PriorityStreams() bool {
	l := t.firstLink()
	return l != nil && l.prioW != nil
}

// Ext returns the handshake extension made by Config.Ext (nil without one).
func (t *Unicast) Ext() HandshakeExt { return t.ext }

// Done is closed when the transport has shut down.
func (t *Unicast) Done() <-chan struct{} { return t.done }

// Close sends CLOSE to the peer and tears the transport down.
func (t *Unicast) Close() error {
	t.shutdown(nil)
	return nil
}

// -----------------------------------------------------------------------------
// Handshake
// -----------------------------------------------------------------------------

func (t *Unicast) setHandshakeDeadline() func() {
	if d, ok := t.hs.link.(deadliner); ok {
		_ = d.SetDeadline(time.Now().Add(t.cfg.HandshakeTimeout))
		return func() { _ = d.SetDeadline(time.Time{}) }
	}
	return func() {}
}

// closeReason maps an admission error to a CLOSE reason.
func closeReason(err error) byte {
	switch {
	case errors.Is(err, ErrDuplicate):
		return protocol.CloseMaxLinks
	case errors.Is(err, ErrSelf):
		return protocol.CloseConnectionToSelf
	case errors.Is(err, errInvalid):
		return protocol.CloseInvalid
	}
	return protocol.CloseGeneric
}

// admit decides whether the handshake makes a new transport (Config.Admit)
// or adds the link to an existing one (Config.Join); on rejection it tells
// the peer why (link-level CLOSE) and returns the error.
func (t *Unicast) admit() error {
	var err error
	if t.cfg.Join != nil {
		if ex := t.cfg.Join(t.peerZID); ex != nil {
			if err = ex.canJoin(t); err == nil {
				t.join = ex
				return nil
			}
		}
	}
	if err == nil && t.cfg.Admit != nil {
		err = t.cfg.Admit(t.peerZID, t.peerWhatAmI)
	}
	if err == nil {
		return nil
	}
	_ = t.writeTransport(t.hs, &protocol.Close{Reason: closeReason(err)})
	return fmt.Errorf("%w: %w", ErrHandshake, err)
}

// canJoin checks that the handshake c may add its link to t: both ends did
// the multilink exchange with the same keys and the same parameters.
func (t *Unicast) canJoin(c *Unicast) error {
	if t.peerMLKey == nil || c.peerMLKey == nil || t.lowLatency || c.lowLatency {
		return ErrDuplicate
	}
	if !bytes.Equal(t.peerMLKey, c.peerMLKey) || t.qos != c.qos || t.snMask != c.snMask ||
		t.peerWhatAmI != c.peerWhatAmI {
		return errInvalid
	}
	t.lmu.Lock()
	defer t.lmu.Unlock()
	if t.closed.Load() || len(t.links) == 0 || len(t.links) >= t.cfg.MaxLinks {
		return ErrDuplicate
	}
	return nil
}

// leaseFields encodes our lease like zenoh-rust: in seconds when whole.
func (t *Unicast) leaseFields() (seconds bool, v uint64) {
	if t.cfg.Lease%time.Second == 0 {
		return true, uint64(t.cfg.Lease / time.Second)
	}
	return false, uint64(t.cfg.Lease / time.Millisecond)
}

func leaseOf(o *protocol.Open) time.Duration {
	if o.LeaseSeconds {
		return time.Duration(o.Lease) * time.Second
	}
	return time.Duration(o.Lease) * time.Millisecond
}

// setPeerSN validates the peer's initial SN and primes the receive window.
func (t *Unicast) setPeerSN(isn uint64) error {
	if isn&^t.snMask != 0 {
		return fmt.Errorf("%w: peer initial SN %#x outside resolution", ErrHandshake, isn)
	}
	t.rx.init(t.snMask, t.qos, t.cfg.MaxMessageSize, t.deliver)
	t.rx.sync(isn)
	return nil
}

func (t *Unicast) openHandshake() error {
	defer t.setHandshakeDeadline()()
	l := t.hs

	syn := &protocol.Init{
		Version:    protocol.ProtocolVersion,
		WhatAmI:    t.cfg.WhatAmI,
		ZID:        t.cfg.ZID,
		HasParams:  true,
		Resolution: t.cfg.Resolution,
		BatchSize:  l.propose,
	}
	if t.cfg.QoS {
		syn.Exts = append(syn.Exts, protocol.Extension{ID: extQoS, Enc: protocol.ExtUnit})
	}
	if t.cfg.Compression {
		syn.Exts = append(syn.Exts, protocol.Extension{ID: extCompression, Enc: protocol.ExtUnit})
	}
	if t.cfg.LowLatency {
		syn.Exts = append(syn.Exts, llExt())
	}
	syn.Exts = append(syn.Exts, patchExt(patchCurrent))
	syn.Exts = addExt(syn.Exts, t.mlInitSyn())
	syn.Exts = addExt(syn.Exts, t.authInitSyn())
	syn.Exts = addExt(syn.Exts, t.regionNameExt())
	if t.ext != nil {
		syn.Exts = addExt(syn.Exts, t.ext.InitSyn())
	}
	if err := t.writeTransport(l, syn); err != nil {
		return fmt.Errorf("%w: send INIT: %v", ErrHandshake, err)
	}
	m, err := t.readTransport(l)
	if err != nil {
		return fmt.Errorf("%w: read INIT ACK: %v", ErrHandshake, err)
	}
	ack, ok := m.(*protocol.Init)
	if !ok || !ack.IsAck {
		return unexpected("INIT ACK", m)
	}
	if ack.Version != protocol.ProtocolVersion {
		return fmt.Errorf("%w: peer protocol version %#x, want %#x", ErrHandshake, ack.Version, protocol.ProtocolVersion)
	}
	t.peerZID, t.peerWhatAmI = ack.ZID, ack.WhatAmI
	if err := t.readRegionName(ack.Exts); err != nil {
		_ = t.writeTransport(l, &protocol.Close{Reason: protocol.CloseInvalid})
		return err
	}
	if t.cfg.QoS {
		if err := t.negotiateQoS(ack.Exts); err != nil {
			_ = t.writeTransport(l, &protocol.Close{Reason: protocol.CloseInvalid})
			return err
		}
	}
	if err := t.mlInitAck(findExt(ack.Exts, extMultiLink)); err != nil {
		_ = t.writeTransport(l, &protocol.Close{Reason: protocol.CloseInvalid})
		return err
	}
	if err := t.authInitAck(findExt(ack.Exts, extAuth)); err != nil {
		_ = t.writeTransport(l, &protocol.Close{Reason: protocol.CloseInvalid})
		return err
	}
	if t.ext != nil {
		t.ext.InitAck(findExt(ack.Exts, t.ext.ExtID()))
	}
	// Without multilink the link cannot join an existing transport, so decide
	// now, before OPEN SYN: rejecting a duplicate here means the peer never
	// sets up a transport that we would tear down at once (which, when both
	// nodes dial each other at the same time, races with the link we keep).
	admitted := false
	if !t.ml.on {
		if err := t.admit(); err != nil {
			return err
		}
		admitted = true
	}
	t.lowLatency = t.cfg.LowLatency && findExt(ack.Exts, extLowLatency) != nil
	useCompression := t.cfg.Compression && findExt(ack.Exts, extCompression) != nil && !t.lowLatency
	t.patch = negotiatePatch(ack.Exts)

	res, batch := t.cfg.Resolution, l.propose
	if ack.HasParams {
		res = ack.Resolution // the responder may only lower it
		batch = min(batch, ack.BatchSize)
	}
	l.batchSize = int(batch)
	t.snMask = snMask(res.FrameSNBits())
	isn := rand.Uint64() & t.snMask
	t.setTxSN(isn)

	open := &protocol.Open{InitialSN: isn, Cookie: ack.Cookie}
	open.LeaseSeconds, open.Lease = t.leaseFields()
	open.Exts = addExt(open.Exts, t.mlOpenSyn())
	open.Exts = addExt(open.Exts, t.authOpenSyn())
	rb, err := t.remoteBoundExt()
	if err != nil {
		_ = t.writeTransport(l, &protocol.Close{Reason: protocol.CloseGeneric})
		return err
	}
	open.Exts = addExt(open.Exts, rb)
	if t.ext != nil {
		open.Exts = addExt(open.Exts, t.ext.OpenSyn(l.reliable))
	}
	if err := t.writeTransport(l, open); err != nil {
		return fmt.Errorf("%w: send OPEN: %v", ErrHandshake, err)
	}
	m, err = t.readTransport(l)
	if err != nil {
		return fmt.Errorf("%w: read OPEN ACK: %v", ErrHandshake, err)
	}
	oack, ok := m.(*protocol.Open)
	if !ok || !oack.IsAck {
		return unexpected("OPEN ACK", m)
	}
	l.peerLease = leaseOf(oack)
	// Everything after OPEN ACK, both ways, has the batch header.
	l.enableCompression(useCompression)
	if t.lowLatency {
		l.enableLowLatency()
	}
	if err := t.setPeerSN(oack.InitialSN); err != nil {
		return err
	}
	t.mlOpenAck(findExt(oack.Exts, extMultiLink))
	if err := t.readRemoteBound(oack.Exts); err != nil {
		_ = t.writeTransport(l, &protocol.Close{Reason: protocol.CloseInvalid})
		return err
	}
	if err := t.authOpenAck(findExt(oack.Exts, extAuth)); err != nil {
		_ = t.writeTransport(l, &protocol.Close{Reason: protocol.CloseInvalid})
		return err
	}
	if t.ext != nil {
		if err := t.ext.OpenAck(findExt(oack.Exts, t.ext.ExtID())); err != nil {
			_ = t.writeTransport(l, &protocol.Close{Reason: protocol.CloseInvalid})
			return fmt.Errorf("%w: %w", ErrHandshake, err)
		}
	}
	if admitted {
		return nil
	}
	return t.admit()
}

// minResolution lowers each 2-bit field of ours to the peer's proposal.
func minResolution(ours, theirs protocol.Resolution) protocol.Resolution {
	var r protocol.Resolution
	for shift := 0; shift < 8; shift += 2 {
		m := protocol.Resolution(3) << shift
		r |= min(ours&m, theirs&m)
	}
	return r
}

// acceptHandshake is the responder side: INIT SYN -> INIT ACK (with a cookie)
// -> OPEN SYN (cookie echoed) -> OPEN ACK. None of zenoh-rust's handshake
// extensions are mandatory: we answer QoS, compression, multilink and
// Config.Ext's, and leave the others off.
func (t *Unicast) acceptHandshake() error {
	defer t.setHandshakeDeadline()()
	l := t.hs

	m, err := t.readTransport(l)
	if err != nil {
		return fmt.Errorf("%w: read INIT SYN: %v", ErrHandshake, err)
	}
	syn, ok := m.(*protocol.Init)
	if !ok || syn.IsAck {
		return unexpected("INIT SYN", m)
	}
	if syn.Version != protocol.ProtocolVersion {
		_ = t.writeTransport(l, &protocol.Close{Reason: protocol.CloseInvalid})
		return fmt.Errorf("%w: peer protocol version %#x, want %#x", ErrHandshake, syn.Version, protocol.ProtocolVersion)
	}
	if syn.ZID.Equal(t.cfg.ZID) {
		_ = t.writeTransport(l, &protocol.Close{Reason: protocol.CloseConnectionToSelf})
		return fmt.Errorf("%w: %w", ErrHandshake, ErrSelf)
	}
	t.peerZID, t.peerWhatAmI = syn.ZID, syn.WhatAmI
	if err := t.readRegionName(syn.Exts); err != nil {
		_ = t.writeTransport(l, &protocol.Close{Reason: protocol.CloseInvalid})
		return err
	}
	if t.cfg.QoS {
		if err := t.negotiateQoS(syn.Exts); err != nil {
			_ = t.writeTransport(l, &protocol.Close{Reason: protocol.CloseInvalid})
			return err
		}
	}
	if err := t.mlAcceptInitSyn(findExt(syn.Exts, extMultiLink)); err != nil {
		_ = t.writeTransport(l, &protocol.Close{Reason: protocol.CloseInvalid})
		return err
	}
	if err := t.authAcceptInitSyn(findExt(syn.Exts, extAuth)); err != nil {
		_ = t.writeTransport(l, &protocol.Close{Reason: protocol.CloseInvalid})
		return err
	}
	if t.ext != nil {
		t.ext.AcceptInitSyn(findExt(syn.Exts, t.ext.ExtID()))
	}

	// Without the S flag the initiator uses the protocol defaults.
	theirRes, theirBatch := protocol.DefaultResolution, uint16(65535)
	if syn.HasParams {
		theirRes, theirBatch = syn.Resolution, syn.BatchSize
	}
	res := minResolution(t.cfg.Resolution, theirRes)
	batch := min(l.propose, theirBatch)
	l.batchSize = int(batch)
	t.snMask = snMask(res.FrameSNBits())

	// The cookie only has to survive this one exchange on this link, so a
	// random nonce kept in memory replaces zenoh-rust's encrypted state blob.
	cookie := make([]byte, 16)
	for i := range cookie {
		cookie[i] = byte(rand.Uint32())
	}
	ack := &protocol.Init{
		IsAck:      true,
		Version:    protocol.ProtocolVersion,
		WhatAmI:    t.cfg.WhatAmI,
		ZID:        t.cfg.ZID,
		HasParams:  true,
		Resolution: res,
		BatchSize:  batch,
		Cookie:     cookie,
	}
	if t.qos {
		ack.Exts = append(ack.Exts, l.qosExt())
	}
	t.lowLatency = t.cfg.LowLatency && findExt(syn.Exts, extLowLatency) != nil
	if t.lowLatency {
		ack.Exts = append(ack.Exts, llExt())
	}
	useCompression := t.cfg.Compression && findExt(syn.Exts, extCompression) != nil && !t.lowLatency
	if useCompression {
		ack.Exts = append(ack.Exts, protocol.Extension{ID: extCompression, Enc: protocol.ExtUnit})
	}
	if t.patch = negotiatePatch(syn.Exts); t.patch != 0 {
		ack.Exts = append(ack.Exts, patchExt(t.patch))
	}
	ack.Exts = addExt(ack.Exts, t.mlAcceptInitAck())
	ack.Exts = addExt(ack.Exts, t.authAcceptInitAck())
	ack.Exts = addExt(ack.Exts, t.regionNameExt())
	if t.ext != nil {
		ack.Exts = addExt(ack.Exts, t.ext.AcceptInitAck())
	}
	if err := t.writeTransport(l, ack); err != nil {
		return fmt.Errorf("%w: send INIT ACK: %v", ErrHandshake, err)
	}

	m, err = t.readTransport(l)
	if err != nil {
		return fmt.Errorf("%w: read OPEN SYN: %v", ErrHandshake, err)
	}
	osyn, ok := m.(*protocol.Open)
	if !ok || osyn.IsAck {
		return unexpected("OPEN SYN", m)
	}
	if string(osyn.Cookie) != string(cookie) {
		_ = t.writeTransport(l, &protocol.Close{Reason: protocol.CloseInvalid})
		return fmt.Errorf("%w: OPEN SYN carries an unknown cookie", ErrHandshake)
	}
	l.peerLease = leaseOf(osyn)
	if err := t.setPeerSN(osyn.InitialSN); err != nil {
		_ = t.writeTransport(l, &protocol.Close{Reason: protocol.CloseInvalid})
		return err
	}
	if err := t.mlAcceptOpenSyn(findExt(osyn.Exts, extMultiLink)); err != nil {
		_ = t.writeTransport(l, &protocol.Close{Reason: protocol.CloseInvalid})
		return err
	}
	if err := t.authAcceptOpenSyn(findExt(osyn.Exts, extAuth)); err != nil {
		_ = t.writeTransport(l, &protocol.Close{Reason: protocol.CloseInvalid})
		return err
	}
	if err := t.readRemoteBound(osyn.Exts); err != nil {
		_ = t.writeTransport(l, &protocol.Close{Reason: protocol.CloseInvalid})
		return err
	}
	if t.ext != nil {
		if err := t.ext.AcceptOpenSyn(findExt(osyn.Exts, t.ext.ExtID())); err != nil {
			_ = t.writeTransport(l, &protocol.Close{Reason: protocol.CloseInvalid})
			return fmt.Errorf("%w: %w", ErrHandshake, err)
		}
	}
	if err := t.admit(); err != nil {
		return err
	}

	isn := rand.Uint64() & t.snMask
	t.setTxSN(isn)
	oack := &protocol.Open{IsAck: true, InitialSN: isn}
	oack.LeaseSeconds, oack.Lease = t.leaseFields()
	oack.Exts = addExt(oack.Exts, t.mlAcceptOpenAck())
	oack.Exts = addExt(oack.Exts, t.authAcceptOpenAck())
	rb, err := t.remoteBoundExt()
	if err != nil {
		_ = t.writeTransport(l, &protocol.Close{Reason: protocol.CloseGeneric})
		return err
	}
	oack.Exts = addExt(oack.Exts, rb)
	if t.ext != nil {
		oack.Exts = addExt(oack.Exts, t.ext.AcceptOpenAck(l.reliable))
	}
	if err := t.writeTransport(l, oack); err != nil {
		return fmt.Errorf("%w: send OPEN ACK: %v", ErrHandshake, err)
	}
	l.enableCompression(useCompression)
	if t.lowLatency {
		l.enableLowLatency()
	}
	return nil
}

func unexpected(want string, m protocol.Message) error {
	if c, ok := m.(*protocol.Close); ok {
		return fmt.Errorf("%w: peer sent CLOSE (reason %#x) instead of %s", ErrHandshake, c.Reason, want)
	}
	return fmt.Errorf("%w: expected %s, got %T", ErrHandshake, want, m)
}

func (t *Unicast) readTransport(l *tlink) (protocol.Message, error) {
	batch, err := l.readBatch(l.link, l.rxBuf)
	if err != nil {
		return nil, err
	}
	return protocol.DecodeTransport(codec.NewReader(batch))
}

// readBatch reads the next batch: length-prefixed on streams, one packet on
// packet links (empty packets are skipped).
func (l *tlink) readBatch(r io.Reader, buf []byte) ([]byte, error) {
	if l.lowLatency && l.prefix > 0 {
		return l.readLowLatency()
	}
	if l.prefix > 0 {
		return ReadBatch(r, buf)
	}
	for {
		n, err := r.Read(buf)
		if n > 0 {
			return buf[:n], nil
		}
		if err != nil {
			return nil, err
		}
	}
}

// -----------------------------------------------------------------------------
// Transmit
// -----------------------------------------------------------------------------

var lenPlaceholder = []byte{0, 0, 0, 0, 0} // length prefix (2, or 4 in low latency) + batch header

// writeTransport sends a single transport message in its own batch.
func (t *Unicast) writeTransport(l *tlink, m protocol.Message) error {
	t.txMu.Lock()
	defer t.txMu.Unlock()
	t.startBatchLocked(l)
	m.Encode(&t.txBatch)
	return t.flushBatchLocked(l)
}

// streamPrefix is the stream-link length prefix. zenoh-rust counts it INSIDE
// the negotiated batch size: its receive buffer is exactly batch_size bytes and
// the batch body is read at offset 2 (io/zenoh-transport/src/unicast/link.rs
// recv_batch), so a body may be at most batch_size-2 bytes.
const streamPrefix = 2

// hdrLen is the batch header size: 1 with compression, else 0.
func (l *tlink) hdrLen() int {
	if l.compress {
		return 1
	}
	return 0
}

// maxBody returns the largest batch body we may send.
func (l *tlink) maxBody() int { return l.batchSize - l.prefix - l.hdrLen() }

// startBatchLocked resets txBatch to an empty batch for l (length prefix and
// header placeholders).
func (t *Unicast) startBatchLocked(l *tlink) {
	t.txPrio = protocol.PriorityControl
	t.txBatch.Reset()
	t.txBatch.WriteRaw(lenPlaceholder[:l.prefix+l.hdrLen()])
}

// flushBatchLocked writes txBatch to l.
func (t *Unicast) flushBatchLocked(l *tlink) error {
	w := io.Writer(l.link)
	if l.prioW != nil {
		w = l.prioW[t.txPrio]
	}
	var zc *lz4.Compressor
	var zbuf *[]byte
	if l.lz != nil {
		zc, zbuf = &l.lz.c, &l.lz.txBuf
	}
	if err := l.writeBatch(w, t.txBatch.Bytes(), zc, zbuf); err != nil {
		return err
	}
	l.stats.addTransport(Tx)
	return nil
}

// writeBatch compresses the batch b if that is on, patches its length
// prefix and writes it to w (l or one of its priority streams).
func (l *tlink) writeBatch(w io.Writer, b []byte, zc *lz4.Compressor, zbuf *[]byte) error {
	if l.compress {
		b = l.compressBatch(b, zc, zbuf)
	}
	n := len(b) - l.prefix
	switch {
	case l.lowLatency:
		// zenoh-rust reads the u32 length, then the message into a buffer
		// of batch size.
		if l.batchSize > 0 && n > l.batchSize {
			return ErrBatchSize
		}
		if l.prefix > 0 {
			b[0], b[1], b[2], b[3] = byte(n), byte(n>>8), byte(n>>16), byte(n>>24)
		}
	case n > 0xffff || (l.batchSize > 0 && n > l.batchSize-l.prefix):
		return ErrBatchSize
	case l.prefix > 0:
		b[0], b[1] = byte(n), byte(n>>8)
	}
	if _, err := w.Write(b); err != nil {
		return err
	}
	l.lastTx.Store(time.Now().UnixNano())
	l.stats.addBytes(Tx, len(b))
	return nil
}

func (t *Unicast) nextSNLocked(prio protocol.Priority, reliable bool) uint64 {
	i := 0
	if reliable {
		i = 1
	}
	sn := t.txSN[prio][i]
	t.txSN[prio][i] = (sn + 1) & t.snMask
	return sn
}

// txPriority is the channel a message goes on: its own priority with QoS,
// the default one without.
func (t *Unicast) txPriority(m Encoder) protocol.Priority {
	if !t.qos {
		return protocol.PriorityData
	}
	return protocol.QoSOf(m).Priority()
}

// qosExts is the FRAME/FRAGMENT extension list for a priority: none for the
// default one.
func qosExts(prio protocol.Priority) []protocol.Extension {
	if prio == protocol.PriorityData {
		return nil
	}
	return []protocol.Extension{{ID: extQoS, Enc: protocol.ExtZ64, Mandatory: true, U64: uint64(prio)}}
}

// selectLink picks the link for a message as zenoh-rust does: one whose
// reliability matches and whose priority range (if any) is the narrowest
// containing the priority; else the first with a matching reliability; else
// the first link.
func selectLink(links []*tlink, reliable bool, prio protocol.Priority) *tlink {
	var full, partial, other *tlink
	fullWidth := 0
	for _, l := range links {
		match := l.reliable == reliable
		inRange := l.hasRange && l.prioLo <= prio && prio <= l.prioHi
		width := int(l.prioHi) - int(l.prioLo)
		switch {
		case match && inRange && (full == nil || fullWidth > width):
			full, fullWidth = l, width
		case match && !inRange && partial == nil:
			partial = l
		case other == nil:
			other = l
		}
	}
	switch {
	case full != nil:
		return full
	case partial != nil:
		return partial
	}
	return other
}

// frameCapacity is the room for network messages in one FRAME on l.
func (t *Unicast) frameCapacity(l *tlink) int {
	c := l.maxBody() - (1 + vleLen(t.snMask)) // FRAME header + max SN
	if t.qos {
		c -= 2 // QoS extension: header + 1-byte value
	}
	return c
}

// Send transmits network messages on the reliable or best-effort channel. As
// many messages as fit are packed into each FRAME; a message larger than a
// batch is split into FRAGMENTs. With QoS, consecutive messages of the same
// priority share a FRAME, which carries that priority. With several links,
// each message goes on the link selectLink picks.
//
// With transmission queues (TxQueued, see TxQueue), messages are queued and
// subject to congestion control: those dropped are listed by a returned
// *CongestionError. Without, Send writes the batches itself.
func (t *Unicast) Send(reliable bool, msgs ...Encoder) error {
	if t.closed.Load() {
		return ErrClosed
	}
	if t.cfg.TxQueue.Mode == TxQueued && !t.lowLatency {
		links := t.linkList()
		if len(links) == 0 {
			return ErrClosed
		}
		return t.sendQueued(links, reliable, msgs)
	}
	t.txMu.Lock()
	defer t.txMu.Unlock()
	links := t.linkList()
	if len(links) == 0 {
		return ErrClosed
	}
	if t.lowLatency {
		if st := links[0].stats; st != nil {
			for _, m := range msgs {
				st.addNetwork(Tx, m)
			}
		}
		return t.sendLowLatencyLocked(links[0], msgs)
	}

	p := &t.txPayload
	p.Reset()
	var cur *tlink
	prio := protocol.PriorityData
	capacity := 0
	for _, m := range msgs {
		mp := t.txPriority(m)
		ml := links[0]
		if len(links) > 1 {
			ml = selectLink(links, reliable, mp)
		}
		if ml != cur || mp != prio {
			if p.Len() > 0 {
				if err := t.sendFrameLocked(cur, prio, reliable, p.Bytes()); err != nil {
					return err
				}
				p.Reset()
			}
			cur, prio, capacity = ml, mp, t.frameCapacity(ml)
		}
		cur.stats.addNetwork(Tx, m)
		mark := p.Len()
		m.Encode(p)
		if p.Len() <= capacity {
			continue
		}
		// This message overflowed the current frame.
		over := append([]byte(nil), p.Bytes()[mark:]...)
		p.Truncate(mark)
		if mark > 0 {
			if err := t.sendFrameLocked(cur, prio, reliable, p.Bytes()); err != nil {
				return err
			}
			p.Reset()
		}
		if len(over) <= capacity {
			p.WriteRaw(over)
			continue
		}
		if err := t.sendFragmentsLocked(cur, prio, reliable, over, capacity); err != nil {
			return err
		}
	}
	if p.Len() > 0 {
		return t.sendFrameLocked(cur, prio, reliable, p.Bytes())
	}
	return nil
}

// setStreamPrioLocked picks the per-priority stream for the batch being
// built: its priority with QoS, Control without (as zenoh-rust, whose
// single non-QoS queue is index 0).
func (t *Unicast) setStreamPrioLocked(prio protocol.Priority) {
	if t.qos {
		t.txPrio = prio
	}
}

func (t *Unicast) sendFrameLocked(l *tlink, prio protocol.Priority, reliable bool, payload []byte) error {
	t.startBatchLocked(l)
	t.setStreamPrioLocked(prio)
	f := protocol.Frame{Reliable: reliable, SN: t.nextSNLocked(prio, reliable), Exts: qosExts(prio), Payload: payload}
	f.Encode(&t.txBatch)
	return t.flushBatchLocked(l)
}

func (t *Unicast) sendFragmentsLocked(l *tlink, prio protocol.Priority, reliable bool, msg []byte, capacity int) error {
	exts := qosExts(prio)
	first := t.patch >= patchFragmentMarkers
	for len(msg) > 0 {
		fexts, fcap := exts, capacity
		if first {
			// Patched peers get the start of each message marked, so a
			// receiver that lost the tail of the previous one resyncs.
			fexts = append(append([]protocol.Extension(nil), exts...),
				protocol.Extension{ID: extFragFirst, Enc: protocol.ExtUnit})
			fcap--
			first = false
		}
		n := min(fcap, len(msg))
		t.startBatchLocked(l)
		t.setStreamPrioLocked(prio)
		f := protocol.Fragment{
			Reliable: reliable,
			More:     n < len(msg),
			SN:       t.nextSNLocked(prio, reliable),
			Exts:     fexts,
			Payload:  msg[:n],
		}
		f.Encode(&t.txBatch)
		if err := t.flushBatchLocked(l); err != nil {
			return err
		}
		msg = msg[n:]
	}
	return nil
}

// -----------------------------------------------------------------------------
// Receive
// -----------------------------------------------------------------------------

// rxLoop receives batches from r (the link, or one of its per-priority
// streams) into buf.
func (t *Unicast) rxLoop(l *tlink, r io.Reader, buf []byte) {
	if rxBuffered && l.prefix > 0 && !l.lowLatency {
		r = bufio.NewReaderSize(r, len(buf)+l.prefix)
	}
	var zbuf []byte
	for {
		batch, err := l.readBatch(r, buf)
		if err == nil {
			l.lastRx.Store(time.Now().UnixNano())
			l.stats.addBytes(Rx, l.prefix+len(batch))
			if l.compress {
				batch, err = l.decompress(batch, &zbuf)
			}
		}
		if err == nil {
			t.rxMu.Lock()
			t.rxLink = l
			if l.lowLatency {
				err = t.handleLowLatency(batch)
			} else {
				err = t.handleBatch(batch)
			}
			t.rxMu.Unlock()
		}
		if err != nil {
			t.dropLink(l, err)
			return
		}
	}
}

func (t *Unicast) handleBatch(batch []byte) error {
	r := codec.NewReader(batch)
	for r.Remaining() > 0 {
		hdr, _ := r.PeekByte()
		t.rxStats().addTransport(Rx)
		switch codec.MsgID(hdr) {
		case protocol.IDFrame:
			if err := t.rx.frame(r); err != nil {
				return err
			}
		case protocol.IDFragment:
			m, err := protocol.DecodeTransport(r) // a FRAGMENT runs to the end of the batch
			if err != nil {
				return err
			}
			if err := t.rx.fragment(m.(*protocol.Fragment)); err != nil {
				return err
			}
		case protocol.IDKeepAlive:
			if _, err := protocol.DecodeTransport(r); err != nil {
				return err
			}
		case protocol.IDClose:
			m, err := protocol.DecodeTransport(r)
			if err != nil {
				return err
			}
			c := m.(*protocol.Close)
			if !c.Session {
				return fmt.Errorf("%w (reason %#x)", errLinkClosed, c.Reason)
			}
			return fmt.Errorf("%w (reason %#x)", ErrPeerClosed, c.Reason)
		default:
			return fmt.Errorf("transport: unexpected message id %#x", codec.MsgID(hdr))
		}
	}
	return nil
}

// rxStats are the counters of the link being read (nil if none).
func (t *Unicast) rxStats() *LinkStats {
	if t.rxLink == nil {
		return nil
	}
	return t.rxLink.stats
}

func (t *Unicast) deliver(m protocol.Message, reliable bool) error {
	t.rxStats().addNetwork(Rx, m)
	if t.cb.OnMessage == nil {
		return nil
	}
	return t.cb.OnMessage(m, reliable)
}

// -----------------------------------------------------------------------------
// Keep-alive, lease, shutdown
// -----------------------------------------------------------------------------

func (t *Unicast) keepAliveLoop(l *tlink) {
	interval := t.cfg.Lease / time.Duration(t.cfg.KeepAliveDivisor)
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		select {
		case <-l.done:
			return
		case now := <-tick.C:
			if l.peerLease > 0 && now.Sub(time.Unix(0, l.lastRx.Load())) > l.peerLease {
				t.dropLink(l, ErrLeaseExpired)
				return
			}
			if now.Sub(time.Unix(0, l.lastTx.Load())) >= interval {
				var err error
				if l.txq != nil {
					// A full queue is being written: no keep-alive needed.
					_, err = l.txq.pushTransport(&protocol.KeepAlive{}, 0)
				} else {
					err = t.writeTransport(l, &protocol.KeepAlive{})
				}
				if err != nil {
					t.dropLink(l, err)
					return
				}
			}
		}
	}
}

// dropLink takes a failed link out of the transport, which shuts down with
// its last link or when the peer closes the whole session.
func (t *Unicast) dropLink(l *tlink, cause error) {
	t.lmu.Lock()
	i := -1
	for k, x := range t.links {
		if x == l {
			i = k
		}
	}
	if i < 0 {
		t.lmu.Unlock()
		l.close()
		return
	}
	if len(t.links) == 1 || errors.Is(cause, ErrPeerClosed) {
		t.lmu.Unlock()
		if errors.Is(cause, errLinkClosed) {
			cause = fmt.Errorf("%w: %w", ErrPeerClosed, cause)
		}
		t.shutdown(cause)
		return
	}
	t.links = append(t.links[:i:i], t.links[i+1:]...)
	t.lmu.Unlock()
	if !errors.Is(cause, errLinkClosed) {
		t.sendClose(l, &protocol.Close{Reason: closeCause(cause)}, t.closeWait())
	}
	l.close()
}

func closeCause(cause error) byte {
	if errors.Is(cause, ErrLeaseExpired) {
		return protocol.CloseExpired
	}
	return protocol.CloseGeneric
}

func (t *Unicast) shutdown(cause error) {
	t.closeOnce.Do(func() {
		t.closed.Store(true)
		t.lmu.Lock()
		links := t.links
		t.links = nil
		t.lmu.Unlock()
		if !errors.Is(cause, ErrPeerClosed) {
			var wg sync.WaitGroup
			for _, l := range links {
				wg.Add(1)
				go func() {
					defer wg.Done()
					t.sendClose(l, &protocol.Close{Session: true, Reason: closeCause(cause)}, t.closeWait())
				}()
			}
			wg.Wait()
		}
		close(t.done)
		for _, l := range links {
			l.close()
		}
		if t.cb.OnClose != nil {
			t.cb.OnClose(cause)
		}
		if t.ext != nil {
			t.ext.Close()
		}
	})
}

// -----------------------------------------------------------------------------
// helpers
// -----------------------------------------------------------------------------

// snMask returns the mask of valid sequence numbers for a nominal resolution,
// mirroring zenoh-rust's get_mask (io/zenoh-transport/src/common/seq_num.rs):
// the SN must fit in (bits/8) VLE bytes, so 8→0x7F, 16→0x3FFF, 32→0x0FFFFFFF.
// For 64 the Rust value is u64::MAX>>1 truncated to its u32 TransportSn, i.e.
// 0xFFFFFFFF.
//
// Verified against zenohd 1.10.2 ("32bit"): initial SN 0x10000000 is accepted at
// OPEN (the router validates initial_sn-1) but 0x10000001 is rejected.
func snMask(bits uint) uint64 {
	switch bits {
	case 8:
		return 0x7f
	case 16:
		return 0x3fff
	case 32:
		return 0x0fffffff
	default:
		return 0xffffffff
	}
}

// vleLen returns the encoded size of v as a Zenoh VLE integer.
func vleLen(v uint64) int {
	n := 1
	for v >= 0x80 && n < 9 {
		v >>= 7
		n++
	}
	return n
}

// LinkInfo describes one link of a transport.
type LinkInfo struct {
	Link     Link
	Streamed bool // a byte stream (length-prefixed batches), not packets
	Reliable bool
	MTU      int // the link's packet size limit, 0 for streams
	// Stats are the link's traffic counters (nil without Config.Stats).
	Stats *LinkStats
	// HasRange: the link carries only priorities PrioLo..PrioHi (QUIC
	// per-priority links).
	HasRange       bool
	PrioLo, PrioHi protocol.Priority
}

// LinkInfos describes the links the transport runs over.
func (t *Unicast) LinkInfos() []LinkInfo {
	t.lmu.Lock()
	defer t.lmu.Unlock()
	out := make([]LinkInfo, 0, len(t.links))
	for _, l := range t.links {
		li := LinkInfo{Link: l.link, Streamed: l.prefix > 0, Reliable: l.reliable,
			HasRange: l.hasRange, PrioLo: l.prioLo, PrioHi: l.prioHi, Stats: l.stats}
		if p, ok := l.link.(PacketLink); ok {
			li.MTU = p.MTU()
		}
		out = append(out, li)
	}
	return out
}
