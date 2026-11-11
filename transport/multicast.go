package transport

import (
	"errors"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shabunin/tiny-zenoh-go/internal/codec"
	"github.com/shabunin/tiny-zenoh-go/protocol"
)

// Multicast transport (zenoh-rust io/zenoh-transport/src/multicast). Every
// node on a multicast group sends its batches to the whole group; there is
// no handshake. Instead each node sends a JOIN every JoinInterval with its
// identity, parameters, lease and the next SN of each channel. A receiver
// ignores a sender until it has seen its JOIN, then keeps one receive state
// (SN windows, fragments) per sender, and forgets it when it is silent for
// its lease or says CLOSE. Members must agree on SN resolution and batch
// size (default 8192); QoS is optional (zenoh-rust's default: off).

// MulticastLink is a datagram link bound to a multicast group.
type MulticastLink interface {
	// ReadFrom reads one datagram sent to the group by another node; src
	// identifies the sender (its unicast address). Our own datagrams, looped
	// back by the network stack, must be skipped.
	ReadFrom(b []byte) (n int, src string, err error)
	// Write sends one datagram to the group.
	Write(b []byte) (int, error)
	Close() error
	// MTU is the largest datagram the link sends.
	MTU() int
}

// MulticastConfig parameterizes a multicast transport. Zero fields take
// defaults.
type MulticastConfig struct {
	ZID     protocol.ZID // required
	WhatAmI protocol.WhatAmI
	// Lease announced in JOIN: how long the others keep us without news.
	// Default 10s.
	Lease time.Duration
	// JoinInterval between JOINs (they double as keep-alives). Default 2.5s.
	JoinInterval time.Duration
	// BatchSize is the largest batch; all members must use the same.
	// Default 8192 (capped by the link MTU).
	BatchSize uint16
	// QoS: per-priority channels, as for unicast. All members should agree.
	QoS bool
	// MaxPeers bounds the remote nodes tracked. Default 1000.
	MaxPeers int
	// MaxMessageSize bounds a reassembled message. Default 16 MiB.
	MaxMessageSize int
	// Stats keeps traffic counters for the group (Multicast.Stats).
	Stats bool
}

// MulticastPeer is a remote node seen on the group.
type MulticastPeer struct {
	ZID     protocol.ZID
	WhatAmI protocol.WhatAmI
	Addr    string // the sender address its datagrams come from
	QoS     bool
	Lease   time.Duration
}

// MulticastCallbacks receive the group's traffic, from the receive goroutine.
type MulticastCallbacks struct {
	// OnPeer is called when a node's first JOIN is accepted.
	OnPeer func(p *MulticastPeer)
	// OnMessage is called for each network message from a known node.
	OnMessage func(p *MulticastPeer, m protocol.Message, reliable bool)
	// OnPeerGone is called when a node leaves (CLOSE) or its lease expires.
	OnPeerGone func(p *MulticastPeer, err error)
	// OnClose is called once when the transport stops (nil err: Close).
	OnClose func(err error)
}

type mcastPeer struct {
	info   MulticastPeer
	join   protocol.Join
	lastRx atomic.Int64
	rx     rxState
}

// Multicast is a multicast transport on one group.
type Multicast struct {
	link   MulticastLink
	cfg    MulticastConfig
	cb     MulticastCallbacks
	snMask uint64

	txMu      sync.Mutex
	txSN      [numPrio][2]uint64
	txBatch   codec.Writer
	txPayload codec.Writer

	pmu   sync.Mutex
	peers map[string]*mcastPeer

	closed    atomic.Bool
	closeOnce sync.Once
	done      chan struct{}

	stats *LinkStats // nil without MulticastConfig.Stats
}

// Stats are the group's traffic counters, all senders together (nil
// without MulticastConfig.Stats).
func (m *Multicast) Stats() *LinkStats { return m.stats }

// NewMulticast starts a multicast transport on link: it sends a first JOIN
// and starts the receive and JOIN goroutines.
func NewMulticast(link MulticastLink, cfg MulticastConfig, cb MulticastCallbacks) (*Multicast, error) {
	if cfg.ZID.IsZero() {
		_ = link.Close()
		return nil, errors.New("transport: MulticastConfig.ZID is required")
	}
	if cfg.Lease == 0 {
		cfg.Lease = 10 * time.Second
	}
	if cfg.JoinInterval == 0 {
		cfg.JoinInterval = 2500 * time.Millisecond
	}
	if cfg.BatchSize == 0 {
		cfg.BatchSize = protocol.MulticastBatchSize
	}
	if mtu := link.MTU(); mtu > 0 && mtu < int(cfg.BatchSize) {
		cfg.BatchSize = uint16(mtu)
	}
	if cfg.MaxPeers == 0 {
		cfg.MaxPeers = 1000
	}
	if cfg.MaxMessageSize == 0 {
		cfg.MaxMessageSize = 16 << 20
	}
	m := &Multicast{
		link:   link,
		cfg:    cfg,
		cb:     cb,
		snMask: snMask(protocol.DefaultResolution.FrameSNBits()),
		peers:  map[string]*mcastPeer{},
		done:   make(chan struct{}),
	}
	if cfg.Stats {
		m.stats = &LinkStats{}
	}
	isn := rand.Uint64() & m.snMask
	for p := range m.txSN {
		m.txSN[p] = [2]uint64{isn, isn}
	}
	if err := m.sendJoin(); err != nil {
		_ = link.Close()
		return nil, err
	}
	go m.rxLoop()
	go m.joinLoop()
	return m, nil
}

// Done is closed when the transport has stopped.
func (m *Multicast) Done() <-chan struct{} { return m.done }

// Peers lists the nodes currently known on the group.
func (m *Multicast) Peers() []MulticastPeer {
	m.pmu.Lock()
	defer m.pmu.Unlock()
	out := make([]MulticastPeer, 0, len(m.peers))
	for _, p := range m.peers {
		out = append(out, p.info)
	}
	return out
}

// Close tells the group we leave and stops the transport.
func (m *Multicast) Close() error {
	m.shutdown(nil)
	return nil
}

func (m *Multicast) shutdown(cause error) {
	m.closeOnce.Do(func() {
		m.closed.Store(true)
		if cause == nil {
			_ = m.writeTransport(&protocol.Close{Session: true, Reason: protocol.CloseGeneric})
		}
		close(m.done)
		_ = m.link.Close()
		if m.cb.OnClose != nil {
			m.cb.OnClose(cause)
		}
	})
}

// -----------------------------------------------------------------------------
// Transmit
// -----------------------------------------------------------------------------

func (m *Multicast) writeTransport(msg protocol.Message) error {
	m.txMu.Lock()
	defer m.txMu.Unlock()
	m.txBatch.Reset()
	msg.Encode(&m.txBatch)
	return m.flushLocked()
}

func (m *Multicast) flushLocked() error {
	b := m.txBatch.Bytes()
	if len(b) > int(m.cfg.BatchSize) {
		return ErrBatchSize
	}
	_, err := m.link.Write(b)
	if err == nil {
		m.stats.addBytes(Tx, len(b))
		m.stats.addTransport(Tx)
	}
	return err
}

// sendJoin announces us with the next SN of every channel.
func (m *Multicast) sendJoin() error {
	m.txMu.Lock()
	j := &protocol.Join{
		Version:     protocol.ProtocolVersion,
		WhatAmI:     m.cfg.WhatAmI,
		ZID:         m.cfg.ZID,
		Resolution:  protocol.DefaultResolution,
		BatchSize:   m.cfg.BatchSize,
		LeaseMillis: uint64(m.cfg.Lease / time.Millisecond),
	}
	if m.cfg.QoS {
		var q [numPrio]protocol.PrioritySN
		for p := range q {
			q[p] = protocol.PrioritySN{Reliable: m.txSN[p][1], BestEffort: m.txSN[p][0]}
		}
		j.QoS = &q
	} else {
		d := m.txSN[protocol.PriorityData]
		j.NextSN = protocol.PrioritySN{Reliable: d[1], BestEffort: d[0]}
	}
	m.txBatch.Reset()
	j.Encode(&m.txBatch)
	err := m.flushLocked()
	m.txMu.Unlock()
	return err
}

func (m *Multicast) joinLoop() {
	tick := time.NewTicker(m.cfg.JoinInterval)
	defer tick.Stop()
	for {
		select {
		case <-m.done:
			return
		case now := <-tick.C:
			_ = m.sendJoin() // a lost datagram is no reason to stop
			m.expirePeers(now)
		}
	}
}

func (m *Multicast) nextSNLocked(prio protocol.Priority, reliable bool) uint64 {
	i := 0
	if reliable {
		i = 1
	}
	sn := m.txSN[prio][i]
	m.txSN[prio][i] = (sn + 1) & m.snMask
	return sn
}

// Send transmits network messages to the group, packed into FRAMEs (split
// into FRAGMENTs when larger than a batch) as on a unicast transport.
func (m *Multicast) Send(reliable bool, msgs ...Encoder) error {
	if m.closed.Load() {
		return ErrClosed
	}
	m.txMu.Lock()
	defer m.txMu.Unlock()
	capacity := int(m.cfg.BatchSize) - (1 + vleLen(m.snMask))
	if m.cfg.QoS {
		capacity -= 2
	}
	p := &m.txPayload
	p.Reset()
	prio := protocol.PriorityData
	for _, msg := range msgs {
		mp := protocol.PriorityData
		if m.cfg.QoS {
			mp = protocol.QoSOf(msg).Priority()
		}
		if mp != prio {
			if p.Len() > 0 {
				if err := m.sendFrameLocked(prio, reliable, p.Bytes()); err != nil {
					return err
				}
				p.Reset()
			}
			prio = mp
		}
		m.stats.addNetwork(Tx, msg)
		mark := p.Len()
		msg.Encode(p)
		if p.Len() <= capacity {
			continue
		}
		over := append([]byte(nil), p.Bytes()[mark:]...)
		p.Truncate(mark)
		if mark > 0 {
			if err := m.sendFrameLocked(prio, reliable, p.Bytes()); err != nil {
				return err
			}
			p.Reset()
		}
		if len(over) <= capacity {
			p.WriteRaw(over)
			continue
		}
		exts := qosExts(prio)
		for len(over) > 0 {
			n := min(capacity, len(over))
			m.txBatch.Reset()
			f := protocol.Fragment{Reliable: reliable, More: n < len(over),
				SN: m.nextSNLocked(prio, reliable), Exts: exts, Payload: over[:n]}
			f.Encode(&m.txBatch)
			if err := m.flushLocked(); err != nil {
				return err
			}
			over = over[n:]
		}
	}
	if p.Len() > 0 {
		return m.sendFrameLocked(prio, reliable, p.Bytes())
	}
	return nil
}

func (m *Multicast) sendFrameLocked(prio protocol.Priority, reliable bool, payload []byte) error {
	m.txBatch.Reset()
	f := protocol.Frame{Reliable: reliable, SN: m.nextSNLocked(prio, reliable), Exts: qosExts(prio), Payload: payload}
	f.Encode(&m.txBatch)
	return m.flushLocked()
}

// -----------------------------------------------------------------------------
// Receive
// -----------------------------------------------------------------------------

func (m *Multicast) rxLoop() {
	buf := make([]byte, 0xffff)
	for {
		n, src, err := m.link.ReadFrom(buf)
		if err != nil {
			if !m.closed.Load() {
				m.shutdown(err)
			}
			return
		}
		if n > 0 {
			m.stats.addBytes(Rx, n)
			m.handleBatch(src, buf[:n])
		}
	}
}

func (m *Multicast) peer(src string) *mcastPeer {
	m.pmu.Lock()
	defer m.pmu.Unlock()
	return m.peers[src]
}

func (m *Multicast) dropPeer(src string, p *mcastPeer, err error) {
	m.pmu.Lock()
	if m.peers[src] != p {
		m.pmu.Unlock()
		return
	}
	delete(m.peers, src)
	m.pmu.Unlock()
	if m.cb.OnPeerGone != nil {
		m.cb.OnPeerGone(&p.info, err)
	}
}

func (m *Multicast) expirePeers(now time.Time) {
	m.pmu.Lock()
	var gone []string
	for src, p := range m.peers {
		if now.Sub(time.Unix(0, p.lastRx.Load())) > p.info.Lease {
			gone = append(gone, src)
		}
	}
	m.pmu.Unlock()
	for _, src := range gone {
		if p := m.peer(src); p != nil {
			m.dropPeer(src, p, ErrLeaseExpired)
		}
	}
}

// handleBatch processes one datagram from src. Errors only affect that
// sender: its state is dropped and rebuilt from its next JOIN.
func (m *Multicast) handleBatch(src string, batch []byte) {
	p := m.peer(src)
	if p != nil {
		p.lastRx.Store(time.Now().UnixNano())
	}
	r := codec.NewReader(batch)
	for r.Remaining() > 0 {
		hdr, _ := r.PeekByte()
		var err error
		if codec.MsgID(hdr) <= protocol.IDJoin {
			m.stats.addTransport(Rx)
		}
		switch codec.MsgID(hdr) {
		case protocol.IDFrame:
			if p == nil {
				return // unknown sender: wait for its JOIN
			}
			err = p.rx.frame(r)
		case protocol.IDFragment:
			if p == nil {
				return
			}
			var msg protocol.Message
			if msg, err = protocol.DecodeTransport(r); err == nil {
				err = p.rx.fragment(msg.(*protocol.Fragment))
			}
		case protocol.IDJoin:
			var msg protocol.Message
			if msg, err = protocol.DecodeTransport(r); err == nil {
				p = m.handleJoin(src, p, msg.(*protocol.Join))
			}
		case protocol.IDKeepAlive:
			_, err = protocol.DecodeTransport(r)
		case protocol.IDClose:
			if _, err = protocol.DecodeTransport(r); err == nil && p != nil {
				m.dropPeer(src, p, ErrPeerClosed)
				return
			}
		default:
			return // nothing else belongs on a group
		}
		if err != nil {
			if p != nil {
				m.dropPeer(src, p, err)
			}
			return
		}
	}
}

// handleJoin admits a new sender or checks a known one's JOIN; it returns
// the sender's state (nil if not admitted).
func (m *Multicast) handleJoin(src string, p *mcastPeer, j *protocol.Join) *mcastPeer {
	if p != nil {
		if !j.ZID.Equal(p.join.ZID) || j.WhatAmI != p.join.WhatAmI || j.Version != p.join.Version ||
			j.Resolution != p.join.Resolution || j.LeaseMillis != p.join.LeaseMillis || (j.QoS != nil) != p.info.QoS {
			// Inconsistent: a new node at that address? Start over with it.
			m.dropPeer(src, p, errInvalid)
			return nil
		}
		return p
	}
	if j.Version != protocol.ProtocolVersion || j.Resolution != protocol.DefaultResolution ||
		j.BatchSize != m.cfg.BatchSize || (j.QoS != nil && !m.cfg.QoS) || j.ZID.Equal(m.cfg.ZID) {
		return nil
	}
	np := &mcastPeer{join: *j}
	np.info = MulticastPeer{ZID: j.ZID, WhatAmI: j.WhatAmI, Addr: src, QoS: j.QoS != nil,
		Lease: time.Duration(j.LeaseMillis) * time.Millisecond}
	np.lastRx.Store(time.Now().UnixNano())
	np.rx.init(snMask(j.Resolution.FrameSNBits()), j.QoS != nil, m.cfg.MaxMessageSize,
		func(msg protocol.Message, reliable bool) error {
			m.stats.addNetwork(Rx, msg)
			if m.cb.OnMessage != nil {
				m.cb.OnMessage(&np.info, msg, reliable)
			}
			return nil
		})
	for prio := range numPrio {
		next := j.NextSN
		if j.QoS != nil {
			next = j.QoS[prio]
		}
		np.rx.syncChannel(protocol.Priority(prio), next)
	}
	m.pmu.Lock()
	if len(m.peers) >= m.cfg.MaxPeers {
		m.pmu.Unlock()
		return nil
	}
	m.peers[src] = np
	m.pmu.Unlock()
	// Tell the newcomer about us now rather than at our next JOIN.
	_ = m.sendJoin()
	if m.cb.OnPeer != nil {
		m.cb.OnPeer(&np.info)
	}
	return np
}
