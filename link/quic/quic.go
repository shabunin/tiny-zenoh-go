//go:build !tinygo

package quic

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	quicgo "github.com/quic-go/quic-go"

	"github.com/shabunin/gozenoh/link"
	"github.com/shabunin/gozenoh/protocol"
	"github.com/shabunin/gozenoh/transport"
)

// ALPN protocols (zenoh-link-commons quic/utils.rs).
const (
	alpnLegacy        = "hq-29"
	alpnSingle        = "zenoh"
	alpnMixedRel      = "zenoh-mr"
	alpnMultiStream   = "zenoh-ms"
	alpnMultiMixedRel = "zenoh-ms-mr"
)

const (
	numPrio = 8
	// mixedRelMTU is the batch size on the datagram companion of a mixed
	// reliability link (zenoh-link-quic_datagram MIXED_RELIABILITY_MTU).
	mixedRelMTU = 1150
	// datagramMTU caps batches on a datagram-only link: zenoh-rust uses the
	// connection's current max datagram size, which starts around 1200.
	datagramMTU = 1150
)

func init() {
	link.Register("quic", link.Protocol{Dial: dial, Listen: listen})
}

// setting is a three-way locator option: "0", "1" or "auto".
type setting int

const (
	off setting = iota
	on
	auto
)

func parseSetting(v string, def setting) (setting, error) {
	switch v {
	case "":
		return def, nil
	case "0":
		return off, nil
	case "1":
		return on, nil
	case "auto":
		return auto, nil
	}
	return 0, errors.New("quic: invalid setting " + v)
}

// options are the locator's metadata as zenoh-rust reads it: "rel=0" makes
// a datagram-only link (zenoh-link-quic_datagram); "multistream" (default
// auto) and "mixed_rel" (default 0) select the stream variants through ALPN.
type options struct {
	datagram         bool
	multi, mixed     setting
	alpn             []string
	maxUni           int64
	datagramsEnabled bool
}

func parseOptions(l link.Locator) (options, error) {
	md := l.MetadataValues()
	var o options
	var err error
	if o.multi, err = parseSetting(md["multistream"], auto); err != nil {
		return o, err
	}
	if o.mixed, err = parseSetting(md["mixed_rel"], off); err != nil {
		return o, err
	}
	switch md["rel"] {
	case "", "1":
	case "0":
		o.datagram = true
	default:
		return o, errors.New("quic: invalid rel " + md["rel"])
	}
	if o.datagram {
		o.alpn = []string{alpnLegacy}
		o.maxUni = -1
		o.datagramsEnabled = true
		return o, nil
	}
	o.alpn = alpnProtocols(o.multi, o.mixed)
	o.maxUni = -1
	if o.multi != off {
		o.maxUni = numPrio - 1
	}
	o.datagramsEnabled = o.mixed != off
	return o, nil
}

// alpnProtocols lists the variants we accept, preferred first
// (compute_alpn_protocols in zenoh-link-commons quic/unicast.rs).
func alpnProtocols(ms, mr setting) []string {
	var p []string
	if ms != off && mr != off {
		p = append(p, alpnMultiMixedRel)
	}
	if ms != off && mr != on {
		p = append(p, alpnMultiStream)
	}
	if ms != on && mr != off {
		p = append(p, alpnMixedRel)
	}
	if ms != on && mr != on {
		p = append(p, alpnSingle, alpnLegacy)
	}
	return p
}

func (o options) quicConfig() *quicgo.Config {
	bidi := int64(1)
	if o.datagram {
		bidi = -1
	}
	return &quicgo.Config{
		MaxIncomingStreams:    bidi,
		MaxIncomingUniStreams: o.maxUni,
		EnableDatagrams:       o.datagramsEnabled,
		MaxIdleTimeout:        60 * time.Second, // zenoh's own lease is the real liveness check
		KeepAlivePeriod:       10 * time.Second,
	}
}

// conn is a stream-based zenoh link over one QUIC connection: the
// bidirectional Control stream, plus, with multistream, one unidirectional
// stream per other priority each way, and, with mixed reliability, a
// datagram companion for best-effort traffic.
type conn struct {
	*quicgo.Stream
	qc   *quicgo.Conn
	once sync.Once

	multi bool
	send  [numPrio]*quicgo.SendStream
	recv  [numPrio]*prioReader
	dg    *dgram
}

var (
	_ transport.PriorityLink         = (*conn)(nil)
	_ transport.MixedReliabilityLink = (*conn)(nil)
)

// newConn sets up the variant negotiated through ALPN on a connection whose
// Control stream is st.
func newConn(qc *quicgo.Conn, st *quicgo.Stream) (*conn, error) {
	c := &conn{Stream: st, qc: qc}
	alpn := qc.ConnectionState().TLS.NegotiatedProtocol
	if alpn == alpnMultiStream || alpn == alpnMultiMixedRel {
		c.multi = true
		// Streams are numbered in the order they are opened, and both sides
		// map uni stream n (from 0) to priority n+1.
		for p := 1; p < numPrio; p++ {
			s, err := qc.OpenUniStream()
			if err != nil {
				return nil, err
			}
			c.send[p] = s
		}
		accepted := make([]chan *quicgo.ReceiveStream, numPrio)
		for p := 1; p < numPrio; p++ {
			accepted[p] = make(chan *quicgo.ReceiveStream, 1)
			c.recv[p] = &prioReader{qc: qc, ch: accepted[p]}
		}
		go acceptUni(qc, accepted)
	}
	if alpn == alpnMixedRel || alpn == alpnMultiMixedRel {
		ctx, cancel := context.WithCancel(qc.Context())
		c.dg = &dgram{qc: qc, ctx: ctx, cancel: cancel, mtu: mixedRelMTU}
	}
	return c, nil
}

// acceptUni hands the peer's priority streams to their readers as they
// arrive (a stream shows up when its first data does).
func acceptUni(qc *quicgo.Conn, accepted []chan *quicgo.ReceiveStream) {
	for n := 1; n < numPrio; n++ {
		s, err := qc.AcceptUniStream(qc.Context())
		if err != nil {
			return
		}
		if p := int(s.StreamID().StreamNum()); p > 0 && p < numPrio {
			accepted[p] <- s
		}
	}
}

// prioReader reads one priority's incoming stream once the peer opened it.
type prioReader struct {
	qc *quicgo.Conn
	ch chan *quicgo.ReceiveStream
	s  *quicgo.ReceiveStream
}

func (r *prioReader) Read(b []byte) (int, error) {
	if r.s == nil {
		select {
		case r.s = <-r.ch:
		case <-r.qc.Context().Done():
			return 0, io.ErrClosedPipe
		}
	}
	return r.s.Read(b)
}

func (c *conn) PriorityStreams() bool { return c.multi }

func (c *conn) PriorityWriter(p protocol.Priority) io.Writer {
	if p == 0 || !c.multi {
		return c.Stream
	}
	return c.send[p]
}

func (c *conn) PriorityReader(p protocol.Priority) io.Reader {
	if p == 0 || !c.multi {
		return c.Stream
	}
	return c.recv[p]
}

func (c *conn) BestEffort() transport.PacketLink {
	if c.dg == nil {
		return nil
	}
	return c.dg
}

func (c *conn) Close() error {
	c.once.Do(func() {
		_ = c.Stream.Close()
		_ = c.qc.CloseWithError(0, "")
	})
	return nil
}

// dgram carries one batch per QUIC datagram: a datagram-only link (own:
// closing it closes the connection) or a mixed-reliability companion.
type dgram struct {
	qc     *quicgo.Conn
	ctx    context.Context
	cancel context.CancelFunc
	mtu    int
	own    bool
}

var _ transport.PacketLink = (*dgram)(nil)

func (d *dgram) MTU() int { return d.mtu }

func (d *dgram) Read(b []byte) (int, error) {
	p, err := d.qc.ReceiveDatagram(d.ctx)
	if err != nil {
		return 0, err
	}
	if len(p) > len(b) {
		return 0, transport.ErrBatchLength
	}
	return copy(b, p), nil
}

// Write sends b as one datagram; one too large for the path right now is
// dropped, as any best-effort packet may be.
func (d *dgram) Write(b []byte) (int, error) {
	err := d.qc.SendDatagram(b)
	var tooLarge *quicgo.DatagramTooLargeError
	if errors.As(err, &tooLarge) {
		return len(b), nil
	}
	if err != nil {
		return 0, err
	}
	return len(b), nil
}

func (d *dgram) Close() error {
	d.cancel()
	if d.own {
		_ = d.qc.CloseWithError(0, "")
	}
	return nil
}

func newDatagramLink(qc *quicgo.Conn) *dgram {
	ctx, cancel := context.WithCancel(qc.Context())
	return &dgram{qc: qc, ctx: ctx, cancel: cancel, mtu: datagramMTU, own: true}
}

func dial(l link.Locator, timeout time.Duration, o link.Options) (transport.Link, error) {
	opts, err := parseOptions(l)
	if err != nil {
		return nil, err
	}
	tc, err := link.TLSClientConfig(l, o.TLS)
	if err != nil {
		return nil, err
	}
	tc.NextProtos = opts.alpn
	tc.MinVersion = 0x0304 // QUIC requires TLS 1.3
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	qc, err := quicgo.DialAddr(ctx, l.Address, tc, opts.quicConfig())
	if err != nil {
		return nil, err
	}
	if opts.datagram {
		return newDatagramLink(qc), nil
	}
	st, err := qc.OpenStreamSync(ctx)
	if err == nil {
		var c *conn
		if c, err = newConn(qc, st); err == nil {
			return c, nil
		}
	}
	_ = qc.CloseWithError(0, "")
	return nil, err
}

type listener struct {
	ln     *quicgo.Listener
	loc    link.Locator
	opts   options
	ready  chan transport.Link
	ctx    context.Context
	cancel context.CancelFunc
}

func listen(l link.Locator, o link.Options) (link.Listener, error) {
	opts, err := parseOptions(l)
	if err != nil {
		return nil, err
	}
	tc, err := link.TLSServerConfig(l, o.TLS)
	if err != nil {
		return nil, err
	}
	tc.NextProtos = opts.alpn
	tc.MinVersion = 0x0304
	ln, err := quicgo.ListenAddr(l.Address, tc, opts.quicConfig())
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	q := &listener{ln: ln, opts: opts, ready: make(chan transport.Link), ctx: ctx, cancel: cancel,
		loc: link.Locator{Protocol: l.Protocol, Address: ln.Addr().String(), Metadata: l.Metadata}}
	go q.acceptLoop()
	return q, nil
}

// acceptLoop accepts connections and waits, per connection, for the peer to
// open its Control stream (which happens when it sends INIT), so one slow
// peer doesn't hold up the others.
func (q *listener) acceptLoop() {
	for {
		qc, err := q.ln.Accept(q.ctx)
		if err != nil {
			q.cancel()
			return
		}
		go func() {
			var l transport.Link
			if q.opts.datagram {
				l = newDatagramLink(qc)
			} else {
				ctx, cancel := context.WithTimeout(q.ctx, 10*time.Second)
				defer cancel()
				st, err := qc.AcceptStream(ctx)
				if err != nil {
					_ = qc.CloseWithError(0, "")
					return
				}
				c, err := newConn(qc, st)
				if err != nil {
					_ = qc.CloseWithError(0, "")
					return
				}
				l = c
			}
			select {
			case q.ready <- l:
			case <-q.ctx.Done():
				_ = l.Close()
			}
		}()
	}
}

var errClosed = errors.New("quic: listener closed")

func (q *listener) Accept() (transport.Link, error) {
	select {
	case c := <-q.ready:
		return c, nil
	case <-q.ctx.Done():
		return nil, errClosed
	}
}

func (q *listener) Close() error {
	q.cancel()
	return q.ln.Close()
}

func (q *listener) Locator() link.Locator { return q.loc }
