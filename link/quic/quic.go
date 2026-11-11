//go:build !tinygo

package quic

import (
	"context"
	"errors"
	"sync"
	"time"

	quicgo "github.com/quic-go/quic-go"

	"github.com/shabunin/gozenoh/link"
	"github.com/shabunin/gozenoh/transport"
)

// ALPN protocols, in our order of preference (zenoh-link-commons quic/utils.rs).
var alpn = []string{"zenoh", "hq-29"}

func init() {
	link.Register("quic", link.Protocol{Dial: dial, Listen: listen})
}

func quicConfig() *quicgo.Config {
	return &quicgo.Config{
		MaxIncomingStreams:    1, // zenoh uses a single bidirectional stream
		MaxIncomingUniStreams: -1,
		MaxIdleTimeout:        60 * time.Second, // zenoh's own lease is the real liveness check
		KeepAlivePeriod:       10 * time.Second,
	}
}

// conn is a zenoh link: one stream of one QUIC connection.
type conn struct {
	*quicgo.Stream
	qc   *quicgo.Conn
	once sync.Once
}

var _ transport.Link = (*conn)(nil)

func (c *conn) Close() error {
	c.once.Do(func() {
		_ = c.Stream.Close()
		_ = c.qc.CloseWithError(0, "")
	})
	return nil
}

func dial(l link.Locator, timeout time.Duration, o link.Options) (transport.Link, error) {
	tc, err := link.TLSClientConfig(l, o.TLS)
	if err != nil {
		return nil, err
	}
	tc.NextProtos = alpn
	tc.MinVersion = 0x0304 // QUIC requires TLS 1.3
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	qc, err := quicgo.DialAddr(ctx, l.Address, tc, quicConfig())
	if err != nil {
		return nil, err
	}
	st, err := qc.OpenStreamSync(ctx)
	if err != nil {
		_ = qc.CloseWithError(0, "")
		return nil, err
	}
	return &conn{Stream: st, qc: qc}, nil
}

type listener struct {
	ln     *quicgo.Listener
	loc    link.Locator
	ready  chan *conn
	ctx    context.Context
	cancel context.CancelFunc
}

func listen(l link.Locator, o link.Options) (link.Listener, error) {
	tc, err := link.TLSServerConfig(l, o.TLS)
	if err != nil {
		return nil, err
	}
	tc.NextProtos = alpn
	tc.MinVersion = 0x0304
	ln, err := quicgo.ListenAddr(l.Address, tc, quicConfig())
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	q := &listener{ln: ln, ready: make(chan *conn), ctx: ctx, cancel: cancel,
		loc: link.Locator{Protocol: l.Protocol, Address: ln.Addr().String(), Metadata: l.Metadata}}
	go q.acceptLoop()
	return q, nil
}

// acceptLoop accepts connections and waits, per connection, for the peer to
// open its stream (which happens when it sends INIT), so one slow peer
// doesn't hold up the others.
func (q *listener) acceptLoop() {
	for {
		qc, err := q.ln.Accept(q.ctx)
		if err != nil {
			q.cancel()
			return
		}
		go func() {
			ctx, cancel := context.WithTimeout(q.ctx, 10*time.Second)
			defer cancel()
			st, err := qc.AcceptStream(ctx)
			if err != nil {
				_ = qc.CloseWithError(0, "")
				return
			}
			c := &conn{Stream: st, qc: qc}
			select {
			case q.ready <- c:
			case <-q.ctx.Done():
				_ = c.Close()
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
