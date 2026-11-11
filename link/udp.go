package link

import (
	"errors"
	"net"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shabunin/tiny-zenoh-go/transport"
)

// UDP links (zenoh-rust io/zenoh-links/zenoh-link-udp): one batch per
// datagram, no length prefix, best effort. A listener shares one socket among
// all its remote endpoints and tells them apart by source address.

// udpMTU mirrors zenoh-rust's UDP_DEFAULT_MTU: 65535 minus the UDP and IPv6
// headers where the OS allows that much, smaller elsewhere.
func udpMTU() int {
	switch runtime.GOOS {
	case "linux", "windows":
		return 0xffff - 8 - 40
	case "darwin", "ios":
		return 9216
	default:
		return 8192
	}
}

// udpSockBuf is the socket buffer we ask for, so a burst of fragments
// (each up to 64 KiB) isn't dropped by the kernel; the OS may grant less.
const udpSockBuf = 4 << 20

// udpQueue bounds the datagrams waiting for a listener-side link's reader;
// beyond it datagrams are dropped, as the network would.
const udpQueue = 256

var errUDPClosed = errors.New("link: udp link closed")

// udpDialed is a connected UDP socket.
type udpDialed struct{ *net.UDPConn }

func (c udpDialed) MTU() int { return udpMTU() }

var _ transport.PacketLink = udpDialed{}

func dialUDP(l Locator) (transport.Link, error) {
	addr, err := net.ResolveUDPAddr("udp", l.Address)
	if err != nil {
		return nil, err
	}
	c, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		return nil, err
	}
	_ = c.SetReadBuffer(udpSockBuf)
	_ = c.SetWriteBuffer(udpSockBuf)
	return udpDialed{c}, nil
}

type udpListener struct {
	pc     *net.UDPConn
	loc    Locator
	accept chan *udpLink
	done   chan struct{}
	once   sync.Once

	mu    sync.Mutex
	links map[string]*udpLink
}

// errNoUDPListen is returned by UDP listeners in TinyGo builds.
var errNoUDPListen = errors.New("link: udp listeners are not available in TinyGo builds")

func listenUDP(l Locator, bound Locator) (Listener, error) {
	if !udpCanListen {
		return nil, errNoUDPListen
	}
	addr, err := net.ResolveUDPAddr("udp", l.Address)
	if err != nil {
		return nil, err
	}
	pc, err := net.ListenUDP("udp", addr)
	if err != nil {
		return nil, err
	}
	_ = pc.SetReadBuffer(udpSockBuf)
	_ = pc.SetWriteBuffer(udpSockBuf)
	bound.Address = pc.LocalAddr().String()
	ln := &udpListener{pc: pc, loc: bound, accept: make(chan *udpLink, 16),
		done: make(chan struct{}), links: map[string]*udpLink{}}
	go ln.readLoop()
	return ln, nil
}

func (ln *udpListener) readLoop() {
	buf := make([]byte, 0xffff)
	for {
		n, addr, err := ln.pc.ReadFromUDP(buf)
		if err != nil {
			select {
			case <-ln.done:
				return
			default:
			}
			if errors.Is(err, net.ErrClosed) {
				_ = ln.Close()
				return
			}
			time.Sleep(100 * time.Millisecond) // zenoh-rust's accept throttle
			continue
		}
		key := addr.String()
		ln.mu.Lock()
		c := ln.links[key]
		fresh := c == nil
		if fresh {
			c = &udpLink{ln: ln, addr: addr, key: key, in: make(chan []byte, udpQueue), done: make(chan struct{})}
			ln.links[key] = c
		}
		ln.mu.Unlock()
		if fresh {
			select {
			case ln.accept <- c:
			case <-ln.done:
				return
			}
		}
		pkt := append([]byte(nil), buf[:n]...)
		select {
		case c.in <- pkt:
		default: // reader too slow: drop
		}
	}
}

func (ln *udpListener) Accept() (transport.Link, error) {
	select {
	case c := <-ln.accept:
		return c, nil
	case <-ln.done:
		return nil, net.ErrClosed
	}
}

func (ln *udpListener) Close() error {
	ln.once.Do(func() {
		close(ln.done)
		_ = ln.pc.Close()
		ln.mu.Lock()
		links := ln.links
		ln.links = map[string]*udpLink{}
		ln.mu.Unlock()
		for _, c := range links {
			c.shut()
		}
	})
	return nil
}

func (ln *udpListener) Locator() Locator { return ln.loc }

// udpLink is the listener side of one remote UDP endpoint.
type udpLink struct {
	ln       *udpListener
	addr     *net.UDPAddr
	key      string
	in       chan []byte
	done     chan struct{}
	once     sync.Once
	deadline atomic.Int64 // read deadline, unix nanos; 0: none
}

var _ transport.PacketLink = (*udpLink)(nil)

func (c *udpLink) MTU() int { return udpMTU() }

func (c *udpLink) Read(b []byte) (int, error) {
	var timeout <-chan time.Time
	if d := c.deadline.Load(); d != 0 {
		wait := time.Until(time.Unix(0, d))
		if wait <= 0 {
			return 0, os.ErrDeadlineExceeded
		}
		t := time.NewTimer(wait)
		defer t.Stop()
		timeout = t.C
	}
	select {
	case p := <-c.in:
		return copy(b, p), nil
	case <-c.done:
		return 0, errUDPClosed
	case <-timeout:
		return 0, os.ErrDeadlineExceeded
	}
}

func (c *udpLink) Write(b []byte) (int, error) {
	select {
	case <-c.done:
		return 0, errUDPClosed
	default:
	}
	return c.ln.pc.WriteToUDP(b, c.addr)
}

// SetDeadline bounds reads (the transport uses it during the handshake).
func (c *udpLink) SetDeadline(t time.Time) error {
	if t.IsZero() {
		c.deadline.Store(0)
	} else {
		c.deadline.Store(t.UnixNano())
	}
	return nil
}

func (c *udpLink) shut() { c.once.Do(func() { close(c.done) }) }

// Close forgets the endpoint; a later datagram from it starts a new link.
func (c *udpLink) Close() error {
	c.ln.mu.Lock()
	if c.ln.links[c.key] == c {
		delete(c.ln.links, c.key)
	}
	c.ln.mu.Unlock()
	c.shut()
	return nil
}
