// Package link dials Zenoh locators ("proto/address[?metadata][#config]") and
// returns a transport.Link. It is kept separate from package transport so the
// transport core stays free of the "net" package.
package link

import (
	"errors"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/shabunin/gozenoh/transport"
)

// ErrUnsupportedProtocol is returned for a locator whose protocol has no dialer.
var ErrUnsupportedProtocol = errors.New("link: unsupported locator protocol")

// ErrBadLocator is returned for a malformed locator.
var ErrBadLocator = errors.New("link: malformed locator")

// Locator is a parsed Zenoh endpoint locator.
type Locator struct {
	Protocol string // e.g. "tcp"
	Address  string // e.g. "127.0.0.1:7447"
	Metadata string // text after '?', if any
	Config   string // text after '#', if any
}

// String renders the locator in Zenoh form.
func (l Locator) String() string {
	s := l.Protocol + "/" + l.Address
	if l.Metadata != "" {
		s += "?" + l.Metadata
	}
	if l.Config != "" {
		s += "#" + l.Config
	}
	return s
}

// Parse parses a locator such as "tcp/127.0.0.1:7447" or "tcp/[::1]:7447".
func Parse(s string) (Locator, error) {
	var l Locator
	if i := strings.IndexByte(s, '#'); i >= 0 {
		s, l.Config = s[:i], s[i+1:]
	}
	if i := strings.IndexByte(s, '?'); i >= 0 {
		s, l.Metadata = s[:i], s[i+1:]
	}
	proto, addr, ok := strings.Cut(s, "/")
	if !ok || proto == "" || addr == "" {
		return Locator{}, ErrBadLocator
	}
	l.Protocol, l.Address = proto, addr
	return l, nil
}

// Protocol is a link implementation registered with Register.
type Protocol struct {
	// Dial opens a link to l.
	Dial func(l Locator, timeout time.Duration, o Options) (transport.Link, error)
	// Listen listens on l. The returned Listener's Locator must carry the
	// bound address and no config part.
	Listen func(l Locator, o Options) (Listener, error)
}

var (
	protoMu sync.RWMutex
	protos  = map[string]Protocol{}
)

// Register makes a link protocol (e.g. "quic", from package link/quic)
// available to Dial and Listen. Built-in protocols can't be replaced.
func Register(name string, p Protocol) {
	protoMu.Lock()
	defer protoMu.Unlock()
	protos[name] = p
}

func registered(name string) (Protocol, bool) {
	protoMu.RLock()
	defer protoMu.RUnlock()
	p, ok := protos[name]
	return p, ok
}

// Options configures links beyond what a locator says.
type Options struct {
	// TLS holds defaults for "tls/" locators; keys in a locator's config part
	// ("#key=value;...") override them.
	TLS *TLSConfig
}

// ConfigValues parses the config part of a locator ("k=v;k=v").
func (l Locator) ConfigValues() map[string]string {
	return parseParams(l.Config)
}

// MetadataValues parses the metadata part of a locator ("k=v;k=v").
func (l Locator) MetadataValues() map[string]string {
	return parseParams(l.Metadata)
}

func parseParams(s string) map[string]string {
	m := map[string]string{}
	for _, kv := range strings.Split(s, ";") {
		if kv == "" {
			continue
		}
		k, v, _ := strings.Cut(kv, "=")
		m[k] = v
	}
	return m
}

// Dial opens a link to a locator with default options.
func Dial(locator string, timeout time.Duration) (transport.Link, error) {
	return DialWith(locator, timeout, Options{})
}

// DialWith opens a link to a locator: tcp, udp, tls, ws, unixsock-stream, or
// a registered protocol.
func DialWith(locator string, timeout time.Duration, o Options) (transport.Link, error) {
	l, err := Parse(locator)
	if err != nil {
		return nil, err
	}
	switch l.Protocol {
	case "tcp":
		c, err := net.DialTimeout("tcp", l.Address, timeout)
		if err != nil {
			return nil, err
		}
		if tc, ok := c.(*net.TCPConn); ok {
			_ = tc.SetNoDelay(true)
		}
		return c, nil
	case "udp":
		return dialUDP(l)
	case "tls":
		return dialTLS(l, timeout, o.TLS)
	case "ws":
		return dialWS(l, timeout)
	case "unixsock-stream":
		return dialUnix(l, timeout)
	case "serial":
		return dialSerial(l, timeout)
	default:
		if p, ok := registered(l.Protocol); ok && p.Dial != nil {
			return p.Dial(l, timeout, o)
		}
		return nil, ErrUnsupportedProtocol
	}
}

// Listener accepts inbound links on a locator.
type Listener interface {
	// Accept waits for the next inbound link.
	Accept() (transport.Link, error)
	// Close stops listening; a blocked Accept returns an error.
	Close() error
	// Locator is the locator actually bound (e.g. with the real port when the
	// requested port was 0), without the config part.
	Locator() Locator
}

type tcpListener struct {
	ln  net.Listener
	loc Locator
}

func (l *tcpListener) Accept() (transport.Link, error) {
	c, err := l.ln.Accept()
	if err != nil {
		return nil, err
	}
	if tc, ok := c.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}
	return c, nil
}

func (l *tcpListener) Close() error     { return l.ln.Close() }
func (l *tcpListener) Locator() Locator { return l.loc }

// Listen starts listening on a locator with default options.
func Listen(locator string) (Listener, error) {
	return ListenWith(locator, Options{})
}

// ListenWith starts listening on a locator such as "tcp/0.0.0.0:7447",
// "udp/[::]:0" or "tls/0.0.0.0:7447#listen_certificate_file=...".
func ListenWith(locator string, o Options) (Listener, error) {
	l, err := Parse(locator)
	if err != nil {
		return nil, err
	}
	bound := Locator{Protocol: l.Protocol, Metadata: l.Metadata}
	switch l.Protocol {
	case "tcp":
		ln, err := net.Listen("tcp", l.Address)
		if err != nil {
			return nil, err
		}
		bound.Address = ln.Addr().String()
		return &tcpListener{ln: ln, loc: bound}, nil
	case "udp":
		return listenUDP(l, bound)
	case "tls":
		return listenTLS(l, bound, o.TLS)
	case "ws":
		return listenWS(l, bound)
	case "unixsock-stream":
		return listenUnix(l, bound)
	case "serial":
		return listenSerial(l, bound)
	default:
		if p, ok := registered(l.Protocol); ok && p.Listen != nil {
			return p.Listen(l, o)
		}
		return nil, ErrUnsupportedProtocol
	}
}
