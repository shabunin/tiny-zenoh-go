//go:build tinygo

package link

import (
	"errors"
	"time"

	"github.com/shabunin/tiny-zenoh-go/transport"
)

// TinyGo builds dial tls/ with what registers itself for "tls" — package
// link/tls13link, a TLS 1.3 client (package tls13) — so that builds
// without it don't carry its ~200 KB. They can't listen on tls/.
var (
	errNoTLS       = errors.New(`link: tls/ in TinyGo builds needs import _ "github.com/shabunin/tiny-zenoh-go/link/tls13link"`)
	errNoTLSListen = errors.New("link: tls listeners are not available in TinyGo builds")
)

func dialTLS(l Locator, timeout time.Duration, def *TLSConfig) (transport.Link, error) {
	if p, ok := registered("tls"); ok && p.Dial != nil {
		return p.Dial(l, timeout, Options{TLS: def})
	}
	return nil, errNoTLS
}

func listenTLS(Locator, Locator, *TLSConfig) (Listener, error) { return nil, errNoTLSListen }

func tlsCommonName(l any) string { return tls13CommonName(l) }
