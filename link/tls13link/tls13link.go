// Package tls13link gives TinyGo builds tls/ links: imported for its side
// effect, it makes package link dial tls/ locators with package tls13, a
// TLS 1.3 client.
//
//	import _ "github.com/shabunin/tiny-zenoh-go/link/tls13link"
//
// A root CA is required (root_ca_certificate, or link.TLSConfig's
// RootCACertificate or RootCACertificatePEM): there are no system roots.
// Regular Go builds keep crypto/tls for tls/ and ignore this package (use
// link.DialTLS13 to pick tls13 there).
package tls13link

import (
	"time"

	"github.com/shabunin/tiny-zenoh-go/link"
	"github.com/shabunin/tiny-zenoh-go/transport"
)

func init() {
	link.Register("tls", link.Protocol{Dial: func(l link.Locator, timeout time.Duration, o link.Options) (transport.Link, error) {
		return link.DialTLS13(l.String(), timeout, o.TLS)
	}})
}
