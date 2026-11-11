//go:build tinygo

package link

import (
	"errors"
	"time"

	"github.com/shabunin/gozenoh/transport"
)

// TinyGo's crypto/tls has no TLS implementation for these targets.
var errNoTLS = errors.New("link: tls is not available in TinyGo builds")

func dialTLS(Locator, time.Duration, *TLSConfig) (transport.Link, error) { return nil, errNoTLS }

func listenTLS(Locator, Locator, *TLSConfig) (Listener, error) { return nil, errNoTLS }

func tlsCommonName(any) string { return "" }
