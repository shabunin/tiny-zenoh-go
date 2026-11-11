//go:build !unix || tinygo

package link

import (
	"errors"
	"time"

	"github.com/shabunin/gozenoh/transport"
)

var errNoUnix = errors.New("link: unixsock-stream is not available on this platform")

func dialUnix(Locator, time.Duration) (transport.Link, error) { return nil, errNoUnix }

func listenUnix(Locator, Locator) (Listener, error) { return nil, errNoUnix }
