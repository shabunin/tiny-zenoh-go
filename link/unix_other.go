//go:build !unix || tinygo

package link

import (
	"errors"
	"time"

	"github.com/shabunin/tiny-zenoh-go/transport"
)

var errNoUnix = errors.New("link: unixsock-stream is not available on this platform")

func dialUnix(Locator, time.Duration) (transport.Link, error) { return nil, errNoUnix }

func listenUnix(Locator, Locator) (Listener, error) { return nil, errNoUnix }

var errNoPipe = errors.New("link: unixpipe is not available on this platform")

func dialPipe(Locator, time.Duration) (transport.Link, error) { return nil, errNoPipe }

func listenPipe(Locator, Locator) (Listener, error) { return nil, errNoPipe }
