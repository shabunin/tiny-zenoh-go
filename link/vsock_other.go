//go:build !linux || tinygo

package link

import (
	"errors"
	"time"

	"github.com/shabunin/tiny-zenoh-go/transport"
)

var errNoVsock = errors.New("link: vsock is only available on Linux")

func dialVsock(Locator, time.Duration) (transport.Link, error) { return nil, errNoVsock }

func listenVsock(Locator, Locator) (Listener, error) { return nil, errNoVsock }
