//go:build unix && !tinygo

package link

import (
	"fmt"
	"net"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/shabunin/tiny-zenoh-go/transport"
)

// Unix domain stream sockets, "unixsock-stream/<path>" (zenoh-rust
// io/zenoh-links/zenoh-link-unixsock_stream): framed like TCP. A listener
// holds an exclusive flock on "<path>.lock"; when it gets the lock, a socket
// file left by a dead process is removed and rebound. Closing the listener
// removes both files.

func dialUnix(l Locator, timeout time.Duration) (transport.Link, error) {
	return net.DialTimeout("unix", l.Address, timeout)
}

type unixListener struct {
	ln   *net.UnixListener
	loc  Locator
	path string
	lock *os.File
	once sync.Once
}

func listenUnix(l Locator, bound Locator) (Listener, error) {
	path := l.Address
	lock, err := os.OpenFile(path+".lock", os.O_RDONLY|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("link: unixsock-stream lock file: %w", err)
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lock.Close()
		return nil, fmt.Errorf("link: unixsock-stream %s is in use: %w", path, err)
	}
	// We hold the lock, so no live process uses the socket: clear a stale one.
	if fi, err := os.Lstat(path); err == nil && fi.Mode()&os.ModeSocket != 0 {
		_ = os.Remove(path)
	}
	ln, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		lock.Close()
		_ = os.Remove(path + ".lock")
		return nil, err
	}
	ln.SetUnlinkOnClose(true)
	bound.Address = path
	return &unixListener{ln: ln, loc: bound, path: path, lock: lock}, nil
}

func (u *unixListener) Accept() (transport.Link, error) {
	c, err := u.ln.Accept()
	if err != nil {
		return nil, err
	}
	return c, nil
}

func (u *unixListener) Close() error {
	var err error
	u.once.Do(func() {
		err = u.ln.Close()
		_ = os.Remove(u.path + ".lock")
		_ = u.lock.Close() // releases the flock
	})
	return err
}

func (u *unixListener) Locator() Locator { return u.loc }
