//go:build linux && !tinygo

package link

import (
	"fmt"
	"os"
	"sync"
	"time"

	"golang.org/x/sys/unix"

	"github.com/shabunin/tiny-zenoh-go/transport"
)

// VM sockets, "vsock/<cid>:<port>" (zenoh-rust io/zenoh-links/zenoh-link-vsock):
// AF_VSOCK stream sockets between a VM and its host (or, with the
// vsock_loopback module, inside one machine: CID 1, VMADDR_CID_LOCAL),
// framed like TCP. The CID may be a number or VMADDR_CID_ANY /
// VMADDR_CID_LOCAL / VMADDR_CID_HOST / VMADDR_CID_HYPERVISOR, the port a
// number or VMADDR_PORT_ANY; -1 means "any" for both.

func dialVsock(l Locator, timeout time.Duration) (transport.Link, error) {
	cid, port, err := parseVsock(l.Address)
	if err != nil {
		return nil, err
	}
	fd, err := unix.Socket(unix.AF_VSOCK, unix.SOCK_STREAM|unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("link: vsock: %w", err)
	}
	err = unix.Connect(fd, &unix.SockaddrVM{CID: cid, Port: port})
	f := os.NewFile(uintptr(fd), "vsock:"+l.Address)
	if err == unix.EINPROGRESS {
		err = waitConnected(f, timeout)
	}
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("link: vsock %s: %w", l.Address, err)
	}
	return f, nil
}

// waitConnected waits for a non-blocking connect to finish.
func waitConnected(f *os.File, timeout time.Duration) error {
	if timeout > 0 {
		_ = f.SetWriteDeadline(time.Now().Add(timeout))
		defer f.SetWriteDeadline(time.Time{})
	}
	rc, err := f.SyscallConn()
	if err != nil {
		return err
	}
	var soErr int
	waited := false
	werr := rc.Write(func(fd uintptr) bool {
		if !waited {
			waited = true
			return false // wait until writable
		}
		soErr, err = unix.GetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_ERROR)
		return true
	})
	if werr != nil {
		return werr
	}
	if err != nil {
		return err
	}
	if soErr != 0 {
		return unix.Errno(soErr)
	}
	return nil
}

type vsockListener struct {
	f    *os.File
	loc  Locator
	once sync.Once
}

func listenVsock(l Locator, bound Locator) (Listener, error) {
	cid, port, err := parseVsock(l.Address)
	if err != nil {
		return nil, err
	}
	fd, err := unix.Socket(unix.AF_VSOCK, unix.SOCK_STREAM|unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("link: vsock: %w", err)
	}
	if err := unix.Bind(fd, &unix.SockaddrVM{CID: cid, Port: port}); err != nil {
		unix.Close(fd)
		return nil, fmt.Errorf("link: vsock %s: %w", l.Address, err)
	}
	if err := unix.Listen(fd, 128); err != nil {
		unix.Close(fd)
		return nil, fmt.Errorf("link: vsock %s: %w", l.Address, err)
	}
	if sa, err := unix.Getsockname(fd); err == nil {
		if vm, ok := sa.(*unix.SockaddrVM); ok {
			port = vm.Port
		}
	}
	bound.Address = fmt.Sprintf("%d:%d", cid, port)
	return &vsockListener{f: os.NewFile(uintptr(fd), "vsock-listener"), loc: bound}, nil
}

func (v *vsockListener) Accept() (transport.Link, error) {
	rc, err := v.f.SyscallConn()
	if err != nil {
		return nil, err
	}
	var nfd int
	var aerr error
	if err := rc.Read(func(fd uintptr) bool {
		nfd, _, aerr = unix.Accept4(int(fd), unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC)
		return aerr != unix.EAGAIN
	}); err != nil {
		return nil, err
	}
	if aerr != nil {
		return nil, aerr
	}
	return os.NewFile(uintptr(nfd), "vsock"), nil
}

func (v *vsockListener) Close() error {
	var err error
	v.once.Do(func() { err = v.f.Close() })
	return err
}

func (v *vsockListener) Locator() Locator { return v.loc }
