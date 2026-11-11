//go:build unix && !tinygo

package link

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/shabunin/tiny-zenoh-go/transport"
)

// Named pipes, "unixpipe/<path>" (zenoh-rust
// io/zenoh-links/zenoh-link-unixpipe), framed like TCP:
//
//   - a listener owns the FIFO "<path>_uplink", opened read-write and
//     non-blocking with an exclusive flock (which is how connectors tell a
//     live listener from a stale FIFO);
//   - a connector picks a random u32 suffix, creates and locks its own FIFO
//     "<path>_downlink<suffix>", and writes the 8-byte invitation
//     DE AD BE EF + suffix (native byte order) to "<path>_uplink";
//   - the listener opens that downlink for writing, creates and locks
//     "<path>_uplink<suffix>", and sends the invitation back on the
//     downlink; the connector opens the new uplink for writing and confirms
//     with the invitation once more.
//
// Each side unlinks the FIFO it reads from when it closes. Readers hold
// their FIFO open read-write, so they never see EOF: a dead peer is found
// by the session lease, as with zenoh-rust.

var pipeInvitation = []byte{0xDE, 0xAD, 0xBE, 0xEF}

const (
	pipeDefaultMask = 0o777
	pipeTries       = 100
	pipeHandshake   = 10 * time.Second
)

func pipeMask(l Locator) uint32 {
	if v, ok := l.ConfigValues()["file_mask"]; ok {
		if m, err := strconv.ParseUint(v, 10, 32); err == nil {
			return uint32(m)
		}
	}
	return pipeDefaultMask
}

// openPipeR creates (if needed) and opens the FIFO we read from, locked.
func openPipeR(path string, mask uint32) (*os.File, error) {
	created := syscall.Mkfifo(path, mask) == nil
	f, err := os.OpenFile(path, os.O_RDWR|syscall.O_NONBLOCK, 0)
	if err == nil {
		if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
			f.Close()
		}
	}
	if err != nil {
		if created {
			_ = os.Remove(path)
		}
		return nil, err
	}
	if !created {
		drainPipe(f) // whatever a previous owner left unread
	}
	return f, nil
}

func drainPipe(f *os.File) {
	rc, err := f.SyscallConn()
	if err != nil {
		return
	}
	var buf [512]byte
	_ = rc.Read(func(fd uintptr) bool {
		for {
			if n, err := syscall.Read(int(fd), buf[:]); n <= 0 || err != nil {
				return true
			}
		}
	})
}

// openPipeW opens a FIFO someone else reads, checking that its reader holds
// the lock (else it is a stale FIFO with nobody behind it).
func openPipeW(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_WRONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	if syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB) == nil {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		f.Close()
		return nil, fmt.Errorf("link: unixpipe %s: no listener", path)
	}
	return f, nil
}

func pipeInvite(w io.Writer, suffix uint32) error {
	var msg [8]byte
	copy(msg[:], pipeInvitation)
	binary.NativeEndian.PutUint32(msg[4:], suffix)
	_, err := w.Write(msg[:])
	return err
}

func pipeReceive(r *os.File, deadline time.Time) (uint32, error) {
	_ = r.SetReadDeadline(deadline)
	defer r.SetReadDeadline(time.Time{})
	var msg [8]byte
	if _, err := io.ReadFull(r, msg[:]); err != nil {
		return 0, err
	}
	if !bytes.Equal(msg[:4], pipeInvitation) {
		return 0, errors.New("link: unixpipe: unexpected invitation")
	}
	return binary.NativeEndian.Uint32(msg[4:]), nil
}

func pipeExpect(r *os.File, suffix uint32, deadline time.Time) error {
	got, err := pipeReceive(r, deadline)
	if err != nil {
		return err
	}
	if got != suffix {
		return fmt.Errorf("link: unixpipe: suffix mismatch: expected %d got %d", suffix, got)
	}
	return nil
}

func pipeNames(path string, suffix uint32) (down, up string) {
	s := strconv.FormatUint(uint64(suffix), 10)
	return path + "_downlink" + s, path + "_uplink" + s
}

// pipeLink is one established unixpipe link.
type pipeLink struct {
	r, w  *os.File
	rpath string
	once  sync.Once
}

func (p *pipeLink) Read(b []byte) (int, error)  { return p.r.Read(b) }
func (p *pipeLink) Write(b []byte) (int, error) { return p.w.Write(b) }

func (p *pipeLink) Close() error {
	p.once.Do(func() {
		p.w.Close()
		p.r.Close()
		_ = os.Remove(p.rpath)
	})
	return nil
}

func dialPipe(l Locator, timeout time.Duration) (transport.Link, error) {
	path, mask := l.Address, pipeMask(l)
	req, err := openPipeW(path + "_uplink")
	if err != nil {
		return nil, err
	}
	defer req.Close()
	var (
		r        *os.File
		suffix   uint32
		down, up string
	)
	for range pipeTries {
		suffix = rand.Uint32()
		down, up = pipeNames(path, suffix)
		if r, err = openPipeR(down, mask); err != nil {
			continue
		}
		// Like zenoh-rust: make sure the uplink name is free too.
		u, err := openPipeR(up, mask)
		if err != nil {
			r.Close()
			_ = os.Remove(down)
			r = nil
			continue
		}
		u.Close()
		_ = os.Remove(up)
		break
	}
	if r == nil {
		return nil, errors.New("link: unixpipe: unable to dedicate a pipe")
	}
	if timeout <= 0 {
		timeout = pipeHandshake
	}
	deadline := time.Now().Add(timeout)
	fail := func(err error) (transport.Link, error) {
		r.Close()
		_ = os.Remove(down)
		return nil, err
	}
	if err := pipeInvite(req, suffix); err != nil {
		return fail(err)
	}
	if err := pipeExpect(r, suffix, deadline); err != nil {
		return fail(err)
	}
	w, err := openPipeW(up)
	if err != nil {
		return fail(err)
	}
	if err := pipeInvite(w, suffix); err != nil {
		w.Close()
		return fail(err)
	}
	return &pipeLink{r: r, w: w, rpath: down}, nil
}

type pipeListener struct {
	req  *os.File
	path string
	mask uint32
	loc  Locator
	once sync.Once
}

func listenPipe(l Locator, bound Locator) (Listener, error) {
	mask := pipeMask(l)
	req, err := openPipeR(l.Address+"_uplink", mask)
	if err != nil {
		return nil, fmt.Errorf("link: unixpipe %s: %w", l.Address, err)
	}
	bound.Address = l.Address
	return &pipeListener{req: req, path: l.Address, mask: mask, loc: bound}, nil
}

func (p *pipeListener) Accept() (transport.Link, error) {
	for {
		suffix, err := pipeReceive(p.req, time.Time{})
		if errors.Is(err, os.ErrClosed) {
			return nil, err
		}
		if err != nil {
			continue // a bad invitation: zenoh-rust drops it too
		}
		if l, err := p.dedicate(suffix); err == nil {
			return l, nil
		}
	}
}

func (p *pipeListener) dedicate(suffix uint32) (transport.Link, error) {
	down, up := pipeNames(p.path, suffix)
	w, err := openPipeW(down)
	if err != nil {
		return nil, err
	}
	r, err := openPipeR(up, p.mask)
	if err != nil {
		w.Close()
		return nil, err
	}
	l := &pipeLink{r: r, w: w, rpath: up}
	if err := pipeInvite(w, suffix); err != nil {
		l.Close()
		return nil, err
	}
	if err := pipeExpect(r, suffix, time.Now().Add(pipeHandshake)); err != nil {
		l.Close()
		return nil, err
	}
	return l, nil
}

func (p *pipeListener) Close() error {
	p.once.Do(func() {
		p.req.Close()
		_ = os.Remove(p.path + "_uplink")
	})
	return nil
}

func (p *pipeListener) Locator() Locator { return p.loc }
