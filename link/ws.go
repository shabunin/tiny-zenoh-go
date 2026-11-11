package link

import (
	"bufio"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/textproto"
	"strings"
	"sync"
	"time"

	"github.com/shabunin/tiny-zenoh-go/transport"
)

// WebSocket links, "ws/host:port" (zenoh-rust io/zenoh-links/zenoh-link-ws):
// each batch is one binary WebSocket message, without the stream length
// prefix. zenoh-rust treats any non-binary data message as an error and never
// pings. This is a minimal RFC 6455 implementation (no extensions, no
// subprotocols) so the core stays free of dependencies and TinyGo-friendly.

const (
	wsGUID       = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
	wsMTU        = 0xffff
	wsMaxMessage = 1 << 24 // refuse larger messages: batches are < 64 KiB

	wsCont   = 0x0
	wsText   = 0x1
	wsBinary = 0x2
	wsClose  = 0x8
	wsPing   = 0x9
	wsPong   = 0xa
)

var (
	errWSHandshake = errors.New("link: websocket handshake failed")
	errWSProtocol  = errors.New("link: websocket protocol error")
)

func wsAccept(key string) string {
	h := sha1.Sum([]byte(key + wsGUID))
	return base64.StdEncoding.EncodeToString(h[:])
}

// wsConn is one WebSocket connection carrying zenoh batches.
type wsConn struct {
	c      net.Conn
	br     *bufio.Reader
	client bool // client frames are masked

	hsOnce sync.Once // server side: the HTTP upgrade runs on first use
	hsErr  error

	wmu       sync.Mutex
	closeSent bool

	rest []byte // unread part of the current message
	msg  []byte
}

var _ transport.PacketLink = (*wsConn)(nil)

func (w *wsConn) MTU() int { return wsMTU }

// Reliable: WebSocket runs over TCP, so no batch is ever lost.
func (w *wsConn) Reliable() bool { return true }

func (w *wsConn) SetDeadline(t time.Time) error { return w.c.SetDeadline(t) }

func dialWS(l Locator, timeout time.Duration) (transport.Link, error) {
	c, err := net.DialTimeout("tcp", l.Address, timeout)
	if err != nil {
		return nil, err
	}
	if tc, ok := c.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}
	_ = c.SetDeadline(time.Now().Add(timeout))
	var nonce [16]byte
	binary.LittleEndian.PutUint64(nonce[:8], rand.Uint64())
	binary.LittleEndian.PutUint64(nonce[8:], rand.Uint64())
	key := base64.StdEncoding.EncodeToString(nonce[:])
	req := "GET / HTTP/1.1\r\nHost: " + l.Address + "\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" +
		"Sec-WebSocket-Key: " + key + "\r\nSec-WebSocket-Version: 13\r\n\r\n"
	if _, err := io.WriteString(c, req); err != nil {
		c.Close()
		return nil, err
	}
	br := bufio.NewReader(c)
	tp := textproto.NewReader(br)
	status, err := tp.ReadLine()
	if err != nil {
		c.Close()
		return nil, fmt.Errorf("%w: %v", errWSHandshake, err)
	}
	if f := strings.Fields(status); len(f) < 2 || f[1] != "101" {
		c.Close()
		return nil, fmt.Errorf("%w: server answered %q", errWSHandshake, status)
	}
	h, err := tp.ReadMIMEHeader()
	if err != nil {
		c.Close()
		return nil, fmt.Errorf("%w: %v", errWSHandshake, err)
	}
	if h.Get("Sec-Websocket-Accept") != wsAccept(key) {
		c.Close()
		return nil, fmt.Errorf("%w: bad Sec-WebSocket-Accept", errWSHandshake)
	}
	_ = c.SetDeadline(time.Time{})
	w := &wsConn{c: c, br: br, client: true}
	w.hsOnce.Do(func() {})
	return w, nil
}

// serverHandshake answers the HTTP upgrade request (any path).
func (w *wsConn) serverHandshake() error {
	w.hsOnce.Do(func() {
		tp := textproto.NewReader(w.br)
		line, err := tp.ReadLine()
		if err != nil {
			w.hsErr = fmt.Errorf("%w: %v", errWSHandshake, err)
			return
		}
		if f := strings.Fields(line); len(f) != 3 || f[0] != "GET" {
			w.hsErr = fmt.Errorf("%w: request %q", errWSHandshake, line)
			return
		}
		h, err := tp.ReadMIMEHeader()
		if err != nil {
			w.hsErr = fmt.Errorf("%w: %v", errWSHandshake, err)
			return
		}
		key := h.Get("Sec-Websocket-Key")
		if !strings.EqualFold(h.Get("Upgrade"), "websocket") || key == "" {
			_, _ = io.WriteString(w.c, "HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\n")
			w.hsErr = fmt.Errorf("%w: not a websocket upgrade", errWSHandshake)
			return
		}
		resp := "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" +
			"Sec-WebSocket-Accept: " + wsAccept(key) + "\r\n\r\n"
		if _, err := io.WriteString(w.c, resp); err != nil {
			w.hsErr = err
		}
	})
	return w.hsErr
}

// writeFrame sends one complete frame.
func (w *wsConn) writeFrame(op byte, payload []byte) error {
	w.wmu.Lock()
	defer w.wmu.Unlock()
	if w.closeSent {
		return net.ErrClosed
	}
	if op == wsClose {
		w.closeSent = true
	}
	hdr := make([]byte, 0, 14+len(payload))
	hdr = append(hdr, 0x80|op)
	var mbit byte
	if w.client {
		mbit = 0x80
	}
	switch n := len(payload); {
	case n < 126:
		hdr = append(hdr, mbit|byte(n))
	case n <= 0xffff:
		hdr = append(hdr, mbit|126)
		hdr = binary.BigEndian.AppendUint16(hdr, uint16(n))
	default:
		hdr = append(hdr, mbit|127)
		hdr = binary.BigEndian.AppendUint64(hdr, uint64(n))
	}
	if w.client {
		var mask [4]byte
		binary.LittleEndian.PutUint32(mask[:], rand.Uint32())
		hdr = append(hdr, mask[:]...)
		start := len(hdr)
		hdr = append(hdr, payload...)
		for i := range payload {
			hdr[start+i] ^= mask[i&3]
		}
	} else {
		hdr = append(hdr, payload...)
	}
	_, err := w.c.Write(hdr)
	return err
}

// Write sends b as one binary message.
func (w *wsConn) Write(b []byte) (int, error) {
	if err := w.serverHandshake(); err != nil {
		return 0, err
	}
	if err := w.writeFrame(wsBinary, b); err != nil {
		return 0, err
	}
	return len(b), nil
}

// readMessage reads the next data message, answering control frames.
func (w *wsConn) readMessage() ([]byte, error) {
	w.msg = w.msg[:0]
	var hdr [8]byte
	started := false
	for {
		if _, err := io.ReadFull(w.br, hdr[:2]); err != nil {
			return nil, err
		}
		fin, op := hdr[0]&0x80 != 0, hdr[0]&0x0f
		if hdr[0]&0x70 != 0 {
			return nil, fmt.Errorf("%w: reserved bits set", errWSProtocol)
		}
		masked, n := hdr[1]&0x80 != 0, uint64(hdr[1]&0x7f)
		switch n {
		case 126:
			if _, err := io.ReadFull(w.br, hdr[:2]); err != nil {
				return nil, err
			}
			n = uint64(binary.BigEndian.Uint16(hdr[:2]))
		case 127:
			if _, err := io.ReadFull(w.br, hdr[:8]); err != nil {
				return nil, err
			}
			n = binary.BigEndian.Uint64(hdr[:8])
		}
		var mask [4]byte
		if masked {
			if _, err := io.ReadFull(w.br, mask[:]); err != nil {
				return nil, err
			}
		}
		if n > wsMaxMessage || uint64(len(w.msg))+n > wsMaxMessage {
			return nil, fmt.Errorf("%w: message too large", errWSProtocol)
		}
		start := len(w.msg)
		w.msg = append(w.msg, make([]byte, n)...)
		p := w.msg[start:]
		if _, err := io.ReadFull(w.br, p); err != nil {
			return nil, err
		}
		if masked {
			for i := range p {
				p[i] ^= mask[i&3]
			}
		}
		if op >= 0x8 { // control frame, may come between fragments
			ctl := append([]byte(nil), p...)
			w.msg = w.msg[:start]
			switch op {
			case wsPing:
				_ = w.writeFrame(wsPong, ctl)
			case wsClose:
				_ = w.writeFrame(wsClose, nil)
				return nil, io.EOF
			}
			continue
		}
		switch {
		case !started && op == wsBinary:
			started = true
		case !started && op == wsText:
			return nil, fmt.Errorf("%w: text message on a zenoh link", errWSProtocol)
		case started && op == wsCont:
		default:
			return nil, fmt.Errorf("%w: unexpected opcode %#x", errWSProtocol, op)
		}
		if fin {
			return w.msg, nil
		}
	}
}

// Read returns one binary message (or what is left of it if b was short).
func (w *wsConn) Read(b []byte) (int, error) {
	if err := w.serverHandshake(); err != nil {
		return 0, err
	}
	if len(w.rest) == 0 {
		for {
			m, err := w.readMessage()
			if err != nil {
				return 0, err
			}
			if len(m) > 0 {
				w.rest = m
				break
			}
		}
	}
	n := copy(b, w.rest)
	w.rest = w.rest[n:]
	return n, nil
}

func (w *wsConn) Close() error {
	_ = w.c.SetWriteDeadline(time.Now().Add(time.Second))
	_ = w.writeFrame(wsClose, nil)
	return w.c.Close()
}

type wsListener struct {
	ln  net.Listener
	loc Locator
}

func listenWS(l Locator, bound Locator) (Listener, error) {
	ln, err := net.Listen("tcp", l.Address)
	if err != nil {
		return nil, err
	}
	bound.Address = ln.Addr().String()
	return &wsListener{ln: ln, loc: bound}, nil
}

// Accept returns the next connection; its HTTP upgrade runs on first use,
// under the transport's handshake deadline.
func (l *wsListener) Accept() (transport.Link, error) {
	c, err := l.ln.Accept()
	if err != nil {
		return nil, err
	}
	if tc, ok := c.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}
	return &wsConn{c: c, br: bufio.NewReader(c)}, nil
}

func (l *wsListener) Close() error     { return l.ln.Close() }
func (l *wsListener) Locator() Locator { return l.loc }
