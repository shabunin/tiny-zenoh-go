// Package tls13 is a small TLS 1.3 client (RFC 8446) that builds with
// TinyGo, for microcontrollers that reach zenoh routers over tls/ links.
//
// It supports TLS_AES_128_GCM_SHA256 and TLS_AES_256_GCM_SHA384 with X25519
// key exchange; server certificates with ECDSA (P-256, P-384), RSA (PSS
// handshake signatures, PKCS #1 v1.5 or ECDSA certificate signatures) and
// Ed25519 keys, verified against configured roots (there are no system
// roots); client certificates (mutual TLS); KeyUpdate. It has no TLS 1.2,
// no session resumption or 0-RTT, no HelloRetryRequest (servers must accept
// X25519) and no server side.
package tls13

import (
	"bytes"
	"crypto/ecdh"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"io"
	"net"
	"net/netip"
	"sync"
	"time"
)

// Config configures a client connection.
type Config struct {
	// ServerName is sent in SNI (unless it is an IP address) and checked
	// against the server certificate.
	ServerName string
	// RootCAs are the trust anchors; a server certificate may also be
	// pinned by listing it here.
	RootCAs []*Certificate
	// Certificate (DER, leaf first) and Key are presented when the server
	// asks for a client certificate.
	Certificate [][]byte
	Key         *PrivateKey
	// SkipNameVerification accepts a certificate for another name; the
	// chain is still verified.
	SkipNameVerification bool
	// InsecureSkipVerify accepts any server certificate. For tests.
	InsecureSkipVerify bool
	// Time returns the current time (default time.Now). Before 2024 — a
	// board whose clock was never set — validity periods are not checked.
	Time func() time.Time
	// Rand is the source of randomness (default crypto/rand.Reader).
	Rand io.Reader
}

const (
	recChangeCipherSpec = 20
	recAlert            = 21
	recHandshake        = 22
	recApplicationData  = 23

	maxPlaintext  = 1 << 14
	maxCiphertext = maxPlaintext + 256

	hsClientHello         = 1
	hsServerHello         = 2
	hsNewSessionTicket    = 4
	hsEncryptedExtensions = 8
	hsCertificate         = 11
	hsCertificateRequest  = 13
	hsCertificateVerify   = 15
	hsFinished            = 20
	hsKeyUpdate           = 24

	extServerName          = 0
	extSupportedGroups     = 10
	extSignatureAlgorithms = 13
	extSupportedVersions   = 43
	extKeyShare            = 51

	groupX25519 = 0x001d
	version13   = 0x0304

	alertCloseNotify       = 0
	alertUnexpectedMessage = 10
	alertBadRecordMAC      = 20
	alertHandshakeFailure  = 40
	alertBadCertificate    = 42
	alertIllegalParameter  = 47
	alertDecodeError       = 50
	alertDecryptError      = 51
)

// keyUpdateAfter: change keys well before AES-GCM's limit (2^24.5
// records).
var keyUpdateAfter uint64 = 1 << 24

// AlertError is a fatal alert received from the server.
type AlertError uint8

func (e AlertError) Error() string { return fmt.Sprintf("tls13: server sent alert %d", uint8(e)) }

// Conn is a TLS 1.3 client connection. Read and Write may be called
// concurrently.
type Conn struct {
	conn  io.ReadWriteCloser
	cfg   *Config
	suite *suite
	peer  []*Certificate

	in struct {
		sync.Mutex
		hc    halfConn
		hdr   [5]byte
		rec   []byte
		plain []byte // unread application data
		hs    []byte // partial handshake messages
		err   error
	}
	out struct {
		sync.Mutex
		hc  halfConn
		buf []byte
		err error
	}
}

// Client runs a TLS 1.3 handshake over conn.
func Client(conn io.ReadWriteCloser, cfg *Config) (*Conn, error) {
	c := &Conn{conn: conn, cfg: cfg}
	if err := c.handshake(); err != nil {
		return nil, err
	}
	return c, nil
}

// PeerCertificates is the server's chain, leaf first.
func (c *Conn) PeerCertificates() []*Certificate { return c.peer }

// PeerCommonName is the common name of the server's certificate.
func (c *Conn) PeerCommonName() string {
	if len(c.peer) == 0 {
		return ""
	}
	return c.peer[0].CommonName
}

// CipherSuite is the negotiated suite.
func (c *Conn) CipherSuite() uint16 { return c.suite.id }

// NetConn is the underlying connection.
func (c *Conn) NetConn() io.ReadWriteCloser { return c.conn }

// LocalAddr and RemoteAddr are the underlying connection's, if it has them.
func (c *Conn) LocalAddr() net.Addr {
	if a, ok := c.conn.(interface{ LocalAddr() net.Addr }); ok {
		return a.LocalAddr()
	}
	return nil
}

func (c *Conn) RemoteAddr() net.Addr {
	if a, ok := c.conn.(interface{ RemoteAddr() net.Addr }); ok {
		return a.RemoteAddr()
	}
	return nil
}

// SetDeadline sets the underlying connection's deadline, if it has one.
func (c *Conn) SetDeadline(t time.Time) error {
	if d, ok := c.conn.(interface{ SetDeadline(time.Time) error }); ok {
		return d.SetDeadline(t)
	}
	return nil
}

func (c *Conn) rand() io.Reader {
	if c.cfg.Rand != nil {
		return c.cfg.Rand
	}
	return rand.Reader
}

func (c *Conn) now() time.Time {
	if c.cfg.Time != nil {
		return c.cfg.Time()
	}
	return time.Now()
}

// --- records

// readRecord returns the next record, decrypted (the content type is the
// inner one). The data is valid until the next call.
func (c *Conn) readRecord() (byte, []byte, error) {
	if _, err := io.ReadFull(c.conn, c.in.hdr[:]); err != nil {
		return 0, nil, err
	}
	typ := c.in.hdr[0]
	n := int(binary.BigEndian.Uint16(c.in.hdr[3:]))
	if n > maxCiphertext || c.in.hdr[1] != 3 {
		return 0, nil, c.fail(alertDecodeError, errors.New("tls13: bad record header"))
	}
	if cap(c.in.rec) < n {
		c.in.rec = make([]byte, n, max(n, 2048))
	}
	rec := c.in.rec[:n]
	if _, err := io.ReadFull(c.conn, rec); err != nil {
		return 0, nil, err
	}
	if c.in.hc.aead == nil || typ == recChangeCipherSpec {
		return typ, rec, nil
	}
	if typ != recApplicationData {
		return 0, nil, c.fail(alertUnexpectedMessage, errors.New("tls13: unprotected record after the handshake keys"))
	}
	plain, err := c.in.hc.aead.Open(rec[:0], c.in.hc.nonce(), rec, c.in.hdr[:])
	if err != nil {
		return 0, nil, c.fail(alertBadRecordMAC, errors.New("tls13: record authentication failed"))
	}
	i := len(plain) - 1
	for i >= 0 && plain[i] == 0 {
		i--
	}
	if i < 0 || len(plain) > maxPlaintext+1 {
		return 0, nil, c.fail(alertUnexpectedMessage, errors.New("tls13: bad inner record"))
	}
	return plain[i], plain[:i], nil
}

// writeRecord sends data as records of type typ; called with out locked
// (or during the handshake).
func (c *Conn) writeRecord(typ byte, data []byte) error {
	for first := true; first || len(data) > 0; first = false {
		n := min(len(data), maxPlaintext)
		frag := data[:n]
		data = data[n:]
		b := c.out.buf[:0]
		if c.out.hc.aead == nil {
			ver := uint16(0x0303)
			if typ == recHandshake && n > 0 && frag[0] == hsClientHello {
				ver = 0x0301
			}
			b = append(b, typ, byte(ver>>8), byte(ver))
			b = binary.BigEndian.AppendUint16(b, uint16(n))
			b = append(b, frag...)
		} else {
			size := n + 1 + c.out.hc.aead.Overhead()
			b = append(b, recApplicationData, 3, 3, byte(size>>8), byte(size))
			hdr := len(b)
			b = append(b, frag...)
			b = append(b, typ)
			b = c.out.hc.aead.Seal(b[:hdr], c.out.hc.nonce(), b[hdr:], b[:hdr])
		}
		c.out.buf = b
		if _, err := c.conn.Write(b); err != nil {
			return err
		}
	}
	return nil
}

func (c *Conn) sendAlert(desc byte) {
	level := byte(2)
	if desc == alertCloseNotify {
		level = 1
	}
	_ = c.writeRecord(recAlert, []byte{level, desc})
}

// fail sends a fatal alert (best effort) and returns err.
func (c *Conn) fail(desc byte, err error) error {
	if c.out.TryLock() {
		if c.out.err == nil {
			c.sendAlert(desc)
			c.out.err = err
		}
		c.out.Unlock()
	}
	return err
}

// readHandshake returns the next handshake message (header included).
func (c *Conn) readHandshake() ([]byte, error) {
	for {
		if len(c.in.hs) >= 4 {
			n := 4 + (int(c.in.hs[1])<<16 | int(c.in.hs[2])<<8 | int(c.in.hs[3]))
			if n > 1<<16+4 {
				return nil, c.fail(alertDecodeError, errors.New("tls13: handshake message too large"))
			}
			if len(c.in.hs) >= n {
				msg := c.in.hs[:n:n]
				c.in.hs = c.in.hs[n:]
				return msg, nil
			}
		}
		typ, data, err := c.readRecord()
		if err != nil {
			return nil, err
		}
		switch typ {
		case recHandshake:
			c.in.hs = append(c.in.hs, data...)
		case recChangeCipherSpec:
			if !bytes.Equal(data, []byte{1}) {
				return nil, c.fail(alertUnexpectedMessage, errors.New("tls13: bad change_cipher_spec"))
			}
		case recAlert:
			return nil, alertErr(data)
		default:
			return nil, c.fail(alertUnexpectedMessage, fmt.Errorf("tls13: unexpected record type %d during the handshake", typ))
		}
	}
}

func alertErr(data []byte) error {
	if len(data) != 2 {
		return errors.New("tls13: malformed alert")
	}
	if data[1] == alertCloseNotify {
		return io.EOF
	}
	return AlertError(data[1])
}

// --- application data

func (c *Conn) Read(p []byte) (int, error) {
	c.in.Lock()
	defer c.in.Unlock()
	for len(c.in.plain) == 0 {
		if c.in.err != nil {
			return 0, c.in.err
		}
		if err := c.readPostHandshake(); err != nil {
			c.in.err = err
		}
	}
	n := copy(p, c.in.plain)
	c.in.plain = c.in.plain[n:]
	return n, nil
}

// readPostHandshake reads one record after the handshake.
func (c *Conn) readPostHandshake() error {
	typ, data, err := c.readRecord()
	if err != nil {
		return err
	}
	switch typ {
	case recApplicationData:
		c.in.plain = data
		return nil
	case recAlert:
		return alertErr(data)
	case recHandshake:
		c.in.hs = append(c.in.hs, data...)
		for len(c.in.hs) >= 4 {
			n := 4 + (int(c.in.hs[1])<<16 | int(c.in.hs[2])<<8 | int(c.in.hs[3]))
			if n > 1<<16+4 {
				return c.fail(alertDecodeError, errors.New("tls13: handshake message too large"))
			}
			if len(c.in.hs) < n {
				break
			}
			msg := c.in.hs[:n]
			c.in.hs = c.in.hs[n:]
			switch msg[0] {
			case hsNewSessionTicket: // no resumption
			case hsKeyUpdate:
				if n != 5 || msg[4] > 1 || len(c.in.hs) != 0 {
					return c.fail(alertDecodeError, errors.New("tls13: bad KeyUpdate"))
				}
				c.in.hc.update()
				if msg[4] == 1 {
					c.out.Lock()
					err := c.out.err
					if err == nil {
						if err = c.writeRecord(recHandshake, []byte{hsKeyUpdate, 0, 0, 1, 0}); err == nil {
							c.out.hc.update()
						}
						c.out.err = err
					}
					c.out.Unlock()
					if err != nil {
						return err
					}
				}
			default:
				return c.fail(alertUnexpectedMessage, fmt.Errorf("tls13: unexpected handshake message %d", msg[0]))
			}
		}
		if len(c.in.hs) == 0 {
			c.in.hs = nil
		}
		return nil
	}
	return c.fail(alertUnexpectedMessage, fmt.Errorf("tls13: unexpected record type %d", typ))
}

func (c *Conn) Write(p []byte) (int, error) {
	c.out.Lock()
	defer c.out.Unlock()
	if c.out.err != nil {
		return 0, c.out.err
	}
	if len(p) == 0 {
		return 0, nil
	}
	if c.out.hc.seq >= keyUpdateAfter {
		if err := c.writeRecord(recHandshake, []byte{hsKeyUpdate, 0, 0, 1, 0}); err != nil {
			c.out.err = err
			return 0, err
		}
		c.out.hc.update()
	}
	if err := c.writeRecord(recApplicationData, p); err != nil {
		c.out.err = err
		return 0, err
	}
	return len(p), nil
}

// Close sends close_notify and closes the connection.
func (c *Conn) Close() error {
	if c.out.TryLock() {
		if c.out.err == nil {
			c.sendAlert(alertCloseNotify)
			c.out.err = errors.New("tls13: use of closed connection")
		}
		c.out.Unlock()
	}
	return c.conn.Close()
}

// --- handshake

var hrrRandom = []byte{
	0xcf, 0x21, 0xad, 0x74, 0xe5, 0x9a, 0x61, 0x11, 0xbe, 0x1d, 0x8c, 0x02, 0x1e, 0x65, 0xb8, 0x91,
	0xc2, 0xa2, 0x11, 0x16, 0x7a, 0xbb, 0x8c, 0x5e, 0x07, 0x9e, 0x09, 0xe2, 0xc8, 0xa8, 0x33, 0x9c,
}

// builder appends length-prefixed vectors.
type builder []byte

func (b *builder) u8(v byte)    { *b = append(*b, v) }
func (b *builder) u16(v uint16) { *b = binary.BigEndian.AppendUint16(*b, v) }
func (b *builder) bytes(v []byte) {
	*b = append(*b, v...)
}

// vec writes a vector with an n-byte length prefix filled in by f.
func (b *builder) vec(n int, f func()) {
	start := len(*b)
	*b = append(*b, make([]byte, n)...)
	f()
	l := len(*b) - start - n
	for i := n - 1; i >= 0; i-- {
		(*b)[start+i] = byte(l)
		l >>= 8
	}
}

// reader consumes length-prefixed vectors.
type reader struct {
	b   []byte
	bad bool
}

func (r *reader) take(n int) []byte {
	if r.bad || len(r.b) < n {
		r.bad = true
		return nil
	}
	v := r.b[:n]
	r.b = r.b[n:]
	return v
}

func (r *reader) u8() byte {
	if v := r.take(1); v != nil {
		return v[0]
	}
	return 0
}

func (r *reader) u16() uint16 {
	if v := r.take(2); v != nil {
		return binary.BigEndian.Uint16(v)
	}
	return 0
}

func (r *reader) vec(n int) []byte {
	l := 0
	for _, x := range r.take(n) {
		l = l<<8 | int(x)
	}
	return r.take(l)
}

func (r *reader) done() bool { return !r.bad && len(r.b) == 0 }

var errDecode = errors.New("tls13: malformed handshake message")

func (c *Conn) clientHello(pub []byte) ([]byte, error) {
	var random, session [32]byte
	if _, err := io.ReadFull(c.rand(), random[:]); err != nil {
		return nil, err
	}
	if _, err := io.ReadFull(c.rand(), session[:]); err != nil {
		return nil, err
	}
	var b builder
	b.u8(hsClientHello)
	b.vec(3, func() {
		b.u16(0x0303)
		b.bytes(random[:])
		b.vec(1, func() { b.bytes(session[:]) }) // middlebox compatibility
		b.vec(2, func() {
			for _, s := range suites {
				b.u16(s.id)
			}
		})
		b.vec(1, func() { b.u8(0) })
		b.vec(2, func() {
			if name := c.cfg.ServerName; name != "" {
				if _, err := netip.ParseAddr(name); err != nil {
					b.u16(extServerName)
					b.vec(2, func() {
						b.vec(2, func() {
							b.u8(0)
							b.vec(2, func() { b.bytes([]byte(name)) })
						})
					})
				}
			}
			b.u16(extSupportedVersions)
			b.vec(2, func() { b.vec(1, func() { b.u16(version13) }) })
			b.u16(extSupportedGroups)
			b.vec(2, func() { b.vec(2, func() { b.u16(groupX25519) }) })
			b.u16(extKeyShare)
			b.vec(2, func() {
				b.vec(2, func() {
					b.u16(groupX25519)
					b.vec(2, func() { b.bytes(pub) })
				})
			})
			b.u16(extSignatureAlgorithms)
			b.vec(2, func() {
				b.vec(2, func() {
					for _, s := range supportedSchemes {
						b.u16(s)
					}
				})
			})
		})
	})
	return b, nil
}

func (c *Conn) handshake() error {
	var seed [32]byte
	if _, err := io.ReadFull(c.rand(), seed[:]); err != nil {
		return err
	}
	priv, err := ecdh.X25519().NewPrivateKey(seed[:])
	if err != nil {
		return err
	}
	hello, err := c.clientHello(priv.PublicKey().Bytes())
	if err != nil {
		return err
	}
	if err := c.writeRecord(recHandshake, hello); err != nil {
		return err
	}

	// ServerHello.
	msg, err := c.readHandshake()
	if err != nil {
		return err
	}
	if msg[0] != hsServerHello {
		return c.fail(alertUnexpectedMessage, fmt.Errorf("tls13: expected ServerHello, got message %d", msg[0]))
	}
	r := reader{b: msg[4:]}
	r.u16()
	random := r.take(32)
	echo := r.vec(1)
	suiteID := r.u16()
	comp := r.u8()
	exts := reader{b: r.vec(2)}
	if !r.done() {
		return c.fail(alertDecodeError, errDecode)
	}
	if bytes.Equal(random, hrrRandom) {
		return c.fail(alertHandshakeFailure, errors.New("tls13: server sent HelloRetryRequest (it must accept an X25519 key share)"))
	}
	var version uint16
	var share []byte
	for len(exts.b) > 0 && !exts.bad {
		typ, data := exts.u16(), reader{b: exts.vec(2)}
		switch typ {
		case extSupportedVersions:
			version = data.u16()
		case extKeyShare:
			if data.u16() != groupX25519 {
				return c.fail(alertIllegalParameter, errors.New("tls13: server chose a key share group we did not offer"))
			}
			share = data.vec(2)
		}
		if data.bad {
			return c.fail(alertDecodeError, errDecode)
		}
	}
	if exts.bad {
		return c.fail(alertDecodeError, errDecode)
	}
	if version != version13 {
		return c.fail(alertHandshakeFailure, errors.New("tls13: server does not speak TLS 1.3"))
	}
	if c.suite = suiteByID(suiteID); c.suite == nil || comp != 0 || !bytes.Equal(echo, hello[39:71]) {
		return c.fail(alertIllegalParameter, errors.New("tls13: bad ServerHello"))
	}
	if len(share) != 32 {
		return c.fail(alertIllegalParameter, errors.New("tls13: ServerHello has no X25519 key share"))
	}
	peerKey, err := ecdh.X25519().NewPublicKey(share)
	if err != nil {
		return c.fail(alertIllegalParameter, err)
	}
	shared, err := priv.ECDH(peerKey)
	if err != nil {
		return c.fail(alertIllegalParameter, err)
	}
	if len(c.in.hs) != 0 {
		return c.fail(alertUnexpectedMessage, errors.New("tls13: data after ServerHello in the same record"))
	}

	s := c.suite
	th := s.hash()
	th.Write(hello)
	th.Write(msg)
	early := s.extract(nil, nil)
	hsSecret := s.extract(s.deriveSecret(early, "derived", nil), shared)
	cHS := s.deriveSecret(hsSecret, "c hs traffic", th)
	sHS := s.deriveSecret(hsSecret, "s hs traffic", th)
	master := s.extract(s.deriveSecret(hsSecret, "derived", nil), nil)
	c.in.hc.setSecret(s, sHS)

	// Server flight.
	var (
		certReq []byte // request context, when asked for a certificate
		asked   bool
		reqSigs []uint16
	)
	next := func(want byte) ([]byte, error) {
		m, err := c.readHandshake()
		if err != nil {
			return nil, err
		}
		if m[0] != want {
			return nil, c.fail(alertUnexpectedMessage, fmt.Errorf("tls13: expected handshake message %d, got %d", want, m[0]))
		}
		return m, nil
	}
	if msg, err = next(hsEncryptedExtensions); err != nil {
		return err
	}
	th.Write(msg)
	if msg, err = c.readHandshake(); err != nil {
		return err
	}
	if msg[0] == hsCertificateRequest {
		r := reader{b: msg[4:]}
		certReq = r.vec(1)
		exts := reader{b: r.vec(2)}
		for len(exts.b) > 0 && !exts.bad {
			typ, data := exts.u16(), exts.vec(2)
			if typ == extSignatureAlgorithms {
				l := reader{b: data}
				list := reader{b: l.vec(2)}
				for len(list.b) > 0 && !list.bad {
					reqSigs = append(reqSigs, list.u16())
				}
			}
		}
		if !r.done() || exts.bad {
			return c.fail(alertDecodeError, errDecode)
		}
		asked = true
		th.Write(msg)
		if msg, err = c.readHandshake(); err != nil {
			return err
		}
	}
	if msg[0] != hsCertificate {
		return c.fail(alertUnexpectedMessage, fmt.Errorf("tls13: expected Certificate, got message %d", msg[0]))
	}
	if err := c.serverCertificates(msg); err != nil {
		return err
	}
	th.Write(msg)
	if msg, err = next(hsCertificateVerify); err != nil {
		return err
	}
	r = reader{b: msg[4:]}
	scheme, sig := r.u16(), r.vec(2)
	if !r.done() {
		return c.fail(alertDecodeError, errDecode)
	}
	if err := verifySignature(&c.peer[0].key, scheme, signedContent("TLS 1.3, server CertificateVerify", th), sig, false); err != nil {
		return c.fail(alertDecryptError, err)
	}
	th.Write(msg)
	if msg, err = next(hsFinished); err != nil {
		return err
	}
	if want := s.finished(sHS, th); !hmacEqual(msg[4:], want) {
		return c.fail(alertDecryptError, errors.New("tls13: server Finished does not verify"))
	}
	th.Write(msg)
	if len(c.in.hs) != 0 {
		return c.fail(alertUnexpectedMessage, errors.New("tls13: data after server Finished in the same record"))
	}
	cAP := s.deriveSecret(master, "c ap traffic", th)
	sAP := s.deriveSecret(master, "s ap traffic", th)
	c.in.hc.setSecret(s, sAP)

	// Client flight.
	if err := c.writeRecord(recChangeCipherSpec, []byte{1}); err != nil {
		return err
	}
	c.out.hc.setSecret(s, cHS)
	if asked {
		if err := c.clientCertificate(certReq, reqSigs, th); err != nil {
			return err
		}
	}
	fin := append([]byte{hsFinished, 0, 0, byte(s.size())}, s.finished(cHS, th)...)
	if err := c.writeRecord(recHandshake, fin); err != nil {
		return err
	}
	c.out.hc.setSecret(s, cAP)
	c.in.hs = nil
	return nil
}

func hmacEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	var v byte
	for i := range a {
		v |= a[i] ^ b[i]
	}
	return v == 0
}

// signedContent is what CertificateVerify signs (RFC 8446 4.4.3).
func signedContent(context string, th hash.Hash) []byte {
	b := bytes.Repeat([]byte{0x20}, 64)
	b = append(b, context...)
	b = append(b, 0)
	return th.Sum(b)
}

func (c *Conn) serverCertificates(msg []byte) error {
	r := reader{b: msg[4:]}
	if ctx := r.vec(1); len(ctx) != 0 {
		return c.fail(alertIllegalParameter, errors.New("tls13: server Certificate has a request context"))
	}
	list := reader{b: r.vec(3)}
	if !r.done() {
		return c.fail(alertDecodeError, errDecode)
	}
	for len(list.b) > 0 && !list.bad {
		der := list.vec(3)
		list.vec(2) // extensions (OCSP, SCT): ignored
		if list.bad {
			break
		}
		cert, err := ParseCertificate(append([]byte(nil), der...)) // don't pin the handshake buffer
		if err != nil {
			return c.fail(alertBadCertificate, err)
		}
		c.peer = append(c.peer, cert)
	}
	if list.bad || len(c.peer) == 0 {
		return c.fail(alertDecodeError, errors.New("tls13: server sent no certificate"))
	}
	if c.cfg.InsecureSkipVerify {
		return nil
	}
	if len(c.cfg.RootCAs) == 0 {
		return c.fail(alertBadCertificate, errors.New("tls13: no root CA configured"))
	}
	if err := verifyChain(c.peer, c.cfg.RootCAs, c.now(), oidEKUServer); err != nil {
		return c.fail(alertBadCertificate, err)
	}
	if !c.cfg.SkipNameVerification && c.cfg.ServerName != "" {
		if err := c.peer[0].VerifyHostname(c.cfg.ServerName); err != nil {
			return c.fail(alertBadCertificate, err)
		}
	}
	return nil
}

// clientCertificate sends Certificate (empty without one configured) and
// CertificateVerify.
func (c *Conn) clientCertificate(ctx []byte, sigs []uint16, th hash.Hash) error {
	have := len(c.cfg.Certificate) > 0 && c.cfg.Key != nil
	var b builder
	b.u8(hsCertificate)
	b.vec(3, func() {
		b.vec(1, func() { b.bytes(ctx) })
		b.vec(3, func() {
			if have {
				for _, der := range c.cfg.Certificate {
					b.vec(3, func() { b.bytes(der) })
					b.vec(2, func() {})
				}
			}
		})
	})
	th.Write(b)
	if err := c.writeRecord(recHandshake, b); err != nil {
		return err
	}
	if !have {
		return nil
	}
	scheme := c.cfg.Key.scheme()
	ok := len(sigs) == 0
	for _, s := range sigs {
		ok = ok || s == scheme
	}
	if !ok {
		return c.fail(alertHandshakeFailure, fmt.Errorf("tls13: server does not accept client key signature scheme %#04x", scheme))
	}
	sig, err := c.cfg.Key.sign(c.rand(), scheme, signedContent("TLS 1.3, client CertificateVerify", th))
	if err != nil {
		return c.fail(alertHandshakeFailure, err)
	}
	b = b[:0]
	b.u8(hsCertificateVerify)
	b.vec(3, func() {
		b.u16(scheme)
		b.vec(2, func() { b.bytes(sig) })
	})
	th.Write(b)
	return c.writeRecord(recHandshake, b)
}
