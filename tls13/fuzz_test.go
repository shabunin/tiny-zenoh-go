package tls13

import (
	"bufio"
	"bytes"
	"crypto"
	"crypto/ecdh"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"encoding/binary"
	"encoding/hex"
	"encoding/pem"
	"hash"
	"io"
	"math/big"
	"net"
	"net/netip"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// Fuzz targets for everything the client reads from a server: records,
// handshake messages, certificates and chains, keys, signatures and the
// elliptic curve arithmetic behind them. Malformed input must give an
// error, never a panic, a hang or a huge allocation; where the standard
// library has a reference (crypto/x509, crypto/ecdh, crypto/ecdsa,
// math/big) the results are compared too.
//
// Keys are fixed so that a saved crashing input reproduces in another
// process: certificates in the corpus must match the keys the fake server
// signs CertificateVerify with, and the fixed root.

var fuzzNow = time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)

// fuzzRSAKey is a throwaway 2048-bit test key.
var fuzzRSAKey = strings.ReplaceAll(`-----BEGIN RSA TESTING KEY-----
MIIEowIBAAKCAQEArb37jjhuvFn+mjIrrn0Nn/ReOUxQi4Qg/11VGwfdCWMiN3EG
tSP8fXMfO79Qe8sEWmkKaE7K00kULynLR0QEDJMdiFkOdkQuDKcZcx7vwPh8NvEh
GYUpodDNazUIYV/4g9se5t386Z4b6sJ1UziE8MQ6BjgNqNO/um7Ay4GH3bYLGCsJ
V/qMPcBMSLSBomlpWcHihAeOAQhNrjkaVsBbeRQ0qmbojo5hawrNxMGB3BKtJVdq
b/7GysFMdasbzYMAs4YBKCoKaGCWDB+6R3tPXo0x9CZIMMDwEg96IyCRNisiynbG
u9NSBOWf4iV6RT53zPkCKkiUogmQqBs6fW0PEwIDAQABAoIBABFM/8WzvOMomv/D
yzG+5iJtdQyDmumpqJeFNbpRWCNdKcUXjjFvee8MDaonbN5G4xIVr6GfZvcPKeFg
aguYrEaMR76SD6ybMXK1GQWrqPgwRj46WK9v4Stp3Wq1hzYbFmYa5cLbjBm/UsxX
vNMip6pJ//Uqk8GiBE+OQ8iAB4V+NwWYpct0zj/OTy9m2krPTesp0ijH7Dujh8IV
woZQ53xVVCENlBKW3XNHlL0Ic8bu81TzcZYiFsylsXG6Nf/D4VpGjN62AksZ73ta
WEZuFwcA5FkZo6ceN0HRuX5wl4Vy6A7opeYDnbBC0OJXk+I9EOYK6FqVu2Kxcodx
3yKXgyECgYEA5oCeyn1iuwkudI3JmyHzLlmp1omALfez6oF9BYCh9QymglxaVZhy
2e4Kd+T3a6UD5g8iw1ice8uI0HQLoDuHkC+d3S2hjPt+wdvQWM1jl53NZnb/0Pi/
M8LNcHTtWqMkJGl1aLYP1V3V1+W92AkE9Epw6VkBkQEjyonJGFpqi8sCgYEAwPYF
Z5Fy9GhoJNz3OtrHo3wQTmggtze0wRnYiWsMrUx3F8+rPsWp7JyUEPdblvwH3PrL
WC6KgQYa+0ijfqGFZhMXe5BZOGth7HWXz0i62Q23lR6cqyBlmDf/JG1kfBWPcmv2
HdHsc8q9M4C9sMG38HDjAt6wncbWWUQKy2zjsNkCgYEAna4e8iYUBB6jyNe1sonm
8S4lIL2vEcMBMMqj27/DCv88aLEEmslOyAYu4xWeq+SWZiq0fS2A3q69SNwZl+Mh
OWFZfJRajzb732GfhR/7oRwyBoYhObLwGViJcQSL2hUABGigCKGC81LdlnNzCvYg
jXzfAMICcE93THY1OpOfj0ECgYB6kkywk0XwVvoAoJQzQJIcGUzKYX6Wo74gA2j3
MZbl5nKoH3Rp9WmRMY0kkdwONp5QEsdkhnvYpaYyBNxGLhfs1SfPh8wE+MFFURVT
yKFYC0yYXs+qLHQck0VxQEPaRl/OkSHPo2+NxjUVKCxWwVc0VqD9S7PpJUs3fO9W
dR7VUQKBgD+MP2aFdnBXTDO/unMBzH4CHjLeulAJyB1fDsthocr9AK0GMSOEa9Z/
LvzSmnBJwxaNb9oG04Hr7GxXq5DDkLzUCSkDyCTFPcd/VhQsP0sm/rIDjtxEEPVp
bsP0f7q8f33cMzcG8fPDtCBUENsKEwhOwvNOMAQ2K7rFHWzWf5pp
-----END RSA TESTING KEY-----
`, "TESTING KEY", "PRIVATE KEY")

const (
	fkP256 = iota
	fkP384
	fkRSA
	fkEd25519
)

// fuzzPKI is the fixed key material: an Ed25519 root (deterministic
// signatures, so the root certificate is the same in every process),
// leaves of every key type, chains through intermediates and a client
// certificate.
type fuzzPKI struct {
	leafKey  [4]crypto.Signer
	rootDER  []byte
	root     *Certificate
	leafDER  [4][]byte // signed by the root
	viaRSA   [][]byte  // P-256 leaf, RSA intermediate (PKCS #1 signatures)
	viaP384  [][]byte  // P-256 leaf, P-384 intermediate
	odd      [][]byte  // extra shapes for the certificate parser
	hiKU     []byte    // key usage with undefined high bits
	client   [][]byte
	clientPK *PrivateKey
}

var fuzzKeys = sync.OnceValue(func() *fuzzPKI {
	must := func(err error) {
		if err != nil {
			panic(err)
		}
	}
	seed := func(s string) []byte { d := sha512.Sum512([]byte(s)); return d[:] }
	p := &fuzzPKI{}
	p256k, err := ecdsa.ParseRawPrivateKey(elliptic.P256(), seed("p256")[:32])
	must(err)
	p384k, err := ecdsa.ParseRawPrivateKey(elliptic.P384(), seed("p384")[:48])
	must(err)
	blk, _ := pem.Decode([]byte(fuzzRSAKey))
	rsak, err := x509.ParsePKCS1PrivateKey(blk.Bytes)
	must(err)
	p.leafKey = [4]crypto.Signer{p256k, p384k, rsak, ed25519.NewKeyFromSeed(seed("leaf ed25519")[:32])}
	rootKey := ed25519.NewKeyFromSeed(seed("root")[:32])

	notBefore := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	notAfter := time.Date(2035, 1, 1, 0, 0, 0, 0, time.UTC)
	serial := int64(0)
	type ca struct {
		tmpl *x509.Certificate
		key  crypto.Signer
	}
	issue := func(tmpl *x509.Certificate, pub crypto.PublicKey, parent *ca) []byte {
		serial++
		tmpl.SerialNumber = big.NewInt(serial)
		tmpl.NotBefore, tmpl.NotAfter = notBefore, notAfter
		der, err := x509.CreateCertificate(rand.Reader, tmpl, parent.tmpl, pub, parent.key)
		must(err)
		return der
	}
	caT := func(name string) *x509.Certificate {
		return &x509.Certificate{Subject: pkix.Name{CommonName: name}, IsCA: true, BasicConstraintsValid: true,
			KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	}
	leafT := func(name string, eku x509.ExtKeyUsage) *x509.Certificate {
		return &x509.Certificate{Subject: pkix.Name{CommonName: name}, DNSNames: []string{"localhost", "*.example.org"},
			IPAddresses: []net.IP{net.IPv4(127, 0, 0, 1), net.ParseIP("::1")}, KeyUsage: x509.KeyUsageDigitalSignature,
			ExtKeyUsage: []x509.ExtKeyUsage{eku}}
	}
	root := &ca{caT("fuzz root"), rootKey}
	root.tmpl.SerialNumber = big.NewInt(1000)
	root.tmpl.NotBefore, root.tmpl.NotAfter = notBefore, notAfter
	p.rootDER, err = x509.CreateCertificate(rand.Reader, root.tmpl, root.tmpl, rootKey.Public(), rootKey)
	must(err)
	p.root, err = ParseCertificate(p.rootDER)
	must(err)
	for i, k := range p.leafKey {
		p.leafDER[i] = issue(leafT("srv", x509.ExtKeyUsageServerAuth), k.Public(), root)
	}
	midRSA := &ca{caT("rsa intermediate"), rsak}
	p.viaRSA = [][]byte{nil, issue(midRSA.tmpl, rsak.Public(), root)}
	p.viaRSA[0] = issue(leafT("srv", x509.ExtKeyUsageServerAuth), p256k.Public(), midRSA)
	midP384 := &ca{caT("p384 intermediate"), p384k}
	p.viaP384 = [][]byte{nil, issue(midP384.tmpl, p384k.Public(), root)}
	p.viaP384[0] = issue(leafT("srv", x509.ExtKeyUsageServerAuth), p256k.Public(), midP384)
	p.client = [][]byte{issue(leafT("client", x509.ExtKeyUsageClientAuth), p256k.Public(), root)}
	pk8, err := x509.MarshalPKCS8PrivateKey(p256k)
	must(err)
	p.clientPK, err = parsePKCS8(pk8)
	must(err)

	// Odd but valid shapes: critical and unknown extensions, no EKU, any
	// EKU, IPv6 and v4-in-v6 addresses, GeneralizedTime, policies.
	odd := &x509.Certificate{Subject: pkix.Name{CommonName: "odd", Organization: []string{"o"}},
		DNSNames: []string{"Example.COM."}, IPAddresses: []net.IP{net.ParseIP("::ffff:10.0.0.1"), net.ParseIP("2001:db8::1")},
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageAny}, UnknownExtKeyUsage: []asn1.ObjectIdentifier{{1, 2, 3}},
		Policies:        []x509.OID{mustOID("1.2.3.4")},
		ExtraExtensions: []pkix.Extension{{Id: asn1.ObjectIdentifier{1, 2, 3, 4}, Value: []byte{5, 0}}},
	}
	p.odd = append(p.odd, issue(odd, p384k.Public(), root))
	gen := &x509.Certificate{Subject: pkix.Name{CommonName: "gen"}, KeyUsage: x509.KeyUsageKeyEncipherment}
	der := issue(gen, rsak.Public(), &ca{caT("rsa intermediate"), rsak})
	p.odd = append(p.odd, der)
	crit := &x509.Certificate{Subject: pkix.Name{CommonName: "crit"},
		ExtraExtensions: []pkix.Extension{{Id: asn1.ObjectIdentifier{1, 2, 3, 5}, Critical: true, Value: []byte{5, 0}}}}
	p.odd = append(p.odd, issue(crit, rootKey.Public(), root))
	// Key usage with bit 63 set (once read as a negative int: "no
	// extension"), and only keyEncipherment of the defined bits.
	ku, err := asn1.Marshal(asn1.BitString{Bytes: []byte{0x20, 0, 0, 0, 0, 0, 0, 1}, BitLength: 64})
	must(err)
	hiKU := leafT("srv", x509.ExtKeyUsageServerAuth)
	hiKU.ExtraExtensions = []pkix.Extension{{Id: asn1.ObjectIdentifier{2, 5, 29, 15}, Critical: true, Value: ku}}
	p.hiKU = issue(hiKU, p256k.Public(), root)
	p.odd = append(p.odd, p.hiKU)
	gt := caT("generalized")
	gt.SerialNumber = big.NewInt(77)
	gt.NotBefore, gt.NotAfter = notBefore, time.Date(2051, 1, 1, 0, 0, 0, 0, time.UTC) // GeneralizedTime
	der, err = x509.CreateCertificate(rand.Reader, gt, root.tmpl, p256k.Public(), rootKey)
	must(err)
	p.odd = append(p.odd, der)
	return p
})

func mustOID(s string) x509.OID {
	o, err := x509.ParseOID(s)
	if err != nil {
		panic(err)
	}
	return o
}

// detRand is a fixed "random" stream: the client's key share, random and
// session ID are the same in every run, so a captured server flight
// replays.
type detRand struct{ n byte }

func (r *detRand) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = r.n*31 + 7
		r.n++
	}
	return len(p), nil
}

// fuzzConfig: conf bit 0 skips verification, bit 1 adds a client
// certificate, bits 2-3 pick the server key, bits 4-5 the server name, bit
// 6 an unset clock.
func fuzzConfig(conf byte) *Config {
	p := fuzzKeys()
	cfg := &Config{Rand: &detRand{}, Time: func() time.Time { return fuzzNow }, RootCAs: []*Certificate{p.root},
		InsecureSkipVerify: conf&1 != 0}
	if conf&2 != 0 {
		cfg.Certificate, cfg.Key = p.client, p.clientPK
	}
	cfg.ServerName = [4]string{"localhost", "127.0.0.1", "a.example.org", ""}[conf>>4&3]
	if conf&0x40 != 0 {
		cfg.Time = func() time.Time { return time.Unix(0, 0) }
	}
	return cfg
}

// fuzzServer is the client's connection: it plays a script of server
// records. Each step is a control byte, a 2-byte length and a body:
//
//	0xff        the body as raw bytes
//	0xfe        a CertificateVerify signed with the server key over the
//	            transcript so far (the body is ignored)
//	0xfd        a valid server Finished
//	otherwise   a record of that content type: protected with the client's
//	            current read keys once it has them (except change_cipher_spec),
//	            plaintext before
//
// Records are made when the client wants to read, with its keys at that
// moment, so the script never needs secrets.
type fuzzServer struct {
	c      *Conn
	script []byte
	out    []byte
	hello  []byte // ClientHello, from the client's first write
	th     []byte // handshake messages sent, for the transcript
	signer crypto.Signer
	hc     halfConn
	sent   []byte // application data the client may read, in order
}

func (s *fuzzServer) Read(p []byte) (int, error) {
	for len(s.out) == 0 {
		if len(s.script) == 0 {
			return 0, io.EOF
		}
		s.step()
	}
	n := copy(p, s.out)
	s.out = s.out[n:]
	return n, nil
}

func (s *fuzzServer) Write(p []byte) (int, error) {
	if s.hello == nil && len(p) > 5 {
		s.hello = append([]byte(nil), p[5:]...)
	}
	return len(p), nil
}

func (s *fuzzServer) Close() error { return nil }

func (s *fuzzServer) step() {
	ctl := s.script[0]
	n, rest := 0, s.script[1:]
	if len(rest) >= 2 {
		n, rest = int(binary.BigEndian.Uint16(rest)), rest[2:]
	}
	n = min(n, len(rest))
	body := rest[:n]
	s.script = rest[n:]
	switch ctl {
	case 0xff:
		s.out = append(s.out, body...)
		return
	case 0xfe:
		body, ctl = s.certificateVerify(), recHandshake
	case 0xfd:
		body, ctl = s.finished(), recHandshake
	}
	if body == nil && ctl == recHandshake {
		return
	}
	if ctl == recHandshake {
		s.th = append(s.th, body...)
	}
	s.record(ctl, body)
}

func (s *fuzzServer) transcript() hash.Hash {
	th := s.c.suite.hash()
	th.Write(s.hello)
	th.Write(s.th)
	return th
}

func (s *fuzzServer) certificateVerify() []byte {
	if s.c.suite == nil || s.signer == nil {
		return nil
	}
	scheme, sig := fuzzSign(s.signer, signedContent("TLS 1.3, server CertificateVerify", s.transcript()))
	var b builder
	b.u8(hsCertificateVerify)
	b.vec(3, func() {
		b.u16(scheme)
		b.vec(2, func() { b.bytes(sig) })
	})
	return b
}

func (s *fuzzServer) finished() []byte {
	if s.c.suite == nil || s.c.in.hc.secret == nil {
		return nil
	}
	fin := s.c.suite.finished(s.c.in.hc.secret, s.transcript())
	return append([]byte{hsFinished, 0, 0, byte(len(fin))}, fin...)
}

func (s *fuzzServer) record(typ byte, body []byte) {
	in := &s.c.in.hc
	if in.aead == nil || typ == recChangeCipherSpec {
		s.out = append(s.out, typ, 3, 3, byte(len(body)>>8), byte(len(body)))
		s.out = append(s.out, body...)
		return
	}
	if s.hc.suite != in.suite || !bytes.Equal(s.hc.secret, in.secret) {
		s.hc.setSecret(in.suite, in.secret)
	}
	s.hc.seq = in.seq
	inner := append(append([]byte(nil), body...), typ)
	size := len(inner) + s.hc.aead.Overhead()
	hdr := []byte{recApplicationData, 3, 3, byte(size >> 8), byte(size)}
	s.out = append(s.out, hdr...)
	s.out = s.hc.aead.Seal(s.out, s.hc.nonce(), inner, hdr)
	// What the client returns from Read for it, if anything.
	i := len(inner) - 1
	for i >= 0 && inner[i] == 0 {
		i--
	}
	if i >= 0 && inner[i] == recApplicationData {
		s.sent = append(s.sent, inner[:i]...)
	}
}

// fuzzSign signs a CertificateVerify the way a server with key k would.
func fuzzSign(k crypto.Signer, msg []byte) (uint16, []byte) {
	var scheme uint16
	var sig []byte
	var err error
	switch k := k.(type) {
	case *ecdsa.PrivateKey:
		if k.Curve == elliptic.P256() {
			d := sha256.Sum256(msg)
			scheme = schemeECDSASHA256
			sig, err = ecdsa.SignASN1(rand.Reader, k, d[:])
		} else {
			d := sha512.Sum384(msg)
			scheme = schemeECDSASHA384
			sig, err = ecdsa.SignASN1(rand.Reader, k, d[:])
		}
	case *rsa.PrivateKey:
		d := sha256.Sum256(msg)
		scheme = schemePSSSHA256
		sig, err = rsa.SignPSS(rand.Reader, k, crypto.SHA256, d[:], &rsa.PSSOptions{SaltLength: rsa.PSSSaltLengthEqualsHash})
	case ed25519.PrivateKey:
		scheme, sig = schemeEd25519, ed25519.Sign(k, msg)
	}
	if err != nil {
		panic(err)
	}
	return scheme, sig
}

// step encodes one script step.
func step(ctl byte, body []byte) []byte {
	return append([]byte{ctl, byte(len(body) >> 8), byte(len(body))}, body...)
}

// runHandshake drives a client handshake against a script, then reads
// what follows; it returns the data read or the handshake error.
func runHandshake(t *testing.T, conf byte, script []byte) ([]byte, error) {
	s := &fuzzServer{script: script, signer: fuzzKeys().leafKey[conf>>2&3]}
	c := &Conn{conn: s, cfg: fuzzConfig(conf)}
	s.c = c
	if err := c.handshake(); err != nil {
		return nil, err
	}
	if len(c.peer) == 0 {
		t.Fatal("handshake done without a server certificate")
	}
	_, _ = c.Write([]byte("ping"))
	sent := len(s.sent)
	got := readAll(c, 1+int(conf>>7)*1000)
	if !bytes.HasPrefix(s.sent[sent:], got) {
		t.Fatalf("read %q, server sent %q", got, s.sent[sent:])
	}
	_ = c.Close()
	return got, nil
}

// readAll reads until an error, n bytes at a time.
func readAll(c *Conn, n int) []byte {
	var got []byte
	buf := make([]byte, n)
	for {
		k, err := c.Read(buf)
		got = append(got, buf[:k]...)
		if err != nil {
			return got
		}
	}
}

// recConn records what the server sends.
type recConn struct {
	net.Conn
	got bytes.Buffer
}

func (r *recConn) Read(p []byte) (int, error) {
	n, err := r.Conn.Read(p)
	r.got.Write(p[:n])
	return n, err
}

// captureFlight runs our client (configured by conf) against crypto/tls
// and returns the server's bytes, decrypted with the server's key log, as
// a script; auto also has CertificateVerify and Finished generated.
func captureFlight(tb testing.TB, conf byte, chain [][]byte, key crypto.Signer, clientAuth bool) (raw, auto []byte) {
	tb.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatal(err)
	}
	defer ln.Close()
	var keyLog bytes.Buffer
	scfg := &tls.Config{MinVersion: tls.VersionTLS13, KeyLogWriter: &keyLog,
		Certificates: []tls.Certificate{{Certificate: chain, PrivateKey: key}}}
	if clientAuth {
		scfg.ClientAuth = tls.RequestClientCert
	}
	done := make(chan error, 1)
	go func() {
		nc, err := ln.Accept()
		if err != nil {
			done <- err
			return
		}
		s := tls.Server(nc, scfg)
		if err = s.Handshake(); err == nil {
			buf := make([]byte, 4)
			if _, err = io.ReadFull(s, buf); err == nil {
				_, err = s.Write([]byte("hello from crypto/tls"))
			}
		}
		_ = s.Close()
		done <- err
	}()
	nc, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		tb.Fatal(err)
	}
	rc := &recConn{Conn: nc}
	c, err := Client(rc, fuzzConfig(conf))
	if err != nil {
		tb.Fatal(err)
	}
	if _, err := c.Write([]byte("ping")); err != nil {
		tb.Fatal(err)
	}
	if got := readAll(c, 100); string(got) != "hello from crypto/tls" {
		tb.Fatalf("read %q", got)
	}
	_ = c.Close()
	if err := <-done; err != nil {
		tb.Fatal(err)
	}

	secrets := map[string][]byte{}
	sc := bufio.NewScanner(&keyLog)
	for sc.Scan() {
		if f := strings.Fields(sc.Text()); len(f) == 3 {
			secrets[f[0]], _ = hex.DecodeString(f[2])
		}
	}
	s := suiteByID(c.CipherSuite())
	var hs, app halfConn
	hs.setSecret(s, secrets["SERVER_HANDSHAKE_TRAFFIC_SECRET"])
	app.setSecret(s, secrets["SERVER_TRAFFIC_SECRET_0"])
	cur := &hs
	b := rc.got.Bytes()
	for len(b) >= 5 {
		typ, n := b[0], int(binary.BigEndian.Uint16(b[3:5]))
		hdr, body := b[:5], b[5:5+n]
		b = b[5+n:]
		if typ != recApplicationData {
			raw = append(raw, step(typ, body)...)
			auto = append(auto, step(typ, body)...)
			continue
		}
		plain, err := cur.aead.Open(nil, cur.nonce(), body, hdr)
		if err != nil && cur == &hs {
			cur = &app
			plain, err = cur.aead.Open(nil, cur.nonce(), body, hdr)
		}
		if err != nil {
			tb.Fatal("cannot decrypt the captured flight")
		}
		i := len(plain) - 1
		for plain[i] == 0 {
			i--
		}
		typ, plain = plain[i], plain[:i]
		raw = append(raw, step(typ, plain)...)
		switch {
		case typ == recHandshake && len(plain) > 4 && plain[0] == hsCertificateVerify:
			auto = append(auto, step(0xfe, nil)...)
		case typ == recHandshake && len(plain) > 4 && plain[0] == hsFinished && cur == &hs:
			auto = append(auto, step(0xfd, nil)...)
		default:
			auto = append(auto, step(typ, plain)...)
		}
	}
	return raw, auto
}

func FuzzHandshake(f *testing.F) {
	p := fuzzKeys()
	add := func(conf byte, script []byte) { f.Add(conf, script) }
	for k := range 4 {
		chain := [][]byte{p.leafDER[k]}
		conf := byte(k << 2)
		raw, auto := captureFlight(f, conf, chain, p.leafKey[k], false)
		add(conf, raw)
		add(conf, auto)
		if k == fkP256 {
			add(conf|0x81, auto) // no verification, bigger reads
			add(conf|0x10, raw)  // IP name: ClientHello changes, so use the generated messages
			add(conf|0x10, auto)
			add(conf|0x20, auto)
			add(conf|0x40, auto)
			// The other suite, chosen in a mutated ServerHello.
			if i := bytes.Index(auto, []byte{0x13, 0x01, 0x00}); i > 0 {
				s := slices.Clone(auto)
				s[i+1] = 0x02
				add(conf, s)
			}
			// Post-handshake messages: tickets, KeyUpdate (requested and
			// not), padding, alerts, a change_cipher_spec.
			post := slices.Clone(auto)
			post = append(post, step(recHandshake, []byte{hsNewSessionTicket, 0, 0, 1, 0})...)
			post = append(post, step(recHandshake, []byte{hsKeyUpdate, 0, 0, 1, 1})...)
			post = append(post, step(recApplicationData, []byte("after update"))...)
			post = append(post, step(recHandshake, []byte{hsKeyUpdate, 0, 0, 1, 0})...)
			post = append(post, step(0, []byte("padded\x17\x00\x00"))...)
			post = append(post, step(recAlert, []byte{1, 0})...)
			add(conf, post)
			add(conf, append(slices.Clone(auto), step(recChangeCipherSpec, []byte{1})...))
			add(conf, append(slices.Clone(auto), step(recAlert, []byte{2, 40})...))
			add(conf, append(slices.Clone(auto), step(0xff, []byte{23, 3, 3, 0, 1, 0})...))
		}
	}
	for _, chain := range [][][]byte{p.viaRSA, p.viaP384} {
		raw, auto := captureFlight(f, 0, chain, p.leafKey[fkP256], false)
		add(0, raw)
		add(0, auto)
	}
	// CertificateRequest, with and without a client certificate.
	for _, conf := range []byte{0, 2} {
		raw, auto := captureFlight(f, conf, [][]byte{p.leafDER[fkP256]}, p.leafKey[fkP256], true)
		add(conf, raw)
		add(conf, auto)
	}
	// A HelloRetryRequest; garbage.
	_, auto := captureFlight(f, 0, [][]byte{p.leafDER[fkP256]}, p.leafKey[fkP256], false)
	hrr := slices.Clone(auto)
	copy(hrr[3+6:], hrrRandom)
	add(0, hrr)
	add(0, step(recAlert, []byte{2, 70}))
	add(0, step(0xff, []byte{22, 3, 3, 0xff, 0xff}))
	add(0, nil)

	f.Fuzz(func(t *testing.T, conf byte, script []byte) {
		_, _ = runHandshake(t, conf, script)
	})
}

// TestFuzzHarness: captured crypto/tls flights replay through the fake
// server, as they are and with generated CertificateVerify and Finished,
// so the fuzzer starts from complete handshakes.
func TestFuzzHarness(t *testing.T) {
	p := fuzzKeys()
	type tc struct {
		name       string
		conf       byte
		chain      [][]byte
		key        crypto.Signer
		clientAuth bool
	}
	cases := []tc{
		{"rsa chain", 0, p.viaRSA, p.leafKey[fkP256], false},
		{"p384 chain", 0x10, p.viaP384, p.leafKey[fkP256], false},
		{"client auth", 2, [][]byte{p.leafDER[fkP256]}, p.leafKey[fkP256], true},
		{"no client cert", 0x20, [][]byte{p.leafDER[fkP256]}, p.leafKey[fkP256], true},
	}
	for k, name := range []string{"p256", "p384", "rsa", "ed25519"} {
		cases = append(cases, tc{name, byte(k << 2), [][]byte{p.leafDER[k]}, p.leafKey[k], false})
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw, auto := captureFlight(t, tc.conf, tc.chain, tc.key, tc.clientAuth)
			for _, script := range [][]byte{raw, auto} {
				got, err := runHandshake(t, tc.conf, script)
				if err != nil || string(got) != "hello from crypto/tls" {
					t.Fatalf("replay: %v, read %q", err, got)
				}
			}
			if bytes.Equal(raw, auto) {
				t.Fatal("nothing generated")
			}
		})
	}
}

// FuzzRecords: the record layer and post-handshake messages on an
// established connection (much faster than going through a handshake).
func FuzzRecords(f *testing.F) {
	f.Add(byte(0), step(recApplicationData, []byte("hello")))
	f.Add(byte(1), slices.Concat(
		step(recApplicationData, []byte("hello")),
		step(recHandshake, []byte{hsNewSessionTicket, 0, 0, 8, 0, 0, 0, 1, 0, 0, 0, 2}),
		step(recHandshake, []byte{hsKeyUpdate, 0, 0, 1, 1}),
		step(recApplicationData, []byte("after update")),
		step(recHandshake, []byte{hsKeyUpdate, 0, 0, 1, 0}),
		step(recApplicationData, bytes.Repeat([]byte{'x'}, maxPlaintext)),
		step(recAlert, []byte{1, 0}),
	))
	f.Add(byte(2), slices.Concat(
		step(0, []byte("padded\x17\x00\x00\x00")),
		step(0, []byte{0, 0, 0}),
	))
	f.Add(byte(3), slices.Concat(
		step(recHandshake, []byte{hsKeyUpdate, 0, 0}),
		step(recHandshake, []byte{1, hsKeyUpdate, 0, 0, 1, 1}),
	))
	f.Add(byte(4), slices.Concat(
		step(recChangeCipherSpec, []byte{1}),
		step(recAlert, []byte{2, 40}),
	))
	f.Add(byte(5), slices.Concat(
		step(recApplicationData, bytes.Repeat([]byte{'x'}, maxPlaintext+1)),
	))
	f.Add(byte(6), slices.Concat(
		step(0xff, []byte{23, 3, 3, 0x41, 0x01}),
		step(0xff, []byte{23, 3, 3, 0, 0}),
		step(0xff, []byte{23, 4, 3, 0, 0}),
		step(recHandshake, []byte{hsNewSessionTicket, 0x01, 0, 0}),
	))
	f.Fuzz(func(t *testing.T, conf byte, script []byte) {
		s := &suites[conf&1]
		c := &Conn{cfg: &Config{Rand: &detRand{}}, suite: s}
		c.in.hc.setSecret(s, bytes.Repeat([]byte{1}, s.size()))
		c.out.hc.setSecret(s, bytes.Repeat([]byte{2}, s.size()))
		srv := &fuzzServer{c: c, script: script}
		c.conn = srv
		got := readAll(c, 1+int(conf>>1)*97)
		if !bytes.HasPrefix(srv.sent, got) {
			t.Fatalf("read %q, server sent %q", got, srv.sent)
		}
	})
}

// certMsg is a Certificate handshake message for a chain.
func certMsg(chain [][]byte) []byte {
	var b builder
	b.u8(hsCertificate)
	b.vec(3, func() {
		b.vec(1, func() {})
		b.vec(3, func() {
			for _, der := range chain {
				b.vec(3, func() { b.bytes(der) })
				b.vec(2, func() {})
			}
		})
	})
	return b
}

// FuzzServerCertificates: the Certificate message, chain verification
// against the fixed root and the name check.
func FuzzServerCertificates(f *testing.F) {
	p := fuzzKeys()
	for k := range 4 {
		f.Add(byte(0), certMsg([][]byte{p.leafDER[k]})[4:])
	}
	f.Add(byte(0x10), certMsg(p.viaRSA)[4:])
	f.Add(byte(0x20), certMsg(p.viaP384)[4:])
	f.Add(byte(0), certMsg([][]byte{p.viaRSA[0], p.viaP384[1], p.viaRSA[1], p.rootDER})[4:])
	f.Add(byte(0), certMsg(append([][]byte{p.client[0]}, p.odd...))[4:])
	f.Add(byte(0x40), certMsg([][]byte{p.odd[0]})[4:])
	f.Fuzz(func(t *testing.T, conf byte, body []byte) {
		c := &Conn{conn: &fuzzServer{}, cfg: fuzzConfig(conf)}
		msg := append([]byte{hsCertificate, 0, 0, 0}, body...)
		if err := c.serverCertificates(msg); err == nil && len(c.peer) == 0 {
			t.Fatal("accepted an empty chain")
		}
	})
}

// FuzzParseCertificate: the X.509 parser, compared with crypto/x509
// where both accept a certificate.
func FuzzParseCertificate(f *testing.F) {
	p := fuzzKeys()
	f.Add(p.rootDER)
	for _, d := range p.leafDER {
		f.Add(d)
	}
	for _, d := range slices.Concat(p.viaRSA, p.viaP384, p.client, p.odd) {
		f.Add(d)
	}
	f.Fuzz(func(t *testing.T, der []byte) {
		c, err := ParseCertificate(der)
		if err != nil {
			return
		}
		for _, h := range []string{"localhost", "127.0.0.1", "[::1]", "a.example.org", "example.com", ""} {
			_ = c.VerifyHostname(h)
		}
		_ = c.checkTime(fuzzNow)
		_ = c.checkSignatureFrom(c)
		_ = verifyChain([]*Certificate{c, c}, []*Certificate{p.root}, fuzzNow, oidEKUServer)
		_ = verifyChain([]*Certificate{c}, []*Certificate{c}, fuzzNow, oidEKUClient)

		ref, err := x509.ParseCertificate(der)
		if err != nil {
			return
		}
		if !bytes.Equal(c.rawTBS, ref.RawTBSCertificate) || !bytes.Equal(c.rawIssuer, ref.RawIssuer) ||
			!bytes.Equal(c.rawSubject, ref.RawSubject) || !bytes.Equal(c.signature, ref.Signature) {
			t.Fatal("raw fields differ from crypto/x509")
		}
		if !c.NotBefore.Equal(ref.NotBefore) || !c.NotAfter.Equal(ref.NotAfter) {
			t.Fatalf("validity %v-%v, crypto/x509 %v-%v", c.NotBefore, c.NotAfter, ref.NotBefore, ref.NotAfter)
		}
		if !slices.Equal(c.DNSNames, ref.DNSNames) {
			t.Fatalf("DNS names %q, crypto/x509 %q", c.DNSNames, ref.DNSNames)
		}
		var ips []netip.Addr
		for _, ip := range ref.IPAddresses {
			a, _ := netip.AddrFromSlice(ip)
			ips = append(ips, a.Unmap())
		}
		if !slices.Equal(c.IPs, ips) {
			t.Fatalf("IPs %v, crypto/x509 %v", c.IPs, ips)
		}
		if c.isCA != (ref.BasicConstraintsValid && ref.IsCA) {
			t.Fatalf("isCA %v, crypto/x509 %v", c.isCA, ref.IsCA)
		}
		hasKU := slices.ContainsFunc(ref.Extensions, func(e pkix.Extension) bool { return e.Id.Equal(asn1.ObjectIdentifier{2, 5, 29, 15}) })
		if hasKU != (c.keyUsage >= 0) || hasKU && c.keyUsage&0x1ff != int(ref.KeyUsage) {
			t.Fatalf("key usage %#x, crypto/x509 %#x (extension %v)", c.keyUsage, ref.KeyUsage, hasKU)
		}
		for _, eku := range []struct {
			oid []byte
			ref x509.ExtKeyUsage
		}{{oidEKUServer, x509.ExtKeyUsageServerAuth}, {oidEKUClient, x509.ExtKeyUsageClientAuth}} {
			want := len(ref.ExtKeyUsage) == 0 && len(ref.UnknownExtKeyUsage) == 0 ||
				slices.Contains(ref.ExtKeyUsage, eku.ref) || slices.Contains(ref.ExtKeyUsage, x509.ExtKeyUsageAny)
			if c.hasEKU(eku.oid) != want {
				t.Fatalf("EKU %x: %v, crypto/x509 %v", eku.oid, !want, want)
			}
		}
		switch k := ref.PublicKey.(type) {
		case *rsa.PublicKey:
			if c.key.typ != keyRSA || c.key.rsa.n.Cmp(k.N) != 0 || c.key.rsa.e != k.E {
				t.Fatal("RSA key differs from crypto/x509")
			}
		case *ecdsa.PublicKey:
			want := map[elliptic.Curve]keyType{elliptic.P256(): keyP256, elliptic.P384(): keyP384}[k.Curve]
			if c.key.typ != want || c.key.ec.x.Cmp(k.X) != 0 || c.key.ec.y.Cmp(k.Y) != 0 {
				t.Fatal("EC key differs from crypto/x509")
			}
		case ed25519.PublicKey:
			if c.key.typ != keyEd25519 || !bytes.Equal(c.key.raw, k) {
				t.Fatal("Ed25519 key differs from crypto/x509")
			}
		default:
			t.Fatalf("crypto/x509 key %T", k)
		}
	})
}

// parseSPKI parses a DER SubjectPublicKeyInfo.
func parseSPKI(der []byte) (*publicKey, error) {
	spki, _, err := expect(der, 0x30)
	if err != nil {
		return nil, err
	}
	var k publicKey
	return &k, k.parse(spki)
}

// FuzzVerifySignature: SubjectPublicKeyInfo parsing and signature checks
// for every scheme (DER ECDSA signatures, RSA PSS and PKCS #1, Ed25519).
func FuzzVerifySignature(f *testing.F) {
	p := fuzzKeys()
	msg := []byte("tls13 fuzz")
	add := func(spki []byte, scheme uint16, sig []byte, cert bool) {
		k, err := parseSPKI(spki)
		if err == nil {
			err = verifySignature(k, scheme, msg, sig, cert)
		}
		if err != nil {
			f.Fatalf("seed %#04x: %v", scheme, err)
		}
		f.Add(spki, scheme, msg, sig, cert)
	}
	for k, key := range p.leafKey {
		spki, err := x509.MarshalPKIXPublicKey(key.Public())
		if err != nil {
			f.Fatal(err)
		}
		scheme, sig := fuzzSign(key, msg)
		add(spki, scheme, sig, false)
		switch k {
		case fkRSA:
			d := sha256.Sum256(msg)
			sig, _ := rsa.SignPKCS1v15(rand.Reader, key.(*rsa.PrivateKey), crypto.SHA256, d[:])
			add(spki, schemePKCS1SHA256, sig, true)
			f.Add(spki, schemePKCS1SHA256, msg, sig, false)
		case fkP384:
			d := sha512.Sum512(msg)
			sig, _ := ecdsa.SignASN1(rand.Reader, key.(*ecdsa.PrivateKey), d[:])
			add(spki, schemeECDSASHA512, sig, true)
		}
	}
	f.Fuzz(func(t *testing.T, spki []byte, scheme uint16, msg, sig []byte, cert bool) {
		k, err := parseSPKI(spki)
		if err != nil {
			return
		}
		_ = verifySignature(k, scheme, msg, sig, cert)
	})
}

// fuzzCurve picks a curve and its crypto/ecdh twin.
func fuzzCurve(wide bool) (*curve, ecdh.Curve, elliptic.Curve) {
	if wide {
		return p384Curve(), ecdh.P384(), elliptic.P384()
	}
	return p256Curve(), ecdh.P256(), elliptic.P256()
}

func encodePoint(c *curve, p *ecPoint) []byte {
	b := make([]byte, 1+2*c.size)
	b[0] = 4
	p.x.FillBytes(b[1 : 1+c.size])
	p.y.FillBytes(b[1+c.size:])
	return b
}

// scalarMod reduces b mod n into a fixed-size big-endian scalar.
func scalarMod(c *curve, b []byte) (*big.Int, []byte) {
	v := new(big.Int).SetBytes(b)
	v.Mod(v, c.n)
	return v, v.FillBytes(make([]byte, c.size))
}

// FuzzDecodePoint: SEC 1 point decoding against crypto/ecdh.
func FuzzDecodePoint(f *testing.F) {
	for _, wide := range []bool{false, true} {
		c, ec, _ := fuzzCurve(wide)
		k, _ := ec.NewPrivateKey(bytes.Repeat([]byte{3}, c.size))
		f.Add(wide, k.PublicKey().Bytes())
		f.Add(wide, encodePoint(c, &c.g))
		f.Add(wide, append([]byte{4}, c.p.FillBytes(make([]byte, 2*c.size))...))
		f.Add(wide, []byte{0})
	}
	f.Fuzz(func(t *testing.T, wide bool, b []byte) {
		c, ec, _ := fuzzCurve(wide)
		pt, err := c.decodePoint(b)
		_, rerr := ec.NewPublicKey(b)
		if (err == nil) != (rerr == nil) {
			t.Fatalf("decodePoint: %v, crypto/ecdh: %v", err, rerr)
		}
		if err == nil && !bytes.Equal(encodePoint(c, pt), b) {
			t.Fatal("decoded point re-encodes differently")
		}
	})
}

// FuzzScalarMult: u1·G + u2·Q (Jacobian additions, doublings and their
// special cases) against crypto/ecdh, with Q = q·G so the edge cases (Q =
// ±G, u1·G = -u2·Q, ...) are reachable.
func FuzzScalarMult(f *testing.F) {
	for _, wide := range []bool{false, true} {
		c, _, _ := fuzzCurve(wide)
		nm1 := new(big.Int).Sub(c.n, bigOne).Bytes()
		f.Add(wide, []byte{1}, []byte{1}, []byte{1})
		f.Add(wide, []byte{1}, nm1, []byte{1})
		f.Add(wide, []byte{2}, []byte{1}, nm1)
		f.Add(wide, []byte{0}, []byte{0}, []byte{5})
		f.Add(wide, nm1, nm1, nm1)
		f.Add(wide, []byte{0xde, 0xad, 0xbe, 0xef}, []byte{7}, []byte{0x1, 0x2, 0x3})
	}
	f.Fuzz(func(t *testing.T, wide bool, u1b, qb, u2b []byte) {
		c, ec, _ := fuzzCurve(wide)
		u1, _ := scalarMod(c, u1b)
		q, qs := scalarMod(c, qb)
		u2, _ := scalarMod(c, u2b)
		Q := &ecPoint{inf: true}
		if q.Sign() != 0 {
			k, err := ec.NewPrivateKey(qs)
			if err != nil {
				t.Fatal(err)
			}
			if Q, err = c.decodePoint(k.PublicKey().Bytes()); err != nil {
				t.Fatal(err)
			}
		}
		got := c.mul2(u1, Q, u2)
		want := new(big.Int).Mul(q, u2)
		want.Add(want, u1)
		_, ws := scalarMod(c, want.Bytes())
		if new(big.Int).SetBytes(ws).Sign() == 0 {
			if !got.inf {
				t.Fatal("expected the point at infinity")
			}
			return
		}
		k, err := ec.NewPrivateKey(ws)
		if err != nil {
			t.Fatal(err)
		}
		if got.inf || !bytes.Equal(encodePoint(c, got), k.PublicKey().Bytes()) {
			t.Fatalf("u1·G + u2·Q differs from crypto/ecdh (u1 %x q %x u2 %x)", u1, q, u2)
		}
	})
}

var bigOne = big.NewInt(1)

// FuzzField: Montgomery arithmetic against math/big on reduced values.
func FuzzField(f *testing.F) {
	for _, wide := range []bool{false, true} {
		c, _, _ := fuzzCurve(wide)
		pm1 := new(big.Int).Sub(c.p, bigOne).Bytes()
		f.Add(wide, []byte{0}, []byte{1})
		f.Add(wide, pm1, pm1)
		f.Add(wide, pm1, []byte{2})
		f.Add(wide, bytes.Repeat([]byte{0xff}, 64), bytes.Repeat([]byte{0x80}, 7))
	}
	f.Fuzz(func(t *testing.T, wide bool, ab, bb []byte) {
		c, _, _ := fuzzCurve(wide)
		fl, p := c.f, c.p
		x := new(big.Int).SetBytes(ab)
		x.Mod(x, p)
		y := new(big.Int).SetBytes(bb)
		y.Mod(y, p)
		mx, my := fl.mont(x), fl.mont(y)
		if fl.unmont(&mx).Cmp(x) != 0 {
			t.Fatalf("mont round trip of %x", x)
		}
		var z fe
		check := func(op string, want *big.Int) {
			t.Helper()
			want.Mod(want, p)
			if g := fl.unmont(&z); g.Cmp(want) != 0 {
				t.Fatalf("%s(%x, %x) = %x, want %x", op, x, y, g, want)
			}
			if fl.isZero(&z) != (want.Sign() == 0) {
				t.Fatalf("%s(%x, %x): isZero wrong (unreduced result)", op, x, y)
			}
		}
		fl.mul(&z, &mx, &my)
		check("mul", new(big.Int).Mul(x, y))
		fl.add(&z, &mx, &my)
		check("add", new(big.Int).Add(x, y))
		fl.sub(&z, &mx, &my)
		check("sub", new(big.Int).Sub(x, y))
		if x.Sign() != 0 {
			fl.inv(&z, &mx)
			check("inv", new(big.Int).ModInverse(x, p))
		}
	})
}

// FuzzECDSAVerify: our verifier against crypto/ecdsa for the same key. We
// may accept a non-canonical DER encoding crypto/ecdsa refuses, but never
// a different (r, s).
func FuzzECDSAVerify(f *testing.F) {
	for _, wide := range []bool{false, true} {
		c, _, ell := fuzzCurve(wide)
		d := []byte{0x42, 0x17}
		_, ds := scalarMod(c, d)
		k, err := ecdsa.ParseRawPrivateKey(ell, ds)
		if err != nil {
			f.Fatal(err)
		}
		for _, n := range []int{20, 32, 48, 64} {
			digest := bytes.Repeat([]byte{byte(n)}, n)
			sig, _ := ecdsa.SignASN1(rand.Reader, k, digest)
			f.Add(wide, d, digest, sig)
		}
		f.Add(wide, d, []byte{}, []byte{0x30, 0x06, 0x02, 0x01, 0x01, 0x02, 0x01, 0x01})
	}
	f.Fuzz(func(t *testing.T, wide bool, d, digest, sig []byte) {
		c, _, ell := fuzzCurve(wide)
		dv, ds := scalarMod(c, d)
		if dv.Sign() == 0 {
			return
		}
		k, err := ecdsa.ParseRawPrivateKey(ell, ds)
		if err != nil {
			t.Fatal(err)
		}
		pub, _ := k.PublicKey.Bytes()
		q, err := c.decodePoint(pub)
		if err != nil {
			t.Fatal(err)
		}
		ours, ref := c.ecdsaVerify(q, digest, sig), ecdsa.VerifyASN1(&k.PublicKey, digest, sig)
		if ref && !ours {
			t.Fatal("valid signature rejected")
		}
		if ours && !ref {
			// Only the encoding may differ: re-encode (r, s) canonically.
			seq, _, _ := expect(sig, 0x30)
			r, seq, _ := derInt(seq)
			s, _, _ := derInt(seq)
			if !ecdsa.VerifyASN1(&k.PublicKey, digest, marshalECDSA(r, s)) {
				t.Fatal("accepted a signature crypto/ecdsa rejects")
			}
		}
	})
}

// FuzzParsePrivateKey: the key and PEM parsers (local configuration, not
// server data, but they still must not crash).
func FuzzParsePrivateKey(f *testing.F) {
	p := fuzzKeys()
	for _, k := range p.leafKey {
		der, err := x509.MarshalPKCS8PrivateKey(k)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(der)
		f.Add(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))
	}
	sec1, _ := x509.MarshalECPrivateKey(p.leafKey[fkP384].(*ecdsa.PrivateKey))
	f.Add(sec1)
	f.Add(x509.MarshalPKCS1PrivateKey(p.leafKey[fkRSA].(*rsa.PrivateKey)))
	f.Add(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: p.rootDER}))
	f.Fuzz(func(t *testing.T, b []byte) {
		_, _ = ParseCertificatesPEM(b)
		keys := []func() (*PrivateKey, error){
			func() (*PrivateKey, error) { return ParsePrivateKeyPEM(b) },
			func() (*PrivateKey, error) { return parsePKCS8(b) },
			func() (*PrivateKey, error) { return parseSEC1(b, nil) },
			func() (*PrivateKey, error) { return parseSEC1(b, oidP256) },
			func() (*PrivateKey, error) { return parsePKCS1(b) },
		}
		for _, parse := range keys {
			k, err := parse()
			if err != nil {
				continue
			}
			if k.typ == keyRSA && k.rsa.N.BitLen() > 4096 {
				continue // slow, not interesting
			}
			_, _ = k.sign(&detRand{}, k.scheme(), []byte("msg"))
		}
	})
}

// TestKeyUsageHighBits: key usage bits past the defined ones don't turn
// the extension into "no extension" (regression).
func TestKeyUsageHighBits(t *testing.T) {
	p := fuzzKeys()
	c, err := ParseCertificate(p.hiKU)
	if err != nil {
		t.Fatal(err)
	}
	if c.keyUsage != 1<<2 {
		t.Fatalf("key usage %#x, want keyEncipherment only", c.keyUsage)
	}
	err = verifyChain([]*Certificate{c}, []*Certificate{p.root}, fuzzNow, oidEKUServer)
	if err == nil || !strings.Contains(err.Error(), "lacks digitalSignature") {
		t.Fatalf("err = %v", err)
	}
}

// TestRSAKeySize: huge RSA moduli would make a signature check take
// seconds (minutes on a microcontroller).
func TestRSAKeySize(t *testing.T) {
	for _, tc := range []struct {
		bits int
		ok   bool
	}{{2048, true}, {maxRSABits, true}, {maxRSABits + 1, false}, {1 << 18, false}} {
		n := new(big.Int).Lsh(big.NewInt(1), uint(tc.bits-1))
		n.Add(n, big.NewInt(1))
		spki, err := x509.MarshalPKIXPublicKey(&rsa.PublicKey{N: n, E: 65537})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := parseSPKI(spki); (err == nil) != tc.ok {
			t.Errorf("%d bits: %v", tc.bits, err)
		}
	}
}

// TestChainSignatureBudget: a chain stuffed with certificates that have
// the right subject but don't verify is cut off after a bounded number of
// signature checks.
func TestChainSignatureBudget(t *testing.T) {
	root := issue(t, genKey(t, "p256"), nil, caTmpl("root"))
	mid := issue(t, genKey(t, "p256"), &root, caTmpl("mid"))
	decoy := issue(t, genKey(t, "p256"), &root, caTmpl("mid"))
	leaf := issue(t, genKey(t, "p256"), &mid, leafTmpl("srv", x509.ExtKeyUsageServerAuth))
	chain := func(decoys int) []*Certificate {
		var ders []*x509.Certificate
		ders = append(ders, leaf.cert)
		for range decoys {
			ders = append(ders, decoy.cert)
		}
		return roots(t, append(ders, mid.cert)...)
	}
	if err := verifyChain(chain(4), roots(t, root.cert), time.Now(), oidEKUServer); err != nil {
		t.Fatal(err)
	}
	if err := verifyChain(chain(maxSignatureChecks), roots(t, root.cert), time.Now(), oidEKUServer); err == nil {
		t.Fatal("verified after too many signature checks")
	}
}
