package tls13

import (
	"bytes"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net/netip"
	"strings"
	"time"
)

// A minimal X.509 reader: enough of a certificate to verify a server's
// chain (names, validity, CA constraints, key usage, keys and signatures),
// without crypto/x509 (too large for microcontrollers).

var errDER = errors.New("tls13: malformed DER")

// der reads one TLV: tag, contents and what follows.
func der(b []byte) (tag byte, val, rest []byte, err error) {
	if len(b) < 2 {
		return 0, nil, nil, errDER
	}
	tag, l := b[0], int(b[1])
	b = b[2:]
	if tag&0x1f == 0x1f {
		return 0, nil, nil, errDER // multi-byte tags: not used here
	}
	if l&0x80 != 0 {
		n := l & 0x7f
		if n == 0 || n > 3 || len(b) < n {
			return 0, nil, nil, errDER
		}
		l = 0
		for _, c := range b[:n] {
			l = l<<8 | int(c)
		}
		b = b[n:]
	}
	if l > len(b) {
		return 0, nil, nil, errDER
	}
	return tag, b[:l], b[l:], nil
}

// expect reads a TLV that must have the given tag.
func expect(b []byte, tag byte) (val, rest []byte, err error) {
	t, v, r, err := der(b)
	if err != nil {
		return nil, nil, err
	}
	if t != tag {
		return nil, nil, fmt.Errorf("tls13: DER tag %#x, want %#x", t, tag)
	}
	return v, r, nil
}

// rawTLV returns the whole encoding of the next TLV.
func rawTLV(b []byte) (raw, rest []byte, err error) {
	_, _, r, err := der(b)
	if err != nil {
		return nil, nil, err
	}
	return b[:len(b)-len(r)], r, nil
}

func derInt(b []byte) (*big.Int, []byte, error) {
	v, r, err := expect(b, 0x02)
	if err != nil {
		return nil, nil, err
	}
	if len(v) == 0 || v[0]&0x80 != 0 {
		return nil, nil, errDER // empty or negative
	}
	return new(big.Int).SetBytes(v), r, nil
}

// Object identifiers, as their DER contents.
var (
	oidRSA          = []byte{0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, 0x01}
	oidSHA256RSA    = []byte{0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, 0x0b}
	oidSHA384RSA    = []byte{0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, 0x0c}
	oidSHA512RSA    = []byte{0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, 0x0d}
	oidEC           = []byte{0x2a, 0x86, 0x48, 0xce, 0x3d, 0x02, 0x01}
	oidP256         = []byte{0x2a, 0x86, 0x48, 0xce, 0x3d, 0x03, 0x01, 0x07}
	oidP384         = []byte{0x2b, 0x81, 0x04, 0x00, 0x22}
	oidECDSASHA256  = []byte{0x2a, 0x86, 0x48, 0xce, 0x3d, 0x04, 0x03, 0x02}
	oidECDSASHA384  = []byte{0x2a, 0x86, 0x48, 0xce, 0x3d, 0x04, 0x03, 0x03}
	oidECDSASHA512  = []byte{0x2a, 0x86, 0x48, 0xce, 0x3d, 0x04, 0x03, 0x04}
	oidEd25519      = []byte{0x2b, 0x65, 0x70}
	oidExtSAN       = []byte{0x55, 0x1d, 0x11}
	oidExtBasic     = []byte{0x55, 0x1d, 0x13}
	oidExtKeyUsage  = []byte{0x55, 0x1d, 0x0f}
	oidExtExtKU     = []byte{0x55, 0x1d, 0x25}
	oidExtSKI       = []byte{0x55, 0x1d, 0x0e}
	oidExtAKI       = []byte{0x55, 0x1d, 0x23}
	oidExtPolicies  = []byte{0x55, 0x1d, 0x20}
	oidEKUServer    = []byte{0x2b, 0x06, 0x01, 0x05, 0x05, 0x07, 0x03, 0x01}
	oidEKUClient    = []byte{0x2b, 0x06, 0x01, 0x05, 0x05, 0x07, 0x03, 0x02}
	oidEKUAny       = []byte{0x55, 0x1d, 0x25, 0x00}
	oidCommonName   = []byte{0x55, 0x04, 0x03}
	oidRSAPSSParams = []byte{0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, 0x0a}
)

type keyType int

const (
	keyRSA keyType = iota + 1
	keyP256
	keyP384
	keyEd25519
)

// publicKey is a certificate's subject public key.
type publicKey struct {
	typ keyType
	rsa struct {
		n *big.Int
		e int
	}
	ec  *ecPoint // P-256 / P-384
	raw []byte   // Ed25519: 32 bytes; EC: the encoded point
}

// Key usage bits (RFC 5280; bit 0 is the most significant bit).
const (
	kuDigitalSignature = 1 << 0
	kuKeyCertSign      = 1 << 5
)

const (
	// maxRSABits bounds the work a server can make us do per RSA
	// signature check (crypto/tls has the same limit).
	maxRSABits = 8192
	// maxSignatureChecks bounds the signature checks of a chain: a server
	// could send many certificates with the same subject.
	maxSignatureChecks = 32
)

// Certificate is a parsed X.509 certificate.
type Certificate struct {
	Raw        []byte
	rawTBS     []byte
	rawIssuer  []byte
	rawSubject []byte
	NotBefore  time.Time
	NotAfter   time.Time
	key        publicKey
	sigAlg     []byte // signature algorithm OID
	signature  []byte

	isCA       bool
	keyUsage   int // -1: no extension
	extKU      [][]byte
	DNSNames   []string
	IPs        []netip.Addr
	CommonName string
}

// ParseCertificate parses one DER certificate.
func ParseCertificate(raw []byte) (*Certificate, error) {
	c := &Certificate{Raw: raw, keyUsage: -1}
	body, rest, err := expect(raw, 0x30)
	if err != nil || len(rest) != 0 {
		return nil, errDER
	}
	if c.rawTBS, body, err = rawTLV(body); err != nil {
		return nil, err
	}
	algSeq, body, err := expect(body, 0x30)
	if err != nil {
		return nil, err
	}
	if c.sigAlg, _, err = expect(algSeq, 0x06); err != nil {
		return nil, err
	}
	bits, _, err := expect(body, 0x03)
	if err != nil || len(bits) < 1 || bits[0] != 0 {
		return nil, errDER
	}
	c.signature = bits[1:]

	tbs, _, err := expect(c.rawTBS, 0x30)
	if err != nil {
		return nil, err
	}
	if len(tbs) > 0 && tbs[0] == 0xa0 { // [0] version
		if _, tbs, err = rawTLV(tbs); err != nil {
			return nil, err
		}
	}
	if _, tbs, err = rawTLV(tbs); err != nil { // serial
		return nil, err
	}
	if _, tbs, err = rawTLV(tbs); err != nil { // signature algorithm
		return nil, err
	}
	if c.rawIssuer, tbs, err = rawTLV(tbs); err != nil {
		return nil, err
	}
	validity, tbs, err := expect(tbs, 0x30)
	if err != nil {
		return nil, err
	}
	if c.NotBefore, validity, err = derTime(validity); err != nil {
		return nil, err
	}
	if c.NotAfter, _, err = derTime(validity); err != nil {
		return nil, err
	}
	if c.rawSubject, tbs, err = rawTLV(tbs); err != nil {
		return nil, err
	}
	c.CommonName = commonName(c.rawSubject)
	spki, tbs, err := expect(tbs, 0x30)
	if err != nil {
		return nil, err
	}
	if err := c.key.parse(spki); err != nil {
		return nil, err
	}
	for len(tbs) > 0 {
		t, v, r, err := der(tbs)
		if err != nil {
			return nil, err
		}
		tbs = r
		if t == 0xa3 {
			if err := c.parseExtensions(v); err != nil {
				return nil, err
			}
		}
	}
	return c, nil
}

func derTime(b []byte) (time.Time, []byte, error) {
	t, v, r, err := der(b)
	if err != nil {
		return time.Time{}, nil, err
	}
	var layout string
	switch t {
	case 0x17:
		layout = "060102150405Z0700"
	case 0x18:
		layout = "20060102150405Z0700"
	default:
		return time.Time{}, nil, errDER
	}
	tm, err := time.Parse(layout, string(v))
	if err != nil {
		return time.Time{}, nil, errDER
	}
	if t == 0x17 && tm.Year() >= 2050 {
		tm = tm.AddDate(-100, 0, 0)
	}
	return tm, r, nil
}

// commonName digs the CN out of a Name, for error messages.
func commonName(name []byte) string {
	seq, _, err := expect(name, 0x30)
	if err != nil {
		return ""
	}
	for len(seq) > 0 {
		set, r, err := expect(seq, 0x31)
		if err != nil {
			return ""
		}
		seq = r
		atv, _, err := expect(set, 0x30)
		if err != nil {
			return ""
		}
		oid, val, err := expect(atv, 0x06)
		if err == nil && bytes.Equal(oid, oidCommonName) {
			if _, s, _, err := der(val); err == nil {
				return string(s)
			}
		}
	}
	return ""
}

func (k *publicKey) parse(spki []byte) error {
	alg, rest, err := expect(spki, 0x30)
	if err != nil {
		return err
	}
	oid, params, err := expect(alg, 0x06)
	if err != nil {
		return err
	}
	bits, _, err := expect(rest, 0x03)
	if err != nil || len(bits) < 1 || bits[0] != 0 {
		return errDER
	}
	key := bits[1:]
	switch {
	case bytes.Equal(oid, oidRSA):
		seq, _, err := expect(key, 0x30)
		if err != nil {
			return err
		}
		n, seq, err := derInt(seq)
		if err != nil {
			return err
		}
		e, _, err := derInt(seq)
		if err != nil || !e.IsInt64() || e.Int64() < 3 || e.Int64() > 1<<31-1 {
			return errDER
		}
		if n.BitLen() > maxRSABits {
			return errors.New("tls13: RSA key too large")
		}
		k.typ, k.rsa.n, k.rsa.e = keyRSA, n, int(e.Int64())
	case bytes.Equal(oid, oidEC):
		curveOID, _, err := expect(params, 0x06)
		if err != nil {
			return err
		}
		var c *curve
		switch {
		case bytes.Equal(curveOID, oidP256):
			k.typ, c = keyP256, p256()
		case bytes.Equal(curveOID, oidP384):
			k.typ, c = keyP384, p384()
		default:
			return errors.New("tls13: unsupported elliptic curve")
		}
		if k.ec, err = c.decodePoint(key); err != nil {
			return err
		}
		k.raw = key
	case bytes.Equal(oid, oidEd25519):
		if len(key) != 32 {
			return errDER
		}
		k.typ, k.raw = keyEd25519, key
	default:
		return errors.New("tls13: unsupported public key algorithm")
	}
	return nil
}

func (c *Certificate) parseExtensions(b []byte) error {
	seq, _, err := expect(b, 0x30)
	if err != nil {
		return err
	}
	for len(seq) > 0 {
		ext, r, err := expect(seq, 0x30)
		if err != nil {
			return err
		}
		seq = r
		oid, ext, err := expect(ext, 0x06)
		if err != nil {
			return err
		}
		critical := false
		if len(ext) > 0 && ext[0] == 0x01 {
			var v []byte
			if v, ext, err = expect(ext, 0x01); err != nil || len(v) != 1 {
				return errDER
			}
			critical = v[0] != 0
		}
		val, _, err := expect(ext, 0x04)
		if err != nil {
			return err
		}
		switch {
		case bytes.Equal(oid, oidExtSAN):
			err = c.parseSAN(val)
		case bytes.Equal(oid, oidExtBasic):
			bc, _, e := expect(val, 0x30)
			if e != nil {
				return e
			}
			if len(bc) > 0 && bc[0] == 0x01 {
				v, _, e := expect(bc, 0x01)
				if e != nil || len(v) != 1 {
					return errDER
				}
				c.isCA = v[0] != 0
			}
		case bytes.Equal(oid, oidExtKeyUsage):
			bits, _, e := expect(val, 0x03)
			if e != nil || len(bits) < 1 {
				return errDER
			}
			c.keyUsage = 0
			// Bits 0-8 are defined; more would overflow into the sign bit
			// (-1 means no extension).
			for i, by := range bits[1:min(len(bits), 3)] {
				for j := 0; j < 8; j++ {
					if by&(0x80>>j) != 0 {
						c.keyUsage |= 1 << (8*i + j)
					}
				}
			}
		case bytes.Equal(oid, oidExtExtKU):
			list, _, e := expect(val, 0x30)
			if e != nil {
				return e
			}
			for len(list) > 0 {
				var o []byte
				if o, list, e = expect(list, 0x06); e != nil {
					return e
				}
				c.extKU = append(c.extKU, o)
			}
		case bytes.Equal(oid, oidExtSKI), bytes.Equal(oid, oidExtAKI), bytes.Equal(oid, oidExtPolicies):
		default:
			if critical {
				return errors.New("tls13: certificate has an unsupported critical extension")
			}
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Certificate) parseSAN(b []byte) error {
	seq, _, err := expect(b, 0x30)
	if err != nil {
		return err
	}
	for len(seq) > 0 {
		t, v, r, err := der(seq)
		if err != nil {
			return err
		}
		seq = r
		switch t {
		case 0x82: // dNSName
			c.DNSNames = append(c.DNSNames, string(v))
		case 0x87: // iPAddress
			if ip, ok := netip.AddrFromSlice(v); ok {
				c.IPs = append(c.IPs, ip.Unmap())
			}
		}
	}
	return nil
}

// hasEKU reports whether the certificate may be used for purpose (no
// extended key usage extension allows anything).
func (c *Certificate) hasEKU(purpose []byte) bool {
	if c.extKU == nil {
		return true
	}
	for _, o := range c.extKU {
		if bytes.Equal(o, purpose) || bytes.Equal(o, oidEKUAny) {
			return true
		}
	}
	return false
}

// checkSignatureFrom verifies that parent signed c.
func (c *Certificate) checkSignatureFrom(parent *Certificate) error {
	var scheme uint16
	switch {
	case bytes.Equal(c.sigAlg, oidSHA256RSA):
		scheme = schemePKCS1SHA256
	case bytes.Equal(c.sigAlg, oidSHA384RSA):
		scheme = schemePKCS1SHA384
	case bytes.Equal(c.sigAlg, oidSHA512RSA):
		scheme = schemePKCS1SHA512
	case bytes.Equal(c.sigAlg, oidECDSASHA256):
		scheme = schemeECDSASHA256
	case bytes.Equal(c.sigAlg, oidECDSASHA384):
		scheme = schemeECDSASHA384
	case bytes.Equal(c.sigAlg, oidECDSASHA512):
		scheme = schemeECDSASHA512
	case bytes.Equal(c.sigAlg, oidEd25519):
		scheme = schemeEd25519
	case bytes.Equal(c.sigAlg, oidRSAPSSParams):
		return errors.New("tls13: RSASSA-PSS certificate signatures are not supported")
	default:
		return errors.New("tls13: unsupported certificate signature algorithm")
	}
	return verifySignature(&parent.key, scheme, c.rawTBS, c.signature, true)
}

// ParseCertificatesPEM parses every CERTIFICATE block of a PEM text.
func ParseCertificatesPEM(b []byte) ([]*Certificate, error) {
	var out []*Certificate
	for {
		var blk *pem.Block
		blk, b = pem.Decode(b)
		if blk == nil {
			break
		}
		if blk.Type != "CERTIFICATE" {
			continue
		}
		c, err := ParseCertificate(blk.Bytes)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	if len(out) == 0 {
		return nil, errors.New("tls13: no certificate in PEM")
	}
	return out, nil
}

// clockSet: a board without a real-time clock starts at its epoch; the
// validity period can't be checked before the time is known.
func clockSet(t time.Time) bool { return t.Year() >= 2024 }

func (c *Certificate) checkTime(now time.Time) error {
	if !clockSet(now) {
		return nil
	}
	if now.Before(c.NotBefore) || now.After(c.NotAfter) {
		return fmt.Errorf("tls13: certificate %q is not valid at %s (valid %s to %s)", c.CommonName,
			now.UTC().Format(time.RFC3339), c.NotBefore.Format(time.RFC3339), c.NotAfter.Format(time.RFC3339))
	}
	return nil
}

// verifyChain checks that chain[0] leads, through the other certificates,
// to one of roots (or is one of them).
func verifyChain(chain []*Certificate, roots []*Certificate, now time.Time, eku []byte) error {
	leaf := chain[0]
	if err := leaf.checkTime(now); err != nil {
		return err
	}
	if !leaf.hasEKU(eku) {
		return errors.New("tls13: certificate is not for this use (extended key usage)")
	}
	if leaf.keyUsage >= 0 && leaf.keyUsage&kuDigitalSignature == 0 {
		return errors.New("tls13: certificate key usage lacks digitalSignature")
	}
	checks := 0
	signedBy := func(c, parent *Certificate) bool {
		checks++
		return checks <= maxSignatureChecks && c.checkSignatureFrom(parent) == nil
	}
	cur := leaf
	for depth := 0; depth < 10; depth++ {
		for _, r := range roots {
			if bytes.Equal(r.Raw, cur.Raw) {
				return nil
			}
		}
		for _, r := range roots {
			if bytes.Equal(r.rawSubject, cur.rawIssuer) && signedBy(cur, r) {
				return r.checkTime(now)
			}
		}
		var next *Certificate
		for _, ic := range chain[1:] {
			if ic != cur && bytes.Equal(ic.rawSubject, cur.rawIssuer) && signedBy(cur, ic) {
				next = ic
				break
			}
		}
		if next == nil {
			return fmt.Errorf("tls13: certificate %q is not signed by a trusted authority", cur.CommonName)
		}
		if !next.isCA || (next.keyUsage >= 0 && next.keyUsage&kuKeyCertSign == 0) {
			return fmt.Errorf("tls13: certificate %q is not a certificate authority", next.CommonName)
		}
		if err := next.checkTime(now); err != nil {
			return err
		}
		cur = next
	}
	return errors.New("tls13: certificate chain too long")
}

// VerifyHostname checks the certificate's subject alternative names
// against a DNS name or an IP address.
func (c *Certificate) VerifyHostname(host string) error {
	if ip, err := netip.ParseAddr(strings.Trim(host, "[]")); err == nil {
		for _, a := range c.IPs {
			if a == ip.Unmap() {
				return nil
			}
		}
		return fmt.Errorf("tls13: certificate is not valid for IP %s", host)
	}
	host = strings.ToLower(strings.TrimSuffix(host, "."))
	for _, n := range c.DNSNames {
		if matchHostname(strings.ToLower(strings.TrimSuffix(n, ".")), host) {
			return nil
		}
	}
	return fmt.Errorf("tls13: certificate is not valid for %s (names %v)", host, c.DNSNames)
}

func matchHostname(pattern, host string) bool {
	if pattern == host {
		return true
	}
	if !strings.HasPrefix(pattern, "*.") {
		return false
	}
	i := strings.IndexByte(host, '.')
	return i > 0 && host[i:] == pattern[1:]
}
