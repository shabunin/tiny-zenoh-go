package tls13

import (
	"bytes"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
)

// TLS SignatureScheme values.
const (
	schemePKCS1SHA256 uint16 = 0x0401
	schemePKCS1SHA384 uint16 = 0x0501
	schemePKCS1SHA512 uint16 = 0x0601
	schemeECDSASHA256 uint16 = 0x0403
	schemeECDSASHA384 uint16 = 0x0503
	schemeECDSASHA512 uint16 = 0x0603
	schemePSSSHA256   uint16 = 0x0804
	schemePSSSHA384   uint16 = 0x0805
	schemePSSSHA512   uint16 = 0x0806
	schemeEd25519     uint16 = 0x0807
)

// supportedSchemes is what the client advertises (signature_algorithms):
// handshake signatures and, via the PKCS #1 entries, certificate ones.
var supportedSchemes = func() []uint16 {
	s := []uint16{
		schemeECDSASHA256, schemeECDSASHA384, schemeECDSASHA512,
		schemePSSSHA256, schemePSSSHA384, schemePSSSHA512,
		schemePKCS1SHA256, schemePKCS1SHA384, schemePKCS1SHA512,
	}
	if haveEd25519 {
		s = append(s, schemeEd25519)
	}
	return s
}()

func schemeHash(scheme uint16) (crypto.Hash, func([]byte) []byte) {
	switch scheme {
	case schemePKCS1SHA256, schemeECDSASHA256, schemePSSSHA256:
		return crypto.SHA256, func(m []byte) []byte { d := sha256.Sum256(m); return d[:] }
	case schemePKCS1SHA384, schemeECDSASHA384, schemePSSSHA384:
		return crypto.SHA384, func(m []byte) []byte { d := sha512.Sum384(m); return d[:] }
	case schemePKCS1SHA512, schemeECDSASHA512, schemePSSSHA512:
		return crypto.SHA512, func(m []byte) []byte { d := sha512.Sum512(m); return d[:] }
	}
	return 0, nil
}

var (
	errSignature = errors.New("tls13: invalid signature")
	errNoEd25519 = errors.New("tls13: Ed25519 left out of this TinyGo build (build tag tls13_ed25519 adds it)")
)

// verifySignature checks sig over msg with key. PKCS #1 v1.5 is only
// allowed in certificates (cert); TLS 1.3 handshakes use PSS.
func verifySignature(key *publicKey, scheme uint16, msg, sig []byte, cert bool) error {
	switch scheme {
	case schemeEd25519:
		if !haveEd25519 {
			return errNoEd25519
		}
		if key.typ != keyEd25519 || !ed25519Verify(key.raw, msg, sig) {
			return errSignature
		}
		return nil
	case schemeECDSASHA256, schemeECDSASHA384, schemeECDSASHA512:
		var c *curve
		switch key.typ {
		case keyP256:
			c = p256()
		case keyP384:
			c = p384()
		default:
			return errSignature
		}
		// TLS 1.3 binds the curve to the hash; certificates don't.
		if !cert && ((scheme == schemeECDSASHA256) != (key.typ == keyP256) ||
			(scheme == schemeECDSASHA384) != (key.typ == keyP384)) {
			return errSignature
		}
		_, h := schemeHash(scheme)
		if !c.ecdsaVerify(key.ec, h(msg), sig) {
			return errSignature
		}
		return nil
	case schemePSSSHA256, schemePSSSHA384, schemePSSSHA512, schemePKCS1SHA256, schemePKCS1SHA384, schemePKCS1SHA512:
		if key.typ != keyRSA {
			return errSignature
		}
		pub := &rsa.PublicKey{N: key.rsa.n, E: key.rsa.e}
		hid, h := schemeHash(scheme)
		var err error
		if scheme>>8 == 0x08 {
			err = rsa.VerifyPSS(pub, hid, h(msg), sig, &rsa.PSSOptions{SaltLength: rsa.PSSSaltLengthEqualsHash})
		} else if cert {
			err = rsa.VerifyPKCS1v15(pub, hid, h(msg), sig)
		} else {
			return errors.New("tls13: PKCS #1 v1.5 signature in a TLS 1.3 handshake")
		}
		if err != nil {
			return errSignature
		}
		return nil
	}
	return errors.New("tls13: unsupported signature scheme")
}

// PrivateKey is a client certificate's key: ECDSA P-256/P-384, RSA or
// Ed25519.
type PrivateKey struct {
	typ keyType
	d   *big.Int // ECDSA
	rsa *rsa.PrivateKey
	ed  []byte // Ed25519 private key (seed and public key)
}

func (k *PrivateKey) scheme() uint16 {
	switch k.typ {
	case keyP256:
		return schemeECDSASHA256
	case keyP384:
		return schemeECDSASHA384
	case keyRSA:
		return schemePSSSHA256
	}
	return schemeEd25519
}

func (k *PrivateKey) sign(rand io.Reader, scheme uint16, msg []byte) ([]byte, error) {
	switch k.typ {
	case keyP256:
		_, h := schemeHash(scheme)
		return p256().ecdsaSign(rand, k.d, h(msg))
	case keyP384:
		_, h := schemeHash(scheme)
		return p384().ecdsaSign(rand, k.d, h(msg))
	case keyRSA:
		hid, h := schemeHash(scheme)
		return rsa.SignPSS(rand, k.rsa, hid, h(msg), &rsa.PSSOptions{SaltLength: rsa.PSSSaltLengthEqualsHash})
	}
	return ed25519Sign(k.ed, msg), nil
}

// ParsePrivateKeyPEM reads a PKCS #8, SEC 1 ("EC PRIVATE KEY") or PKCS #1
// ("RSA PRIVATE KEY") key.
func ParsePrivateKeyPEM(b []byte) (*PrivateKey, error) {
	for {
		var blk *pem.Block
		if blk, b = pem.Decode(b); blk == nil {
			return nil, errors.New("tls13: no private key in PEM")
		}
		switch blk.Type {
		case "PRIVATE KEY":
			return parsePKCS8(blk.Bytes)
		case "EC PRIVATE KEY":
			return parseSEC1(blk.Bytes, nil)
		case "RSA PRIVATE KEY":
			return parsePKCS1(blk.Bytes)
		}
	}
}

func parsePKCS8(b []byte) (*PrivateKey, error) {
	seq, _, err := expect(b, 0x30)
	if err != nil {
		return nil, err
	}
	if _, seq, err = rawTLV(seq); err != nil { // version
		return nil, err
	}
	alg, seq, err := expect(seq, 0x30)
	if err != nil {
		return nil, err
	}
	oid, params, err := expect(alg, 0x06)
	if err != nil {
		return nil, err
	}
	key, _, err := expect(seq, 0x04)
	if err != nil {
		return nil, err
	}
	switch {
	case bytes.Equal(oid, oidRSA):
		return parsePKCS1(key)
	case bytes.Equal(oid, oidEC):
		curveOID, _, err := expect(params, 0x06)
		if err != nil {
			return nil, err
		}
		return parseSEC1(key, curveOID)
	case bytes.Equal(oid, oidEd25519):
		seed, _, err := expect(key, 0x04)
		if err != nil || len(seed) != 32 {
			return nil, errDER
		}
		if !haveEd25519 {
			return nil, errNoEd25519
		}
		return &PrivateKey{typ: keyEd25519, ed: ed25519FromSeed(seed)}, nil
	}
	return nil, errors.New("tls13: unsupported private key algorithm")
}

func parseSEC1(b, curveOID []byte) (*PrivateKey, error) {
	seq, _, err := expect(b, 0x30)
	if err != nil {
		return nil, err
	}
	if _, seq, err = rawTLV(seq); err != nil { // version
		return nil, err
	}
	d, seq, err := expect(seq, 0x04)
	if err != nil {
		return nil, err
	}
	if len(seq) > 0 && seq[0] == 0xa0 {
		params, _, err := expect(seq, 0xa0)
		if err != nil {
			return nil, err
		}
		if curveOID, _, err = expect(params, 0x06); err != nil {
			return nil, err
		}
	}
	k := &PrivateKey{d: new(big.Int).SetBytes(d)}
	var c *curve
	switch {
	case bytes.Equal(curveOID, oidP256):
		k.typ, c = keyP256, p256()
	case bytes.Equal(curveOID, oidP384):
		k.typ, c = keyP384, p384()
	default:
		return nil, errors.New("tls13: unsupported elliptic curve")
	}
	if k.d.Sign() == 0 || k.d.Cmp(c.n) >= 0 {
		return nil, errDER
	}
	return k, nil
}

func parsePKCS1(b []byte) (*PrivateKey, error) {
	seq, _, err := expect(b, 0x30)
	if err != nil {
		return nil, err
	}
	if _, seq, err = rawTLV(seq); err != nil { // version
		return nil, err
	}
	var v [5]*big.Int // n, e, d, p, q
	for i := range v {
		if v[i], seq, err = derInt(seq); err != nil {
			return nil, err
		}
	}
	if !v[1].IsInt64() || v[1].Int64() > 1<<31-1 {
		return nil, errDER
	}
	k := &rsa.PrivateKey{
		PublicKey: rsa.PublicKey{N: v[0], E: int(v[1].Int64())},
		D:         v[2], Primes: []*big.Int{v[3], v[4]},
	}
	k.Precompute()
	return &PrivateKey{typ: keyRSA, rsa: k}, nil
}
