package transport

import (
	"encoding/pem"
	"errors"
	"math/big"
)

// PKCS #1 RSA keys in PEM ("RSA PRIVATE KEY" / "RSA PUBLIC KEY"), the
// format zenoh-rust's transport/auth/pubkey settings take. A minimal DER
// reader keeps crypto/x509 (and its reflection-based ASN.1) out of the
// core, which must stay TinyGo-friendly.

var errBadKey = errors.New("transport: bad PKCS #1 RSA key")

// ParseRSAKeyPEM parses a PKCS #1 RSA private key ("RSA PRIVATE KEY").
func ParseRSAKeyPEM(b []byte) (*RSAKey, error) {
	blk, _ := pem.Decode(b)
	if blk == nil || blk.Type != "RSA PRIVATE KEY" {
		return nil, errors.New("transport: no RSA PRIVATE KEY PEM block")
	}
	// RSAPrivateKey ::= SEQUENCE { version, n, e, d, p, q, dP, dQ, qInv, ... }
	ints, err := derIntSeq(blk.Bytes, 9)
	if err != nil {
		return nil, err
	}
	if ints[0].Sign() != 0 {
		return nil, errors.New("transport: multi-prime RSA keys are not supported")
	}
	n, e, d := ints[1], ints[2], ints[3]
	if n.Sign() <= 0 || e.Sign() <= 0 || d.Sign() <= 0 {
		return nil, errBadKey
	}
	return newRSAKey(n, e, d), nil
}

// ParseRSAPublicKeyPEM parses a PKCS #1 RSA public key ("RSA PUBLIC KEY").
func ParseRSAPublicKeyPEM(b []byte) (RSAPublicKey, error) {
	blk, _ := pem.Decode(b)
	if blk == nil || blk.Type != "RSA PUBLIC KEY" {
		return RSAPublicKey{}, errors.New("transport: no RSA PUBLIC KEY PEM block")
	}
	// RSAPublicKey ::= SEQUENCE { n, e }
	ints, err := derIntSeq(blk.Bytes, 2)
	if err != nil {
		return RSAPublicKey{}, err
	}
	if ints[0].Sign() <= 0 || ints[1].Sign() <= 0 {
		return RSAPublicKey{}, errBadKey
	}
	return RSAPublicKey{ints[0], ints[1]}, nil
}

// derIntSeq reads a DER SEQUENCE and returns its first want INTEGERs.
func derIntSeq(b []byte, want int) ([]*big.Int, error) {
	tag, body, _, err := derTLV(b)
	if err != nil || tag != 0x30 {
		return nil, errBadKey
	}
	var out []*big.Int
	for len(out) < want {
		tag, v, rest, err := derTLV(body)
		if err != nil || tag != 0x02 || len(v) == 0 || v[0]&0x80 != 0 {
			return nil, errBadKey // missing, not an INTEGER, or negative
		}
		out = append(out, new(big.Int).SetBytes(v))
		body = rest
	}
	return out, nil
}

// derTLV splits off one DER element: tag, contents, remainder.
func derTLV(b []byte) (byte, []byte, []byte, error) {
	if len(b) < 2 {
		return 0, nil, nil, errBadKey
	}
	tag, l := b[0], int(b[1])
	b = b[2:]
	if l&0x80 != 0 {
		nb := l & 0x7f
		if nb == 0 || nb > 4 || len(b) < nb {
			return 0, nil, nil, errBadKey
		}
		l = 0
		for _, c := range b[:nb] {
			l = l<<8 | int(c)
		}
		b = b[nb:]
	}
	if l < 0 || l > len(b) {
		return 0, nil, nil, errBadKey
	}
	return tag, b[:l], b[l:], nil
}
