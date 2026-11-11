package tls13

import (
	"crypto/sha512"
	"errors"
	"io"
	"math/big"
	"sync"
)

// Short Weierstrass curves y² = x³ - 3x + b (NIST P-256 and P-384) for
// ECDSA: crypto/ecdsa pulls in far too much code for a microcontroller.
// Points are added in Jacobian coordinates over field.go's Montgomery
// arithmetic; scalars mod n use math/big (a handful of operations). Only
// public data goes through verification; signing (client certificates)
// is not constant-time.

type curve struct {
	p, n, b *big.Int
	g       ecPoint
	size    int // bytes per coordinate
	f       *field
}

// ecPoint is affine; inf marks the point at infinity.
type ecPoint struct {
	x, y *big.Int
	inf  bool
}

func hexInt(s string) *big.Int {
	v, ok := new(big.Int).SetString(s, 16)
	if !ok {
		panic("tls13: bad constant")
	}
	return v
}

var p256Curve = sync.OnceValue(func() *curve {
	return &curve{
		p:    hexInt("ffffffff00000001000000000000000000000000ffffffffffffffffffffffff"),
		n:    hexInt("ffffffff00000000ffffffffffffffffbce6faada7179e84f3b9cac2fc632551"),
		b:    hexInt("5ac635d8aa3a93e7b3ebbd55769886bc651d06b0cc53b0f63bce3c3e27d2604b"),
		g:    ecPoint{x: hexInt("6b17d1f2e12c4247f8bce6e563a440f277037d812deb33a0f4a13945d898c296"), y: hexInt("4fe342e2fe1a7f9b8ee7eb4a7c0f9e162bce33576b315ececbb6406837bf51f5")},
		size: 32,
		f:    newField(hexInt("ffffffff00000001000000000000000000000000ffffffffffffffffffffffff")),
	}
})

var p384Curve = sync.OnceValue(func() *curve {
	return &curve{
		p:    hexInt("fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffeffffffff0000000000000000ffffffff"),
		n:    hexInt("ffffffffffffffffffffffffffffffffffffffffffffffffc7634d81f4372ddf581a0db248b0a77aecec196accc52973"),
		b:    hexInt("b3312fa7e23ee7e4988e056be3f82d19181d9c6efe8141120314088f5013875ac656398d8a2ed19d2a85c8edd3ec2aef"),
		g:    ecPoint{x: hexInt("aa87ca22be8b05378eb1c71ef320ad746e1d3b628ba79b9859f741e082542a385502f25dbf55296c3a545e3872760ab7"), y: hexInt("3617de4a96262c6f5d9e98bf9292dc29f8f41dbd289a147ce9da3113b5f0b8c00a60b1ce1d7e819d7a431d7c90ea0e5f")},
		size: 48,
		f:    newField(hexInt("fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffeffffffff0000000000000000ffffffff")),
	}
})

func p256() *curve { return p256Curve() }
func p384() *curve { return p384Curve() }

// onCurve checks y² = x³ - 3x + b (mod p).
func (c *curve) onCurve(x, y *big.Int) bool {
	if x.Sign() < 0 || x.Cmp(c.p) >= 0 || y.Sign() < 0 || y.Cmp(c.p) >= 0 {
		return false
	}
	y2 := new(big.Int).Mul(y, y)
	y2.Mod(y2, c.p)
	x3 := new(big.Int).Mul(x, x)
	x3.Mul(x3, x)
	t := new(big.Int).Lsh(x, 1)
	t.Add(t, x)
	x3.Sub(x3, t)
	x3.Add(x3, c.b)
	x3.Mod(x3, c.p)
	return x3.Cmp(y2) == 0
}

// decodePoint reads an uncompressed SEC1 point.
func (c *curve) decodePoint(b []byte) (*ecPoint, error) {
	if len(b) != 1+2*c.size || b[0] != 4 {
		return nil, errors.New("tls13: unsupported elliptic curve point encoding")
	}
	x := new(big.Int).SetBytes(b[1 : 1+c.size])
	y := new(big.Int).SetBytes(b[1+c.size:])
	if !c.onCurve(x, y) {
		return nil, errors.New("tls13: elliptic curve point is not on the curve")
	}
	return &ecPoint{x: x, y: y}, nil
}

// jac is a point in Jacobian coordinates, (X/Z², Y/Z³), in Montgomery
// form; Z = 0 is the point at infinity.
type jac struct{ x, y, z fe }

func (c *curve) toJac(p *ecPoint) jac {
	if p.inf {
		return jac{}
	}
	return jac{c.f.mont(p.x), c.f.mont(p.y), c.f.one}
}

func (c *curve) affine(j *jac) *ecPoint {
	f := c.f
	if f.isZero(&j.z) {
		return &ecPoint{inf: true}
	}
	var zi, zi2, x, y fe
	f.inv(&zi, &j.z)
	f.mul(&zi2, &zi, &zi)
	f.mul(&x, &j.x, &zi2)
	f.mul(&zi2, &zi2, &zi)
	f.mul(&y, &j.y, &zi2)
	return &ecPoint{x: f.unmont(&x), y: f.unmont(&y)}
}

// double: dbl-2001-b (a = -3).
func (c *curve) double(r, a *jac) {
	f := c.f
	if f.isZero(&a.z) || f.isZero(&a.y) {
		*r = jac{}
		return
	}
	var delta, gamma, beta, alpha, t1, t2, x3, y3, z3 fe
	f.mul(&delta, &a.z, &a.z)
	f.mul(&gamma, &a.y, &a.y)
	f.mul(&beta, &a.x, &gamma)
	f.sub(&t1, &a.x, &delta)
	f.add(&t2, &a.x, &delta)
	f.mul(&alpha, &t1, &t2)
	f.add(&t1, &alpha, &alpha)
	f.add(&alpha, &t1, &alpha) // 3·(x-δ)(x+δ)
	f.mul(&x3, &alpha, &alpha)
	f.add(&t1, &beta, &beta) // 2β
	f.add(&t1, &t1, &t1)     // 4β
	f.add(&t2, &t1, &t1)     // 8β
	f.sub(&x3, &x3, &t2)
	f.add(&z3, &a.y, &a.z)
	f.mul(&z3, &z3, &z3)
	f.sub(&z3, &z3, &gamma)
	f.sub(&z3, &z3, &delta)
	f.sub(&t1, &t1, &x3) // 4β - x3
	f.mul(&y3, &alpha, &t1)
	f.mul(&t2, &gamma, &gamma)
	f.add(&t2, &t2, &t2)
	f.add(&t2, &t2, &t2)
	f.add(&t2, &t2, &t2) // 8γ²
	f.sub(&y3, &y3, &t2)
	r.x, r.y, r.z = x3, y3, z3
}

// add: add-2007-bl.
func (c *curve) add(r, a, b *jac) {
	f := c.f
	if f.isZero(&a.z) {
		*r = *b
		return
	}
	if f.isZero(&b.z) {
		*r = *a
		return
	}
	var z1z1, z2z2, u1, u2, s1, s2, h, rr, i, j, v, t fe
	f.mul(&z1z1, &a.z, &a.z)
	f.mul(&z2z2, &b.z, &b.z)
	f.mul(&u1, &a.x, &z2z2)
	f.mul(&u2, &b.x, &z1z1)
	f.mul(&s1, &a.y, &b.z)
	f.mul(&s1, &s1, &z2z2)
	f.mul(&s2, &b.y, &a.z)
	f.mul(&s2, &s2, &z1z1)
	f.sub(&h, &u2, &u1)
	f.sub(&rr, &s2, &s1)
	if f.isZero(&h) {
		if f.isZero(&rr) {
			c.double(r, a)
			return
		}
		*r = jac{}
		return
	}
	f.add(&rr, &rr, &rr)
	f.add(&i, &h, &h)
	f.mul(&i, &i, &i)
	f.mul(&j, &h, &i)
	f.mul(&v, &u1, &i)
	var x3, y3, z3 fe
	f.mul(&x3, &rr, &rr)
	f.sub(&x3, &x3, &j)
	f.sub(&x3, &x3, &v)
	f.sub(&x3, &x3, &v)
	f.sub(&t, &v, &x3)
	f.mul(&y3, &rr, &t)
	f.mul(&t, &s1, &j)
	f.add(&t, &t, &t)
	f.sub(&y3, &y3, &t)
	f.add(&z3, &a.z, &b.z)
	f.mul(&z3, &z3, &z3)
	f.sub(&z3, &z3, &z1z1)
	f.sub(&z3, &z3, &z2z2)
	f.mul(&z3, &z3, &h)
	r.x, r.y, r.z = x3, y3, z3
}

// mul2 computes u1·G + u2·Q (Shamir's trick).
func (c *curve) mul2(u1 *big.Int, q *ecPoint, u2 *big.Int) *ecPoint {
	g, qj := c.toJac(&c.g), c.toJac(q)
	var gq, acc jac
	c.add(&gq, &g, &qj)
	for i := max(u1.BitLen(), u2.BitLen()) - 1; i >= 0; i-- {
		c.double(&acc, &acc)
		switch b1, b2 := u1.Bit(i), u2.Bit(i); {
		case b1 == 1 && b2 == 1:
			c.add(&acc, &acc, &gq)
		case b1 == 1:
			c.add(&acc, &acc, &g)
		case b2 == 1:
			c.add(&acc, &acc, &qj)
		}
	}
	return c.affine(&acc)
}

func (c *curve) scalarBaseMul(k *big.Int) *ecPoint {
	return c.mul2(k, &ecPoint{inf: true}, new(big.Int))
}

// hashToInt truncates a digest to the bit length of n (SEC1 4.1.3).
func (c *curve) hashToInt(digest []byte) *big.Int {
	if size := (c.n.BitLen() + 7) / 8; len(digest) > size {
		digest = digest[:size]
	}
	e := new(big.Int).SetBytes(digest)
	if excess := len(digest)*8 - c.n.BitLen(); excess > 0 {
		e.Rsh(e, uint(excess))
	}
	return e
}

// ecdsaVerify checks an ASN.1 (r, s) signature over a digest.
func (c *curve) ecdsaVerify(q *ecPoint, digest, sig []byte) bool {
	seq, rest, err := expect(sig, 0x30)
	if err != nil || len(rest) != 0 {
		return false
	}
	r, seq, err := derInt(seq)
	if err != nil {
		return false
	}
	s, seq, err := derInt(seq)
	if err != nil || len(seq) != 0 {
		return false
	}
	if r.Sign() <= 0 || s.Sign() <= 0 || r.Cmp(c.n) >= 0 || s.Cmp(c.n) >= 0 {
		return false
	}
	e := c.hashToInt(digest)
	w := new(big.Int).ModInverse(s, c.n)
	u1 := e.Mul(e, w)
	u1.Mod(u1, c.n)
	u2 := w.Mul(r, w)
	u2.Mod(u2, c.n)
	p := c.mul2(u1, q, u2)
	if p.inf {
		return false
	}
	return p.x.Mod(p.x, c.n).Cmp(r) == 0
}

// ecdsaSign signs a digest with private scalar d. The nonce is hashed
// from the key, the digest and fresh randomness (so a weak random source
// alone doesn't leak the key).
func (c *curve) ecdsaSign(rand io.Reader, d *big.Int, digest []byte) ([]byte, error) {
	var entropy [32]byte
	if _, err := io.ReadFull(rand, entropy[:]); err != nil {
		return nil, err
	}
	e := c.hashToInt(digest)
	for ctr := byte(0); ; ctr++ {
		h := sha512.New()
		h.Write(d.FillBytes(make([]byte, c.size)))
		h.Write(digest)
		h.Write(entropy[:])
		h.Write([]byte{ctr})
		seed := h.Sum(nil) // 64 bytes: k is unbiased enough mod n
		k := new(big.Int).SetBytes(seed)
		k.Mod(k, c.n)
		if k.Sign() == 0 {
			continue
		}
		p := c.scalarBaseMul(k)
		r := new(big.Int).Mod(p.x, c.n)
		if r.Sign() == 0 {
			continue
		}
		s := new(big.Int).Mul(r, d)
		s.Add(s, e)
		s.Mul(s, new(big.Int).ModInverse(k, c.n))
		s.Mod(s, c.n)
		if s.Sign() == 0 {
			continue
		}
		return marshalECDSA(r, s), nil
	}
}

func marshalECDSA(r, s *big.Int) []byte {
	enc := func(v *big.Int) []byte {
		b := v.Bytes()
		if len(b) == 0 || b[0]&0x80 != 0 {
			b = append([]byte{0}, b...)
		}
		return derTLV(0x02, b)
	}
	return derTLV(0x30, append(enc(r), enc(s)...))
}

func derTLV(tag byte, v []byte) []byte {
	out := []byte{tag}
	switch l := len(v); {
	case l < 0x80:
		out = append(out, byte(l))
	case l < 0x100:
		out = append(out, 0x81, byte(l))
	default:
		out = append(out, 0x82, byte(l>>8), byte(l))
	}
	return append(out, v...)
}
