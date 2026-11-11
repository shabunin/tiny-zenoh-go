package tls13

import (
	"math/big"
	"math/bits"
)

// Arithmetic modulo the curve primes in Montgomery form on fixed arrays of
// 64-bit limbs: no heap allocations (math/big made a handshake churn
// megabytes of garbage on a board with a 300 KB heap) and much faster.
// Variable-time: it handles public values (signature verification) and,
// for client certificates, signing nonces on the device's own key.

const maxLimbs = 6 // P-384

// fe is a field element, little-endian limbs (only the first n used).
type fe [maxLimbs]uint64

type field struct {
	n    int // limbs
	p    fe
	pinv uint64 // -p⁻¹ mod 2⁶⁴
	r2   fe     // R² mod p, R = 2^(64n)
	one  fe     // R mod p: 1 in Montgomery form
	pm2  []byte // p - 2, big-endian, for inversion
}

func newField(p *big.Int) *field {
	f := &field{n: (p.BitLen() + 63) / 64}
	f.p = feFromBig(p)
	inv := uint64(1) // Newton: p·inv ≡ 1 mod 2⁶⁴
	for range 6 {
		inv *= 2 - f.p[0]*inv
	}
	f.pinv = -inv
	r := new(big.Int).Lsh(big.NewInt(1), uint(64*f.n))
	f.one = feFromBig(new(big.Int).Mod(r, p))
	f.r2 = feFromBig(new(big.Int).Mod(new(big.Int).Mul(r, r), p))
	f.pm2 = new(big.Int).Sub(p, big.NewInt(2)).Bytes()
	return f
}

func feFromBig(v *big.Int) fe {
	var z fe
	b := v.Bytes()
	for i, j := len(b)-1, 0; i >= 0; i, j = i-1, j+1 {
		z[j/8] |= uint64(b[i]) << (8 * (j % 8))
	}
	return z
}

func (f *field) toBig(x *fe) *big.Int {
	b := make([]byte, 8*f.n)
	for i := 0; i < f.n; i++ {
		for k := 0; k < 8; k++ {
			b[len(b)-1-8*i-k] = byte(x[i] >> (8 * k))
		}
	}
	return new(big.Int).SetBytes(b)
}

// mont converts a reduced big.Int into Montgomery form.
func (f *field) mont(v *big.Int) fe {
	x := feFromBig(v)
	var z fe
	f.mul(&z, &x, &f.r2)
	return z
}

// unmont converts out of Montgomery form.
func (f *field) unmont(x *fe) *big.Int {
	var one, z fe
	one[0] = 1
	f.mul(&z, x, &one)
	return f.toBig(&z)
}

// mul sets z = x·y·R⁻¹ mod p (CIOS Montgomery multiplication).
func (f *field) mul(z, x, y *fe) {
	n := f.n
	var t [maxLimbs + 2]uint64
	for i := 0; i < n; i++ {
		var c uint64
		xi := x[i]
		for j := 0; j < n; j++ {
			hi, lo := bits.Mul64(xi, y[j])
			lo, c1 := bits.Add64(lo, t[j], 0)
			hi += c1
			lo, c1 = bits.Add64(lo, c, 0)
			hi += c1
			t[j], c = lo, hi
		}
		var c1 uint64
		t[n], c1 = bits.Add64(t[n], c, 0)
		t[n+1] = c1
		m := t[0] * f.pinv
		hi, lo := bits.Mul64(m, f.p[0])
		_, c1 = bits.Add64(lo, t[0], 0)
		c = hi + c1
		for j := 1; j < n; j++ {
			hi, lo := bits.Mul64(m, f.p[j])
			lo, c1 := bits.Add64(lo, t[j], 0)
			hi += c1
			lo, c1 = bits.Add64(lo, c, 0)
			hi += c1
			t[j-1], c = lo, hi
		}
		t[n-1], c1 = bits.Add64(t[n], c, 0)
		t[n] = t[n+1] + c1
	}
	f.reduce(z, t[:n], t[n])
}

// reduce sets z = t (+ carry·2^(64n)) mod p, for t < 2p.
func (f *field) reduce(z *fe, t []uint64, carry uint64) {
	var s fe
	var b uint64
	for i := range t {
		s[i], b = bits.Sub64(t[i], f.p[i], b)
	}
	if carry == 0 && b == 1 { // t < p
		copy(z[:], t)
		return
	}
	*z = s
}

func (f *field) add(z, x, y *fe) {
	var t [maxLimbs]uint64
	var c uint64
	for i := 0; i < f.n; i++ {
		t[i], c = bits.Add64(x[i], y[i], c)
	}
	f.reduce(z, t[:f.n], c)
}

func (f *field) sub(z, x, y *fe) {
	var b uint64
	var t fe
	for i := 0; i < f.n; i++ {
		t[i], b = bits.Sub64(x[i], y[i], b)
	}
	if b != 0 {
		var c uint64
		for i := 0; i < f.n; i++ {
			t[i], c = bits.Add64(t[i], f.p[i], c)
		}
	}
	*z = t
}

func (f *field) isZero(x *fe) bool {
	var v uint64
	for i := 0; i < f.n; i++ {
		v |= x[i]
	}
	return v == 0
}

// inv sets z = x⁻¹ (x^(p-2)), in Montgomery form.
func (f *field) inv(z, x *fe) {
	r := f.one
	for _, by := range f.pm2 {
		for k := 7; k >= 0; k-- {
			f.mul(&r, &r, &r)
			if by>>k&1 == 1 {
				f.mul(&r, &r, x)
			}
		}
	}
	*z = r
}
