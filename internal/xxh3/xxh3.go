// Package xxh3 is XXH3-64 (seed 0, default secret), the hash zenoh's
// storage replication uses for its fingerprints (xxhash-rust's xxh3_64 /
// Xxh3::default). Hashing several byte strings with Sum is the same as
// streaming them into one hasher.
package xxh3

import (
	"encoding/binary"
	"math/bits"
)

const (
	prime32_1 = 0x9E3779B1
	prime32_2 = 0x85EBCA77
	prime32_3 = 0xC2B2AE3D
	prime64_1 = 0x9E3779B185EBCA87
	prime64_2 = 0xC2B2AE3D27D4EB4F
	prime64_3 = 0x165667B19E3779F9
	prime64_4 = 0x85EBCA77C2B2AE63
	prime64_5 = 0x27D4EB2F165667C5
	primeMX1  = 0x165667919E3779F9
	primeMX2  = 0x9FB21C651E98DF25
)

var secret = [192]byte{
	0xb8, 0xfe, 0x6c, 0x39, 0x23, 0xa4, 0x4b, 0xbe, 0x7c, 0x01, 0x81, 0x2c, 0xf7, 0x21, 0xad, 0x1c,
	0xde, 0xd4, 0x6d, 0xe9, 0x83, 0x90, 0x97, 0xdb, 0x72, 0x40, 0xa4, 0xa4, 0xb7, 0xb3, 0x67, 0x1f,
	0xcb, 0x79, 0xe6, 0x4e, 0xcc, 0xc0, 0xe5, 0x78, 0x82, 0x5a, 0xd0, 0x7d, 0xcc, 0xff, 0x72, 0x21,
	0xb8, 0x08, 0x46, 0x74, 0xf7, 0x43, 0x24, 0x8e, 0xe0, 0x35, 0x90, 0xe6, 0x81, 0x3a, 0x26, 0x4c,
	0x3c, 0x28, 0x52, 0xbb, 0x91, 0xc3, 0x00, 0xcb, 0x88, 0xd0, 0x65, 0x8b, 0x1b, 0x53, 0x2e, 0xa3,
	0x71, 0x64, 0x48, 0x97, 0xa2, 0x0d, 0xf9, 0x4e, 0x38, 0x19, 0xef, 0x46, 0xa9, 0xde, 0xac, 0xd8,
	0xa8, 0xfa, 0x76, 0x3f, 0xe3, 0x9c, 0x34, 0x3f, 0xf9, 0xdc, 0xbb, 0xc7, 0xc7, 0x0b, 0x4f, 0x1d,
	0x8a, 0x51, 0xe0, 0x4b, 0xcd, 0xb4, 0x59, 0x31, 0xc8, 0x9f, 0x7e, 0xc9, 0xd9, 0x78, 0x73, 0x64,
	0xea, 0xc5, 0xac, 0x83, 0x34, 0xd3, 0xeb, 0xc3, 0xc5, 0x81, 0xa0, 0xff, 0xfa, 0x13, 0x63, 0xeb,
	0x17, 0x0d, 0xdd, 0x51, 0xb7, 0xf0, 0xda, 0x49, 0xd3, 0x16, 0x55, 0x26, 0x29, 0xd4, 0x68, 0x9e,
	0x2b, 0x16, 0xbe, 0x58, 0x7d, 0x47, 0xa1, 0xfc, 0x8f, 0xf8, 0xb8, 0xd1, 0x7a, 0xd0, 0x31, 0xce,
	0x45, 0xcb, 0x3a, 0x8f, 0x95, 0x16, 0x04, 0x28, 0xaf, 0xd7, 0xfb, 0xca, 0xbb, 0x4b, 0x40, 0x7e,
}

func r64(b []byte, i int) uint64 { return binary.LittleEndian.Uint64(b[i:]) }
func r32(b []byte, i int) uint32 { return binary.LittleEndian.Uint32(b[i:]) }

func mulFold(a, b uint64) uint64 {
	hi, lo := bits.Mul64(a, b)
	return hi ^ lo
}

func avalanche(h uint64) uint64 {
	h ^= h >> 37
	h *= primeMX1
	return h ^ h>>32
}

func avalanche64(h uint64) uint64 {
	h ^= h >> 33
	h *= prime64_2
	h ^= h >> 29
	h *= prime64_3
	return h ^ h>>32
}

func rrmxmx(h uint64, n uint64) uint64 {
	h ^= bits.RotateLeft64(h, 49) ^ bits.RotateLeft64(h, 24)
	h *= primeMX2
	h ^= (h >> 35) + n
	h *= primeMX2
	return h ^ h>>28
}

func mix16(in []byte, i int, s int) uint64 {
	return mulFold(r64(in, i)^r64(secret[:], s), r64(in, i+8)^r64(secret[:], s+8))
}

// Sum returns the XXH3-64 hash of the concatenation of parts.
func Sum(parts ...[]byte) uint64 {
	if len(parts) == 1 {
		return Hash(parts[0])
	}
	var all []byte
	for _, p := range parts {
		all = append(all, p...)
	}
	return Hash(all)
}

// Hash returns the XXH3-64 hash of b.
func Hash(b []byte) uint64 {
	n := len(b)
	s := secret[:]
	switch {
	case n == 0:
		return avalanche64(r64(s, 56) ^ r64(s, 64))
	case n <= 3:
		c := uint32(b[0])<<16 | uint32(b[n>>1])<<24 | uint32(b[n-1]) | uint32(n)<<8
		return avalanche64(uint64(c) ^ uint64(r32(s, 0)^r32(s, 4)))
	case n <= 8:
		in1, in2 := r32(b, 0), r32(b, n-4)
		in64 := uint64(in2) + uint64(in1)<<32
		return rrmxmx(in64^(r64(s, 8)^r64(s, 16)), uint64(n))
	case n <= 16:
		lo := r64(b, 0) ^ (r64(s, 24) ^ r64(s, 32))
		hi := r64(b, n-8) ^ (r64(s, 40) ^ r64(s, 48))
		return avalanche(uint64(n) + bits.ReverseBytes64(lo) + hi + mulFold(lo, hi))
	case n <= 128:
		acc := uint64(n) * prime64_1
		if n > 32 {
			if n > 64 {
				if n > 96 {
					acc += mix16(b, 48, 96) + mix16(b, n-64, 112)
				}
				acc += mix16(b, 32, 64) + mix16(b, n-48, 80)
			}
			acc += mix16(b, 16, 32) + mix16(b, n-32, 48)
		}
		acc += mix16(b, 0, 0) + mix16(b, n-16, 16)
		return avalanche(acc)
	case n <= 240:
		acc := uint64(n) * prime64_1
		for i := 0; i < 8; i++ {
			acc += mix16(b, 16*i, 16*i)
		}
		acc = avalanche(acc)
		for i := 8; i < n/16; i++ {
			acc += mix16(b, 16*i, 16*(i-8)+3)
		}
		acc += mix16(b, n-16, 136-17)
		return avalanche(acc)
	}
	return hashLong(b)
}

func accumulate512(acc *[8]uint64, in []byte, i int, s int) {
	for j := 0; j < 8; j++ {
		v := r64(in, i+8*j)
		k := v ^ r64(secret[:], s+8*j)
		acc[j^1] += v
		acc[j] += (k & 0xFFFFFFFF) * (k >> 32)
	}
}

func hashLong(b []byte) uint64 {
	acc := [8]uint64{prime32_3, prime64_1, prime64_2, prime64_3, prime64_4, prime32_2, prime64_5, prime32_1}
	const stripesPerBlock = (192 - 64) / 8
	const blockLen = 64 * stripesPerBlock
	n := len(b)
	blocks := (n - 1) / blockLen
	for blk := 0; blk < blocks; blk++ {
		for st := 0; st < stripesPerBlock; st++ {
			accumulate512(&acc, b, blk*blockLen+st*64, st*8)
		}
		for j := range acc {
			a := acc[j]
			a ^= a >> 47
			a ^= r64(secret[:], 192-64+8*j)
			acc[j] = a * prime32_1
		}
	}
	stripes := ((n - 1) - blockLen*blocks) / 64
	for st := 0; st < stripes; st++ {
		accumulate512(&acc, b, blocks*blockLen+st*64, st*8)
	}
	accumulate512(&acc, b, n-64, 192-64-7)
	res := uint64(n) * prime64_1
	for i := 0; i < 4; i++ {
		res += mulFold(acc[2*i]^r64(secret[:], 11+16*i), acc[2*i+1]^r64(secret[:], 11+16*i+8))
	}
	return avalanche(res)
}
