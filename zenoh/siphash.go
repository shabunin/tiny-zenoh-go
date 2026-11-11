package zenoh

import (
	"encoding/binary"
	"math/bits"
)

// sipHash13 is SipHash-1-3 with key (0, 0): what Rust's
// std::collections::hash_map::DefaultHasher computes, which zenoh-rust uses to
// break ties between equal link weights. Routers must agree bit for bit on
// edge weights to build the same routing trees.
func sipHash13(msg []byte) uint64 {
	v0 := uint64(0x736f6d6570736575)
	v1 := uint64(0x646f72616e646f6d)
	v2 := uint64(0x6c7967656e657261)
	v3 := uint64(0x7465646279746573)
	round := func() {
		v0 += v1
		v1 = bits.RotateLeft64(v1, 13)
		v1 ^= v0
		v0 = bits.RotateLeft64(v0, 32)
		v2 += v3
		v3 = bits.RotateLeft64(v3, 16)
		v3 ^= v2
		v0 += v3
		v3 = bits.RotateLeft64(v3, 21)
		v3 ^= v0
		v2 += v1
		v1 = bits.RotateLeft64(v1, 17)
		v1 ^= v2
		v2 = bits.RotateLeft64(v2, 32)
	}
	n := len(msg)
	for len(msg) >= 8 {
		m := binary.LittleEndian.Uint64(msg)
		v3 ^= m
		round()
		v0 ^= m
		msg = msg[8:]
	}
	var last [8]byte
	copy(last[:], msg)
	last[7] = byte(n)
	m := binary.LittleEndian.Uint64(last[:])
	v3 ^= m
	round()
	v0 ^= m
	v2 ^= 0xff
	round()
	round()
	round()
	return v0 ^ v1 ^ v2 ^ v3
}
