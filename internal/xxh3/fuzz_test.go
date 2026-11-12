package xxh3

import "testing"

// FuzzHash runs every length branch on arbitrary bytes (the replication
// fingerprints hash keys received from other replicas) and checks that Sum
// over any split is the hash of the whole.
func FuzzHash(f *testing.F) {
	for _, v := range vectors {
		f.Add(input(v.n), uint16(v.n/2))
	}
	f.Add([]byte("a/b\xef\xcd\xab\x89\x67\x45\x23\x01\xab\xcd\x01"), uint16(3))
	f.Fuzz(func(t *testing.T, b []byte, split uint16) {
		h := Hash(b)
		i := int(split) % (len(b) + 1)
		if s := Sum(b[:i], b[i:]); s != h {
			t.Fatalf("len %d split %d: Sum %016x, Hash %016x", len(b), i, s, h)
		}
		if s := Sum(b[:i:i], nil, b[i:]); s != h {
			t.Fatalf("len %d split %d: Sum with an empty part %016x, Hash %016x", len(b), i, s, h)
		}
	})
}
