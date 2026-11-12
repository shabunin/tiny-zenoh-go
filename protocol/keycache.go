package protocol

import "sync/atomic"

// keyCache remembers recently decoded key expressions, so that the same key
// arriving over and over (a publisher sending full keys, a router forwarding
// them) is one string instead of an allocation per message. It is a
// direct-mapped table of immutable strings: lock-free, bounded, and a
// collision only costs the allocation it would have saved.
var keyCache [keyCacheSize]atomic.Pointer[string]

// keyCacheMaxLen keeps long keys out of the cache.
const keyCacheMaxLen = 128

func internKey(b []byte) string {
	if keyCacheSize == 0 || len(b) > keyCacheMaxLen {
		return string(b)
	}
	h := uint32(2166136261) // FNV-1a
	for _, c := range b {
		h = (h ^ uint32(c)) * 16777619
	}
	slot := &keyCache[h%keyCacheSize]
	if p := slot.Load(); p != nil && *p == string(b) {
		return *p
	}
	s := string(b)
	slot.Store(&s)
	return s
}
