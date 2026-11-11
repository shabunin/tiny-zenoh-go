package keyexpr

import "strings"

// IsNonWild reports whether s is a valid key expression without wildcards
// ("*", "**", "$*"), as zenoh's nonwild_keyexpr: what a namespace may be.
func IsNonWild(s string) bool {
	return Validate(s) == nil && !strings.Contains(s, "*")
}

// StripNonWildPrefix returns what is left of key expression ke once the
// non-wild prefix is removed: the part of ke that, appended to prefix, gives
// the keys of ke under prefix. Wildcards in ke may cover (part of) prefix,
// e.g. "**" stripped of "a/b" is "**" and "*/b/c" is "c". ok is false when ke
// has nothing under prefix (ke equal to prefix included). Port of zenoh-rust
// keyexpr::strip_nonwild_prefix, used by session namespaces.
func StripNonWildPrefix(ke, prefix string) (rest string, ok bool) {
	return stripInner(ke, prefix)
}

func indexSlash(s string, from int) int {
	if i := strings.IndexByte(s[from:], '/'); i >= 0 {
		return from + i
	}
	return len(s)
}

func stripInner(target, prefix string) (string, bool) {
	ti, pi := 0, 0
	for ti < len(target) && pi < len(prefix) {
		tEnd := indexSlash(target, ti)
		pEnd := indexSlash(prefix, pi)
		tChunk := target[ti:tEnd]
		if tChunk == "**" {
			rem := prefix[pi:]
			p := strings.IndexByte(rem, '@')
			if p < 0 {
				// "**" can match all remaining non-verbatim chunks.
				return target[ti:], true
			}
			if tEnd+1 >= len(target) {
				return "", false // a final "**" may not match @verbatim chunks
			}
			for {
				// Let "**" match as many non-verbatim chunks as possible.
				if ke, ok := stripInner(target[tEnd+1:], rem[p:]); ok {
					return ke, true
				}
				if p == 0 {
					return "", false
				}
				p -= 2 // back to the start of the previous chunk
				for p > 0 && rem[p-1] != '/' {
					p--
				}
			}
		}
		if tEnd == len(target) {
			// No more chunks in target than in prefix, the last not "**".
			return "", false
		}
		if !chunkMatching(tChunk, prefix[pi:pEnd]) {
			return "", false
		}
		if pEnd == len(prefix) {
			return target[tEnd+1:], true
		}
		ti, pi = tEnd+1, pEnd+1
	}
	return "", false
}

// chunkMatching reports whether a target chunk (possibly with "*" / "$*")
// matches a non-wild prefix chunk.
func chunkMatching(target, prefix string) bool {
	ti, pi := 0, 0
	prev := byte('/')
	if len(prefix) > 0 && prefix[0] == '@' && (len(target) == 0 || target[0] != '@') {
		return false // a verbatim chunk only matches a verbatim chunk
	}
	for ti < len(target) && pi < len(prefix) {
		switch c := target[ti]; {
		case c == '*':
			if prev == '*' || ti+1 == len(target) {
				return true // "**", or a "*" ending the chunk
			}
			if prev == '$' {
				for i := pi; i < len(prefix)-1; i++ {
					if chunkMatching(target[ti+1:], prefix[i:]) {
						return true
					}
				}
			}
		case c == prefix[pi]:
			pi++
		case c != '$':
			return false
		}
		prev = target[ti]
		ti++
	}
	if pi != len(prefix) {
		return false
	}
	return ti == len(target) || (ti+2 == len(target) && target[ti] == '$')
}
