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
	return stripInner(ke, prefix, nil)
}

// stripMemo caches stripInner by the lengths of its arguments (suffixes of
// ke and prefix): trying every split of prefix for each "**" otherwise
// takes exponential time.
type stripMemo map[[2]int]stripResult

type stripResult struct {
	rest string
	ok   bool
}

func indexSlash(s string, from int) int {
	if i := strings.IndexByte(s[from:], '/'); i >= 0 {
		return from + i
	}
	return len(s)
}

// verbatimChunk returns where the first chunk of s starting with '@' starts
// (a '@' elsewhere in a chunk is an ordinary character), or -1.
func verbatimChunk(s string) int {
	if strings.HasPrefix(s, "@") {
		return 0
	}
	if i := strings.Index(s, "/@"); i >= 0 {
		return i + 1
	}
	return -1
}

func stripInner(target, prefix string, memo stripMemo) (string, bool) {
	if memo != nil {
		k := [2]int{len(target), len(prefix)}
		r, ok := memo[k]
		if !ok {
			r.rest, r.ok = stripChunks(target, prefix, memo)
			memo[k] = r
		}
		return r.rest, r.ok
	}
	return stripChunks(target, prefix, nil)
}

func stripChunks(target, prefix string, memo stripMemo) (string, bool) {
	ti, pi := 0, 0
	for ti < len(target) && pi < len(prefix) {
		tEnd := indexSlash(target, ti)
		pEnd := indexSlash(prefix, pi)
		tChunk := target[ti:tEnd]
		if tChunk == "**" {
			rem := prefix[pi:]
			p := verbatimChunk(rem)
			if p < 0 {
				// "**" can match all remaining non-verbatim chunks.
				return target[ti:], true
			}
			if tEnd+1 >= len(target) {
				return "", false // a final "**" may not match @verbatim chunks
			}
			if memo == nil {
				memo = stripMemo{}
			}
			for {
				// Let "**" match as many non-verbatim chunks as possible.
				if ke, ok := stripInner(target[tEnd+1:], rem[p:], memo); ok {
					return ke, true
				}
				if p == 0 {
					return "", false
				}
				p = max(p-2, 0) // back to the start of the previous chunk (prefix "a//@x" has an empty one)
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
// matches a non-wild prefix chunk. (Trying every split for each "$*"
// recursively took exponential time on remote keys such as "$*b$*b$*b...c",
// and missed a "$*" taking all but the last byte, as "$*c" for "abc".)
func chunkMatching(target, prefix string) bool {
	if strings.HasPrefix(target, "@") || strings.HasPrefix(prefix, "@") {
		return target == prefix // a verbatim chunk only matches itself
	}
	return matchGlob(parseChunk(target), prefix)
}
