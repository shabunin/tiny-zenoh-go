// Package keyexpr implements Zenoh key expressions: validation, canonical form,
// and the three relations that routing depends on — matching a concrete key,
// intersection, and inclusion.
//
// A key expression is a '/'-separated sequence of chunks. Three wildcards are
// supported, per the Zenoh 1.0.0 specification plus the `$*` sub-chunk wildcard
// implemented by the reference Zenoh stack:
//
//   - "*"   matches exactly one non-empty chunk (regex [^/]+).
//   - "**"  matches zero or more whole chunks.
//   - "$*"  a sub-chunk wildcard: matches any run of characters within a single
//     chunk (e.g. "a/b$*" matches "a/b", "a/because", "a/blue").
//
// A chunk starting with '@' is verbatim (zenoh 1.x): no wildcard matches it,
// only the identical chunk does, so "a/**" does not cover "a/@adv/x".
//
// The relations are defined by the spec as:
//
//   - A concrete key K matches a key expression E if K is in E's language.
//   - E1 intersects E2 if some concrete key matches both.
//   - E1 includes E2 if every key matching E2 also matches E1.
//
// Routing forwards a sample published on key A to a subscriber declared on key
// B if and only if A and B intersect.
//
// The relations are computed by dynamic programming over the chunks (and,
// within a chunk, over the glob tokens), in time quadratic at worst, as keys
// come from remote nodes. The package is dependency-free
// beyond the standard library and TinyGo-compatible (no reflection).
package keyexpr

import (
	"errors"
	"strings"
)

// Errors returned by New and Validate.
var (
	ErrEmpty       = errors.New("keyexpr: empty key expression")
	ErrEmptyChunk  = errors.New("keyexpr: empty chunk (// or leading/trailing /)")
	ErrBadChar     = errors.New("keyexpr: invalid character in chunk")
	ErrNotCanon    = errors.New("keyexpr: not in canonical form")
	ErrHasWildcard = errors.New("keyexpr: concrete key must not contain wildcards")
)

// -----------------------------------------------------------------------------
// Internal representation
// -----------------------------------------------------------------------------

// tok is one token of a single chunk's glob pattern: either a star (matching any
// run of non-'/' characters) or a literal byte.
type tok struct {
	star bool
	ch   byte
}

// patChunk is one chunk of a compiled key expression: either the "**" double
// wildcard, or a glob pattern (toks) for a single chunk.
type patChunk struct {
	dw   bool // "**"
	verb bool // verbatim: starts with '@'
	raw  string
	toks []tok
}

// chunkInter and chunkIncl apply the verbatim rule, then the glob relations.
func chunkInter(a, b patChunk) bool {
	if a.raw == b.raw {
		return true
	}
	if a.verb || b.verb {
		return false
	}
	return chunkIntersect(a.toks, b.toks)
}

func chunkIncl(a, b patChunk) bool {
	if a.raw == b.raw {
		return true
	}
	if a.verb || b.verb {
		return false
	}
	return chunkIncludes(a.toks, b.toks)
}

// Key is a validated, compiled key expression.
type Key struct {
	s  string
	pc []patChunk
}

// New validates s (which must be in canonical form) and returns a compiled Key.
func New(s string) (*Key, error) {
	if err := Validate(s); err != nil {
		return nil, err
	}
	return &Key{s: s, pc: compile(s)}, nil
}

// MustNew is like New but panics on error; intended for package-level constants
// and tests.
func MustNew(s string) *Key {
	k, err := New(s)
	if err != nil {
		panic(err)
	}
	return k
}

// String returns the canonical text of the key expression.
func (k *Key) String() string { return k.s }

// Intersects reports whether k and o share at least one concrete key.
func (k *Key) Intersects(o *Key) bool { return intersectChunks(k.pc, o.pc) }

// Includes reports whether every key matching o also matches k.
func (k *Key) Includes(o *Key) bool { return includesChunks(k.pc, o.pc) }

// Matches reports whether the concrete key (no wildcards) is in k's language.
func (k *Key) Matches(key string) bool {
	return matchChunks(k.pc, strings.Split(key, "/"))
}

func parseChunk(c string) []tok {
	if c == "*" {
		return []tok{{star: true}}
	}
	toks := make([]tok, 0, len(c))
	for i := 0; i < len(c); {
		if c[i] == '$' && i+1 < len(c) && c[i+1] == '*' {
			toks = append(toks, tok{star: true})
			i += 2
			continue
		}
		toks = append(toks, tok{ch: c[i]})
		i++
	}
	return toks
}

func compile(s string) []patChunk {
	parts := strings.Split(s, "/")
	pcs := make([]patChunk, len(parts))
	for i, p := range parts {
		if p == "**" {
			pcs[i] = patChunk{dw: true, raw: p}
		} else {
			pcs[i] = patChunk{verb: strings.HasPrefix(p, "@"), raw: p, toks: parseChunk(p)}
		}
	}
	return pcs
}

// -----------------------------------------------------------------------------
// Convenience functions on raw strings (compiled without canonical validation).
// -----------------------------------------------------------------------------

// Match reports whether the concrete key is in the language of key expression e.
func Match(e, key string) bool {
	return matchChunks(compile(e), strings.Split(key, "/"))
}

// Intersect reports whether key expressions a and b share a concrete key.
func Intersect(a, b string) bool { return intersectChunks(compile(a), compile(b)) }

// Include reports whether every key matching b also matches a.
func Include(a, b string) bool { return includesChunks(compile(a), compile(b)) }

// -----------------------------------------------------------------------------
// Key-expression-level relations (handle the '/' structure and "**").
// -----------------------------------------------------------------------------

// The relations below are decided over the suffixes a[i:], b[j:] by dynamic
// programming, a row (fixed i) at a time: trying every way a "**" can
// absorb chunks by recursion takes exponential time on keys like
// "**/a/**/a/**/a/**/b" against "a/a/a/a/a/a/c". next[j] is the relation of
// a[i+1:] and b[j:], cur[j] that of a[i:] and b[j:].

func matchChunk(p patChunk, c string) bool {
	if p.verb || strings.HasPrefix(c, "@") {
		return p.raw == c
	}
	return matchGlob(p.toks, c)
}

func matchChunks(pat []patChunk, key []string) bool {
	m := len(key)
	if !hasDoubleWild(pat) { // chunk by chunk (the table would be len(pat)·m)
		if len(pat) != m {
			return false
		}
		for i := range pat {
			if !matchChunk(pat[i], key[i]) {
				return false
			}
		}
		return true
	}
	next, cur := make([]bool, m+1), make([]bool, m+1)
	next[m] = true
	for i := len(pat) - 1; i >= 0; i-- {
		p := pat[i]
		cur[m] = p.dw && next[m]
		for j := m - 1; j >= 0; j-- {
			switch {
			case p.dw:
				// "**" matches zero chunks, or one more (never a verbatim one).
				cur[j] = next[j] || (!strings.HasPrefix(key[j], "@") && cur[j+1])
			default:
				cur[j] = next[j+1] && matchChunk(p, key[j])
			}
		}
		next, cur = cur, next
	}
	return next[0]
}

func hasDoubleWild(pcs []patChunk) bool {
	for _, c := range pcs {
		if c.dw {
			return true
		}
	}
	return false
}

func intersectChunks(a, b []patChunk) bool {
	if !hasDoubleWild(a) && !hasDoubleWild(b) {
		if len(a) != len(b) {
			return false
		}
		for i := range a {
			if !chunkInter(a[i], b[i]) {
				return false
			}
		}
		return true
	}
	m := len(b)
	next, cur := make([]bool, m+1), make([]bool, m+1)
	next[m] = true
	for j := m - 1; j >= 0; j-- {
		next[j] = b[j].dw && next[j+1] // a is empty: b must be all "**"
	}
	for i := len(a) - 1; i >= 0; i-- {
		cur[m] = a[i].dw && next[m]
		for j := m - 1; j >= 0; j-- {
			switch {
			case a[i].dw:
				// "**" consumes zero chunks, or absorbs one non-verbatim chunk from b.
				cur[j] = next[j] || (!b[j].verb && cur[j+1])
			case b[j].dw:
				cur[j] = cur[j+1] || (!a[i].verb && next[j])
			default:
				cur[j] = next[j+1] && chunkInter(a[i], b[j])
			}
		}
		next, cur = cur, next
	}
	return next[0]
}

func includesChunks(a, b []patChunk) bool { // does a include b?
	m := len(b)
	if !hasDoubleWild(a) {
		if len(a) != m {
			return false
		}
		for i := range a {
			if b[i].dw || !chunkIncl(a[i], b[i]) {
				return false
			}
		}
		return true
	}
	next, cur := make([]bool, m+1), make([]bool, m+1)
	next[m] = true
	for i := len(a) - 1; i >= 0; i-- {
		cur[m] = a[i].dw && next[m]
		for j := m - 1; j >= 0; j-- {
			switch {
			case a[i].dw:
				// "**" covers zero chunks of b, or one-or-more (any value, including "**").
				cur[j] = next[j] || (!b[j].verb && cur[j+1])
			case b[j].dw:
				// A single chunk cannot cover "**" (which spans any number of chunks).
				cur[j] = false
			default:
				cur[j] = next[j+1] && chunkIncl(a[i], b[j])
			}
		}
		next, cur = cur, next
	}
	return next[0]
}

// -----------------------------------------------------------------------------
// Single-chunk glob matching (concrete side has no wildcards).
// -----------------------------------------------------------------------------

// matchGlob reports whether the glob pattern toks matches concrete string s.
// Stars ("*" and "$*") match any run of characters, including empty.
func matchGlob(toks []tok, s string) bool {
	ti, si := 0, 0
	star, match := -1, 0
	for si < len(s) {
		switch {
		case ti < len(toks) && toks[ti].star:
			star, match = ti, si
			ti++
		case ti < len(toks) && !toks[ti].star && toks[ti].ch == s[si]:
			ti++
			si++
		case star != -1:
			ti = star + 1
			match++
			si = match
		default:
			return false
		}
	}
	for ti < len(toks) && toks[ti].star {
		ti++
	}
	return ti == len(toks)
}

// -----------------------------------------------------------------------------
// Chunk-level relations between two glob patterns. Both are quadratic at
// worst: an exploration of the product of the patterns' automata (over the
// literals plus one "other" byte) took seconds on remote chunks of a few
// kilobytes.
// -----------------------------------------------------------------------------

// chunkIntersect reports whether two single-chunk glob patterns share a
// concrete string, by dynamic programming over the suffixes a[i:], b[j:] (a
// product-automaton search costs about n² states of n·256 steps each, which
// a long remote chunk turns into seconds): a star either stops, or takes the
// other side's next token (a byte, or what the other star matches). next[j]
// is the answer for a[i+1:], b[j:]; cur[j] for a[i:], b[j:].
func chunkIntersect(a, b []tok) bool {
	m := len(b)
	next, cur := make([]bool, m+1), make([]bool, m+1)
	next[m] = true
	for j := m - 1; j >= 0; j-- {
		next[j] = b[j].star && next[j+1]
	}
	for i := len(a) - 1; i >= 0; i-- {
		cur[m] = a[i].star && next[m]
		for j := m - 1; j >= 0; j-- {
			switch {
			case a[i].star:
				cur[j] = next[j] || cur[j+1]
			case b[j].star:
				cur[j] = cur[j+1] || next[j]
			default:
				cur[j] = a[i].ch == b[j].ch && next[j+1]
			}
		}
		next, cur = cur, next
	}
	return next[0]
}

// chunkIncludes reports whether L(b) ⊆ L(a): every concrete string matched by b
// is matched by a. That is a matching b's tokens, b's stars being a symbol
// only a star of a matches: then a star of a covers whatever b's star
// stands for; and if a can't match that way, it can't match b's stars
// replaced by a byte a never uses.
func chunkIncludes(a, b []tok) bool {
	ti, si := 0, 0
	star, match := -1, 0
	for si < len(b) {
		switch {
		case ti < len(a) && a[ti].star:
			star, match = ti, si
			ti++
		case ti < len(a) && !b[si].star && a[ti].ch == b[si].ch:
			ti++
			si++
		case star != -1:
			ti = star + 1
			match++
			si = match
		default:
			return false
		}
	}
	for ti < len(a) && a[ti].star {
		ti++
	}
	return ti == len(a)
}
