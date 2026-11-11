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
// The chunk-level relations are computed on a small nondeterministic automaton
// so matching, intersection and inclusion share one provably-consistent core
// rather than three hand-rolled special cases. The package is dependency-free
// beyond the standard library and TinyGo-compatible (no reflection).
package keyexpr

import (
	"errors"
	"strconv"
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

func allDoubleWild(pcs []patChunk) bool {
	for _, c := range pcs {
		if !c.dw {
			return false
		}
	}
	return true
}

func matchChunks(pat []patChunk, key []string) bool {
	if len(pat) == 0 {
		return len(key) == 0
	}
	if pat[0].dw {
		// "**" matches zero chunks...
		if matchChunks(pat[1:], key) {
			return true
		}
		// ...or one or more (never a verbatim chunk).
		if len(key) > 0 && !strings.HasPrefix(key[0], "@") && matchChunks(pat, key[1:]) {
			return true
		}
		return false
	}
	if len(key) == 0 {
		return false
	}
	if pat[0].verb || strings.HasPrefix(key[0], "@") {
		if pat[0].raw != key[0] {
			return false
		}
	} else if !matchGlob(pat[0].toks, key[0]) {
		return false
	}
	return matchChunks(pat[1:], key[1:])
}

func intersectChunks(a, b []patChunk) bool {
	if len(a) == 0 {
		return allDoubleWild(b)
	}
	if len(b) == 0 {
		return allDoubleWild(a)
	}
	if a[0].dw {
		// "**" consumes zero chunks, or absorbs one non-verbatim chunk from b.
		return intersectChunks(a[1:], b) || (!b[0].verb && intersectChunks(a, b[1:]))
	}
	if b[0].dw {
		return intersectChunks(a, b[1:]) || (!a[0].verb && intersectChunks(a[1:], b))
	}
	if !chunkInter(a[0], b[0]) {
		return false
	}
	return intersectChunks(a[1:], b[1:])
}

func includesChunks(a, b []patChunk) bool { // does a include b?
	if len(a) == 0 {
		return len(b) == 0
	}
	if a[0].dw {
		// "**" covers zero chunks of b, or one-or-more (any value, including "**").
		if includesChunks(a[1:], b) {
			return true
		}
		if len(b) > 0 && !b[0].verb && includesChunks(a, b[1:]) {
			return true
		}
		return false
	}
	if len(b) == 0 {
		return false
	}
	if b[0].dw {
		// A single chunk cannot cover "**" (which spans any number of chunks).
		return false
	}
	if !chunkIncl(a[0], b[0]) {
		return false
	}
	return includesChunks(a[1:], b[1:])
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
// Chunk-level automaton: a glob pattern over the byte alphabet is treated as an
// NFA whose positions are token indices. A "star" token has an epsilon edge to
// the next position (skip) and a self-loop on every symbol (consume one char);
// a literal token advances on its own byte only. Intersection and inclusion are
// decided by exploring the product of two such automata.
// -----------------------------------------------------------------------------

const other = 256 // a symbol standing for "some byte not among the literals"

// epsClosure expands a set of positions along star-skip epsilon edges.
func epsClosure(toks []tok, in []int) []int {
	seen := make(map[int]bool, len(in)+2)
	stack := append([]int(nil), in...)
	for _, i := range in {
		seen[i] = true
	}
	for len(stack) > 0 {
		i := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if i < len(toks) && toks[i].star && !seen[i+1] {
			seen[i+1] = true
			stack = append(stack, i+1)
		}
	}
	return sortedKeys(seen)
}

// step advances a position set by consuming one symbol, then takes the epsilon
// closure of the result.
func step(toks []tok, set []int, sym int) []int {
	seen := make(map[int]bool, len(set))
	for _, i := range set {
		if i >= len(toks) {
			continue
		}
		if toks[i].star {
			seen[i] = true // star consumes any symbol and stays
		} else if int(toks[i].ch) == sym {
			seen[i+1] = true
		}
	}
	return epsClosure(toks, sortedKeys(seen))
}

func accepts(toks []tok, set []int) bool {
	for _, i := range set {
		if i == len(toks) {
			return true
		}
	}
	return false
}

// alphabet returns the distinct literal bytes used by either pattern, plus the
// "other" sentinel. A string over this alphabet is representative of all
// concrete strings for the purpose of intersection/inclusion.
func alphabet(a, b []tok) []int {
	m := make(map[int]bool)
	for _, t := range a {
		if !t.star {
			m[int(t.ch)] = true
		}
	}
	for _, t := range b {
		if !t.star {
			m[int(t.ch)] = true
		}
	}
	syms := sortedKeys(m)
	return append(syms, other)
}

// chunkIntersect reports whether two single-chunk glob patterns share a
// concrete string (product-automaton reachability of a jointly-accepting state).
func chunkIntersect(a, b []tok) bool {
	syms := alphabet(a, b)
	start := prodState{epsClosure(a, []int{0}), epsClosure(b, []int{0})}
	visited := map[string]bool{}
	stack := []prodState{start}
	for len(stack) > 0 {
		st := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		key := st.key()
		if visited[key] {
			continue
		}
		visited[key] = true
		if accepts(a, st.a) && accepts(b, st.b) {
			return true
		}
		for _, sym := range syms {
			na := step(a, st.a, sym)
			nb := step(b, st.b, sym)
			if len(na) == 0 || len(nb) == 0 {
				continue
			}
			stack = append(stack, prodState{na, nb})
		}
	}
	return false
}

// chunkIncludes reports whether L(b) ⊆ L(a): every concrete string matched by b
// is matched by a. It fails as soon as it reaches a product state where b
// accepts but a does not — a witness string in b but not in a.
func chunkIncludes(a, b []tok) bool {
	syms := alphabet(a, b)
	start := prodState{epsClosure(a, []int{0}), epsClosure(b, []int{0})}
	visited := map[string]bool{}
	stack := []prodState{start}
	for len(stack) > 0 {
		st := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		key := st.key()
		if visited[key] {
			continue
		}
		visited[key] = true
		if accepts(b, st.b) && !accepts(a, st.a) {
			return false
		}
		for _, sym := range syms {
			nb := step(b, st.b, sym)
			if len(nb) == 0 {
				continue // b is dead on this symbol: no witness this way
			}
			na := step(a, st.a, sym) // may be empty (a dead): kept so a stays non-accepting
			stack = append(stack, prodState{na, nb})
		}
	}
	return true
}

type prodState struct {
	a, b []int
}

func (p prodState) key() string {
	var sb strings.Builder
	for _, i := range p.a {
		sb.WriteString(strconv.Itoa(i))
		sb.WriteByte(',')
	}
	sb.WriteByte('|')
	for _, i := range p.b {
		sb.WriteString(strconv.Itoa(i))
		sb.WriteByte(',')
	}
	return sb.String()
}

func sortedKeys(m map[int]bool) []int {
	out := make([]int, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	// insertion sort: sets are tiny.
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j-1] > out[j]; j-- {
			out[j-1], out[j] = out[j], out[j-1]
		}
	}
	return out
}
