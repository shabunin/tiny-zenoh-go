package keyexpr

import (
	"regexp"
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"
)

var fuzzKeys = []string{
	"a", "a/b/c", "a/*/c", "a/**/c", "**", "a/b$*", "*/b", "a/%2A/b", "$*/x", "a/x$*y", "a/$*b$*c/d",
	"demo/example/**", "demo/@adv/pub/1/2/_", "@/router/x", "@a/**", "a/**/@v", "*/**", "a/**/b/c",
	"south:0:peer", "é/ü", "a/**/**", "a/**/*", "a//b", "/a", "a/", "$*", "a$b", "a#b", "a?b", "a*b",
	"**/a/**/a/**/a/**/a/**/a/**/b", "a/a/a/a/a/a/a/a/a/a/a/a/c", "$*ab$*ab$*x", "$*ab$*ab$*y$*",
}

// FuzzCanonicalize: canonical form is valid, idempotent and keeps the
// language of the key expression.
func FuzzCanonicalize(f *testing.F) {
	for _, k := range fuzzKeys {
		f.Add(k, []byte{1, 2, 3, 0})
	}
	f.Fuzz(func(t *testing.T, s string, choices []byte) {
		err := Validate(s)
		_, nerr := New(s)
		if (err == nil) != (nerr == nil) || IsCanonical(s) != (err == nil) {
			t.Fatalf("%q: Validate %v, New %v", s, err, nerr)
		}
		IsConcrete(s)
		IsNonWild(s)
		c := Canonicalize(s)
		if err == nil && c != s {
			t.Fatalf("valid %q canonicalized to %q", s, c)
		}
		if validateWellFormed(s) != nil {
			return
		}
		if err := Validate(c); err != nil {
			t.Fatalf("Canonicalize(%q) = %q: %v", s, c, err)
		}
		if again := Canonicalize(c); again != c {
			t.Fatalf("Canonicalize not idempotent: %q -> %q -> %q", s, c, again)
		}
		// (Include is only complete on canonical forms: "a/**/*" doesn't
		// include "a/*/**", its own canonical form.)
		ch := newChooser(choices, s)
		if w, ok := witness(s, ch); ok && !Match(c, w) {
			t.Fatalf("%q matches %q but not its canonical form %q", w, s, c)
		}
		if w, ok := witness(c, ch); ok && !Match(s, w) {
			t.Fatalf("%q matches the canonical form %q of %q but not it", w, c, s)
		}
	})
}

// FuzzRelations checks Intersect, Include and Match against each other,
// against concrete keys in a key expression's language, against a regexp
// matcher and (chunk by chunk) against the product-automaton relations.
func FuzzRelations(f *testing.F) {
	for i, a := range fuzzKeys {
		f.Add(a, fuzzKeys[(i*7+3)%len(fuzzKeys)], []byte{byte(i), 1, 2, 3})
	}
	f.Fuzz(func(t *testing.T, a, b string, choices []byte) {
		// Raw strings from the network reach the relations unvalidated.
		Intersect(a, b)
		Include(a, b)
		Match(a, b)
		if validateWellFormed(a) != nil || validateWellFormed(b) != nil {
			return
		}
		a, b = Canonicalize(a), Canonicalize(b)
		ka, kb := MustNew(a), MustNew(b)
		inter, incl := Intersect(a, b), Include(a, b)
		if Intersect(b, a) != inter {
			t.Fatalf("Intersect(%q, %q) = %v but not symmetric", a, b, inter)
		}
		if incl && !inter {
			t.Fatalf("Include(%q, %q) but not Intersect", a, b)
		}
		if ka.Intersects(kb) != inter || ka.Includes(kb) != incl {
			t.Fatalf("%q, %q: Key methods disagree", a, b)
		}
		if !Include(a, a) || !Intersect(a, a) {
			t.Fatalf("%q doesn't include or intersect itself", a)
		}
		if incl && Include(b, a) && a != b {
			t.Fatalf("%q and %q include each other but differ", a, b)
		}
		ch := newChooser(choices, a+b)
		for _, p := range [][2]string{{a, b}, {b, a}} {
			x, y := p[0], p[1]
			w, ok := witness(y, ch)
			if !ok {
				continue
			}
			mx := Match(x, w)
			if !Match(y, w) {
				t.Fatalf("witness %q not in %q", w, y)
			}
			if Include(x, y) && !mx {
				t.Fatalf("Include(%q, %q) but %q matches only the second", x, y, w)
			}
			if mx && !Intersect(x, y) {
				t.Fatalf("%q matches %q and %q, which don't intersect", w, x, y)
			}
			if Intersect(x, w) != mx || Include(x, w) != mx || MustNew(x).Matches(w) != mx {
				t.Fatalf("Match(%q, %q) = %v, Intersect/Include of the concrete key disagree", x, w, mx)
			}
			if re := globRegexp(x); re != nil && utf8.ValidString(w) && !strings.Contains(w, "\x00") {
				if got := re.MatchString(markVerbatim(w)); got != mx {
					t.Fatalf("Match(%q, %q) = %v, regexp %v", x, w, mx, got)
				}
			}
		}
		for _, x := range compile(a) {
			for _, y := range compile(b) {
				if x.dw || y.dw || len(x.toks) > 24 || len(y.toks) > 24 {
					continue
				}
				if chunkIntersect(x.toks, y.toks) != nfaIntersect(x.toks, y.toks) {
					t.Fatalf("chunkIntersect(%q, %q) differs from the automaton", x.raw, y.raw)
				}
				if chunkIncludes(x.toks, y.toks) != nfaIncludes(x.toks, y.toks) {
					t.Fatalf("chunkIncludes(%q, %q) differs from the automaton", x.raw, y.raw)
				}
			}
		}
	})
}

// FuzzStripNonWildPrefix: what is left of ke under prefix only covers keys
// of ke.
func FuzzStripNonWildPrefix(f *testing.F) {
	for _, c := range [][2]string{
		{"demo/example/test/**", "demo/example/test"}, {"*/example/test/1", "demo/example/test"},
		{"demo/**/ex$*/t$*/xyz", "demo/example/test"}, {"demo/example/test$*/something", "demo/example/test"},
		{"**/@demo/test/something", "@demo/test"}, {"@demo/**/@test/something", "@demo/example/@test"},
		{"**/x", "a@b"}, {"a/$*b$*c$*d/e", "a/bxcxdx"}, {"$*c/x", "abc"},
		{"a/$*b$*b$*b$*b$*b$*b$*b$*b$*b$*b$*b$*b$*c/x", "a/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"},
		{strings.Repeat("**/x/", 16) + "**/@v/y", strings.Repeat("x/", 40) + "@v"},
		{strings.Repeat("**/x/", 16) + "**/@w/y", strings.Repeat("x/", 40) + "@v"},
	} {
		f.Add(c[0], c[1], []byte{1, 2})
	}
	f.Fuzz(func(t *testing.T, ke, prefix string, choices []byte) {
		rest, ok := StripNonWildPrefix(ke, prefix)
		if Validate(ke) != nil || !IsNonWild(prefix) {
			return
		}
		// Without "**", stripping is exact: a key of ke under prefix is
		// prefix/w with w in rest. (With "**", zenoh's algorithm may leave
		// some out: "demo/**/ex$*/*/xyz" under "demo/example/test" is
		// "**/ex$*/*/xyz", without "xyz".)
		if w, wok := witness(ke, newChooser(choices, prefix)); wok && !strings.Contains(ke, "**") &&
			strings.HasPrefix(w, prefix+"/") && (!ok || !Match(rest, w[len(prefix)+1:])) {
			t.Fatalf("%q matches %q, but StripNonWildPrefix(%q, %q) = %q, %v", w, ke, ke, prefix, rest, ok)
		}
		if !ok {
			return
		}
		if Validate(rest) != nil {
			t.Fatalf("StripNonWildPrefix(%q, %q) = %q: not a key expression", ke, prefix, rest)
		}
		ch := newChooser(choices, ke)
		if w, ok := witness(rest, ch); ok && !Match(ke, prefix+"/"+w) {
			t.Fatalf("StripNonWildPrefix(%q, %q) = %q, but %q/%q doesn't match", ke, prefix, rest, prefix, w)
		}
	})
}

// chooser hands out the fuzzer's choices, then zeros.
type chooser struct {
	b     []byte
	alpha []byte // filler bytes: the literals in play, and 'z'
}

func newChooser(b []byte, lits string) *chooser {
	c := &chooser{b: b, alpha: []byte{'z'}}
	for i := 0; i < len(lits); i++ {
		switch ch := lits[i]; ch {
		case '/', '*', '$', '@', '#', '?':
		default:
			c.alpha = append(c.alpha, ch)
		}
	}
	return c
}

func (c *chooser) next() int {
	if len(c.b) == 0 {
		return 0
	}
	v := c.b[0]
	c.b = c.b[1:]
	return int(v)
}

func (c *chooser) fill(min int) string {
	n := min + c.next()%3
	var sb strings.Builder
	for range n {
		sb.WriteByte(c.alpha[c.next()%len(c.alpha)])
	}
	return sb.String()
}

// witness builds a concrete key matched by the (well-formed) key expression
// k, replacing its wildcards with filler.
func witness(k string, c *chooser) (string, bool) {
	var out []string
	for _, ch := range strings.Split(k, "/") {
		switch {
		case ch == "**":
			for range c.next() % 3 {
				out = append(out, c.fill(1))
			}
		case ch == "*":
			out = append(out, c.fill(1))
		case strings.HasPrefix(ch, "@"):
			out = append(out, ch) // verbatim: only itself (none if it has "$*")
		default:
			parts := strings.Split(ch, "$*")
			for i := 1; i < len(parts); i++ {
				parts[i] = c.fill(0) + parts[i]
			}
			s := strings.Join(parts, "")
			if s == "" || s[0] == '@' {
				s = "z" + s // "$*@x": a wildcard never yields a verbatim chunk
			}
			out = append(out, s)
		}
	}
	w := strings.Join(out, "/")
	return w, Validate(w) == nil && IsConcrete(w)
}

// markVerbatim prefixes the verbatim chunks of a concrete key with a NUL,
// which no wildcard of globRegexp matches.
func markVerbatim(k string) string {
	parts := strings.Split(k, "/")
	for i, p := range parts {
		if strings.HasPrefix(p, "@") {
			parts[i] = "\x00" + p
		}
	}
	return strings.Join(parts, "/") + "/"
}

// globRegexp translates a key expression into a regexp over markVerbatim
// keys (nil when it can't: invalid UTF-8 or NUL).
func globRegexp(k string) *regexp.Regexp {
	if !utf8.ValidString(k) || strings.Contains(k, "\x00") {
		return nil
	}
	const first = `(?:[^/\x00][^/]*)?` // a star at the start of a chunk
	var sb strings.Builder
	sb.WriteString("^")
	for _, ch := range strings.Split(k, "/") {
		switch {
		case ch == "**":
			sb.WriteString(`(?:[^/\x00][^/]*/)*`)
			continue
		case ch == "*":
			sb.WriteString(first)
		case strings.HasPrefix(ch, "@"):
			sb.WriteString(regexp.QuoteMeta("\x00" + ch))
		default:
			lits := strings.Split(ch, "$*")
			for i, lit := range lits {
				switch {
				case i == 1 && lits[0] == "":
					sb.WriteString(first)
				case i > 0:
					sb.WriteString(`[^/]*`)
				}
				sb.WriteString(regexp.QuoteMeta(lit))
			}
		}
		sb.WriteString("/")
	}
	sb.WriteString("$")
	return regexp.MustCompile(sb.String())
}

// The product-automaton chunk relations this package used before: a glob
// pattern over the byte alphabet is an NFA whose positions are token
// indices; a star has an epsilon edge to the next position and a self-loop
// on every symbol, a literal advances on its own byte only.

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

// nfaIntersect reports whether two single-chunk glob patterns share a
// concrete string (product-automaton reachability of a jointly-accepting state).
func nfaIntersect(a, b []tok) bool {
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

// nfaIncludes reports whether L(b) ⊆ L(a): every concrete string matched by b
// is matched by a. It fails as soon as it reaches a product state where b
// accepts but a does not — a witness string in b but not in a.
func nfaIncludes(a, b []tok) bool {
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

// FuzzConcreteFastPath: the allocation-free match of a concrete key against
// a pattern agrees with the general relations.
func FuzzConcreteFastPath(f *testing.F) {
	for _, c := range [][2]string{
		{"**", "a/b"}, {"a/**", "a"}, {"a/*/c", "a/b/c"}, {"**/b/**/c", "a/b/x/b/c"},
		{"a$*b/**", "axyb/c"}, {"$*a$*", "bab"}, {"a/**/b", "a/b/b/b"}, {"*/**/*", "x"},
	} {
		f.Add(c[0], c[1])
	}
	f.Fuzz(func(t *testing.T, pat, key string) {
		if Validate(pat) != nil || Validate(key) != nil || !plain(pat) || !plain(key) || !concrete(key) {
			return
		}
		want := matchChunks(compile(pat), strings.Split(key, "/"))
		if got := globKey(pat, key); got != want {
			t.Fatalf("globKey(%q, %q) = %v, want %v", pat, key, got, want)
		}
		if got := intersectChunks(compile(pat), compile(key)); got != want {
			t.Fatalf("intersectChunks(%q, %q) = %v, want %v", pat, key, got, want)
		}
		if Intersect(pat, key) != want || Intersect(key, pat) != want || Include(pat, key) != want || Match(pat, key) != want {
			t.Fatalf("public relations disagree on %q, %q", pat, key)
		}
	})
}
