package keyexpr

import "testing"

func TestMatch(t *testing.T) {
	cases := []struct {
		e, key string
		want   bool
	}{
		// single wildcard "*" — exactly one chunk
		{"a/*/c", "a/b/c", true},
		{"a/*/c", "a/xyz/c", true},
		{"a/*/c", "a/b/d/c", false},
		{"a/*/c", "a/c", false},
		// "**" — zero or more chunks
		{"a/**/c", "a/c", true},
		{"a/**/c", "a/b/c", true},
		{"a/**/c", "a/b/d/c", true},
		{"a/**/c", "a/c/d", false},
		{"**", "a/b/c", true},
		{"**", "x", true},
		// "$*" — sub-chunk wildcard
		{"a/b$*", "a/b", true},
		{"a/b$*", "a/because", true},
		{"a/b$*", "a/blue", true},
		{"a/b$*", "a/c", false},
		{"$*/x", "anything/x", true},
		{"a/x$*y", "a/xINNERy", true},
		{"a/x$*y", "a/xy", true},
		{"a/x$*y", "a/xyz", false},
		// exact
		{"a/b/c", "a/b/c", true},
		{"a/b/c", "a/b/d", false},
	}
	for _, c := range cases {
		if got := Match(c.e, c.key); got != c.want {
			t.Errorf("Match(%q, %q) = %v, want %v", c.e, c.key, got, c.want)
		}
	}
}

func TestIntersect(t *testing.T) {
	cases := []struct {
		a, b string
		want bool
	}{
		{"a/*/b", "a/c/b", true},
		{"a/*/b", "a/*/c", false},
		{"a/*/b", "b/*/a", false},
		{"a/**/b", "a/**/b/c", false}, // spec example
		{"a/**/b", "a/x/y/b", true},
		{"**", "any/thing", true},
		{"a/**", "a/b/c", true},
		{"a/**", "b/c", false},
		{"*/*/*", "a/b/c", true},
		{"*/*/*", "a/b", false},
		{"a/b$*", "a/$*c", true},  // "a/bc" matches both
		{"a/b$*", "a/x$*", false}, // chunk 2 must start with both b and x
		{"foo/bar", "foo/bar", true},
		{"foo/bar", "foo/baz", false},
		// intersection is symmetric
		{"a/**/b/c", "a/**/b", false},
	}
	for _, c := range cases {
		if got := Intersect(c.a, c.b); got != c.want {
			t.Errorf("Intersect(%q, %q) = %v, want %v", c.a, c.b, got, c.want)
		}
		if got := Intersect(c.b, c.a); got != c.want {
			t.Errorf("Intersect(%q, %q) [swapped] = %v, want %v", c.b, c.a, got, c.want)
		}
	}
}

func TestInclude(t *testing.T) {
	cases := []struct {
		a, b string
		want bool
	}{
		{"a/*", "a/b", true},
		{"a/b", "a/*", false},
		{"**", "a/b/c", true},
		{"**", "**", true},
		{"a/**/c", "a/x/c", true},
		{"a/**/c", "a/c", true},
		{"a/**", "a/b/c/d", true},
		{"*/**", "a/b/c", true},
		{"a/*", "a/*", true},
		{"a/*/c", "a/**/c", false}, // "*" cannot cover "**"
		{"a/b$*", "a/b", true},
		{"a/b$*", "a/because", true},
		{"a/b$*", "a/c", false},
		{"a/$*", "a/*", true}, // $* and * both match any single non-empty chunk
		{"a/*", "a/$*", true},
	}
	for _, c := range cases {
		if got := Include(c.a, c.b); got != c.want {
			t.Errorf("Include(%q, %q) = %v, want %v", c.a, c.b, got, c.want)
		}
	}
}

func TestIncludeImpliesIntersect(t *testing.T) {
	// If a includes b then a intersects b (b is non-empty language).
	pairs := [][2]string{
		{"a/*", "a/b"}, {"**", "x/y/z"}, {"a/**/c", "a/1/2/c"}, {"*/**", "p/q"},
	}
	for _, p := range pairs {
		if Include(p[0], p[1]) && !Intersect(p[0], p[1]) {
			t.Errorf("Include(%q,%q) but not Intersect", p[0], p[1])
		}
	}
}

func TestValidateAndCanon(t *testing.T) {
	valid := []string{"a", "a/b/c", "a/*/c", "a/**/c", "**", "a/b$*", "*/b", "a/%2A/b"}
	for _, s := range valid {
		if err := Validate(s); err != nil {
			t.Errorf("Validate(%q) = %v, want nil", s, err)
		}
	}
	// "$*/b" and "a/$*" are well-formed but NOT canonical: a lone "$*" chunk
	// must be written "*".
	invalid := []string{"", "/a", "a/", "a//b", "a/**/**", "a/**/*", "$*", "$*/b", "a/b*c", "a/*x"}
	for _, s := range invalid {
		if err := Validate(s); err == nil {
			t.Errorf("Validate(%q) = nil, want error", s)
		}
	}
	canon := map[string]string{
		"a/**/**":           "a/**",
		"a/**/*":            "a/*/**",
		"robot/sensor/**/*": "robot/sensor/*/**",
		"a/$*":              "a/*",
		"a//b":              "a/b",
		"a/**/**/**/b":      "a/**/b",
		"a/b/c":             "a/b/c",
	}
	for in, want := range canon {
		if got := Canonicalize(in); got != want {
			t.Errorf("Canonicalize(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestNewRejectsNonCanon(t *testing.T) {
	if _, err := New("a/**/**"); err == nil {
		t.Errorf("New(non-canonical) = nil error, want ErrNotCanon")
	}
	k, err := New("a/*/c")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if !k.Matches("a/b/c") {
		t.Errorf("Key.Matches failed")
	}
	o := MustNew("a/**/c")
	if !o.Includes(k) {
		t.Errorf("a/**/c should include a/*/c")
	}
}
