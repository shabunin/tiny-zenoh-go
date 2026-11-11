package keyexpr

import "strings"

// Validate reports whether s is a well-formed key expression in canonical form.
//
// Well-formed requires: non-empty; no empty chunks (so no leading/trailing '/'
// and no "//"); every chunk uses only unreserved characters, percent-encoding,
// or the wildcards "*", "**", "$*".
//
// Canonical additionally requires: no chunk is the bare "$*" (write "*"); no
// "**" is adjacent to another "**"; and a "**" is never immediately followed by
// a "*" (the canonical order is "*/**"). Canonicalize rewrites a valid-but-
// non-canonical expression into this form.
func Validate(s string) error {
	if err := validateWellFormed(s); err != nil {
		return err
	}
	if Canonicalize(s) != s {
		return ErrNotCanon
	}
	return nil
}

func validateWellFormed(s string) error {
	if s == "" {
		return ErrEmpty
	}
	for _, c := range strings.Split(s, "/") {
		if c == "" {
			return ErrEmptyChunk
		}
		if err := validChunk(c); err != nil {
			return err
		}
	}
	return nil
}

func validChunk(c string) error {
	if c == "*" || c == "**" {
		return nil
	}
	for i := 0; i < len(c); i++ {
		ch := c[i]
		switch {
		case ch == '$':
			if i+1 < len(c) && c[i+1] == '*' {
				i++ // consume the "$*" wildcard
				continue
			}
			// a lone '$' is an unreserved character.
		case ch == '*':
			// A bare '*' may only appear as the whole chunk ("*"/"**") or in "$*".
			return ErrBadChar
		case ch == '%':
			if i+2 >= len(c) || !isHex(c[i+1]) || !isHex(c[i+2]) {
				return ErrBadChar
			}
			i += 2
		case isUnreserved(ch):
			// ok
		default:
			return ErrBadChar
		}
	}
	return nil
}

func isUnreserved(ch byte) bool {
	switch {
	case ch >= 'a' && ch <= 'z', ch >= 'A' && ch <= 'Z', ch >= '0' && ch <= '9':
		return true
	}
	switch ch {
	case '-', '_', '.', '~', '@', '$':
		return true
	}
	return false
}

func isHex(ch byte) bool {
	return (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f') || (ch >= 'A' && ch <= 'F')
}

// IsCanonical reports whether s is well-formed and already in canonical form.
func IsCanonical(s string) bool { return Validate(s) == nil }

// Canonicalize returns the canonical form of a well-formed key expression:
// collapses runs of "**", rewrites a bare "$*" chunk to "*", and reorders
// "**/*" to "*/**". It does not validate characters; feed it well-formed input.
func Canonicalize(s string) string {
	if s == "" {
		return s
	}
	parts := strings.Split(s, "/")
	out := make([]string, 0, len(parts))
	for _, c := range parts {
		if c == "" {
			continue // drop empty chunks
		}
		if c == "$*" {
			c = "*"
		}
		out = append(out, c)
	}
	// Iteratively collapse "**/**" -> "**" and swap "**/*" -> "*/**".
	for changed := true; changed; {
		changed = false
		for i := 0; i+1 < len(out); i++ {
			switch {
			case out[i] == "**" && out[i+1] == "**":
				out = append(out[:i], out[i+1:]...)
				changed = true
			case out[i] == "**" && out[i+1] == "*":
				out[i], out[i+1] = "*", "**"
				changed = true
			}
			if changed {
				break
			}
		}
	}
	return strings.Join(out, "/")
}

// IsConcrete reports whether key contains no wildcards (it is a concrete key,
// not a pattern).
func IsConcrete(key string) bool {
	for _, c := range strings.Split(key, "/") {
		if c == "*" || c == "**" {
			return false
		}
		if strings.Contains(c, "$*") {
			return false
		}
	}
	return true
}
