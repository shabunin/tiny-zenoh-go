package keyexpr

import "testing"

// Characters zenoh-rust accepts and refuses in key expressions.
func TestValidateCharacters(t *testing.T) {
	for _, k := range []string{"south:0:peer", "a b/c,d", "é/ü", "a%zz", "a+b=c", "@/x/@adv", "a$*b", "$*a/b$*"} {
		if err := Validate(k); err != nil {
			t.Errorf("%q refused: %v", k, err)
		}
	}
	for _, k := range []string{"a$b", "a$", "$", "a#b", "a?b", "a*b", "**a", "a/", "/a", "a//b"} {
		if err := Validate(k); err == nil {
			t.Errorf("%q accepted", k)
		}
	}
}
