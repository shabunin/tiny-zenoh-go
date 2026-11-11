package keyexpr

import "testing"

// zenoh-rust's test_keyexpr_strip_nonwild_prefix table.
func TestStripNonWildPrefix(t *testing.T) {
	cases := []struct{ ke, prefix, want string }{
		{"demo/example/test/**", "demo/example/test", "**"},
		{"demo/example/**", "demo/example/test", "**"},
		{"**", "demo/example/test", "**"},
		{"*/example/test/1", "demo/example/test", "1"},
		{"demo/*/test/1", "demo/example/test", "1"},
		{"*/*/test/1", "demo/example/test", "1"},
		{"*/*/*/1", "demo/example/test", "1"},
		{"*/test/1", "demo/example/test", ""},
		{"*/*/1", "demo/example/test", ""},
		{"*/*/**", "demo/example/test", "**"},
		{"demo/example/test/**/x$*/**", "demo/example/test", "**/x$*/**"},
		{"demo/**/xyz", "demo/example/test", "**/xyz"},
		{"demo/**/test/**", "demo/example/test", "**/test/**"},
		{"demo/**/ex$*/*/xyz", "demo/example/test", "**/ex$*/*/xyz"},
		{"demo/**/ex$*/t$*/xyz", "demo/example/test", "**/ex$*/t$*/xyz"},
		{"demo/**/te$*/*/xyz", "demo/example/test", "**/te$*/*/xyz"},
		{"demo/example/test", "demo/example/test", ""},
		{"demo/example/test1/something", "demo/example/test", ""},
		{"demo/example/test$*/something", "demo/example/test", "something"},
		{"*/example/test/something", "@demo/example/test", ""},
		{"**/test/something", "@demo/example/test", ""},
		{"**/test/something", "demo/example/@test", ""},
		{"@demo/*/test/something", "@demo/example/test", "something"},
		{"@demo/*/test/something", "@demo/@example/test", ""},
		{"**/@demo/test/something", "@demo/test", "something"},
		{"**/@test/something", "demo/@test", "something"},
		{"@demo/**/@test/something", "@demo/example/@test", "something"},
	}
	for _, c := range cases {
		got, ok := StripNonWildPrefix(c.ke, c.prefix)
		if ok != (c.want != "") || got != c.want {
			t.Errorf("StripNonWildPrefix(%q, %q) = %q, %v; want %q", c.ke, c.prefix, got, ok, c.want)
		}
	}
}

func TestIsNonWild(t *testing.T) {
	for s, want := range map[string]bool{"a/b": true, "@x/y": true, "a/*": false, "a/**": false, "a$*": false, "a//b": false} {
		if IsNonWild(s) != want {
			t.Errorf("IsNonWild(%q) != %v", s, want)
		}
	}
}
