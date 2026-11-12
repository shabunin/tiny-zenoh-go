package json5

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

// FuzzToJSON checks that ToJSON never panics, that whatever it accepts
// comes out as valid JSON, and that it agrees with encoding/json on
// plain JSON (a JSON text is a JSON5 text with the same value; JSON5
// accepts more, so encoding/json refusing an input says nothing).
func FuzzToJSON(f *testing.F) {
	for _, s := range []string{
		`{mode: 'router', listen: {endpoints: ["tcp/0.0.0.0:7447",],},}`,
		"// comment\n{a: 1, /* block */ b: [+1, -2, .5, 5., 0x1F, 1e3, -Infinity, NaN]}",
		`{"quoted": "a\"b", str: 'it\'s \x41B \
continued', $id_2: null, t: true, f: false}`,
		`{"a": [1, 2.5e-3, -0, "x\u00e9\ud83d\ude00", {"b": null}], "c": false}`,
		`"\v\0\a"`, `"\uD800"`, `'\uDBFF\uDFFF'`, "\"tab\tin\"", "\"\xff\"", "{\xc3\xa9: 1}",
		`--1`, `+-1`, `01`, `.`, `.e5`, `1.e5`, `-.5`, `0x`, `-0x10`, `1e400`, `[1 2]`, `{a:}`,
		"\ufeff{}", "\u00a0[]", `/* unterminated`, strings.Repeat("[", 200) + strings.Repeat("]", 200),
	} {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, src []byte) {
		out, err := ToJSON(src)
		var v5 any
		if err == nil {
			if !json.Valid(out) {
				t.Fatalf("ToJSON(%q) = %q: not JSON", src, out)
			}
			if again, err := ToJSON(out); err != nil || string(again) != string(out) {
				t.Fatalf("ToJSON not stable on its output %q: %q, %v", out, again, err)
			}
			if err := json.Unmarshal(out, &v5); err != nil {
				// Only the float range differs (1e400 is valid JSON text).
				if !strings.Contains(err.Error(), "cannot unmarshal number") {
					t.Fatalf("ToJSON(%q) = %q: %v", src, out, err)
				}
				return
			}
		}
		var vj any
		if json.Unmarshal(src, &vj) != nil {
			return
		}
		if err != nil {
			if strings.Contains(err.Error(), "nested too deep") {
				return
			}
			t.Fatalf("JSON %q refused: %v", src, err)
		}
		if !reflect.DeepEqual(v5, vj) {
			t.Fatalf("JSON %q: got %#v (via %q), want %#v", src, v5, out, vj)
		}
	})
}
