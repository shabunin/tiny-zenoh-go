package json5

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestToJSON(t *testing.T) {
	src := `// zenoh-style config
{
  mode: 'router', /* block
  comment */
  listen: { endpoints: ["tcp/0.0.0.0:7447",], },
  "quoted": "a\"b",
  num: [+1, -2, .5, 5., 0x1F, 1e3, -Infinity, NaN],
  str: 'it\'s \x41B \
continued',
  $id_2: null, t: true, f: false,
  nested: [{a: 1}, [],],
}`
	b, err := ToJSON([]byte(src))
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("%v\n%s", err, b)
	}
	want := map[string]any{
		"mode":   "router",
		"listen": map[string]any{"endpoints": []any{"tcp/0.0.0.0:7447"}},
		"quoted": `a"b`,
		"num":    []any{1.0, -2.0, 0.5, 5.0, 31.0, 1000.0, nil, nil},
		"str":    "it's AB continued",
		"$id_2":  nil, "t": true, "f": false,
		"nested": []any{map[string]any{"a": 1.0}, []any{}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %#v\n%s", got, b)
	}
	for _, bad := range []string{`{a:}`, `{a 1}`, `[1 2]`, `'x`, `{a:1} x`, `{wat: nope}`} {
		if _, err := ToJSON([]byte(bad)); err == nil {
			t.Errorf("%s: no error", bad)
		}
	}
}
