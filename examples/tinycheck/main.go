// Command tinycheck is a minimal program that exercises the tiny-zenoh-go core
// packages. Its purpose is to prove that the codec and keyexpr packages compile
// and run under TinyGo (no reflection, no unsupported stdlib):
//
//	tinygo build -o /dev/null ./examples/tinycheck
//	tinygo run ./examples/tinycheck
package main

import (
	"github.com/shabunin/tiny-zenoh-go/internal/codec"
	"github.com/shabunin/tiny-zenoh-go/keyexpr"
)

func main() {
	// Exercise the wire codec.
	w := codec.NewWriter()
	w.WriteZInt(300)
	w.WriteString("demo/key")
	r := codec.NewReader(w.Bytes())
	n, _ := r.ReadZInt()
	s, _ := r.ReadString()
	if n != 300 || s != "demo/key" {
		panic("codec round-trip failed")
	}

	// Exercise key expressions.
	sub := keyexpr.MustNew("demo/**")
	if !sub.Matches("demo/example/zenoh") {
		panic("keyexpr match failed")
	}
	if !keyexpr.Intersect("a/*/c", "a/b/c") {
		panic("keyexpr intersect failed")
	}
	println("tiny-zenoh-go core OK:", n, s)
}
