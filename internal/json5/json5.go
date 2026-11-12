// Package json5 turns JSON5 text (the format of zenoh's configuration
// files) into plain JSON for encoding/json: comments, unquoted keys,
// single-quoted strings, trailing commas, hexadecimal numbers, leading or
// trailing decimal points, explicit '+', Infinity/NaN (as null: JSON has no
// such numbers) and line continuations in strings.
package json5

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"
)

// maxDepth bounds the nesting of objects and arrays, so that hostile input
// can't exhaust the stack (small on TinyGo).
const maxDepth = 64

// ToJSON converts JSON5 to JSON.
func ToJSON(src []byte) ([]byte, error) {
	c := &conv{s: src}
	c.ws()
	if err := c.value(); err != nil {
		return nil, err
	}
	c.ws()
	if c.i < len(c.s) {
		return nil, c.errf("unexpected %q after the value", c.s[c.i])
	}
	return []byte(c.out.String()), nil
}

type conv struct {
	s     []byte
	i     int
	depth int
	out   strings.Builder
}

func (c *conv) errf(f string, a ...any) error {
	line := 1 + strings.Count(string(c.s[:min(c.i, len(c.s))]), "\n")
	return fmt.Errorf("json5: line %d: %s", line, fmt.Sprintf(f, a...))
}

// ws skips whitespace and comments.
func (c *conv) ws() {
	for c.i < len(c.s) {
		switch ch := c.s[c.i]; {
		case ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' || ch == '\f' || ch == '\v':
			c.i++
		case ch == 0xC2 && c.i+1 < len(c.s) && c.s[c.i+1] == 0xA0: // NBSP
			c.i += 2
		case ch == 0xEF && c.i+2 < len(c.s) && c.s[c.i+1] == 0xBB && c.s[c.i+2] == 0xBF: // BOM
			c.i += 3
		case ch == '/' && c.i+1 < len(c.s) && c.s[c.i+1] == '/':
			for c.i < len(c.s) && c.s[c.i] != '\n' {
				c.i++
			}
		case ch == '/' && c.i+1 < len(c.s) && c.s[c.i+1] == '*':
			end := bytes.Index(c.s[c.i+2:], []byte("*/"))
			if end < 0 {
				c.i = len(c.s)
				return
			}
			c.i += end + 4
		default:
			return
		}
	}
}

func (c *conv) value() error {
	if c.i >= len(c.s) {
		return c.errf("unexpected end of input")
	}
	switch ch := c.s[c.i]; {
	case ch == '{':
		return c.object()
	case ch == '[':
		return c.array()
	case ch == '"' || ch == '\'':
		return c.str()
	case ch == '-' || ch == '+' || ch == '.' || (ch >= '0' && ch <= '9'):
		return c.number()
	default:
		id := c.ident()
		switch id {
		case "true", "false", "null":
			c.out.WriteString(id)
		case "Infinity", "NaN":
			c.out.WriteString("null")
		case "":
			return c.errf("unexpected %q", ch)
		default:
			return c.errf("unexpected word %q", id)
		}
		return nil
	}
}

func (c *conv) nest() error {
	if c.depth++; c.depth > maxDepth {
		return c.errf("nested too deep")
	}
	return nil
}

func (c *conv) object() error {
	if err := c.nest(); err != nil {
		return err
	}
	c.i++ // {
	c.out.WriteByte('{')
	first := true
	for {
		c.ws()
		if c.i >= len(c.s) {
			return c.errf("unterminated object")
		}
		if c.s[c.i] == '}' {
			c.i++
			c.depth--
			c.out.WriteByte('}')
			return nil
		}
		if !first {
			c.out.WriteByte(',')
		}
		first = false
		if ch := c.s[c.i]; ch == '"' || ch == '\'' {
			if err := c.str(); err != nil {
				return err
			}
		} else {
			id := c.ident()
			if id == "" {
				return c.errf("expected a key, got %q", ch)
			}
			c.quote(id)
		}
		c.ws()
		if c.i >= len(c.s) || c.s[c.i] != ':' {
			return c.errf("expected ':'")
		}
		c.i++
		c.out.WriteByte(':')
		c.ws()
		if err := c.value(); err != nil {
			return err
		}
		c.ws()
		if c.i < len(c.s) && c.s[c.i] == ',' {
			c.i++
		} else if c.i < len(c.s) && c.s[c.i] != '}' {
			return c.errf("expected ',' or '}'")
		}
	}
}

func (c *conv) array() error {
	if err := c.nest(); err != nil {
		return err
	}
	c.i++ // [
	c.out.WriteByte('[')
	first := true
	for {
		c.ws()
		if c.i >= len(c.s) {
			return c.errf("unterminated array")
		}
		if c.s[c.i] == ']' {
			c.i++
			c.depth--
			c.out.WriteByte(']')
			return nil
		}
		if !first {
			c.out.WriteByte(',')
		}
		first = false
		if err := c.value(); err != nil {
			return err
		}
		c.ws()
		if c.i < len(c.s) && c.s[c.i] == ',' {
			c.i++
		} else if c.i < len(c.s) && c.s[c.i] != ']' {
			return c.errf("expected ',' or ']'")
		}
	}
}

// ident reads an identifier (unquoted key or keyword).
func (c *conv) ident() string {
	start := c.i
	for c.i < len(c.s) {
		ch := c.s[c.i]
		if ch == '_' || ch == '$' || ch >= 0x80 || (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') ||
			(c.i > start && ch >= '0' && ch <= '9') {
			c.i++
			continue
		}
		break
	}
	return string(c.s[start:c.i])
}

func (c *conv) str() error {
	q := c.s[c.i]
	c.i++
	var b strings.Builder
	for {
		if c.i >= len(c.s) {
			return c.errf("unterminated string")
		}
		ch := c.s[c.i]
		c.i++
		switch {
		case ch == q:
			c.quote(b.String())
			return nil
		case ch == '\n':
			return c.errf("newline in string")
		case ch != '\\':
			b.WriteByte(ch)
			continue
		}
		if c.i >= len(c.s) {
			return c.errf("unterminated string")
		}
		e := c.s[c.i]
		c.i++
		switch e {
		case 'n':
			b.WriteByte('\n')
		case 't':
			b.WriteByte('\t')
		case 'r':
			b.WriteByte('\r')
		case 'b':
			b.WriteByte('\b')
		case 'f':
			b.WriteByte('\f')
		case 'v':
			b.WriteByte('\v')
		case '0':
			b.WriteByte(0)
		case '\n': // line continuation
		case '\r':
			if c.i < len(c.s) && c.s[c.i] == '\n' {
				c.i++
			}
		case 'x', 'u':
			n := 2
			if e == 'u' {
				n = 4
			}
			if c.i+n > len(c.s) {
				return c.errf("bad escape")
			}
			v, err := strconv.ParseUint(string(c.s[c.i:c.i+n]), 16, 32)
			if err != nil {
				return c.errf("bad escape")
			}
			c.i += n
			r := rune(v)
			if e == 'u' && r >= 0xD800 && r < 0xDC00 && c.i+6 <= len(c.s) && c.s[c.i] == '\\' && c.s[c.i+1] == 'u' {
				if lo, err := strconv.ParseUint(string(c.s[c.i+2:c.i+6]), 16, 32); err == nil && lo >= 0xDC00 && lo < 0xE000 {
					r = 0x10000 + (r-0xD800)<<10 + (rune(lo) - 0xDC00)
					c.i += 6
				}
			}
			b.WriteRune(r)
		default:
			b.WriteByte(e) // \' \" \\ \/ and any other character as itself
		}
	}
}

func (c *conv) number() error {
	start := c.i
	neg := false
	if ch := c.s[c.i]; ch == '+' || ch == '-' {
		neg = ch == '-'
		c.i++
	}
	if id := c.ident(); id == "Infinity" || id == "NaN" {
		c.out.WriteString("null")
		return nil
	} else if id != "" {
		return c.errf("bad number %q", string(c.s[start:c.i]))
	}
	if c.i+1 < len(c.s) && c.s[c.i] == '0' && (c.s[c.i+1] == 'x' || c.s[c.i+1] == 'X') {
		c.i += 2
		h := c.i
		for c.i < len(c.s) && strings.IndexByte("0123456789abcdefABCDEF", c.s[c.i]) >= 0 {
			c.i++
		}
		v, err := strconv.ParseUint(string(c.s[h:c.i]), 16, 64)
		if err != nil {
			return c.errf("bad hex number")
		}
		if neg {
			c.out.WriteByte('-')
		}
		c.out.WriteString(strconv.FormatUint(v, 10))
		return nil
	}
	// JSON5 decimal: digits with no leading zero, '.' and digits, an
	// exponent; the integer or the fraction may be missing, not both.
	intPart := c.digits()
	if c.i < len(c.s) && c.s[c.i] == '.' {
		c.i++
	}
	frac := c.digits()
	if intPart == "" && frac == "" || len(intPart) > 1 && intPart[0] == '0' {
		return c.errf("bad number %q", string(c.s[start:c.i]))
	}
	if intPart == "" {
		intPart = "0"
	}
	num := intPart
	if frac != "" {
		num += "." + frac
	}
	if c.i < len(c.s) && (c.s[c.i] == 'e' || c.s[c.i] == 'E') {
		e := c.i
		c.i++
		if c.i < len(c.s) && (c.s[c.i] == '+' || c.s[c.i] == '-') {
			c.i++
		}
		if c.digits() == "" {
			return c.errf("bad number %q", string(c.s[start:c.i]))
		}
		num += string(c.s[e:c.i])
	}
	if _, err := strconv.ParseFloat(num, 64); err != nil {
		return c.errf("bad number %q", num)
	}
	if neg {
		c.out.WriteByte('-')
	}
	c.out.WriteString(num)
	return nil
}

func (c *conv) digits() string {
	d := c.i
	for c.i < len(c.s) && c.s[c.i] >= '0' && c.s[c.i] <= '9' {
		c.i++
	}
	return string(c.s[d:c.i])
}

// quote writes s as a JSON string (strconv.Quote's \x, \v, \U escapes are
// not JSON); invalid UTF-8 becomes U+FFFD, as encoding/json does.
func (c *conv) quote(s string) {
	const hex = "0123456789abcdef"
	c.out.WriteByte('"')
	for i := 0; i < len(s); {
		r, n := utf8.DecodeRuneInString(s[i:])
		switch {
		case r == '"' || r == '\\':
			c.out.WriteByte('\\')
			c.out.WriteByte(byte(r))
		case r < 0x20:
			c.out.WriteString(`\u00`)
			c.out.WriteByte(hex[r>>4])
			c.out.WriteByte(hex[r&0xF])
		case r == utf8.RuneError && n == 1:
			c.out.WriteRune(utf8.RuneError)
		default:
			c.out.WriteString(s[i : i+n])
		}
		i += n
	}
	c.out.WriteByte('"')
}
