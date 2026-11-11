// Package json5 turns JSON5 text (the format of zenoh's configuration
// files) into plain JSON for encoding/json: comments, unquoted keys,
// single-quoted strings, trailing commas, hexadecimal numbers, leading or
// trailing decimal points, explicit '+', Infinity/NaN (as null: JSON has no
// such numbers) and line continuations in strings.
package json5

import (
	"fmt"
	"strconv"
	"strings"
)

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
	s   []byte
	i   int
	out strings.Builder
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
			end := strings.Index(string(c.s[c.i+2:]), "*/")
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

func (c *conv) object() error {
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
			c.out.WriteString(strconv.Quote(id))
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
			c.out.WriteString(strconv.Quote(b.String()))
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
	d := c.i
	for c.i < len(c.s) && strings.IndexByte("0123456789.eE+-", c.s[c.i]) >= 0 {
		c.i++
	}
	num := string(c.s[d:c.i])
	if strings.HasPrefix(num, ".") {
		num = "0" + num
	}
	if strings.HasSuffix(num, ".") {
		num += "0"
	}
	num = strings.Replace(num, ".e", ".0e", 1)
	num = strings.Replace(num, ".E", ".0E", 1)
	if _, err := strconv.ParseFloat(num, 64); err != nil {
		return c.errf("bad number %q", num)
	}
	if neg {
		c.out.WriteByte('-')
	}
	c.out.WriteString(num)
	return nil
}
