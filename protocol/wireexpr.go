package protocol

import "github.com/shabunin/tiny-zenoh-go/internal/codec"

// WireExpr is a key expression as carried inside a network message. Instead of
// repeating the full text every time, Zenoh lets peers declare a numeric
// resource id (a "mapping") for a key-expression prefix; later messages send
// that Scope id plus an optional Suffix.
//
//	Scope == 0 && Suffix != ""  : the full key expression is in Suffix.
//	Scope != 0 && Suffix == ""  : the key expression is exactly the declared id.
//	Scope != 0 && Suffix != ""  : declared prefix (Scope) + Suffix.
//
// The two flag bits that describe a WireExpr live in the ENCLOSING message's
// header byte, not in the WireExpr body:
//
//	N (bit 5)  the Suffix field is present.
//	M (bit 6)  mapping space: 1 = sender's, 0 = receiver's.
//
// Call Flags to obtain those bits to OR into the parent header, and pass the
// parent's flags back to DecodeWireExpr.
type WireExpr struct {
	Scope   uint16
	Suffix  string
	Mapping bool // M flag
}

// WireExpr flag bits within the enclosing message header.
const (
	flagWireN = codec.Flag1 // bit 5: suffix present ("named")
	flagWireM = codec.Flag2 // bit 6: mapping is sender's space
)

// HasSuffix reports whether the WireExpr carries a suffix string.
func (we WireExpr) HasSuffix() bool { return we.Suffix != "" }

// Flags returns the N/M bits this WireExpr contributes to its enclosing
// message's header byte.
func (we WireExpr) Flags() byte {
	var f byte
	if we.HasSuffix() {
		f |= flagWireN
	}
	if we.Mapping {
		f |= flagWireM
	}
	return f
}

// Encode writes the WireExpr body: the scope id (VLE), then the suffix string
// when present. The caller must already have set the N/M flags (see Flags) in
// the enclosing header.
func (we WireExpr) Encode(w *codec.Writer) {
	w.WriteZInt(uint64(we.Scope))
	if we.HasSuffix() {
		w.WriteString(we.Suffix)
	}
}

// DecodeWireExpr reads a WireExpr body. named and mapping come from the N and M
// flags of the enclosing message header.
func DecodeWireExpr(r *codec.Reader, named, mapping bool) (WireExpr, error) {
	we := WireExpr{Mapping: mapping}
	scope, err := readU16(r)
	if err != nil {
		return WireExpr{}, err
	}
	we.Scope = scope
	if named {
		s, err := r.ReadString()
		if err != nil {
			return WireExpr{}, err
		}
		we.Suffix = s
	}
	return we, nil
}
