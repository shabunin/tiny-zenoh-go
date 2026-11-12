package protocol

import (
	"errors"

	"github.com/shabunin/tiny-zenoh-go/internal/codec"
)

// ErrReservedExtEnc is returned when an extension uses the reserved ENC value.
var ErrReservedExtEnc = errors.New("protocol: reserved extension encoding")

// ExtEnc is an extension's payload-encoding selector (bits 6:5 of the extension
// header byte).
type ExtEnc byte

// Extension encodings.
const (
	ExtUnit ExtEnc = 0 // no payload
	ExtZ64  ExtEnc = 1 // one VLE integer
	ExtZBuf ExtEnc = 2 // VLE-length-prefixed byte array
	// 3 is reserved and MUST NOT be used.
)

// Extension is one extension block. The extension header byte is laid out as:
//
//	bit  7    Z   another extension follows
//	bits 6:5  ENC payload encoding (Unit/Z64/ZBuf)
//	bit  4    M   mandatory (unknown mandatory ext => reject message)
//	bits 3:0  ID
type Extension struct {
	ID        byte
	Enc       ExtEnc
	Mandatory bool
	U64       uint64 // valid when Enc == ExtZ64
	Buf       []byte // valid when Enc == ExtZBuf
	// ts, when set, is written instead of Buf: a timestamp extension made
	// while encoding, so its encoding needs no buffer that outlives the
	// call (which would put the encoder's extensions on the heap).
	ts *Timestamp
}

// WriteExtensions writes a chain of extensions, setting the Z ("more") bit on
// every extension except the last. The caller is responsible for setting the
// enclosing message's Z flag when len(exts) > 0.
func WriteExtensions(w *codec.Writer, exts []Extension) {
	for i := range exts {
		e := &exts[i]
		hdr := (e.ID & 0x0f) | (byte(e.Enc) << 5)
		if e.Mandatory {
			hdr |= 0x10
		}
		if i < len(exts)-1 {
			hdr |= 0x80 // more extensions follow
		}
		_ = w.WriteByte(hdr)
		switch e.Enc {
		case ExtZ64:
			w.WriteZInt(e.U64)
		case ExtZBuf:
			if e.ts != nil {
				var b [32]byte // 9 + 1 + 16 at most
				w.WriteZBytes(appendTimestamp(b[:0], e.ts))
			} else {
				w.WriteZBytes(e.Buf)
			}
		case ExtUnit:
			// no payload
		}
	}
}

// ReadExtensions reads a chain of extensions until one has the Z bit clear.
// It should be called only when the enclosing message's Z flag is set.
func ReadExtensions(r *codec.Reader) ([]Extension, error) {
	return readExts(nil, r, true)
}

// readExts appends an extension chain to out; ZBuf contents are copied out
// of r's buffer when copyBufs is set, and alias it otherwise (for callers
// that only parse them).
func readExts(out []Extension, r *codec.Reader, copyBufs bool) ([]Extension, error) {
	for {
		hdr, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		e := Extension{
			ID:        hdr & 0x0f,
			Enc:       ExtEnc((hdr >> 5) & 0x03),
			Mandatory: hdr&0x10 != 0,
		}
		switch e.Enc {
		case ExtUnit:
		case ExtZ64:
			v, err := r.ReadZInt()
			if err != nil {
				return nil, err
			}
			e.U64 = v
		case ExtZBuf:
			b, err := r.ReadZBytes()
			if err != nil {
				return nil, err
			}
			if copyBufs {
				// copy out of the aliased buffer so the extension can outlive it
				b = append([]byte(nil), b...)
			}
			e.Buf = b
		default:
			return nil, ErrReservedExtEnc
		}
		out = append(out, e)
		if hdr&0x80 == 0 {
			break
		}
	}
	return out, nil
}
