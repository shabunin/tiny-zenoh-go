package protocol

import (
	"errors"
	"time"

	"github.com/shabunin/tiny-zenoh-go/internal/codec"
)

// Errors shared by network/data message decoders.
var (
	// ErrUnknownMandatoryExt is returned when a message carries an extension
	// with the M (mandatory) bit that this implementation does not understand.
	ErrUnknownMandatoryExt = errors.New("protocol: unknown mandatory extension")
	// ErrOutOfRange is returned when a bounded integer exceeds its bound.
	ErrOutOfRange = errors.New("protocol: value out of range")
)

// key returns the extension's identity as zenoh-rust matches it: the header
// byte without the Z bit (ID | M | ENC).
func (e Extension) key() byte {
	k := (e.ID & 0x0f) | byte(e.Enc)<<5
	if e.Mandatory {
		k |= 0x10
	}
	return k
}

// extFromKey builds an Extension from a header-style key (ID | M | ENC).
func extFromKey(k byte) Extension {
	return Extension{ID: k & 0x0f, Enc: ExtEnc((k >> 5) & 0x03), Mandatory: k&0x10 != 0}
}

func extZ64(k byte, v uint64) Extension { e := extFromKey(k); e.U64 = v; return e }
func extZBuf(k byte, b []byte) Extension {
	e := extFromKey(k)
	e.Buf = b
	return e
}

// checkUnknown returns an error if an extension nobody claimed is mandatory.
func checkUnknown(e Extension) error {
	if e.Mandatory {
		return ErrUnknownMandatoryExt
	}
	return nil
}

func readU16(r *codec.Reader) (uint16, error) {
	v, err := r.ReadZInt()
	if err != nil {
		return 0, err
	}
	if v > 0xffff {
		return 0, ErrOutOfRange
	}
	return uint16(v), nil
}

func readU32(r *codec.Reader) (uint32, error) {
	v, err := r.ReadZInt()
	if err != nil {
		return 0, err
	}
	if v > 0xffffffff {
		return 0, ErrOutOfRange
	}
	return uint32(v), nil
}

// -----------------------------------------------------------------------------
// QoS
// -----------------------------------------------------------------------------

// Priority is a Zenoh message priority class (lower is more urgent).
type Priority byte

// Priorities.
const (
	PriorityControl         Priority = 0
	PriorityRealTime        Priority = 1
	PriorityInteractiveHigh Priority = 2
	PriorityInteractiveLow  Priority = 3
	PriorityDataHigh        Priority = 4
	PriorityData            Priority = 5 // default
	PriorityDataLow         Priority = 6
	PriorityBackground      Priority = 7
)

// QoS is the network-message QoS extension value:
//
//	bits 0:2 priority, bit 3 D (don't drop / congestion-control Block),
//	bit 4 E (express: don't batch), bit 5 F (block first).
//
// The Go zero value means "default" (priority Data, drop, not express = 0x05),
// which is omitted on the wire. Decoders normalize an explicit 0x05 to 0. The
// wire value 0x00 (priority Control, drop, not express) is held as qosZero so
// that it stays distinct from the default; use NewQoS and Wire rather than
// converting bytes.
type QoS byte

// qosZero is the in-memory form of wire value 0x00 (bit 6 is never set on
// the wire).
const qosZero QoS = 0x40

// QoSFromWire converts an extension value as decoded from the wire.
func QoSFromWire(v uint64) QoS {
	q := QoS(v & 0x3f)
	switch q {
	case DefaultQoS:
		return 0
	case 0:
		return qosZero
	}
	return q
}

// Wire returns the value zenoh puts on the wire.
func (q QoS) Wire() byte { return byte(q.norm() & 0x3f) }

// DefaultQoS is the wire value zenoh omits.
const DefaultQoS QoS = 0x05

// QoS presets used by zenoh-rust for specific message kinds.
const (
	QoSDeclare QoS = 0x08 // Control + Block
	QoSRequest QoS = 0x0d // Data + Block
)

// NewQoS composes a QoS value.
func NewQoS(p Priority, block, express bool) QoS {
	q := QoS(p & 0x07)
	if block {
		q |= 0x08
	}
	if express {
		q |= 0x10
	}
	switch q {
	case DefaultQoS:
		return 0
	case 0:
		return qosZero
	}
	return q
}

func (q QoS) norm() QoS {
	if q == 0 {
		return DefaultQoS
	}
	return q &^ qosZero
}

// Priority returns the priority class.
func (q QoS) Priority() Priority { return Priority(q.norm() & 0x07) }

// Block reports whether congestion control blocks instead of dropping.
func (q QoS) Block() bool { return q.norm()&0x08 != 0 }

// Express reports whether the message must not be batched.
func (q QoS) Express() bool { return q.norm()&0x10 != 0 }

// -----------------------------------------------------------------------------
// Timestamp (HLC: NTP64 time + source ZID)
// -----------------------------------------------------------------------------

// Timestamp is a hybrid-logical-clock timestamp: a 64-bit NTP time (32.32
// fixed point, seconds since the UNIX epoch as used by uhlc) and the ZID of the
// node that produced it.
type Timestamp struct {
	Time uint64
	ID   ZID
}

// NTP64FromTime converts t to uhlc's 32.32 fixed-point representation.
func NTP64FromTime(t time.Time) uint64 {
	secs := uint64(t.Unix())
	frac := uint64(t.Nanosecond()) << 32 / 1_000_000_000
	return secs<<32 | frac
}

// GoTime converts the timestamp's NTP64 time to a time.Time.
func (ts Timestamp) GoTime() time.Time {
	secs := int64(ts.Time >> 32)
	nanos := int64((ts.Time & 0xffffffff) * 1_000_000_000 >> 32)
	return time.Unix(secs, nanos)
}

func (ts *Timestamp) encode(w *codec.Writer) {
	w.WriteZInt(ts.Time)
	w.WriteZBytes(ts.ID.Bytes())
}

func decodeTimestamp(r *codec.Reader) (*Timestamp, error) {
	t, err := r.ReadZInt()
	if err != nil {
		return nil, err
	}
	b, err := r.ReadZBytes()
	if err != nil {
		return nil, err
	}
	id, err := NewZID(b)
	if err != nil {
		return nil, err
	}
	return &Timestamp{Time: t, ID: id}, nil
}

// -----------------------------------------------------------------------------
// Encoding
// -----------------------------------------------------------------------------

// Encoding describes a payload's format: a numeric id (see the zenoh package
// for the well-known table) plus an optional schema. Schema nil means absent.
type Encoding struct {
	ID     uint16
	Schema []byte
}

// IsEmpty reports whether e is the default encoding (id 0, no schema), which
// is omitted on the wire.
func (e Encoding) IsEmpty() bool { return e.ID == 0 && e.Schema == nil }

func (e *Encoding) encode(w *codec.Writer) {
	v := uint64(e.ID) << 1
	if e.Schema != nil {
		v |= 1
	}
	w.WriteZInt(v)
	if e.Schema != nil {
		w.WriteZBytes(e.Schema)
	}
}

func decodeEncoding(r *codec.Reader) (Encoding, error) {
	v, err := r.ReadZInt()
	if err != nil {
		return Encoding{}, err
	}
	if v>>1 > 0xffff {
		return Encoding{}, ErrOutOfRange
	}
	e := Encoding{ID: uint16(v >> 1)}
	if v&1 != 0 {
		s, err := r.ReadZBytes()
		if err != nil {
			return Encoding{}, err
		}
		e.Schema = append([]byte{}, s...)
	}
	return e, nil
}

// -----------------------------------------------------------------------------
// Entity ids
// -----------------------------------------------------------------------------

// EntityGlobalID identifies an entity (publisher, queryable, ...) globally: the
// owning node's ZID plus a node-local entity id.
type EntityGlobalID struct {
	ZID ZID
	EID uint32
}

func (g *EntityGlobalID) encode(w *codec.Writer) {
	writeZIDNibble(w, g.ZID, 0)
	w.WriteZInt(uint64(g.EID))
}

func decodeEntityGlobalID(r *codec.Reader) (*EntityGlobalID, error) {
	z, _, err := readZIDNibble(r)
	if err != nil {
		return nil, err
	}
	eid, err := readU32(r)
	if err != nil {
		return nil, err
	}
	return &EntityGlobalID{ZID: z, EID: eid}, nil
}

// SourceInfo identifies the origin of a data message: the source entity and a
// per-source sequence number.
type SourceInfo struct {
	ZID ZID
	EID uint32
	SN  uint32
}

func (s *SourceInfo) encode(w *codec.Writer) {
	writeZIDNibble(w, s.ZID, 0)
	w.WriteZInt(uint64(s.EID))
	w.WriteZInt(uint64(s.SN))
}

func decodeSourceInfo(r *codec.Reader) (*SourceInfo, error) {
	z, _, err := readZIDNibble(r)
	if err != nil {
		return nil, err
	}
	eid, err := readU32(r)
	if err != nil {
		return nil, err
	}
	sn, err := readU32(r)
	if err != nil {
		return nil, err
	}
	return &SourceInfo{ZID: z, EID: eid, SN: sn}, nil
}

// encodeTo serializes fn's output into a fresh slice (for ZBuf extensions).
func encodeTo(fn func(w *codec.Writer)) []byte {
	w := codec.NewWriter()
	fn(w)
	return w.Bytes()
}

// QoSOf returns the QoS of a network message (the zero QoS for messages
// without one, and for anything that isn't a network message).
func QoSOf(m any) QoS {
	switch m := m.(type) {
	case *Push:
		return m.QoS
	case *Request:
		return m.QoS
	case *Response:
		return m.QoS
	case *ResponseFinal:
		return m.QoS
	case *Declare:
		return m.QoS
	case *Interest:
		return m.QoS
	case *OAM:
		return m.QoS
	}
	return 0
}
