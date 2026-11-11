package protocol

import (
	"errors"

	"github.com/shabunin/gozenoh/internal/codec"
)

// ErrUnknownMessage is returned by the decoders for an unrecognized ID.
var ErrUnknownMessage = errors.New("protocol: unknown message id")

// CLOSE reason codes (zenoh-protocol transport/close.rs).
const (
	CloseGeneric          byte = 0x00
	CloseUnsupported      byte = 0x01
	CloseInvalid          byte = 0x02
	CloseMaxSessions      byte = 0x03
	CloseMaxLinks         byte = 0x04
	CloseExpired          byte = 0x05
	CloseUnresponsive     byte = 0x06
	CloseConnectionToSelf byte = 0x07
)

// -----------------------------------------------------------------------------
// INIT
// -----------------------------------------------------------------------------

// Init is the session-establishment INIT message (SYN when IsAck is false, ACK
// when true). Resolution and BatchSize are present on the wire only when
// HasParams (the S flag) is set. Cookie is present only on the ACK.
type Init struct {
	IsAck      bool
	Version    byte
	WhatAmI    WhatAmI
	ZID        ZID
	HasParams  bool
	Resolution Resolution
	BatchSize  uint16
	Cookie     []byte // ACK only
	Exts       []Extension
}

func (m *Init) msgID() byte { return IDInit }

// Encode writes the INIT message.
func (m *Init) Encode(w *codec.Writer) {
	var flags byte
	if m.IsAck {
		flags |= flagInitA
	}
	if m.HasParams {
		flags |= flagInitS
	}
	if len(m.Exts) > 0 {
		flags |= codec.FlagZ
	}
	_ = w.WriteByte(codec.Header(IDInit, flags))
	_ = w.WriteByte(m.Version)
	writePackedZID(w, m.ZID, m.WhatAmI)
	if m.HasParams {
		_ = w.WriteByte(byte(m.Resolution))
		_ = w.WriteByte(byte(m.BatchSize))
		_ = w.WriteByte(byte(m.BatchSize >> 8))
	}
	if m.IsAck {
		w.WriteZBytes(m.Cookie)
	}
	WriteExtensions(w, m.Exts)
}

func decodeInit(r *codec.Reader, hdr byte) (*Init, error) {
	m := &Init{
		IsAck:     hdr&flagInitA != 0,
		HasParams: hdr&flagInitS != 0,
	}
	var err error
	if m.Version, err = r.ReadByte(); err != nil {
		return nil, err
	}
	if m.ZID, m.WhatAmI, err = readPackedZID(r); err != nil {
		return nil, err
	}
	if m.HasParams {
		res, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		m.Resolution = Resolution(res)
		lo, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		hi, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		m.BatchSize = uint16(lo) | uint16(hi)<<8
	}
	if m.IsAck {
		cookie, err := r.ReadZBytes()
		if err != nil {
			return nil, err
		}
		m.Cookie = append([]byte(nil), cookie...)
	}
	if codec.HasZ(hdr) {
		if m.Exts, err = ReadExtensions(r); err != nil {
			return nil, err
		}
	}
	return m, nil
}

// -----------------------------------------------------------------------------
// OPEN
// -----------------------------------------------------------------------------

// Open is the session-establishment OPEN message (SYN when IsAck is false, ACK
// when true). LeaseSeconds selects the lease unit (the T flag). Cookie is echoed
// in the SYN only.
type Open struct {
	IsAck        bool
	LeaseSeconds bool // T flag: true => Lease is in seconds, false => milliseconds
	Lease        uint64
	InitialSN    uint64
	Cookie       []byte // SYN only
	Exts         []Extension
}

func (m *Open) msgID() byte { return IDOpen }

// Encode writes the OPEN message.
func (m *Open) Encode(w *codec.Writer) {
	var flags byte
	if m.IsAck {
		flags |= flagOpenA
	}
	if m.LeaseSeconds {
		flags |= flagOpenT
	}
	if len(m.Exts) > 0 {
		flags |= codec.FlagZ
	}
	_ = w.WriteByte(codec.Header(IDOpen, flags))
	w.WriteZInt(m.Lease)
	w.WriteZInt(m.InitialSN)
	if !m.IsAck {
		w.WriteZBytes(m.Cookie)
	}
	WriteExtensions(w, m.Exts)
}

func decodeOpen(r *codec.Reader, hdr byte) (*Open, error) {
	m := &Open{
		IsAck:        hdr&flagOpenA != 0,
		LeaseSeconds: hdr&flagOpenT != 0,
	}
	var err error
	if m.Lease, err = r.ReadZInt(); err != nil {
		return nil, err
	}
	if m.InitialSN, err = r.ReadZInt(); err != nil {
		return nil, err
	}
	if !m.IsAck {
		cookie, err := r.ReadZBytes()
		if err != nil {
			return nil, err
		}
		m.Cookie = append([]byte(nil), cookie...)
	}
	if codec.HasZ(hdr) {
		if m.Exts, err = ReadExtensions(r); err != nil {
			return nil, err
		}
	}
	return m, nil
}

// -----------------------------------------------------------------------------
// CLOSE
// -----------------------------------------------------------------------------

// Close requests teardown. Session reflects the S flag: close the whole session
// (true) or only this link (false). Reason is an implementation-defined code.
type Close struct {
	Session bool
	Reason  byte
	Exts    []Extension
}

func (m *Close) msgID() byte { return IDClose }

// Encode writes the CLOSE message.
func (m *Close) Encode(w *codec.Writer) {
	var flags byte
	if m.Session {
		flags |= flagCloseS
	}
	if len(m.Exts) > 0 {
		flags |= codec.FlagZ
	}
	_ = w.WriteByte(codec.Header(IDClose, flags))
	_ = w.WriteByte(m.Reason)
	WriteExtensions(w, m.Exts)
}

func decodeClose(r *codec.Reader, hdr byte) (*Close, error) {
	m := &Close{Session: hdr&flagCloseS != 0}
	var err error
	if m.Reason, err = r.ReadByte(); err != nil {
		return nil, err
	}
	if codec.HasZ(hdr) {
		if m.Exts, err = ReadExtensions(r); err != nil {
			return nil, err
		}
	}
	return m, nil
}

// -----------------------------------------------------------------------------
// KEEP_ALIVE
// -----------------------------------------------------------------------------

// KeepAlive keeps the session lease fresh. It has no body.
type KeepAlive struct {
	Exts []Extension
}

func (m *KeepAlive) msgID() byte { return IDKeepAlive }

// Encode writes the KEEP_ALIVE message.
func (m *KeepAlive) Encode(w *codec.Writer) {
	var flags byte
	if len(m.Exts) > 0 {
		flags |= codec.FlagZ
	}
	_ = w.WriteByte(codec.Header(IDKeepAlive, flags))
	WriteExtensions(w, m.Exts)
}

func decodeKeepAlive(r *codec.Reader, hdr byte) (*KeepAlive, error) {
	m := &KeepAlive{}
	if codec.HasZ(hdr) {
		var err error
		if m.Exts, err = ReadExtensions(r); err != nil {
			return nil, err
		}
	}
	return m, nil
}

// -----------------------------------------------------------------------------
// FRAME
// -----------------------------------------------------------------------------

// Frame carries one or more serialized network messages on a reliability
// channel. Payload holds the concatenated network-message bytes that fill the
// rest of the batch; a Frame must not span two batches.
type Frame struct {
	Reliable bool
	SN       uint64
	Exts     []Extension
	Payload  []byte
}

func (m *Frame) msgID() byte { return IDFrame }

// Encode writes the FRAME header, sequence number, extensions, then the raw
// payload.
func (m *Frame) Encode(w *codec.Writer) {
	var flags byte
	if m.Reliable {
		flags |= flagFrameR
	}
	if len(m.Exts) > 0 {
		flags |= codec.FlagZ
	}
	_ = w.WriteByte(codec.Header(IDFrame, flags))
	w.WriteZInt(m.SN)
	WriteExtensions(w, m.Exts)
	w.WriteRaw(m.Payload)
}

// IsTransportID reports whether a header byte carries a transport-layer message
// ID (0x00–0x07). Network-message IDs never collide with these, which is how a
// FRAME's run of network messages is terminated within a batch.
func IsTransportID(hdr byte) bool { return codec.MsgID(hdr) <= IDJoin }

// DecodeFrameHeader reads a FRAME's header byte, sequence number and extensions,
// leaving r positioned at the first network message. Use this in a streaming
// receive path: the caller decodes network messages until the next header byte
// is a transport ID (see IsTransportID) or the batch ends.
func DecodeFrameHeader(r *codec.Reader) (*Frame, error) {
	hdr, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	if codec.MsgID(hdr) != IDFrame {
		return nil, ErrUnknownMessage
	}
	m := &Frame{Reliable: hdr&flagFrameR != 0}
	if m.SN, err = r.ReadZInt(); err != nil {
		return nil, err
	}
	if codec.HasZ(hdr) {
		if m.Exts, err = ReadExtensions(r); err != nil {
			return nil, err
		}
	}
	return m, nil
}

// decodeFrame consumes all remaining bytes of r as the payload, so r must be
// scoped to a batch in which the FRAME is the last transport message. The
// streaming receive path uses DecodeFrameHeader instead.
func decodeFrame(r *codec.Reader, hdr byte) (*Frame, error) {
	m := &Frame{Reliable: hdr&flagFrameR != 0}
	var err error
	if m.SN, err = r.ReadZInt(); err != nil {
		return nil, err
	}
	if codec.HasZ(hdr) {
		if m.Exts, err = ReadExtensions(r); err != nil {
			return nil, err
		}
	}
	payload, err := r.ReadRaw(r.Remaining())
	if err != nil {
		return nil, err
	}
	m.Payload = append([]byte(nil), payload...)
	return m, nil
}

// -----------------------------------------------------------------------------
// FRAGMENT
// -----------------------------------------------------------------------------

// Fragment carries a slice of a network message too large for one batch. More
// is true on every fragment except the last.
type Fragment struct {
	Reliable bool
	More     bool
	SN       uint64
	Exts     []Extension
	Payload  []byte
}

func (m *Fragment) msgID() byte { return IDFragment }

// Encode writes the FRAGMENT header, sequence number, extensions, then the raw
// fragment payload.
func (m *Fragment) Encode(w *codec.Writer) {
	var flags byte
	if m.Reliable {
		flags |= flagFragR
	}
	if m.More {
		flags |= flagFragM
	}
	if len(m.Exts) > 0 {
		flags |= codec.FlagZ
	}
	_ = w.WriteByte(codec.Header(IDFragment, flags))
	w.WriteZInt(m.SN)
	WriteExtensions(w, m.Exts)
	w.WriteRaw(m.Payload)
}

func decodeFragment(r *codec.Reader, hdr byte) (*Fragment, error) {
	m := &Fragment{
		Reliable: hdr&flagFragR != 0,
		More:     hdr&flagFragM != 0,
	}
	var err error
	if m.SN, err = r.ReadZInt(); err != nil {
		return nil, err
	}
	if codec.HasZ(hdr) {
		if m.Exts, err = ReadExtensions(r); err != nil {
			return nil, err
		}
	}
	payload, err := r.ReadRaw(r.Remaining())
	if err != nil {
		return nil, err
	}
	m.Payload = append([]byte(nil), payload...)
	return m, nil
}

// -----------------------------------------------------------------------------
// Dispatch
// -----------------------------------------------------------------------------

// DecodeTransport reads one transport message from r (header byte first).
// For FRAME and FRAGMENT, r must be scoped to a single batch because their
// payloads extend to the end of the reader.
func DecodeTransport(r *codec.Reader) (Message, error) {
	hdr, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	switch codec.MsgID(hdr) {
	case IDInit:
		return decodeInit(r, hdr)
	case IDOpen:
		return decodeOpen(r, hdr)
	case IDClose:
		return decodeClose(r, hdr)
	case IDKeepAlive:
		return decodeKeepAlive(r, hdr)
	case IDFrame:
		return decodeFrame(r, hdr)
	case IDFragment:
		return decodeFragment(r, hdr)
	case IDJoin:
		return decodeJoin(r, hdr)
	default:
		return nil, ErrUnknownMessage
	}
}

// -----------------------------------------------------------------------------
// JOIN (multicast transport)
// -----------------------------------------------------------------------------

// MulticastBatchSize is the default batch size of multicast transports; the
// S flag of a JOIN is set when Resolution or BatchSize differ from defaults.
const MulticastBatchSize = 8192

// JOIN extension ids.
const (
	JoinExtQoS   = 0x1 // ZBuf, mandatory: next SNs of the 8 priorities
	JoinExtShm   = 0x2 // ZBuf, mandatory
	JoinExtPatch = 0x7 // Z64
)

// PrioritySN is a pair of next sequence numbers.
type PrioritySN struct{ Reliable, BestEffort uint64 }

// Join announces a node on a multicast group, periodically: its identity,
// parameters, and the next SN of each channel (with QoS, NextSN is zero and
// QoS holds one pair per priority).
type Join struct {
	Version    byte
	WhatAmI    WhatAmI
	ZID        ZID
	Resolution Resolution
	BatchSize  uint16
	// Lease in milliseconds; sent in seconds (T flag) when whole.
	LeaseMillis uint64
	NextSN      PrioritySN
	QoS         *[8]PrioritySN
	Exts        []Extension // others (SHM, patch, unknown)
}

const flagJoinT = 0x20

func (m *Join) msgID() byte { return IDJoin }

// Encode writes the JOIN message.
func (m *Join) Encode(w *codec.Writer) {
	exts := m.Exts
	if m.QoS != nil {
		qw := codec.NewWriter()
		for _, p := range m.QoS {
			qw.WriteZInt(p.Reliable)
			qw.WriteZInt(p.BestEffort)
		}
		exts = append([]Extension{{ID: JoinExtQoS, Enc: ExtZBuf, Mandatory: true, Buf: qw.Bytes()}}, exts...)
	}
	var flags byte
	if m.LeaseMillis%1000 == 0 {
		flags |= flagJoinT
	}
	res := m.Resolution
	if res == 0 {
		res = DefaultResolution
	}
	params := res != DefaultResolution || m.BatchSize != MulticastBatchSize
	if params {
		flags |= flagInitS
	}
	if len(exts) > 0 {
		flags |= codec.FlagZ
	}
	_ = w.WriteByte(codec.Header(IDJoin, flags))
	_ = w.WriteByte(m.Version)
	writePackedZID(w, m.ZID, m.WhatAmI)
	if params {
		_ = w.WriteByte(byte(res))
		_ = w.WriteByte(byte(m.BatchSize))
		_ = w.WriteByte(byte(m.BatchSize >> 8))
	}
	if flags&flagJoinT != 0 {
		w.WriteZInt(m.LeaseMillis / 1000)
	} else {
		w.WriteZInt(m.LeaseMillis)
	}
	w.WriteZInt(m.NextSN.Reliable)
	w.WriteZInt(m.NextSN.BestEffort)
	WriteExtensions(w, exts)
}

func decodeJoin(r *codec.Reader, hdr byte) (*Join, error) {
	m := &Join{Resolution: DefaultResolution, BatchSize: MulticastBatchSize}
	var err error
	if m.Version, err = r.ReadByte(); err != nil {
		return nil, err
	}
	if m.ZID, m.WhatAmI, err = readPackedZID(r); err != nil {
		return nil, err
	}
	if hdr&flagInitS != 0 {
		res, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		m.Resolution = Resolution(res & 0x3f)
		b, err := r.ReadRaw(2)
		if err != nil {
			return nil, err
		}
		m.BatchSize = uint16(b[0]) | uint16(b[1])<<8
	}
	if m.LeaseMillis, err = r.ReadZInt(); err != nil {
		return nil, err
	}
	if hdr&flagJoinT != 0 {
		m.LeaseMillis *= 1000
	}
	if m.NextSN.Reliable, err = r.ReadZInt(); err != nil {
		return nil, err
	}
	if m.NextSN.BestEffort, err = r.ReadZInt(); err != nil {
		return nil, err
	}
	if codec.HasZ(hdr) {
		exts, err := ReadExtensions(r)
		if err != nil {
			return nil, err
		}
		for _, e := range exts {
			if e.ID == JoinExtQoS && e.Enc == ExtZBuf {
				qr := codec.NewReader(e.Buf)
				var q [8]PrioritySN
				for i := range q {
					if q[i].Reliable, err = qr.ReadZInt(); err != nil {
						return nil, err
					}
					if q[i].BestEffort, err = qr.ReadZInt(); err != nil {
						return nil, err
					}
				}
				m.QoS = &q
				continue
			}
			m.Exts = append(m.Exts, e)
		}
	}
	return m, nil
}
