package protocol

import (
	"github.com/shabunin/tiny-zenoh-go/internal/codec"
)

// Data sub-messages: the payload-carrying bodies of PUSH, REQUEST and RESPONSE.
// Layouts follow zenoh-rust's codec (commons/zenoh-codec/src/zenoh/*.rs) where
// it differs from the doc comments.

// Data-message extension keys (header byte without Z = ID | M | ENC).
const (
	extSourceInfo    byte = 0x41 // ZBuf, id 1
	extPutAttachment byte = 0x43 // ZBuf, id 3 (PUT)
	extDelAttachment byte = 0x42 // ZBuf, id 2 (DEL)
	extQueryBody     byte = 0x43 // ZBuf, id 3 (QUERY): encoding + raw payload
	extQryAttachment byte = 0x45 // ZBuf, id 5 (QUERY)
)

// Data-message flags.
const (
	flagPutT   = codec.Flag1 // timestamp present
	flagPutE   = codec.Flag2 // encoding present
	flagDelT   = codec.Flag1
	flagQueryC = codec.Flag1 // consolidation present
	flagQueryP = codec.Flag2 // parameters present
	flagReplyC = codec.Flag1
	flagErrE   = codec.Flag2
)

// Consolidation is a query's reply-consolidation mode.
type Consolidation byte

// Consolidation modes.
const (
	ConsolidationAuto      Consolidation = 0 // default, omitted on the wire
	ConsolidationNone      Consolidation = 1
	ConsolidationMonotonic Consolidation = 2
	ConsolidationLatest    Consolidation = 3
)

// DataBody is a PUT or DEL (the body of a PUSH, or of a REPLY).
type DataBody interface {
	Message
	isDataBody()
}

// -----------------------------------------------------------------------------
// PUT
// -----------------------------------------------------------------------------

// Put publishes a value.
type Put struct {
	Timestamp  *Timestamp
	Encoding   Encoding
	SourceInfo *SourceInfo
	Attachment []byte // nil = absent
	Payload    []byte
	// Parts, when set, is sent instead of Payload with the SHM extension;
	// a decoded PUT with that extension has Parts and no Payload.
	Parts   []PayloadPart
	Unknown []Extension // non-mandatory extensions preserved for forwarding
	// ShmBuf, in-process only (never encoded), is the shared-memory buffer
	// (*shm.Buf) Payload lies in: SHM links get a reference to it.
	ShmBuf any
}

func (*Put) msgID() byte { return IDPut }
func (*Put) isDataBody() {}

// Encode writes the PUT sub-message.
func (m *Put) Encode(w *codec.Writer) {
	var exts []Extension
	if m.SourceInfo != nil {
		exts = append(exts, extZBuf(extSourceInfo, encodeTo(m.SourceInfo.encode)))
	}
	if m.Parts != nil {
		exts = append(exts, extFromKey(extPutShm))
	}
	if m.Attachment != nil {
		exts = append(exts, extZBuf(extPutAttachment, m.Attachment))
	}
	exts = append(exts, m.Unknown...)

	var flags byte
	if m.Timestamp != nil {
		flags |= flagPutT
	}
	if !m.Encoding.IsEmpty() {
		flags |= flagPutE
	}
	if len(exts) > 0 {
		flags |= codec.FlagZ
	}
	_ = w.WriteByte(codec.Header(IDPut, flags))
	if m.Timestamp != nil {
		m.Timestamp.encode(w)
	}
	if !m.Encoding.IsEmpty() {
		m.Encoding.encode(w)
	}
	WriteExtensions(w, exts)
	if m.Parts != nil {
		encodeParts(w, m.Parts)
		return
	}
	w.WriteZBytes(m.Payload)
}

func decodePut(r *codec.Reader, hdr byte) (*Put, error) {
	m := &Put{}
	var err error
	shm := false
	if hdr&flagPutT != 0 {
		if m.Timestamp, err = decodeTimestamp(r); err != nil {
			return nil, err
		}
	}
	if hdr&flagPutE != 0 {
		if m.Encoding, err = decodeEncoding(r); err != nil {
			return nil, err
		}
	}
	if codec.HasZ(hdr) {
		var arr [4]Extension
		exts, err := readExts(arr[:0], r, false) // copied below where kept
		if err != nil {
			return nil, err
		}
		for _, e := range exts {
			switch e.key() {
			case extSourceInfo:
				if m.SourceInfo, err = decodeSourceInfo(codec.NewReader(e.Buf)); err != nil {
					return nil, err
				}
			case extPutAttachment:
				m.Attachment = append([]byte(nil), e.Buf...)
			case extPutShm:
				shm = true
			default:
				if err := checkUnknown(e); err != nil {
					return nil, err
				}
				if e.Buf != nil {
					e.Buf = append([]byte(nil), e.Buf...)
				}
				m.Unknown = append(m.Unknown, e)
			}
		}
	}
	if shm {
		m.Parts, err = decodeParts(r)
		return m, err
	}
	pl, err := r.ReadZBytes()
	if err != nil {
		return nil, err
	}
	m.Payload = append([]byte{}, pl...)
	return m, nil
}

// -----------------------------------------------------------------------------
// DEL
// -----------------------------------------------------------------------------

// Del deletes a value.
type Del struct {
	Timestamp  *Timestamp
	SourceInfo *SourceInfo
	Attachment []byte
	Unknown    []Extension
}

func (*Del) msgID() byte { return IDDel }
func (*Del) isDataBody() {}

// Encode writes the DEL sub-message.
func (m *Del) Encode(w *codec.Writer) {
	var exts []Extension
	if m.SourceInfo != nil {
		exts = append(exts, extZBuf(extSourceInfo, encodeTo(m.SourceInfo.encode)))
	}
	if m.Attachment != nil {
		exts = append(exts, extZBuf(extDelAttachment, m.Attachment))
	}
	exts = append(exts, m.Unknown...)

	var flags byte
	if m.Timestamp != nil {
		flags |= flagDelT
	}
	if len(exts) > 0 {
		flags |= codec.FlagZ
	}
	_ = w.WriteByte(codec.Header(IDDel, flags))
	if m.Timestamp != nil {
		m.Timestamp.encode(w)
	}
	WriteExtensions(w, exts)
}

func decodeDel(r *codec.Reader, hdr byte) (*Del, error) {
	m := &Del{}
	var err error
	if hdr&flagDelT != 0 {
		if m.Timestamp, err = decodeTimestamp(r); err != nil {
			return nil, err
		}
	}
	if codec.HasZ(hdr) {
		exts, err := ReadExtensions(r)
		if err != nil {
			return nil, err
		}
		for _, e := range exts {
			switch e.key() {
			case extSourceInfo:
				if m.SourceInfo, err = decodeSourceInfo(codec.NewReader(e.Buf)); err != nil {
					return nil, err
				}
			case extDelAttachment:
				m.Attachment = e.Buf
			default:
				if err := checkUnknown(e); err != nil {
					return nil, err
				}
				m.Unknown = append(m.Unknown, e)
			}
		}
	}
	return m, nil
}

func decodeDataBody(r *codec.Reader) (DataBody, error) {
	hdr, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	switch codec.MsgID(hdr) {
	case IDPut:
		return decodePut(r, hdr)
	case IDDel:
		return decodeDel(r, hdr)
	default:
		return nil, ErrUnknownMessage
	}
}

// -----------------------------------------------------------------------------
// QUERY
// -----------------------------------------------------------------------------

// QueryBody is the optional value sent along with a query.
type QueryBody struct {
	Encoding Encoding
	Payload  []byte
	Parts    []PayloadPart // SHM form, as for Put.Parts
}

// Query is the body of a REQUEST.
type Query struct {
	Consolidation Consolidation
	Parameters    string
	SourceInfo    *SourceInfo
	Body          *QueryBody
	Attachment    []byte
	Unknown       []Extension
}

func (*Query) msgID() byte { return IDQuery }

// Encode writes the QUERY sub-message.
func (m *Query) Encode(w *codec.Writer) {
	var exts []Extension
	if m.SourceInfo != nil {
		exts = append(exts, extZBuf(extSourceInfo, encodeTo(m.SourceInfo.encode)))
	}
	if m.Body != nil {
		// Encoding followed by the raw payload, with no inner length: the payload
		// runs to the end of the extension. With SHM, a unit marker directly
		// precedes it and the payload is in the sliced form.
		if m.Body.Parts != nil {
			exts = append(exts, extFromKey(extQueryShm))
		}
		exts = append(exts, extZBuf(extQueryBody, encodeTo(func(w *codec.Writer) {
			m.Body.Encoding.encode(w)
			if m.Body.Parts != nil {
				encodeParts(w, m.Body.Parts)
			} else {
				w.WriteRaw(m.Body.Payload)
			}
		})))
	}
	if m.Attachment != nil {
		exts = append(exts, extZBuf(extQryAttachment, m.Attachment))
	}
	exts = append(exts, m.Unknown...)

	var flags byte
	if m.Consolidation != ConsolidationAuto {
		flags |= flagQueryC
	}
	if m.Parameters != "" {
		flags |= flagQueryP
	}
	if len(exts) > 0 {
		flags |= codec.FlagZ
	}
	_ = w.WriteByte(codec.Header(IDQuery, flags))
	if m.Consolidation != ConsolidationAuto {
		w.WriteZInt(uint64(m.Consolidation))
	}
	if m.Parameters != "" {
		w.WriteString(m.Parameters)
	}
	WriteExtensions(w, exts)
}

func decodeConsolidation(r *codec.Reader) (Consolidation, error) {
	v, err := r.ReadZInt()
	if err != nil {
		return 0, err
	}
	if v > 3 {
		return ConsolidationAuto, nil // zenoh-rust decodes unknown values as Auto
	}
	return Consolidation(v), nil
}

func decodeQuery(r *codec.Reader, hdr byte) (*Query, error) {
	m := &Query{}
	var err error
	shm := false
	if hdr&flagQueryC != 0 {
		if m.Consolidation, err = decodeConsolidation(r); err != nil {
			return nil, err
		}
	}
	if hdr&flagQueryP != 0 {
		if m.Parameters, err = r.ReadString(); err != nil {
			return nil, err
		}
	}
	if codec.HasZ(hdr) {
		exts, err := ReadExtensions(r)
		if err != nil {
			return nil, err
		}
		for _, e := range exts {
			switch e.key() {
			case extSourceInfo:
				if m.SourceInfo, err = decodeSourceInfo(codec.NewReader(e.Buf)); err != nil {
					return nil, err
				}
			case extQueryShm:
				shm = true
			case extQueryBody:
				br := codec.NewReader(e.Buf)
				enc, err := decodeEncoding(br)
				if err != nil {
					return nil, err
				}
				if shm {
					parts, err := decodeParts(br)
					if err != nil {
						return nil, err
					}
					m.Body = &QueryBody{Encoding: enc, Parts: parts}
					break
				}
				pl, _ := br.ReadRaw(br.Remaining())
				m.Body = &QueryBody{Encoding: enc, Payload: pl}
			case extQryAttachment:
				m.Attachment = e.Buf
			default:
				if err := checkUnknown(e); err != nil {
					return nil, err
				}
				m.Unknown = append(m.Unknown, e)
			}
		}
	}
	return m, nil
}

// -----------------------------------------------------------------------------
// REPLY / ERR
// -----------------------------------------------------------------------------

// ResponseBody is a REPLY or ERR (the body of a RESPONSE).
type ResponseBody interface {
	Message
	isResponseBody()
}

// Reply carries one answer to a query: a full PUT or DEL.
type Reply struct {
	Consolidation Consolidation
	Body          DataBody
	Unknown       []Extension
}

func (*Reply) msgID() byte     { return IDReply }
func (*Reply) isResponseBody() {}

// Encode writes the REPLY sub-message.
func (m *Reply) Encode(w *codec.Writer) {
	var flags byte
	if m.Consolidation != ConsolidationAuto {
		flags |= flagReplyC
	}
	if len(m.Unknown) > 0 {
		flags |= codec.FlagZ
	}
	_ = w.WriteByte(codec.Header(IDReply, flags))
	if m.Consolidation != ConsolidationAuto {
		w.WriteZInt(uint64(m.Consolidation))
	}
	WriteExtensions(w, m.Unknown)
	m.Body.Encode(w)
}

func decodeReply(r *codec.Reader, hdr byte) (*Reply, error) {
	m := &Reply{}
	var err error
	if hdr&flagReplyC != 0 {
		if m.Consolidation, err = decodeConsolidation(r); err != nil {
			return nil, err
		}
	}
	if codec.HasZ(hdr) {
		exts, err := ReadExtensions(r)
		if err != nil {
			return nil, err
		}
		for _, e := range exts {
			if err := checkUnknown(e); err != nil {
				return nil, err
			}
			m.Unknown = append(m.Unknown, e)
		}
	}
	if m.Body, err = decodeDataBody(r); err != nil {
		return nil, err
	}
	return m, nil
}

// Err is an error answer to a query.
type Err struct {
	Encoding   Encoding
	SourceInfo *SourceInfo
	Payload    []byte
	Parts      []PayloadPart // SHM form, as for Put.Parts
	Unknown    []Extension
}

func (*Err) msgID() byte     { return IDErr }
func (*Err) isResponseBody() {}

// Encode writes the ERR sub-message.
func (m *Err) Encode(w *codec.Writer) {
	var exts []Extension
	if m.SourceInfo != nil {
		exts = append(exts, extZBuf(extSourceInfo, encodeTo(m.SourceInfo.encode)))
	}
	if m.Parts != nil {
		exts = append(exts, extFromKey(extPutShm))
	}
	exts = append(exts, m.Unknown...)
	var flags byte
	if !m.Encoding.IsEmpty() {
		flags |= flagErrE
	}
	if len(exts) > 0 {
		flags |= codec.FlagZ
	}
	_ = w.WriteByte(codec.Header(IDErr, flags))
	if !m.Encoding.IsEmpty() {
		m.Encoding.encode(w)
	}
	WriteExtensions(w, exts)
	if m.Parts != nil {
		encodeParts(w, m.Parts)
		return
	}
	w.WriteZBytes(m.Payload)
}

func decodeErr(r *codec.Reader, hdr byte) (*Err, error) {
	m := &Err{}
	var err error
	shm := false
	if hdr&flagErrE != 0 {
		if m.Encoding, err = decodeEncoding(r); err != nil {
			return nil, err
		}
	}
	if codec.HasZ(hdr) {
		exts, err := ReadExtensions(r)
		if err != nil {
			return nil, err
		}
		for _, e := range exts {
			switch e.key() {
			case extSourceInfo:
				if m.SourceInfo, err = decodeSourceInfo(codec.NewReader(e.Buf)); err != nil {
					return nil, err
				}
			case extPutShm:
				shm = true
			default:
				if err := checkUnknown(e); err != nil {
					return nil, err
				}
				m.Unknown = append(m.Unknown, e)
			}
		}
	}
	if shm {
		m.Parts, err = decodeParts(r)
		return m, err
	}
	pl, err := r.ReadZBytes()
	if err != nil {
		return nil, err
	}
	m.Payload = append([]byte{}, pl...)
	return m, nil
}

func decodeResponseBody(r *codec.Reader) (ResponseBody, error) {
	hdr, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	switch codec.MsgID(hdr) {
	case IDReply:
		return decodeReply(r, hdr)
	case IDErr:
		return decodeErr(r, hdr)
	default:
		return nil, ErrUnknownMessage
	}
}
