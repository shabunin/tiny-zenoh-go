package protocol

import (
	"github.com/shabunin/gozenoh/internal/codec"
)

// Network messages travel inside FRAMEs. Layouts follow zenoh-rust's codec
// (commons/zenoh-codec/src/network/*.rs, tag 1.10.2).

// Network extension keys (header byte without Z = ID | M | ENC).
const (
	extQoS         byte = 0x21 // Z64, id 1
	extTimestamp   byte = 0x42 // ZBuf, id 2
	extNodeID      byte = 0x33 // Z64, id 3, mandatory
	extResponderID byte = 0x43 // ZBuf, id 3 (RESPONSE only)
	extTarget      byte = 0x34 // Z64, id 4, mandatory (REQUEST)
	extBudget      byte = 0x25 // Z64, id 5 (REQUEST)
	extTimeout     byte = 0x26 // Z64, id 6, milliseconds (REQUEST)

	extQueryableInfo byte = 0x21 // Z64, id 1 (D_QUERYABLE)
	extUndeclWire    byte = 0x5f // ZBuf, id 0xf, mandatory (U_* declarations)
)

const flagDeclareI = codec.Flag1 // DECLARE carries an interest id

// buildCommon returns the QoS / Timestamp extensions in zenoh-rust's write
// order. NodeID is appended by callers where the order requires it.
func buildCommon(q QoS, ts *Timestamp) []Extension {
	var exts []Extension
	if q != 0 && q != DefaultQoS {
		exts = append(exts, extZ64(extQoS, uint64(q.Wire())))
	}
	if ts != nil {
		exts = append(exts, extZBuf(extTimestamp, encodeTo(ts.encode)))
	}
	return exts
}

func nodeExt(exts []Extension, node uint16) []Extension {
	if node != 0 {
		exts = append(exts, extZ64(extNodeID, uint64(node)))
	}
	return exts
}

// takeCommon applies a QoS / Timestamp / NodeID extension if e is one; it
// reports whether e was consumed.
func takeCommon(e Extension, q *QoS, ts **Timestamp, node *uint16) (bool, error) {
	switch e.key() {
	case extQoS:
		*q = QoSFromWire(e.U64)
		return true, nil
	case extTimestamp:
		t, err := decodeTimestamp(codec.NewReader(e.Buf))
		if err != nil {
			return true, err
		}
		*ts = t
		return true, nil
	case extNodeID:
		if node == nil {
			return false, nil
		}
		*node = uint16(e.U64)
		return true, nil
	}
	return false, nil
}

// readExtsIf reads an extension chain when the header's Z flag is set.
func readExtsIf(r *codec.Reader, hdr byte) ([]Extension, error) {
	if !codec.HasZ(hdr) {
		return nil, nil
	}
	return ReadExtensions(r)
}

func zflag(exts []Extension) byte {
	if len(exts) > 0 {
		return codec.FlagZ
	}
	return 0
}

// -----------------------------------------------------------------------------
// PUSH
// -----------------------------------------------------------------------------

// Push delivers a PUT or DEL to subscribers of WireExpr.
type Push struct {
	WireExpr  WireExpr
	QoS       QoS
	Timestamp *Timestamp
	NodeID    uint16
	Body      DataBody
}

func (*Push) msgID() byte { return IDPush }

// Encode writes the PUSH message.
func (m *Push) Encode(w *codec.Writer) {
	exts := nodeExt(buildCommon(m.QoS, m.Timestamp), m.NodeID)
	_ = w.WriteByte(codec.Header(IDPush, m.WireExpr.Flags()|zflag(exts)))
	m.WireExpr.Encode(w)
	WriteExtensions(w, exts)
	m.Body.Encode(w)
}

func decodePush(r *codec.Reader, hdr byte) (*Push, error) {
	m := &Push{}
	var err error
	if m.WireExpr, err = DecodeWireExpr(r, hdr&flagWireN != 0, hdr&flagWireM != 0); err != nil {
		return nil, err
	}
	exts, err := readExtsIf(r, hdr)
	if err != nil {
		return nil, err
	}
	for _, e := range exts {
		ok, err := takeCommon(e, &m.QoS, &m.Timestamp, &m.NodeID)
		if err != nil {
			return nil, err
		}
		if !ok {
			if err := checkUnknown(e); err != nil {
				return nil, err
			}
		}
	}
	if m.Body, err = decodeDataBody(r); err != nil {
		return nil, err
	}
	return m, nil
}

// -----------------------------------------------------------------------------
// REQUEST
// -----------------------------------------------------------------------------

// QueryTarget selects which queryables a request reaches.
type QueryTarget byte

// Query targets.
const (
	TargetBestMatching QueryTarget = 0 // default, omitted
	TargetAll          QueryTarget = 1
	TargetAllComplete  QueryTarget = 2
)

// Request carries a QUERY to queryables matching WireExpr.
type Request struct {
	ID        uint32
	WireExpr  WireExpr
	QoS       QoS
	Timestamp *Timestamp
	NodeID    uint16
	Target    QueryTarget
	Budget    uint32 // max responses, 0 = unlimited
	TimeoutMS uint64 // 0 = unset
	Query     *Query
}

func (*Request) msgID() byte { return IDRequest }

// Encode writes the REQUEST message.
func (m *Request) Encode(w *codec.Writer) {
	exts := buildCommon(m.QoS, m.Timestamp)
	if m.Target != TargetBestMatching {
		exts = append(exts, extZ64(extTarget, uint64(m.Target)))
	}
	if m.Budget != 0 {
		exts = append(exts, extZ64(extBudget, uint64(m.Budget)))
	}
	if m.TimeoutMS != 0 {
		exts = append(exts, extZ64(extTimeout, m.TimeoutMS))
	}
	exts = nodeExt(exts, m.NodeID)
	_ = w.WriteByte(codec.Header(IDRequest, m.WireExpr.Flags()|zflag(exts)))
	w.WriteZInt(uint64(m.ID))
	m.WireExpr.Encode(w)
	WriteExtensions(w, exts)
	m.Query.Encode(w)
}

func decodeRequest(r *codec.Reader, hdr byte) (*Request, error) {
	m := &Request{}
	var err error
	if m.ID, err = readU32(r); err != nil {
		return nil, err
	}
	if m.WireExpr, err = DecodeWireExpr(r, hdr&flagWireN != 0, hdr&flagWireM != 0); err != nil {
		return nil, err
	}
	exts, err := readExtsIf(r, hdr)
	if err != nil {
		return nil, err
	}
	for _, e := range exts {
		ok, err := takeCommon(e, &m.QoS, &m.Timestamp, &m.NodeID)
		if err != nil {
			return nil, err
		}
		if ok {
			continue
		}
		switch e.key() {
		case extTarget:
			if e.U64 > 2 {
				return nil, ErrOutOfRange
			}
			m.Target = QueryTarget(e.U64)
		case extBudget:
			m.Budget = uint32(e.U64)
		case extTimeout:
			m.TimeoutMS = e.U64
		default:
			if err := checkUnknown(e); err != nil {
				return nil, err
			}
		}
	}
	hdr2, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	if codec.MsgID(hdr2) != IDQuery {
		return nil, ErrUnknownMessage
	}
	if m.Query, err = decodeQuery(r, hdr2); err != nil {
		return nil, err
	}
	return m, nil
}

// -----------------------------------------------------------------------------
// RESPONSE / RESPONSE_FINAL
// -----------------------------------------------------------------------------

// Response carries one REPLY or ERR for request RequestID.
type Response struct {
	RequestID uint32
	WireExpr  WireExpr
	QoS       QoS
	Timestamp *Timestamp
	Responder *EntityGlobalID
	Body      ResponseBody
}

func (*Response) msgID() byte { return IDResponse }

// Encode writes the RESPONSE message.
func (m *Response) Encode(w *codec.Writer) {
	exts := buildCommon(m.QoS, m.Timestamp)
	if m.Responder != nil {
		exts = append(exts, extZBuf(extResponderID, encodeTo(m.Responder.encode)))
	}
	_ = w.WriteByte(codec.Header(IDResponse, m.WireExpr.Flags()|zflag(exts)))
	w.WriteZInt(uint64(m.RequestID))
	m.WireExpr.Encode(w)
	WriteExtensions(w, exts)
	m.Body.Encode(w)
}

func decodeResponse(r *codec.Reader, hdr byte) (*Response, error) {
	m := &Response{}
	var err error
	if m.RequestID, err = readU32(r); err != nil {
		return nil, err
	}
	if m.WireExpr, err = DecodeWireExpr(r, hdr&flagWireN != 0, hdr&flagWireM != 0); err != nil {
		return nil, err
	}
	exts, err := readExtsIf(r, hdr)
	if err != nil {
		return nil, err
	}
	for _, e := range exts {
		ok, err := takeCommon(e, &m.QoS, &m.Timestamp, nil)
		if err != nil {
			return nil, err
		}
		if ok {
			continue
		}
		if e.key() == extResponderID {
			if m.Responder, err = decodeEntityGlobalID(codec.NewReader(e.Buf)); err != nil {
				return nil, err
			}
		} else if err := checkUnknown(e); err != nil {
			return nil, err
		}
	}
	if m.Body, err = decodeResponseBody(r); err != nil {
		return nil, err
	}
	return m, nil
}

// ResponseFinal ends the stream of responses for RequestID.
type ResponseFinal struct {
	RequestID uint32
	QoS       QoS
	Timestamp *Timestamp
}

func (*ResponseFinal) msgID() byte { return IDResponseFinal }

// Encode writes the RESPONSE_FINAL message.
func (m *ResponseFinal) Encode(w *codec.Writer) {
	exts := buildCommon(m.QoS, m.Timestamp)
	_ = w.WriteByte(codec.Header(IDResponseFinal, zflag(exts)))
	w.WriteZInt(uint64(m.RequestID))
	WriteExtensions(w, exts)
}

func decodeResponseFinal(r *codec.Reader, hdr byte) (*ResponseFinal, error) {
	m := &ResponseFinal{}
	var err error
	if m.RequestID, err = readU32(r); err != nil {
		return nil, err
	}
	exts, err := readExtsIf(r, hdr)
	if err != nil {
		return nil, err
	}
	for _, e := range exts {
		ok, err := takeCommon(e, &m.QoS, &m.Timestamp, nil)
		if err != nil {
			return nil, err
		}
		if !ok {
			if err := checkUnknown(e); err != nil {
				return nil, err
			}
		}
	}
	return m, nil
}

// -----------------------------------------------------------------------------
// DECLARE
// -----------------------------------------------------------------------------

// Declaration sub-message IDs.
const (
	idDKeyExpr    byte = 0x00
	idUKeyExpr    byte = 0x01
	idDSubscriber byte = 0x02
	idUSubscriber byte = 0x03
	idDQueryable  byte = 0x04
	idUQueryable  byte = 0x05
	idDToken      byte = 0x06
	idUToken      byte = 0x07
	idDFinal      byte = 0x1a
)

// Declaration is the body of a DECLARE message.
type Declaration interface {
	encodeDecl(w *codec.Writer)
}

// Declare carries one declaration. When HasInterestID is set it answers the
// INTEREST with that id (mode Current or CurrentFuture).
type Declare struct {
	HasInterestID bool
	InterestID    uint32
	QoS           QoS
	Timestamp     *Timestamp
	NodeID        uint16
	Body          Declaration
}

func (*Declare) msgID() byte { return IDDeclare }

// Encode writes the DECLARE message.
func (m *Declare) Encode(w *codec.Writer) {
	exts := nodeExt(buildCommon(m.QoS, m.Timestamp), m.NodeID)
	var flags byte
	if m.HasInterestID {
		flags |= flagDeclareI
	}
	_ = w.WriteByte(codec.Header(IDDeclare, flags|zflag(exts)))
	if m.HasInterestID {
		w.WriteZInt(uint64(m.InterestID))
	}
	WriteExtensions(w, exts)
	m.Body.encodeDecl(w)
}

func decodeDeclare(r *codec.Reader, hdr byte) (*Declare, error) {
	m := &Declare{HasInterestID: hdr&flagDeclareI != 0}
	var err error
	if m.HasInterestID {
		if m.InterestID, err = readU32(r); err != nil {
			return nil, err
		}
	}
	exts, err := readExtsIf(r, hdr)
	if err != nil {
		return nil, err
	}
	for _, e := range exts {
		ok, err := takeCommon(e, &m.QoS, &m.Timestamp, &m.NodeID)
		if err != nil {
			return nil, err
		}
		if !ok {
			if err := checkUnknown(e); err != nil {
				return nil, err
			}
		}
	}
	if m.Body, err = decodeDeclaration(r); err != nil {
		return nil, err
	}
	return m, nil
}

// DeclareKeyExpr maps a numeric id (in the sender's mapping space) to a key
// expression, so later messages can refer to it compactly.
type DeclareKeyExpr struct {
	ID       uint16
	WireExpr WireExpr
}

// UndeclareKeyExpr drops a key-expression mapping.
type UndeclareKeyExpr struct{ ID uint16 }

// DeclareSubscriber declares interest in data matching WireExpr.
type DeclareSubscriber struct {
	ID       uint32
	WireExpr WireExpr
}

// UndeclareSubscriber removes subscriber ID. WireExpr is optional context.
type UndeclareSubscriber struct {
	ID       uint32
	WireExpr WireExpr
}

// DeclareQueryable declares a queryable on WireExpr.
type DeclareQueryable struct {
	ID       uint32
	WireExpr WireExpr
	Complete bool   // answers every key it matches
	Distance uint16 // hops from the declaring node
}

// UndeclareQueryable removes queryable ID.
type UndeclareQueryable struct {
	ID       uint32
	WireExpr WireExpr
}

// DeclareToken declares a liveliness token.
type DeclareToken struct {
	ID       uint32
	WireExpr WireExpr
}

// UndeclareToken removes liveliness token ID.
type UndeclareToken struct {
	ID       uint32
	WireExpr WireExpr
}

// DeclareFinal ends the "current" part of an interest reply.
type DeclareFinal struct{}

func declHeaderWE(id byte, we WireExpr, withM bool) byte {
	f := we.Flags()
	if !withM {
		f &^= flagWireM
	}
	return codec.Header(id, f)
}

func (d *DeclareKeyExpr) encodeDecl(w *codec.Writer) {
	_ = w.WriteByte(declHeaderWE(idDKeyExpr, d.WireExpr, false))
	w.WriteZInt(uint64(d.ID))
	d.WireExpr.Encode(w)
}

func (d *UndeclareKeyExpr) encodeDecl(w *codec.Writer) {
	_ = w.WriteByte(idUKeyExpr)
	w.WriteZInt(uint64(d.ID))
}

func (d *DeclareSubscriber) encodeDecl(w *codec.Writer) {
	_ = w.WriteByte(declHeaderWE(idDSubscriber, d.WireExpr, true))
	w.WriteZInt(uint64(d.ID))
	d.WireExpr.Encode(w)
}

func (d *UndeclareSubscriber) encodeDecl(w *codec.Writer) {
	if isNullWE(d.WireExpr) {
		_ = w.WriteByte(idUSubscriber)
		w.WriteZInt(uint64(d.ID))
		return
	}
	_ = w.WriteByte(idUSubscriber | codec.FlagZ)
	w.WriteZInt(uint64(d.ID))
	WriteExtensions(w, []Extension{extZBuf(extUndeclWire, encodeUndeclWE(d.WireExpr))})
}

func (d *DeclareQueryable) encodeDecl(w *codec.Writer) {
	var exts []Extension
	if d.Complete || d.Distance != 0 {
		v := uint64(d.Distance) << 8
		if d.Complete {
			v |= 1
		}
		exts = append(exts, extZ64(extQueryableInfo, v))
	}
	_ = w.WriteByte(declHeaderWE(idDQueryable, d.WireExpr, true) | zflag(exts))
	w.WriteZInt(uint64(d.ID))
	d.WireExpr.Encode(w)
	WriteExtensions(w, exts)
}

func (d *UndeclareQueryable) encodeDecl(w *codec.Writer) {
	_ = w.WriteByte(idUQueryable | codec.FlagZ) // always carries the WireExpr ext
	w.WriteZInt(uint64(d.ID))
	WriteExtensions(w, []Extension{extZBuf(extUndeclWire, encodeUndeclWE(d.WireExpr))})
}

func (d *DeclareToken) encodeDecl(w *codec.Writer) {
	_ = w.WriteByte(declHeaderWE(idDToken, d.WireExpr, true))
	w.WriteZInt(uint64(d.ID))
	d.WireExpr.Encode(w)
}

func (d *UndeclareToken) encodeDecl(w *codec.Writer) {
	_ = w.WriteByte(idUToken | codec.FlagZ)
	w.WriteZInt(uint64(d.ID))
	WriteExtensions(w, []Extension{extZBuf(extUndeclWire, encodeUndeclWE(d.WireExpr))})
}

func (d *DeclareFinal) encodeDecl(w *codec.Writer) { _ = w.WriteByte(idDFinal) }

func isNullWE(we WireExpr) bool { return we.Scope == 0 && we.Suffix == "" && !we.Mapping }

// encodeUndeclWE builds the body of the undeclare WireExpr extension: a flags
// byte (bit0 N, bit1 M), the scope, then the suffix as raw bytes running to the
// end of the extension (no length prefix — the codec differs from the docs).
func encodeUndeclWE(we WireExpr) []byte {
	return encodeTo(func(w *codec.Writer) {
		var f byte
		if we.HasSuffix() {
			f |= 0x01
		}
		if we.Mapping {
			f |= 0x02
		}
		_ = w.WriteByte(f)
		w.WriteZInt(uint64(we.Scope))
		if we.HasSuffix() {
			w.WriteRaw([]byte(we.Suffix))
		}
	})
}

func decodeUndeclWE(b []byte) (WireExpr, error) {
	r := codec.NewReader(b)
	f, err := r.ReadByte()
	if err != nil {
		return WireExpr{}, err
	}
	scope, err := readU16(r)
	if err != nil {
		return WireExpr{}, err
	}
	we := WireExpr{Scope: scope, Mapping: f&0x02 != 0}
	if f&0x01 != 0 {
		s, _ := r.ReadRaw(r.Remaining())
		we.Suffix = string(s)
	}
	return we, nil
}

// skipExts consumes an extension chain without interpreting it (zenoh-rust's
// skip_all, used by several declarations).
func skipExts(r *codec.Reader, hdr byte) error {
	_, err := readExtsIf(r, hdr)
	return err
}

// undeclWE reads the extensions of an U_* declaration, returning the WireExpr
// extension if present.
func undeclWE(r *codec.Reader, hdr byte) (WireExpr, error) {
	exts, err := readExtsIf(r, hdr)
	if err != nil {
		return WireExpr{}, err
	}
	var we WireExpr
	for _, e := range exts {
		if e.key() == extUndeclWire {
			if we, err = decodeUndeclWE(e.Buf); err != nil {
				return WireExpr{}, err
			}
		} else if err := checkUnknown(e); err != nil {
			return WireExpr{}, err
		}
	}
	return we, nil
}

func decodeDeclaration(r *codec.Reader) (Declaration, error) {
	hdr, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	named, mapping := hdr&flagWireN != 0, hdr&flagWireM != 0
	switch codec.MsgID(hdr) {
	case idDKeyExpr:
		id, err := readU16(r)
		if err != nil {
			return nil, err
		}
		we, err := DecodeWireExpr(r, named, false)
		if err != nil {
			return nil, err
		}
		return &DeclareKeyExpr{ID: id, WireExpr: we}, skipExts(r, hdr)
	case idUKeyExpr:
		id, err := readU16(r)
		if err != nil {
			return nil, err
		}
		return &UndeclareKeyExpr{ID: id}, skipExts(r, hdr)
	case idDSubscriber, idDToken:
		id, err := readU32(r)
		if err != nil {
			return nil, err
		}
		we, err := DecodeWireExpr(r, named, mapping)
		if err != nil {
			return nil, err
		}
		if err := skipExts(r, hdr); err != nil {
			return nil, err
		}
		if codec.MsgID(hdr) == idDToken {
			return &DeclareToken{ID: id, WireExpr: we}, nil
		}
		return &DeclareSubscriber{ID: id, WireExpr: we}, nil
	case idDQueryable:
		id, err := readU32(r)
		if err != nil {
			return nil, err
		}
		we, err := DecodeWireExpr(r, named, mapping)
		if err != nil {
			return nil, err
		}
		d := &DeclareQueryable{ID: id, WireExpr: we}
		exts, err := readExtsIf(r, hdr)
		if err != nil {
			return nil, err
		}
		for _, e := range exts {
			if e.key() == extQueryableInfo {
				d.Complete = e.U64&1 != 0
				d.Distance = uint16(e.U64 >> 8)
			} else if err := checkUnknown(e); err != nil {
				return nil, err
			}
		}
		return d, nil
	case idUSubscriber, idUQueryable, idUToken:
		id, err := readU32(r)
		if err != nil {
			return nil, err
		}
		we, err := undeclWE(r, hdr)
		if err != nil {
			return nil, err
		}
		switch codec.MsgID(hdr) {
		case idUSubscriber:
			return &UndeclareSubscriber{ID: id, WireExpr: we}, nil
		case idUQueryable:
			return &UndeclareQueryable{ID: id, WireExpr: we}, nil
		default:
			return &UndeclareToken{ID: id, WireExpr: we}, nil
		}
	case idDFinal:
		return &DeclareFinal{}, skipExts(r, hdr)
	default:
		return nil, ErrUnknownMessage
	}
}

// -----------------------------------------------------------------------------
// INTEREST
// -----------------------------------------------------------------------------

// InterestMode selects whether an interest wants current state, future
// changes, both, or is being cancelled (Final).
type InterestMode byte

// Interest modes.
const (
	InterestFinal         InterestMode = 0
	InterestCurrent       InterestMode = 1
	InterestFuture        InterestMode = 2
	InterestCurrentFuture InterestMode = 3
)

// InterestOptions selects which declaration kinds an interest covers.
type InterestOptions byte

// Interest options. R/N/M (0x10/0x20/0x40) are derived from WireExpr.
const (
	InterestKeyExprs    InterestOptions = 0x01
	InterestSubscribers InterestOptions = 0x02
	InterestQueryables  InterestOptions = 0x04
	InterestTokens      InterestOptions = 0x08
	InterestAggregate   InterestOptions = 0x80
)

const (
	interestOptR = 0x10
	interestOptN = 0x20
	interestOptM = 0x40
)

// Interest asks the peer to declare (now and/or in the future) the entities
// matching WireExpr (all entities when WireExpr is nil).
type Interest struct {
	ID        uint32
	Mode      InterestMode
	Options   InterestOptions
	WireExpr  *WireExpr
	QoS       QoS
	Timestamp *Timestamp
	NodeID    uint16
}

func (*Interest) msgID() byte { return IDInterest }

// Encode writes the INTEREST message.
func (m *Interest) Encode(w *codec.Writer) {
	exts := nodeExt(buildCommon(m.QoS, m.Timestamp), m.NodeID)
	_ = w.WriteByte(codec.Header(IDInterest, byte(m.Mode&0x03)<<5|zflag(exts)))
	w.WriteZInt(uint64(m.ID))
	if m.Mode != InterestFinal {
		opts := byte(m.Options) & 0x8f
		if m.WireExpr != nil {
			opts |= interestOptR
			if m.WireExpr.HasSuffix() {
				opts |= interestOptN
			}
			if m.WireExpr.Mapping {
				opts |= interestOptM
			}
		}
		_ = w.WriteByte(opts)
		if m.WireExpr != nil {
			m.WireExpr.Encode(w)
		}
	}
	WriteExtensions(w, exts)
}

func decodeInterest(r *codec.Reader, hdr byte) (*Interest, error) {
	m := &Interest{Mode: InterestMode((hdr >> 5) & 0x03)}
	var err error
	if m.ID, err = readU32(r); err != nil {
		return nil, err
	}
	if m.Mode != InterestFinal {
		opts, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		m.Options = InterestOptions(opts & 0x8f)
		if opts&interestOptR != 0 {
			we, err := DecodeWireExpr(r, opts&interestOptN != 0, opts&interestOptM != 0)
			if err != nil {
				return nil, err
			}
			m.WireExpr = &we
		}
	}
	exts, err := readExtsIf(r, hdr)
	if err != nil {
		return nil, err
	}
	for _, e := range exts {
		ok, err := takeCommon(e, &m.QoS, &m.Timestamp, &m.NodeID)
		if err != nil {
			return nil, err
		}
		if !ok {
			if err := checkUnknown(e); err != nil {
				return nil, err
			}
		}
	}
	return m, nil
}

// -----------------------------------------------------------------------------
// OAM (network)
// -----------------------------------------------------------------------------

// OAM is an operations-and-management message (e.g. router link-state). Its
// body is a single Unit / Z64 / ZBuf value selected by Enc.
type OAM struct {
	ID        uint16
	Enc       ExtEnc
	U64       uint64
	Buf       []byte
	QoS       QoS
	Timestamp *Timestamp
}

func (*OAM) msgID() byte { return IDOAMNetwork }

// Encode writes the network OAM message.
func (m *OAM) Encode(w *codec.Writer) {
	exts := buildCommon(m.QoS, m.Timestamp)
	_ = w.WriteByte(codec.Header(IDOAMNetwork, byte(m.Enc&0x03)<<5|zflag(exts)))
	w.WriteZInt(uint64(m.ID))
	WriteExtensions(w, exts)
	switch m.Enc {
	case ExtZ64:
		w.WriteZInt(m.U64)
	case ExtZBuf:
		w.WriteZBytes(m.Buf)
	}
}

func decodeOAM(r *codec.Reader, hdr byte) (*OAM, error) {
	m := &OAM{Enc: ExtEnc((hdr >> 5) & 0x03)}
	var err error
	if m.ID, err = readU16(r); err != nil {
		return nil, err
	}
	exts, err := readExtsIf(r, hdr)
	if err != nil {
		return nil, err
	}
	for _, e := range exts {
		ok, err := takeCommon(e, &m.QoS, &m.Timestamp, nil)
		if err != nil {
			return nil, err
		}
		if !ok {
			if err := checkUnknown(e); err != nil {
				return nil, err
			}
		}
	}
	switch m.Enc {
	case ExtZ64:
		m.U64, err = r.ReadZInt()
	case ExtZBuf:
		var b []byte
		b, err = r.ReadZBytes()
		m.Buf = append([]byte{}, b...)
	case ExtUnit:
	default:
		err = ErrReservedExtEnc
	}
	if err != nil {
		return nil, err
	}
	return m, nil
}

// -----------------------------------------------------------------------------
// Dispatch
// -----------------------------------------------------------------------------

// IsNetworkID reports whether a header byte carries a network-message ID.
func IsNetworkID(hdr byte) bool {
	id := codec.MsgID(hdr)
	return id >= IDInterest && id <= IDOAMNetwork
}

// DecodeNetwork reads one network message from r (header byte first).
func DecodeNetwork(r *codec.Reader) (Message, error) {
	hdr, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	switch codec.MsgID(hdr) {
	case IDPush:
		return decodePush(r, hdr)
	case IDRequest:
		return decodeRequest(r, hdr)
	case IDResponse:
		return decodeResponse(r, hdr)
	case IDResponseFinal:
		return decodeResponseFinal(r, hdr)
	case IDDeclare:
		return decodeDeclare(r, hdr)
	case IDInterest:
		return decodeInterest(r, hdr)
	case IDOAMNetwork:
		return decodeOAM(r, hdr)
	default:
		return nil, ErrUnknownMessage
	}
}
