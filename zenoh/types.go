package zenoh

import (
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/shabunin/tiny-zenoh-go/protocol"
)

// Re-exported protocol types.
type (
	// ZID is a Zenoh node identifier.
	ZID = protocol.ZID
	// Timestamp is a hybrid-logical-clock timestamp.
	Timestamp = protocol.Timestamp
	// Priority is a message priority class.
	Priority = protocol.Priority
	// QueryTarget selects which queryables a query reaches.
	QueryTarget = protocol.QueryTarget
	// ConsolidationMode selects how replies to a query are consolidated.
	ConsolidationMode = protocol.Consolidation
	// EntityGlobalID identifies an entity (e.g. the queryable that replied).
	EntityGlobalID = protocol.EntityGlobalID
	// SourceInfo identifies the origin of a sample.
	SourceInfo = protocol.SourceInfo
	// WhatAmI is a node's role: router, peer or client.
	WhatAmI = protocol.WhatAmI
)

// Node roles.
const (
	WhatAmIRouter = protocol.Router
	WhatAmIPeer   = protocol.Peer
	WhatAmIClient = protocol.Client
)

// Priorities usable by applications (Control is reserved for Zenoh itself).
const (
	PriorityRealTime        = protocol.PriorityRealTime
	PriorityInteractiveHigh = protocol.PriorityInteractiveHigh
	PriorityInteractiveLow  = protocol.PriorityInteractiveLow
	PriorityDataHigh        = protocol.PriorityDataHigh
	PriorityData            = protocol.PriorityData
	PriorityDataLow         = protocol.PriorityDataLow
	PriorityBackground      = protocol.PriorityBackground
)

// Query targets.
const (
	TargetBestMatching = protocol.TargetBestMatching
	TargetAll          = protocol.TargetAll
	TargetAllComplete  = protocol.TargetAllComplete
)

// Consolidation modes.
const (
	ConsolidationAuto      = protocol.ConsolidationAuto
	ConsolidationNone      = protocol.ConsolidationNone
	ConsolidationMonotonic = protocol.ConsolidationMonotonic
	ConsolidationLatest    = protocol.ConsolidationLatest
)

// CongestionControl selects what happens when a link's transmission queue
// is full (see Config.TxQueue).
type CongestionControl int

// Congestion-control modes.
const (
	// CongestionDrop waits briefly for room, then drops the message (the
	// default for data; best-effort messages are always droppable).
	CongestionDrop CongestionControl = iota
	// CongestionBlock makes the sender wait; a link stuck beyond
	// TxQueue.WaitBeforeClose is closed.
	CongestionBlock
	// CongestionBlockFirst (zenoh unstable) waits in the background, one
	// message per priority at a time; the sender doesn't wait, and messages
	// arriving meanwhile are dropped.
	CongestionBlockFirst
)

// Reliability selects the transport channel.
type Reliability int

// Reliability modes.
const (
	Reliable Reliability = iota // default
	BestEffort
)

// -----------------------------------------------------------------------------
// Encoding
// -----------------------------------------------------------------------------

// Encoding describes a payload's format, e.g. "text/plain;charset=utf-8".
type Encoding struct {
	ID     uint16
	Schema string
}

// customEncodingID marks an encoding whose name is not in the well-known table;
// the whole name is carried in the schema.
const customEncodingID = 0xffff

var encodingNames = [...]string{
	"zenoh/bytes", "zenoh/string", "zenoh/serialized", "application/octet-stream",
	"text/plain", "application/json", "text/json", "application/cdr",
	"application/cbor", "application/yaml", "text/yaml", "text/json5",
	"application/python-serialized-object", "application/protobuf",
	"application/java-serialized-object", "application/openmetrics-text",
	"image/png", "image/jpeg", "image/gif", "image/bmp", "image/webp",
	"application/xml", "application/x-www-form-urlencoded", "text/html",
	"text/xml", "text/css", "text/javascript", "text/markdown", "text/csv",
	"application/sql", "application/coap-payload", "application/json-patch+json",
	"application/json-seq", "application/jsonpath", "application/jwt",
	"application/mp4", "application/soap+xml", "application/yang",
	"audio/aac", "audio/flac", "audio/mp4", "audio/ogg", "audio/vorbis",
	"video/h261", "video/h263", "video/h264", "video/h265", "video/h266",
	"video/mp4", "video/ogg", "video/raw", "video/vp8", "video/vp9",
}

// Well-known encodings.
var (
	EncodingZenohBytes             = Encoding{ID: 0}
	EncodingZenohString            = Encoding{ID: 1}
	EncodingZenohSerialized        = Encoding{ID: 2}
	EncodingApplicationOctetStream = Encoding{ID: 3}
	EncodingTextPlain              = Encoding{ID: 4}
	EncodingApplicationJSON        = Encoding{ID: 5}
	EncodingTextJSON               = Encoding{ID: 6}
	EncodingApplicationCDR         = Encoding{ID: 7}
	EncodingApplicationCBOR        = Encoding{ID: 8}
	EncodingApplicationYAML        = Encoding{ID: 9}
	EncodingApplicationProtobuf    = Encoding{ID: 13}
	EncodingApplicationXML         = Encoding{ID: 21}
	EncodingTextHTML               = Encoding{ID: 23}
	EncodingTextCSV                = Encoding{ID: 28}
	EncodingImagePNG               = Encoding{ID: 16}
	EncodingImageJPEG              = Encoding{ID: 17}
)

// ParseEncoding parses an encoding string the way zenoh-rust does: the part
// before ';' is looked up in the well-known table and the rest is the schema;
// an unknown name yields the custom id with the whole string as schema.
func ParseEncoding(s string) Encoding {
	if s == "" {
		return Encoding{}
	}
	name, schema, _ := strings.Cut(s, ";")
	for i, n := range encodingNames {
		if n == name {
			return Encoding{ID: uint16(i), Schema: schema}
		}
	}
	return Encoding{ID: customEncodingID, Schema: s}
}

// WithSchema returns a copy of e with the given schema.
func (e Encoding) WithSchema(schema string) Encoding { e.Schema = schema; return e }

// String renders the encoding as zenoh-rust does.
func (e Encoding) String() string {
	if e.ID == customEncodingID {
		return e.Schema
	}
	var name string
	if int(e.ID) < len(encodingNames) {
		name = encodingNames[e.ID]
	} else {
		name = strconv.Itoa(int(e.ID))
	}
	if e.Schema != "" {
		return name + ";" + e.Schema
	}
	return name
}

func (e Encoding) wire() protocol.Encoding {
	pe := protocol.Encoding{ID: e.ID}
	if e.Schema != "" {
		pe.Schema = []byte(e.Schema)
	}
	return pe
}

func encodingFromWire(pe protocol.Encoding) Encoding {
	return Encoding{ID: pe.ID, Schema: string(pe.Schema)}
}

// -----------------------------------------------------------------------------
// Sample
// -----------------------------------------------------------------------------

// SampleKind distinguishes a value from a deletion.
type SampleKind int

// Sample kinds.
const (
	SampleKindPut SampleKind = iota
	SampleKindDelete
)

func (k SampleKind) String() string {
	if k == SampleKindDelete {
		return "DELETE"
	}
	return "PUT"
}

// Sample is a value (or deletion) received on a key expression.
type Sample struct {
	KeyExpr    string
	Kind       SampleKind
	Payload    []byte
	Encoding   Encoding
	Timestamp  *Timestamp
	Attachment []byte
	SourceInfo *SourceInfo
	Priority   Priority
	Congestion CongestionControl
	Express    bool
}

func sampleFromBody(key string, body protocol.DataBody, qos protocol.QoS) Sample {
	s := Sample{KeyExpr: key, Priority: qos.Priority(), Express: qos.Express()}
	if qos.Block() {
		s.Congestion = CongestionBlock
	} else if qos.BlockFirst() {
		s.Congestion = CongestionBlockFirst
	}
	switch b := body.(type) {
	case *protocol.Put:
		s.Kind, s.Payload, s.Encoding = SampleKindPut, b.Payload, encodingFromWire(b.Encoding)
		s.Timestamp, s.Attachment, s.SourceInfo = b.Timestamp, b.Attachment, b.SourceInfo
	case *protocol.Del:
		s.Kind, s.Timestamp, s.Attachment, s.SourceInfo = SampleKindDelete, b.Timestamp, b.Attachment, b.SourceInfo
	}
	return s
}

// -----------------------------------------------------------------------------
// Put options (shared by Session.Put/Delete, publishers and query replies)
// -----------------------------------------------------------------------------

type putOptions struct {
	encoding    Encoding
	attachment  []byte
	priority    Priority
	congestion  CongestionControl
	express     bool
	reliability Reliability
	timestamp   *Timestamp
	autoTS      bool
	sourceInfo  *SourceInfo
	destination Locality
}

func defaultPutOptions() putOptions {
	return putOptions{priority: PriorityData, congestion: CongestionDrop}
}

func (o *putOptions) qos() protocol.QoS {
	q := protocol.NewQoS(o.priority, o.congestion == CongestionBlock, o.express)
	if o.congestion == CongestionBlockFirst {
		q = q.WithBlockFirst()
	}
	return q
}

// PutOption customizes a put, delete, publisher or reply.
type PutOption interface{ applyPut(*putOptions) }

type putFn func(*putOptions)

func (f putFn) applyPut(o *putOptions) { f(o) }

// WithEncoding sets the payload encoding.
func WithEncoding(e Encoding) PutOption { return putFn(func(o *putOptions) { o.encoding = e }) }

// WithAttachment attaches opaque user bytes.
func WithAttachment(b []byte) PutOption { return putFn(func(o *putOptions) { o.attachment = b }) }

// WithPriority sets the message priority.
func WithPriority(p Priority) PutOption { return putFn(func(o *putOptions) { o.priority = p }) }

// WithCongestionControl sets the congestion-control mode.
func WithCongestionControl(c CongestionControl) PutOption {
	return putFn(func(o *putOptions) { o.congestion = c })
}

// WithExpress disables batching for the message.
func WithExpress(e bool) PutOption { return putFn(func(o *putOptions) { o.express = e }) }

// WithReliability selects the reliable or best-effort channel.
func WithReliability(r Reliability) PutOption {
	return putFn(func(o *putOptions) { o.reliability = r })
}

// WithTimestamp sets an explicit timestamp.
func WithTimestamp(ts Timestamp) PutOption { return putFn(func(o *putOptions) { o.timestamp = &ts }) }

// WithAutoTimestamp stamps the message with the session's HLC.
func WithAutoTimestamp() PutOption { return putFn(func(o *putOptions) { o.autoTS = true }) }

// WithSourceInfo tags the message with its source entity and sequence number.
func WithSourceInfo(si SourceInfo) PutOption {
	return putFn(func(o *putOptions) { o.sourceInfo = &si })
}

// -----------------------------------------------------------------------------
// Hybrid logical clock
// -----------------------------------------------------------------------------

type hlc struct {
	mu   sync.Mutex
	last uint64
}

// update takes a received timestamp into account: it reports false, leaving
// the clock alone, if t is more than hlcMaxDelta ahead of our time.
func (h *hlc) update(t uint64) bool {
	now := protocol.NTP64FromTime(time.Now())
	if t > now && t-now > hlcMaxDelta {
		return false
	}
	h.mu.Lock()
	if t > h.last {
		h.last = t
	}
	h.mu.Unlock()
	return true
}

func (h *hlc) now() uint64 {
	t := protocol.NTP64FromTime(time.Now())
	h.mu.Lock()
	defer h.mu.Unlock()
	if t <= h.last {
		t = h.last + 1
	}
	h.last = t
	return t
}

// tsAfter reports whether a is strictly later than b, with nil before non-nil
// (zenoh-rust's Option<Timestamp> ordering).
func tsAfter(a, b *Timestamp) bool {
	switch {
	case a == nil:
		return false
	case b == nil:
		return true
	case a.Time != b.Time:
		return a.Time > b.Time
	default:
		return string(a.ID.Bytes()) > string(b.ID.Bytes())
	}
}

// -----------------------------------------------------------------------------
// Unbounded FIFO feeding a channel, so the receive path never blocks on a slow
// consumer.
// -----------------------------------------------------------------------------

type fifo[T any] struct {
	mu     sync.Mutex
	q      []T
	closed bool
	wake   chan struct{}
	out    chan T
}

func newFifo[T any]() *fifo[T] {
	f := &fifo[T]{wake: make(chan struct{}, 1), out: make(chan T)}
	go f.run()
	return f
}

func (f *fifo[T]) push(v T) {
	f.mu.Lock()
	if !f.closed {
		f.q = append(f.q, v)
	}
	f.mu.Unlock()
	f.signal()
}

func (f *fifo[T]) close() {
	f.mu.Lock()
	f.closed = true
	f.mu.Unlock()
	f.signal()
}

func (f *fifo[T]) signal() {
	select {
	case f.wake <- struct{}{}:
	default:
	}
}

func (f *fifo[T]) run() {
	for {
		f.mu.Lock()
		if len(f.q) > 0 {
			v := f.q[0]
			var zero T
			f.q[0] = zero
			f.q = f.q[1:]
			f.mu.Unlock()
			f.out <- v
			continue
		}
		if f.closed {
			f.mu.Unlock()
			close(f.out)
			return
		}
		f.mu.Unlock()
		<-f.wake
	}
}
