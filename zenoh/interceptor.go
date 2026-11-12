package zenoh

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/shabunin/tiny-zenoh-go/keyexpr"
	"github.com/shabunin/tiny-zenoh-go/protocol"
	"github.com/shabunin/tiny-zenoh-go/transport"
)

// Interceptors (zenoh-rust zenoh/src/net/routing/interceptor: downsampling.rs,
// low_pass.rs, qos_overwrite.rs; config sections "downsampling",
// "low_pass_filter" and "qos/network") act on the data messages crossing a
// face, in either direction (ingress: received from the node; egress: sent
// to it):
//
//   - downsampling lets at most freq messages per second through per rule;
//   - the low-pass filter drops messages whose payload plus attachment is
//     larger than a limit;
//   - QoS overwrite changes the priority, congestion control or express flag
//     of matching messages.
//
// Each entry applies to the faces it matches (by local network interface,
// link protocol and, for QoS overwrite, ZID). As in zenoh-rust they run in
// the order downsampling, access control, QoS overwrite, low-pass filter,
// on unicast faces only.

// DataMessage is a kind of data message an interceptor acts on.
type DataMessage string

// Data message kinds.
const (
	MessagePut    DataMessage = "put"
	MessageDelete DataMessage = "delete"
	MessageQuery  DataMessage = "query"
	MessageReply  DataMessage = "reply"
)

// DownsamplingConfig limits the rate of messages on key expressions
// (zenoh-rust "downsampling" entries).
type DownsamplingConfig struct {
	ID            string             `json:"id,omitempty"`
	Interfaces    []string           `json:"interfaces,omitempty"`     // local network interfaces (nil: any)
	LinkProtocols []string           `json:"link_protocols,omitempty"` // "tcp", "udp", "tls", "quic", ... (nil: any)
	Messages      []DataMessage      `json:"messages"`
	Rules         []DownsamplingRule `json:"rules"`
	Flows         []ACLFlow          `json:"flows,omitempty"` // nil: both
}

// DownsamplingRule lets at most Freq messages per second through on
// KeyExpr (0: none). The first rule whose key expression intersects the
// message's key applies; messages no rule matches pass.
type DownsamplingRule struct {
	KeyExpr string  `json:"key_expr"`
	Freq    float64 `json:"freq"`
}

// LowPassFilterConfig drops messages larger than SizeLimit bytes (payload
// plus attachment) on key expressions (zenoh-rust "low_pass_filter"
// entries). When several apply to a message, the smallest limit wins.
type LowPassFilterConfig struct {
	ID            string        `json:"id,omitempty"`
	Interfaces    []string      `json:"interfaces,omitempty"`
	LinkProtocols []string      `json:"link_protocols,omitempty"`
	Flows         []ACLFlow     `json:"flows,omitempty"`
	Messages      []DataMessage `json:"messages"`
	KeyExprs      []string      `json:"key_exprs"` // must include the message's key
	SizeLimit     int           `json:"size_limit"`
}

// QoSOverwriteConfig changes the QoS of messages (zenoh-rust "qos/network"
// entries). Messages are put, delete or query.
type QoSOverwriteConfig struct {
	ID            string        `json:"id,omitempty"`
	ZIDs          []string      `json:"zids,omitempty"`
	Interfaces    []string      `json:"interfaces,omitempty"`
	LinkProtocols []string      `json:"link_protocols,omitempty"`
	Messages      []DataMessage `json:"messages"`
	KeyExprs      []string      `json:"key_exprs,omitempty"` // nil: every key
	Overwrite     QoSOverwrites `json:"overwrite"`
	Flows         []ACLFlow     `json:"flows,omitempty"`
	// QoS, if set, restricts the overwrite to messages with that QoS.
	QoS *QoSFilter `json:"qos,omitempty"`
	// PayloadSize, if set, restricts it to payloads in an inclusive range
	// "min..max" (either bound may be left out).
	PayloadSize string `json:"payload_size,omitempty"`
}

// QoSOverwrites is what a QoS overwrite sets; nil/empty fields are kept.
type QoSOverwrites struct {
	CongestionControl string          `json:"congestion_control,omitempty"` // "drop" or "block"
	Priority          *PriorityUpdate `json:"priority,omitempty"`
	Express           *bool           `json:"express,omitempty"`
}

// PriorityUpdate sets a priority, or with Priority 0, moves it by
// Increment (positive: less urgent, toward background). In JSON it is a
// priority name ("real_time", "interactive_high", "interactive_low",
// "data_high", "data", "data_low", "background") or an increment -7..7.
type PriorityUpdate struct {
	Priority  Priority
	Increment int
}

// UnmarshalJSON decodes a priority name or an increment.
func (p *PriorityUpdate) UnmarshalJSON(b []byte) error {
	s := strings.TrimSpace(string(b))
	if strings.HasPrefix(s, `"`) {
		name, err := strconv.Unquote(s)
		if err != nil {
			return err
		}
		pr, ok := priorityByName(name)
		if !ok {
			return fmt.Errorf("unknown priority %q", name)
		}
		*p = PriorityUpdate{Priority: pr}
		return nil
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return fmt.Errorf("priority: want a name or an increment, got %s", s)
	}
	if n < -7 || n > 7 {
		return fmt.Errorf("invalid priority increment %d (-7..7)", n)
	}
	*p = PriorityUpdate{Increment: n}
	return nil
}

// MarshalJSON encodes a priority name or an increment.
func (p PriorityUpdate) MarshalJSON() ([]byte, error) {
	if p.Priority != 0 {
		return []byte(strconv.Quote(priorityNames[p.Priority])), nil
	}
	return []byte(strconv.Itoa(p.Increment)), nil
}

// QoSFilter selects messages by QoS; empty fields match anything.
type QoSFilter struct {
	CongestionControl string `json:"congestion_control,omitempty"` // "drop" or "block"
	Priority          string `json:"priority,omitempty"`           // a priority name
	Express           *bool  `json:"express,omitempty"`
	Reliability       string `json:"reliability,omitempty"` // "reliable" or "best_effort"
}

// TimestampingConfig is zenoh-rust's "timestamping" section: when enabled
// (by default only in router mode), PUTs that cross the node without a
// timestamp get one from its clock, as do the session's own publications;
// a timestamp more than 500ms ahead of the clock is replaced, or with
// DropFutureTimestamp the message is dropped.
type TimestampingConfig struct {
	Enabled             *bool `json:"enabled,omitempty"`
	DropFutureTimestamp bool  `json:"drop_future_timestamp,omitempty"`
}

var priorityNames = [8]string{"control", "real_time", "interactive_high", "interactive_low",
	"data_high", "data", "data_low", "background"}

func priorityByName(name string) (Priority, bool) {
	for i, n := range priorityNames[1:] {
		if n == name {
			return Priority(i + 1), true
		}
	}
	return 0, false
}

// -----------------------------------------------------------------------------
// Compiled configuration
// -----------------------------------------------------------------------------

// msgClass is a set of data message kinds.
type msgClass uint8

const (
	clsPut msgClass = 1 << iota
	clsDel
	clsQuery
	clsReply
)

func classOf(m any) (msgClass, *protocol.WireExpr) {
	switch m := m.(type) {
	case *protocol.Push:
		if _, del := m.Body.(*protocol.Del); del {
			return clsDel, &m.WireExpr
		}
		return clsPut, &m.WireExpr
	case *protocol.Request:
		return clsQuery, &m.WireExpr
	case *protocol.Response:
		return clsReply, &m.WireExpr
	}
	return 0, nil
}

func parseMessages(msgs []DataMessage, reply bool) (msgClass, error) {
	var c msgClass
	for _, m := range msgs {
		switch m {
		case MessagePut:
			c |= clsPut
		case MessageDelete:
			c |= clsDel
		case MessageQuery:
			c |= clsQuery
		case MessageReply:
			if !reply {
				return 0, fmt.Errorf("message %q not supported here", m)
			}
			c |= clsReply
		default:
			return 0, fmt.Errorf("unknown message %q", m)
		}
	}
	return c, nil
}

// flows: bit 0 ingress, bit 1 egress.
func parseFlows(fl []ACLFlow) (uint8, error) {
	if len(fl) == 0 {
		return 3, nil
	}
	var f uint8
	for _, x := range fl {
		switch x {
		case ACLIngress:
			f |= 1
		case ACLEgress:
			f |= 2
		default:
			return 0, fmt.Errorf("unknown flow %q", x)
		}
	}
	return f, nil
}

func parseKeys(keys []string) ([]*keyexpr.Key, error) {
	var out []*keyexpr.Key
	for _, k := range keys {
		ke, err := keyexpr.New(k)
		if err != nil {
			return nil, fmt.Errorf("key expression %q: %w", k, err)
		}
		out = append(out, ke)
	}
	return out, nil
}

// faceMatch is which faces an entry applies to (nil lists: any).
type faceMatch struct {
	ifaces, protos, zids []string
}

func (m faceMatch) matches(f *face) bool {
	if m.ifaces != nil && !anyIn(f.ifaces, m.ifaces) {
		return false
	}
	if m.protos != nil && !anyIn([]string{f.proto}, m.protos) {
		return false
	}
	if m.zids != nil && !anyIn([]string{strings.ToLower(f.zid.String())}, m.zids) {
		return false
	}
	return true
}

func anyIn(have, want []string) bool {
	for _, h := range have {
		for _, w := range want {
			if h == w {
				return true
			}
		}
	}
	return false
}

type dsRule struct {
	key       *keyexpr.Key
	threshold time.Duration // < 0: drop everything
}

type dsEntry struct {
	match faceMatch
	flows uint8
	msgs  msgClass
	rules []dsRule
}

type lpEntry struct {
	match faceMatch
	flows uint8
	msgs  msgClass
	keys  []*keyexpr.Key
	limit int
}

type qosEntry struct {
	match faceMatch
	flows uint8
	msgs  msgClass
	keys  []*keyexpr.Key // nil: all

	setPrio   Priority // 0: none
	incPrio   int
	setCC     int // 0 keep, 1 drop, 2 block
	setExpr   *bool
	fPrio     Priority // 0: any
	fCC       int      // 0 any, 1 drop, 2 block
	fExpr     *bool
	fRel      int // 0 any, 1 best effort, 2 reliable
	sizeLo    uint64
	sizeHi    uint64
	sizeCheck bool
}

// interceptors is the session's compiled interceptor configuration.
type interceptors struct {
	ds  []*dsEntry
	lp  []*lpEntry
	qos []*qosEntry
}

func parseCC(s string) (int, error) {
	switch s {
	case "":
		return 0, nil
	case "drop":
		return 1, nil
	case "block":
		return 2, nil
	}
	return 0, fmt.Errorf("unknown congestion control %q", s)
}

func parseRange(s string) (lo, hi uint64, err error) {
	a, b, ok := strings.Cut(s, "..")
	if !ok {
		return 0, 0, fmt.Errorf("invalid range %q", s)
	}
	hi = math.MaxUint64
	if a != "" {
		if lo, err = strconv.ParseUint(a, 10, 64); err != nil {
			return 0, 0, fmt.Errorf("invalid range %q", s)
		}
	}
	if b != "" {
		if hi, err = strconv.ParseUint(b, 10, 64); err != nil {
			return 0, 0, fmt.Errorf("invalid range %q", s)
		}
	}
	return lo, hi, nil
}

func checkIDs(section string, ids []string) error {
	seen := map[string]bool{}
	for _, id := range ids {
		if id == "" {
			continue
		}
		if seen[id] {
			return fmt.Errorf("invalid %s config: id '%s' is repeated", section, id)
		}
		seen[id] = true
	}
	return nil
}

func newInterceptors(ds []DownsamplingConfig, lp []LowPassFilterConfig, qos []QoSOverwriteConfig) (*interceptors, error) {
	if len(ds) == 0 && len(lp) == 0 && len(qos) == 0 {
		return nil, nil
	}
	ic := &interceptors{}
	var ids []string
	for _, c := range ds {
		ids = append(ids, c.ID)
	}
	if err := checkIDs("downsampling", ids); err != nil {
		return nil, err
	}
	for _, c := range ds {
		e := &dsEntry{match: faceMatch{ifaces: c.Interfaces, protos: c.LinkProtocols}}
		var err error
		if e.msgs, err = parseMessages(c.Messages, true); err != nil {
			return nil, fmt.Errorf("downsampling: %w", err)
		}
		if e.flows, err = parseFlows(c.Flows); err != nil {
			return nil, fmt.Errorf("downsampling: %w", err)
		}
		if len(c.Rules) == 0 {
			return nil, errors.New("downsampling: no rules")
		}
		for _, r := range c.Rules {
			k, err := keyexpr.New(r.KeyExpr)
			if err != nil {
				return nil, fmt.Errorf("downsampling: key expression %q: %w", r.KeyExpr, err)
			}
			if !(r.Freq >= 0) || math.IsInf(r.Freq, 0) {
				return nil, fmt.Errorf("downsampling: invalid freq %v", r.Freq)
			}
			rule := dsRule{key: k, threshold: -1}
			if r.Freq > 0 {
				rule.threshold = time.Duration(float64(time.Second) / r.Freq)
			}
			e.rules = append(e.rules, rule)
		}
		ic.ds = append(ic.ds, e)
	}
	ids = ids[:0]
	for _, c := range lp {
		ids = append(ids, c.ID)
	}
	if err := checkIDs("low-pass filter", ids); err != nil {
		return nil, err
	}
	for _, c := range lp {
		e := &lpEntry{match: faceMatch{ifaces: c.Interfaces, protos: c.LinkProtocols}, limit: c.SizeLimit}
		var err error
		if e.msgs, err = parseMessages(c.Messages, true); err != nil {
			return nil, fmt.Errorf("low_pass_filter: %w", err)
		}
		if e.flows, err = parseFlows(c.Flows); err != nil {
			return nil, fmt.Errorf("low_pass_filter: %w", err)
		}
		if e.keys, err = parseKeys(c.KeyExprs); err != nil {
			return nil, fmt.Errorf("low_pass_filter: %w", err)
		}
		if c.SizeLimit < 0 {
			return nil, fmt.Errorf("low_pass_filter: invalid size_limit %d", c.SizeLimit)
		}
		ic.lp = append(ic.lp, e)
	}
	ids = ids[:0]
	for _, c := range qos {
		ids = append(ids, c.ID)
	}
	if err := checkIDs("Qos Overwrite", ids); err != nil {
		return nil, err
	}
	for _, c := range qos {
		var zids []string
		for _, z := range c.ZIDs {
			zid, err := protocol.ParseZID(z)
			if err != nil {
				return nil, fmt.Errorf("qos/network: zid %q: %w", z, err)
			}
			zids = append(zids, strings.ToLower(zid.String()))
		}
		if c.ZIDs != nil && zids == nil {
			zids = []string{}
		}
		e := &qosEntry{match: faceMatch{ifaces: c.Interfaces, protos: c.LinkProtocols, zids: zids}}
		var err error
		if e.msgs, err = parseMessages(c.Messages, false); err != nil {
			return nil, fmt.Errorf("qos/network: %w", err)
		}
		if e.flows, err = parseFlows(c.Flows); err != nil {
			return nil, fmt.Errorf("qos/network: %w", err)
		}
		if c.KeyExprs != nil {
			if e.keys, err = parseKeys(c.KeyExprs); err != nil {
				return nil, fmt.Errorf("qos/network: %w", err)
			}
			if e.keys == nil {
				e.keys = []*keyexpr.Key{}
			}
		}
		if p := c.Overwrite.Priority; p != nil {
			if p.Priority > PriorityBackground || p.Increment < -7 || p.Increment > 7 {
				return nil, fmt.Errorf("qos/network: invalid priority overwrite %+v", *p)
			}
			e.setPrio, e.incPrio = p.Priority, p.Increment
		}
		if e.setCC, err = parseCC(c.Overwrite.CongestionControl); err != nil {
			return nil, fmt.Errorf("qos/network: %w", err)
		}
		e.setExpr = c.Overwrite.Express
		if q := c.QoS; q != nil {
			if e.fCC, err = parseCC(q.CongestionControl); err != nil {
				return nil, fmt.Errorf("qos/network: %w", err)
			}
			if q.Priority != "" {
				var ok bool
				if e.fPrio, ok = priorityByName(q.Priority); !ok {
					return nil, fmt.Errorf("qos/network: unknown priority %q", q.Priority)
				}
			}
			e.fExpr = q.Express
			switch q.Reliability {
			case "":
			case "best_effort":
				e.fRel = 1
			case "reliable":
				e.fRel = 2
			default:
				return nil, fmt.Errorf("qos/network: unknown reliability %q", q.Reliability)
			}
		}
		if c.PayloadSize != "" {
			if e.sizeLo, e.sizeHi, err = parseRange(c.PayloadSize); err != nil {
				return nil, fmt.Errorf("qos/network: payload_size: %w", err)
			}
			e.sizeCheck = true
		}
		ic.qos = append(ic.qos, e)
	}
	return ic, nil
}

// -----------------------------------------------------------------------------
// Per-face chains
// -----------------------------------------------------------------------------

// faceInterceptors are the interceptors of one face, per direction (0:
// ingress, 1: egress).
type faceInterceptors struct {
	ds  [2][]*dsState
	qos [2][]*qosEntry
	lp  [2][]*lpEntry
}

// dsState is a downsampling entry's state on one face and direction: when
// each rule last let a message through.
type dsState struct {
	e    *dsEntry
	mu   sync.Mutex
	last []time.Time
}

// setupInterceptors picks the interceptors that apply to f.
func (s *Session) setupInterceptors(f *face) {
	ic := s.icpt
	if ic == nil {
		return
	}
	fi := &faceInterceptors{}
	used := false
	for dir := 0; dir < 2; dir++ {
		bit := uint8(1) << dir
		for _, e := range ic.ds {
			if e.flows&bit != 0 && e.match.matches(f) {
				fi.ds[dir] = append(fi.ds[dir], &dsState{e: e, last: make([]time.Time, len(e.rules))})
				used = true
			}
		}
		for _, e := range ic.qos {
			if e.flows&bit != 0 && e.match.matches(f) {
				fi.qos[dir] = append(fi.qos[dir], e)
				used = true
			}
		}
		for _, e := range ic.lp {
			if e.flows&bit != 0 && e.match.matches(f) {
				fi.lp[dir] = append(fi.lp[dir], e)
				used = true
			}
		}
	}
	if used {
		f.icpt = fi
	}
}

// msgKey lazily resolves the key of the message being intercepted.
type msgKey struct {
	f    *face
	we   *protocol.WireExpr
	dir  int
	done bool
	key  *keyexpr.Key
}

func (k *msgKey) get() *keyexpr.Key {
	if !k.done {
		k.done = true
		var s string
		var ok bool
		if k.dir == 0 {
			s, ok = k.f.resolve(*k.we)
		} else {
			s, ok = k.f.resolveOwn(*k.we)
		}
		if ok {
			k.key, _ = keyexpr.New(s)
		}
	}
	return k.key
}

// interceptPre runs the stages before access control (downsampling): it
// reports whether m passes.
func (fi *faceInterceptors) interceptPre(f *face, dir int, m any) bool {
	if len(fi.ds[dir]) == 0 {
		return true
	}
	cls, we := classOf(m)
	if cls == 0 {
		return true
	}
	k := msgKey{f: f, we: we, dir: dir}
	now := time.Now()
	for _, d := range fi.ds[dir] {
		if !d.pass(cls, &k, now) {
			return false
		}
	}
	return true
}

func (d *dsState) pass(cls msgClass, k *msgKey, now time.Time) bool {
	if d.e.msgs&cls == 0 {
		return true
	}
	key := k.get()
	if key == nil {
		return true
	}
	for i, r := range d.e.rules {
		if !r.key.Intersects(key) {
			continue
		}
		if r.threshold < 0 {
			return false
		}
		d.mu.Lock()
		defer d.mu.Unlock()
		if d.last[i].IsZero() || now.Sub(d.last[i]) >= r.threshold {
			d.last[i] = now
			return true
		}
		return false
	}
	return true
}

// interceptPost runs the stages after access control (QoS overwrite, then
// the low-pass filter): it returns m, or a modified copy, and whether it
// passes.
func (fi *faceInterceptors) interceptPost(f *face, dir int, m any, reliable bool) (any, bool) {
	if len(fi.qos[dir]) == 0 && len(fi.lp[dir]) == 0 {
		return m, true
	}
	cls, we := classOf(m)
	if cls == 0 {
		return m, true
	}
	k := msgKey{f: f, we: we, dir: dir}
	for _, e := range fi.qos[dir] {
		m = e.apply(m, cls, &k, reliable)
	}
	if len(fi.lp[dir]) > 0 {
		limit := math.MaxInt
		if key := k.get(); key == nil {
			limit = 0
		} else {
			for _, e := range fi.lp[dir] {
				if e.msgs&cls == 0 {
					continue
				}
				for _, ek := range e.keys {
					if e.limit < limit && ek.Includes(key) {
						limit = e.limit
					}
				}
			}
		}
		if msgSize(m) > limit {
			return m, false
		}
	}
	return m, true
}

// msgSize is the payload plus attachment size of a data message.
func msgSize(m any) int {
	data := func(b protocol.DataBody) int {
		switch b := b.(type) {
		case *protocol.Put:
			return len(b.Payload) + partsLen(b.Parts) + len(b.Attachment)
		case *protocol.Del:
			return len(b.Attachment)
		}
		return 0
	}
	switch m := m.(type) {
	case *protocol.Push:
		return data(m.Body)
	case *protocol.Request:
		if q := m.Query; q != nil {
			n := len(q.Attachment)
			if q.Body != nil {
				n += len(q.Body.Payload) + partsLen(q.Body.Parts)
			}
			return n
		}
	case *protocol.Response:
		switch b := m.Body.(type) {
		case *protocol.Reply:
			return data(b.Body)
		case *protocol.Err:
			return len(b.Payload) + partsLen(b.Parts)
		}
	}
	return 0
}

func partsLen(parts []protocol.PayloadPart) int {
	n := 0
	for _, p := range parts {
		if p.Shm != nil {
			n += int(p.Shm.DataLen)
		} else {
			n += len(p.Raw)
		}
	}
	return n
}

// payloadSize is the payload size QoS overwrite filters on (zenoh-rust:
// PUT, query body, reply PUT and error payloads).
func payloadSize(m any) uint64 {
	switch m := m.(type) {
	case *protocol.Push:
		if p, ok := m.Body.(*protocol.Put); ok {
			return uint64(len(p.Payload) + partsLen(p.Parts))
		}
	case *protocol.Request:
		if m.Query != nil && m.Query.Body != nil {
			return uint64(len(m.Query.Body.Payload) + partsLen(m.Query.Body.Parts))
		}
	}
	return 0
}

func qosOf(m any) protocol.QoS {
	switch m := m.(type) {
	case *protocol.Push:
		return m.QoS
	case *protocol.Request:
		return m.QoS
	}
	return 0
}

func (e *qosEntry) apply(m any, cls msgClass, k *msgKey, reliable bool) any {
	if e.msgs&cls == 0 {
		return m
	}
	q := qosOf(m)
	switch {
	case e.fPrio != 0 && q.Priority() != e.fPrio,
		e.fCC != 0 && q.Block() != (e.fCC == 2),
		e.fExpr != nil && q.Express() != *e.fExpr,
		e.fRel != 0 && reliable != (e.fRel == 2):
		return m
	}
	if e.sizeCheck {
		if n := payloadSize(m); n < e.sizeLo || n > e.sizeHi {
			return m
		}
	}
	if e.keys != nil {
		key := k.get()
		if key == nil {
			return m
		}
		hit := false
		for _, ek := range e.keys {
			if ek.Includes(key) {
				hit = true
				break
			}
		}
		if !hit {
			return m
		}
	}
	prio, block, express := q.Priority(), q.Block(), q.Express()
	switch {
	case e.setPrio != 0:
		prio = e.setPrio
	case e.incPrio != 0:
		// zenoh-rust 1.10 means negative increments to raise the priority
		// but ignores them (a bug); we apply them, down to Control.
		p := int(prio) + e.incPrio
		prio = Priority(max(int(protocol.PriorityControl), min(p, int(protocol.PriorityBackground))))
	}
	if e.setCC != 0 {
		block = e.setCC == 2
	}
	if e.setExpr != nil {
		express = *e.setExpr
	}
	nq := protocol.NewQoS(prio, block, express)
	switch m := m.(type) {
	case *protocol.Push:
		cp := *m
		cp.QoS = nq
		return &cp
	case *protocol.Request:
		cp := *m
		cp.QoS = nq
		return &cp
	}
	return m
}

// interceptIngress applies f's interceptors and access control to a
// received message: it returns the message to process, or false to drop it.
// Like zenoh-rust, it answers a dropped query with a RESPONSE_FINAL and a
// dropped interest with a D_FINAL, so that the sender doesn't wait for its
// timeout.
func (f *face) interceptIngress(m protocol.Message, reliable bool) (protocol.Message, bool) {
	fi := f.icpt
	ok, reason := true, uint8(dropDownsampling)
	if fi != nil {
		ok = fi.interceptPre(f, 0, m)
	}
	if ok && f.acl != nil {
		ok, reason = f.aclIngress(m), dropACL
	}
	if ok && fi != nil {
		var out any
		if out, ok = fi.interceptPost(f, 0, m, reliable); ok {
			m = out.(protocol.Message)
		}
		reason = dropLowPass
	}
	if !ok {
		f.statsDrop(transport.Rx, reason, m)
		switch m := m.(type) {
		case *protocol.Request:
			_ = f.tr.Send(true, &protocol.ResponseFinal{RequestID: m.ID, QoS: m.QoS})
		case *protocol.Interest:
			if m.Mode != protocol.InterestFinal {
				_ = f.tr.Send(true, &protocol.Declare{HasInterestID: true, InterestID: m.ID,
					QoS: protocol.QoSDeclare, Body: &protocol.DeclareFinal{}})
			}
		}
		return nil, false
	}
	return m, true
}

// refused handles a request or interest we were about to send to f and
// that an interceptor or access control dropped: as zenoh-rust does, it
// acts as if f had answered it with a final (asynchronously: the caller
// may hold locks), so whoever waits for f's answer doesn't wait for a
// timeout.
func (f *face) refused(m transport.Encoder) {
	var final protocol.Message
	switch m := m.(type) {
	case *protocol.Request:
		final = &protocol.ResponseFinal{RequestID: m.ID, QoS: m.QoS}
	case *protocol.Interest:
		if m.Mode == protocol.InterestFinal {
			return
		}
		final = &protocol.Declare{HasInterestID: true, InterestID: m.ID, QoS: protocol.QoSDeclare,
			Body: &protocol.DeclareFinal{}}
	default:
		return
	}
	go f.dispatch(final, true)
}

// interceptEgress applies f's interceptors and access control to messages
// about to be sent to it, returning those that may go (msgs is not
// modified).
func (f *face) interceptEgress(msgs []transport.Encoder, reliable bool) []transport.Encoder {
	fi := f.icpt
	if fi != nil {
		var out []transport.Encoder
		for _, m := range msgs {
			if fi.interceptPre(f, 1, m) {
				out = append(out, m)
			} else {
				f.statsDrop(transport.Tx, dropDownsampling, m)
				f.refused(m)
			}
		}
		msgs = out
	}
	if f.acl != nil {
		msgs = f.aclEgress(msgs)
	}
	if fi != nil && (len(fi.qos[1]) > 0 || len(fi.lp[1]) > 0) {
		out := msgs[:0:0]
		for _, m := range msgs {
			if nm, ok := fi.interceptPost(f, 1, m, reliable); ok {
				out = append(out, nm.(transport.Encoder))
			} else {
				f.statsDrop(transport.Tx, dropLowPass, m)
				f.refused(m)
			}
		}
		msgs = out
	}
	return msgs
}

// -----------------------------------------------------------------------------
// Timestamping
// -----------------------------------------------------------------------------

// hlcMaxDelta is how far ahead of our clock a timestamp may be (uhlc's
// default), in NTP64 units: 500ms.
const hlcMaxDelta = uint64(1) << 31

// stampIngress applies timestamping to a PUT crossing the node: it stamps
// it if it has no timestamp, and handles timestamps from the future. It
// reports whether the message may go on.
func (s *Session) stampIngress(p *protocol.Put) bool {
	if p.Timestamp == nil {
		p.SetTimestamp(s.NewTimestamp())
		return true
	}
	if s.clock.update(p.Timestamp.Time) {
		return true
	}
	if s.tsDrop {
		return false
	}
	p.SetTimestamp(s.NewTimestamp())
	return true
}
