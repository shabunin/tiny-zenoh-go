//go:build !tinygo || zenoh_stats

package zenoh

import (
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/shabunin/tiny-zenoh-go/keyexpr"
	"github.com/shabunin/tiny-zenoh-go/link"
	"github.com/shabunin/tiny-zenoh-go/protocol"
	"github.com/shabunin/tiny-zenoh-go/transport"
)

// Traffic statistics, as zenoh-rust's "stats" feature (commons/zenoh-stats)
// keeps them and serves them on the admin space:
//
//   - per link (package transport): bytes, transport messages, and network
//     messages by priority, kind and SHM use;
//   - per transport: the payload sizes of the data messages (PUT, DELETE,
//     query, reply) crossing it, by key space (admin or user), and of those
//     the node dropped (access control, downsampling, low-pass filter, no
//     link left); also per configured key expression (StatsConfig.Filters).
//
// <root>/metrics serves them in OpenMetrics text with zenohd's metric names
// and labels; <root>?_stats=true adds them to the node's JSON. A link's
// counters move to its transport when the link goes; a closed transport's
// are kept a minute (shown with ?disconnected=true; a peer reconnecting in
// that time resumes them), then added to totals of their own.
//
// Multicast: a group is one transport (remote_group set, no remote_zid)
// whose single link counts all of its senders.

// statsDefault: statistics are on unless Config.Stats disables them.
const statsDefault = true

// statsGrace is how long a closed transport's statistics stay its own
// (zenoh-rust GARBAGE_COLLECTION_DELAY).
const statsGrace = time.Minute

// payloadBuckets are the payload histogram bounds (PAYLOAD_SIZE_BUCKETS).
var payloadBuckets = [...]uint64{0, 1 << 5, 1 << 10, 1 << 15, 1 << 20, 1 << 25, 1 << 30}

type histogram struct {
	sum uint64
	n   [len(payloadBuckets) + 1]uint64 // per bucket, the last is +Inf
}

func (h *histogram) observe(v uint64) {
	i := 0
	for i < len(payloadBuckets) && v > payloadBuckets[i] {
		i++
	}
	h.n[i]++
	h.sum += v
}

func (h *histogram) add(o *histogram) {
	h.sum += o.sum
	for i := range h.n {
		h.n[i] += o.n[i]
	}
}

func (h *histogram) count() (c uint64) {
	for _, n := range h.n {
		c += n
	}
	return c
}

type payloadKey struct {
	dir, space, prio, shm uint8 // space: 0 user, 1 admin
	kind                  transport.MessageKind
}

type perKeyKey struct {
	payloadKey
	filter int
}

type dropKey struct {
	dir, prio, reason uint8
	kind              transport.MessageKind
	proto             string // only for congestion
}

// statsLabels identify a transport (zenoh-rust TransportLabels).
type statsLabels struct{ zid, whatami, group, cn string }

// statsTransport are the statistics of one transport.
type statsTransport struct {
	labels statsLabels
	face   *face       // unicast
	group  *mcastGroup // multicast
	closed time.Time   // when it closed, zero while open (guarded by statsRegistry.mu)

	mu      sync.Mutex
	retired map[string]*transport.LinkCounts // links gone, by protocol
	payload map[payloadKey]*histogram
	perKey  map[perKeyKey]*histogram
	dropped map[dropKey]*histogram
}

func newStatsTransport(l statsLabels) *statsTransport {
	return &statsTransport{labels: l, retired: map[string]*transport.LinkCounts{},
		payload: map[payloadKey]*histogram{}, perKey: map[perKeyKey]*histogram{}, dropped: map[dropKey]*histogram{}}
}

// statsRegistry are a session's statistics.
type statsRegistry struct {
	filters []string

	mu     sync.Mutex
	open   []*statsTransport // open and recently closed
	gone   *statsTransport   // transports closed for longer, together
	protos map[string]bool   // protocols links were opened on
}

func newStatsRegistry(cfg StatsConfig) (*statsRegistry, error) {
	r := &statsRegistry{gone: newStatsTransport(statsLabels{}), protos: map[string]bool{}}
	for _, f := range cfg.Filters {
		k, err := keyexpr.New(f)
		if err != nil {
			return nil, err
		}
		r.filters = append(r.filters, k.String())
	}
	return r, nil
}

// gcLocked folds the transports closed for over statsGrace into r.gone.
func (r *statsRegistry) gcLocked(now time.Time) {
	keep := r.open[:0]
	for _, t := range r.open {
		if t.closed.IsZero() || now.Sub(t.closed) <= statsGrace {
			keep = append(keep, t)
			continue
		}
		t.mu.Lock()
		g := r.gone
		g.mu.Lock()
		for p, c := range t.retired {
			if g.retired[p] == nil {
				g.retired[p] = &transport.LinkCounts{}
			}
			g.retired[p].Add(c)
		}
		mergeHists(g.payload, t.payload)
		mergeHists(g.perKey, t.perKey)
		mergeHists(g.dropped, t.dropped)
		g.mu.Unlock()
		t.mu.Unlock()
	}
	clear(r.open[len(keep):])
	r.open = keep
}

func mergeHists[K comparable](dst, src map[K]*histogram) {
	for k, h := range src {
		if dst[k] == nil {
			dst[k] = &histogram{}
		}
		dst[k].add(h)
	}
}

// transportUp starts the statistics of a transport, resuming those of a
// recently closed one with the same labels.
func (r *statsRegistry) transportUp(l statsLabels) *statsTransport {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.gcLocked(time.Now())
	for _, t := range r.open {
		if t.labels == l && !t.closed.IsZero() {
			t.closed = time.Time{}
			return t
		}
	}
	t := newStatsTransport(l)
	r.open = append(r.open, t)
	return t
}

func (r *statsRegistry) transportDown(t *statsTransport) {
	r.mu.Lock()
	t.closed = time.Now()
	t.face, t.group = nil, nil
	r.mu.Unlock()
}

// linkProto is a link's protocol label: its destination's protocol if
// zenoh-rust knows it, else the whole locator.
func linkProto(dst string) string {
	p, _, _ := strings.Cut(dst, "/")
	switch p {
	case "tcp", "udp", "tls", "quic", "unixsock-stream", "ws", "serial", "unixpipe", "vsock":
		return p
	}
	return dst
}

func (r *statsRegistry) linkSeen(dst string) {
	r.mu.Lock()
	r.protos[linkProto(dst)] = true
	r.mu.Unlock()
}

// retire moves a gone link's counters to its transport.
func (t *statsTransport) retire(dst string, s *transport.LinkStats) {
	if s == nil {
		return
	}
	c := s.Snapshot()
	p := linkProto(dst)
	t.mu.Lock()
	if t.retired[p] == nil {
		t.retired[p] = &transport.LinkCounts{}
	}
	t.retired[p].Add(&c)
	t.mu.Unlock()
}

// observe records a data message's payload crossing the transport.
func (t *statsTransport) observe(filters []string, dir int, m any, key string, shm bool) {
	kind, ok := transport.KindOf(m)
	if !ok || kind > transport.KindReplyErr {
		return
	}
	k := payloadKey{dir: uint8(dir), prio: uint8(protocol.QoSOf(m).Priority() & 7), kind: kind}
	if strings.HasPrefix(key, "@/") {
		k.space = 1
	}
	if shm {
		k.shm = 1
	}
	size := uint64(msgSize(m))
	t.mu.Lock()
	defer t.mu.Unlock()
	h := t.payload[k]
	if h == nil {
		h = &histogram{}
		t.payload[k] = h
	}
	h.observe(size)
	for i, f := range filters {
		if key == "" || !keyexpr.Intersect(f, key) {
			continue
		}
		pk := perKeyKey{k, i}
		h := t.perKey[pk]
		if h == nil {
			h = &histogram{}
			t.perKey[pk] = h
		}
		h.observe(size)
	}
}

// drop records a message the node dropped.
func (t *statsTransport) drop(dir int, reason uint8, m any) {
	kind, ok := transport.KindOf(m)
	if !ok {
		return
	}
	k := dropKey{dir: uint8(dir), prio: uint8(protocol.QoSOf(m).Priority() & 7), reason: reason, kind: kind}
	t.mu.Lock()
	h := t.dropped[k]
	if h == nil {
		h = &histogram{}
		t.dropped[k] = h
	}
	h.observe(uint64(msgSize(m)))
	t.mu.Unlock()
}

// -----------------------------------------------------------------------------
// Session hooks
// -----------------------------------------------------------------------------

// statsUp starts the statistics of a new face.
func (s *Session) statsUp(f *face) {
	if s.stats == nil || f.tr == nil {
		return
	}
	l := statsLabels{zid: f.zid.String(), whatami: f.whatami.String()}
	if infos := f.tr.LinkInfos(); len(infos) > 0 {
		l.cn = link.PeerCommonName(infos[0].Link)
	}
	t := s.stats.transportUp(l)
	s.stats.mu.Lock()
	t.face = f
	s.stats.mu.Unlock()
	f.stats = t
}

// statsLinks notes the links f has now and retires those it lost.
func (s *Session) statsLinks(f *face, old, now []adminLink) {
	if f.stats == nil {
		return
	}
	for _, l := range now {
		s.stats.linkSeen(l.dst)
	}
next:
	for _, o := range old {
		for _, l := range now {
			if l.info.Stats == o.info.Stats {
				continue next
			}
		}
		f.stats.retire(o.dst, o.info.Stats)
	}
}

// statsDown ends the statistics of a face that closed with links.
func (s *Session) statsDown(f *face, links []adminLink) {
	if f.stats == nil {
		return
	}
	for _, l := range links {
		f.stats.retire(l.dst, l.info.Stats)
	}
	s.stats.transportDown(f.stats)
}

// statsGroupUp and statsGroupDown start and end a multicast group's
// statistics.
func (s *Session) statsGroupUp(g *mcastGroup) {
	if s.stats == nil {
		return
	}
	st := s.stats.transportUp(statsLabels{group: g.locator})
	s.stats.mu.Lock()
	st.group = g
	s.stats.mu.Unlock()
	s.stats.linkSeen(g.locator)
	s.mu.Lock()
	g.stats = st
	s.mu.Unlock()
}

func (s *Session) statsGroupDown(g *mcastGroup) {
	s.mu.Lock()
	st := g.stats
	s.mu.Unlock()
	if st != nil {
		st.retire(g.locator, g.tr.Stats())
		s.stats.transportDown(st)
	}
}

// observeRx and observeTx record the data messages crossing f.
func (f *face) observeRx(m protocol.Message, shm bool) {
	var we *protocol.WireExpr
	switch m := m.(type) {
	case *protocol.Push:
		we = &m.WireExpr
	case *protocol.Request:
		we = &m.WireExpr
	case *protocol.Response:
		we = &m.WireExpr
	default:
		return
	}
	key, _ := f.resolve(*we)
	f.stats.observe(f.s.stats.filters, transport.Rx, m, key, shm)
}

func (f *face) observeTx(msgs []transport.Encoder) {
	for _, m := range msgs {
		var we *protocol.WireExpr
		switch m := m.(type) {
		case *protocol.Push:
			we = &m.WireExpr
		case *protocol.Request:
			we = &m.WireExpr
		case *protocol.Response:
			we = &m.WireExpr
		default:
			continue
		}
		key, _ := f.resolveOwn(*we)
		f.stats.observe(f.s.stats.filters, transport.Tx, m, key, transport.IsShm(m))
	}
}

// statsDrop records a message dropped on its way in (dir Rx) or out.
func (f *face) statsDrop(dir int, reason uint8, m any) {
	if f.stats != nil {
		f.stats.drop(dir, reason, m)
	}
}

// -----------------------------------------------------------------------------
// Collection
// -----------------------------------------------------------------------------

type linkSnap struct {
	proto, src, dst string
	c               transport.LinkCounts
	link            Link
}

type transportSnap struct {
	labels       statsLabels
	disconnected bool
	info         TransportInfo
	links        []linkSnap
	retired      map[string]transport.LinkCounts
	payload      map[payloadKey]histogram
	perKey       map[perKeyKey]histogram
	dropped      map[dropKey]histogram
}

// byProto sums the transport's link counters by protocol.
func (t *transportSnap) byProto() map[string]*transport.LinkCounts {
	out := map[string]*transport.LinkCounts{}
	for p, c := range t.retired {
		c := c
		out[p] = &c
	}
	for _, l := range t.links {
		if out[l.proto] == nil {
			out[l.proto] = &transport.LinkCounts{}
		}
		out[l.proto].Add(&l.c)
	}
	return out
}

func copyHists[K comparable](src map[K]*histogram) map[K]histogram {
	out := make(map[K]histogram, len(src))
	for k, h := range src {
		out[k] = *h
	}
	return out
}

// statsSnap reads the statistics: the open (and recently closed)
// transports, and the closed ones together. live and links counts are for
// the gauges: open transports and links by protocol.
func (s *Session) statsSnap() (open []*transportSnap, gone *transportSnap, links map[string]int) {
	r := s.stats
	r.mu.Lock()
	r.gcLocked(time.Now())
	ts := append([]*statsTransport(nil), r.open...)
	type src struct {
		f      *face
		g      *mcastGroup
		closed bool
	}
	srcs := make([]src, len(ts))
	for i, t := range ts {
		srcs[i] = src{t.face, t.group, !t.closed.IsZero()}
	}
	links = map[string]int{}
	for p := range r.protos {
		links[p] = 0
	}
	r.mu.Unlock()

	snap := func(t *statsTransport) *transportSnap {
		t.mu.Lock()
		defer t.mu.Unlock()
		out := &transportSnap{labels: t.labels, retired: map[string]transport.LinkCounts{},
			payload: copyHists(t.payload), perKey: copyHists(t.perKey), dropped: copyHists(t.dropped)}
		for p, c := range t.retired {
			out.retired[p] = *c
		}
		return out
	}
	for i, t := range ts {
		ss := snap(t)
		ss.disconnected = srcs[i].closed
		switch {
		case srcs[i].closed:
		case srcs[i].f != nil:
			f := srcs[i].f
			if f.tr != nil {
				ss.info = f.transportInfo()
			}
			for _, l := range f.adminLinks() {
				ls := linkSnap{proto: linkProto(l.dst), src: l.src, dst: l.dst, c: l.info.Stats.Snapshot(), link: f.linkOf(l)}
				ss.links = append(ss.links, ls)
				links[ls.proto]++
			}
		case srcs[i].g != nil:
			g := srcs[i].g
			ss.info = TransportInfo{Multicast: true, Proto: "udp", Links: 1, BatchSize: g.batch}
			k := Link{Multicast: true, Src: g.src, Dst: g.locator, Group: g.locator, MTU: g.mtu,
				Interfaces: []string{}, Reliability: BestEffort}
			ss.links = append(ss.links, linkSnap{proto: linkProto(g.locator), src: g.src, dst: g.locator,
				c: g.tr.Stats().Snapshot(), link: k})
			links[linkProto(g.locator)]++
		}
		open = append(open, ss)
	}
	sort.SliceStable(open, func(i, j int) bool { return open[i].labels.less(open[j].labels) })
	return open, snap(r.gone), links
}

func (a statsLabels) less(b statsLabels) bool {
	if a.zid != b.zid {
		return a.zid < b.zid
	}
	if a.group != b.group {
		return a.group < b.group
	}
	if a.whatami != b.whatami {
		return a.whatami < b.whatami
	}
	return a.cn < b.cn
}

// traffic sums a transport's counters into Tx and Rx.
func (t *transportSnap) traffic() (tx, rx TrafficStats) {
	ds := [2]*TrafficStats{&tx, &rx}
	for _, c := range t.byProto() {
		for d := range 2 {
			ds[d].Bytes += c.Bytes[d]
			ds[d].TransportMessages += c.TransportMessages[d]
			ds[d].NetworkMessages += c.NetworkTotal(d, -1)
		}
	}
	for k, h := range t.payload {
		ds[k.dir].Messages += h.count()
		ds[k.dir].PayloadBytes += h.sum
	}
	for k, h := range t.dropped {
		ds[k.dir].Dropped += h.count()
	}
	return tx, rx
}

func linkTraffic(c *transport.LinkCounts) (tx, rx TrafficStats) {
	ds := [2]*TrafficStats{&tx, &rx}
	for d := range 2 {
		ds[d].Bytes = c.Bytes[d]
		ds[d].TransportMessages = c.TransportMessages[d]
		ds[d].NetworkMessages = c.NetworkTotal(d, -1)
	}
	return tx, rx
}

func addTraffic(a *TrafficStats, b TrafficStats) {
	a.Bytes += b.Bytes
	a.TransportMessages += b.TransportMessages
	a.NetworkMessages += b.NetworkMessages
	a.Messages += b.Messages
	a.PayloadBytes += b.PayloadBytes
	a.Dropped += b.Dropped
}

// Stats returns the node's traffic statistics (zero without them, see
// Config.Stats).
func (s *Session) Stats() Stats {
	var out Stats
	if s.stats == nil {
		return out
	}
	open, gone, _ := s.statsSnap()
	tx, rx := gone.traffic()
	addTraffic(&out.Tx, tx)
	addTraffic(&out.Rx, rx)
	for _, t := range open {
		tx, rx := t.traffic()
		addTraffic(&out.Tx, tx)
		addTraffic(&out.Rx, rx)
		if t.disconnected {
			continue
		}
		ts := TransportStats{Transport: t.info, Tx: tx, Rx: rx}
		for _, l := range t.links {
			ls := LinkStats{Link: l.link}
			ls.Tx, ls.Rx = linkTraffic(&l.c)
			ts.Links = append(ts.Links, ls)
		}
		out.Transports = append(out.Transports, ts)
	}
	return out
}

// -----------------------------------------------------------------------------
// OpenMetrics
// -----------------------------------------------------------------------------

var promPriorityNames = [8]string{"control", "real-time", "interactive-high", "interactive-low",
	"data-high", "data", "data-low", "background"}

type metricsWriter struct {
	b      strings.Builder
	consts string // the constant labels
	descs  bool
}

func promEscape(v string) string {
	if !strings.ContainsAny(v, "\\\"\n") {
		return v
	}
	var b strings.Builder
	for i := 0; i < len(v); i++ {
		switch c := v[i]; c {
		case '\\', '"':
			b.WriteByte('\\')
			b.WriteByte(c)
		case '\n':
			b.WriteString(`\n`)
		default:
			b.WriteByte(c)
		}
	}
	return b.String()
}

// lbl formats label pairs: name, value, name, value...
func lbl(kv ...string) string {
	var b strings.Builder
	for i := 0; i+1 < len(kv); i += 2 {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(kv[i] + `="` + promEscape(kv[i+1]) + `"`)
	}
	return b.String()
}

func joinLabels(parts ...string) string {
	out := parts[:0:0]
	for _, p := range parts {
		if p != "" {
			out = append(out, p)
		}
	}
	return strings.Join(out, ",")
}

func (w *metricsWriter) desc(name, help, unit, typ string) {
	if !w.descs {
		return
	}
	w.b.WriteString("# HELP " + name + " " + help + "\n# TYPE " + name + " " + typ + "\n")
	if unit != "" {
		w.b.WriteString("# UNIT " + name + " " + unit + "\n")
	}
}

func (w *metricsWriter) sample(name, labels, value string) {
	w.b.WriteString(name + "{" + joinLabels(w.consts, labels) + "} " + value + "\n")
}

func (w *metricsWriter) hist(name, labels string, h *histogram) {
	w.sample(name+"_sum", labels, rustFloat(float64(h.sum)))
	w.sample(name+"_count", labels, strconv.FormatUint(h.count(), 10))
	var cum uint64
	for i, n := range h.n {
		cum += n
		le := "+Inf"
		if i < len(payloadBuckets) {
			le = rustFloat(float64(payloadBuckets[i]))
		}
		w.b.WriteString(name + "_bucket{" + joinLabels(w.consts, lbl("le", le), labels) + "} " +
			strconv.FormatUint(cum, 10) + "\n")
	}
}

func (l statsLabels) prom() string {
	return lbl("remote_zid", l.zid, "remote_whatami", l.whatami, "remote_group", l.group, "remote_cn", l.cn)
}

// series is one metric value of a family: its own labels, for a transport
// (nil: the closed ones together) and link (nil: none).
type series struct {
	labels string
	t      *transportSnap
	l      *linkSnap
	v      uint64
	h      *histogram
}

// family writes a zenoh-rust TransportFamily: the totals by label set,
// then per transport, then per link.
func (w *metricsWriter) family(name, help, unit string, hist bool, opts MetricsOptions, ss []series) {
	typ, suffix := "counter", "_total"
	if hist {
		typ, suffix = "histogram", ""
	}
	full := func(n string) string {
		if unit != "" {
			n += "_" + unit
		}
		return n
	}
	emit := func(n string, labels string, s *series) {
		if hist {
			w.hist(n, labels, s.h)
		} else {
			w.sample(n+suffix, labels, strconv.FormatUint(s.v, 10))
		}
	}
	// Totals: sums by label set, in first-seen order of sorted series.
	n := full("zenoh_" + name)
	w.desc(n, help, unit, typ)
	var order []string
	tot := map[string]*series{}
	for i := range ss {
		s := &ss[i]
		if s.t != nil && s.t.disconnected && !opts.Disconnected {
			continue
		}
		a := tot[s.labels]
		if a == nil {
			a = &series{labels: s.labels, h: &histogram{}}
			tot[s.labels] = a
			order = append(order, s.labels)
		}
		a.v += s.v
		if s.h != nil {
			a.h.add(s.h)
		}
	}
	for _, k := range order {
		emit(n, k, tot[k])
	}
	if !opts.NoPerTransport {
		n := full("zenoh_" + name + "_per_transport")
		w.desc(n, help+" (per transport)", unit, typ)
		type tk struct {
			labels string
			t      *transportSnap
		}
		var order []tk
		per := map[tk]*series{}
		for i := range ss {
			s := &ss[i]
			if s.t == nil || s.t.disconnected && !opts.Disconnected {
				continue
			}
			k := tk{s.labels, s.t}
			a := per[k]
			if a == nil {
				a = &series{labels: s.labels, t: s.t, h: &histogram{}}
				per[k] = a
				order = append(order, k)
			}
			a.v += s.v
			if s.h != nil {
				a.h.add(s.h)
			}
		}
		for _, k := range order {
			emit(n, joinLabels(k.labels, k.t.labels.prom(), lbl("disconnected", jsonBool(k.t.disconnected))), per[k])
		}
	}
	if !opts.NoPerLink {
		var links []*series
		for i := range ss {
			if s := &ss[i]; s.l != nil && !(s.t.disconnected && !opts.Disconnected) {
				links = append(links, s)
			}
		}
		if len(links) > 0 {
			n := full("zenoh_" + name + "_per_link")
			w.desc(n, help+" (per link)", unit, typ)
			for _, s := range links {
				emit(n, joinLabels(s.labels, s.t.labels.prom(), lbl("src_locator", s.l.src, "dst_locator", s.l.dst)), s)
			}
		}
	}
}

// perKeyFamily writes the per-filter payload histograms.
func (w *metricsWriter) perKeyFamily(name, help string, opts MetricsOptions, filters []string,
	all []*transportSnap, dir uint8) {
	type entry struct {
		k perKeyKey
		t *transportSnap
		h histogram
	}
	var es []entry
	for _, t := range all {
		if t.disconnected && !opts.Disconnected {
			continue
		}
		for k, h := range t.perKey {
			if k.dir == dir && k.filter < len(filters) {
				es = append(es, entry{k, t, h})
			}
		}
	}
	sort.SliceStable(es, func(i, j int) bool { return payloadLess(es[i].k.payloadKey, es[j].k.payloadKey) })
	n := "zenoh_" + name + "_bytes"
	w.desc(n, help, "bytes", "histogram")
	var order []perKeyKey
	tot := map[perKeyKey]*histogram{}
	for _, e := range es {
		if tot[e.k] == nil {
			tot[e.k] = &histogram{}
			order = append(order, e.k)
		}
		tot[e.k].add(&e.h)
	}
	for _, k := range order {
		w.hist(n, joinLabels(payloadLabels(k.payloadKey), lbl("key", filters[k.filter])), tot[k])
	}
	if !opts.NoPerTransport {
		n := "zenoh_" + name + "_per_transport_bytes"
		w.desc(n, help+" (per transport)", "bytes", "histogram")
		for _, e := range es {
			if e.t.labels == (statsLabels{}) {
				continue // the closed transports together
			}
			w.hist(n, joinLabels(payloadLabels(e.k.payloadKey), e.t.labels.prom(),
				lbl("disconnected", jsonBool(e.t.disconnected), "key", filters[e.k.filter])), &e.h)
		}
	}
}

func payloadLess(a, b payloadKey) bool {
	if a.space != b.space {
		return a.space < b.space
	}
	if a.kind != b.kind {
		return a.kind < b.kind
	}
	if a.prio != b.prio {
		return a.prio < b.prio
	}
	return a.shm < b.shm
}

func payloadLabels(k payloadKey) string {
	space := "user"
	if k.space == 1 {
		space = "admin"
	}
	return lbl("space", space, "priority", promPriorityNames[k.prio], "message", k.kind.String(),
		"shm", jsonBool(k.shm == 1))
}

// Metrics renders the statistics in OpenMetrics text, as the admin space
// serves them at <root>/metrics (uncompressed) and with zenohd's metric
// names. Without statistics it only has the build information.
func (s *Session) Metrics(opts MetricsOptions) string { return s.metrics(opts) }

// metrics renders the statistics like zenohd's <root>/metrics.
func (s *Session) metrics(opts MetricsOptions) string {
	if s.stats == nil {
		return s.buildInfoMetrics()
	}
	w := &metricsWriter{descs: !opts.NoDescriptors,
		consts: lbl("local_id", s.zid.String(), "local_whatami", s.cfg.Mode.String())}
	w.desc("zenoh_build", "Zenoh build version.", "", "info")
	w.sample("zenoh_build_info", lbl("version", versionString()), "1")
	open, gone, links := s.statsSnap()
	all := append([]*transportSnap{gone}, open...)
	gone.labels = statsLabels{}
	live := 0
	for _, t := range open {
		if !t.disconnected {
			live++
		}
	}
	w.desc("zenoh_transports_opened", "Count of transports currently opened.", "", "gauge")
	w.sample("zenoh_transports_opened", "", strconv.Itoa(live))
	if len(links) > 0 {
		w.desc("zenoh_links_opened", "Count of transports currently opened.", "", "gauge")
		var ps []string
		for p := range links {
			ps = append(ps, p)
		}
		sort.Strings(ps)
		for _, p := range ps {
			w.sample("zenoh_links_opened", lbl("protocol", p), strconv.Itoa(links[p]))
		}
	}
	// tOf is nil for the closed transports together.
	tOf := func(t *transportSnap) *transportSnap {
		if t == gone {
			return nil
		}
		return t
	}
	for dir, d := range []string{"tx", "rx"} {
		action := []string{"sent", "received"}[dir]
		var bytes, tmsgs, nmsgs, payload, dropped []series
		for _, t := range all {
			// Per protocol: the transport's retired counters, then its links.
			seen := map[string]bool{}
			var ps []string
			for p := range t.retired {
				seen[p] = true
				ps = append(ps, p)
			}
			for _, l := range t.links {
				if !seen[l.proto] {
					seen[l.proto] = true
					ps = append(ps, l.proto)
				}
			}
			sort.Strings(ps)
			for _, p := range ps {
				pl := lbl("protocol", p)
				if c, ok := t.retired[p]; ok {
					bytes = append(bytes, series{labels: pl, t: tOf(t), v: c.Bytes[dir]})
					tmsgs = append(tmsgs, series{labels: pl, t: tOf(t), v: c.TransportMessages[dir]})
					nmsgs = appendNet(nmsgs, &c, dir, p, tOf(t), nil)
				}
				for i := range t.links {
					l := &t.links[i]
					if l.proto != p {
						continue
					}
					bytes = append(bytes, series{labels: pl, t: t, l: l, v: l.c.Bytes[dir]})
					tmsgs = append(tmsgs, series{labels: pl, t: t, l: l, v: l.c.TransportMessages[dir]})
					nmsgs = appendNet(nmsgs, &l.c, dir, p, t, l)
				}
			}
			var pks []payloadKey
			for k := range t.payload {
				if int(k.dir) == dir {
					pks = append(pks, k)
				}
			}
			sort.Slice(pks, func(i, j int) bool { return payloadLess(pks[i], pks[j]) })
			for _, k := range pks {
				h := t.payload[k]
				payload = append(payload, series{labels: payloadLabels(k), t: tOf(t), h: &h})
			}
			var dks []dropKey
			for k := range t.dropped {
				if int(k.dir) == dir {
					dks = append(dks, k)
				}
			}
			sort.Slice(dks, func(i, j int) bool {
				a, b := dks[i], dks[j]
				if a.reason != b.reason {
					return a.reason < b.reason
				}
				if a.kind != b.kind {
					return a.kind < b.kind
				}
				return a.prio < b.prio
			})
			for _, k := range dks {
				h := t.dropped[k]
				dropped = append(dropped, series{labels: lbl("priority", promPriorityNames[k.prio], "message", k.kind.String(),
					"protocol", k.proto, "reason", dropReasonNames[k.reason]), t: tOf(t), h: &h})
			}
		}
		w.family(d, "Count of transport messages bytes "+action, "bytes", false, opts, bytes)
		w.family(d+"_transport_message", "Count of transport messages "+action, "", false, opts, tmsgs)
		w.family(d+"_network_message", "Count of network messages "+action, "", false, opts, nmsgs)
		w.family(d+"_network_message_payload", "Histogram of network messages payload "+action, "bytes", true, opts, payload)
		w.family(d+"_network_message_dropped_payload", "Histogram of network messages payload dropped while "+action,
			"bytes", true, opts, dropped)
		if !opts.NoPerKey {
			w.perKeyFamily(d+"_network_message_payload_per_key", "Histogram of network messages payload "+action+" per key",
				opts, s.stats.filters, all, uint8(dir))
		}
	}
	w.b.WriteString("# EOF\n")
	return w.b.String()
}

// appendNet adds the non-zero network message counters of c.
func appendNet(ss []series, c *transport.LinkCounts, dir int, proto string, t *transportSnap, l *linkSnap) []series {
	for k := range transport.NumMessageKinds {
		for p := range 8 {
			for h := range 2 {
				if v := c.Network(dir, protocol.Priority(p), k, h); v > 0 {
					ss = append(ss, series{labels: lbl("priority", promPriorityNames[p], "message", k.String(),
						"shm", jsonBool(h == 1), "protocol", proto), t: t, l: l, v: v})
				}
			}
		}
	}
	return ss
}

// -----------------------------------------------------------------------------
// <root>?_stats
// -----------------------------------------------------------------------------

// jsonStats are the counters zenoh-rust adds to the admin JSON (merge_stats).
type jsonStats struct {
	bytes, tmsgs     [2]uint64
	nmsgs            [2][2]uint64 // net, shm
	nDropped         [2]uint64
	dsDropped        [2]uint64
	lpDropped        [2]uint64
	lpDroppedBytes   [2]uint64
	zMsgs, zBytes    [2][4][2]uint64 // dir, put/del/query/reply, user/admin
	link, transportS bool
}

func (j *jsonStats) addCounts(c *transport.LinkCounts) {
	for d := range 2 {
		j.bytes[d] += c.Bytes[d]
		j.tmsgs[d] += c.TransportMessages[d]
		j.nmsgs[d][0] += c.NetworkTotal(d, 0)
		j.nmsgs[d][1] += c.NetworkTotal(d, 1)
	}
}

func (j *jsonStats) addPayload(payload map[payloadKey]histogram) {
	for k, h := range payload {
		i := map[transport.MessageKind]int{transport.KindPut: 0, transport.KindDel: 1, transport.KindQuery: 2,
			transport.KindReply: 3, transport.KindReplyErr: 3}[k.kind]
		j.zMsgs[k.dir][i][k.space] += h.count()
		j.zBytes[k.dir][i][k.space] += h.sum
	}
}

func (j *jsonStats) addTransport(t *transportSnap) {
	for _, c := range t.retired {
		j.addCounts(&c)
	}
	for _, l := range t.links {
		j.addCounts(&l.c)
	}
	j.addPayload(t.payload)
	for k, h := range t.dropped {
		switch k.reason {
		case dropCongestion:
			if k.dir == transport.Tx {
				j.nDropped[0] += h.count()
			}
		case dropDownsampling:
			j.dsDropped[k.dir] += h.count()
		case dropLowPass:
			j.lpDropped[k.dir] += h.count()
			j.lpDroppedBytes[k.dir] += h.sum
		}
	}
}

func (j *jsonStats) json() string {
	var fs []jsonField
	num := func(v uint64) string { return strconv.FormatUint(v, 10) }
	pair := func(a, b string, x, y uint64) string { return jsonObj(jsonField{a, num(x)}, jsonField{b, num(y)}) }
	zNames := [4]string{"del", "put", "query", "reply"}
	zIdx := [4]int{1, 0, 2, 3}
	for d, dn := range []string{"rx", "tx"} {
		dir := 1 - d // rx first: sorted keys
		fs = append(fs, jsonField{dn + "_bytes", num(j.bytes[dir])})
		if j.transportS {
			fs = append(fs, jsonField{dn + "_downsampler_dropped_msgs", num(j.dsDropped[dir])},
				jsonField{dn + "_low_pass_dropped_bytes", num(j.lpDroppedBytes[dir])},
				jsonField{dn + "_low_pass_dropped_msgs", num(j.lpDropped[dir])})
		}
		fs = append(fs, jsonField{dn + "_n_dropped", num(j.nDropped[dir])},
			jsonField{dn + "_n_msgs", pair("net", "shm", j.nmsgs[dir][0], j.nmsgs[dir][1])},
			jsonField{dn + "_t_msgs", num(j.tmsgs[dir])})
		if j.transportS {
			for n, name := range zNames {
				i := zIdx[n]
				fs = append(fs,
					jsonField{dn + "_z_" + name + "_msgs", pair("admin", "user", j.zMsgs[dir][i][1], j.zMsgs[dir][i][0])},
					jsonField{dn + "_z_" + name + "_pl_bytes", pair("admin", "user", j.zBytes[dir][i][1], j.zBytes[dir][i][0])})
			}
		}
	}
	return jsonObj(fs...)
}

// payloadJSON is the payload part of the stats only (stats_filtered).
func payloadJSON(payload map[payloadKey]histogram) string {
	var j jsonStats
	j.addPayload(payload)
	var fs []jsonField
	num := func(v uint64) string { return strconv.FormatUint(v, 10) }
	pair := func(x, y uint64) string { return jsonObj(jsonField{"admin", num(x)}, jsonField{"user", num(y)}) }
	zNames := [4]string{"del", "put", "query", "reply"}
	zIdx := [4]int{1, 0, 2, 3}
	for d, dn := range []string{"rx", "tx"} {
		dir := 1 - d
		for n, name := range zNames {
			i := zIdx[n]
			fs = append(fs, jsonField{dn + "_z_" + name + "_msgs", pair(j.zMsgs[dir][i][1], j.zMsgs[dir][i][0])},
				jsonField{dn + "_z_" + name + "_pl_bytes", pair(j.zBytes[dir][i][1], j.zBytes[dir][i][0])})
		}
	}
	return jsonObj(fs...)
}

// filteredJSON is a stats_filtered list: per filter, its payload stats.
func filteredJSON(filters []string, ts ...*transportSnap) string {
	var out []string
	for i, f := range filters {
		p := map[payloadKey]histogram{}
		for _, t := range ts {
			for k, h := range t.perKey {
				if k.filter == i {
					a := p[k.payloadKey]
					a.add(&h)
					p[k.payloadKey] = a
				}
			}
		}
		out = append(out, jsonObj(jsonField{"key", jsonStr(f)}, jsonField{"stats", payloadJSON(p)}))
	}
	return jsonArr(out)
}

// adminStats are the statistics for the admin JSON: the root's, and by
// peer ZID each open face's (transport and links by src/dst).
type adminStats struct {
	root, rootFiltered string
	faces              map[string]*transportSnap
}

func (s *Session) adminStats() *adminStats {
	open, gone, _ := s.statsSnap()
	a := &adminStats{faces: map[string]*transportSnap{}}
	root := jsonStats{transportS: true}
	root.addTransport(gone)
	for _, t := range open {
		root.addTransport(t)
		if !t.disconnected && t.labels.zid != "" {
			a.faces[t.labels.zid] = t
		}
	}
	a.root = root.json()
	if len(s.stats.filters) > 0 {
		a.rootFiltered = filteredJSON(s.stats.filters, append(open, gone)...)
	}
	return a
}

// sessionFields are a face's "stats" (and "stats_filtered") fields.
func (a *adminStats) sessionFields(filters []string, zid string) []jsonField {
	t := a.faces[zid]
	if t == nil {
		t = &transportSnap{}
	}
	j := jsonStats{transportS: true}
	j.addTransport(t)
	fs := []jsonField{{"stats", j.json()}}
	if len(filters) > 0 {
		fs = append(fs, jsonField{"stats_filtered", filteredJSON(filters, t)})
	}
	return fs
}

// linkField is a link's "stats" field.
func (a *adminStats) linkField(zid, src, dst string) jsonField {
	var j jsonStats
	if t := a.faces[zid]; t != nil {
		for _, l := range t.links {
			if l.src == src && l.dst == dst {
				j.addCounts(&l.c)
			}
		}
	}
	return jsonField{"stats", j.json()}
}
