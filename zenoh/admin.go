package zenoh

import (
	"errors"
	"hash/fnv"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/shabunin/tiny-zenoh-go/keyexpr"
	"github.com/shabunin/tiny-zenoh-go/link"
	"github.com/shabunin/tiny-zenoh-go/protocol"
	"github.com/shabunin/tiny-zenoh-go/transport"
)

type transportLinkInfo = transport.LinkInfo

// Version is the library version reported in the admin space.
const Version = "v0.1.1"

// versionString is what the admin space reports as "version" (zenoh-rust:
// LONG_VERSION, "v1.10.2-<commit> built with rustc ...").
func versionString() string {
	return Version + "-tiny-zenoh-go built with " + runtime.Version()
}

// AdminSpaceConfig is zenoh-rust's "adminspace" section.
type AdminSpaceConfig struct {
	// Enabled serves the node's admin space, @/<zid>/<mode>/**. Unset, it
	// is served in router mode only (like zenohd; zenoh-rust's library
	// default is off).
	Enabled *bool
	// DenyRead answers no admin query (adminspace/permissions/read false).
	DenyRead bool
	// Write accepts configuration changes put on <root>/config/<key>
	// (adminspace/permissions/write): as in zenoh-rust, only keys under
	// plugins/ (others are refused), a PUT carrying the key's new JSON5
	// value, a DELETE removing it. OnConfig applies them.
	Write bool
	// OnConfig applies a configuration change accepted with Write: key is
	// the config path (e.g. plugins/storage_manager/storages/demo; its last
	// segment may be an array filter <field>=<value>, as zenoh-rust's
	// plugins/x/list/id=a), value the JSON5 text of a PUT or nil for a
	// DELETE. A non-nil error is reported to OnConfigError.
	OnConfig func(key string, value []byte) error
	// OnConfigError, if set, learns about refused or failed config writes
	// (zenoh-rust logs them).
	OnConfigError func(key string, err error)
}

// Admin space (zenoh-rust zenoh/src/net/runtime/adminspace.rs), served under
// @/<zid>/<router|peer|client> when enabled:
//
//	<root>                      JSON: zid, version, metadata, locators, sessions
//	<root>/metrics              OpenMetrics text (gzip unless ?compression=false)
//	<root>/linkstate/<region>   the region's link-state graph (Graphviz)
//	<root>/subscriber/<key>     who declared entities on <key>: JSON
//	<root>/queryable/<key>        {"routers": [...], "peers": [...], "clients": [...]}
//	<root>/token/<key>
//	<root>/publisher/<key>      (publishers and queriers: from their INTERESTs)
//	<root>/querier/<key>
//	<root>/route/successor/src/<zid>/dst/<zid>
//	                            next router from us on the tree of src toward dst
//
// and, in every session and visible to it only (zenoh-rust api/admin.rs):
//
//	@/<zid>/session/transport/unicast/<peer>              JSON transport info
//	@/<zid>/session/transport/unicast/<peer>/link/<id>    JSON link info
//
// published as PUT/DELETE samples when transports and links come and go.

func (s *Session) adminRoot() string {
	return "@/" + s.zid.String() + "/" + s.cfg.Mode.String()
}

func (s *Session) adminEnabled() bool {
	if e := s.cfg.AdminSpace.Enabled; e != nil {
		return *e
	}
	return s.cfg.Mode == ModeRouter
}

// startAdmin declares the admin space queryables: the node's (if enabled)
// and the session's.
func (s *Session) startAdmin() error {
	if s.adminEnabled() {
		k, err := validKey(s.adminRoot() + "/**")
		if err != nil {
			return err
		}
		if _, err = s.declareQueryable(k, s.onAdminQuery, true, nil); err != nil {
			return err
		}
		if s.cfg.AdminSpace.Write {
			if err = s.declareAdminConfig(); err != nil {
				return err
			}
		}
	}
	own := "@/" + s.zid.String() + "/session"
	adv := "@adv/pub/" + s.zid.String() + "/_/_/@/" + own
	for _, prefix := range []string{own, adv} {
		k, err := validKey(prefix + "/**")
		if err != nil {
			return err
		}
		match := prefix
		if _, err = s.declareQueryable(k, func(q *Query) { s.onSessionAdminQuery(q, match, own) }, true,
			[]QueryableOption{WithAllowedOrigin(LocalitySessionLocal)}); err != nil {
			return err
		}
	}
	return nil
}

// declareAdminConfig subscribes to <root>/config/** for config writes, as
// zenohd does (outside the namespace, like the rest of the admin space).
func (s *Session) declareAdminConfig() error {
	prefix := s.adminRoot() + "/config/"
	k, err := validKey(prefix + "**")
	if err != nil {
		return err
	}
	sub := &Subscriber{s: s, id: s.entityID(), key: k, stop: make(chan struct{})}
	sub.handler = func(smp Sample) { s.onAdminConfig(strings.TrimPrefix(smp.KeyExpr, prefix), smp) }
	s.mu.Lock()
	s.subs[sub.id] = sub
	s.subsChangedLocked()
	return s.declareLocalLocked(kindSub, k.String(), &protocol.Declare{QoS: protocol.QoSDeclare,
		Body: &protocol.DeclareSubscriber{ID: sub.id, WireExpr: protocol.WireExpr{Suffix: k.String()}}})
}

// errConfigKey refuses writes outside plugins/ (zenoh-rust
// ensure_config_key_is_dynamically_writable).
var errConfigKey = errors.New("zenoh: updating config is only supported for keys starting with `plugins/`")

func (s *Session) onAdminConfig(key string, smp Sample) {
	a := &s.cfg.AdminSpace
	var err error
	switch {
	case !strings.HasPrefix(key, "plugins/") || strings.ContainsAny(key, "*$"):
		err = errConfigKey
	case a.OnConfig == nil:
		err = errors.New("zenoh: no OnConfig to apply config writes")
	case smp.Kind == SampleKindDelete:
		err = a.OnConfig(key, nil)
	case !utf8.Valid(smp.Payload):
		err = errors.New("zenoh: config value is not UTF-8")
	default:
		v := smp.Payload
		if v == nil {
			v = []byte{}
		}
		err = a.OnConfig(key, v)
	}
	if err != nil && a.OnConfigError != nil {
		a.OnConfigError(key, err)
	}
}

// adminHandler answers the part of the admin space under its prefix.
type adminHandler struct {
	suffix string // under the root ("" for the root itself)
	glob   string // the keys it serves under suffix ("" for suffix only)
	fn     func(s *Session, q *Query, prefix string)
}

var adminHandlers = []adminHandler{
	{"", "", (*Session).adminLocal},
	{"metrics", "", (*Session).adminMetrics},
	{"linkstate", "*", (*Session).adminLinkState},
	{"subscriber", "**", func(s *Session, q *Query, p string) { s.adminEntities(q, p, kindSub) }},
	{"publisher", "**", func(s *Session, q *Query, p string) { s.adminInterests(q, p, protocol.InterestSubscribers) }},
	{"queryable", "**", func(s *Session, q *Query, p string) { s.adminEntities(q, p, kindQbl) }},
	{"querier", "**", func(s *Session, q *Query, p string) { s.adminInterests(q, p, protocol.InterestQueryables) }},
	{"token", "**", func(s *Session, q *Query, p string) { s.adminEntities(q, p, kindToken) }},
	{"route/successor", "**", (*Session).adminSuccessors},
}

func (s *Session) onAdminQuery(q *Query) {
	if s.cfg.AdminSpace.DenyRead {
		return
	}
	root := s.adminRoot()
	for _, h := range adminHandlers {
		prefix := root
		if h.suffix != "" {
			prefix += "/" + h.suffix
		}
		full := prefix
		if h.glob != "" {
			full += "/" + h.glob
		}
		if keyexpr.Intersect(q.KeyExpr(), full) {
			h.fn(s, q, prefix)
		}
	}
}

// adminReply replies on key if the query's key expression intersects it.
func adminReply(q *Query, key string, payload []byte, enc Encoding) {
	if keyexpr.Intersect(q.KeyExpr(), key) {
		_ = q.Reply(key, payload, WithEncoding(enc))
	}
}

// -----------------------------------------------------------------------------
// A small JSON writer (no encoding/json in the core: TinyGo)
// -----------------------------------------------------------------------------

// jsonObj and friends build JSON values as strings; objects take their
// fields already in zenoh-rust's (serde_json Map: sorted) order.
type jsonField struct{ k, v string }

func jsonObj(fields ...jsonField) string {
	var b strings.Builder
	b.WriteByte('{')
	for i, f := range fields {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(jsonStr(f.k))
		b.WriteByte(':')
		b.WriteString(f.v)
	}
	b.WriteByte('}')
	return b.String()
}

func jsonArr(vals []string) string { return "[" + strings.Join(vals, ",") + "]" }

func jsonStrs(vals []string) string {
	out := make([]string, len(vals))
	for i, v := range vals {
		out[i] = jsonStr(v)
	}
	return jsonArr(out)
}

func jsonStr(v string) string {
	var b strings.Builder
	b.WriteByte('"')
	for _, r := range v {
		switch {
		case r == '"' || r == '\\':
			b.WriteByte('\\')
			b.WriteRune(r)
		case r == '\n':
			b.WriteString(`\n`)
		case r == '\r':
			b.WriteString(`\r`)
		case r == '\t':
			b.WriteString(`\t`)
		case r < 0x20:
			b.WriteString(`\u00`)
			b.WriteByte("0123456789abcdef"[r>>4])
			b.WriteByte("0123456789abcdef"[r&0xf])
		default:
			b.WriteRune(r)
		}
	}
	b.WriteByte('"')
	return b.String()
}

func jsonBool(v bool) string {
	if v {
		return "true"
	}
	return "false"
}

// -----------------------------------------------------------------------------
// <root>
// -----------------------------------------------------------------------------

// regionName is a region as zenoh-rust prints it: north, south:<id>:<mode>.
func regionName(r Region) string {
	if !r.South {
		return "north"
	}
	return "south:" + strconv.Itoa(r.ID) + ":" + r.Mode.String()
}

// faceLinks lists the links of f with their locators.
func (f *face) adminLinks() []adminLink {
	var out []adminLink
	if f.tr == nil {
		return nil
	}
	f.s.mu.Lock()
	joined := make(map[transport.Link][2]string, len(f.joined))
	for l, m := range f.joined {
		joined[l] = m
	}
	f.s.mu.Unlock()
	for _, li := range f.tr.LinkInfos() {
		proto, dialed := f.proto, f.dialed
		if m, ok := joined[li.Link]; ok {
			proto, dialed = m[0], m[1]
		}
		src, dst := link.Endpoints(li.Link, proto)
		if src == "" {
			src = proto + "/"
		}
		if dst == "" {
			dst = dialed
			if dst == "" {
				dst = proto + "/"
			}
		}
		out = append(out, adminLink{info: li, src: src, dst: dst})
	}
	return out
}

func (s *Session) adminLocal(q *Query, prefix string) {
	if !keyexpr.Intersect(q.KeyExpr(), prefix) {
		return
	}
	// ?_stats (any value but false) adds the traffic statistics, as
	// zenoh-rust's merge_stats.
	var st *adminStats
	if v, ok := Param(q.Parameters(), "_stats"); ok && v != "false" && s.stats != nil {
		st = s.adminStats()
	}
	var sessions []string
	faces := s.faceList()
	sort.Slice(faces, func(i, j int) bool { return faces[i].zid.String() < faces[j].zid.String() })
	for _, f := range faces {
		var links []string
		for _, l := range f.adminLinks() {
			fs := []jsonField{{"dst", jsonStr(l.dst)}, {"src", jsonStr(l.src)}}
			if st != nil {
				fs = append(fs, st.linkField(f.zid.String(), l.src, l.dst))
			}
			links = append(links, jsonObj(fs...))
		}
		fs := []jsonField{
			{"links", jsonArr(links)},
			{"peer", jsonStr(f.zid.String())},
			{"region", jsonStr(regionName(f.region))},
			{"shm", jsonBool(f.shm != nil)},
		}
		if st != nil {
			fs = append(fs, st.sessionFields(s.stats.filters, f.zid.String())...)
		}
		fs = append(fs, jsonField{"weight", s.adminWeight(f)}, jsonField{"whatami", jsonStr(f.whatami.String())})
		sessions = append(sessions, jsonObj(fs...))
	}
	meta := "null"
	if len(s.cfg.Metadata) > 0 {
		meta = string(s.cfg.Metadata)
	}
	fs := []jsonField{
		{"locators", jsonStrs(s.Locators())},
		{"metadata", meta},
		{"plugins", "null"},
		{"sessions", jsonArr(sessions)},
	}
	if st != nil {
		fs = append(fs, jsonField{"stats", st.root})
		if st.rootFiltered != "" {
			fs = append(fs, jsonField{"stats_filtered", st.rootFiltered})
		}
	}
	fs = append(fs, jsonField{"version", jsonStr(versionString())}, jsonField{"zid", jsonStr(s.zid.String())})
	body := jsonObj(fs...)
	adminReply(q, prefix, []byte(body), EncodingApplicationJSON)
}

// adminWeight is a router link's weights (zenoh-rust links_info), null for
// other faces.
func (s *Session) adminWeight(f *face) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, m := range s.meshes {
		if m.net.links[f] == nil {
			continue
		}
		j := m.net.idx(f.zid)
		if j < 0 {
			return "null"
		}
		w := func(a, b int) string {
			if v := m.net.nodes[a].links[zkey(m.net.nodes[b].zid)]; v != 0 {
				return strconv.Itoa(int(v))
			}
			return "null"
		}
		return jsonObj(
			jsonField{"actual_weight", strconv.Itoa(int(uint16(m.net.edgeWeight(0, j))))},
			jsonField{"dst_weight", w(j, 0)},
			jsonField{"src_weight", w(0, j)},
		)
	}
	return "null"
}

// -----------------------------------------------------------------------------
// <root>/metrics
// -----------------------------------------------------------------------------

const metricsEncoding = "application/openmetrics-text; version=1.0.0; charset=utf-8"

func (s *Session) adminMetrics(q *Query, prefix string) {
	if !keyexpr.Intersect(q.KeyExpr(), prefix) {
		return
	}
	params := q.Parameters()
	is := func(k, v string) bool { x, _ := Param(params, k); return x == v }
	opts := MetricsOptions{NoPerTransport: is("per_transport", "false"), NoPerLink: is("per_link", "false"),
		NoPerKey: is("per_key", "false"), Disconnected: is("disconnected", "true")}
	m := s.metrics(opts)
	if is("descriptors", "false") {
		var b strings.Builder
		for _, l := range strings.SplitAfter(m, "\n") {
			if l != "" && !strings.HasPrefix(l, "#") {
				b.WriteString(l)
			}
		}
		b.WriteString("# EOF\n")
		m = b.String()
	}
	payload, enc := []byte(m), metricsEncoding
	if v, _ := Param(params, "compression"); v != "false" {
		if z, ok := gzipBytes(payload); ok {
			payload, enc = z, enc+";content-encoding=gzip"
		}
	}
	_ = q.Reply(prefix, payload, WithEncoding(ParseEncoding(enc)))
}

// -----------------------------------------------------------------------------
// <root>/linkstate/<region>
// -----------------------------------------------------------------------------

// adminGraph is one region's link-state graph.
type adminGraph struct {
	region Region
	dot    string
}

func (s *Session) adminLinkState(q *Query, prefix string) {
	s.mu.Lock()
	var graphs []adminGraph
	switch {
	case s.cfg.Mode == ModeRouter:
		for r, m := range s.meshes {
			graphs = append(graphs, adminGraph{r, m.net.dot()})
		}
	case s.gossipNet != nil:
		graphs = append(graphs, adminGraph{North, s.gossipNet.dot()})
	case s.gossip != nil:
		graphs = append(graphs, adminGraph{North, s.gossip.dot()})
	}
	for r, n := range s.southNets {
		graphs = append(graphs, adminGraph{r, n.dot()})
	}
	if s.cfg.Mode == ModeRouter && s.cfg.Gateway.South == nil {
		// zenoh's "auto" preset always has a south peer region.
		r := defaultSouth(protocol.Peer)
		if s.southNets[r] == nil {
			graphs = append(graphs, adminGraph{r, "graph {\n    0 [ label = \"" + s.zid.String() + "\" ]\n}\n"})
		}
	}
	s.mu.Unlock()
	sort.Slice(graphs, func(i, j int) bool { return regionName(graphs[i].region) < regionName(graphs[j].region) })
	for _, g := range graphs {
		adminReply(q, prefix+"/"+regionName(g.region), []byte(g.dot), EncodingTextPlain)
	}
}

// dot prints the graph like zenoh-rust (petgraph's Dot of an undirected
// graph of ZIDs weighted by link cost).
func (n *lsNetwork) dot() string {
	var b strings.Builder
	b.WriteString("graph {\n")
	for i, nd := range n.nodes {
		if nd != nil {
			b.WriteString("    " + strconv.Itoa(i) + " [ label = \"" + nd.zid.String() + "\" ]\n")
		}
	}
	for i, a := range n.nodes {
		if a == nil {
			continue
		}
		for j := i + 1; j < len(n.nodes); j++ {
			bn := n.nodes[j]
			if bn == nil {
				continue
			}
			_, ab := a.links[zkey(bn.zid)]
			_, ba := bn.links[zkey(a.zid)]
			direct := i == 0 && n.isNeighbour(bn.zid)
			if (ab && ba) || direct {
				b.WriteString("    " + strconv.Itoa(i) + " -- " + strconv.Itoa(j) +
					" [ label = \"" + rustFloat(n.edgeWeight(i, j)) + "\" ]\n")
			}
		}
	}
	b.WriteString("}\n")
	return b.String()
}

// dot prints a (non-multihop) gossip graph like zenoh-rust.
func (g *gossipState) dot() string {
	var b strings.Builder
	b.WriteString("graph {\n")
	for i, nd := range g.nodes {
		if nd != nil {
			b.WriteString("    " + strconv.Itoa(i) + " [ label = \"" + nd.zid.String() + "\" ]\n")
		}
	}
	b.WriteString("}\n")
	return b.String()
}

// rustFloat formats f like Rust's Debug for f64 (shortest form, ".0" for
// integers).
func rustFloat(f float64) string {
	v := strconv.FormatFloat(f, 'f', -1, 64)
	if !strings.ContainsAny(v, ".eEN") {
		v += ".0"
	}
	return v
}

// -----------------------------------------------------------------------------
// <root>/{subscriber,queryable,token,publisher,querier}/<key>
// -----------------------------------------------------------------------------

// adminSources is who declared an entity on a key (zenoh-rust Sources).
type adminSources [3]map[string]bool // routers, peers, clients

func (a *adminSources) add(w WhatAmI, z ZID) {
	i := 2
	switch w {
	case protocol.Router:
		i = 0
	case protocol.Peer:
		i = 1
	}
	if a[i] == nil {
		a[i] = map[string]bool{}
	}
	a[i][z.String()] = true
}

func (a *adminSources) json() string {
	list := func(m map[string]bool) string {
		var zs []string
		for z := range m {
			zs = append(zs, z)
		}
		sort.Strings(zs)
		return jsonStrs(zs)
	}
	// A struct in zenoh-rust: fields in declaration order.
	return `{"routers":` + list(a[0]) + `,"peers":` + list(a[1]) + `,"clients":` + list(a[2]) + `}`
}

func adminSendSources(q *Query, prefix string, srcs map[string]*adminSources) {
	keys := make([]string, 0, len(srcs))
	for k := range srcs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		adminReply(q, prefix+"/"+k, []byte(srcs[k].json()), EncodingApplicationJSON)
	}
}

// adminEntities lists the subscribers, queryables or tokens of other nodes
// we know about, with who declared them. Entities from a south region also
// list us (zenoh-rust add_north_source): we stand for them in our north.
func (s *Session) adminEntities(q *Query, prefix string, k declKind) {
	srcs := map[string]*adminSources{}
	get := func(key string) *adminSources {
		a := srcs[key]
		if a == nil {
			a = &adminSources{}
			srcs[key] = a
		}
		return a
	}
	self := s.cfg.Mode.whatAmI()
	s.mu.Lock()
	for _, f := range s.faces {
		if !f.live || f.mesh {
			continue
		}
		var keys []string
		switch k {
		case kindSub:
			for _, key := range f.subs {
				keys = append(keys, key)
			}
		case kindQbl:
			for _, qb := range f.qbls {
				keys = append(keys, qb.key)
			}
		case kindToken:
			for _, key := range f.tokens {
				keys = append(keys, key)
			}
		}
		for _, key := range keys {
			a := get(key)
			a.add(f.whatami, f.zid)
			if f.region.South {
				a.add(self, s.zid)
			}
		}
	}
	for r, m := range s.meshes {
		for key, set := range m.remote[k] {
			for router := range set {
				z, err := protocol.NewZID([]byte(router))
				if err != nil || z.Equal(s.zid) {
					continue
				}
				a := get(key)
				a.add(protocol.Router, z)
				if r.South {
					a.add(self, s.zid)
				}
			}
		}
	}
	s.mu.Unlock()
	adminSendSources(q, prefix, srcs)
}

// adminInterests lists the publishers (INTERESTs in subscribers) or
// queriers (in queryables) of the nodes south of us, from the future
// interests they sent us.
func (s *Session) adminInterests(q *Query, prefix string, opt protocol.InterestOptions) {
	srcs := map[string]*adminSources{}
	s.mu.Lock()
	for _, f := range s.faces {
		if !f.live || f.router == nil {
			continue
		}
		for _, in := range f.router.interests {
			if in.opts&opt == 0 || in.key == "" || in.mode == protocol.InterestCurrent {
				continue
			}
			a := srcs[in.key]
			if a == nil {
				a = &adminSources{}
				srcs[in.key] = a
			}
			a.add(f.whatami, f.zid)
		}
	}
	s.mu.Unlock()
	adminSendSources(q, prefix, srcs)
}

// -----------------------------------------------------------------------------
// <root>/route/successor/src/<zid>/dst/<zid>
// -----------------------------------------------------------------------------

func (s *Session) adminSuccessors(q *Query, prefix string) {
	type entry struct{ key, succ string }
	var out []entry
	s.mu.Lock()
	for _, m := range s.meshes {
		n := m.net
		for src, t := range n.trees {
			if src >= len(n.nodes) || n.nodes[src] == nil {
				continue
			}
			for dst, dir := range t.directions {
				if dir < 0 || n.nodes[dst] == nil || n.nodes[dir] == nil {
					continue
				}
				out = append(out, entry{prefix + "/src/" + n.nodes[src].zid.String() + "/dst/" + n.nodes[dst].zid.String(),
					n.nodes[dir].zid.String()})
			}
		}
	}
	s.mu.Unlock()
	sort.Slice(out, func(i, j int) bool { return out[i].key < out[j].key })
	for _, e := range out {
		adminReply(q, e.key, []byte(jsonStr(e.succ)), EncodingApplicationJSON)
	}
}

// -----------------------------------------------------------------------------
// @/<zid>/session/** (session-local)
// -----------------------------------------------------------------------------

// adminLink is a link as the session admin space shows it.
type adminLink struct {
	info     transportLinkInfo
	src, dst string
}

// id is the link's key chunk: a hash of its locators (zenoh-rust hashes the
// link description).
func (l adminLink) id() string {
	h := fnv.New64a()
	h.Write([]byte(l.src))
	h.Write([]byte{0})
	h.Write([]byte(l.dst))
	return strconv.FormatUint(h.Sum64(), 10)
}

var rustPriorityNames = [8]string{"Control", "RealTime", "InteractiveHigh", "InteractiveLow",
	"DataHigh", "Data", "DataLow", "Background"}

func (f *face) adminTransportJSON() string {
	qos := f.tr != nil && f.tr.QoS()
	return jsonObj(
		jsonField{"zid", jsonStr(f.zid.String())},
		jsonField{"whatami", jsonStr(f.whatami.String())},
		jsonField{"is_qos", jsonBool(qos)},
		jsonField{"is_shm", jsonBool(f.shm != nil)},
	)
}

func (f *face) adminLinkJSON(l adminLink) string { return linkJSON(f.linkOf(l)) }

// linkJSON is a link as zenoh-rust's admin space shows it.
func linkJSON(k Link) string {
	auth, group := "null", "null"
	if k.AuthIdentifier != "" {
		auth = jsonStr(k.AuthIdentifier)
	}
	if k.Group != "" {
		group = jsonStr(k.Group)
	}
	fields := []jsonField{
		{"src", jsonStr(k.Src)},
		{"dst", jsonStr(k.Dst)},
		{"group", group},
		{"mtu", strconv.Itoa(k.MTU)},
		{"is_streamed", jsonBool(k.Streamed)},
		{"interfaces", jsonStrs(k.Interfaces)},
		{"auth_identifier", auth},
	}
	if p := k.Priorities; p != nil {
		fields = append(fields, jsonField{"priorities", jsonObj(
			jsonField{"start", jsonStr(rustPriorityNames[p.Start&7])}, jsonField{"end", jsonStr(rustPriorityNames[p.End&7])})})
	}
	rel := "BestEffort"
	if k.Reliability == Reliable {
		rel = "Reliable"
	}
	fields = append(fields, jsonField{"reliability", jsonStr(rel)})
	return jsonObj(fields...)
}

func (s *Session) onSessionAdminQuery(q *Query, match, own string) {
	for _, f := range s.faceList() {
		tk := "/transport/unicast/" + f.zid.String()
		if keyexpr.Intersect(q.KeyExpr(), match+tk) {
			_ = q.Reply(own+tk, []byte(f.adminTransportJSON()), WithEncoding(EncodingApplicationJSON))
		}
		for _, l := range f.adminLinks() {
			lk := tk + "/link/" + l.id()
			if keyexpr.Intersect(q.KeyExpr(), match+lk) {
				_ = q.Reply(own+lk, []byte(f.adminLinkJSON(l)), WithEncoding(EncodingApplicationJSON))
			}
		}
	}
}

// adminWatched reports whether one of the session's subscribers would get
// admin events under key (its payloads are worth building).
func (s *Session) adminWatched(key string) bool {
	full := "@/" + s.zid.String() + "/session" + key
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, sub := range s.subs {
		if sub.origin.allows(true) && keyexpr.Intersect(sub.key.String(), full) {
			return true
		}
	}
	return false
}

// adminEvent publishes a transport or link coming (payload) or going (nil
// payload func) to the session's own subscribers.
func (s *Session) adminEvent(key string, payload func() string) {
	smp := Sample{KeyExpr: "@/" + s.zid.String() + "/session" + key, Encoding: EncodingApplicationJSON,
		Kind: SampleKindDelete}
	if payload != nil {
		smp.Kind, smp.Payload = SampleKindPut, []byte(payload())
	}
	s.dispatchSample(smp, true)
}

// adminFaceUp publishes a new transport and its links.
func (s *Session) adminFaceUp(f *face) {
	tk := "/transport/unicast/" + f.zid.String()
	links := f.adminLinks()
	s.mu.Lock()
	f.published = append([]adminLink(nil), links...)
	s.mu.Unlock()
	s.statsLinks(f, nil, links)
	if h := s.evHooks.Load(); h != nil && f.tr != nil {
		h.faceUp(f, links)
	}
	if !s.adminWatched(tk + "/**") {
		return
	}
	s.adminEvent(tk, f.adminTransportJSON)
	for _, l := range links {
		s.adminEvent(tk+"/link/"+l.id(), func() string { return f.adminLinkJSON(l) })
	}
}

// adminLinksChanged publishes the links f gained and lost.
func (s *Session) adminLinksChanged(f *face) {
	tk := "/transport/unicast/" + f.zid.String()
	now := f.adminLinks()
	s.mu.Lock()
	if !f.live {
		s.mu.Unlock()
		return
	}
	old := f.published
	f.published = append([]adminLink(nil), now...)
	s.mu.Unlock()
	s.statsLinks(f, old, now)
	if h := s.evHooks.Load(); h != nil {
		h.faceLinks(f, now)
	}
	if !s.adminWatched(tk + "/link/*") {
		return
	}
	has := func(ls []adminLink, id string) bool {
		for _, l := range ls {
			if l.id() == id {
				return true
			}
		}
		return false
	}
	for _, l := range old {
		if !has(now, l.id()) {
			s.adminEvent(tk+"/link/"+l.id(), nil)
		}
	}
	for _, l := range now {
		if !has(old, l.id()) {
			s.adminEvent(tk+"/link/"+l.id(), func() string { return f.adminLinkJSON(l) })
		}
	}
}

// adminFaceDown publishes the end of a transport and its links.
func (s *Session) adminFaceDown(f *face, links []adminLink) {
	s.statsDown(f, links)
	if h := s.evHooks.Load(); h != nil {
		h.faceDown(f)
	}
	tk := "/transport/unicast/" + f.zid.String()
	if !s.adminWatched(tk + "/**") {
		return
	}
	for _, l := range links {
		s.adminEvent(tk+"/link/"+l.id(), nil)
	}
	s.adminEvent(tk, nil)
}
