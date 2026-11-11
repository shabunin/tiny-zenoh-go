package zenoh

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"os"
	"sort"
	"strings"
	"testing"
	"time"
)

// adminDump returns the replies to an admin space query: key -> payload.
func adminDump(t *testing.T, s *Session, sel string) map[string]string {
	t.Helper()
	replies, err := collect(s.Get(sel, WithTimeout(3*time.Second)))
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	for _, r := range replies {
		if r.Sample == nil {
			t.Fatalf("%s: error reply %+v", sel, r)
		}
		out[r.Sample.KeyExpr] = string(r.Sample.Payload)
	}
	return out
}

// adminRouters runs fn against a Go router and, with ZENOH_PY, a Rust one
// with its admin space enabled.
func adminRouters(t *testing.T, fn func(t *testing.T, router string)) {
	t.Run("go", func(t *testing.T) {
		r, err := Open(Config{Mode: ModeRouter, Listen: []string{"tcp/127.0.0.1:0"},
			Scouting: ScoutingConfig{Disable: true}})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = r.Close() })
		fn(t, r.Locators()[0])
	})
	t.Run("rust", func(t *testing.T) {
		l := freeLocator(t)
		startRust(t, jsonConf(map[string]any{"mode": "router", "listen/endpoints": []string{l},
			"scouting/multicast/enabled": false, "adminspace/enabled": true}), true, "token", "admrouter/x", "60")
		fn(t, l)
	})
}

// adminView is what a router's admin space shows about one client's
// entities, with ZIDs replaced by R (router) and C (client), comparable
// across implementations.
type adminView struct {
	entities  map[string]string // key -> sources
	rootKeys  []string
	session   map[string]any // the client's entry, links counted
	linkstate map[string]string
	metrics   string
}

func adminCollect(t *testing.T, router string) adminView {
	key := "admtest"
	c := icptClient(t, router)
	rz := c.RoutersZID()[0].String()
	cz := c.ZID().String()
	norm := func(s string) string {
		return strings.ReplaceAll(strings.ReplaceAll(s, rz, "R"), cz, "C")
	}
	sub, err := c.DeclareSubscriber(key+"/sub", func(Sample) {})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Undeclare()
	q, err := c.DeclareQueryable(key+"/q/*", func(*Query) {})
	if err != nil {
		t.Fatal(err)
	}
	defer q.Undeclare()
	tok, err := c.Liveliness().DeclareToken(key + "/tok")
	if err != nil {
		t.Fatal(err)
	}
	defer tok.Undeclare()
	pub, err := c.DeclarePublisher(key + "/pub")
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Undeclare()
	qr, err := c.DeclareQuerier(key + "/q/x")
	if err != nil {
		t.Fatal(err)
	}
	defer qr.Undeclare()
	settle()

	v := adminView{entities: map[string]string{}, linkstate: map[string]string{}}
	root := "@/" + rz + "/router"
	for k, p := range adminDump(t, c, root+"/*/"+key+"/**") {
		v.entities[norm(k)] = norm(p)
	}
	for k, p := range adminDump(t, c, root+"/linkstate/*") {
		v.linkstate[norm(k)] = norm(p)
	}
	m := adminDump(t, c, root+"/metrics?compression=false")[root+"/metrics"]
	v.metrics = norm(m[:strings.Index(m, "version=")])

	info := adminDump(t, c, root)[root]
	var obj map[string]any
	if err := json.Unmarshal([]byte(info), &obj); err != nil {
		t.Fatalf("root: %v\n%s", err, info)
	}
	for k := range obj {
		v.rootKeys = append(v.rootKeys, k)
	}
	sort.Strings(v.rootKeys)
	if obj["zid"] != rz {
		t.Errorf("zid %v", obj["zid"])
	}
	for _, s := range obj["sessions"].([]any) {
		e := s.(map[string]any)
		if e["peer"] != cz {
			continue
		}
		links := e["links"].([]any)
		e["links"] = len(links)
		e["peer"] = "C"
		v.session = e
		l := links[0].(map[string]any)
		if !strings.HasPrefix(l["src"].(string), "tcp/") || !strings.HasPrefix(l["dst"].(string), "tcp/") {
			t.Errorf("link %v", l)
		}
	}
	// Metrics come gzipped by default.
	replies, _ := collect(c.Get(root+"/metrics", WithTimeout(3*time.Second)))
	if len(replies) != 1 || !strings.HasSuffix(replies[0].Sample.Encoding.String(), "content-encoding=gzip") {
		t.Fatalf("metrics replies %+v", replies)
	}
	zr, err := gzip.NewReader(bytes.NewReader(replies[0].Sample.Payload))
	if err != nil {
		t.Fatal(err)
	}
	if b, _ := io.ReadAll(zr); string(b) != m {
		t.Errorf("gunzipped metrics %q, want %q", b, m)
	}
	return v
}

func TestAdminSpace(t *testing.T) {
	views := map[string]adminView{}
	adminRouters(t, func(t *testing.T, router string) {
		v := adminCollect(t, router)
		views[t.Name()] = v
		want := map[string]string{
			"@/R/router/subscriber/admtest/sub": `{"routers":["R"],"peers":[],"clients":["C"]}`,
			"@/R/router/queryable/admtest/q/*":  `{"routers":["R"],"peers":[],"clients":["C"]}`,
			"@/R/router/token/admtest/tok":      `{"routers":["R"],"peers":[],"clients":["C"]}`,
			"@/R/router/publisher/admtest/pub":  `{"routers":[],"peers":[],"clients":["C"]}`,
			"@/R/router/querier/admtest/q/x":    `{"routers":[],"peers":[],"clients":["C"]}`,
		}
		for k, p := range want {
			if v.entities[k] != p {
				t.Errorf("%s: %q, want %q", k, v.entities[k], p)
			}
		}
		if len(v.entities) != len(want) {
			t.Errorf("entities %v", v.entities)
		}
		if strings.Join(v.rootKeys, ",") != "locators,metadata,plugins,sessions,version,zid" {
			t.Errorf("root keys %v", v.rootKeys)
		}
		if v.session == nil || v.session["region"] != "south:0:client" || v.session["whatami"] != "client" ||
			v.session["links"] != 1 || v.session["shm"] != false || v.session["weight"] != nil {
			t.Errorf("session %v", v.session)
		}
		self := "graph {\n    0 [ label = \"R\" ]\n}\n"
		if v.linkstate["@/R/router/linkstate/north"] != self || v.linkstate["@/R/router/linkstate/south:0:peer"] != self ||
			len(v.linkstate) != 2 {
			t.Errorf("linkstate %q", v.linkstate)
		}
		if !strings.HasPrefix(v.metrics, "# HELP zenoh_build Zenoh build version.\n# TYPE zenoh_build info\n"+
			"zenoh_build_info{local_id=\"R\",local_whatami=\"router\",") {
			t.Errorf("metrics %q", v.metrics)
		}
	})
	if g, r := views["TestAdminSpace/go"], views["TestAdminSpace/rust"]; r.entities != nil {
		if gs, rs := jsonOf(g), jsonOf(r); gs != rs {
			t.Errorf("go and rust differ:\n go  %s\n rust %s", gs, rs)
		}
	}
}

func jsonOf(v adminView) string {
	b, _ := json.Marshal([]any{v.entities, v.rootKeys, v.session, v.linkstate, v.metrics})
	return string(b)
}

// A Go router meshed with a Rust router: both show the same two-node graph
// (same edge weight: the SipHash-derived jitter matches) and next hops.
func TestAdminSpaceMesh(t *testing.T) {
	if os.Getenv("ZENOH_PY") == "" {
		t.Skip("set ZENOH_PY")
	}
	l := freeLocator(t)
	startRust(t, jsonConf(map[string]any{"mode": "router", "listen/endpoints": []string{l},
		"scouting/multicast/enabled": false, "adminspace/enabled": true}), true, "token", "admrouter/y", "60")
	g := openRouterTo(t, l)
	waitFor(t, "routers linked", func() bool { return len(g.RoutersZID()) == 1 })
	rz, gz := g.RoutersZID()[0].String(), g.ZID().String()
	time.Sleep(500 * time.Millisecond)
	norm := func(s string) string {
		return strings.ReplaceAll(strings.ReplaceAll(s, rz, "B"), gz, "A")
	}
	goGraph := norm(adminDump(t, g, "@/"+gz+"/router/linkstate/north")["@/"+gz+"/router/linkstate/north"])
	rustGraph := norm(adminDump(t, g, "@/"+rz+"/router/linkstate/north")["@/"+rz+"/router/linkstate/north"])
	edge := func(graph string) string {
		for _, line := range strings.Split(graph, "\n") {
			if strings.Contains(line, "--") {
				return line[strings.Index(line, "["):]
			}
		}
		return ""
	}
	if !strings.Contains(goGraph, `0 [ label = "A" ]`) || !strings.Contains(goGraph, `[ label = "B" ]`) ||
		edge(goGraph) == "" || edge(goGraph) != edge(rustGraph) {
		t.Errorf("graphs differ:\n go   %q\n rust %q", goGraph, rustGraph)
	}
	succ := func(z string) []string {
		var out []string
		for k, v := range adminDump(t, g, "@/"+z+"/router/route/successor/**") {
			out = append(out, norm(k)+"="+norm(v))
		}
		sort.Strings(out)
		return out
	}
	goS, rustS := succ(gz), succ(rz)
	if len(goS) == 0 || len(goS) != len(rustS) {
		t.Errorf("successors:\n go   %v\n rust %v", goS, rustS)
	}
	// Our weight report for the router link matches theirs.
	weight := func(z, peer string) any {
		var obj map[string]any
		_ = json.Unmarshal([]byte(adminDump(t, g, "@/"+z+"/router")["@/"+z+"/router"]), &obj)
		for _, s := range obj["sessions"].([]any) {
			if e := s.(map[string]any); e["peer"] == peer {
				return e["weight"]
			}
		}
		return "missing"
	}
	gw, rw := weight(gz, rz), weight(rz, gz)
	if gb, _ := json.Marshal(gw); string(gb) == "null" || string(gb) != mustJSON(rw) {
		t.Errorf("weights: go %s rust %s", gb, mustJSON(rw))
	}
}

func mustJSON(v any) string { b, _ := json.Marshal(v); return string(b) }

// The session admin space: visible to the session only, with events.
func TestSessionAdminSpace(t *testing.T) {
	a := openRegionNode(t, regionOpts{mode: ModePeer, listen: true})
	own := "@/" + a.ZID().String() + "/session"
	ev := newSink()
	sub, err := a.DeclareSubscriber(own+"/**", ev.handler)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Undeclare()
	b := openRegionNode(t, regionOpts{mode: ModePeer}, loc(a))
	waitFor(t, "peers linked", func() bool { return len(a.PeersZID()) == 1 })
	tk := own + "/transport/unicast/" + b.ZID().String()

	got := ev.wait(t, 2)
	if got[0].KeyExpr != tk || !strings.Contains(string(got[0].Payload), `"whatami":"peer","is_qos":true`) ||
		!strings.HasPrefix(got[1].KeyExpr, tk+"/link/") {
		t.Fatalf("events %q %q", got[0].KeyExpr, got[1].KeyExpr)
	}
	dump := adminDump(t, a, own+"/**")
	if len(dump) != 2 || dump[tk] != string(got[0].Payload) || dump[got[1].KeyExpr] != string(got[1].Payload) {
		t.Fatalf("dump %v", dump)
	}
	var lk map[string]any
	if err := json.Unmarshal(got[1].Payload, &lk); err != nil {
		t.Fatal(err)
	}
	if lk["is_streamed"] != true || lk["reliability"] != "Reliable" || lk["mtu"] != float64(65480) ||
		!strings.HasPrefix(lk["src"].(string), "tcp/127.0.0.1:") {
		t.Errorf("link %s", got[1].Payload)
	}
	// History for advanced subscribers: the @adv form, with _anyke.
	adv := "@adv/pub/" + a.ZID().String() + "/_/_/@/" + own + "/**?_anyke"
	if n := len(adminDump(t, a, adv)); n != 2 {
		t.Errorf("@adv query: %d replies", n)
	}
	// Other sessions don't see it.
	if n := len(adminDump(t, b, own+"/**")); n != 0 {
		t.Errorf("remote session got %d replies", n)
	}
	_ = b.Close()
	got = ev.wait(t, 2)
	if got[2].Kind != SampleKindDelete || got[3].Kind != SampleKindDelete || got[3].KeyExpr != tk {
		t.Errorf("close events %v %q, %v %q", got[2].Kind, got[2].KeyExpr, got[3].Kind, got[3].KeyExpr)
	}
}

func TestAdminSpaceConfig(t *testing.T) {
	yes := true
	p, err := Open(Config{Mode: ModePeer, Listen: []string{"tcp/127.0.0.1:0"}, Scouting: ScoutingConfig{Disable: true},
		AdminSpace: AdminSpaceConfig{Enabled: &yes}, Metadata: []byte(`{"name":"strawberry"}`)})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	root := "@/" + p.ZID().String() + "/peer"
	d := adminDump(t, p, root)
	if !strings.Contains(d[root], `"metadata":{"name":"strawberry"}`) {
		t.Errorf("peer admin %v", d)
	}
	cl, err := Open(Config{Connect: p.Locators()})
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if n := len(adminDump(t, cl, "@/"+cl.ZID().String()+"/client")); n != 0 {
		t.Errorf("client admin space served by default (%d)", n)
	}
	r, err := Open(Config{Mode: ModeRouter, Listen: []string{"tcp/127.0.0.1:0"}, Scouting: ScoutingConfig{Disable: true},
		AdminSpace: AdminSpaceConfig{DenyRead: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if n := len(adminDump(t, r, "@/"+r.ZID().String()+"/router/**")); n != 0 {
		t.Errorf("read denied, got %d replies", n)
	}
}

// A link joining a live transport (multilink) shows up as a link event.
func TestSessionAdminSpaceMultiLink(t *testing.T) {
	a := openMLPeer(t, 2, []string{"tcp/127.0.0.1:0", "udp/127.0.0.1:0"})
	own := "@/" + a.ZID().String() + "/session"
	ev := newSink()
	sub, err := a.DeclareSubscriber(own+"/**", ev.handler)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Undeclare()
	b := openMLPeer(t, 2, nil, a.Locators()...)
	waitFor(t, "two links", func() bool { return links(a) == 2 && links(b) == 2 })
	got := ev.wait(t, 3)
	protos := map[string]bool{}
	for _, e := range got[1:] {
		var l map[string]any
		if err := json.Unmarshal(e.Payload, &l); err != nil || !strings.Contains(e.KeyExpr, "/link/") {
			t.Fatalf("event %q %s", e.KeyExpr, e.Payload)
		}
		protos[strings.SplitN(l["src"].(string), "/", 2)[0]+"/"+jsonOfAny(l["is_streamed"])] = true
	}
	if !protos["tcp/true"] || !protos["udp/false"] {
		t.Errorf("links %v", protos)
	}
	if n := len(adminDump(t, a, own+"/**")); n != 3 {
		t.Errorf("dump has %d entries", n)
	}
}

func jsonOfAny(v any) string { b, _ := json.Marshal(v); return string(b) }
