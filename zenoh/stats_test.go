package zenoh

import (
	"bufio"
	"encoding/json"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"
)

func statsRouter(t *testing.T, cfg Config) *Session {
	t.Helper()
	cfg.Mode = ModeRouter
	cfg.Listen = []string{"tcp/127.0.0.1:0"}
	cfg.Scouting = ScoutingConfig{Disable: true}
	on := true
	cfg.Stats.Enabled = &on
	r, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	return r
}

// statsTraffic sends known traffic between two clients of a router: 10
// PUTs (0..90 bytes), one of 5000 bytes, a DELETE and a query answered
// with 100 bytes.
func statsTraffic(t *testing.T, a, b *Session, key string) {
	t.Helper()
	sink := newSink()
	sub, err := b.DeclareSubscriber(key+"/**", sink.handler)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Undeclare()
	q, err := b.DeclareQueryable(key+"/q", func(q *Query) { _ = q.Reply(key+"/q", make([]byte, 100)) })
	if err != nil {
		t.Fatal(err)
	}
	defer q.Undeclare()
	settle()
	for i := range 10 {
		if err := a.Put(key+"/a", make([]byte, i*10)); err != nil {
			t.Fatal(err)
		}
	}
	_ = a.Put(key+"/big", make([]byte, 5000))
	_ = a.Delete(key + "/a")
	sink.wait(t, 12)
	if r, err := collect(a.Get(key+"/q", WithTimeout(3*time.Second))); err != nil || len(r) != 1 {
		t.Fatalf("get: %v %v", r, err)
	}
}

// metricLines parses OpenMetrics samples: "name{labels}" -> value, with
// the constant labels dropped.
func metricLines(m string) map[string]string {
	out := map[string]string{}
	consts := regexp.MustCompile(`local_id="[^"]*",local_whatami="[^"]*",?`)
	for _, l := range strings.Split(m, "\n") {
		if l == "" || strings.HasPrefix(l, "#") {
			continue
		}
		i := strings.LastIndexByte(l, ' ')
		out[consts.ReplaceAllString(l[:i], "")] = l[i+1:]
	}
	return out
}

// Counters agree between the two ends of a link and with what was sent.
func TestStatsCounters(t *testing.T) {
	r := statsRouter(t, Config{})
	on := true
	open := func() *Session {
		c, err := Open(Config{Connect: r.Locators(), Stats: StatsConfig{Enabled: &on}})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = c.Close() })
		return c
	}
	a, b := open(), open()
	statsTraffic(t, a, b, uniq(t))

	// The ends of a link see the same bytes and messages (wait for
	// in-flight keep-alives to land).
	deadline := time.Now().Add(3 * time.Second)
	for {
		as, rs := a.Stats(), r.Stats()
		var rl *TransportStats
		for i := range rs.Transports {
			if rs.Transports[i].Transport.ZID.String() == a.ZID().String() {
				rl = &rs.Transports[i]
			}
		}
		if len(as.Transports) != 1 || rl == nil || len(as.Transports[0].Links) != 1 || len(rl.Links) != 1 {
			t.Fatalf("transports %+v / %+v", as.Transports, rl)
		}
		al, ral := as.Transports[0].Links[0], rl.Links[0]
		ok := al.Tx.Bytes == ral.Rx.Bytes && al.Rx.Bytes == ral.Tx.Bytes &&
			al.Tx.TransportMessages == ral.Rx.TransportMessages && al.Tx.NetworkMessages == ral.Rx.NetworkMessages &&
			al.Rx.NetworkMessages == ral.Tx.NetworkMessages
		if ok {
			if al.Link.Src != ral.Link.Dst || al.Link.Dst != ral.Link.Src {
				t.Errorf("links %+v / %+v", al.Link, ral.Link)
			}
			// a sent 11 PUTs, a DELETE and a query; the router saw them all.
			if rl.Rx.Messages != 13 || rl.Rx.PayloadBytes != 450+5000 || as.Tx.Messages != 13 {
				t.Errorf("router rx %+v, client tx %+v", rl.Rx, as.Tx)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("link ends differ: client %+v, router %+v", al, ral)
		}
		time.Sleep(50 * time.Millisecond)
	}

	m := metricLines(r.Metrics(MetricsOptions{}))
	want := map[string]string{
		`zenoh_rx_network_message_payload_bytes_sum{space="user",priority="data",message="put",shm="false"}`:                "5450.0",
		`zenoh_rx_network_message_payload_bytes_count{space="user",priority="data",message="put",shm="false"}`:              "11",
		`zenoh_rx_network_message_payload_bytes_bucket{le="0.0",space="user",priority="data",message="put",shm="false"}`:    "1",
		`zenoh_rx_network_message_payload_bytes_bucket{le="32.0",space="user",priority="data",message="put",shm="false"}`:   "4",
		`zenoh_rx_network_message_payload_bytes_bucket{le="1024.0",space="user",priority="data",message="put",shm="false"}`: "10",
		`zenoh_rx_network_message_payload_bytes_bucket{le="+Inf",space="user",priority="data",message="put",shm="false"}`:   "11",
		`zenoh_tx_network_message_payload_bytes_count{space="user",priority="data",message="delete",shm="false"}`:           "1",
		`zenoh_rx_network_message_payload_bytes_sum{space="user",priority="data",message="reply",shm="false"}`:              "100.0",
		`zenoh_rx_network_message_total{priority="data",message="put",shm="false",protocol="tcp"}`:                          "11",
		`zenoh_tx_network_message_total{priority="data",message="put",shm="false",protocol="tcp"}`:                          "11",
		`zenoh_tx_network_message_total{priority="data",message="query",shm="false",protocol="tcp"}`:                        "1",
		`zenoh_transports_opened{}`:          "2",
		`zenoh_links_opened{protocol="tcp"}`: "2",
		`zenoh_rx_network_message_per_transport_total{priority="data",message="put",shm="false",protocol="tcp",` +
			`remote_zid="` + a.ZID().String() + `",remote_whatami="client",remote_group="",remote_cn="",disconnected="false"}`: "11",
	}
	for k, v := range want {
		if m[k] != v {
			t.Errorf("%s = %q, want %q", k, m[k], v)
		}
	}
}

// The admin space serves the metrics and ?_stats JSON with zenohd's
// shape; a closed transport shows with ?disconnected=true and resumes when
// the peer comes back.
func TestStatsAdmin(t *testing.T) {
	r := statsRouter(t, Config{Stats: StatsConfig{Filters: []string{"**/big"}}})
	a := icptClient(t, r.Locators()[0])
	b := icptClient(t, r.Locators()[0])
	key := uniq(t)
	statsTraffic(t, a, b, key)
	root := "@/" + r.ZID().String() + "/router"

	m := adminDump(t, a, root+"/metrics?compression=false")[root+"/metrics"]
	if !strings.HasSuffix(m, "\n# EOF\n") {
		t.Errorf("metrics end %q", m[max(0, len(m)-20):])
	}
	var descs []string
	for _, l := range strings.Split(m, "\n") {
		if strings.HasPrefix(l, "# HELP ") || strings.HasPrefix(l, "# UNIT ") {
			descs = append(descs, l)
		}
	}
	golden, err := os.ReadFile("testdata/metrics_descriptors.txt")
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(descs, "\n") + "\n"; got != string(golden) {
		t.Errorf("descriptors differ from zenohd's:\n%s", got)
	}
	ml := metricLines(m)
	big := `zenoh_rx_network_message_payload_per_key_bytes_count{space="user",priority="data",message="put",shm="false",key="**/big"}`
	if ml[big] != "1" {
		t.Errorf("%s = %q", big, ml[big])
	}
	nodesc := adminDump(t, a, root+"/metrics?compression=false;descriptors=false;per_transport=false;per_link=false;per_key=false")[root+"/metrics"]
	if strings.Contains(nodesc, "# HELP") || strings.Contains(nodesc, "_per_") || !strings.HasSuffix(nodesc, "# EOF\n") {
		t.Errorf("filtered metrics:\n%s", nodesc)
	}

	js := adminDump(t, a, root+"?_stats=true")[root]
	var obj map[string]any
	if err := json.Unmarshal([]byte(js), &obj); err != nil {
		t.Fatal(err)
	}
	var shape []string
	var walk func(p string, v any)
	walk = func(p string, v any) {
		switch v := v.(type) {
		case map[string]any:
			for k, x := range v {
				walk(p+"/"+k, x)
			}
		case []any:
			if len(v) > 0 {
				walk(p+"[]", v[0])
			}
		default:
			shape = append(shape, p)
		}
	}
	walk("", obj)
	sort.Strings(shape)
	golden, err = os.ReadFile("testdata/stats_json_shape.txt")
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(shape, "\n") + "\n"; got != string(golden) {
		t.Errorf("_stats JSON shape differs from zenohd's:\n%s", got)
	}
	st := obj["stats"].(map[string]any)
	if v := st["rx_z_put_msgs"].(map[string]any)["user"]; v != 11.0 {
		t.Errorf("rx_z_put_msgs %v", v)
	}
	if f := obj["stats_filtered"].([]any)[0].(map[string]any); f["key"] != "**/big" ||
		f["stats"].(map[string]any)["rx_z_put_pl_bytes"].(map[string]any)["user"] != 5000.0 {
		t.Errorf("stats_filtered %v", f)
	}
	if _, ok := adminDump(t, a, root)[root]; !ok || strings.Contains(adminDump(t, a, root)[root], `"stats"`) {
		t.Error("stats without ?_stats")
	}

	// b goes: its transport is only shown with ?disconnected=true.
	bz := b.ZID().String()
	_ = b.Close()
	time.Sleep(300 * time.Millisecond)
	tag := `remote_zid="` + bz + `"`
	if m := r.Metrics(MetricsOptions{}); strings.Contains(m, tag) {
		t.Error("closed transport shown")
	}
	m = r.Metrics(MetricsOptions{Disconnected: true})
	if !strings.Contains(m, tag+`,remote_whatami="client",remote_group="",remote_cn="",disconnected="true"`) {
		t.Errorf("closed transport not shown with Disconnected:\n%s", m)
	}
	if st := r.Stats(); len(st.Transports) != 1 || st.Rx.Messages < 13 {
		t.Errorf("stats after close %+v", st)
	}
	if strings.Contains(m, `zenoh_transports_opened 2`) {
		t.Error("transports_opened counts the closed one")
	}
}

// Dropped messages are counted by reason.
func TestStatsDropped(t *testing.T) {
	key := uniq(t)
	r := statsRouter(t, Config{LowPassFilter: []LowPassFilterConfig{
		{Messages: msgs(MessagePut), KeyExprs: []string{key + "/**"}, SizeLimit: 10, Flows: []ACLFlow{ACLIngress}},
	}})
	a := icptClient(t, r.Locators()[0])
	settle()
	_ = a.Put(key+"/x", make([]byte, 100))
	_ = a.Put(key+"/y", make([]byte, 5))
	settle()
	m := metricLines(r.Metrics(MetricsOptions{}))
	k := `zenoh_rx_network_message_dropped_payload_bytes_sum{priority="data",message="put",protocol="",reason="low-pass"}`
	if m[k] != "100.0" {
		t.Errorf("%s = %q", k, m[k])
	}
	if st := r.Stats(); st.Rx.Dropped != 1 || st.Rx.Messages != 1 {
		t.Errorf("rx %+v", st.Rx)
	}
}

// A multicast group is a transport with one link.
func TestStatsMulticast(t *testing.T) {
	group := mcastLocator(t)
	a := openMcastPeer(t, group)
	b := openMcastPeer(t, group)
	waitFor(t, "group members", func() bool { return len(a.MulticastPeers()) == 1 && len(b.MulticastPeers()) == 1 })
	_ = a.Put(uniq(t), []byte("x"))
	time.Sleep(200 * time.Millisecond)
	var g *TransportStats
	for i, tr := range a.Stats().Transports {
		if tr.Transport.Multicast {
			g = &a.Stats().Transports[i]
		}
	}
	if g == nil || len(g.Links) != 1 || g.Tx.Bytes == 0 || g.Rx.TransportMessages == 0 || g.Links[0].Link.Group == "" {
		t.Fatalf("group %+v", g)
	}
	if m := a.Metrics(MetricsOptions{}); !strings.Contains(m, `remote_zid="",remote_whatami="",remote_group="`+g.Links[0].Link.Group+`"`) {
		t.Errorf("metrics:\n%s", m)
	}
}

// Off: no counters, the build information only (as zenohd without stats).
func TestStatsOff(t *testing.T) {
	off := false
	r, err := Open(Config{Mode: ModeRouter, Listen: []string{"tcp/127.0.0.1:0"}, Scouting: ScoutingConfig{Disable: true},
		Stats: StatsConfig{Enabled: &off}})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if m := r.Metrics(MetricsOptions{}); strings.Count(m, "\n") != 4 || !strings.HasSuffix(m, "# EOF\n ") {
		t.Errorf("metrics %q", m)
	}
	if st := r.Stats(); st.Transports != nil || st.Rx != (TrafficStats{}) {
		t.Errorf("stats %+v", st)
	}
	if _, err := Open(Config{Mode: ModeRouter, Listen: []string{"tcp/127.0.0.1:0"}, Scouting: ScoutingConfig{Disable: true},
		Stats: StatsConfig{Filters: []string{"a//b"}}}); err == nil {
		t.Error("bad filter accepted")
	}
}

// Against zenoh-rust built with its "stats" feature (ZENOHD_STATS: a
// router binary taking a JSON config and printing its ZID, see
// testdata/zstatsd): the same traffic gives the same metric families,
// series and values (bytes and transport messages aside: zenoh-rust packs
// more messages per batch).
func TestStatsRust(t *testing.T) {
	bin := os.Getenv("ZENOHD_STATS")
	if bin == "" {
		t.Skip("set ZENOHD_STATS to a zenoh-rust router built with the stats feature")
	}
	loc := freeLocator(t)
	cmd := exec.Command(bin, `{"listen/endpoints":["`+loc+`"],"scouting/multicast/enabled":false}`)
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	sc := bufio.NewScanner(out)
	if !sc.Scan() {
		t.Fatal("no zid from zstatsd")
	}
	rz := strings.TrimSpace(sc.Text())

	run := func(router, root string) map[string]string {
		a, b := icptClient(t, router), icptClient(t, router)
		key := "statsrust"
		statsTraffic(t, a, b, key)
		m := adminDump(t, a, root+"/metrics?compression=false")[root+"/metrics"]
		norm := map[string]string{}
		for k, v := range metricLines(m) {
			if strings.Contains(k, "_per_") || strings.Contains(k, "build_info") ||
				strings.HasPrefix(k, "zenoh_tx_bytes") || strings.HasPrefix(k, "zenoh_rx_bytes") ||
				strings.Contains(k, "transport_message") {
				continue
			}
			norm[k] = v
		}
		return norm
	}
	rust := run(loc, "@/"+rz+"/router")
	r := statsRouter(t, Config{})
	gom := run(r.Locators()[0], "@/"+r.ZID().String()+"/router")
	// Our admin replies differ in size (version string, ZIDs): skip the
	// admin-space reply payloads.
	for k := range rust {
		if strings.Contains(k, `space="admin"`) && strings.Contains(k, "reply") {
			delete(rust, k)
			delete(gom, k)
		}
	}
	keys := map[string]bool{}
	for k := range rust {
		keys[k] = true
	}
	for k := range gom {
		keys[k] = true
	}
	for k := range keys {
		if rust[k] != gom[k] {
			t.Errorf("%s: rust %q, go %q", k, rust[k], gom[k])
		}
	}
}
