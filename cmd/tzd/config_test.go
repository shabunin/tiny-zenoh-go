package main

import (
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/shabunin/tiny-zenoh-go/zenoh"
)

func TestConfigTree(t *testing.T) {
	file := filepath.Join(t.TempDir(), "z.json5")
	if err := os.WriteFile(file, []byte(`{
		// a zenohd-style file
		mode: 'router',
		id: "a1b2",
		listen: { endpoints: { router: ["tcp/127.0.0.1:0"], peer: ["tcp/127.0.0.1:1"] } },
		connect: { endpoints: [], timeout_ms: -1, },
		scouting: { multicast: { enabled: false, address: "224.0.0.224:7446" } },
		timestamping: { enabled: { router: true, peer: false } },
		plugins: { storage_manager: { storages: { a: { key_expr: "a/**", volume: "memory" } } } },
	}`), 0o600); err != nil {
		t.Fatal(err)
	}
	tree, err := configTree(file, "127.0.0.1:0", []string{
		`plugins/storage_manager/storages/b:{key_expr:"b/**",strip_prefix:"b",volume:{id:"memory"},complete:"true",garbage_collection:{period:5,lifespan:60}}`,
		`/metadata:{name:"x"}`,
		`plugins/storage_manager/storages/c:{key_expr:"c/**",volume:"memory",replication:{interval:1.5,sub_intervals:3,hot:2,warm:4,propagation_delay:100}}`,
	})
	if err != nil {
		t.Fatal(err)
	}
	var cfg zenoh.Config
	p, err := applyConfig(tree, &cfg)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Mode != zenoh.ModeRouter || len(cfg.Listen) != 1 || cfg.Listen[0] != "tcp/127.0.0.1:0" || len(cfg.Connect) != 0 {
		t.Fatalf("mode/endpoints: %v %v %v", cfg.Mode, cfg.Listen, cfg.Connect)
	}
	if cfg.ZID.String() != "a1b2" {
		t.Fatalf("id: %v", cfg.ZID)
	}
	if !cfg.Scouting.Disable || cfg.Timestamping.Enabled == nil || !*cfg.Timestamping.Enabled {
		t.Fatalf("scouting/timestamping: %+v %+v", cfg.Scouting, cfg.Timestamping)
	}
	if string(cfg.Metadata) != `{"name":"x"}` {
		t.Fatalf("metadata: %s", cfg.Metadata)
	}
	if p.restAddr != "127.0.0.1:0" || len(p.storages) != 3 {
		t.Fatalf("plugins: %+v", p)
	}
	b := p.storages[1]
	if b.name != "b" || !b.cfg.Complete || b.cfg.StripPrefix != "b" || b.cfg.GCPeriod != 5*time.Second || b.cfg.GCLifespan != time.Minute {
		t.Fatalf("storage b: %+v", b)
	}
	if r := p.storages[2].cfg.Replication; r == nil || r.Interval != 1500*time.Millisecond || r.SubIntervals != 3 ||
		r.Hot != 2 || r.Warm != 4 || r.PropagationDelay != 100*time.Millisecond {
		t.Fatalf("storage c replication: %+v", r)
	}

	tree, err = configTree("", "", []string{
		`transport:{unicast:{lowlatency:true,max_links:3,qos:{enabled:false},compression:{enabled:true}},` +
			`link:{tx:{lease:5000,batch_size:8192,queue:{size:{control:1,background:16},` +
			`congestion_control:{drop:{wait_before_drop:2000,max_wait_before_drop_fragments:40000},block:{wait_before_close:3000000}}}},tls:{root_ca_certificate:"ca.pem",verify_name_on_connect:false}},` +
			`shared_memory:{enabled:true},auth:{usrpwd:{user:"u",password:"p"}}}`,
		`region_name:"r1"`, `queries_default_timeout:2500`, `namespace:"ns"`,
		`stats:{filters:[{key:"a/**"},{key:"b/*"}]}`,
	})
	if err != nil {
		t.Fatal(err)
	}
	cfg = zenoh.Config{}
	if _, err = applyConfig(tree, &cfg); err != nil {
		t.Fatal(err)
	}
	if !cfg.LowLatency || cfg.MaxLinks != 3 || !cfg.DisableQoS || !cfg.Compression || cfg.Lease != 5*time.Second ||
		cfg.BatchSize != 8192 || cfg.TLS == nil || cfg.TLS.RootCACertificate != "ca.pem" || !cfg.TLS.SkipNameVerification ||
		cfg.SharedMemory == nil || cfg.Auth == nil || cfg.Auth.User != "u" || cfg.Auth.Password != "p" ||
		cfg.RegionName != "r1" || cfg.QueryTimeout != 2500*time.Millisecond || cfg.Namespace != "ns" ||
		len(cfg.Stats.Filters) != 2 || cfg.Stats.Filters[1] != "b/*" {
		t.Fatalf("transport etc.: %+v", cfg)
	}
	if q := cfg.TxQueue; q.Size[0] != 1 || q.Size[7] != 16 || q.Size[5] != 0 || q.WaitBeforeDrop != 2*time.Millisecond ||
		q.MaxWaitBeforeDropFragments != 40*time.Millisecond || q.WaitBeforeClose != 3*time.Second {
		t.Fatalf("tx queue: %+v", q)
	}
	if err := applyTxQueue(map[string]int{"data": 17}, &cfg.TxQueue); err == nil {
		t.Error("queue size 17 accepted")
	}

	// The whole configuration as --cfg ':...' replaces the file.
	tree, err = configTree(file, "", []string{`:{mode:"peer"}`})
	if err != nil {
		t.Fatal(err)
	}
	if len(tree) != 1 || tree["mode"] != "peer" {
		t.Fatalf("inline config: %v", tree)
	}

	for _, bad := range [][]string{
		{"nocolon"},
		{`plugins/storage_manager/storages/x:{key_expr:"x/**",volume:"rocksdb"}`},
		{`plugins/storage_manager/storages/x:{volume:"memory"}`},
		{`mode:"satellite"`},
		{`plugins/storage_manager/volumes/rocksdb:{}`},
		{`plugins/storage_manager/volumes/memory:{}`},
		{`plugins/storage_manager/volumes/files:{backend:3}`},
		{`plugins/storage_manager/volumes/fs:1`},
		{`plugins/storage_manager/volumes/fs:{}`, `plugins/storage_manager/storages/x:{key_expr:"x/**",volume:{dir:"x"}}`},
	} {
		tree, err := configTree("", "", bad)
		if err == nil {
			_, err = applyConfig(tree, &zenoh.Config{})
		}
		if err == nil {
			t.Errorf("%v: no error", bad)
		}
	}
}

// Volumes: named after their backend or naming it; a storage's volume
// object gives the storage's settings.
func TestConfigVolumes(t *testing.T) {
	tree, err := configTree("", "", []string{
		`plugins/storage_manager/volumes/fs:{__required__:true}`,
		`plugins/storage_manager/volumes/files2:{backend:"fs"}`,
		`plugins/storage_manager/storages/a:{key_expr:"a/**",volume:{id:"files2",dir:"a",read_only:true}}`,
		`plugins/storage_manager/storages/b:{key_expr:"b/**",volume:"fs"}`,
	})
	if err != nil {
		t.Fatal(err)
	}
	p, err := applyConfig(tree, &zenoh.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if len(p.volumes) != 2 || p.volumes[0].name != "files2" || p.volumes[0].backend != "fs" || p.volumes[1].backend != "fs" ||
		len(p.volumes[1].cfg) != 0 {
		t.Errorf("volumes %+v", p.volumes)
	}
	a := p.storages[0]
	if a.volume != "files2" || a.params["dir"] != "a" || a.params["read_only"] != true || len(a.params) != 2 ||
		p.storages[1].volume != "fs" || p.storages[1].params != nil {
		t.Errorf("storages %+v", p.storages)
	}
	t.Setenv("ZENOH_BACKEND_FS_ROOT", t.TempDir())
	r, err := zenoh.Open(zenoh.Config{Mode: zenoh.ModeRouter, Listen: []string{"tcp/127.0.0.1:0"}, Scouting: zenoh.ScoutingConfig{Disable: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	stop, err := startPlugins(r, p)
	if err == nil {
		stop()
		t.Error("fs storage without dir started")
	}
}

// TestPlugins runs a storage and the REST API on a router the way gozenohd
// does: a client's put lands in the storage, and REST reads it back.
func TestPlugins(t *testing.T) {
	tree, err := configTree("", "127.0.0.1:0", []string{
		`plugins/storage_manager/storages/demo:{key_expr:"demo/**",volume:"memory"}`,
		`listen/endpoints:["tcp/127.0.0.1:0"]`,
		`scouting/multicast/enabled:false`,
	})
	if err != nil {
		t.Fatal(err)
	}
	cfg := zenoh.Config{Mode: zenoh.ModeRouter}
	p, err := applyConfig(tree, &cfg)
	if err != nil {
		t.Fatal(err)
	}
	r, err := zenoh.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	p.restAddr = "127.0.0.1:0"
	stop, err := startPlugins(r, p)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	c, err := zenoh.Open(zenoh.Config{Connect: r.Locators()})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	time.Sleep(200 * time.Millisecond)
	if err := c.Put("demo/a", []byte("hello"), zenoh.WithEncoding(zenoh.EncodingTextPlain)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)

	n := 0
	for rep := range mustGet(t, c, "demo/**") {
		if rep.Sample == nil || string(rep.Sample.Payload) != "hello" || rep.Sample.Timestamp == nil {
			t.Fatalf("storage reply: %+v", rep)
		}
		n++
	}
	if n != 1 {
		t.Fatalf("%d replies from the storage", n)
	}
	// startPlugins' REST server is on a random port it only prints; read
	// through a second one.
	closeREST, addr, err := startREST(r, "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer closeREST()
	resp, err := http.Get("http://" + addr + "/demo/**")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	var out []struct {
		Key, Value, Encoding string
		Timestamp            *string
	}
	if err := json.Unmarshal(body, &out); err != nil || len(out) != 1 || out[0].Key != "demo/a" ||
		out[0].Value != "hello" || out[0].Timestamp == nil || !strings.Contains(*out[0].Timestamp, "/") {
		t.Fatalf("REST: %s (%v)", body, err)
	}
}

func mustGet(t *testing.T, s *zenoh.Session, sel string) <-chan zenoh.Reply {
	t.Helper()
	ch, err := s.Get(sel)
	if err != nil {
		t.Fatal(err)
	}
	return ch
}

// Storages come and go with PUT/DELETE on the admin space config, as with
// zenohd (adminspace/permissions/write).
func TestAdminConfigWrites(t *testing.T) {
	tree, err := configTree("", "", []string{
		`plugins/storage_manager/storages/a:{key_expr:"cw/a/**",volume:"memory"}`,
		`listen/endpoints:["tcp/127.0.0.1:0"]`, `scouting/multicast/enabled:false`,
		`adminspace/permissions:{read:true,write:true}`,
	})
	if err != nil {
		t.Fatal(err)
	}
	cfg := zenoh.Config{Mode: zenoh.ModeRouter}
	p, err := applyConfig(tree, &cfg)
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.AdminSpace.Write {
		t.Fatal("write permission not applied")
	}
	runner := newPluginRunner(tree["plugins"].(map[string]any))
	refused := make(chan string, 4)
	cfg.AdminSpace.OnConfig = runner.onConfig
	cfg.AdminSpace.OnConfigError = func(key string, err error) { refused <- key }
	r, err := zenoh.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if err := runner.start(r, p); err != nil {
		t.Fatal(err)
	}
	defer runner.stop()
	c, err := zenoh.Open(zenoh.Config{Connect: r.Locators()})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	time.Sleep(200 * time.Millisecond)
	root := "@/" + r.ZID().String() + "/router/config/"
	count := func(sel string) int {
		n := 0
		for rep := range mustGet(t, c, sel) {
			if rep.Sample != nil {
				n++
			}
		}
		return n
	}
	put := func(key, v string) {
		t.Helper()
		if err := c.Put(key, []byte(v), zenoh.WithCongestionControl(zenoh.CongestionBlock)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(200 * time.Millisecond)
	}

	put(root+"plugins/storage_manager/storages/b", `{key_expr:"cw/b/**",volume:"memory"}`)
	put("cw/a/1", "x")
	put("cw/b/1", "y")
	if n := count("cw/**"); n != 2 {
		t.Fatalf("%d values stored, want 2", n)
	}
	// Changed: restarted (empty); deleted: gone.
	put(root+"plugins/storage_manager/storages/b", `{key_expr:"cw/b/**",volume:"memory",complete:true}`)
	if err := c.Delete(root + "plugins/storage_manager/storages/a"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	if n := count("cw/**"); n != 0 {
		t.Fatalf("%d values after the changes, want 0", n)
	}
	runner.mu.Lock()
	names := len(runner.storages)
	_, hasB := runner.storages["b"]
	runner.mu.Unlock()
	if names != 1 || !hasB {
		t.Fatalf("running storages: %d (b: %v)", names, hasB)
	}
	// Only plugins/ is writable; bad values are refused.
	put(root+"mode", `"peer"`)
	put(root+"plugins/storage_manager/storages/c", `{volume:"memory"}`)
	for _, want := range []string{"mode", "plugins/storage_manager/storages/c"} {
		select {
		case k := <-refused:
			if k != want {
				t.Errorf("refused %s, want %s", k, want)
			}
		case <-time.After(time.Second):
			t.Fatalf("%s not refused", want)
		}
	}
}

func TestConfigArrayFilter(t *testing.T) {
	tree := map[string]any{"x": map[string]any{"list": []any{map[string]any{"id": "a", "v": 1.0}}}}
	if err := insertConfig(tree, "x/list/id=b", map[string]any{"id": "b"}); err != nil {
		t.Fatal(err)
	}
	if err := insertConfig(tree, "x/list/id=a", map[string]any{"id": "a", "v": 2.0}); err != nil {
		t.Fatal(err)
	}
	if err := insertConfig(tree, "x/list/id=c", map[string]any{"id": "d"}); err == nil {
		t.Error("mismatched filter accepted")
	}
	l := tree["x"].(map[string]any)["list"].([]any)
	if len(l) != 2 || l[0].(map[string]any)["v"] != 2.0 {
		t.Fatalf("list %v", l)
	}
	if err := removePath(tree, "x/list/id=a"); err != nil {
		t.Fatal(err)
	}
	if l = tree["x"].(map[string]any)["list"].([]any); len(l) != 1 || l[0].(map[string]any)["id"] != "b" {
		t.Fatalf("list %v", l)
	}
	if err := removePath(tree, "x/list"); err != nil || len(tree["x"].(map[string]any)) != 0 {
		t.Fatalf("remove: %v %v", err, tree)
	}
	if err := removePath(tree, "x/nope"); err == nil {
		t.Error("removing a missing key")
	}
}
