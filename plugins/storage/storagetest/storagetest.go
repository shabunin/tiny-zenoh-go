// Package storagetest checks storage backends: what every backend must do
// (TestBackend) and a storage running on one over zenoh (TestStorage).
package storagetest

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/shabunin/tiny-zenoh-go/plugins/storage"
	"github.com/shabunin/tiny-zenoh-go/protocol"
	"github.com/shabunin/tiny-zenoh-go/zenoh"
)

var zid, _ = protocol.NewZID([]byte{0x5a, 0x17})

// Stamp is a timestamp at t.
func Stamp(t time.Time) zenoh.Timestamp {
	return zenoh.Timestamp{Time: protocol.NTP64FromTime(t), ID: zid}
}

func payloads(vs []storage.Value) string {
	var out []string
	for _, v := range vs {
		out = append(out, string(v.Payload))
	}
	return strings.Join(out, ",")
}

// TestBackend checks a backend b of capability c; prefix keeps its keys
// apart from other runs' (a backend on a shared server).
func TestBackend(t *testing.T, b storage.Backend, c storage.Capability, prefix string) {
	t.Helper()
	k := func(s string) string { return prefix + "/" + s }
	base := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
	at := func(sec int) zenoh.Timestamp { return Stamp(base.Add(time.Duration(sec) * time.Second)) }
	put := func(key, payload string, sec int, enc zenoh.Encoding) storage.Result {
		t.Helper()
		r, err := b.Put(key, storage.Value{Payload: []byte(payload), Encoding: enc, Timestamp: at(sec)})
		if err != nil {
			t.Fatalf("Put %s: %v", key, err)
		}
		return r
	}
	get := func(key, params string) []storage.Value {
		t.Helper()
		vs, err := b.Get(key, params)
		if err != nil {
			t.Fatalf("Get %s: %v", key, err)
		}
		return vs
	}
	plain := zenoh.ParseEncoding("text/plain;charset=utf-8")
	custom := zenoh.ParseEncoding("my/own;v=1")

	put(k("a"), "a1", 1, plain)
	put(k("a"), "a2", 2, plain)
	put(k("b/c"), "line1\nline \"2\", \\ x=y", 3, custom)
	bin := string([]byte{0, 0xff, 0xfe, 'x'})
	put(k("bin"), bin, 4, zenoh.EncodingApplicationOctetStream)
	put("", "none", 5, zenoh.EncodingZenohBytes)

	vs := get(k("a"), "")
	if p := payloads(vs); p != "a2" {
		t.Errorf("latest of a: %q", p)
	}
	if len(vs) == 1 && (vs[0].Timestamp.Time != at(2).Time || !vs[0].Timestamp.ID.Equal(zid) || vs[0].Encoding != plain) {
		t.Errorf("a: %+v", vs[0])
	}
	if vs := get(k("b/c"), ""); len(vs) != 1 || string(vs[0].Payload) != "line1\nline \"2\", \\ x=y" || vs[0].Encoding != custom {
		t.Errorf("b/c: %+v", vs)
	}
	if vs := get(k("bin"), ""); len(vs) != 1 || string(vs[0].Payload) != bin {
		t.Errorf("bin: %+v", vs)
	}
	if vs := get("", ""); len(vs) != 1 || string(vs[0].Payload) != "none" || vs[0].Encoding != zenoh.EncodingZenohBytes {
		t.Errorf("none key: %+v", vs)
	}
	if vs := get(k("nothing"), ""); len(vs) != 0 {
		t.Errorf("nothing: %+v", vs)
	}

	if c.History == storage.HistoryAll {
		r := fmt.Sprintf("_time=[%s..%s]", base.Add(500*time.Millisecond).Format(time.RFC3339Nano),
			base.Add(2*time.Second).Format(time.RFC3339Nano))
		if p := payloads(get(k("a"), r)); p != "a1,a2" {
			t.Errorf("a in %s: %q", r, p)
		}
		r = fmt.Sprintf("_time=]%s..]", base.Add(time.Second).Format(time.RFC3339Nano))
		if p := payloads(get(k("a"), r)); p != "a2" {
			t.Errorf("a in %s: %q", r, p)
		}
		if p := payloads(get(k("a"), "_time=[now(-2h)..]")); p != "a1,a2" {
			t.Errorf("a since 2h: %q", p)
		}
	}

	es, err := b.Entries()
	if err != nil {
		t.Fatal(err)
	}
	var keys []string
	for _, e := range es {
		if e.Key == "" || strings.HasPrefix(e.Key, prefix+"/") {
			keys = append(keys, e.Key)
			if e.Key == k("a") && e.Timestamp.Time != at(2).Time {
				t.Errorf("entry a at %v", e.Timestamp)
			}
		}
	}
	sort.Strings(keys)
	if got, want := strings.Join(keys, ","), strings.Join([]string{"", k("a"), k("b/c"), k("bin")}, ","); got != want {
		t.Errorf("entries %s, want %s", got, want)
	}

	if r, err := b.Delete(k("a"), at(6)); err != nil || r != storage.Deleted {
		t.Fatalf("Delete: %v %v", r, err)
	}
	if r, err := b.Delete("", at(6)); err != nil || r != storage.Deleted {
		t.Fatalf("Delete none key: %v %v", r, err)
	}
	if vs := get(k("a"), ""); len(vs) != 0 {
		t.Errorf("a after delete: %q", payloads(vs))
	}
	if c.History == storage.HistoryAll {
		// Such backends keep older puts out themselves.
		if r := put(k("a"), "old", 5, plain); r != storage.Outdated {
			t.Errorf("put older than a delete: %v", r)
		}
		if vs := get(k("a"), ""); len(vs) != 0 {
			t.Errorf("a after older put: %q", payloads(vs))
		}
	}
	put(k("a"), "a3", 7, plain)
	if p := payloads(get(k("a"), "")); p != "a3" {
		t.Errorf("a after new put: %q", p)
	}
	es, _ = b.Entries()
	n := 0
	for _, e := range es {
		if e.Key == "" {
			t.Error("deleted none key listed")
		}
		if strings.HasPrefix(e.Key, prefix+"/") {
			n++
		}
	}
	if n != 3 {
		t.Errorf("%d entries after deletes", n)
	}
}

// TestStorage runs a storage on vol (with params) under a router and
// checks that clients' puts, deletes and queries work, and that a durable
// volume keeps the values across restarts.
func TestStorage(t *testing.T, vol storage.Volume, params map[string]any, prefix string) {
	t.Helper()
	r, err := zenoh.Open(zenoh.Config{Mode: zenoh.ModeRouter, Listen: []string{"tcp/127.0.0.1:0"},
		Scouting: zenoh.ScoutingConfig{Disable: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	c, err := zenoh.Open(zenoh.Config{Connect: r.Locators()})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	cfg := storage.Config{KeyExpr: prefix + "/**", StripPrefix: prefix,
		OnError: func(err error) { t.Error(err) }}
	st, err := storage.New(r, cfg, vol, params)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	settle := func() { time.Sleep(400 * time.Millisecond) }
	settle()
	query := func(sel string) string {
		t.Helper()
		ch, err := c.Get(sel, zenoh.WithTimeout(5*time.Second))
		if err != nil {
			t.Fatal(err)
		}
		var out []string
		for rep := range ch {
			if rep.Sample == nil {
				t.Errorf("error reply %v", rep.Err)
				continue
			}
			if rep.Sample.Timestamp == nil {
				t.Errorf("%s without timestamp", rep.Sample.KeyExpr)
			}
			out = append(out, strings.TrimPrefix(rep.Sample.KeyExpr, prefix)+"="+string(rep.Sample.Payload))
		}
		sort.Strings(out)
		return strings.Join(out, " ")
	}
	_ = c.Put(prefix+"/x/1", []byte("one"))
	_ = c.Put(prefix+"/x/2", []byte("two"), zenoh.WithEncoding(zenoh.EncodingApplicationJSON))
	_ = c.Put(prefix+"/y", []byte("why"))
	_ = c.Put(prefix, []byte("top"))
	settle()
	_ = c.Put(prefix+"/x/1", []byte("uno"))
	_ = c.Delete(prefix + "/y")
	settle()
	if got, want := query(prefix+"/**"), "/x/1=uno /x/2=two =top"; got != want {
		t.Errorf("query: %q, want %q", got, want)
	}
	if got, want := query(prefix+"/x/*"), "/x/1=uno /x/2=two"; got != want {
		t.Errorf("query x/*: %q, want %q", got, want)
	}
	// Wildcard delete.
	_ = c.Delete(prefix + "/x/*")
	settle()
	if got, want := query(prefix+"/**"), "=top"; got != want {
		t.Errorf("after wildcard delete: %q, want %q", got, want)
	}
	_ = c.Put(prefix+"/z", []byte("zed"))
	settle()
	if !st.Capability().Durable {
		return
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	if st, err = storage.New(r, cfg, vol, params); err != nil {
		t.Fatal(err)
	}
	settle()
	if got, want := query(prefix+"/**"), "/z=zed =top"; got != want {
		t.Errorf("after restart: %q, want %q", got, want)
	}
}

// Probe runs zenoh-rust's backend through the bprobe tool (named by
// $ZENOH_BPROBE; the test is skipped without it): backend, the
// storage_manager config with its one volume and one storage, the
// operation and its arguments. It returns the JSON objects printed.
type Probe func(op string, args ...string) []map[string]string

// RustProbe makes a Probe on zenoh-rust's backend.
func RustProbe(t *testing.T, backend string, config map[string]any) Probe {
	t.Helper()
	bin := os.Getenv("ZENOH_BPROBE")
	if bin == "" {
		t.Skip("ZENOH_BPROBE not set")
	}
	cfg, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	return func(op string, args ...string) []map[string]string {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		out, err := exec.CommandContext(ctx, bin, append([]string{backend, string(cfg), op}, args...)...).CombinedOutput()
		if err != nil {
			t.Fatalf("bprobe %s %v: %v\n%s", op, args, err, out)
		}
		var res []map[string]string
		for _, l := range strings.Split(strings.TrimSpace(string(out)), "\n") {
			if !strings.HasPrefix(l, "{") {
				continue
			}
			var m map[string]string
			if err := json.Unmarshal([]byte(l), &m); err != nil {
				t.Fatalf("bprobe output %q: %v", l, err)
			}
			res = append(res, m)
		}
		return res
	}
}

// RustQuirks are zenoh-rust backend behaviours TestRustCompat allows.
type RustQuirks struct {
	// ListsDeleted: its entries include deleted keys (InfluxDB backends:
	// their latest point is a deletion).
	ListsDeleted bool
	// GetMissingFails: a get of a missing key fails (S3 backend, when the
	// server's error does not read "NoSuchKey").
	GetMissingFails bool
}

// TestRustCompat checks that zenoh-rust's backend (rust) and ours (b), on
// the same database, read each other's values; prefix keeps the keys of
// the run apart.
func TestRustCompat(t *testing.T, b storage.Backend, rust Probe, prefix string, q RustQuirks) {
	t.Helper()
	// zenoh-rust writes, we read.
	rust("put", prefix+"/r", "from rust", "text/plain;charset=utf-8")
	rust("put", "-", "rust root", "zenoh/string")
	rts := rust("get", prefix+"/r", "")
	if len(rts) != 1 {
		t.Fatalf("rust get: %v", rts)
	}
	vs, err := b.Get(prefix+"/r", "")
	if err != nil || len(vs) != 1 || string(vs[0].Payload) != "from rust" || vs[0].Encoding.String() != "text/plain;charset=utf-8" ||
		storage.FormatTimestamp(vs[0].Timestamp) != rts[0]["timestamp"] {
		t.Errorf("rust's value read: %+v %v (rust: %v)", vs, err, rts)
	}
	if vs, err := b.Get("", ""); err != nil || len(vs) != 1 || string(vs[0].Payload) != "rust root" || vs[0].Encoding != zenoh.EncodingZenohString {
		t.Errorf("rust's root value read: %+v %v", vs, err)
	}
	// We write, zenoh-rust reads.
	now := time.Now()
	enc := zenoh.ParseEncoding("application/json;v=2")
	if _, err := b.Put(prefix+"/g", storage.Value{Payload: []byte(`{"from":"go"}`), Encoding: enc, Timestamp: Stamp(now)}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Put(prefix+"/gone", storage.Value{Payload: []byte("x"), Timestamp: Stamp(now)}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Delete(prefix+"/gone", Stamp(now.Add(time.Millisecond))); err != nil {
		t.Fatal(err)
	}
	g := rust("get", prefix+"/g", "")
	if len(g) != 1 || g[0]["payload"] != `{"from":"go"}` || g[0]["encoding"] != "application/json;v=2" ||
		g[0]["timestamp"] != storage.FormatTimestamp(Stamp(now)) {
		t.Errorf("rust read our value: %v", g)
	}
	if !q.GetMissingFails {
		if g := rust("get", prefix+"/gone", ""); len(g) != 0 {
			t.Errorf("rust read our deleted value: %v", g)
		}
	}
	keys := map[string]string{}
	for _, e := range rust("entries") {
		keys[e["key"]] = e["timestamp"]
	}
	if keys[prefix+"/g"] != storage.FormatTimestamp(Stamp(now)) || keys[prefix+"/r"] != rts[0]["timestamp"] {
		t.Errorf("rust entries: %v", keys)
	}
	if _, ok := keys[prefix+"/gone"]; ok && !q.ListsDeleted {
		t.Errorf("rust lists our deleted key: %v", keys)
	}
}
