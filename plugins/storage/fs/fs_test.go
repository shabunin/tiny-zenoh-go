package fs

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/shabunin/tiny-zenoh-go/plugins/storage"
	"github.com/shabunin/tiny-zenoh-go/plugins/storage/storagetest"
	"github.com/shabunin/tiny-zenoh-go/protocol"
	"github.com/shabunin/tiny-zenoh-go/zenoh"
)

var testZID, _ = protocol.NewZID([]byte{0xaa, 0xbb, 0xcc})

func ts(sec uint64) zenoh.Timestamp { return zenoh.Timestamp{Time: sec << 32, ID: testZID} }

func open(t *testing.T, v *Volume, params map[string]any) *Storage {
	t.Helper()
	b, err := v.CreateStorage(storage.Config{KeyExpr: "x/**"}, params)
	if err != nil {
		t.Fatal(err)
	}
	return b.(*Storage)
}

func entries(t *testing.T, s *Storage) []string {
	t.Helper()
	es, err := s.Entries()
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range es {
		out = append(out, e.Key)
	}
	sort.Strings(out)
	return out
}

func value(t *testing.T, s *Storage, key string) (string, storage.Value) {
	t.Helper()
	vs, err := s.Get(key, "")
	if err != nil {
		t.Fatal(err)
	}
	switch len(vs) {
	case 0:
		return "<none>", storage.Value{}
	case 1:
		return string(vs[0].Payload), vs[0]
	}
	t.Fatalf("%d values", len(vs))
	return "", storage.Value{}
}

// Files at the keys' paths, conflicts in .##z, metadata kept across
// reopening, empty directories removed.
func TestFilesystemBackend(t *testing.T) {
	v, err := NewVolume(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	s := open(t, v, map[string]any{"dir": "test/a"})
	enc := zenoh.ParseEncoding("text/plain;charset=utf-8")
	put := func(k, p string, sec uint64) {
		t.Helper()
		if r, err := s.Put(k, storage.Value{Payload: []byte(p), Encoding: enc, Timestamp: ts(sec)}); err != nil || r != storage.Inserted {
			t.Fatalf("Put %s: %v %v", k, r, err)
		}
	}
	put("a/b", "ab", 1)
	put("a/b/c", "abc", 2) // a/b must become a directory
	put("a/d", "ad", 3)
	put("", "root", 4)
	put("a/d/e", "ade", 5)
	put("a/d", "ad2", 6) // a/d is a directory now
	base := filepath.Join(v.Root(), "test/a")
	for f, want := range map[string]string{"a/b.##z": "ab", "a/b/c": "abc", "a/d.##z": "ad2", "a/d/e": "ade", "@root": "root"} {
		if b, err := os.ReadFile(filepath.Join(base, f)); err != nil || string(b) != want {
			t.Errorf("%s: %q %v", f, b, err)
		}
	}
	if got := strings.Join(entries(t, s), ","); got != ",a/b,a/b/c,a/d,a/d/e" {
		t.Errorf("entries %s", got)
	}
	if p, val := value(t, s, "a/d"); p != "ad2" || val.Timestamp.Time != 6<<32 || !val.Timestamp.ID.Equal(testZID) ||
		val.Encoding != enc {
		t.Errorf("a/d = %s %+v", p, val)
	}
	if p, _ := value(t, s, ""); p != "root" {
		t.Errorf("root = %s", p)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s = open(t, v, map[string]any{"dir": "test/a"})
	if p, val := value(t, s, "a/b"); p != "ab" || val.Timestamp.Time != 1<<32 || !val.Timestamp.ID.Equal(testZID) {
		t.Errorf("after reopening: a/b = %s %+v", p, val)
	}
	for _, k := range []string{"a/b/c", "a/d/e", "a/b", "a/d", ""} {
		if r, err := s.Delete(k, ts(10)); err != nil || r != storage.Deleted {
			t.Fatalf("Delete %s: %v %v", k, r, err)
		}
	}
	if got := entries(t, s); len(got) != 0 {
		t.Errorf("entries after deleting: %v", got)
	}
	if des, _ := os.ReadDir(base); len(des) != 1 || des[0].Name() != dataInfoDir {
		t.Errorf("left in the directory: %v", des)
	}
	if n := len(s.info.m); n != 0 {
		t.Errorf("%d metadata records left", n)
	}
	s.Close()

	// delete_all removes the directory on closing.
	s = open(t, v, map[string]any{"dir": "test/a", "on_closure": "delete_all"})
	put("k", "v", 1)
	s.Close()
	if _, err := os.Stat(base); !os.IsNotExist(err) {
		t.Errorf("directory kept: %v", err)
	}
}

// Files written by something else: timestamped by their modification time,
// encoded as their extension says; read_only refuses updates; symbolic
// links are skipped unless followed.
func TestFilesystemForeignFiles(t *testing.T) {
	root := t.TempDir()
	base := filepath.Join(root, "foreign")
	mt := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	for f, c := range map[string]string{"doc/index.html": "<p>", "doc/data.json": "{}", "blob": "?"} {
		p := filepath.Join(base, f)
		_ = os.MkdirAll(filepath.Dir(p), 0o755)
		if err := os.WriteFile(p, []byte(c), 0o644); err != nil {
			t.Fatal(err)
		}
		_ = os.Chtimes(p, mt, mt)
	}
	outside := filepath.Join(root, "outside")
	_ = os.WriteFile(outside, []byte("secret"), 0o644)
	if err := os.Symlink(outside, filepath.Join(base, "link")); err != nil {
		t.Fatal(err)
	}
	v, err := NewVolume(root)
	if err != nil {
		t.Fatal(err)
	}
	s := open(t, v, map[string]any{"dir": "foreign", "read_only": true})
	defer s.Close()
	if got := strings.Join(entries(t, s), ","); got != "blob,doc/data.json,doc/index.html" {
		t.Errorf("entries %s", got)
	}
	_, val := value(t, s, "doc/index.html")
	if val.Encoding.String() != "text/html" || val.Timestamp.GoTime().Unix() != mt.Unix() || !val.Timestamp.ID.Equal(fileID) {
		t.Errorf("index.html: %v %v %v", val.Encoding, val.Timestamp.GoTime(), val.Timestamp.ID)
	}
	if _, val := value(t, s, "blob"); val.Encoding != zenoh.EncodingApplicationOctetStream {
		t.Errorf("blob: %v", val.Encoding)
	}
	if p, _ := value(t, s, "link"); p != "<none>" {
		t.Errorf("symlink served: %s", p)
	}
	if _, err := s.Put("x", storage.Value{Timestamp: ts(1)}); err == nil {
		t.Error("read-only storage took a put")
	}
	if _, err := s.Delete("blob", ts(1)); err == nil {
		t.Error("read-only storage took a delete")
	}

	f := open(t, v, map[string]any{"dir": "foreign", "read_only": true, "follow_links": true, "keep_mime_types": false})
	defer f.Close()
	if p, _ := value(t, f, "link"); p != "secret" {
		t.Errorf("followed symlink: %s", p)
	}
	if _, val := value(t, f, "doc/index.html"); val.Encoding != zenoh.EncodingApplicationOctetStream {
		t.Errorf("without keep_mime_types: %v", val.Encoding)
	}

	for _, bad := range []map[string]any{nil, {}, {"dir": "/abs"}, {"dir": "a/../b"}, {"dir": "d", "on_closure": "x"},
		{"dir": "d", "read_only": 3}} {
		if _, err := v.CreateStorage(storage.Config{}, bad); err == nil {
			t.Errorf("accepted %v", bad)
		}
	}
}

// A torn metadata log loses its last record only; the log is compacted.
func TestFilesystemDataInfoLog(t *testing.T) {
	v, _ := NewVolume(t.TempDir())
	s := open(t, v, map[string]any{"dir": "d"})
	for i := range 200 {
		if _, err := s.Put("k", storage.Value{Payload: []byte("v"), Timestamp: ts(uint64(i + 1))}); err != nil {
			t.Fatal(err)
		}
	}
	_, _ = s.Put("j", storage.Value{Payload: []byte("w"), Timestamp: ts(7)})
	s.Close()
	log := filepath.Join(v.Root(), "d", dataInfoDir, dataInfoFile)
	st, _ := os.Stat(log)
	if st.Size() > 4096 {
		t.Errorf("log not compacted: %d bytes", st.Size())
	}
	f, _ := os.OpenFile(log, os.O_WRONLY|os.O_APPEND, 0)
	_, _ = f.Write(appendRecord(nil, "j", dataInfo{ts(99), zenoh.EncodingTextPlain}.marshal())[:10])
	f.Close()
	s = open(t, v, map[string]any{"dir": "d"})
	defer s.Close()
	if _, val := value(t, s, "k"); val.Timestamp.Time != 200<<32 {
		t.Errorf("k: %+v", val.Timestamp)
	}
	if _, val := value(t, s, "j"); val.Timestamp.Time != 7<<32 {
		t.Errorf("j: %+v", val.Timestamp)
	}
}

// A storage on an fs volume keeps its values across restarts, with their
// timestamps, and serves them to queries.
func TestFilesystemStorage(t *testing.T) {
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
	t.Setenv(RootEnv, t.TempDir())
	vol, err := storage.OpenVolume("fs", "files", nil)
	if err != nil {
		t.Fatal(err)
	}
	cfg := storage.Config{KeyExpr: "fsdemo/**", StripPrefix: "fsdemo"}
	params := map[string]any{"dir": "demo"}
	st, err := storage.New(r, cfg, vol, params)
	if err != nil {
		t.Fatal(err)
	}
	settle := func() { time.Sleep(300 * time.Millisecond) }
	settle()
	_ = c.Put("fsdemo/a/b", []byte("ab"), zenoh.WithEncoding(zenoh.EncodingApplicationJSON))
	_ = c.Put("fsdemo/a/b/c", []byte("abc"))
	_ = c.Put("fsdemo", []byte("root"))
	_ = c.Put("fsdemo/gone", []byte("x"))
	settle()
	_ = c.Delete("fsdemo/gone")
	settle()
	query := func() map[string]zenoh.Sample {
		ch, err := c.Get("fsdemo/**", zenoh.WithTimeout(3*time.Second))
		if err != nil {
			t.Fatal(err)
		}
		out := map[string]zenoh.Sample{}
		for rep := range ch {
			if rep.Sample != nil {
				out[rep.Sample.KeyExpr] = *rep.Sample
			}
		}
		return out
	}
	before := query()
	if len(before) != 3 || string(before["fsdemo/a/b"].Payload) != "ab" || string(before["fsdemo"].Payload) != "root" {
		t.Fatalf("before restart: %v", before)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	st, err = storage.New(r, cfg, vol, params)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	settle()
	after := query()
	if len(after) != 3 {
		t.Fatalf("after restart: %v", after)
	}
	for k, b := range before {
		a := after[k]
		if string(a.Payload) != string(b.Payload) || a.Encoding != b.Encoding || a.Timestamp == nil ||
			a.Timestamp.Time != b.Timestamp.Time || !a.Timestamp.ID.Equal(b.Timestamp.ID) {
			t.Errorf("%s: %v / %v", k, a, b)
		}
	}
	// An older put than what the files hold is outdated.
	old := zenoh.Timestamp{Time: before["fsdemo/a/b"].Timestamp.Time - 1<<32, ID: c.ZID()}
	_ = c.Put("fsdemo/a/b", []byte("old"), zenoh.WithTimestamp(old))
	settle()
	if got := query()["fsdemo/a/b"]; string(got.Payload) != "ab" {
		t.Errorf("older put replaced the value: %s", got.Payload)
	}
	if b, _ := os.ReadFile(filepath.Join(os.Getenv(RootEnv), "demo", "a", "b.##z")); string(b) != "ab" {
		t.Errorf("file a/b.##z: %q", b)
	}
}

func TestFilesystemContract(t *testing.T) {
	v, err := NewVolume(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	b, err := v.CreateStorage(storage.Config{KeyExpr: "x/**", StripPrefix: "x"}, map[string]any{"dir": "c"})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	storagetest.TestBackend(t, b, v.Capability(), "k")
	storagetest.TestStorage(t, v, map[string]any{"dir": "s"}, "fscontract")
}
