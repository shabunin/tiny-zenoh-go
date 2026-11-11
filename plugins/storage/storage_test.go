package storage

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/shabunin/tiny-zenoh-go/zenoh"
)

func router(t *testing.T) *zenoh.Session {
	t.Helper()
	r, err := zenoh.Open(zenoh.Config{Mode: zenoh.ModeRouter, Listen: []string{"tcp/127.0.0.1:0"},
		Scouting: zenoh.ScoutingConfig{Disable: true}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	return r
}

func client(t *testing.T, r *zenoh.Session) *zenoh.Session {
	t.Helper()
	s, err := zenoh.Open(zenoh.Config{Connect: r.Locators()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// get returns key=payload for every reply, sorted.
func get(t *testing.T, s *zenoh.Session, sel string) []string {
	t.Helper()
	ch, err := s.Get(sel, zenoh.WithTimeout(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for r := range ch {
		if r.Sample == nil {
			t.Fatalf("error reply %v", r.Err)
		}
		if r.Sample.Timestamp == nil {
			t.Errorf("reply %s without timestamp", r.Sample.KeyExpr)
		}
		out = append(out, r.Sample.KeyExpr+"="+string(r.Sample.Payload))
	}
	sort.Strings(out)
	return out
}

func eq(t *testing.T, what string, got []string, want ...string) {
	t.Helper()
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("%s = %v, want %v", what, got, want)
	}
}

func settle() { time.Sleep(200 * time.Millisecond) }

func TestMemoryStorage(t *testing.T) {
	r := router(t)
	pfx := fmt.Sprintf("st/%d", time.Now().UnixNano())
	if _, err := NewMemory(r, Config{KeyExpr: pfx + "/**", StripPrefix: "other"}); err == nil {
		t.Error("strip_prefix outside key_expr accepted")
	}
	m, err := NewMemory(r, Config{KeyExpr: pfx + "/**", StripPrefix: pfx, Complete: true})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	a, b := client(t, r), client(t, r)
	settle()

	_ = a.Put(pfx+"/k1", []byte("v1"))
	_ = a.Put(pfx+"/k2", []byte("v2"), zenoh.WithEncoding(zenoh.EncodingTextPlain))
	_ = a.Put(pfx+"/k1", []byte("v1b"))
	settle()
	eq(t, "after puts", get(t, b, pfx+"/**"), pfx+"/k1=v1b", pfx+"/k2=v2")
	eq(t, "concrete query", get(t, b, pfx+"/k2"), pfx+"/k2=v2")

	// A delete leaves a tombstone: an older put arriving late stays deleted.
	old := a.NewTimestamp()
	settle()
	_ = a.Delete(pfx + "/k1")
	settle()
	_ = a.Put(pfx+"/k1", []byte("late"), zenoh.WithTimestamp(old))
	settle()
	eq(t, "after delete", get(t, b, pfx+"/**"), pfx+"/k2=v2")

	// Wildcard delete covers stored keys and later older puts.
	_ = a.Put(pfx+"/x/a", []byte("a"))
	_ = a.Put(pfx+"/x/b", []byte("b"))
	settle()
	before := a.NewTimestamp()
	settle()
	_ = a.Delete(pfx + "/x/*")
	settle()
	_ = a.Put(pfx+"/x/c", []byte("c-old"), zenoh.WithTimestamp(before))
	_ = a.Put(pfx+"/x/d", []byte("d-new"))
	settle()
	eq(t, "after wildcard delete", get(t, b, pfx+"/x/**"), pfx+"/x/d=d-new")

	// Wildcard put overrides older values of the keys it covers.
	_ = a.Put(pfx+"/y/a", []byte("a"))
	settle()
	_ = a.Put(pfx+"/y/*", []byte("all"))
	settle()
	eq(t, "after wildcard put", get(t, b, pfx+"/y/**"), pfx+"/y/a=all")
	if n := m.Len(); n != 3 {
		t.Errorf("Len = %d", n)
	}

	// Tombstones go after their lifespan.
	m.cfg.GCLifespan = time.Nanosecond
	m.gc(time.Now().Add(time.Second))
	m.mu.Lock()
	tomb := len(m.latest)
	nwild := len(m.wildDels) + len(m.wildPuts)
	m.mu.Unlock()
	if tomb != 0 || nwild != 0 {
		t.Errorf("after gc: %d tombstones, %d wildcard updates", tomb, nwild)
	}
}

// TestMemoryStorageRustClient: a Rust client (Python binding) stores into
// and reads back from a Go storage (ZENOH_PY).
func TestMemoryStorageRustClient(t *testing.T) {
	py := os.Getenv("ZENOH_PY")
	if py == "" {
		t.Skip("set ZENOH_PY to a python with eclipse-zenoh installed")
	}
	r := router(t)
	pfx := fmt.Sprintf("st/%d", time.Now().UnixNano())
	m, err := NewMemory(r, Config{KeyExpr: pfx + "/**"})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	settle()
	zpeer := "../../zenoh/testdata/zpeer.py"
	if out, err := exec.Command(py, "-I", zpeer, r.Locators()[0], "puts", pfx+"/r1", pfx+"/r2").CombinedOutput(); err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	cmd := exec.Command(py, "-I", zpeer, r.Locators()[0], "get", pfx+"/**")
	out, _ := cmd.StdoutPipe()
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	var got []string
	sc := bufio.NewScanner(out)
	for sc.Scan() {
		var ev struct {
			Ok *struct {
				Key     string `json:"key"`
				Payload string `json:"payload"`
			} `json:"ok"`
		}
		if json.Unmarshal(sc.Bytes(), &ev) == nil && ev.Ok != nil {
			got = append(got, ev.Ok.Key+"="+ev.Ok.Payload)
		}
	}
	_ = cmd.Wait()
	sort.Strings(got)
	eq(t, "rust get", got, pfx+"/r1="+pfx+"/r1", pfx+"/r2="+pfx+"/r2")
}
