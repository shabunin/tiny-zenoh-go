package influxdb

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/shabunin/tiny-zenoh-go/plugins/storage"
	"github.com/shabunin/tiny-zenoh-go/protocol"
	"github.com/shabunin/tiny-zenoh-go/zenoh"
)

func TestInfluxLineEscaping(t *testing.T) {
	zid, _ := protocol.NewZID([]byte{1})
	p := putPoint(`k\1,2 3`, storage.Value{
		Payload:   []byte("line1\nline2\r\\\""),
		Encoding:  zenoh.ParseEncoding("text/plain"),
		Timestamp: zenoh.Timestamp{Time: 1 << 32, ID: zid},
	})
	line := p.line()
	// String fields escape only backslash and quote: InfluxDB stores a raw
	// newline as is, while an escaped one would come back as "\n".
	if !strings.Contains(line, `,value="line1`+"\nline2\r"+`\\\""`) {
		t.Fatalf("value field mis-escaped: %q", line)
	}
	if !strings.HasPrefix(line, `k\1\,2\ 3,`) {
		t.Fatalf("measurement not escaped: %q", line)
	}
}

func TestInfluxV1DeleteAndScheduleDrop(t *testing.T) {
	var mu sync.Mutex
	var queries []string
	var writes []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/query":
			_ = r.ParseForm()
			q := r.FormValue("q")
			mu.Lock()
			queries = append(queries, q)
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"results":[{}]}`))
		case "/write":
			buf := make([]byte, 4096)
			n, _ := r.Body.Read(buf)
			mu.Lock()
			writes = append(writes, string(buf[:n]))
			mu.Unlock()
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	defer srv.Close()

	prevDelay := DropMeasurementDelay
	DropMeasurementDelay = 20 * time.Millisecond
	defer func() { DropMeasurementDelay = prevDelay }()

	s := &V1Storage{
		c:     v1Client{base: srv.URL, db: "db"},
		admin: v1Client{base: srv.URL, db: "db"},
		drops: map[string]*time.Timer{},
	}
	defer s.Close()

	zid, _ := protocol.NewZID([]byte{1})
	ts := zenoh.Timestamp{Time: 10 << 32, ID: zid}
	if _, err := s.Delete("k/x", ts); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	// Concurrent/subsequent Put on same measurement cancels scheduled drop.
	if _, err := s.Put("k/x", storage.Value{Payload: []byte("v"), Timestamp: zenoh.Timestamp{Time: 11 << 32, ID: zid}}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	time.Sleep(60 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	var sawLE bool
	for _, q := range queries {
		if strings.Contains(q, "WHERE time <= ") {
			sawLE = true
		}
		if strings.HasPrefix(q, "DROP MEASUREMENT") {
			t.Fatalf("scheduled drop ran after Put cancelled it: %v", queries)
		}
	}
	if !sawLE {
		t.Fatalf("Delete did not use inclusive <= timestamp predicate: %v", queries)
	}
	if len(writes) < 2 {
		t.Fatalf("expected delete + put writes, got %v", writes)
	}
}

func TestInfluxV2EntriesSortBeforeLast(t *testing.T) {
	var fluxQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Query string `json:"query"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		fluxQuery = body.Query
		w.Header().Set("Content-Type", "text/csv")
		_, _ = w.Write([]byte(""))
	}))
	defer srv.Close()

	s := &V2Storage{c: v2Client{base: srv.URL, org: "o"}, bucket: "b"}
	if _, err := s.Entries(); err != nil {
		t.Fatalf("Entries: %v", err)
	}
	sortIdx := strings.Index(fluxQuery, `sort(columns: ["_time"])`)
	lastIdx := strings.Index(fluxQuery, `last()`)
	if sortIdx < 0 || lastIdx < 0 || sortIdx > lastIdx {
		t.Fatalf("Flux Entries query must sort by _time before last(): %s", fluxQuery)
	}
}
