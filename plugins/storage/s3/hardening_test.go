package s3

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/shabunin/tiny-zenoh-go/plugins/storage"
	"github.com/shabunin/tiny-zenoh-go/protocol"
	"github.com/shabunin/tiny-zenoh-go/zenoh"
)

func TestS3KeyValidationAndRegionXML(t *testing.T) {
	fk := &fakeS3{buckets: map[string]map[string]fakeObject{}}
	srv := httptest.NewServer(fk)
	defer srv.Close()

	vol, err := NewVolume(map[string]any{"url": srv.URL, "region": "us-east-1"})
	if err != nil {
		t.Fatal(err)
	}
	b, err := vol.CreateStorage(storage.Config{KeyExpr: "**"}, map[string]any{"bucket": "test-bkt"})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	zid, _ := protocol.NewZID([]byte{1})
	ts := zenoh.Timestamp{Time: 1 << 32, ID: zid}
	for _, bad := range []string{"../other", "a/../b", ".", "./a", `a\b`, "a//b"} {
		if _, err := b.Put(bad, storage.Value{Payload: []byte("x"), Timestamp: ts}); err == nil {
			t.Errorf("Put(%q) succeeded, want error", bad)
		}
		if _, err := b.Delete(bad, ts); err == nil {
			t.Errorf("Delete(%q) succeeded, want error", bad)
		}
		if _, err := b.Get(bad, ""); err == nil {
			t.Errorf("Get(%q) succeeded, want error", bad)
		}
	}

	// Region containing XML metacharacters must be XML-escaped during bucket creation.
	var createBody string
	fk2 := &fakeS3{buckets: map[string]map[string]fakeObject{}}
	capSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && r.URL.Path == "/b2" {
			raw, _ := io.ReadAll(r.Body)
			createBody = string(raw)
			r.Body = io.NopCloser(strings.NewReader(createBody))
		}
		fk2.ServeHTTP(w, r)
	}))
	defer capSrv.Close()

	vol2, err := NewVolume(map[string]any{"url": capSrv.URL, "region": `bad<region>&"`})
	if err != nil {
		t.Fatal(err)
	}
	b2, err := vol2.CreateStorage(storage.Config{KeyExpr: "**"}, map[string]any{"bucket": "b2"})
	if err != nil {
		t.Fatalf("CreateStorage: %v", err)
	}
	_ = b2.Close()
	if !strings.Contains(createBody, `bad&lt;region&gt;&amp;&#34;`) {
		t.Fatalf("CreateBucketConfiguration region not XML-escaped: %s", createBody)
	}
}
