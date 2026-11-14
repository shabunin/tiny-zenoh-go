package s3

import (
	"crypto/tls"
	"encoding/base64"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/shabunin/tiny-zenoh-go/internal/testcert"
	"github.com/shabunin/tiny-zenoh-go/plugins/storage"
	"github.com/shabunin/tiny-zenoh-go/protocol"
	"github.com/shabunin/tiny-zenoh-go/zenoh"
)

// The backend's settings and failure paths, against an in-memory fake of
// the few S3 requests it makes (no S3_URL needed).

type fakeObject struct {
	body []byte
	hdr  http.Header
}

type fakeS3 struct {
	mu       sync.Mutex
	buckets  map[string]map[string]fakeObject
	created  []string // bodies of bucket creations
	pageSize int
	// fail, if set, answers a request instead of the fake (ok true).
	fail func(r *http.Request, bucket, object string) (status int, body string, ok bool)
}

func newFake(t *testing.T, tlsCfg *tls.Config) (*fakeS3, string) {
	f := &fakeS3{buckets: map[string]map[string]fakeObject{}, pageSize: 2}
	srv := httptest.NewUnstartedServer(f)
	srv.Config.ErrorLog = log.New(io.Discard, "", 0) // the untrusted client's handshake
	if tlsCfg != nil {
		srv.TLS = tlsCfg
		srv.StartTLS()
	} else {
		srv.Start()
	}
	t.Cleanup(srv.Close)
	return f, srv.URL
}

func s3err(code, msg string) string {
	return "<Error><Code>" + code + "</Code><Message>" + msg + "</Message></Error>"
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	bucket, object, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/"), "/")
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail != nil {
		if status, b, ok := f.fail(r, bucket, object); ok {
			w.WriteHeader(status)
			_, _ = io.WriteString(w, b)
			return
		}
	}
	objs, exists := f.buckets[bucket]
	if !exists && !(object == "" && r.Method == "PUT") {
		w.WriteHeader(http.StatusNotFound)
		if r.Method != "HEAD" {
			_, _ = io.WriteString(w, s3err("NoSuchBucket", "no bucket "+bucket))
		}
		return
	}
	switch {
	case object == "" && r.Method == "PUT":
		f.buckets[bucket] = map[string]fakeObject{}
		f.created = append(f.created, string(body))
	case object == "" && r.Method == "HEAD":
	case object == "" && r.Method == "DELETE":
		delete(f.buckets, bucket)
	case object == "" && r.Method == "GET":
		keys := make([]string, 0, len(objs))
		for k := range objs {
			keys = append(keys, k)
		}
		slices.Sort(keys)
		var start int
		if tok := r.URL.Query().Get("continuation-token"); tok != "" {
			_, _ = fmt.Sscan(tok, &start)
		}
		type content struct{ Key string }
		res := struct {
			XMLName               xml.Name `xml:"ListBucketResult"`
			Contents              []content
			IsTruncated           bool
			NextContinuationToken string `xml:",omitempty"`
		}{}
		end := min(start+f.pageSize, len(keys))
		for _, k := range keys[start:end] {
			res.Contents = append(res.Contents, content{k})
		}
		if end < len(keys) {
			res.IsTruncated, res.NextContinuationToken = true, fmt.Sprint(end)
		}
		b, _ := xml.Marshal(res)
		_, _ = w.Write(b)
	case r.Method == "PUT":
		h := http.Header{}
		for _, k := range []string{"Content-Encoding", timestampMeta, timestampMetaV0} {
			if v := r.Header.Get(k); v != "" {
				h.Set(k, v)
			}
		}
		objs[object] = fakeObject{body, h}
	case r.Method == "GET" || r.Method == "HEAD":
		o, ok := objs[object]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			if r.Method == "GET" {
				_, _ = io.WriteString(w, s3err("NoSuchKey", object))
			}
			return
		}
		for k, v := range o.hdr {
			w.Header()[k] = v
		}
		if r.Method == "GET" {
			_, _ = w.Write(o.body)
		}
	case r.Method == "DELETE":
		if _, ok := objs[object]; !ok {
			w.WriteHeader(http.StatusNotFound)
			_, _ = io.WriteString(w, s3err("NoSuchKey", object))
			return
		}
		delete(objs, object)
		w.WriteHeader(http.StatusNoContent)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// put stores an object directly, as another writer would.
func (f *fakeS3) put(bucket, object, body string, hdr ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	h := http.Header{}
	for i := 0; i+1 < len(hdr); i += 2 {
		h.Set(hdr[i], hdr[i+1])
	}
	f.buckets[bucket][object] = fakeObject{[]byte(body), h}
}

func (f *fakeS3) setFail(fn func(r *http.Request, bucket, object string) (int, string, bool)) {
	f.mu.Lock()
	f.fail = fn
	f.mu.Unlock()
}

func fakeVolume(t *testing.T, cfg map[string]any) *Volume {
	t.Helper()
	v, err := NewVolume(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(v.http.CloseIdleConnections)
	return v
}

func stamp(n uint64) zenoh.Timestamp {
	id, _ := protocol.NewZID([]byte{0x5a})
	return zenoh.Timestamp{Time: n << 32, ID: id}
}

func TestS3VolumeConfig(t *testing.T) {
	v := fakeVolume(t, map[string]any{"region": "eu-west-1"})
	if st := v.Status(); st["url"] != "https://s3.eu-west-1.amazonaws.com" || st["region"] != "eu-west-1" {
		t.Errorf("default endpoint: %v", st)
	}
	if c := v.Capability(); !c.Durable || c.History != storage.HistoryLatest {
		t.Errorf("capability %+v", c)
	}
	if err := v.Close(); err != nil {
		t.Error(err)
	}
	dir := t.TempDir()
	notPEM := dir + "/not.pem"
	if err := os.WriteFile(notPEM, []byte("hello"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		cfg  map[string]any
		want string
	}{
		{map[string]any{"region": 1}, "region: not a string"},
		{map[string]any{"url": true}, "url: not a string"},
		{map[string]any{"url": "ftp://example.com"}, "not an http(s) URL"},
		{map[string]any{"url": "http://"}, "not an http(s) URL"},
		{map[string]any{"url": "http://a b"}, "not an http(s) URL"},
		{map[string]any{"tls": "yes"}, "tls: not an object"},
		{map[string]any{"tls": map[string]any{"root_ca_certificate_file": dir + "/missing.pem"}}, "s3: tls:"},
		{map[string]any{"tls": map[string]any{"root_ca_certificate_file": notPEM}}, "no certificate"},
		{map[string]any{"tls": map[string]any{"root_ca_certificate_base64": "!!"}}, "root_ca_certificate_base64"},
		{map[string]any{"tls": map[string]any{"root_ca_certificate_base64": base64.StdEncoding.EncodeToString([]byte("hello"))}}, "no certificate"},
	} {
		if _, err := NewVolume(c.cfg); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%v: %v, want %q", c.cfg, err, c.want)
		}
	}
	// A tls object without a CA keeps the system's.
	fakeVolume(t, map[string]any{"tls": map[string]any{}})
}

func TestS3StorageConfig(t *testing.T) {
	_, u := newFake(t, nil)
	v := fakeVolume(t, map[string]any{"url": u})
	cfg := storage.Config{KeyExpr: "a/**"}
	for _, c := range []struct {
		params map[string]any
		want   string
	}{
		{nil, `"bucket" was not specified`},
		{map[string]any{"bucket": 3}, `"bucket" was not specified`},
		{map[string]any{"bucket": "b", "read_only": "maybe"}, "read_only: not a boolean"},
		{map[string]any{"bucket": "b", "reuse_bucket": 1}, "reuse_bucket: not a boolean"},
		{map[string]any{"bucket": "b", "on_closure": 1}, "on_closure: not a string"},
		{map[string]any{"bucket": "b", "on_closure": "delete_all"}, "on_closure must be"},
		{map[string]any{"bucket": "b", "private": "x"}, "private: not an object"},
		{map[string]any{"bucket": "b", "private": map[string]any{"access_key": "ak", "secret_key": 2}}, "secret_key: not a string"},
		{map[string]any{"bucket": "b", "private": map[string]any{"access_key": "ak"}}, "must both be set"},
		{map[string]any{"bucket": "b", "secret_key": "sk"}, "must both be set"},
	} {
		if _, err := v.CreateStorage(cfg, c.params); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%v: %v, want %q", c.params, err, c.want)
		}
	}

	// Credentials from the environment sign the requests.
	t.Setenv("AWS_ACCESS_KEY_ID", "envak")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "envsk")
	t.Setenv("AWS_SESSION_TOKEN", "envtok")
	b, err := v.CreateStorage(cfg, map[string]any{"bucket": "envb"})
	if err != nil {
		t.Fatal(err)
	}
	if c := b.(*Storage).c.creds; c == nil || c.AccessKey != "envak" || c.SessionToken != "envtok" {
		t.Errorf("credentials %+v", c)
	}
	if b.(*Storage).Bucket() != "envb" {
		t.Errorf("bucket %q", b.(*Storage).Bucket())
	}
}

func TestS3BucketLifecycle(t *testing.T) {
	f, u := newFake(t, nil)
	v := fakeVolume(t, map[string]any{"url": u, "region": "eu-west-1"})
	cfg := storage.Config{KeyExpr: "a/**"}
	if _, err := v.CreateStorage(cfg, map[string]any{"bucket": "b1"}); err != nil {
		t.Fatal(err)
	}
	if len(f.created) != 1 || !strings.Contains(f.created[0], "<LocationConstraint>eu-west-1</LocationConstraint>") {
		t.Errorf("bucket creation bodies %q", f.created)
	}
	if _, err := v.CreateStorage(cfg, map[string]any{"bucket": "b1"}); err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Errorf("existing bucket, no reuse: %v", err)
	}
	if _, err := v.CreateStorage(cfg, map[string]any{"bucket": "b1", "reuse_bucket": "true"}); err != nil {
		t.Errorf("reuse: %v", err)
	}
	f.setFail(func(r *http.Request, bucket, _ string) (int, string, bool) {
		switch {
		case bucket == "denied":
			return http.StatusForbidden, "", true
		case bucket == "nocreate" && r.Method == "PUT":
			return http.StatusConflict, s3err("BucketAlreadyOwnedByYou", "taken"), true
		}
		return 0, "", false
	})
	if _, err := v.CreateStorage(cfg, map[string]any{"bucket": "denied"}); err == nil || !strings.Contains(err.Error(), "checking bucket") ||
		!strings.Contains(err.Error(), "HTTP 403") {
		t.Errorf("forbidden HEAD: %v", err)
	}
	if _, err := v.CreateStorage(cfg, map[string]any{"bucket": "nocreate"}); err == nil ||
		!strings.Contains(err.Error(), "creating bucket") || !strings.Contains(err.Error(), "BucketAlreadyOwnedByYou: taken") {
		t.Errorf("failed creation: %v", err)
	}

	// An unreachable endpoint.
	dead := fakeVolume(t, map[string]any{"url": "http://127.0.0.1:1"})
	if _, err := dead.CreateStorage(cfg, map[string]any{"bucket": "x"}); err == nil {
		t.Error("unreachable endpoint accepted")
	}
}

func TestS3Requests(t *testing.T) {
	f, u := newFake(t, nil)
	v := fakeVolume(t, map[string]any{"url": u})
	b, err := v.CreateStorage(storage.Config{KeyExpr: "p/**", StripPrefix: "p"},
		map[string]any{"bucket": "rq", "on_closure": "destroy_bucket", "private": map[string]any{"access_key": "ak", "secret_key": "sk"}})
	if err != nil {
		t.Fatal(err)
	}
	enc := zenoh.EncodingTextPlain
	for i, k := range []string{"", "x", "y/z"} {
		if r, err := b.Put(k, storage.Value{Payload: []byte(k), Encoding: enc, Timestamp: stamp(uint64(i + 1))}); err != nil || r != storage.Inserted {
			t.Fatalf("put %q: %v %v", k, r, err)
		}
	}
	if _, err := b.Put("nil", storage.Value{Timestamp: stamp(9)}); err != nil {
		t.Fatal(err)
	}
	if got, err := b.Get("x", ""); err != nil || len(got) != 1 || string(got[0].Payload) != "x" || got[0].Timestamp.String() != stamp(2).String() {
		t.Fatalf("get x: %+v %v", got, err)
	}
	if got, err := b.Get("missing", ""); err != nil || got != nil {
		t.Fatalf("get missing: %+v %v", got, err)
	}
	// Objects written by others: the older timestamp metadata is read,
	// a missing one refused (and left out of the entries), as are keys
	// that are no key expressions or outside the storage's.
	f.put("rq", "/old", "o", timestampMetaV0, storage.FormatTimestamp(stamp(5)))
	f.put("rq", "/nots", "n")
	f.put("rq", "/bad//key", "b", timestampMeta, storage.FormatTimestamp(stamp(6)))
	f.put("rq", "/a*b", "u", timestampMeta, storage.FormatTimestamp(stamp(6)))
	if got, err := b.Get("old", ""); err != nil || len(got) != 1 || got[0].Timestamp.String() != stamp(5).String() {
		t.Fatalf("get old: %+v %v", got, err)
	}
	if _, err := b.Get("nots", ""); err == nil || !strings.Contains(err.Error(), "no zenoh-timestamp") {
		t.Fatalf("get without timestamp: %v", err)
	}
	f.put("rq", "/badts", "b", timestampMeta, "garbage")
	if _, err := b.Get("badts", ""); err == nil {
		t.Fatal("bad timestamp accepted")
	}
	entries, err := b.Entries()
	if err != nil {
		t.Fatal(err)
	}
	var keys []string
	for _, e := range entries {
		keys = append(keys, e.Key)
	}
	slices.Sort(keys)
	if want := []string{"", "nil", "old", "x", "y/z"}; !slices.Equal(keys, want) {
		t.Errorf("entries %q, want %q", keys, want)
	}

	// Request failures.
	f.setFail(func(r *http.Request, _, object string) (int, string, bool) {
		switch {
		case object == "/x" && r.Method == "GET":
			return http.StatusInternalServerError, s3err("InternalError", "oops"), true
		case object == "/x" && r.Method == "PUT":
			return http.StatusServiceUnavailable, "not xml", true
		case object == "/x" && r.Method == "DELETE":
			return http.StatusForbidden, s3err("AccessDenied", "no"), true
		case object == "/y/z" && r.Method == "HEAD":
			return http.StatusForbidden, "", true
		}
		return 0, "", false
	})
	if _, err := b.Get("x", ""); err == nil || !strings.Contains(err.Error(), "InternalError: oops") {
		t.Errorf("get error: %v", err)
	}
	if _, err := b.Put("x", storage.Value{Timestamp: stamp(7)}); err == nil || !strings.Contains(err.Error(), "HTTP 503") {
		t.Errorf("put error: %v", err)
	}
	if _, err := b.Delete("x", stamp(7)); err == nil || !strings.Contains(err.Error(), "AccessDenied") {
		t.Errorf("delete error: %v", err)
	}
	if entries, err := b.Entries(); err != nil || len(entries) != 4 {
		t.Errorf("entries with a failing HEAD: %+v %v", entries, err)
	}
	f.setFail(func(r *http.Request, _, object string) (int, string, bool) {
		if object == "" && r.Method == "GET" {
			return http.StatusOK, "<ListBucketResult><Contents>", true
		}
		return 0, "", false
	})
	if _, err := b.Entries(); err == nil || !strings.Contains(err.Error(), "listing") {
		t.Errorf("bad listing: %v", err)
	}
	if err := b.Close(); err == nil {
		t.Error("close with a bad listing succeeded")
	}
	f.setFail(func(r *http.Request, _, object string) (int, string, bool) {
		if object == "/x" && r.Method == "DELETE" {
			return http.StatusInternalServerError, "", true
		}
		return 0, "", false
	})
	if err := b.Close(); err == nil {
		t.Error("close with a failing delete succeeded")
	}
	f.setFail(nil)
	if r, err := b.Delete("x", stamp(8)); err != nil || r != storage.Deleted {
		t.Errorf("delete: %v %v", r, err)
	}
	if r, err := b.Delete("x", stamp(8)); err != nil || r != storage.Deleted {
		t.Errorf("delete of a missing key: %v %v", r, err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if _, ok := f.buckets["rq"]; ok {
		t.Error("destroy_bucket kept the bucket")
	}

	// Read-only storages refuse updates; do_nothing keeps the bucket.
	ro, err := v.CreateStorage(storage.Config{KeyExpr: "r/**"}, map[string]any{"bucket": "ro", "read_only": true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ro.Put("r/a", storage.Value{}); !errors.Is(err, errReadOnly) {
		t.Errorf("read-only put: %v", err)
	}
	if _, err := ro.Delete("r/a", stamp(1)); !errors.Is(err, errReadOnly) {
		t.Errorf("read-only delete: %v", err)
	}
	if err := ro.Close(); err != nil {
		t.Fatal(err)
	}
	if _, ok := f.buckets["ro"]; !ok {
		t.Error("do_nothing removed the bucket")
	}
}

func TestS3TLS(t *testing.T) {
	files, err := testcert.Write(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cert, err := tls.LoadX509KeyPair(files.ServerCert, files.ServerKey)
	if err != nil {
		t.Fatal(err)
	}
	_, u := newFake(t, &tls.Config{Certificates: []tls.Certificate{cert}})
	ca, err := os.ReadFile(files.CA)
	if err != nil {
		t.Fatal(err)
	}
	for name, tc := range map[string]map[string]any{
		"file":   {"root_ca_certificate_file": files.CA},
		"base64": {"private": map[string]any{"root_ca_certificate_base64": base64.StdEncoding.EncodeToString(ca)}},
	} {
		v := fakeVolume(t, map[string]any{"url": u, "tls": tc})
		if _, err := v.CreateStorage(storage.Config{KeyExpr: "t/**"}, map[string]any{"bucket": "tls" + name}); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
	// Without the CA the server isn't trusted.
	v := fakeVolume(t, map[string]any{"url": u})
	if _, err := v.CreateStorage(storage.Config{KeyExpr: "t/**"}, map[string]any{"bucket": "untrusted"}); err == nil {
		t.Error("untrusted server accepted")
	}
}
