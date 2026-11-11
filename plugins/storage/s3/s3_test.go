package s3

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/shabunin/tiny-zenoh-go/plugins/storage"
	"github.com/shabunin/tiny-zenoh-go/plugins/storage/storagetest"
)

// The server of these tests (throwaway keys, local only), SeaweedFS's S3
// gateway with an s3.json granting zthrowawayak/zthrowaway-secret Admin:
//
//	docker run -d --name zs3 -p 127.0.0.1:18333:8333 -v $PWD/s3cfg:/etc/zs3:ro,Z \
//	  chrislusf/seaweedfs server -dir=/data -s3 -s3.port=8333 -s3.config=/etc/zs3/s3.json
//
//	S3_URL=http://127.0.0.1:18333 go test ./plugins/storage/s3

func volume(t *testing.T) *Volume {
	u := os.Getenv("S3_URL")
	if u == "" {
		t.Skip("S3_URL not set")
	}
	v, err := NewVolume(map[string]any{"url": u})
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func params(bucket string, more ...any) map[string]any {
	p := map[string]any{"bucket": bucket, "private": map[string]any{"access_key": "zthrowawayak", "secret_key": "zthrowaway-secret"}}
	for i := 0; i+1 < len(more); i += 2 {
		p[more[i].(string)] = more[i+1]
	}
	return p
}

func unique() string { return fmt.Sprintf("zt%d", time.Now().UnixNano()) }

func TestS3Backend(t *testing.T) {
	v := volume(t)
	bucket := unique()
	b, err := v.CreateStorage(storage.Config{KeyExpr: "zt/**", StripPrefix: "zt"}, params(bucket, "on_closure", "destroy_bucket"))
	if err != nil {
		t.Fatal(err)
	}
	storagetest.TestBackend(t, b, v.Capability(), unique())
	// Another storage on the bucket sees only its key expression's objects.
	o, err := v.CreateStorage(storage.Config{KeyExpr: "zt/nothing/**"}, params(bucket, "reuse_bucket", true))
	if err != nil {
		t.Fatal(err)
	}
	if es, err := o.Entries(); err != nil || len(es) != 0 {
		t.Errorf("other storage's entries: %v %v", es, err)
	}
	if _, err := v.CreateStorage(storage.Config{KeyExpr: "zt/**"}, params(bucket)); err == nil {
		t.Error("existing bucket taken without reuse_bucket")
	}
	ro, err := v.CreateStorage(storage.Config{KeyExpr: "zt/**"}, params(bucket, "reuse_bucket", true, "read_only", true))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ro.Put("k", storage.Value{}); err == nil {
		t.Error("read-only storage took a put")
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := (client{v: v, bucket: bucket, creds: b.(*Storage).c.creds}).do("HEAD", "", nil, nil, nil); !notFound(err) {
		t.Errorf("bucket not destroyed: %v", err)
	}
	bad := params(unique())
	bad["private"] = map[string]any{"access_key": "zthrowawayak", "secret_key": "wrong"}
	if _, err := v.CreateStorage(storage.Config{KeyExpr: "zt/**"}, bad); err == nil {
		t.Error("wrong secret accepted")
	}
}

func TestS3Storage(t *testing.T) {
	v := volume(t)
	bucket := unique()
	storagetest.TestStorage(t, v, params(bucket, "reuse_bucket", true), "s3t/"+unique())
	s, err := v.CreateStorage(storage.Config{KeyExpr: "**"}, params(bucket, "reuse_bucket", true, "on_closure", "destroy_bucket"))
	if err == nil {
		_ = s.Close()
	}
}

func TestS3RustCompat(t *testing.T) {
	v := volume(t)
	bucket := unique()
	b, err := v.CreateStorage(storage.Config{KeyExpr: "x/**", StripPrefix: "x"}, params(bucket, "on_closure", "destroy_bucket"))
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	st := params(bucket, "reuse_bucket", true)
	st["id"] = "s3"
	rust := storagetest.RustProbe(t, "s3", map[string]any{
		"volumes":  map[string]any{"s3": map[string]any{"url": os.Getenv("S3_URL")}},
		"storages": map[string]any{"s": map[string]any{"key_expr": "x/**", "strip_prefix": "x", "volume": st}}})
	storagetest.TestRustCompat(t, b, rust, unique(), storagetest.RustQuirks{GetMissingFails: true})
}
