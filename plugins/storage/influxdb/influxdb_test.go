package influxdb

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/shabunin/tiny-zenoh-go/plugins/storage"
	"github.com/shabunin/tiny-zenoh-go/plugins/storage/storagetest"
)

// The servers of these tests (throwaway credentials, local only):
//
//	docker run -d --name zinflux1 -p 127.0.0.1:18086:8086 -e INFLUXDB_HTTP_AUTH_ENABLED=true \
//	  -e INFLUXDB_ADMIN_USER=zadmin -e INFLUXDB_ADMIN_PASSWORD=throwaway-pw influxdb:1.8
//	docker run -d --name zinflux2 -p 127.0.0.1:18087:8086 -e DOCKER_INFLUXDB_INIT_MODE=setup \
//	  -e DOCKER_INFLUXDB_INIT_USERNAME=zadmin -e DOCKER_INFLUXDB_INIT_PASSWORD=throwaway-pw \
//	  -e DOCKER_INFLUXDB_INIT_ORG=zorg -e DOCKER_INFLUXDB_INIT_BUCKET=zinit \
//	  -e DOCKER_INFLUXDB_INIT_ADMIN_TOKEN=throwaway-token influxdb:2.7
//
//	INFLUXDB1_URL=http://127.0.0.1:18086 INFLUXDB2_URL=http://127.0.0.1:18087 go test ./plugins/storage/influxdb

func v1Config(t *testing.T) map[string]any {
	u := os.Getenv("INFLUXDB1_URL")
	if u == "" {
		t.Skip("INFLUXDB1_URL not set")
	}
	return map[string]any{"url": u, "private": map[string]any{"username": "zadmin", "password": "throwaway-pw"}}
}

func v2Config(t *testing.T) map[string]any {
	u := os.Getenv("INFLUXDB2_URL")
	if u == "" {
		t.Skip("INFLUXDB2_URL not set")
	}
	return map[string]any{"url": u, "private": map[string]any{"org_id": "zorg", "token": "throwaway-token"}}
}

func unique() string { return fmt.Sprintf("t%d", time.Now().UnixNano()) }

func TestInfluxDBv1Backend(t *testing.T) {
	vol, err := storage.OpenVolume("influxdb", "influx", v1Config(t))
	if err != nil {
		t.Fatal(err)
	}
	b, err := vol.CreateStorage(storage.Config{KeyExpr: "x/**"}, map[string]any{"on_closure": "drop_db"})
	if err != nil {
		t.Fatal(err)
	}
	storagetest.TestBackend(t, b, vol.Capability(), unique())
	db := b.(*V1Storage).DB()
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	dbs, _ := vol.(*V1Volume).admin.databases()
	for _, d := range dbs {
		if d == db {
			t.Errorf("database %s not dropped", db)
		}
	}
	if _, err := vol.CreateStorage(storage.Config{}, map[string]any{"db": "zenoh_absent_db"}); err == nil {
		t.Error("storage on a missing database without create_db")
	}
	if _, err := OpenV1(map[string]any{"url": os.Getenv("INFLUXDB1_URL"), "username": "zadmin", "password": "wrong"}); err == nil {
		t.Error("wrong password accepted")
	}
}

func TestInfluxDBv1Storage(t *testing.T) {
	vol, err := OpenV1(v1Config(t))
	if err != nil {
		t.Fatal(err)
	}
	db := "zenoh_test_" + unique()
	storagetest.TestStorage(t, vol, map[string]any{"db": db, "create_db": true}, "influx1/"+unique())
	_, _ = vol.admin.query("DROP DATABASE " + ident(db))
}

func TestInfluxDBv2Backend(t *testing.T) {
	vol, err := storage.OpenVolume("influxdb2", "influx2", v2Config(t))
	if err != nil {
		t.Fatal(err)
	}
	b, err := vol.CreateStorage(storage.Config{KeyExpr: "x/**"}, map[string]any{"on_closure": "drop_db"})
	if err != nil {
		t.Fatal(err)
	}
	storagetest.TestBackend(t, b, vol.Capability(), unique())
	bucket := b.(*V2Storage).Bucket()
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if id, _ := vol.(*V2Volume).admin.bucketID(bucket); id != "" {
		t.Errorf("bucket %s not dropped", bucket)
	}
	if _, err := vol.CreateStorage(storage.Config{}, map[string]any{"db": "zenoh_absent_db"}); err == nil {
		t.Error("storage on a missing bucket without create_db")
	}
	if _, err := OpenV2(map[string]any{"url": os.Getenv("INFLUXDB2_URL"), "org_id": "zorg", "token": "wrong"}); err == nil {
		t.Error("wrong token accepted")
	}
}

func TestInfluxDBv2Storage(t *testing.T) {
	vol, err := OpenV2(v2Config(t))
	if err != nil {
		t.Fatal(err)
	}
	db := "zenoh_test_" + unique()
	storagetest.TestStorage(t, vol, map[string]any{"db": db, "create_db": true}, "influx2/"+unique())
	if id, _ := vol.admin.bucketID(db); id != "" {
		_, _ = vol.admin.do("DELETE", "/api/v2/buckets/"+id, nil, nil, "")
	}
}

func rustConfig(backend string, vol, st map[string]any) map[string]any {
	st["id"] = backend
	return map[string]any{"volumes": map[string]any{backend: vol},
		"storages": map[string]any{"s": map[string]any{"key_expr": "x/**", "strip_prefix": "x", "volume": st}}}
}

func TestInfluxDBv1RustCompat(t *testing.T) {
	vc := v1Config(t)
	db := "zenoh_compat_" + unique()
	st := map[string]any{"db": db, "create_db": true, "private": map[string]any{"username": "zadmin", "password": "throwaway-pw"}}
	rust := storagetest.RustProbe(t, "influxdb", rustConfig("influxdb", vc, st))
	vol, err := OpenV1(vc)
	if err != nil {
		t.Fatal(err)
	}
	b, err := vol.CreateStorage(storage.Config{KeyExpr: "x/**", StripPrefix: "x"}, map[string]any{"db": db, "create_db": true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = vol.admin.query("DROP DATABASE " + ident(db)) }()
	storagetest.TestRustCompat(t, b, rust, unique(), storagetest.RustQuirks{ListsDeleted: true})
}

func TestInfluxDBv2RustCompat(t *testing.T) {
	vc := v2Config(t)
	db := "zenoh_compat_" + unique()
	vol, err := OpenV2(vc)
	if err != nil {
		t.Fatal(err)
	}
	b, err := vol.CreateStorage(storage.Config{KeyExpr: "x/**", StripPrefix: "x"}, map[string]any{"db": db, "create_db": true, "on_closure": "drop_db"})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	rust := storagetest.RustProbe(t, "influxdb2", rustConfig("influxdb2", vc, map[string]any{"db": db}))
	storagetest.TestRustCompat(t, b, rust, unique(), storagetest.RustQuirks{ListsDeleted: true})
}
