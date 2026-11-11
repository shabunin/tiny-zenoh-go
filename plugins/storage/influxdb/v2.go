package influxdb

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/shabunin/tiny-zenoh-go/plugins/storage"
	"github.com/shabunin/tiny-zenoh-go/zenoh"
)

// InfluxDB 2.x ("influxdb2" backend).
//
// Volume settings: url (required), org_id and token (required; preferably
// under "private"; org_id may be the organization's ID or name). Storage
// settings: db (the bucket; default: a generated zenoh_db_<hex> name,
// created), create_db (false: the bucket must exist), on_closure
// ("do_nothing", "drop_db", "drop_series"), org_id and token (the
// storage's own; default: the volume's).

func init() {
	storage.RegisterBackend("influxdb2", func(_ string, cfg map[string]any) (storage.Volume, error) {
		return OpenV2(cfg)
	})
}

// The extremes of InfluxDB's time.
const (
	minTime = "1677-09-21T00:12:44Z"
	maxTime = "2262-04-11T23:47:16Z"
	epoch   = "1970-01-01T00:00:00Z"
)

type v2Client struct {
	base, org, token string // org: the organization's ID
}

// V2Volume is an InfluxDB 2.x volume.
type V2Volume struct {
	url   string
	admin v2Client
}

// OpenV2 connects to an InfluxDB 2.x server.
func OpenV2(cfg map[string]any) (*V2Volume, error) {
	p := storage.Params(cfg)
	u, err := p.String("url", "")
	if err != nil || u == "" {
		return nil, errors.New("influxdb2: the volume needs a \"url\"")
	}
	org, token, err := credentials(p, "org_id", "token")
	if err != nil {
		return nil, fmt.Errorf("influxdb2: %w", err)
	}
	if org == "" {
		return nil, errors.New("influxdb2: the volume needs \"org_id\" and \"token\"")
	}
	v := &V2Volume{url: strings.TrimRight(u, "/")}
	if v.admin, err = newV2Client(v.url, org, token); err != nil {
		return nil, fmt.Errorf("influxdb2: %s: %w", u, err)
	}
	return v, nil
}

// newV2Client resolves org (an ID or a name) to its ID.
func newV2Client(base, org, token string) (v2Client, error) {
	c := v2Client{base: base, token: token}
	b, err := c.do("GET", "/api/v2/orgs", nil, nil, "")
	if err != nil {
		return c, err
	}
	var r struct {
		Orgs []struct{ ID, Name string } `json:"orgs"`
	}
	if err := json.Unmarshal(b, &r); err != nil {
		return c, err
	}
	for _, o := range r.Orgs {
		if o.ID == org || o.Name == org {
			c.org = o.ID
			return c, nil
		}
	}
	return c, fmt.Errorf("no organization %q", org)
}

func (c v2Client) do(method, path string, q url.Values, body []byte, contentType string) ([]byte, error) {
	u := c.base + path
	if len(q) > 0 {
		u += "?" + q.Encode()
	}
	req, err := newRequest(method, u, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Token "+c.token)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	return httpDo(httpClient, req)
}

func (v *V2Volume) Capability() storage.Capability {
	return storage.Capability{Durable: true, History: storage.HistoryAll}
}

func (v *V2Volume) Status() map[string]any { return map[string]any{"url": v.url} }
func (v *V2Volume) Close() error           { return nil }

func (c v2Client) bucketID(name string) (string, error) {
	b, err := c.do("GET", "/api/v2/buckets", url.Values{"orgID": {c.org}, "name": {name}}, nil, "")
	if isNotFound(err) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	var r struct {
		Buckets []struct{ ID, Name string } `json:"buckets"`
	}
	if err := json.Unmarshal(b, &r); err != nil {
		return "", err
	}
	for _, bk := range r.Buckets {
		if bk.Name == name {
			return bk.ID, nil
		}
	}
	return "", nil
}

func (v *V2Volume) CreateStorage(cfg storage.Config, params map[string]any) (storage.Backend, error) {
	p := storage.Params(params)
	sp, err := readStorageParams(p)
	if err != nil {
		return nil, fmt.Errorf("influxdb2: %w", err)
	}
	org, token, err := credentials(p, "org_id", "token")
	if err != nil {
		return nil, fmt.Errorf("influxdb2: %w", err)
	}
	c := v.admin
	if org != "" {
		if c, err = newV2Client(v.url, org, token); err != nil {
			return nil, fmt.Errorf("influxdb2: %w", err)
		}
	}
	id, err := c.bucketID(sp.db)
	if err != nil {
		return nil, fmt.Errorf("influxdb2: %w", err)
	}
	if id == "" {
		if !sp.createDB {
			return nil, fmt.Errorf("influxdb2: bucket %q does not exist", sp.db)
		}
		body, _ := json.Marshal(map[string]any{"orgID": c.org, "name": sp.db, "retentionRules": []any{}})
		if _, err := v.admin.do("POST", "/api/v2/buckets", nil, body, "application/json"); err != nil {
			return nil, fmt.Errorf("influxdb2: creating bucket %q: %w", sp.db, err)
		}
	}
	return &V2Storage{c: c, admin: v.admin, bucket: sp.db, onClose: sp.onClose}, nil
}

// V2Storage is a storage in an InfluxDB 2.x bucket.
type V2Storage struct {
	c, admin v2Client
	bucket   string
	onClose  string
}

// Bucket is the storage's bucket.
func (s *V2Storage) Bucket() string { return s.bucket }

// flux quotes a Flux string literal.
func flux(s string) string {
	return `"` + strings.NewReplacer(`\`, `\\`, `"`, `\"`, `${`, `\${`).Replace(s) + `"`
}

// query runs a Flux query, returning its rows as column → value.
func (s *V2Storage) query(q string) ([]map[string]string, error) {
	body, _ := json.Marshal(map[string]any{"query": q, "type": "flux",
		"dialect": map[string]any{"header": true, "annotations": []string{}}})
	b, err := s.c.do("POST", "/api/v2/query", url.Values{"orgID": {s.c.org}}, body, "application/json")
	if err != nil {
		return nil, err
	}
	r := csv.NewReader(strings.NewReader(string(b)))
	r.FieldsPerRecord = -1
	var header []string
	var out []map[string]string
	for {
		rec, err := r.Read()
		if err == io.EOF {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		if len(rec) > 1 && rec[1] == "result" {
			header = rec
			continue
		}
		if header == nil {
			continue
		}
		row := map[string]string{}
		for i, h := range header {
			if i < len(rec) && h != "" {
				row[h] = rec[i]
			}
		}
		out = append(out, row)
	}
}

func (s *V2Storage) write(lines string) error {
	_, err := s.c.do("POST", "/api/v2/write",
		url.Values{"orgID": {s.c.org}, "bucket": {s.bucket}, "precision": {"ns"}}, []byte(lines), "text/plain")
	return err
}

func (s *V2Storage) deletePoints(start, stop, predicate string) error {
	m := map[string]any{"start": start, "stop": stop}
	if predicate != "" {
		m["predicate"] = predicate
	}
	body, _ := json.Marshal(m)
	_, err := s.c.do("POST", "/api/v2/delete", url.Values{"orgID": {s.c.org}, "bucket": {s.bucket}}, body, "application/json")
	return err
}

func (s *V2Storage) from() string {
	return "from(bucket: " + flux(s.bucket) + ")\n"
}

func (s *V2Storage) Put(key string, v storage.Value) (storage.Result, error) {
	m := measurement(key)
	rows, err := s.query(s.from() + `|> range(start: ` + epoch + `, stop: ` + maxTime + `)
|> filter(fn: (r) => r._measurement == ` + flux(m) + ` and r.kind == "DEL" and r._field == "timestamp")
|> last()`)
	if err != nil {
		return storage.Outdated, fmt.Errorf("influxdb2: %w", err)
	}
	for _, r := range rows {
		if del, err := storage.ParseTimestamp(r["_value"]); err == nil && tsLess(v.Timestamp, del) {
			return storage.Outdated, nil
		}
	}
	if err := s.write(putPoint(key, v).line()); err != nil {
		return storage.Outdated, fmt.Errorf("influxdb2: put %s: %w", m, err)
	}
	return storage.Inserted, nil
}

func rfc3339(ns int64) string { return time.Unix(0, ns).UTC().Format(time.RFC3339Nano) }

func (s *V2Storage) Delete(key string, ts zenoh.Timestamp) (storage.Result, error) {
	m := measurement(key)
	pred := `_measurement="` + strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(m) + `"`
	if err := s.deletePoints(epoch, rfc3339(nanos(ts)), pred); err != nil {
		return storage.Outdated, fmt.Errorf("influxdb2: delete %s: %w", m, err)
	}
	if err := s.write(delPoint(key, ts).line()); err != nil {
		return storage.Outdated, fmt.Errorf("influxdb2: delete %s: %w", m, err)
	}
	return storage.Deleted, nil
}

// boundV2 is a Flux range bound; plus1 moves it 1ns later (Flux ranges
// include their start and exclude their stop).
func boundV2(b zenoh.TimeBound, plus1 bool) string {
	var d time.Duration
	if plus1 {
		d = time.Nanosecond
	}
	if b.Now {
		return strconv.FormatInt(int64(b.Offset+d), 10) + "ns"
	}
	return b.At.Add(d).UTC().Format(time.RFC3339Nano)
}

func (s *V2Storage) Get(key, params string) ([]storage.Value, error) {
	q := s.from()
	tr, err := timeRange(params)
	if err != nil {
		tr = nil
	}
	start, stop := epoch, maxTime
	if tr != nil {
		if tr.Start.Bounded {
			start = boundV2(tr.Start, tr.Start.Exclusive)
		}
		if tr.End.Bounded {
			stop = boundV2(tr.End, !tr.End.Exclusive)
		}
	}
	q += `|> range(start: ` + start + `, stop: ` + stop + `)
|> filter(fn: (r) => r._measurement == ` + flux(measurement(key)) + ` and r.kind == "PUT")
`
	if tr == nil {
		q += "|> last()\n"
	}
	q += `|> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")`
	rows, err := s.query(q)
	if err != nil {
		return nil, fmt.Errorf("influxdb2: %w", err)
	}
	var out []storage.Value
	for _, r := range rows {
		id, _ := strconv.ParseInt(r["encoding_prefix"], 10, 64)
		v, err := stored(r["timestamp"], id, r["encoding_suffix"], r["base64"] == "true", r["value"])
		if err == nil {
			out = append(out, v)
		}
	}
	return out, nil
}

func (s *V2Storage) Entries() ([]storage.Entry, error) {
	rows, err := s.query(s.from() + `|> range(start: ` + epoch + `, stop: ` + maxTime + `)
|> filter(fn: (r) => r._field == "timestamp")
|> group(columns: ["_measurement"])
|> last()`)
	if err != nil {
		return nil, fmt.Errorf("influxdb2: %w", err)
	}
	var out []storage.Entry
	for _, r := range rows {
		if r["kind"] == "DEL" {
			continue
		}
		if ts, err := storage.ParseTimestamp(r["_value"]); err == nil {
			out = append(out, storage.Entry{Key: keyOf(r["_measurement"]), Timestamp: ts})
		}
	}
	return out, nil
}

func (s *V2Storage) Close() error {
	switch s.onClose {
	case "drop_db":
		id, err := s.admin.bucketID(s.bucket)
		if err != nil || id == "" {
			return err
		}
		_, err = s.admin.do("DELETE", "/api/v2/buckets/"+id, nil, nil, "")
		return err
	case "drop_series":
		return s.deletePoints(minTime, maxTime, "")
	}
	return nil
}
