package influxdb

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/shabunin/tiny-zenoh-go/plugins/storage"
	"github.com/shabunin/tiny-zenoh-go/zenoh"
)

// InfluxDB 1.x ("influxdb" backend).
//
// Volume settings: url (required), username and password (an admin's,
// for creating and dropping databases; preferably under "private").
// Storage settings: db (default: a generated zenoh_db_<hex> name, created),
// create_db (false: the database must exist), on_closure ("do_nothing",
// "drop_db", "drop_series"), username and password (the storage's own
// credentials; default: the volume's).

// DropMeasurementDelay is how long after a delete its key's measurement is
// dropped, if nothing was put since (zenoh-rust: 5s).
var DropMeasurementDelay = 5 * time.Second

func init() {
	storage.RegisterBackend("influxdb", func(_ string, cfg map[string]any) (storage.Volume, error) {
		return OpenV1(cfg)
	})
}

// V1Volume is an InfluxDB 1.x volume.
type V1Volume struct {
	url   string
	admin v1Client
}

type v1Client struct {
	base, db, user, pass string
}

// OpenV1 connects to an InfluxDB 1.x server.
func OpenV1(cfg map[string]any) (*V1Volume, error) {
	p := storage.Params(cfg)
	u, err := p.String("url", "")
	if err != nil || u == "" {
		return nil, errors.New("influxdb: the volume needs a \"url\"")
	}
	user, pass, err := credentials(p, "username", "password")
	if err != nil {
		return nil, fmt.Errorf("influxdb: %w", err)
	}
	v := &V1Volume{url: strings.TrimRight(u, "/"), admin: v1Client{base: strings.TrimRight(u, "/"), user: user, pass: pass}}
	if _, err := v.admin.databases(); err != nil {
		return nil, fmt.Errorf("influxdb: %s: %w", u, err)
	}
	return v, nil
}

func (v *V1Volume) Capability() storage.Capability {
	return storage.Capability{Durable: true, History: storage.HistoryAll}
}

func (v *V1Volume) Status() map[string]any { return map[string]any{"url": v.url} }
func (v *V1Volume) Close() error           { return nil }

func (v *V1Volume) CreateStorage(cfg storage.Config, params map[string]any) (storage.Backend, error) {
	p := storage.Params(params)
	sp, err := readStorageParams(p)
	if err != nil {
		return nil, fmt.Errorf("influxdb: %w", err)
	}
	user, pass, err := credentials(p, "username", "password")
	if err != nil {
		return nil, fmt.Errorf("influxdb: %w", err)
	}
	c := v1Client{base: v.url, db: sp.db, user: v.admin.user, pass: v.admin.pass}
	if user != "" {
		c.user, c.pass = user, pass
	}
	dbs, err := c.databases()
	if err != nil {
		return nil, fmt.Errorf("influxdb: %w", err)
	}
	exists := false
	for _, d := range dbs {
		exists = exists || d == sp.db
	}
	if !exists {
		if !sp.createDB {
			return nil, fmt.Errorf("influxdb: database %q does not exist", sp.db)
		}
		if _, err := v.admin.query("CREATE DATABASE " + ident(sp.db)); err != nil {
			return nil, fmt.Errorf("influxdb: creating database %q: %w", sp.db, err)
		}
		if user != "" {
			if _, err := v.admin.query("GRANT ALL ON " + ident(sp.db) + " TO " + ident(user)); err != nil {
				return nil, fmt.Errorf("influxdb: granting %s access to %q: %w", user, sp.db, err)
			}
		}
	}
	admin := v.admin
	admin.db = sp.db
	return &V1Storage{c: c, admin: admin, onClose: sp.onClose, drops: map[string]*time.Timer{}}, nil
}

// V1Storage is a storage in an InfluxDB 1.x database.
type V1Storage struct {
	c, admin v1Client
	onClose  string
	mu       sync.Mutex
	drops    map[string]*time.Timer
	closed   bool
}

// DB is the storage's database.
func (s *V1Storage) DB() string { return s.c.db }

// ident quotes an InfluxQL identifier.
func ident(s string) string {
	return `"` + strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(s) + `"`
}

// lit quotes an InfluxQL string literal.
func lit(s string) string {
	return `'` + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(s) + `'`
}

type v1Series struct {
	Name    string   `json:"name"`
	Columns []string `json:"columns"`
	Values  [][]any  `json:"values"`
}

func (c v1Client) endpoint(path string, q url.Values) string {
	if c.db != "" {
		q.Set("db", c.db)
	}
	return c.base + path + "?" + q.Encode()
}

func (c v1Client) send(path string, q url.Values, body []byte, contentType string) ([]byte, error) {
	req, err := newRequest("POST", c.endpoint(path, q), body)
	if err != nil {
		return nil, err
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if c.user != "" {
		req.SetBasicAuth(c.user, c.pass)
	}
	return httpDo(httpClient, req)
}

// query runs InfluxQL statements, returning the series of the first.
func (c v1Client) query(q string) ([]v1Series, error) {
	form := url.Values{"q": {q}}
	b, err := c.send("/query", url.Values{"epoch": {"ns"}}, []byte(form.Encode()), "application/x-www-form-urlencoded")
	if err != nil {
		return nil, err
	}
	var r struct {
		Error   string `json:"error"`
		Results []struct {
			Error  string     `json:"error"`
			Series []v1Series `json:"series"`
		} `json:"results"`
	}
	d := json.NewDecoder(strings.NewReader(string(b)))
	d.UseNumber()
	if err := d.Decode(&r); err != nil {
		return nil, fmt.Errorf("decoding the reply to %q: %w", q, err)
	}
	if r.Error != "" {
		return nil, fmt.Errorf("%q: %s", q, r.Error)
	}
	if len(r.Results) == 0 {
		return nil, nil
	}
	if e := r.Results[0].Error; e != "" {
		return nil, fmt.Errorf("%q: %s", q, e)
	}
	return r.Results[0].Series, nil
}

func (c v1Client) write(lines string) error {
	_, err := c.send("/write", url.Values{"precision": {"ns"}}, []byte(lines), "text/plain")
	return err
}

func (c v1Client) databases() ([]string, error) {
	c.db = ""
	series, err := c.query("SHOW DATABASES")
	if err != nil {
		return nil, err
	}
	var out []string
	for _, s := range series {
		for _, row := range s.Values {
			if len(row) > 0 {
				if n, ok := row[0].(string); ok {
					out = append(out, n)
				}
			}
		}
	}
	return out, nil
}

// rows maps a series' rows to column name → value.
func rows(s v1Series) []map[string]any {
	out := make([]map[string]any, 0, len(s.Values))
	for _, vals := range s.Values {
		m := map[string]any{}
		for i, c := range s.Columns {
			if i < len(vals) {
				m[c] = vals[i]
			}
		}
		out = append(out, m)
	}
	return out
}

func str(m map[string]any, k string) string {
	s, _ := m[k].(string)
	return s
}

func (s *V1Storage) deletion(m string) (*zenoh.Timestamp, error) {
	series, err := s.c.query(`SELECT "timestamp" FROM ` + ident(m) + ` WHERE kind='DEL' ORDER BY time DESC LIMIT 1`)
	if err != nil {
		return nil, err
	}
	for _, se := range series {
		for _, r := range rows(se) {
			ts, err := storage.ParseTimestamp(str(r, "timestamp"))
			if err != nil {
				return nil, err
			}
			return &ts, nil
		}
	}
	return nil, nil
}

func (s *V1Storage) Put(key string, v storage.Value) (storage.Result, error) {
	m := measurement(key)
	del, err := s.deletion(m)
	if err != nil {
		return storage.Outdated, fmt.Errorf("influxdb: %w", err)
	}
	if del != nil && tsLess(v.Timestamp, *del) {
		return storage.Outdated, nil
	}
	if err := s.c.write(putPoint(key, v).line()); err != nil {
		return storage.Outdated, fmt.Errorf("influxdb: put %s: %w", m, err)
	}
	return storage.Inserted, nil
}

func tsLess(a, b zenoh.Timestamp) bool {
	if a.Time != b.Time {
		return a.Time < b.Time
	}
	var x, y [16]byte
	copy(x[:], a.ID.Bytes())
	copy(y[:], b.ID.Bytes())
	return string(x[:]) < string(y[:])
}

func (s *V1Storage) Delete(key string, ts zenoh.Timestamp) (storage.Result, error) {
	m := measurement(key)
	// Points older than the delete go; a DEL point keeps older puts out.
	if _, err := s.c.query(`DELETE FROM ` + ident(m) + ` WHERE time < ` + strconv.FormatInt(nanos(ts), 10)); err != nil {
		return storage.Outdated, fmt.Errorf("influxdb: delete %s: %w", m, err)
	}
	if err := s.c.write(delPoint(key, ts).line()); err != nil {
		return storage.Outdated, fmt.Errorf("influxdb: delete %s: %w", m, err)
	}
	s.scheduleDrop(m)
	return storage.Deleted, nil
}

// scheduleDrop drops a deleted key's measurement later, if no put came
// since.
func (s *V1Storage) scheduleDrop(m string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	if t := s.drops[m]; t != nil {
		t.Stop()
	}
	s.drops[m] = time.AfterFunc(DropMeasurementDelay, func() {
		s.mu.Lock()
		delete(s.drops, m)
		closed := s.closed
		s.mu.Unlock()
		if closed {
			return
		}
		series, err := s.c.query(`SELECT "kind" FROM ` + ident(m) + ` WHERE kind!='DEL' LIMIT 1`)
		if err != nil || len(series) > 0 {
			return
		}
		_, _ = s.c.query(`DROP MEASUREMENT ` + ident(m))
	})
}

func timeExprV1(b zenoh.TimeBound) string {
	if !b.Now {
		return lit(b.At.UTC().Format(time.RFC3339Nano))
	}
	return "now()" + fmt.Sprintf("%+d", b.Offset.Nanoseconds()) + "ns"
}

func (s *V1Storage) Get(key, params string) ([]storage.Value, error) {
	q := `SELECT * FROM ` + ident(measurement(key)) + ` WHERE kind!='DEL'`
	tr, err := timeRange(params)
	if err != nil {
		tr = nil // as zenoh-rust: the latest value
	}
	if tr == nil {
		q += " ORDER BY time DESC LIMIT 1"
	} else {
		if b := tr.Start; b.Bounded {
			op := " >= "
			if b.Exclusive {
				op = " > "
			}
			q += " AND time" + op + timeExprV1(b)
		}
		if b := tr.End; b.Bounded {
			op := " <= "
			if b.Exclusive {
				op = " < "
			}
			q += " AND time" + op + timeExprV1(b)
		}
	}
	series, err := s.c.query(q)
	if err != nil {
		return nil, fmt.Errorf("influxdb: %w", err)
	}
	var out []storage.Value
	for _, se := range series {
		for _, r := range rows(se) {
			n, _ := r["encoding_prefix"].(json.Number)
			id, _ := n.Int64()
			b64, _ := r["base64"].(bool)
			v, err := stored(str(r, "timestamp"), id, str(r, "encoding_suffix"), b64, str(r, "value"))
			if err != nil {
				continue
			}
			out = append(out, v)
		}
	}
	return out, nil
}

func (s *V1Storage) Entries() ([]storage.Entry, error) {
	series, err := s.c.query(`SELECT "timestamp", "kind" FROM /.*/ ORDER BY time DESC LIMIT 1`)
	if err != nil {
		return nil, fmt.Errorf("influxdb: %w", err)
	}
	var out []storage.Entry
	for _, se := range series {
		for _, r := range rows(se) {
			if str(r, "kind") == "DEL" {
				continue
			}
			if ts, err := storage.ParseTimestamp(str(r, "timestamp")); err == nil {
				out = append(out, storage.Entry{Key: keyOf(se.Name), Timestamp: ts})
			}
		}
	}
	return out, nil
}

func (s *V1Storage) Close() error {
	s.mu.Lock()
	s.closed = true
	for _, t := range s.drops {
		t.Stop()
	}
	s.mu.Unlock()
	switch s.onClose {
	case "drop_db":
		_, err := s.admin.query("DROP DATABASE " + ident(s.c.db))
		return err
	case "drop_series":
		_, err := s.c.query("DROP SERIES FROM /.*/")
		return err
	}
	return nil
}
