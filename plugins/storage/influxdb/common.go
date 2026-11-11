// Package influxdb holds zenoh's InfluxDB storage backends: "influxdb" for
// InfluxDB 1.x (zenoh-rust's zenoh-backend-influxdb, InfluxQL over HTTP)
// and "influxdb2" for InfluxDB 2.x (zenoh-backend-influxdb-v2, Flux over
// HTTP). Importing the package registers both. They keep every value of
// every key (history "all"), in zenoh-rust's layout: one measurement per
// key ("@@none_key@@" for the strip prefix itself), a point per put or
// delete, at the zenoh timestamp's time, tagged kind=PUT or kind=DEL, with
// the fields timestamp (uhlc's text form), encoding_prefix (encoding ID),
// encoding_suffix (the encoding's text form), base64 (whether value is
// the base64 of a non-UTF-8 payload) and value.
//
// A query without a _time parameter gets each key's latest value (as
// zenoh-rust's v1 backend does; its v2 backend replies the whole history);
// with _time=[start..end] it gets every value in that range.
package influxdb

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/shabunin/tiny-zenoh-go/plugins/storage"
	"github.com/shabunin/tiny-zenoh-go/zenoh"
)

// NoneKey is the measurement of the strip prefix itself.
const NoneKey = "@@none_key@@"

func measurement(key string) string {
	if key == "" {
		return NoneKey
	}
	return key
}

func keyOf(m string) string {
	if m == NoneKey {
		return ""
	}
	return m
}

// nanos is the point time of a zenoh timestamp.
func nanos(ts zenoh.Timestamp) int64 { return ts.GoTime().UnixNano() }

// point is one stored put or delete.
type point struct {
	kind     string // PUT or DEL
	ts       zenoh.Timestamp
	encID    uint16
	encText  string
	base64   bool
	value    string
	nanos    int64
	measured string
}

func putPoint(key string, v storage.Value) point {
	p := point{kind: "PUT", ts: v.Timestamp, encID: v.Encoding.ID, encText: v.Encoding.String(),
		nanos: nanos(v.Timestamp), measured: measurement(key)}
	if utf8.Valid(v.Payload) {
		p.value = string(v.Payload)
	} else {
		p.base64, p.value = true, base64.StdEncoding.EncodeToString(v.Payload)
	}
	return p
}

func delPoint(key string, ts zenoh.Timestamp) point {
	return point{kind: "DEL", ts: ts, nanos: nanos(ts), measured: measurement(key)}
}

var (
	measurementEscaper = strings.NewReplacer(`,`, `\,`, ` `, `\ `, "\n", `\n`)
	stringEscaper      = strings.NewReplacer(`\`, `\\`, `"`, `\"`)
)

// line is the point in line protocol.
func (p point) line() string {
	var b strings.Builder
	b.WriteString(measurementEscaper.Replace(p.measured))
	b.WriteString(",kind=" + p.kind)
	b.WriteString(` timestamp="` + stringEscaper.Replace(storage.FormatTimestamp(p.ts)) + `"`)
	b.WriteString(",encoding_prefix=" + strconv.Itoa(int(p.encID)) + "i")
	b.WriteString(`,encoding_suffix="` + stringEscaper.Replace(p.encText) + `"`)
	b.WriteString(",base64=" + strconv.FormatBool(p.base64))
	b.WriteString(`,value="` + stringEscaper.Replace(p.value) + `"`)
	b.WriteString(" " + strconv.FormatInt(p.nanos, 10))
	return b.String()
}

// stored decodes a stored point's fields into a value.
func stored(ts string, encID int64, encText string, isB64 bool, value string) (storage.Value, error) {
	var v storage.Value
	t, err := storage.ParseTimestamp(ts)
	if err != nil {
		return v, err
	}
	v.Timestamp = t
	if encID < 0 || encID > 0xffff {
		return v, fmt.Errorf("encoding %d out of range", encID)
	}
	if encText == "" {
		v.Encoding = zenoh.Encoding{ID: uint16(encID)}
	} else {
		v.Encoding = zenoh.ParseEncoding(encText)
	}
	if isB64 {
		if v.Payload, err = base64.StdEncoding.DecodeString(value); err != nil {
			return v, err
		}
	} else {
		v.Payload = []byte(value)
	}
	return v, nil
}

// timeRange is the query's _time parameter, if any.
func timeRange(params string) (*zenoh.TimeRange, error) {
	s, ok := zenoh.Param(params, "_time")
	if !ok {
		return nil, nil
	}
	r, err := zenoh.ParseTimeRange(s)
	if err != nil {
		return nil, err
	}
	return &r, nil
}

func generatedDB() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return "zenoh_db_" + hex.EncodeToString(b[:])
}

// httpDo sends a request, returning the body of a 2xx response.
func httpDo(c *http.Client, req *http.Request) ([]byte, error) {
	resp, err := c.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode/100 != 2 {
		msg := strings.TrimSpace(string(body))
		if len(msg) > 300 {
			msg = msg[:300] + "..."
		}
		return nil, &httpError{fmt.Sprintf("%s %s: %s: %s", req.Method, req.URL.Path, resp.Status, msg), resp.StatusCode}
	}
	return body, nil
}

type httpError struct {
	msg    string
	status int
}

func (e *httpError) Error() string { return e.msg }

func isNotFound(err error) bool {
	he, ok := err.(*httpError)
	return ok && he.status == http.StatusNotFound
}

func newRequest(method, url string, body []byte) (*http.Request, error) {
	var r io.Reader
	if body != nil {
		r = bytes.NewReader(body)
	}
	return http.NewRequest(method, url, r)
}

var httpClient = &http.Client{Timeout: 30 * time.Second}

// credentials reads a user/secret pair: both or neither.
func credentials(p storage.Params, user, secret string) (string, string, error) {
	u, err := p.Private(user)
	if err != nil {
		return "", "", err
	}
	s, err := p.Private(secret)
	if err != nil {
		return "", "", err
	}
	if (u == "") != (s == "") {
		return "", "", fmt.Errorf("properties %q and %q must both be set, or neither", user, secret)
	}
	return u, s, nil
}

// storageParams reads the settings both versions share.
type storageParams struct {
	db       string
	createDB bool
	onClose  string // do_nothing, drop_db, drop_series
}

func readStorageParams(p storage.Params) (sp storageParams, err error) {
	if sp.onClose, err = p.String("on_closure", "do_nothing"); err != nil {
		return sp, err
	}
	switch sp.onClose {
	case "do_nothing", "drop_db", "drop_series":
	default:
		return sp, fmt.Errorf(`on_closure must be one of "do_nothing" (default), "drop_db" and "drop_series"`)
	}
	if sp.db, err = p.String("db", ""); err != nil {
		return sp, err
	}
	if sp.db == "" {
		sp.db, sp.createDB = generatedDB(), true
		return sp, nil
	}
	sp.createDB, err = p.Bool("create_db", false)
	return sp, err
}
