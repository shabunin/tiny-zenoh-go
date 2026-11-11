// Package s3 is zenoh's S3 storage backend (zenoh-rust's
// zenoh-backend-s3): each key's value is an object of a bucket, on Amazon
// S3 or any S3-compatible server (MinIO, SeaweedFS, ...). Importing the
// package registers the "s3" backend. Requests are path-style and signed
// with AWS Signature Version 4.
//
// Volume settings: url (the endpoint; default https://s3.<region>.amazonaws.com),
// region (default us-east-1), tls: {root_ca_certificate_file or
// root_ca_certificate_base64} (a CA to trust besides the system's).
// Storage settings: bucket (required), access_key and secret_key
// (preferably under "private"; default: $AWS_ACCESS_KEY_ID,
// $AWS_SECRET_ACCESS_KEY and $AWS_SESSION_TOKEN, else unsigned requests),
// reuse_bucket (false: the bucket must not exist yet), read_only (false),
// on_closure ("do_nothing" or "destroy_bucket").
//
// As zenoh-rust: the object of a key is the key, or "/<key>" when the
// storage has a strip_prefix; the strip prefix itself is "@@none_key@@";
// the timestamp is the object's x-amz-meta-zenoh-timestamp metadata and
// the encoding its Content-Encoding.
package s3

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/shabunin/tiny-zenoh-go/keyexpr"
	"github.com/shabunin/tiny-zenoh-go/plugins/storage"
	"github.com/shabunin/tiny-zenoh-go/zenoh"
)

const (
	// NoneKey is the object of the strip prefix itself.
	NoneKey         = "@@none_key@@"
	timestampMeta   = "X-Amz-Meta-Zenoh-Timestamp"
	timestampMetaV0 = "X-Amz-Meta-Timestamp_uhlc" // older zenoh-rust versions'
)

func init() {
	storage.RegisterBackend("s3", func(_ string, cfg map[string]any) (storage.Volume, error) {
		return NewVolume(cfg)
	})
}

// Volume is an S3 endpoint.
type Volume struct {
	endpoint *url.URL
	region   string
	http     *http.Client
}

// NewVolume reads a volume's settings.
func NewVolume(cfg map[string]any) (*Volume, error) {
	p := storage.Params(cfg)
	region, err := p.String("region", "")
	if err != nil {
		return nil, fmt.Errorf("s3: %w", err)
	}
	if region == "" {
		region = "us-east-1"
	}
	ep, err := p.String("url", "")
	if err != nil {
		return nil, fmt.Errorf("s3: %w", err)
	}
	if ep == "" {
		ep = "https://s3." + region + ".amazonaws.com"
	}
	u, err := url.Parse(strings.TrimRight(ep, "/"))
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return nil, fmt.Errorf("s3: url %q: not an http(s) URL", ep)
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.DisableCompression = true // Content-Encoding is the zenoh encoding
	tc, err := p.Map("tls")
	if err != nil {
		return nil, fmt.Errorf("s3: %w", err)
	}
	if tc != nil {
		pool, err := x509.SystemCertPool()
		if err != nil {
			pool = x509.NewCertPool()
		}
		var pemData []byte
		if f, _ := tc.Private("root_ca_certificate_file"); f != "" {
			if pemData, err = os.ReadFile(f); err != nil {
				return nil, fmt.Errorf("s3: tls: %w", err)
			}
		} else if b, _ := tc.Private("root_ca_certificate_base64"); b != "" {
			if pemData, err = base64.StdEncoding.DecodeString(b); err != nil {
				return nil, fmt.Errorf("s3: tls: root_ca_certificate_base64: %w", err)
			}
		}
		if pemData != nil {
			if blk, _ := pem.Decode(pemData); blk == nil || !pool.AppendCertsFromPEM(pemData) {
				return nil, errors.New("s3: tls: no certificate in the root CA")
			}
		}
		tr.TLSClientConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	}
	return &Volume{endpoint: u, region: region, http: &http.Client{Transport: tr, Timeout: 60 * time.Second}}, nil
}

func (v *Volume) Capability() storage.Capability {
	return storage.Capability{Durable: true, History: storage.HistoryLatest}
}

func (v *Volume) Status() map[string]any {
	return map[string]any{"url": v.endpoint.String(), "region": v.region}
}

func (v *Volume) Close() error { return nil }

// CreateStorage opens (creating it unless reused) the storage's bucket.
func (v *Volume) CreateStorage(cfg storage.Config, params map[string]any) (storage.Backend, error) {
	p := storage.Params(params)
	s := &Storage{c: client{v: v}, prefix: cfg.StripPrefix, keyExpr: cfg.KeyExpr}
	var err error
	if s.c.bucket, err = p.String("bucket", ""); err != nil || s.c.bucket == "" {
		return nil, errors.New("s3: property \"bucket\" was not specified")
	}
	if s.readOnly, err = p.Bool("read_only", false); err != nil {
		return nil, fmt.Errorf("s3: %w", err)
	}
	reuse, err := p.Bool("reuse_bucket", false)
	if err != nil {
		return nil, fmt.Errorf("s3: %w", err)
	}
	switch oc, err := p.String("on_closure", "do_nothing"); {
	case err != nil:
		return nil, fmt.Errorf("s3: %w", err)
	case oc == "destroy_bucket":
		s.destroy = true
	case oc != "do_nothing":
		return nil, errors.New(`s3: on_closure must be either "do_nothing" (default) or "destroy_bucket"`)
	}
	ak, err := p.Private("access_key")
	if err != nil {
		return nil, fmt.Errorf("s3: %w", err)
	}
	sk, err := p.Private("secret_key")
	if err != nil {
		return nil, fmt.Errorf("s3: %w", err)
	}
	switch {
	case ak != "" && sk != "":
		s.c.creds = &Credentials{AccessKey: ak, SecretKey: sk}
	case ak != "" || sk != "":
		return nil, errors.New("s3: properties \"access_key\" and \"secret_key\" must both be set, or neither")
	case os.Getenv("AWS_ACCESS_KEY_ID") != "":
		s.c.creds = &Credentials{AccessKey: os.Getenv("AWS_ACCESS_KEY_ID"),
			SecretKey: os.Getenv("AWS_SECRET_ACCESS_KEY"), SessionToken: os.Getenv("AWS_SESSION_TOKEN")}
	}
	if err := s.c.createBucket(reuse); err != nil {
		return nil, err
	}
	return s, nil
}

// client makes the requests of one bucket.
type client struct {
	v      *Volume
	bucket string
	creds  *Credentials
}

type s3Error struct {
	status  int
	code    string
	message string
}

func (e *s3Error) Error() string {
	if e.code == "" {
		return fmt.Sprintf("s3: HTTP %d", e.status)
	}
	return fmt.Sprintf("s3: %s: %s", e.code, e.message)
}

func notFound(err error) bool {
	var e *s3Error
	return errors.As(err, &e) && (e.status == http.StatusNotFound || e.code == "NoSuchKey" || e.code == "NoSuchBucket")
}

// do sends a request on the bucket (object: "" for the bucket itself).
func (c client) do(method, object string, q url.Values, body []byte, hdr http.Header) (*http.Response, []byte, error) {
	path := c.v.endpoint.Path + "/" + c.bucket
	if object != "" {
		path += "/" + object
	}
	u := *c.v.endpoint
	u.Path, u.RawPath = path, uriEncode(path, true)
	u.RawQuery = q.Encode()
	req, err := http.NewRequest(method, u.String(), bytes.NewReader(body))
	if err != nil {
		return nil, nil, err
	}
	req.URL.Path, req.URL.RawPath = path, uriEncode(path, true)
	for k, vs := range hdr {
		req.Header[k] = vs
	}
	if body != nil {
		req.ContentLength = int64(len(body))
	} else {
		req.Body = http.NoBody
	}
	if c.creds != nil {
		sign(req, *c.creds, c.v.region, uriEncode(path, true), sha256Hex(body), time.Now())
	}
	resp, err := c.v.http.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, err
	}
	if resp.StatusCode/100 != 2 {
		e := &s3Error{status: resp.StatusCode}
		var x struct {
			Code, Message string
		}
		if xml.Unmarshal(b, &x) == nil {
			e.code, e.message = x.Code, x.Message
		}
		return resp, nil, e
	}
	return resp, b, nil
}

func (c client) createBucket(reuse bool) error {
	_, _, err := c.do("HEAD", "", nil, nil, nil)
	switch {
	case err == nil && reuse:
		return nil
	case err == nil:
		return fmt.Errorf("s3: bucket %q already exists while reuse_bucket is false", c.bucket)
	case !notFound(err):
		return fmt.Errorf("s3: checking bucket %q: %w", c.bucket, err)
	}
	var body []byte
	if c.v.region != "us-east-1" {
		body = []byte(`<CreateBucketConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><LocationConstraint>` +
			c.v.region + `</LocationConstraint></CreateBucketConfiguration>`)
	}
	if _, _, err := c.do("PUT", "", nil, body, nil); err != nil {
		return fmt.Errorf("s3: creating bucket %q: %w", c.bucket, err)
	}
	return nil
}

// list returns the keys of all the bucket's objects.
func (c client) list() ([]string, error) {
	var keys []string
	token := ""
	for {
		q := url.Values{"list-type": {"2"}}
		if token != "" {
			q.Set("continuation-token", token)
		}
		_, b, err := c.do("GET", "", q, nil, nil)
		if err != nil {
			return nil, err
		}
		var r struct {
			Contents []struct {
				Key string
			}
			IsTruncated           bool
			NextContinuationToken string
		}
		if err := xml.Unmarshal(b, &r); err != nil {
			return nil, fmt.Errorf("s3: listing %q: %w", c.bucket, err)
		}
		for _, o := range r.Contents {
			keys = append(keys, o.Key)
		}
		if !r.IsTruncated || r.NextContinuationToken == "" {
			return keys, nil
		}
		token = r.NextContinuationToken
	}
}

// Storage is a storage in an S3 bucket.
type Storage struct {
	c        client
	prefix   string
	keyExpr  string
	readOnly bool
	destroy  bool
}

// Bucket is the storage's bucket.
func (s *Storage) Bucket() string { return s.c.bucket }

func (s *Storage) object(key string) string {
	if key == "" {
		key = NoneKey
	}
	if s.prefix != "" {
		return "/" + key
	}
	return key
}

var errReadOnly = errors.New("s3: update for a read-only storage")

func (s *Storage) Put(key string, v storage.Value) (storage.Result, error) {
	if s.readOnly {
		return storage.Outdated, errReadOnly
	}
	h := http.Header{}
	h.Set("Content-Encoding", v.Encoding.String())
	h.Set(timestampMeta, storage.FormatTimestamp(v.Timestamp))
	body := v.Payload
	if body == nil {
		body = []byte{}
	}
	if _, _, err := s.c.do("PUT", s.object(key), nil, body, h); err != nil {
		return storage.Outdated, fmt.Errorf("s3: put %q: %w", key, err)
	}
	return storage.Inserted, nil
}

func (s *Storage) Delete(key string, _ zenoh.Timestamp) (storage.Result, error) {
	if s.readOnly {
		return storage.Outdated, errReadOnly
	}
	if _, _, err := s.c.do("DELETE", s.object(key), nil, nil, nil); err != nil && !notFound(err) {
		return storage.Outdated, fmt.Errorf("s3: delete %q: %w", key, err)
	}
	return storage.Deleted, nil
}

func timestampOf(h http.Header) (zenoh.Timestamp, error) {
	t := h.Get(timestampMeta)
	if t == "" {
		t = h.Get(timestampMetaV0)
	}
	if t == "" {
		return zenoh.Timestamp{}, errors.New("no zenoh-timestamp metadata")
	}
	return storage.ParseTimestamp(t)
}

func (s *Storage) Get(key, _ string) ([]storage.Value, error) {
	resp, b, err := s.c.do("GET", s.object(key), nil, nil, nil)
	if notFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("s3: get %q: %w", key, err)
	}
	ts, err := timestampOf(resp.Header)
	if err != nil {
		return nil, fmt.Errorf("s3: get %q: %w", key, err)
	}
	return []storage.Value{{Payload: b, Encoding: zenoh.ParseEncoding(resp.Header.Get("Content-Encoding")), Timestamp: ts}}, nil
}

func (s *Storage) Entries() ([]storage.Entry, error) {
	objs, err := s.c.list()
	if err != nil {
		return nil, err
	}
	var out []storage.Entry
	for _, o := range objs {
		key := strings.TrimLeft(o, "/")
		full := key
		switch {
		case key == NoneKey: // "/@@none_key@@" (some servers drop the '/')
			key, full = "", s.prefix
		case s.prefix != "":
			full = s.prefix + "/" + key
		}
		if full == "" || keyexpr.Validate(full) != nil || !keyexpr.Intersect(full, s.keyExpr) {
			continue
		}
		resp, _, err := s.c.do("HEAD", o, nil, nil, nil)
		if err != nil {
			continue
		}
		if ts, err := timestampOf(resp.Header); err == nil {
			out = append(out, storage.Entry{Key: key, Timestamp: ts})
		}
	}
	return out, nil
}

// Close keeps the bucket, or empties and deletes it with on_closure
// "destroy_bucket".
func (s *Storage) Close() error {
	if !s.destroy {
		return nil
	}
	objs, err := s.c.list()
	if err != nil {
		return err
	}
	for _, o := range objs {
		if _, _, err := s.c.do("DELETE", o, nil, nil, nil); err != nil && !notFound(err) {
			return err
		}
	}
	_, _, err = s.c.do("DELETE", "", nil, nil, nil)
	return err
}
