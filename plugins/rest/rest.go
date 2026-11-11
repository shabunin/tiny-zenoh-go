//go:build !tinygo

// Package rest is zenoh's REST API (zenoh-rust plugins/zenoh-plugin-rest)
// over a session: the URL path is a key expression.
//
//	GET/POST /<key>[?params]    query (body, if any, as the query payload);
//	                            replies as a JSON array of
//	                            {key, value, encoding, timestamp}, as an
//	                            HTML <dl> for Accept: text/html, or the
//	                            first reply's raw payload with ?_raw
//	GET with Accept: text/event-stream
//	                            subscribe: server-sent events named PUT or
//	                            DELETE whose data is the JSON sample
//	PUT/PATCH /<key>            put the body (Content-Type is the encoding)
//	DELETE /<key>               delete
//
// "@/local" at the start of the path stands for "@/<session zid>".
package rest

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"strings"

	"github.com/shabunin/tiny-zenoh-go/keyexpr"
	"github.com/shabunin/tiny-zenoh-go/zenoh"
)

// Sample is the JSON form of a sample or reply.
type Sample struct {
	Key       string          `json:"key"`
	Value     json.RawMessage `json:"value"`
	Encoding  string          `json:"encoding"`
	Timestamp *string         `json:"timestamp"`
}

func jsonSample(key string, payload []byte, enc zenoh.Encoding, ts *zenoh.Timestamp) Sample {
	s := Sample{Key: key, Value: payloadJSON(payload, enc), Encoding: enc.String()}
	if ts != nil {
		t := fmt.Sprintf("%d/%s", ts.Time, ts.ID)
		s.Timestamp = &t
	}
	return s
}

// payloadJSON is zenoh-plugin-rest's payload_to_json: JSON encodings
// inline (base64 if invalid), strings as strings, anything else base64.
func payloadJSON(p []byte, enc zenoh.Encoding) json.RawMessage {
	if len(p) == 0 {
		return json.RawMessage("null")
	}
	b64 := func() json.RawMessage {
		b, _ := json.Marshal(base64.StdEncoding.EncodeToString(p))
		return b
	}
	if enc.Schema != "" {
		return b64()
	}
	switch enc {
	case zenoh.EncodingApplicationJSON, zenoh.EncodingTextJSON, encTextJSON5:
		var v any
		if json.Unmarshal(p, &v) != nil {
			return b64()
		}
		b, _ := json.Marshal(v)
		return b
	case zenoh.EncodingTextPlain, zenoh.EncodingZenohString:
		if !isUTF8(p) {
			return b64()
		}
		b, _ := json.Marshal(string(p))
		return b
	}
	return b64()
}

var encTextJSON5 = zenoh.ParseEncoding("text/json5")

func isUTF8(p []byte) bool { return strings.ToValidUTF8(string(p), "�") == string(p) }

type handler struct{ s *zenoh.Session }

// Handler returns the REST API over s.
func Handler(s *zenoh.Session) http.Handler { return &handler{s: s} }

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	if r.Method == http.MethodOptions {
		w.Header().Set("Access-Control-Allow-Methods", "GET,POST,PUT,PATCH,DELETE")
		w.Header().Set("Access-Control-Allow-Headers", "*")
		return
	}
	key := strings.TrimPrefix(r.URL.Path, "/")
	if rest, ok := strings.CutPrefix(key, "@/local"); ok && (rest == "" || rest[0] == '/') {
		key = "@/" + h.s.ZID().String() + rest
	}
	if err := keyexpr.Validate(key); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	switch r.Method {
	case http.MethodGet, http.MethodPost:
		accept := strings.Split(strings.Split(r.Header.Get("Accept"), ",")[0], ";")[0]
		if accept == "text/event-stream" {
			h.subscribe(w, r, key)
			return
		}
		h.query(w, r, key, accept)
	case http.MethodPut, http.MethodPatch, http.MethodDelete:
		var err error
		if r.Method == http.MethodDelete {
			err = h.s.Delete(key)
		} else {
			body, rerr := io.ReadAll(r.Body)
			if rerr != nil {
				http.Error(w, rerr.Error(), http.StatusInternalServerError)
				return
			}
			err = h.s.Put(key, body, zenoh.WithEncoding(requestEncoding(r)))
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func requestEncoding(r *http.Request) zenoh.Encoding {
	if ct := r.Header.Get("Content-Type"); ct != "" {
		return zenoh.ParseEncoding(ct)
	}
	return zenoh.EncodingZenohBytes
}

func (h *handler) query(w http.ResponseWriter, r *http.Request, key, accept string) {
	params := r.URL.RawQuery
	opts := []zenoh.GetOption{zenoh.WithConsolidation(zenoh.ConsolidationLatest)}
	if _, ok := zenoh.Param(params, "_time"); ok {
		opts[0] = zenoh.WithConsolidation(zenoh.ConsolidationNone)
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if len(body) > 0 {
		opts = append(opts, zenoh.WithPayload(body, requestEncoding(r)))
	}
	sel := key
	if params != "" {
		sel += "?" + params
	}
	replies, err := h.s.Get(sel, opts...)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_, raw := zenoh.Param(params, "_raw")
	switch {
	case raw:
		rep, ok := <-replies
		go drain(replies)
		if !ok {
			return
		}
		payload, enc := replyBody(rep)
		ct, ce := splitContentEncoding(enc.String())
		if _, _, err := mime.ParseMediaType(ct); err == nil {
			w.Header().Set("Content-Type", ct)
		}
		if ce != "" {
			w.Header().Set("Content-Encoding", ce)
		}
		_, _ = w.Write(payload)
	case accept == "text/html":
		var b strings.Builder
		b.WriteString("<dl>\n")
		for rep := range replies {
			k := "ERROR"
			if rep.Sample != nil {
				k = rep.Sample.KeyExpr
			}
			payload, _ := replyBody(rep)
			fmt.Fprintf(&b, "<dt>%s</dt>\n<dd>%s</dd>\n", k, strings.ToValidUTF8(string(payload), "�"))
		}
		b.WriteString("</dl>\n")
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = io.WriteString(w, b.String())
	default:
		out := []Sample{}
		for rep := range replies {
			if rep.Sample != nil {
				s := rep.Sample
				out = append(out, jsonSample(s.KeyExpr, s.Payload, s.Encoding, s.Timestamp))
			} else {
				out = append(out, jsonSample("ERROR", rep.Err.Payload, rep.Err.Encoding, nil))
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(out)
	}
}

func replyBody(r zenoh.Reply) ([]byte, zenoh.Encoding) {
	if r.Sample != nil {
		return r.Sample.Payload, r.Sample.Encoding
	}
	return r.Err.Payload, r.Err.Encoding
}

func drain(ch <-chan zenoh.Reply) {
	for range ch {
	}
}

// splitContentEncoding takes a ";content-encoding=<v>" parameter (zenoh's
// admin metrics carry one) out of an encoding, for the Content-Encoding
// header.
func splitContentEncoding(enc string) (contentType, contentEncoding string) {
	parts := strings.Split(enc, ";")
	for i, p := range parts {
		if v, ok := strings.CutPrefix(strings.TrimLeft(p, " "), "content-encoding="); ok && i > 0 {
			return strings.Join(append(parts[:i:i], parts[i+1:]...), ";"), strings.TrimRight(v, " ")
		}
	}
	return enc, ""
}

func (h *handler) subscribe(w http.ResponseWriter, r *http.Request, key string) {
	fl, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	sub, ch, err := h.s.DeclareSubscriberChan(key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer sub.Undeclare()
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	fl.Flush()
	for {
		select {
		case <-r.Context().Done():
			return
		case s, ok := <-ch:
			if !ok {
				return
			}
			b, _ := json.Marshal(jsonSample(s.KeyExpr, s.Payload, s.Encoding, s.Timestamp))
			if _, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", s.Kind, b); err != nil {
				return
			}
			fl.Flush()
		}
	}
}

// Server is a running REST endpoint.
type Server struct {
	srv *http.Server
	ln  net.Listener
}

// Serve starts the REST API of s on addr (zenoh's plugins/rest/http_port:
// a port number, or host:port).
func Serve(s *zenoh.Session, addr string) (*Server, error) {
	if !strings.Contains(addr, ":") {
		addr = ":" + addr
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("rest: %w", err)
	}
	srv := &http.Server{Handler: Handler(s)}
	go func() { _ = srv.Serve(ln) }()
	return &Server{srv: srv, ln: ln}, nil
}

// Addr is the address the server listens on.
func (s *Server) Addr() string { return s.ln.Addr().String() }

// Close stops the server.
func (s *Server) Close() error { return s.srv.Close() }
