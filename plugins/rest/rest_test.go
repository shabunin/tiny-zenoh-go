package rest

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/shabunin/tiny-zenoh-go/zenoh"
)

func setup(t *testing.T) (r, c *zenoh.Session, base string) {
	t.Helper()
	r, err := zenoh.Open(zenoh.Config{Mode: zenoh.ModeRouter, Listen: []string{"tcp/127.0.0.1:0"},
		Scouting: zenoh.ScoutingConfig{Disable: true}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	c, err = zenoh.Open(zenoh.Config{Connect: r.Locators()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	srv, err := Serve(r, "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = srv.Close() })
	return r, c, "http://" + srv.Addr()
}

func do(t *testing.T, method, url, ctype string, body string, hdr ...string) *http.Response {
	t.Helper()
	req, _ := http.NewRequest(method, url, strings.NewReader(body))
	if ctype != "" {
		req.Header.Set("Content-Type", ctype)
	}
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func read(t *testing.T, resp *http.Response) string {
	t.Helper()
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d: %s", resp.StatusCode, b)
	}
	return string(b)
}

func TestREST(t *testing.T) {
	r, c, base := setup(t)
	pfx := fmt.Sprintf("rest/%d", time.Now().UnixNano())

	// PUT/DELETE -> subscribers
	sub, ch, _ := c.DeclareSubscriberChan(pfx + "/**")
	defer sub.Undeclare()
	time.Sleep(200 * time.Millisecond)
	read(t, do(t, "PUT", base+"/"+pfx+"/a", "text/plain", "hello"))
	read(t, do(t, "DELETE", base+"/"+pfx+"/a", "", ""))
	for i, want := range []string{"PUT text/plain hello", "DELETE zenoh/bytes "} {
		select {
		case s := <-ch:
			if got := fmt.Sprintf("%s %s %s", s.Kind, s.Encoding, s.Payload); got != want || s.KeyExpr != pfx+"/a" {
				t.Errorf("sample %d: %s", i, got)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("timeout")
		}
	}

	// GET -> queryables: JSON, HTML, raw.
	q, _ := c.DeclareQueryable(pfx+"/q/**", func(q *zenoh.Query) {
		switch {
		case q.Parameters() == "fail":
			_ = q.ReplyErr([]byte("bad"), zenoh.EncodingTextPlain)
		case strings.HasPrefix(q.Parameters(), "_raw"):
			_ = q.Reply(pfx+"/q/raw", []byte("raw body"), zenoh.WithEncoding(zenoh.ParseEncoding("text/csv;content-encoding=gzip")))
		default:
			_ = q.Reply(pfx+"/q/json", []byte(`{"n":1}`), zenoh.WithEncoding(zenoh.EncodingApplicationJSON))
			_ = q.Reply(pfx+"/q/text", []byte("txt:"+string(q.Payload())), zenoh.WithEncoding(zenoh.EncodingTextPlain))
			_ = q.Reply(pfx+"/q/bin", []byte{0, 1, 2})
		}
	})
	defer q.Undeclare()
	time.Sleep(200 * time.Millisecond)
	var got []Sample
	if err := json.Unmarshal([]byte(read(t, do(t, "POST", base+"/"+pfx+"/q/**", "text/plain", "body"))), &got); err != nil {
		t.Fatal(err)
	}
	vals := map[string]string{}
	for _, s := range got {
		vals[s.Key] = s.Encoding + " " + string(s.Value)
	}
	want := map[string]string{
		pfx + "/q/json": `application/json {"n":1}`,
		pfx + "/q/text": `text/plain "txt:body"`,
		pfx + "/q/bin":  `zenoh/bytes "AAEC"`,
	}
	if fmt.Sprint(vals) != fmt.Sprint(want) {
		t.Errorf("JSON replies %v", vals)
	}
	if body := read(t, do(t, "GET", base+"/"+pfx+"/q/x?fail", "", "")); !strings.Contains(body, `"key":"ERROR","value":"bad","encoding":"text/plain"`) {
		t.Errorf("error reply %s", body)
	}
	html := read(t, do(t, "GET", base+"/"+pfx+"/q/text", "", "", "Accept", "text/html"))
	if !strings.HasPrefix(html, "<dl>\n") || !strings.Contains(html, "<dt>"+pfx+"/q/text</dt>\n<dd>txt:</dd>\n") {
		t.Errorf("html %q", html)
	}
	// (an explicit Accept-Encoding keeps Go's client from un-gzipping)
	resp := do(t, "GET", base+"/"+pfx+"/q/raw?_raw=true", "", "", "Accept-Encoding", "identity")
	if ct, ce := resp.Header.Get("Content-Type"), resp.Header.Get("Content-Encoding"); ct != "text/csv" || ce != "gzip" {
		t.Errorf("raw headers %q %q", ct, ce)
	}
	if b := read(t, resp); b != "raw body" {
		t.Errorf("raw body %q", b)
	}
	if resp.Header.Get("Access-Control-Allow-Origin") != "*" {
		t.Error("no CORS header")
	}

	// @/local is the router's own admin space.
	adm := read(t, do(t, "GET", base+"/@/local/router", "", ""))
	if !strings.Contains(adm, `"key":"@/`+r.ZID().String()+`/router"`) {
		t.Errorf("admin %s", adm)
	}
	if resp := do(t, "GET", base+"/bad//key", "", ""); resp.StatusCode != http.StatusBadRequest {
		t.Errorf("invalid key: status %d", resp.StatusCode)
	}
}

func TestRESTEventStream(t *testing.T) {
	_, c, base := setup(t)
	pfx := fmt.Sprintf("rest/%d", time.Now().UnixNano())
	resp := do(t, "GET", base+"/"+pfx+"/**", "", "", "Accept", "text/event-stream")
	defer resp.Body.Close()
	if resp.Header.Get("Content-Type") != "text/event-stream" {
		t.Fatalf("content type %q", resp.Header.Get("Content-Type"))
	}
	time.Sleep(200 * time.Millisecond)
	_ = c.Put(pfx+"/e", []byte(`{"x":true}`), zenoh.WithEncoding(zenoh.EncodingApplicationJSON))
	_ = c.Delete(pfx + "/e")
	br := bufio.NewReader(resp.Body)
	var lines []string
	for len(lines) < 4 {
		l, err := br.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		if l = strings.TrimRight(l, "\n"); l != "" {
			lines = append(lines, l)
		}
	}
	if lines[0] != "event: PUT" || !strings.HasPrefix(lines[1], `data: {"key":"`+pfx+`/e","value":{"x":true},"encoding":"application/json","timestamp":"`) ||
		lines[2] != "event: DELETE" || !strings.Contains(lines[3], `"value":null`) {
		t.Errorf("events %q", lines)
	}
}
