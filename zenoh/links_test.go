package zenoh

import (
	"bytes"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/shabunin/tiny-zenoh-go/internal/testcert"
	"github.com/shabunin/tiny-zenoh-go/link"
	_ "github.com/shabunin/tiny-zenoh-go/link/quic"
)

// Sessions over UDP and TLS links, Go to Go and Go to Rust.

type linkCase struct {
	proto string
	tls   *link.TLSConfig // client side
	srv   *link.TLSConfig // listening side
	rust  map[string]any  // config inserts for a Rust node, both sides
	certs *testcert.Files
}

func linkCases(t *testing.T) []linkCase {
	t.Helper()
	f, err := testcert.Write(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	tlsCase := func(proto string) linkCase {
		return linkCase{
			proto: proto,
			tls:   &link.TLSConfig{RootCACertificate: f.CA},
			srv:   &link.TLSConfig{ListenCertificate: f.ServerCert, ListenPrivateKey: f.ServerKey},
			rust: map[string]any{
				"transport/link/tls/root_ca_certificate": f.CA,
				"transport/link/tls/listen_certificate":  f.ServerCert,
				"transport/link/tls/listen_private_key":  f.ServerKey,
			},
			certs: &f,
		}
	}
	return []linkCase{
		{proto: "udp"},
		tlsCase("tls"),
		tlsCase("quic"),
		{proto: "ws"},
		{proto: "unixsock-stream"},
	}
}

// unixPath returns a fresh socket path, short enough for sun_path.
func unixPath(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "gz")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return "unixsock-stream/" + filepath.Join(dir, "s")
}

// listenLoc is a locator to listen on for proto (any free port).
func listenLoc(t *testing.T, proto string) string {
	if proto == "unixsock-stream" {
		return unixPath(t)
	}
	return proto + "/127.0.0.1:0"
}

// freePort returns a currently free loopback locator for proto.
func freePort(t *testing.T, proto string) string {
	t.Helper()
	if proto == "unixsock-stream" {
		return unixPath(t)
	}
	if proto == "udp" || proto == "quic" {
		pc, err := net.ListenPacket("udp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer pc.Close()
		return proto + "/" + pc.LocalAddr().String()
	}
	return proto + "/" + strings.TrimPrefix(freeLocator(t), "tcp/")
}

func goRouterOn(t *testing.T, c linkCase) *Session {
	t.Helper()
	r, err := Open(Config{Mode: ModeRouter, Listen: []string{listenLoc(t, c.proto)},
		TLS: c.srv, Scouting: ScoutingConfig{Disable: true}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	if loc := r.Locators()[0]; !strings.HasPrefix(loc, c.proto+"/") {
		t.Fatalf("router locator %s", loc)
	}
	return r
}

func goClientOn(t *testing.T, c linkCase, loc string) *Session {
	t.Helper()
	s, err := Open(Config{Connect: []string{loc}, TLS: c.tls})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func rustConf(c linkCase, mode string, listen, connect string) string {
	m := map[string]any{"mode": mode, "scouting/multicast/enabled": false, "listen/endpoints": []string{}}
	if listen != "" {
		m["listen/endpoints"] = []string{listen}
	}
	if connect != "" {
		m["connect/endpoints"] = []string{connect}
	}
	for k, v := range c.rust {
		m[k] = v
	}
	b, _ := json.Marshal(m)
	return string(b)
}

func TestLinksGoRouter(t *testing.T) {
	for _, c := range linkCases(t) {
		t.Run(c.proto, func(t *testing.T) {
			r := goRouterOn(t, c)
			a, b := goClientOn(t, c, r.Locators()[0]), goClientOn(t, c, r.Locators()[0])
			checkMesh(t, a, b)

			// A payload far larger than a batch: FRAGMENTs (over UDP, one
			// datagram each).
			pfx := uniq(t)
			sink := newSink()
			sub, _ := b.DeclareSubscriber(pfx+"/big", sink.handler)
			defer sub.Undeclare()
			settle()
			big := bytes.Repeat([]byte("0123456789"), 30000)
			if err := a.Put(pfx+"/big", big); err != nil {
				t.Fatal(err)
			}
			if got := sink.wait(t, 1)[0]; !bytes.Equal(got.Payload, big) {
				t.Errorf("big payload: got %d bytes", len(got.Payload))
			}
		})
	}
}

func TestLinksTLSRejectsUntrusted(t *testing.T) {
	c := linkCases(t)[1]
	r := goRouterOn(t, c)
	if s, err := Open(Config{Connect: []string{r.Locators()[0]}, ConnectTimeout: time.Second}); err == nil {
		s.Close()
		t.Fatal("client without the CA connected")
	}
}

// TestLinksRustClientsGoRouter: Rust clients reach a Go router over UDP/TLS.
func TestLinksRustClientsGoRouter(t *testing.T) {
	for _, c := range linkCases(t) {
		t.Run(c.proto, func(t *testing.T) {
			r := goRouterOn(t, c)
			loc := r.Locators()[0]
			pfx := uniq(t)
			sub := startRust(t, rustConf(c, "client", "", loc), true, "sub", pfx+"/**", "3")
			startRust(t, rustConf(c, "client", "", loc), false, "publisher", pfx+"/x", "3")
			for i := range 3 {
				if e := sub.next(); e.Payload != "pub-"+string(rune('0'+i)) {
					t.Errorf("rust sub got %+v", e)
				}
			}
		})
	}
}

// TestLinksGoRustRouter: a Go client and a Go peer on a Rust router listening
// on UDP/TLS.
func TestLinksGoRustRouter(t *testing.T) {
	for _, c := range linkCases(t) {
		t.Run(c.proto, func(t *testing.T) {
			loc := freePort(t, c.proto)
			pfx := uniq(t)
			startRust(t, rustConf(c, "router", loc, ""), true, "queryable", pfx+"/q/**", "10")
			s := goClientOn(t, c, loc)
			waitFor(t, "rust queryable", func() bool {
				replies, err := collect(s.Get(pfx+"/q/one", WithTimeout(time.Second)))
				return err == nil && len(replies) == 1 && replies[0].Sample != nil
			})
		})
	}
}

// TestLinksRustPeer: Go peer connects to a Rust peer listening on UDP/TLS,
// and a Rust peer connects to a listening Go peer.
func TestLinksRustPeer(t *testing.T) {
	for _, c := range linkCases(t) {
		t.Run(c.proto, func(t *testing.T) {
			loc := freePort(t, c.proto)
			pfx := uniq(t)
			rs := startRust(t, rustConf(c, "peer", loc, ""), true, "sub", pfx+"/**", "1")
			p, err := Open(Config{Mode: ModePeer, Connect: []string{loc}, Listen: []string{listenLoc(t, c.proto)},
				TLS: mergeTLS(c), Scouting: ScoutingConfig{Disable: true}})
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			waitFor(t, "rust subscriber", func() bool {
				p.mu.Lock()
				defer p.mu.Unlock()
				for _, f := range p.faces {
					if len(f.subs) > 0 {
						return true
					}
				}
				return false
			})
			_ = p.Put(pfx+"/a", []byte("go-peer"))
			if e := rs.next(); e.Payload != "go-peer" {
				t.Errorf("rust peer got %+v", e)
			}

			// Rust peer dials the Go peer's listener.
			sink := newSink()
			sub, _ := p.DeclareSubscriber(pfx+"/b/**", sink.handler)
			defer sub.Undeclare()
			startRust(t, rustConf(c, "peer", "", p.Locators()[0]), false, "publisher", pfx+"/b/x", "1")
			if got := sink.wait(t, 1)[0]; string(got.Payload) != "pub-0" {
				t.Errorf("go peer got %+v", got)
			}
		})
	}
}

// mergeTLS gives a peer both client and server TLS settings.
func mergeTLS(c linkCase) *link.TLSConfig {
	if c.tls == nil {
		return nil
	}
	m := *c.tls
	m.ListenCertificate, m.ListenPrivateKey = c.srv.ListenCertificate, c.srv.ListenPrivateKey
	return &m
}

// TestGoOnlyLinks: links the official Rust builds leave out by default
// (zenoh's transport_unixpipe and transport_vsock features), between Go
// nodes: a router listening on the link, clients exchanging data and
// queries through it, and a client going away cleanly.
func TestGoOnlyLinks(t *testing.T) {
	for _, proto := range []string{"unixpipe", "vsock"} {
		t.Run(proto, func(t *testing.T) {
			listen := "unixpipe/" + filepath.Join(t.TempDir(), "zp")
			if proto == "vsock" {
				listen = "vsock/VMADDR_CID_ANY:VMADDR_PORT_ANY"
			}
			r, err := Open(Config{Mode: ModeRouter, Listen: []string{listen}, Scouting: ScoutingConfig{Disable: true}})
			if err != nil {
				t.Skip(err)
			}
			defer r.Close()
			loc := r.Locators()[0]
			if proto == "vsock" {
				_, port, _ := strings.Cut(strings.TrimPrefix(loc, "vsock/"), ":")
				loc = "vsock/VMADDR_CID_LOCAL:" + port
			}
			a, err := Open(Config{Connect: []string{loc}})
			if err != nil {
				t.Skip(err) // vsock without the loopback transport
			}
			defer a.Close()
			b, err := Open(Config{Connect: []string{loc}})
			if err != nil {
				t.Fatal(err)
			}
			pfx := uniq(t)
			sink := newSink()
			sub, _ := a.DeclareSubscriber(pfx+"/**", sink.handler)
			defer sub.Undeclare()
			q, _ := a.DeclareQueryable(pfx+"/q", func(q *Query) { _ = q.Reply(pfx+"/q", []byte("over "+proto)) })
			defer q.Undeclare()
			settle()
			big := make([]byte, 200_000)
			for i := range big {
				big[i] = byte(i)
			}
			_ = b.Put(pfx+"/x", big)
			if got := sink.wait(t, 1); len(got[0].Payload) != len(big) || got[0].Payload[1234] != big[1234] {
				t.Errorf("got %d bytes", len(got[0].Payload))
			}
			ch, _ := b.Get(pfx+"/q", WithTimeout(3*time.Second))
			var replies []string
			for rep := range ch {
				if rep.Sample != nil {
					replies = append(replies, string(rep.Sample.Payload))
				}
			}
			if len(replies) != 1 || replies[0] != "over "+proto {
				t.Errorf("replies %v", replies)
			}
			_ = b.Close()
			waitFor(t, "router to drop the closed client", func() bool { return len(r.faceList()) == 1 })
		})
	}
}
