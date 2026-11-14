package main

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/shabunin/tiny-zenoh-go/internal/leaktest"
	"github.com/shabunin/tiny-zenoh-go/internal/testcert"
	"github.com/shabunin/tiny-zenoh-go/link"
	"github.com/shabunin/tiny-zenoh-go/transport"
	"github.com/shabunin/tiny-zenoh-go/zenoh"
)

// syncBuf is a bytes.Buffer safe for concurrent writes and reads.
type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// tzdRun is a tzd running in-process (run in a goroutine).
type tzdRun struct {
	t        *testing.T
	out, err *syncBuf
	stop     chan os.Signal
	done     chan int
	once     sync.Once
	code     int
	zid      string
	locators []string
}

var listeningRE = regexp.MustCompile(`tzd (\S+) listening on (.*)\n`)

// startTZD runs tzd with args and waits for its "listening on" line. The
// console is process-wide: with two routers running, later output of the
// first one lands in the second one's buffers.
func startTZD(t *testing.T, args ...string) *tzdRun {
	t.Helper()
	p := &tzdRun{t: t, out: &syncBuf{}, err: &syncBuf{}, stop: make(chan os.Signal, 1), done: make(chan int, 1)}
	go func() { p.done <- run(args, p.out, p.err, p.stop) }()
	t.Cleanup(func() { p.exit() })
	deadline := time.After(5 * time.Second)
	for {
		if m := listeningRE.FindStringSubmatch(p.out.String()); m != nil {
			p.zid, p.locators = m[1], strings.Fields(m[2])
			return p
		}
		select {
		case code := <-p.done:
			t.Fatalf("tzd %v exited with %d before listening; stderr:\n%s", args, code, p.err.String())
		case <-deadline:
			t.Fatalf("tzd %v: no listening line; stdout:\n%s", args, p.out.String())
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// exit stops tzd (as SIGTERM does) and returns its exit status.
func (p *tzdRun) exit() int {
	p.once.Do(func() {
		p.stop <- os.Interrupt
		select {
		case p.code = <-p.done:
		case <-time.After(10 * time.Second):
			p.t.Errorf("tzd did not stop; stdout:\n%s", p.out.String())
			p.code = -1
		}
	})
	return p.code
}

// waitOut waits for stdout to contain s.
func (p *tzdRun) waitOut(s string) {
	p.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !strings.Contains(p.out.String(), s) {
		if time.Now().After(deadline) {
			p.t.Fatalf("stdout lacks %q:\n%s", s, p.out.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// runOnce runs tzd expecting it to exit by itself.
func runOnce(t *testing.T, args ...string) (code int, stdout, stderr string) {
	t.Helper()
	var out, errb syncBuf
	done := make(chan int, 1)
	go func() { done <- run(args, &out, &errb, make(chan os.Signal)) }()
	select {
	case code = <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("tzd %v still running", args)
	}
	return code, out.String(), errb.String()
}

func client(t *testing.T, cfg zenoh.Config) *zenoh.Session {
	t.Helper()
	s, err := zenoh.Open(cfg)
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func writeFile(t *testing.T, name, content string) string {
	t.Helper()
	f := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(f, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return f
}

// busyPort returns a loopback address something listens on until the test ends.
func busyPort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	return ln.Addr().String()
}

// pubSub checks that a put from pub reaches a subscriber on sub.
func pubSub(t *testing.T, pub, sub *zenoh.Session, key string) {
	t.Helper()
	got := make(chan string, 16)
	s, err := sub.DeclareSubscriber(key, func(smp zenoh.Sample) { got <- string(smp.Payload) })
	if err != nil {
		t.Fatal(err)
	}
	defer s.Undeclare()
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
		_ = pub.Put(key, []byte("hello"))
		select {
		case v := <-got:
			if v != "hello" {
				t.Fatalf("got %q", v)
			}
			return
		case <-time.After(100 * time.Millisecond):
		}
	}
	t.Fatalf("%s: nothing delivered", key)
}

func adminReplies(t *testing.T, c *zenoh.Session, zid string) int {
	t.Helper()
	n := 0
	for rep := range mustGet(t, c, "@/"+zid+"/router") {
		if rep.Sample != nil {
			n++
		}
	}
	return n
}

var base = []string{"-l", "tcp/127.0.0.1:0", "--no-multicast-scouting"}

func args(extra ...string) []string { return append(append([]string{}, base...), extra...) }

func TestRunVersionAndHelp(t *testing.T) {
	code, out, _ := runOnce(t, "--version")
	if code != 0 || !strings.HasPrefix(out, "tzd "+zenoh.Version+" go") {
		t.Fatalf("--version: %d %q", code, out)
	}
	code, _, errOut := runOnce(t, "-h")
	if code != 0 || !strings.Contains(errOut, "-rest-http-port") {
		t.Fatalf("-h: %d %q", code, errOut)
	}
}

func TestRunUsageErrors(t *testing.T) {
	leaktest.Check(t)
	dir := t.TempDir()
	missing := filepath.Join(dir, "missing")
	_, pub := rsaKeyFiles(t)
	cases := []struct {
		name string
		args []string
		code int
		want string
	}{
		{"unknown flag", []string{"--bogus"}, 2, "flag provided but not defined"},
		{"bad flag value", []string{"--max-links", "x"}, 2, "invalid value"},
		{"stray argument", []string{"extra"}, 2, "unexpected arguments: extra"},
		{"bad zid", []string{"-i", "xyz"}, 2, "bad -i"},
		{"admin perms", []string{"--adminspace-permissions", "w"}, 2, "r, rw or none"},
		{"acl missing", []string{"--acl", missing}, 2, "no such file"},
		{"acl unknown field", []string{"--acl", writeFile(t, "acl.json", `{"enabled":true,"rulez":[]}`)}, 2, "unknown field"},
		{"gateway preset", []string{"--gateway", writeFile(t, "gw.json", `{"south":"north"}`)}, 2, `unknown gateway preset "north"`},
		{"gateway missing", []string{"--gateway", missing}, 2, "no such file"},
		{"config missing", []string{"-c", missing}, 2, "no such file"},
		{"config json5", []string{"-c", writeFile(t, "z.json5", `{mode: `)}, 2, "tzd:"},
		{"bad --cfg", []string{"--cfg", `scouting:{multicast:`}, 2, "tzd:"},
		{"bad section", []string{"--cfg", `transport/unicast/max_links:"many"`}, 2, "config:"},
		{"user without password", []string{"--user", "u"}, 2, "user and password go together"},
		{"usrpwd dict missing", []string{"--usrpwd-dict", missing}, 2, "auth/usrpwd"},
		{"known key alone", []string{"--auth-known-key", pub}, 2, "needs --auth-key"},
		{"auth key missing", []string{"--auth-key", missing}, 2, "no such file"},
		{"auth key not a key", []string{"--auth-key", pub}, 2, "tzd:"},
		{"bad locator", args("-l", "nope/127.0.0.1:1"), 1, "tzd:"},
		{"port in use", []string{"-l", "tcp/" + busyPort(t), "--no-multicast-scouting"}, 1, "tzd:"},
		{"rest port in use", args("--rest-http-port", busyPort(t)), 1, "tzd:"},
		{"storage without volume", args("--cfg", `plugins/storage_manager/storages/x:{key_expr:"x/**",volume:"nope"}`), 2, `volume "nope" is not configured`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			code, _, errOut := runOnce(t, c.args...)
			if code != c.code || !strings.Contains(errOut, c.want) {
				t.Fatalf("exit %d (want %d), stderr %q (want %q)", code, c.code, errOut, c.want)
			}
		})
	}
}

// A router with clients: routing, the admin space, -i, -v and a clean stop.
func TestRunRoutes(t *testing.T) {
	leaktest.Check(t)
	defer func(d time.Duration) { statusEvery = d }(statusEvery)
	statusEvery = 20 * time.Millisecond
	p := startTZD(t, args("-i", "a1b2c3", "-v")...)
	if p.zid != "a1b2c3" || len(p.locators) != 1 || !strings.HasPrefix(p.locators[0], "tcp/127.0.0.1:") {
		t.Fatalf("listening line: %s %v", p.zid, p.locators)
	}
	a := client(t, zenoh.Config{Connect: p.locators})
	b := client(t, zenoh.Config{Connect: p.locators})
	pubSub(t, a, b, "run/routes")
	if n := adminReplies(t, a, p.zid); n == 0 {
		t.Fatal("no admin space reply")
	}
	p.waitOut("clients=2 peers=0 routers=0")
	if code := p.exit(); code != 0 {
		t.Fatalf("exit %d", code)
	}
}

// Two tzd joined with -e route between their clients.
func TestRunConnect(t *testing.T) {
	leaktest.Check(t)
	r1 := startTZD(t, args()...)
	r2 := startTZD(t, args("-e", r1.locators[0])...)
	a := client(t, zenoh.Config{Connect: r1.locators})
	b := client(t, zenoh.Config{Connect: r2.locators})
	pubSub(t, a, b, "run/connect/ab")
	pubSub(t, b, a, "run/connect/ba")
	if r2.exit() != 0 || r1.exit() != 0 {
		t.Fatal("exit status")
	}
}

// Storages and the REST API from flags, a storage added through the
// admin space, and the plugins stopped on exit.
func TestRunPlugins(t *testing.T) {
	leaktest.Check(t)
	p := startTZD(t, args("--rest-http-port", "127.0.0.1:0", "--adminspace-permissions", "rw",
		"--cfg", `plugins/storage_manager/storages/demo:{key_expr:"run/demo/**",volume:"memory"}`)...)
	p.waitOut("storage demo on run/demo/** (volume memory)")
	m := regexp.MustCompile(`REST API on (http://\S+)`).FindStringSubmatch(p.out.String())
	if m == nil {
		t.Fatalf("no REST line:\n%s", p.out.String())
	}
	c := client(t, zenoh.Config{Connect: p.locators})
	if err := c.Put("run/demo/a", []byte("stored"), zenoh.WithCongestionControl(zenoh.CongestionBlock)); err != nil {
		t.Fatal(err)
	}
	var body string
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
		resp, err := http.Get(m[1] + "/run/demo/a")
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if body = string(b); strings.Contains(body, `"value":"c3RvcmVk"`) { // base64("stored")
			break
		}
	}
	if !strings.Contains(body, `"value":"c3RvcmVk"`) {
		t.Fatalf("REST GET: %s", body)
	}
	key := "@/" + p.zid + "/router/config/plugins/storage_manager/storages/more"
	if err := c.Put(key, []byte(`{key_expr:"run/more/**",volume:"memory"}`), zenoh.WithCongestionControl(zenoh.CongestionBlock)); err != nil {
		t.Fatal(err)
	}
	p.waitOut("storage more on run/more/**")
	if code := p.exit(); code != 0 {
		t.Fatalf("exit %d", code)
	}
	for _, s := range []string{"storage demo stopped", "storage more stopped", "REST API stopped"} {
		if !strings.Contains(p.out.String(), s) {
			t.Errorf("stdout lacks %q:\n%s", s, p.out.String())
		}
	}
}

// Configuration tzd can't honour is reported, not fatal.
func TestRunWarnings(t *testing.T) {
	leaktest.Check(t)
	p := startTZD(t, args("--cfg", `plugins/dds:{}`, "--cfg", `qos/publication:[{key_exprs:["a/**"],config:{}}]`,
		"--cfg", `transport/auth/pubkey/public_key_pem:"x"`)...)
	for _, w := range []string{`plugin "dds" is not available`, "ignoring qos/publication", "transport/auth/pubkey/public_key_pem: not supported"} {
		if !strings.Contains(p.err.String(), w) {
			t.Errorf("stderr lacks %q:\n%s", w, p.err.String())
		}
	}
}

func TestRunAdminSpaceOff(t *testing.T) {
	leaktest.Check(t)
	p := startTZD(t, args("--adminspace-permissions", "none")...)
	c := client(t, zenoh.Config{Connect: p.locators})
	time.Sleep(100 * time.Millisecond)
	if n := adminReplies(t, c, p.zid); n != 0 {
		t.Fatalf("%d admin space replies with permissions none", n)
	}
}

func TestRunUsrPwd(t *testing.T) {
	leaktest.Check(t)
	dict := writeFile(t, "users.txt", "alice:wonderland\nbob:builder\n")
	p := startTZD(t, args("--usrpwd-dict", dict)...)
	bad := zenoh.Config{Connect: p.locators, Auth: &transport.Auth{User: "alice", Password: "nope"}}
	if s, err := zenoh.Open(bad); err == nil {
		_ = s.Close()
		t.Fatal("wrong password accepted")
	}
	if s, err := zenoh.Open(zenoh.Config{Connect: p.locators}); err == nil {
		_ = s.Close()
		t.Fatal("no credentials accepted")
	}
	a := client(t, zenoh.Config{Connect: p.locators, Auth: &transport.Auth{User: "alice", Password: "wonderland"}})
	b := client(t, zenoh.Config{Connect: p.locators, Auth: &transport.Auth{User: "bob", Password: "builder"}})
	pubSub(t, a, b, "run/usrpwd")
}

// rsaKeyFiles writes a PKCS#1 RSA key pair, returning the file names.
func rsaKeyFiles(t *testing.T) (priv, pub string) {
	t.Helper()
	k, err := rsa.GenerateKey(rand.Reader, 1024)
	if err != nil {
		t.Fatal(err)
	}
	priv = writeFile(t, "key.pem", string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(k)})))
	pub = writeFile(t, "pub.pem", string(pem.EncodeToMemory(&pem.Block{Type: "RSA PUBLIC KEY", Bytes: x509.MarshalPKCS1PublicKey(&k.PublicKey)})))
	return priv, pub
}

func loadKey(t *testing.T, file string) *transport.RSAKey {
	t.Helper()
	b, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	k, err := transport.ParseRSAKeyPEM(b)
	if err != nil {
		t.Fatal(err)
	}
	return k
}

func TestRunPubKey(t *testing.T) {
	leaktest.Check(t)
	routerKey, _ := rsaKeyFiles(t)
	clientKey, clientPub := rsaKeyFiles(t)
	strangerKey, _ := rsaKeyFiles(t)
	p := startTZD(t, args("--auth-key", routerKey, "--auth-known-key", clientPub)...)
	if s, err := zenoh.Open(zenoh.Config{Connect: p.locators, Auth: &transport.Auth{Key: loadKey(t, strangerKey)}}); err == nil {
		_ = s.Close()
		t.Fatal("unknown key accepted")
	}
	a := client(t, zenoh.Config{Connect: p.locators, Auth: &transport.Auth{Key: loadKey(t, clientKey)}})
	b := client(t, zenoh.Config{Connect: p.locators, Auth: &transport.Auth{Key: loadKey(t, clientKey)}})
	pubSub(t, a, b, "run/pubkey")
}

func TestRunACL(t *testing.T) {
	leaktest.Check(t)
	acl := writeFile(t, "acl.json", `{
		"enabled": true,
		"default_permission": "allow",
		"rules": [{"id": "nosecret", "messages": ["put"], "flows": ["ingress", "egress"],
			"permission": "deny", "key_exprs": ["run/acl/secret/**"]}],
		"subjects": [{"id": "all"}],
		"policies": [{"rules": ["nosecret"], "subjects": ["all"]}]
	}`)
	p := startTZD(t, args("--acl", acl)...)
	a := client(t, zenoh.Config{Connect: p.locators})
	b := client(t, zenoh.Config{Connect: p.locators})
	got := make(chan string, 16)
	sub, err := b.DeclareSubscriber("run/acl/**", func(s zenoh.Sample) { got <- s.KeyExpr })
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Undeclare()
	pubSub(t, a, b, "run/acl/open") // routes are up
	_ = a.Put("run/acl/secret/x", []byte("s"))
	_ = a.Put("run/acl/open2", []byte("o"))
	deadline := time.After(time.Second)
	for {
		select {
		case k := <-got:
			if strings.Contains(k, "secret") {
				t.Fatalf("denied put delivered: %s", k)
			}
			continue
		case <-deadline:
		}
		break
	}

	off := writeFile(t, "off.json", `{"enabled": false, "default_permission": "deny"}`)
	q := startTZD(t, args("--acl", off)...)
	c := client(t, zenoh.Config{Connect: q.locators})
	d := client(t, zenoh.Config{Connect: q.locators})
	pubSub(t, c, d, "run/acl/secret/y") // disabled: nothing denied
}

func TestRunTLS(t *testing.T) {
	leaktest.Check(t)
	f, err := testcert.Write(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	p := startTZD(t, "-l", "tls/127.0.0.1:0", "--no-multicast-scouting",
		"--tls-cert", f.ServerCert, "--tls-key", f.ServerKey, "--tls-ca", f.CA, "--tls-mtls")
	if !strings.HasPrefix(p.locators[0], "tls/") {
		t.Fatalf("locators %v", p.locators)
	}
	if s, err := zenoh.Open(zenoh.Config{Connect: p.locators, TLS: &link.TLSConfig{RootCACertificate: f.CA}}); err == nil {
		_ = s.Close()
		t.Fatal("client without certificate accepted (--tls-mtls)")
	}
	tc := &link.TLSConfig{RootCACertificate: f.CA, ConnectCertificate: f.ClientCert, ConnectPrivateKey: f.ClientKey}
	a := client(t, zenoh.Config{Connect: p.locators, TLS: tc})
	b := client(t, zenoh.Config{Connect: p.locators, TLS: tc})
	pubSub(t, a, b, "run/tls")
}

// The remaining flags reach the configuration and win over the file.
func TestParseArgsFlags(t *testing.T) {
	file := writeFile(t, "z.json5", `{
		scouting: { multicast: { enabled: true, address: "224.0.0.1:1" } },
		transport: { unicast: { max_links: 4, lowlatency: false } },
		timestamping: { enabled: { router: true } },
	}`)
	gw := writeFile(t, "gw.json", `{"south": [{"filters": [{"modes": ["client"]}]}]}`)
	o, err := parseArgs([]string{"-c", file, "--no-multicast-scouting", "--scouting-address", "224.0.0.9:7446",
		"--shm", "--compression", "--max-links", "2", "--lowlatency", "--no-timestamp",
		"--region-name", "south", "--gateway", gw, "--user", "u", "--password", "p",
		"-e", "tcp/127.0.0.1:1", "-l", "tcp/127.0.0.1:0", "-i", "0f", "-v"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	c := o.cfg
	switch {
	case !c.Scouting.Disable || c.Scouting.Address != "224.0.0.9:7446":
		t.Errorf("scouting %+v", c.Scouting)
	case c.SharedMemory == nil || !c.Compression || c.MaxLinks != 2 || !c.LowLatency:
		t.Errorf("transport: shm %v compression %v links %d lowlatency %v", c.SharedMemory, c.Compression, c.MaxLinks, c.LowLatency)
	case c.Timestamping.Enabled == nil || *c.Timestamping.Enabled:
		t.Errorf("timestamping %+v", c.Timestamping)
	case c.RegionName != "south" || len(c.Gateway.South) != 1:
		t.Errorf("region %q gateway %+v", c.RegionName, c.Gateway)
	case c.Auth == nil || c.Auth.User != "u" || c.Auth.Password != "p":
		t.Errorf("auth %+v", c.Auth)
	case len(c.Connect) != 1 || len(c.Listen) != 1 || c.ZID.String() != "f" && c.ZID.String() != "0f":
		t.Errorf("endpoints %v %v zid %v", c.Connect, c.Listen, c.ZID)
	case !o.verbose || o.version:
		t.Errorf("options %+v", o)
	}
}

func TestDecodeGateway(t *testing.T) {
	cases := []struct {
		in      string
		subs    int // -1: South nil (auto)
		filters int // filters of the first subregion, -1: nil
		err     string
	}{
		{`{}`, -1, 0, ""},
		{`{"south": "auto"}`, -1, 0, ""},
		{`{"south": "north"}`, 0, 0, "unknown gateway preset"},
		{`{"south": []}`, 0, 0, ""},
		{`{"south": [{}]}`, 1, -1, ""},
		{`{"south": [{"filters": []}]}`, 1, 0, ""},
		{`{"south": [{"filters": [{"modes": ["router", "peer", "client"], "zids": ["a1"],
			"interfaces": ["lo"], "region_names": ["r"], "negated": true}]}, {}]}`, 2, 1, ""},
		{`{"south": [{"filters": [{"modes": ["gateway"]}]}]}`, 0, 0, `unknown mode "gateway"`},
		{`{"south": [{"filters": [{"zids": ["xyz"]}]}]}`, 0, 0, `zid "xyz"`},
		{`{"south": [{"filterz": []}]}`, 0, 0, "unknown field"},
		{`{"north": []}`, 0, 0, "unknown field"},
	}
	for _, c := range cases {
		gc, err := decodeGateway([]byte(c.in))
		if c.err != "" {
			if err == nil || !strings.Contains(err.Error(), c.err) {
				t.Errorf("%s: err %v, want %q", c.in, err, c.err)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: %v", c.in, err)
			continue
		}
		if c.subs == -1 {
			if gc.South != nil {
				t.Errorf("%s: South %+v, want nil", c.in, gc.South)
			}
			continue
		}
		if len(gc.South) != c.subs {
			t.Errorf("%s: %d subregions", c.in, len(gc.South))
			continue
		}
		if c.subs > 0 {
			f := gc.South[0].Filters
			if (c.filters == -1) != (f == nil) || c.filters >= 0 && len(f) != c.filters {
				t.Errorf("%s: filters %+v", c.in, f)
			}
			if c.filters == 1 {
				g := f[0]
				if g.Modes != zenoh.MatchRouter|zenoh.MatchPeer|zenoh.MatchClient || len(g.ZIDs) != 1 || !g.Negated ||
					len(g.Interfaces) != 1 || len(g.RegionNames) != 1 {
					t.Errorf("%s: filter %+v", c.in, g)
				}
			}
		}
	}
}

func TestDecodeACL(t *testing.T) {
	if a, err := decodeACL([]byte(`{"enabled": false, "rules": []}`)); err != nil || a != nil {
		t.Fatalf("disabled: %v %v", a, err)
	}
	a, err := decodeACL([]byte(`{"default_permission": "allow"}`))
	if err != nil || a == nil || a.DefaultPermission != "allow" {
		t.Fatalf("no enabled field: %+v %v", a, err)
	}
	if _, err := decodeACL([]byte(`{"enabled": "yes"}`)); err == nil {
		t.Fatal("string enabled accepted")
	}
}

func TestRouterValue(t *testing.T) {
	yes, no := true, false
	cases := []struct {
		in   string
		want *bool
		err  bool
	}{
		{``, nil, false},
		{`null`, nil, false},
		{`true`, &yes, false},
		{`false`, &no, false},
		{`{"router": false, "peer": true, "client": true}`, &no, false},
		{`{"peer": true}`, nil, false},
		{`{"gateway": true}`, nil, true},
		{`"on"`, nil, true},
	}
	for _, c := range cases {
		got, err := routerValue([]byte(c.in))
		if (err != nil) != c.err || (got == nil) != (c.want == nil) || got != nil && *got != *c.want {
			t.Errorf("%q: %v %v", c.in, got, err)
		}
	}
}

func TestPubkeyAuthErrors(t *testing.T) {
	priv, pub := rsaKeyFiles(t)
	notPub := writeFile(t, "junk.pem", "-----BEGIN RSA PUBLIC KEY-----\nMAM=\n-----END RSA PUBLIC KEY-----\n")
	var cfg zenoh.Config
	if err := pubkeyAuth(&cfg, priv, []string{pub}); err != nil || cfg.Auth == nil || cfg.Auth.Key == nil || len(cfg.Auth.KnownKeys) != 1 {
		t.Fatalf("good keys: %v %+v", err, cfg.Auth)
	}
	if err := pubkeyAuth(&zenoh.Config{}, priv, []string{notPub}); err == nil || !strings.Contains(err.Error(), "junk.pem") {
		t.Fatalf("bad known key: %v", err)
	}
	if err := pubkeyAuth(&zenoh.Config{}, priv, []string{filepath.Join(t.TempDir(), "none")}); err == nil {
		t.Fatal("missing known key accepted")
	}
	if err := pubkeyAuth(&zenoh.Config{}, "", nil); err != nil {
		t.Fatal(err)
	}
}
