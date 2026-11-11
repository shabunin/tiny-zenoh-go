package tls13

import (
	"bytes"
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func genKey(t *testing.T, kind string) crypto.Signer {
	t.Helper()
	var k crypto.Signer
	var err error
	switch kind {
	case "p256":
		k, err = ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	case "p384":
		k, err = ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	case "rsa":
		k, err = rsa.GenerateKey(rand.Reader, 2048)
	case "ed25519":
		_, k, err = ed25519.GenerateKey(rand.Reader)
	}
	if err != nil {
		t.Fatal(err)
	}
	return k
}

type issued struct {
	cert *x509.Certificate
	key  crypto.Signer
}

var serial int64

// issue makes a certificate for key, signed by parent (nil: self-signed).
func issue(t *testing.T, key crypto.Signer, parent *issued, tmpl *x509.Certificate) issued {
	t.Helper()
	serial++
	tmpl.SerialNumber = big.NewInt(serial)
	if tmpl.NotBefore.IsZero() {
		tmpl.NotBefore = time.Now().Add(-time.Hour)
		tmpl.NotAfter = time.Now().Add(time.Hour)
	}
	signer, ptmpl := key, tmpl
	if parent != nil {
		signer, ptmpl = parent.key, parent.cert
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ptmpl, key.Public(), signer)
	if err != nil {
		t.Fatal(err)
	}
	c, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return issued{c, key}
}

func caTmpl(name string) *x509.Certificate {
	return &x509.Certificate{Subject: pkix.Name{CommonName: name}, IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
}

func leafTmpl(name string, usage x509.ExtKeyUsage) *x509.Certificate {
	return &x509.Certificate{Subject: pkix.Name{CommonName: name}, DNSNames: []string{"localhost", "*.example.org"},
		IPAddresses: []net.IP{net.IPv4(127, 0, 0, 1)}, KeyUsage: x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{usage}}
}

func roots(t *testing.T, cs ...*x509.Certificate) []*Certificate {
	t.Helper()
	var out []*Certificate
	for _, c := range cs {
		p, err := ParseCertificate(c.Raw)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, p)
	}
	return out
}

// echoServer runs a crypto/tls TLS 1.3 echo server; it reports handshake
// errors on the returned channel.
func echoServer(t *testing.T, cfg *tls.Config) (string, <-chan error) {
	t.Helper()
	cfg.MinVersion = tls.VersionTLS13
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	errs := make(chan error, 16)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				s := tls.Server(c, cfg)
				if err := s.Handshake(); err != nil {
					errs <- err
					return
				}
				errs <- nil
				_, _ = io.Copy(s, s)
				_ = s.CloseWrite()
			}()
		}
	}()
	return ln.Addr().String(), errs
}

func dial(t *testing.T, addr string, cfg *Config) (*Conn, error) {
	t.Helper()
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	c, err := Client(nc, cfg)
	if err != nil {
		nc.Close()
		return nil, err
	}
	t.Cleanup(func() { c.Close() })
	return c, nil
}

// echo sends n bytes in uneven writes while reading them back.
func echo(t *testing.T, c *Conn, n int) {
	t.Helper()
	data := make([]byte, n)
	_, _ = rand.Read(data)
	go func() {
		for b, i := data, 1; len(b) > 0; i++ {
			k := min(len(b), i*997%40000+1)
			if _, err := c.Write(b[:k]); err != nil {
				return
			}
			b = b[k:]
		}
	}()
	got := make([]byte, n)
	if _, err := io.ReadFull(c, got); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("echoed data differs")
	}
}

func serverCert(t *testing.T, kind string) (tls.Certificate, *x509.Certificate) {
	ca := issue(t, genKey(t, "p256"), nil, caTmpl("ca"))
	leaf := issue(t, genKey(t, kind), &ca, leafTmpl("srv", x509.ExtKeyUsageServerAuth))
	return tls.Certificate{Certificate: [][]byte{leaf.cert.Raw}, PrivateKey: leaf.key}, ca.cert
}

func TestServerKeys(t *testing.T) {
	for _, kind := range []string{"p256", "p384", "rsa", "ed25519"} {
		t.Run(kind, func(t *testing.T) {
			cert, ca := serverCert(t, kind)
			addr, _ := echoServer(t, &tls.Config{Certificates: []tls.Certificate{cert}})
			c, err := dial(t, addr, &Config{ServerName: "localhost", RootCAs: roots(t, ca)})
			if err != nil {
				t.Fatal(err)
			}
			echo(t, c, 100_000)
		})
	}
}

func TestSuites(t *testing.T) {
	cert, ca := serverCert(t, "p256")
	addr, _ := echoServer(t, &tls.Config{Certificates: []tls.Certificate{cert}})
	saved := suites
	defer func() { suites = saved }()
	for _, s := range saved {
		suites = []suite{s}
		c, err := dial(t, addr, &Config{ServerName: "127.0.0.1", RootCAs: roots(t, ca)})
		if err != nil {
			t.Fatal(err)
		}
		if c.CipherSuite() != s.id {
			t.Fatalf("suite %#x, want %#x", c.CipherSuite(), s.id)
		}
		echo(t, c, 50_000)
	}
}

// TestChains: intermediates, CA kinds and signature algorithms in the
// chain.
func TestChains(t *testing.T) {
	for _, kind := range []string{"p256", "p384", "rsa", "ed25519"} {
		t.Run(kind, func(t *testing.T) {
			root := issue(t, genKey(t, kind), nil, caTmpl("root"))
			mid := issue(t, genKey(t, "rsa"), &root, caTmpl("intermediate"))
			leaf := issue(t, genKey(t, "p256"), &mid, leafTmpl("srv", x509.ExtKeyUsageServerAuth))
			addr, _ := echoServer(t, &tls.Config{Certificates: []tls.Certificate{{
				Certificate: [][]byte{leaf.cert.Raw, mid.cert.Raw}, PrivateKey: leaf.key}}})
			c, err := dial(t, addr, &Config{ServerName: "a.example.org", RootCAs: roots(t, root.cert)})
			if err != nil {
				t.Fatal(err)
			}
			echo(t, c, 1000)
			// The intermediate alone as the anchor works too.
			if _, err := dial(t, addr, &Config{ServerName: "localhost", RootCAs: roots(t, mid.cert)}); err != nil {
				t.Fatal(err)
			}
			// So does the pinned leaf.
			if _, err := dial(t, addr, &Config{ServerName: "localhost", RootCAs: roots(t, leaf.cert)}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRejects(t *testing.T) {
	ca := issue(t, genKey(t, "p256"), nil, caTmpl("ca"))
	other := issue(t, genKey(t, "p256"), nil, caTmpl("other"))
	notCA := issue(t, genKey(t, "p256"), &ca, &x509.Certificate{Subject: pkix.Name{CommonName: "notca"},
		BasicConstraintsValid: true})
	srv := func(leaf issued, chain ...*x509.Certificate) string {
		c := tls.Certificate{Certificate: [][]byte{leaf.cert.Raw}, PrivateKey: leaf.key}
		for _, x := range chain {
			c.Certificate = append(c.Certificate, x.Raw)
		}
		addr, _ := echoServer(t, &tls.Config{Certificates: []tls.Certificate{c}})
		return addr
	}
	good := srv(issue(t, genKey(t, "p256"), &ca, leafTmpl("srv", x509.ExtKeyUsageServerAuth)))
	expired := issue(t, genKey(t, "p256"), &ca, leafTmpl("srv", x509.ExtKeyUsageServerAuth))
	tmpl := leafTmpl("old", x509.ExtKeyUsageServerAuth)
	tmpl.NotBefore, tmpl.NotAfter = time.Now().Add(-48*time.Hour), time.Now().Add(-24*time.Hour)
	expired = issue(t, genKey(t, "p256"), &ca, tmpl)
	clientOnly := srv(issue(t, genKey(t, "p256"), &ca, leafTmpl("srv", x509.ExtKeyUsageClientAuth)))
	viaNotCA := srv(issue(t, genKey(t, "p256"), &notCA, leafTmpl("srv", x509.ExtKeyUsageServerAuth)), notCA.cert)
	expiredAddr := srv(expired)

	cases := []struct {
		name string
		addr string
		cfg  Config
		want string
	}{
		{"unknown CA", good, Config{ServerName: "localhost", RootCAs: roots(t, other.cert)}, "not signed by a trusted"},
		{"no CA", good, Config{ServerName: "localhost"}, "no root CA"},
		{"wrong name", good, Config{ServerName: "zenoh.io", RootCAs: roots(t, ca.cert)}, "not valid for zenoh.io"},
		{"wrong IP", good, Config{ServerName: "10.0.0.1", RootCAs: roots(t, ca.cert)}, "not valid for IP"},
		{"wildcard depth", good, Config{ServerName: "a.b.example.org", RootCAs: roots(t, ca.cert)}, "not valid for"},
		{"expired", expiredAddr, Config{ServerName: "localhost", RootCAs: roots(t, ca.cert)}, "not valid at"},
		{"client EKU", clientOnly, Config{ServerName: "localhost", RootCAs: roots(t, ca.cert)}, "extended key usage"},
		{"not a CA", viaNotCA, Config{ServerName: "localhost", RootCAs: roots(t, ca.cert)}, "not a certificate authority"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := dial(t, tc.addr, &tc.cfg)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want %q", err, tc.want)
			}
		})
	}

	// Accepted on purpose: skipped name check, unset clock.
	if _, err := dial(t, good, &Config{ServerName: "zenoh.io", RootCAs: roots(t, ca.cert), SkipNameVerification: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := dial(t, expiredAddr, &Config{ServerName: "localhost", RootCAs: roots(t, ca.cert),
		Time: func() time.Time { return time.Unix(0, 0) }}); err != nil {
		t.Fatal(err)
	}
}

func pemKey(t *testing.T, k crypto.Signer, format string) []byte {
	t.Helper()
	var der []byte
	var err error
	typ := "PRIVATE KEY"
	switch format {
	case "pkcs8":
		der, err = x509.MarshalPKCS8PrivateKey(k)
	case "sec1":
		der, err = x509.MarshalECPrivateKey(k.(*ecdsa.PrivateKey))
		typ = "EC PRIVATE KEY"
	case "pkcs1":
		der = x509.MarshalPKCS1PrivateKey(k.(*rsa.PrivateKey))
		typ = "RSA PRIVATE KEY"
	}
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der})
}

func TestClientCertificates(t *testing.T) {
	cert, ca := serverCert(t, "p256")
	clientCA := issue(t, genKey(t, "p256"), nil, caTmpl("client ca"))
	pool := x509.NewCertPool()
	pool.AddCert(clientCA.cert)
	var gotCN string
	addr, errs := echoServer(t, &tls.Config{Certificates: []tls.Certificate{cert}, ClientAuth: tls.RequireAndVerifyClientCert,
		ClientCAs: pool, VerifyConnection: func(cs tls.ConnectionState) error {
			gotCN = cs.PeerCertificates[0].Subject.CommonName
			return nil
		}})
	for _, tc := range []struct{ kind, format string }{
		{"p256", "pkcs8"}, {"p256", "sec1"}, {"p384", "pkcs8"}, {"rsa", "pkcs8"}, {"rsa", "pkcs1"}, {"ed25519", "pkcs8"},
	} {
		t.Run(tc.kind+"/"+tc.format, func(t *testing.T) {
			k := genKey(t, tc.kind)
			leaf := issue(t, k, &clientCA, leafTmpl("client-"+tc.kind, x509.ExtKeyUsageClientAuth))
			key, err := ParsePrivateKeyPEM(pemKey(t, k, tc.format))
			if err != nil {
				t.Fatal(err)
			}
			c, err := dial(t, addr, &Config{ServerName: "localhost", RootCAs: roots(t, ca),
				Certificate: [][]byte{leaf.cert.Raw}, Key: key})
			if err != nil {
				t.Fatal(err)
			}
			echo(t, c, 1000)
			if err := <-errs; err != nil {
				t.Fatal(err)
			}
			if gotCN != "client-"+tc.kind {
				t.Fatalf("server saw %q", gotCN)
			}
		})
	}
	// Without a certificate the server refuses (after our Finished, so
	// the error shows on the first read).
	c, err := dial(t, addr, &Config{ServerName: "localhost", RootCAs: roots(t, ca)})
	if err == nil {
		_, err = c.Read(make([]byte, 1))
	}
	if err == nil {
		t.Fatal("connected without a client certificate")
	}
	if serr := <-errs; serr == nil {
		t.Fatal("server accepted no client certificate")
	}
}

// TestKeyUpdate: our KeyUpdate before the GCM record limit.
func TestKeyUpdate(t *testing.T) {
	cert, ca := serverCert(t, "p256")
	addr, _ := echoServer(t, &tls.Config{Certificates: []tls.Certificate{cert}})
	c, err := dial(t, addr, &Config{ServerName: "localhost", RootCAs: roots(t, ca)})
	if err != nil {
		t.Fatal(err)
	}
	defer func(v uint64) { keyUpdateAfter = v }(keyUpdateAfter)
	keyUpdateAfter = 5
	secret := c.out.hc.secret
	echo(t, c, 200_000) // many records, several updates
	if bytes.Equal(secret, c.out.hc.secret) || c.out.hc.seq > 5 {
		t.Fatalf("no key update (seq %d)", c.out.hc.seq)
	}
}

func TestECDSA(t *testing.T) {
	msg := []byte("zenoh")
	for _, tc := range []struct {
		c  *curve
		ec elliptic.Curve
		h  func([]byte) []byte
	}{
		{p256(), elliptic.P256(), func(b []byte) []byte { d := sha256.Sum256(b); return d[:] }},
		{p384(), elliptic.P384(), func(b []byte) []byte { d := sha512.Sum384(b); return d[:] }},
	} {
		k, _ := ecdsa.GenerateKey(tc.ec, rand.Reader)
		pt, err := tc.c.decodePoint(elliptic.Marshal(tc.ec, k.X, k.Y))
		if err != nil {
			t.Fatal(err)
		}
		for range 5 {
			sig, _ := ecdsa.SignASN1(rand.Reader, k, tc.h(msg))
			if !tc.c.ecdsaVerify(pt, tc.h(msg), sig) {
				t.Fatal("valid signature rejected")
			}
			if tc.c.ecdsaVerify(pt, tc.h([]byte("other")), sig) {
				t.Fatal("signature over another message accepted")
			}
			mine, err := tc.c.ecdsaSign(rand.Reader, k.D, tc.h(msg))
			if err != nil {
				t.Fatal(err)
			}
			if !ecdsa.VerifyASN1(&k.PublicKey, tc.h(msg), mine) {
				t.Fatal("our signature rejected by crypto/ecdsa")
			}
		}
	}
}

// TestOpenSSLKeyUpdate: the server updates its keys and asks us to update
// ours (openssl s_server's "K" command).
func TestOpenSSLKeyUpdate(t *testing.T) {
	bin, err := exec.LookPath("openssl")
	if err != nil {
		t.Skip("no openssl")
	}
	dir := t.TempDir()
	ca := issue(t, genKey(t, "p256"), nil, caTmpl("ca"))
	leaf := issue(t, genKey(t, "p256"), &ca, leafTmpl("srv", x509.ExtKeyUsageServerAuth))
	_ = os.WriteFile(filepath.Join(dir, "c.pem"), pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leaf.cert.Raw}), 0o600)
	_ = os.WriteFile(filepath.Join(dir, "k.pem"), pemKey(t, leaf.key, "pkcs8"), 0o600)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	cmd := exec.Command(bin, "s_server", "-tls1_3", "-accept", addr, "-cert", filepath.Join(dir, "c.pem"),
		"-key", filepath.Join(dir, "k.pem"), "-naccept", "1")
	stdin, _ := cmd.StdinPipe()
	var out syncBuffer
	cmd.Stdout, cmd.Stderr = &out, &out
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
	var c *Conn
	for i := 0; ; i++ {
		nc, err := net.Dial("tcp", addr)
		if err == nil {
			if c, err = Client(nc, &Config{ServerName: "localhost", RootCAs: roots(t, ca.cert)}); err != nil {
				t.Fatal(err)
			}
			break
		}
		if i == 50 {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	defer c.Close()
	secret := c.out.hc.secret
	readLine := func(want string) {
		t.Helper()
		_ = c.conn.(net.Conn).SetReadDeadline(time.Now().Add(5 * time.Second))
		var got []byte
		b := make([]byte, 64)
		for !bytes.Contains(got, []byte(want)) {
			n, err := c.Read(b)
			if err != nil {
				t.Fatalf("read: %v (got %q; server: %s)", err, got, out.String())
			}
			got = append(got, b[:n]...)
		}
	}
	waitOut := func(want string) {
		t.Helper()
		for deadline := time.Now().Add(5 * time.Second); !strings.Contains(out.String(), want); time.Sleep(50 * time.Millisecond) {
			if time.Now().After(deadline) {
				t.Fatalf("server never printed %q: %s", want, out.String())
			}
		}
	}
	_, _ = c.Write([]byte("before\n"))
	waitOut("before")
	_, _ = io.WriteString(stdin, "K\n")
	time.Sleep(500 * time.Millisecond) // separate stdin reads
	_, _ = io.WriteString(stdin, "from server\n")
	readLine("from server")
	if bytes.Equal(secret, c.out.hc.secret) {
		t.Fatal("did not update our keys")
	}
	_, _ = c.Write([]byte("after\n"))
	waitOut("after")
}

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// TestField: Montgomery arithmetic against math/big, edge values included.
func TestField(t *testing.T) {
	for _, c := range []*curve{p256(), p384()} {
		f, p := c.f, c.p
		vals := []*big.Int{big.NewInt(0), big.NewInt(1), big.NewInt(2), new(big.Int).Sub(p, big.NewInt(1)),
			new(big.Int).Sub(p, big.NewInt(2)), new(big.Int).Rsh(p, 1)}
		for range 200 {
			v, _ := rand.Int(rand.Reader, p)
			vals = append(vals, v)
		}
		for i, x := range vals {
			y := vals[(i*7+3)%len(vals)]
			mx, my := f.mont(x), f.mont(y)
			var z fe
			check := func(op string, got *fe, want *big.Int) {
				t.Helper()
				if g := f.unmont(got); g.Cmp(want.Mod(want, p)) != 0 {
					t.Fatalf("%d-bit %s(%x, %x) = %x, want %x", p.BitLen(), op, x, y, g, want)
				}
			}
			f.mul(&z, &mx, &my)
			check("mul", &z, new(big.Int).Mul(x, y))
			f.add(&z, &mx, &my)
			check("add", &z, new(big.Int).Add(x, y))
			f.sub(&z, &mx, &my)
			check("sub", &z, new(big.Int).Sub(x, y))
			if x.Sign() != 0 {
				f.inv(&z, &mx)
				check("inv", &z, new(big.Int).ModInverse(x, p))
			}
		}
	}
}

func BenchmarkECDSAVerify(b *testing.B) {
	k, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	d := sha256.Sum256([]byte("zenoh"))
	sig, _ := ecdsa.SignASN1(rand.Reader, k, d[:])
	pub, _ := k.PublicKey.Bytes()
	pt, _ := p256().decodePoint(pub)
	b.ReportAllocs()
	for b.Loop() {
		if !p256().ecdsaVerify(pt, d[:], sig) {
			b.Fatal("rejected")
		}
	}
}
