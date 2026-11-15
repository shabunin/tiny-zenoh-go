package link

import (
	"bufio"
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func testCertPairPEM(t *testing.T, cn string, ips []net.IP) (certPEM, keyPEM []byte) {
	t.Helper()
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: cn},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		IPAddresses:           ips,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &k.PublicKey, k)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(k)
	if err != nil {
		t.Fatal(err)
	}
	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	return certPEM, keyPEM
}

func TestWithoutConfigAndTLSMaterialHardening(t *testing.T) {
	l, err := Parse("tls/127.0.0.1:7447?foo=bar#root_ca_certificate_raw=bad;verify_name_on_connect=false")
	if err != nil {
		t.Fatal(err)
	}
	if stripped := l.WithoutConfig(); stripped.String() != "tls/127.0.0.1:7447?foo=bar" {
		t.Fatalf("WithoutConfig().String() = %q", stripped.String())
	}
	if l.Config == "" {
		t.Fatal("WithoutConfig mutated original value")
	}

	// Empty _raw / _base64 / _file keys in tlsMaterial must fail rather than wipe defaults.
	for _, cfgKey := range []string{"root_ca_certificate_raw", "root_ca_certificate_base64", "root_ca_certificate_file"} {
		cfg := map[string]string{cfgKey: ""}
		if _, err := tlsMaterial(cfg, "root_ca_certificate", "", []byte("existing")); !errors.Is(err, ErrTLSConfig) {
			t.Fatalf("tlsMaterial(%q=\"\") err = %v, want ErrTLSConfig", cfgKey, err)
		}
	}

	// IgnoreLocatorConfig ignores injected #config when dialing tls/.
	srvCert, srvKey := testCertPairPEM(t, "srv.test", []net.IP{net.ParseIP("127.0.0.1")})
	otherCert, _ := testCertPairPEM(t, "other.test", []net.IP{net.ParseIP("127.0.0.1")})
	opts := Options{
		TLS: &TLSConfig{
			RootCACertificatePEM: srvCert,
			ListenCertificatePEM: srvCert,
			ListenPrivateKeyPEM:  srvKey,
		},
	}
	ln, err := ListenWith("tls/127.0.0.1:0", opts)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	accepted := make(chan error, 2)
	go func() {
		for range 2 {
			c, err := ln.Accept()
			if err == nil {
				_, _ = c.Read(make([]byte, 1))
				_ = c.Close()
			}
			accepted <- err
		}
	}()

	// Locator config overriding RootCACertificate with an untrusted PEM fails normally...
	badLoc := ln.Locator().String() + "#root_ca_certificate_raw=" + string(otherCert)
	if c, err := DialWith(badLoc, 2*time.Second, opts); err == nil {
		_ = c.Close()
		t.Fatal("DialWith accepted untrusted overridden root CA")
	}
	<-accepted

	// ...but succeeds when IgnoreLocatorConfig is enabled.
	opts.IgnoreLocatorConfig = true
	c, err := DialWith(badLoc, 2*time.Second, opts)
	if err != nil {
		t.Fatalf("DialWith(IgnoreLocatorConfig=true) failed: %v", err)
	}
	_, _ = c.Write([]byte("ok"))
	_ = c.Close()
	if err := <-accepted; err != nil {
		t.Fatal(err)
	}
}

func TestUDPLinkHardening(t *testing.T) {
	ln, err := Listen("udp/127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	uln := ln.(*udpListener)

	// Send one packet from one "active" peer first so it lands in accept queue and ln.links.
	goodPC, err := net.Dial("udp", ln.Locator().Address)
	if err != nil {
		t.Fatal(err)
	}
	defer goodPC.Close()
	if _, err := goodPC.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	acceptedLink, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer acceptedLink.Close()

	ul, ok := acceptedLink.(*udpLink)
	if !ok {
		t.Fatalf("accepted type = %T, want *udpLink", acceptedLink)
	}
	if ul.LocalAddr() == nil || ul.RemoteAddr() == nil {
		t.Fatalf("udpLink addrs: local=%v remote=%v", ul.LocalAddr(), ul.RemoteAddr())
	}
	buf := make([]byte, 64)
	n, err := ul.Read(buf)
	if err != nil || string(buf[:n]) != "hello" {
		t.Fatalf("first read: n=%d err=%v", n, err)
	}

	// Fill the 16-slot accept queue without draining Accept(); verify readLoop does NOT block
	// and excess unaccepted links are not leaked in ln.links.
	var flood []net.Conn
	defer func() {
		for _, fc := range flood {
			_ = fc.Close()
		}
	}()
	for range 24 {
		pc, err := net.Dial("udp", ln.Locator().Address)
		if err != nil {
			t.Fatal(err)
		}
		flood = append(flood, pc)
		if _, err := pc.Write([]byte("flood")); err != nil {
			t.Fatal(err)
		}
	}

	// Existing accepted link must still receive datagrams immediately without HoL blocking.
	if _, err := goodPC.Write([]byte("after-flood")); err != nil {
		t.Fatal(err)
	}
	_ = ul.SetDeadline(time.Now().Add(2 * time.Second))
	n, err = ul.Read(buf)
	if err != nil || string(buf[:n]) != "after-flood" {
		t.Fatalf("active link blocked during accept flood: n=%d data=%q err=%v", n, string(buf[:n]), err)
	}

	// Even after flooding >16 sources, queued + active links must stay <= 17 (1 active + 16 accept queue).
	uln.mu.Lock()
	linkCount := len(uln.links)
	uln.mu.Unlock()
	if linkCount > 17 {
		t.Fatalf("len(ln.links) = %d, want <= 17 (unaccepted drops must clean map)", linkCount)
	}
}

type serialPipePair struct {
	r *io.PipeReader
	w *io.PipeWriter
}

func (p *serialPipePair) Read(b []byte) (int, error)  { return p.r.Read(b) }
func (p *serialPipePair) Write(b []byte) (int, error) { return p.w.Write(b) }
func (p *serialPipePair) Close() error {
	_ = p.r.Close()
	_ = p.w.Close()
	return nil
}

func newSerialPipePair() (a, b io.ReadWriteCloser) {
	ar, bw := io.Pipe()
	br, aw := io.Pipe()
	return &serialPipePair{r: ar, w: aw}, &serialPipePair{r: br, w: bw}
}

func TestSerialSessionCloseUnblocksRead(t *testing.T) {
	aRW, bRW := newSerialPipePair()
	ln := &serialListener{
		s:      NewSerial(aRW),
		loc:    Locator{Protocol: "serial", Address: "pipe"},
		free:   make(chan struct{}, 1),
		closed: make(chan struct{}),
	}
	ln.free <- struct{}{}
	defer ln.Close()

	cli := NewSerial(bRW)
	defer cli.Close()

	go func() { _ = cli.Connect(2 * time.Second) }()

	s1, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}

	readErr := make(chan error, 1)
	go func() {
		buf := make([]byte, 256)
		_, err := s1.Read(buf)
		readErr <- err
	}()
	time.Sleep(20 * time.Millisecond)

	if err := s1.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-readErr:
		if !errors.Is(err, net.ErrClosed) {
			t.Fatalf("closed session Read err = %v, want net.ErrClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("serialSession.Close did not unblock Read")
	}
	if _, err := s1.Write([]byte("stale")); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("closed session Write err = %v, want net.ErrClosed", err)
	}

	// Subsequent session on the same device must receive frames cleanly without s1 interference.
	go func() { _ = cli.Connect(2 * time.Second) }()
	s2, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	if _, err := cli.Write([]byte("next-session")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 256)
	n, err := s2.Read(buf)
	if err != nil || string(buf[:n]) != "next-session" {
		t.Fatalf("s2.Read: n=%d data=%q err=%v", n, string(buf[:n]), err)
	}
}

func TestWebSocketHardening(t *testing.T) {
	ln, err := Listen("ws/127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	srvErr := make(chan error, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			srvErr <- err
			return
		}
		defer c.Close()
		var buf [64]byte
		n, err := c.Read(buf[:])
		if err == nil {
			_, err = c.Write(buf[:n])
		}
		srvErr <- err
	}()

	// DialWith timeout == 0 must NOT expire immediately.
	c, err := DialWith(ln.Locator().String(), 0, Options{})
	if err != nil {
		t.Fatalf("DialWith ws timeout=0 failed: %v", err)
	}
	wc := c.(*wsConn)
	if wc.LocalAddr() == nil || wc.RemoteAddr() == nil {
		t.Fatalf("wsConn addrs: local=%v remote=%v", wc.LocalAddr(), wc.RemoteAddr())
	}
	if _, err := wc.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	var got [16]byte
	n, err := wc.Read(got[:])
	if err != nil || string(got[:n]) != "ping" {
		t.Fatalf("ws echo: n=%d data=%q err=%v", n, string(got[:n]), err)
	}
	// Concurrent Close must be idempotent and race-free.
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = wc.Close()
		}()
	}
	wg.Wait()
	if err := <-srvErr; err != nil {
		t.Fatal(err)
	}

	// Malformed WebSocket frames must be rejected by readMessage.
	cases := []struct {
		name string
		raw  []byte
	}{
		{"non_canonical_16bit", []byte{0x82, 126, 0, 10, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}},
		{"non_canonical_64bit", func() []byte {
			b := []byte{0x82, 127, 0, 0, 0, 0, 0, 0, 0, 10}
			return append(b, make([]byte, 10)...)
		}()},
		{"fragmented_control", []byte{0x09, 0x00}},
		{"oversized_control", func() []byte {
			var b [128 + 2]byte
			b[0] = 0x89
			b[1] = 126
			binary.BigEndian.PutUint16(b[2:4], 126)
			return b[:]
		}()},
		{"unknown_control_opcode", []byte{0x8b, 0x00}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := &wsConn{br: bufio.NewReader(bytes.NewReader(tc.raw))}
			if _, err := w.readMessage(); !errors.Is(err, errWSProtocol) {
				t.Fatalf("readMessage(%s) err = %v, want errWSProtocol", tc.name, err)
			}
		})
	}
}

func TestUnixSocketErrorCleanupOrder(t *testing.T) {
	dir := t.TempDir()
	// Creating a directory at the socket path guarantees net.ListenUnix fails after acquiring flock.
	sockPath := filepath.Join(dir, "blocked")
	if err := os.Mkdir(sockPath, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := Listen("unixsock-stream/" + sockPath); err == nil {
		t.Fatal("Listen succeeded on existing directory")
	}
	if _, err := os.Lstat(sockPath + ".lock"); !os.IsNotExist(err) {
		t.Fatalf("stale lock file remained after Listen error: err=%v", err)
	}
}
