package zenoh

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/shabunin/tiny-zenoh-go/internal/testcert"
	"github.com/shabunin/tiny-zenoh-go/link"
	"github.com/shabunin/tiny-zenoh-go/transport"
)

// Sessions over tls/ with package tls13 — the TLS of TinyGo builds — on
// the client side, against Go (crypto/tls) and Rust (rustls) listeners.

func readPEM(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// tls13Dial dials with link.DialTLS13 and PEM bytes only, as a board would.
func tls13Dial(t *testing.T, f *testcert.Files, mtls bool) func(string, time.Duration) (transport.Link, error) {
	cfg := &link.TLSConfig{RootCACertificatePEM: readPEM(t, f.CA)}
	if mtls {
		cfg.ConnectCertificatePEM, cfg.ConnectPrivateKeyPEM = readPEM(t, f.ClientCert), readPEM(t, f.ClientKey)
	}
	return func(loc string, timeout time.Duration) (transport.Link, error) {
		return link.DialTLS13(loc, timeout, cfg)
	}
}

// rsaCerts writes an RSA CA and an RSA server certificate for localhost
// (rustls then signs the handshake with RSA-PSS).
func rsaCerts(t *testing.T) (ca, cert, key string) {
	t.Helper()
	dir := t.TempDir()
	caKey, _ := rsa.GenerateKey(rand.Reader, 2048)
	now := time.Now()
	caTmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "rsa ca"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	k, _ := rsa.GenerateKey(rand.Reader, 2048)
	leaf := &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "localhost"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), DNSNames: []string{"localhost"},
		IPAddresses: []net.IP{net.IPv4(127, 0, 0, 1)}, KeyUsage: x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	ca0, _ := x509.ParseCertificate(caDER)
	der, err := x509.CreateCertificate(rand.Reader, leaf, ca0, &k.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	ca, cert, key = filepath.Join(dir, "ca.pem"), filepath.Join(dir, "srv.pem"), filepath.Join(dir, "srv.key")
	_ = os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}), 0o600)
	_ = os.WriteFile(cert, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600)
	_ = os.WriteFile(key, pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(k)}), 0o600)
	return ca, cert, key
}

// TestTLS13GoRouter: tls13 clients on a Go router (crypto/tls), with and
// without mutual TLS.
func TestTLS13GoRouter(t *testing.T) {
	f, err := testcert.Write(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, mtls := range []bool{false, true} {
		t.Run(map[bool]string{false: "tls", true: "mtls"}[mtls], func(t *testing.T) {
			r, err := Open(Config{Mode: ModeRouter, Listen: []string{"tls/127.0.0.1:0"}, Scouting: ScoutingConfig{Disable: true},
				TLS: &link.TLSConfig{ListenCertificate: f.ServerCert, ListenPrivateKey: f.ServerKey,
					RootCACertificate: f.CA, EnableMTLS: mtls}})
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			loc := r.Locators()[0]
			open := func() *Session {
				s, err := Open(Config{Connect: []string{loc}, Dial: tls13Dial(t, &f, mtls)})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = s.Close() })
				return s
			}
			a, b := open(), open()
			checkMesh(t, a, b)
			if mtls {
				// Without a client certificate the router refuses.
				if s, err := Open(Config{Connect: []string{loc}, Dial: tls13Dial(t, &f, false),
					ConnectTimeout: 2 * time.Second}); err == nil {
					s.Close()
					t.Fatal("connected without a client certificate")
				}
			}
		})
	}
}

// TestTLS13RustRouter: tls13 clients on a Rust router (rustls): ECDSA and
// RSA server certificates, and mutual TLS.
func TestTLS13RustRouter(t *testing.T) {
	f, err := testcert.Write(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	rsaCA, rsaCert, rsaKey := rsaCerts(t)
	cases := []struct {
		name string
		rust map[string]any
		dial func(string, time.Duration) (transport.Link, error)
	}{
		{"ecdsa", map[string]any{
			"transport/link/tls/listen_certificate": f.ServerCert,
			"transport/link/tls/listen_private_key": f.ServerKey,
		}, tls13Dial(t, &f, false)},
		{"rsa", map[string]any{
			"transport/link/tls/listen_certificate": rsaCert,
			"transport/link/tls/listen_private_key": rsaKey,
		}, func(loc string, timeout time.Duration) (transport.Link, error) {
			return link.DialTLS13(loc, timeout, &link.TLSConfig{RootCACertificate: rsaCA})
		}},
		{"mtls", map[string]any{
			"transport/link/tls/root_ca_certificate": f.CA,
			"transport/link/tls/listen_certificate":  f.ServerCert,
			"transport/link/tls/listen_private_key":  f.ServerKey,
			"transport/link/tls/enable_mtls":         true,
		}, tls13Dial(t, &f, true)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			loc := freePort(t, "tls")
			pfx := uniq(t)
			startRust(t, rustConf(linkCase{rust: c.rust}, "router", loc, ""), true, "queryable", pfx+"/q/**", "10")
			s, err := Open(Config{Connect: []string{loc}, Dial: c.dial})
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			waitFor(t, "rust queryable", func() bool {
				replies, err := collect(s.Get(pfx+"/q/one", WithTimeout(time.Second)))
				return err == nil && len(replies) == 1 && replies[0].Sample != nil
			})
			// Pub/sub through the router, a payload over many records.
			s2, err := Open(Config{Connect: []string{loc}, Dial: c.dial})
			if err != nil {
				t.Fatal(err)
			}
			defer s2.Close()
			sink := newSink()
			sub, _ := s2.DeclareSubscriber(pfx+"/big", sink.handler)
			defer sub.Undeclare()
			settle()
			big := []byte(strings.Repeat("0123456789", 50000))
			if err := s.Put(pfx+"/big", big); err != nil {
				t.Fatal(err)
			}
			if got := sink.wait(t, 1)[0]; string(got.Payload) != string(big) {
				t.Errorf("big payload: got %d bytes", len(got.Payload))
			}
		})
	}
}
