package quic

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/shabunin/tiny-zenoh-go/link"
)

func selfSignedPEM(t *testing.T, cn string) (certPEM, keyPEM []byte) {
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
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
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
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
}

func TestQUICLinkIdentityAndZeroTimeout(t *testing.T) {
	srvCert, srvKey := selfSignedPEM(t, "quic-srv.test")
	opts := link.Options{
		TLS: &link.TLSConfig{
			RootCACertificatePEM: srvCert,
			ListenCertificatePEM: srvCert,
			ListenPrivateKeyPEM:  srvKey,
		},
	}
	for _, tc := range []struct {
		name  string
		query string
	}{
		{"stream", ""},
		{"datagram", "?rel=0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ln, err := link.ListenWith("quic/127.0.0.1:0"+tc.query, opts)
			if err != nil {
				t.Fatal(err)
			}
			defer ln.Close()

			srvDone := make(chan error, 1)
			clientDone := make(chan struct{})
			finishClient := sync.OnceFunc(func() { close(clientDone) })
			defer finishClient()
			go func() {
				c, err := ln.Accept()
				if err != nil {
					srvDone <- err
					return
				}
				defer c.Close()
				var b [16]byte
				n, rerr := c.Read(b[:])
				if rerr == nil {
					_, rerr = c.Write(b[:n])
				}
				<-clientDone
				srvDone <- rerr
			}()

			// Dial with timeout=0 must not immediately fail due to zero context deadline.
			c, err := link.DialWith(ln.Locator().String(), 0, opts)
			if err != nil {
				t.Fatalf("DialWith(%s, 0) failed: %v", tc.name, err)
			}
			defer c.Close()

			if got := link.PeerCommonName(c); got != "quic-srv.test" {
				t.Fatalf("PeerCommonName = %q, want %q", got, "quic-srv.test")
			}
			if lc, ok := c.(interface{ LocalAddr() net.Addr }); !ok || lc.LocalAddr() == nil {
				t.Fatalf("LocalAddr missing or nil on %T", c)
			}
			if rc, ok := c.(interface{ RemoteAddr() net.Addr }); !ok || rc.RemoteAddr() == nil {
				t.Fatalf("RemoteAddr missing or nil on %T", c)
			}

			if _, err := c.Write([]byte("hi")); err != nil {
				t.Fatal(err)
			}
			var got [16]byte
			n, err := c.Read(got[:])
			finishClient()
			if (err != nil && !errors.Is(err, io.EOF)) || string(got[:n]) != "hi" {
				t.Fatalf("Read: n=%d data=%q err=%v", n, string(got[:n]), err)
			}
			if err := <-srvDone; err != nil {
				t.Fatal(err)
			}
		})
	}
}
