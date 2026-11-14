package link

import (
	"bytes"
	"encoding/base64"
	"io"
	"os"
	"testing"
	"time"

	"github.com/shabunin/tiny-zenoh-go/internal/testcert"
	"github.com/shabunin/tiny-zenoh-go/transport"
)

func TestParseConfig(t *testing.T) {
	l, err := Parse("tls/localhost:7447?rel=1;prio=1-7#root_ca_certificate_base64=QUJD==;enable_mtls=true")
	if err != nil {
		t.Fatal(err)
	}
	if l.Protocol != "tls" || l.Address != "localhost:7447" {
		t.Fatalf("%+v", l)
	}
	c := l.ConfigValues()
	if c["root_ca_certificate_base64"] != "QUJD==" || c["enable_mtls"] != "true" {
		t.Errorf("config = %v", c)
	}
	if m := l.MetadataValues(); m["rel"] != "1" || m["prio"] != "1-7" {
		t.Errorf("metadata = %v", m)
	}
}

// echo accepts one link and echoes what it reads, one Read per Write so
// packet boundaries survive.
func echo(t *testing.T, ln Listener) {
	accepted := make(chan io.Closer, 1)
	t.Cleanup(func() {
		_ = ln.Close()
		select {
		case c := <-accepted:
			_ = c.Close()
		default:
		}
	})
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		accepted <- c
		defer c.Close()
		buf := make([]byte, 0xffff)
		for {
			n, err := c.Read(buf)
			if err != nil {
				return
			}
			if _, err := c.Write(buf[:n]); err != nil {
				return
			}
		}
	}()
}

func roundTrip(t *testing.T, c transport.Link, msgs ...[]byte) {
	t.Helper()
	if d, ok := c.(interface{ SetDeadline(time.Time) error }); ok {
		_ = d.SetDeadline(time.Now().Add(3 * time.Second))
	}
	buf := make([]byte, 0xffff)
	for _, m := range msgs {
		if _, err := c.Write(m); err != nil {
			t.Fatal(err)
		}
		n, err := c.Read(buf)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(buf[:n], m) {
			t.Fatalf("echo %d bytes, want %d", n, len(m))
		}
	}
}

func TestUDP(t *testing.T) {
	ln, err := Listen("udp/127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	if ln.Locator().Protocol != "udp" {
		t.Fatal(ln.Locator())
	}
	echo(t, ln)
	c, err := Dial(ln.Locator().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, ok := c.(transport.PacketLink); !ok {
		t.Fatal("udp link is not a PacketLink")
	}
	big := bytes.Repeat([]byte{7}, 60000)
	roundTrip(t, c, []byte("one"), []byte("two"), big)
}

func TestTLS(t *testing.T) {
	f, err := testcert.Write(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	keyB64 := func(path string) string {
		b, _ := os.ReadFile(path)
		return base64.StdEncoding.EncodeToString(b)
	}
	ln, err := ListenWith("tls/127.0.0.1:0#listen_private_key_base64="+keyB64(f.ServerKey),
		Options{TLS: &TLSConfig{ListenCertificate: f.ServerCert}})
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	if ln.Locator().Config != "" {
		t.Fatalf("listener locator leaks config: %v", ln.Locator())
	}
	echo(t, ln)
	_, port, _ := splitPort(ln.Locator().Address)

	// name verification against localhost
	c, err := DialWith("tls/localhost:"+port, time.Second, Options{TLS: &TLSConfig{RootCACertificate: f.CA}})
	if err != nil {
		t.Fatal(err)
	}
	roundTrip(t, c, []byte("hello over tls"))
	c.Close()

	// untrusted without the CA
	echo(t, ln)
	if c, err := Dial("tls/localhost:"+port, time.Second); err == nil {
		c.Close()
		t.Fatal("dial without CA succeeded")
	}
}

func TestTLSMutual(t *testing.T) {
	f, err := testcert.Write(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	srv := &TLSConfig{RootCACertificate: f.CA, ListenCertificate: f.ServerCert, ListenPrivateKey: f.ServerKey, EnableMTLS: true}
	ln, err := ListenWith("tls/127.0.0.1:0", Options{TLS: srv})
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	echo(t, ln)
	cli := &TLSConfig{RootCACertificate: f.CA, ConnectCertificate: f.ClientCert, ConnectPrivateKey: f.ClientKey}
	c, err := DialWith(ln.Locator().String(), time.Second, Options{TLS: cli})
	if err != nil {
		t.Fatal(err)
	}
	roundTrip(t, c, []byte("mutual"))
	c.Close()

	// no client certificate: the server rejects the handshake
	echo(t, ln)
	c, err = DialWith(ln.Locator().String(), time.Second, Options{TLS: &TLSConfig{RootCACertificate: f.CA}})
	if err == nil {
		defer c.Close()
		_, _ = c.Write([]byte("x"))
		_ = c.(interface{ SetDeadline(time.Time) error }).SetDeadline(time.Now().Add(time.Second))
		if _, err := c.Read(make([]byte, 8)); err == nil {
			t.Fatal("server accepted a client without certificate")
		}
	}
}

func splitPort(addr string) (string, string, error) {
	for i := len(addr) - 1; i >= 0; i-- {
		if addr[i] == ':' {
			return addr[:i], addr[i+1:], nil
		}
	}
	return addr, "", nil
}

func TestParseSortsParams(t *testing.T) {
	l, err := Parse("quic/127.0.0.1:7447?multistream=0;mixed_rel=1#b=2;a=1")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := l.String(), "quic/127.0.0.1:7447?mixed_rel=1;multistream=0#a=1;b=2"; got != want {
		t.Fatalf("%s, want %s", got, want)
	}
}
