package zenoh

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/shabunin/tiny-zenoh-go/transport"
)

// Handshake authentication against zenoh-rust (transport/auth/...).

func authPeer(t *testing.T, a *transport.Auth, connect ...string) (*Session, error) {
	s, err := Open(Config{
		Mode:     ModePeer,
		Listen:   []string{"tcp/127.0.0.1:0"},
		Connect:  connect,
		Scouting: ScoutingConfig{Disable: true},
		Auth:     a,
	})
	if err == nil {
		t.Cleanup(func() { _ = s.Close() })
	}
	return s, err
}

func authRustConf(t *testing.T, kv map[string]any) string {
	t.Helper()
	c := map[string]any{"scouting/multicast/enabled": false, "listen/endpoints": []string{}}
	for k, v := range kv {
		c[k] = v
	}
	b, _ := json.Marshal(c)
	return string(b)
}

// waitExit waits for the Rust process to end and reports whether it failed.
func (p *rustPeer) waitExit(t *testing.T) bool {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- p.cmd.Wait() }()
	select {
	case err := <-done:
		return err != nil
	case <-time.After(15 * time.Second):
		t.Fatal("rust process still running")
	}
	return false
}

func TestAuthUsrPwdRustClient(t *testing.T) {
	pfx := uniq(t)
	s, err := authPeer(t, &transport.Auth{Users: map[string]string{"rusty": "s3cret"}})
	if err != nil {
		t.Fatal(err)
	}
	sink := newSink()
	sub, _ := s.DeclareSubscriber(pfx, sink.handler)
	defer sub.Undeclare()

	t.Run("Good", func(t *testing.T) {
		startRust(t, authRustConf(t, map[string]any{
			"mode": "client", "connect/endpoints": s.Locators(),
			"transport/auth/usrpwd/user": "rusty", "transport/auth/usrpwd/password": "s3cret",
		}), false, "pub", pfx)
		if got := sink.wait(t, 2); string(got[0].Payload) != "from-rust" {
			t.Fatalf("got %q", got[0].Payload)
		}
	})
	t.Run("WrongPassword", func(t *testing.T) {
		rs := startRust(t, authRustConf(t, map[string]any{
			"mode": "client", "connect/endpoints": s.Locators(),
			"transport/auth/usrpwd/user": "rusty", "transport/auth/usrpwd/password": "wrong",
		}), false, "pub", pfx)
		if !rs.waitExit(t) {
			t.Fatal("rust client connected with a wrong password")
		}
	})
	t.Run("NoCredentials", func(t *testing.T) {
		rs := startRust(t, authRustConf(t, map[string]any{"mode": "client", "connect/endpoints": s.Locators()}),
			false, "pub", pfx)
		if !rs.waitExit(t) {
			t.Fatal("rust client connected without credentials")
		}
	})
}

func TestAuthUsrPwdGoToRust(t *testing.T) {
	dict := filepath.Join(t.TempDir(), "users.txt")
	if err := os.WriteFile(dict, []byte("gopher:pa55\nother:x\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	loc := freeLocator(t)
	pfx := uniq(t)
	rs := startRust(t, authRustConf(t, map[string]any{
		"mode": "peer", "listen/endpoints": []string{loc},
		"transport/auth/usrpwd/dictionary_file": dict,
	}), true, "sub", pfx, "1")

	// Wrong password: the Rust peer refuses the handshake.
	if _, err := Open(Config{Mode: ModeClient, Connect: []string{loc},
		Auth: &transport.Auth{User: "gopher", Password: "nope"}}); err == nil {
		t.Fatal("connected with a wrong password")
	}
	s, err := Open(Config{Mode: ModeClient, Connect: []string{loc},
		Auth: &transport.Auth{User: "gopher", Password: "pa55"}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	settle()
	_ = s.Put(pfx, []byte("authenticated"))
	if e := rs.next(); e.Payload != "authenticated" {
		t.Fatalf("rust got %+v", e)
	}
}

func writeRSAPEM(t *testing.T) (privPEM, pubPEM string) {
	t.Helper()
	k, err := rsa.GenerateKey(rand.Reader, 1024)
	if err != nil {
		t.Fatal(err)
	}
	priv := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(k)})
	pub := pem.EncodeToMemory(&pem.Block{Type: "RSA PUBLIC KEY", Bytes: x509.MarshalPKCS1PublicKey(&k.PublicKey)})
	return string(priv), string(pub)
}

func TestAuthPubKeyRustClient(t *testing.T) {
	rPriv, rPub := writeRSAPEM(t)
	gPriv, _ := writeRSAPEM(t)
	key, err := transport.ParseRSAKeyPEM([]byte(gPriv))
	if err != nil {
		t.Fatal(err)
	}
	known, err := transport.ParseRSAPublicKeyPEM([]byte(rPub))
	if err != nil {
		t.Fatal(err)
	}
	pfx := uniq(t)
	s, err := authPeer(t, &transport.Auth{Key: key, KnownKeys: []transport.RSAPublicKey{known}})
	if err != nil {
		t.Fatal(err)
	}
	sink := newSink()
	sub, _ := s.DeclareSubscriber(pfx, sink.handler)
	defer sub.Undeclare()
	startRust(t, authRustConf(t, map[string]any{
		"mode": "client", "connect/endpoints": s.Locators(),
		"transport/auth/pubkey/public_key_pem": rPub, "transport/auth/pubkey/private_key_pem": rPriv,
	}), false, "pub", pfx)
	if got := sink.wait(t, 2); string(got[0].Payload) != "from-rust" {
		t.Fatalf("got %q", got[0].Payload)
	}

	// A key we don't know is refused.
	oPriv, oPub := writeRSAPEM(t)
	rs := startRust(t, authRustConf(t, map[string]any{
		"mode": "client", "connect/endpoints": s.Locators(),
		"transport/auth/pubkey/public_key_pem": oPub, "transport/auth/pubkey/private_key_pem": oPriv,
	}), false, "pub", pfx)
	if !rs.waitExit(t) {
		t.Fatal("rust client with an unknown key connected")
	}
}
