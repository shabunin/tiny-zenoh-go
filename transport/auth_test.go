package transport

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"testing"

	"github.com/shabunin/gozenoh/protocol"
)

func authNode(t *testing.T, a *Auth) *mlNode {
	n := &mlNode{msgs: make(chan string, 64)}
	n.cfg = Config{ZID: testZID(t), WhatAmI: protocol.Peer, Auth: a}
	return n
}

func TestAuthUsrPwd(t *testing.T) {
	users := map[string]string{"alice": "secret", "bob": "hunter2"}
	cases := []struct {
		name       string
		dial, acc  *Auth
		ok         bool
		wantUser   string
		failDialer bool // the dialer itself detects the failure
	}{
		{"good", &Auth{User: "alice", Password: "secret"}, &Auth{Users: users}, true, "alice", false},
		{"wrong password", &Auth{User: "alice", Password: "nope"}, &Auth{Users: users}, false, "", false},
		{"unknown user", &Auth{User: "carol", Password: "secret"}, &Auth{Users: users}, false, "", false},
		{"no credentials", nil, &Auth{Users: users}, false, "", false},
		{"acceptor without auth", &Auth{User: "alice", Password: "secret"}, nil, false, "", true},
		{"neither", nil, nil, true, "", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			a, b := authNode(t, c.dial), authNode(t, c.acc)
			oerr, aerr := connect(t, a, b, nil)
			if c.ok {
				if oerr != nil || aerr != nil {
					t.Fatalf("handshake: %v / %v", oerr, aerr)
				}
				if got := b.tr.AuthUser(); got != c.wantUser {
					t.Errorf("AuthUser = %q, want %q", got, c.wantUser)
				}
				return
			}
			if oerr == nil || aerr == nil {
				t.Fatalf("handshake succeeded: %v / %v", oerr, aerr)
			}
			failed := aerr
			if c.failDialer {
				failed = oerr
			}
			if !errors.Is(failed, ErrAuth) {
				t.Errorf("error is not ErrAuth: %v / %v", oerr, aerr)
			}
		})
	}
}

func TestAuthUsrPwdHMAC(t *testing.T) {
	// HMAC-SHA3-256 (zenoh_crypto::hmac) keyed by the nonce's 8 LE bytes, over the password
	// (reference value from Python's hmac + hashlib.sha3_256).
	got := hex.EncodeToString(usrpwdHMAC(0x0102030405060708, "pwd1"))
	if want := "c71b071e0ef51d0cc3e5ea6bd652c1f3e91a42895955d363ffc23a04182cb190"; got != want {
		t.Fatalf("hmac %s, want %s", got, want)
	}
}

func rsaPEM(t *testing.T, bits int) (priv, pub []byte) {
	t.Helper()
	k, err := rsa.GenerateKey(rand.Reader, bits)
	if err != nil {
		t.Fatal(err)
	}
	priv = pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(k)})
	pub = pem.EncodeToMemory(&pem.Block{Type: "RSA PUBLIC KEY", Bytes: x509.MarshalPKCS1PublicKey(&k.PublicKey)})
	return
}

func TestRSAPEM(t *testing.T) {
	priv, pub := rsaPEM(t, 1024)
	k, err := ParseRSAKeyPEM(priv)
	if err != nil {
		t.Fatal(err)
	}
	p, err := ParseRSAPublicKeyPEM(pub)
	if err != nil {
		t.Fatal(err)
	}
	if !k.Public().Equal(p) {
		t.Fatal("public key does not match the private key")
	}
	ct, err := p.encrypt([]byte("challenge"))
	if err != nil {
		t.Fatal(err)
	}
	if m, err := k.decrypt(ct); err != nil || string(m) != "challenge" {
		t.Fatalf("decrypt: %q %v", m, err)
	}
	if _, err := ParseRSAKeyPEM(pub); err == nil {
		t.Error("public key parsed as private")
	}
	if _, err := ParseRSAPublicKeyPEM([]byte("-----BEGIN RSA PUBLIC KEY-----\nMAM=\n-----END RSA PUBLIC KEY-----\n")); err == nil {
		t.Error("truncated key accepted")
	}
}

func TestAuthPubKey(t *testing.T) {
	ka, _ := NewRSAKey(1024)
	kb, _ := NewRSAKey(1024)
	kc, _ := NewRSAKey(1024)
	cases := []struct {
		name      string
		dial, acc *Auth
		ok        bool
	}{
		{"known", &Auth{Key: ka}, &Auth{Key: kb, KnownKeys: []RSAPublicKey{ka.Public()}}, true},
		{"dialer pins acceptor", &Auth{Key: ka, KnownKeys: []RSAPublicKey{kb.Public()}},
			&Auth{Key: kb, KnownKeys: []RSAPublicKey{ka.Public()}}, true},
		{"unknown dialer", &Auth{Key: kc}, &Auth{Key: kb, KnownKeys: []RSAPublicKey{ka.Public()}}, false},
		{"acceptor knows nobody", &Auth{Key: ka}, &Auth{Key: kb}, false},
		{"dialer rejects acceptor", &Auth{Key: ka, KnownKeys: []RSAPublicKey{kc.Public()}},
			&Auth{Key: kb, KnownKeys: []RSAPublicKey{ka.Public()}}, false},
		{"dialer without key", nil, &Auth{Key: kb, KnownKeys: []RSAPublicKey{ka.Public()}}, false},
		{"both methods", &Auth{Key: ka, User: "u", Password: "p"},
			&Auth{Key: kb, KnownKeys: []RSAPublicKey{ka.Public()}, Users: map[string]string{"u": "p"}}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			oerr, aerr := connect(t, authNode(t, c.dial), authNode(t, c.acc), nil)
			if c.ok != (oerr == nil && aerr == nil) {
				t.Fatalf("handshake: %v / %v", oerr, aerr)
			}
			if !c.ok && !errors.Is(oerr, ErrAuth) && !errors.Is(aerr, ErrAuth) {
				t.Errorf("error is not ErrAuth: %v / %v", oerr, aerr)
			}
		})
	}
}
