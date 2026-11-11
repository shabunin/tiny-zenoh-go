package transport

import (
	"crypto/hmac"
	"crypto/sha3"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"math/rand/v2"
	"strings"

	"github.com/shabunin/gozenoh/internal/codec"
	"github.com/shabunin/gozenoh/protocol"
)

// Handshake authentication (zenoh-rust io/zenoh-transport/src/unicast/
// establishment/ext/auth): INIT/OPEN extension 0x3 carries a chain of
// sub-extensions, one per method.
//
// usrpwd (sub-extension 0x2):
//
//	INIT SYN  initiator: unit (only when it has credentials)
//	INIT ACK  acceptor:  z64 nonce
//	OPEN SYN  initiator: zbytes(user) zbytes(HMAC-SHA3-256(key = nonce LE, password))
//	OPEN ACK  acceptor:  unit
//
// pubkey (sub-extension 0x1), the multilink exchange with an allow-list:
//
//	INIT SYN  alice: zbytes(n LE) zbytes(e LE)
//	INIT ACK  bob:   zbytes(n LE) zbytes(e LE) zbytes(RSA_alice(challenge u64 LE))
//	OPEN SYN  alice: zbytes(RSA_bob(challenge))
//	OPEN ACK  bob:   unit
//
// As in zenoh-rust, a node with a method configured requires it from every
// node that dials it, and a node with credentials requires the acceptor to
// take part.

const (
	extAuth      = 0x3
	authPubKeyID = 0x1
	authUsrPwdID = 0x2
)

// ErrAuth wraps handshake authentication failures.
var ErrAuth = errors.New("transport: authentication failed")

// Auth configures handshake authentication (zenoh-rust transport/auth).
type Auth struct {
	// User and Password are presented to nodes we dial (usrpwd). Both must
	// be set.
	User, Password string
	// Users are the user/password pairs accepted from nodes that dial us.
	// Setting Users or User+Password enables usrpwd, and then nodes that
	// dial us must present credentials found in Users.
	Users map[string]string
	// Key enables pubkey authentication: nodes prove they hold their RSA
	// key's private half.
	Key *RSAKey
	// KnownKeys are the public keys accepted from nodes that dial us (none
	// accepted when empty, as in zenoh-rust) and from nodes we dial (any
	// accepted when empty).
	KnownKeys []RSAPublicKey
}

func (a *Auth) usrpwd() bool {
	return a != nil && (len(a.Users) > 0 || a.creds())
}

func (a *Auth) creds() bool { return a != nil && a.User != "" && a.Password != "" }

func (a *Auth) pubkey() bool { return a != nil && a.Key != nil }

func (a *Auth) known(k RSAPublicKey) bool {
	for _, x := range a.KnownKeys {
		if x.Equal(k) {
			return true
		}
	}
	return false
}

// authState is one handshake's authentication exchange.
type authState struct {
	nonce     uint64 // usrpwd: initiator received / acceptor sent
	pkOpenSyn []byte // pubkey initiator: the challenge encrypted for the peer
	pkChal    []byte // pubkey acceptor: the challenge in clear
	pkInitAck []byte // pubkey acceptor: the challenge encrypted for the peer
}

func usrpwdHMAC(nonce uint64, password string) []byte {
	m := hmac.New(func() hash.Hash { return sha3.New256() }, binary.LittleEndian.AppendUint64(nil, nonce))
	m.Write([]byte(password))
	return m.Sum(nil)
}

func authFail(format string, a ...any) error {
	return fmt.Errorf("%w: %w: %s", ErrHandshake, ErrAuth, fmt.Sprintf(format, a...))
}

// authWrap packs sub-extensions into extension 0x3 (nil when there are none).
func authWrap(subs []protocol.Extension) *protocol.Extension {
	if len(subs) == 0 {
		return nil
	}
	w := codec.NewWriter()
	protocol.WriteExtensions(w, subs)
	return &protocol.Extension{ID: extAuth, Enc: protocol.ExtZBuf, Buf: w.Bytes()}
}

// authSubs unpacks extension 0x3 (absent = no sub-extensions).
func authSubs(e *protocol.Extension) (map[byte]protocol.Extension, error) {
	out := map[byte]protocol.Extension{}
	if e == nil || e.Enc != protocol.ExtZBuf || len(e.Buf) == 0 {
		return out, nil
	}
	list, err := protocol.ReadExtensions(codec.NewReader(e.Buf))
	if err != nil {
		return nil, authFail("bad auth extension: %v", err)
	}
	for _, x := range list {
		out[x.ID] = x
	}
	return out, nil
}

func zbuf(id byte, b []byte) protocol.Extension {
	return protocol.Extension{ID: id, Enc: protocol.ExtZBuf, Buf: b}
}

// ---- initiator ----

func (t *Unicast) authInitSyn() *protocol.Extension {
	a := t.cfg.Auth
	var subs []protocol.Extension
	if a.pubkey() {
		subs = append(subs, zbuf(authPubKeyID, a.Key.pub))
	}
	if a.creds() {
		subs = append(subs, protocol.Extension{ID: authUsrPwdID, Enc: protocol.ExtUnit})
	}
	return authWrap(subs)
}

func (t *Unicast) authInitAck(e *protocol.Extension) error {
	a := t.cfg.Auth
	subs, err := authSubs(e)
	if err != nil {
		return err
	}
	if a.pubkey() {
		x, ok := subs[authPubKeyID]
		if !ok || x.Enc != protocol.ExtZBuf {
			return authFail("the peer does not do pubkey authentication")
		}
		r := codec.NewReader(x.Buf)
		bob, _, err := readPubKey(r)
		if err != nil {
			return authFail("INIT ACK: %v", err)
		}
		if len(a.KnownKeys) > 0 && !a.known(bob) {
			return authFail("unknown peer public key")
		}
		ct, err := r.ReadZBytes()
		if err != nil {
			return authFail("INIT ACK: %v", err)
		}
		chal, err := a.Key.decrypt(ct)
		if err != nil {
			return authFail("%v", err)
		}
		if t.au.pkOpenSyn, err = bob.encrypt(chal); err != nil {
			return authFail("%v", err)
		}
	}
	if a.creds() {
		x, ok := subs[authUsrPwdID]
		if !ok || x.Enc != protocol.ExtZ64 {
			return authFail("the peer does not do usrpwd authentication")
		}
		t.au.nonce = x.U64
	}
	return nil
}

func (t *Unicast) authOpenSyn() *protocol.Extension {
	a := t.cfg.Auth
	var subs []protocol.Extension
	if a.pubkey() {
		w := codec.NewWriter()
		w.WriteZBytes(t.au.pkOpenSyn)
		subs = append(subs, zbuf(authPubKeyID, w.Bytes()))
	}
	if a.creds() {
		w := codec.NewWriter()
		w.WriteZBytes([]byte(a.User))
		w.WriteZBytes(usrpwdHMAC(t.au.nonce, a.Password))
		subs = append(subs, zbuf(authUsrPwdID, w.Bytes()))
	}
	return authWrap(subs)
}

func (t *Unicast) authOpenAck(e *protocol.Extension) error {
	a := t.cfg.Auth
	subs, err := authSubs(e)
	if err != nil {
		return err
	}
	if _, ok := subs[authPubKeyID]; a.pubkey() && !ok {
		return authFail("OPEN ACK lacks the pubkey confirmation")
	}
	if _, ok := subs[authUsrPwdID]; a.creds() && !ok {
		return authFail("OPEN ACK lacks the usrpwd confirmation")
	}
	return nil
}

// ---- acceptor ----

func (t *Unicast) authAcceptInitSyn(e *protocol.Extension) error {
	a := t.cfg.Auth
	subs, err := authSubs(e)
	if err != nil {
		return err
	}
	if a.pubkey() {
		x, ok := subs[authPubKeyID]
		if !ok || x.Enc != protocol.ExtZBuf {
			return authFail("the peer does not do pubkey authentication")
		}
		alice, _, err := readPubKey(codec.NewReader(x.Buf))
		if err != nil {
			return authFail("INIT SYN: %v", err)
		}
		if !a.known(alice) {
			return authFail("unknown peer public key")
		}
		t.au.pkChal = binary.LittleEndian.AppendUint64(nil, rand.Uint64())
		if t.au.pkInitAck, err = alice.encrypt(t.au.pkChal); err != nil {
			return authFail("%v", err)
		}
	}
	if a.usrpwd() {
		if _, ok := subs[authUsrPwdID]; !ok {
			return authFail("the peer presents no credentials")
		}
		t.au.nonce = rand.Uint64()
	}
	return nil
}

func (t *Unicast) authAcceptInitAck() *protocol.Extension {
	a := t.cfg.Auth
	var subs []protocol.Extension
	if a.pubkey() {
		w := codec.NewWriter()
		w.WriteRaw(a.Key.pub)
		w.WriteZBytes(t.au.pkInitAck)
		subs = append(subs, zbuf(authPubKeyID, w.Bytes()))
	}
	if a.usrpwd() {
		subs = append(subs, protocol.Extension{ID: authUsrPwdID, Enc: protocol.ExtZ64, U64: t.au.nonce})
	}
	return authWrap(subs)
}

func (t *Unicast) authAcceptOpenSyn(e *protocol.Extension) error {
	a := t.cfg.Auth
	subs, err := authSubs(e)
	if err != nil {
		return err
	}
	if a.pubkey() {
		x, ok := subs[authPubKeyID]
		if !ok || x.Enc != protocol.ExtZBuf {
			return authFail("OPEN SYN lacks the pubkey proof")
		}
		ct, err := codec.NewReader(x.Buf).ReadZBytes()
		if err != nil {
			return authFail("OPEN SYN: %v", err)
		}
		got, err := a.Key.decrypt(ct)
		if err != nil || !hmac.Equal(got, t.au.pkChal) {
			return authFail("the peer did not prove its key")
		}
	}
	if a.usrpwd() {
		x, ok := subs[authUsrPwdID]
		if !ok || x.Enc != protocol.ExtZBuf {
			return authFail("OPEN SYN lacks credentials")
		}
		r := codec.NewReader(x.Buf)
		user, err1 := r.ReadZBytes()
		mac, err2 := r.ReadZBytes()
		if err1 != nil || err2 != nil {
			return authFail("bad credentials")
		}
		pwd, ok := a.Users[string(user)]
		if !ok || !hmac.Equal(mac, usrpwdHMAC(t.au.nonce, pwd)) {
			return authFail("invalid user or password")
		}
		t.authUser = string(user)
	}
	return nil
}

func (t *Unicast) authAcceptOpenAck() *protocol.Extension {
	a := t.cfg.Auth
	var subs []protocol.Extension
	if a.pubkey() {
		subs = append(subs, protocol.Extension{ID: authPubKeyID, Enc: protocol.ExtUnit})
	}
	if a.usrpwd() {
		subs = append(subs, protocol.Extension{ID: authUsrPwdID, Enc: protocol.ExtUnit})
	}
	return authWrap(subs)
}

// AuthUser is the user name the peer authenticated with (usrpwd), on the
// accepting side; "" otherwise.
func (t *Unicast) AuthUser() string { return t.authUser }

// ParseUsrPwdDict parses a zenoh-rust usrpwd dictionary file: one
// "user:password" per line (both trimmed, neither empty; blank lines skipped).
func ParseUsrPwdDict(b []byte) (map[string]string, error) {
	out := map[string]string{}
	for i, line := range strings.Split(string(b), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		u, p, ok := strings.Cut(line, ":")
		u, p = strings.TrimSpace(u), strings.TrimSpace(p)
		if !ok || u == "" || p == "" {
			return nil, fmt.Errorf("transport: usrpwd dictionary line %d: want user:password", i+1)
		}
		out[u] = p
	}
	return out, nil
}
