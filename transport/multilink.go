package transport

import (
	"bytes"
	crand "crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"math/big"
	"math/rand/v2"

	"github.com/shabunin/tiny-zenoh-go/internal/codec"
	"github.com/shabunin/tiny-zenoh-go/protocol"
)

// Multilink (zenoh-rust io/zenoh-transport/src/unicast/establishment/ext/
// multilink.rs and ext/auth/pubkey.rs). Each node has one RSA key pair; in
// INIT/OPEN extension 0x4 the two nodes swap public keys and each proves it
// holds its private key:
//
//	INIT SYN  alice: zbytes(n LE) zbytes(e LE)
//	INIT ACK  bob:   zbytes(n LE) zbytes(e LE) zbytes(RSA_alice(challenge u64 LE))
//	OPEN SYN  alice: zbytes(RSA_bob(the challenge alice decrypted))
//	OPEN ACK  bob:   unit
//
// RSA is PKCS #1 v1.5 encryption with 512-bit keys, as zenoh-rust uses. A
// later link from the same peer may join its transport only if it presents
// the same, proven, key: the exchange binds links together, it is not meant
// to keep secrets (512-bit RSA would not).

const (
	extMultiLink = 0x4
	mlKeyBits    = 512
)

var errMultiLink = errors.New("transport: multilink handshake failed")

// RSAKey is an RSA key pair, as used by multilink and by pubkey
// authentication.
type RSAKey struct {
	n, e, d *big.Int
	pub     []byte // encoded public key
}

// MultiLinkKey is a node's multilink RSA key pair.
type MultiLinkKey = RSAKey

// NewMultiLinkKey generates a key pair (512-bit RSA, as zenoh-rust).
func NewMultiLinkKey() (*MultiLinkKey, error) { return NewRSAKey(mlKeyBits) }

// NewRSAKey generates an RSA key pair of the given size (e = 65537).
func NewRSAKey(bits int) (*RSAKey, error) {
	e := big.NewInt(65537)
	one := big.NewInt(1)
	for range 100 {
		p, err := crand.Prime(crand.Reader, bits/2)
		if err != nil {
			return nil, err
		}
		q, err := crand.Prime(crand.Reader, bits-bits/2)
		if err != nil {
			return nil, err
		}
		if p.Cmp(q) == 0 {
			continue
		}
		n := new(big.Int).Mul(p, q)
		if n.BitLen() != bits {
			continue
		}
		phi := new(big.Int).Mul(new(big.Int).Sub(p, one), new(big.Int).Sub(q, one))
		d := new(big.Int).ModInverse(e, phi)
		if d == nil {
			continue
		}
		return newRSAKey(n, e, d), nil
	}
	return nil, errors.New("transport: could not generate an RSA key")
}

func newRSAKey(n, e, d *big.Int) *RSAKey {
	return &RSAKey{n: n, e: e, d: d, pub: encodePubKey(n, e)}
}

// Public returns the public half of k.
func (k *RSAKey) Public() RSAPublicKey { return RSAPublicKey{k.n, k.e} }

// leBytes is BigUint::to_bytes_le: little-endian, no high zero bytes.
func leBytes(x *big.Int) []byte {
	b := x.Bytes()
	for i, j := 0, len(b)-1; i < j; i, j = i+1, j-1 {
		b[i], b[j] = b[j], b[i]
	}
	if len(b) == 0 {
		b = []byte{0}
	}
	return b
}

func fromLE(b []byte) *big.Int {
	r := make([]byte, len(b))
	for i := range b {
		r[len(b)-1-i] = b[i]
	}
	return new(big.Int).SetBytes(r)
}

func encodePubKey(n, e *big.Int) []byte {
	w := codec.NewWriter()
	w.WriteZBytes(leBytes(n))
	w.WriteZBytes(leBytes(e))
	return w.Bytes()
}

// RSAPublicKey is an RSA public key.
type RSAPublicKey struct{ n, e *big.Int }

type rsaPub = RSAPublicKey

// Equal reports whether k and o are the same key.
func (k RSAPublicKey) Equal(o RSAPublicKey) bool {
	return k.n != nil && o.n != nil && k.n.Cmp(o.n) == 0 && k.e.Cmp(o.e) == 0
}

func readPubKey(r *codec.Reader) (rsaPub, []byte, error) {
	nb, err := r.ReadZBytes()
	if err != nil {
		return rsaPub{}, nil, err
	}
	eb, err := r.ReadZBytes()
	if err != nil {
		return rsaPub{}, nil, err
	}
	k := rsaPub{fromLE(nb), fromLE(eb)}
	if k.n.BitLen() < 64 || k.n.BitLen() > 8192 || k.e.Sign() <= 0 || k.e.BitLen() > 64 {
		return rsaPub{}, nil, errors.New("bad RSA public key")
	}
	return k, encodePubKey(k.n, k.e), nil
}

// encrypt is RSAES-PKCS1-v1_5.
func (k RSAPublicKey) encrypt(msg []byte) ([]byte, error) {
	size := (k.n.BitLen() + 7) / 8
	if len(msg) > size-11 {
		return nil, errors.New("message too long for RSA key")
	}
	em := make([]byte, size)
	em[1] = 2
	ps := em[2 : size-len(msg)-1]
	for i := range ps {
		for ps[i] == 0 {
			ps[i] = byte(rand.Uint32())
		}
	}
	copy(em[size-len(msg):], msg)
	c := new(big.Int).Exp(new(big.Int).SetBytes(em), k.e, k.n)
	return c.FillBytes(make([]byte, size)), nil
}

func (k *RSAKey) decrypt(ct []byte) ([]byte, error) {
	size := (k.n.BitLen() + 7) / 8
	c := new(big.Int).SetBytes(ct)
	if len(ct) != size || c.Cmp(k.n) >= 0 {
		return nil, errors.New("bad RSA ciphertext")
	}
	em := new(big.Int).Exp(c, k.d, k.n).FillBytes(make([]byte, size))
	if em[0] != 0 || em[1] != 2 {
		return nil, errors.New("bad RSA padding")
	}
	sep := bytes.IndexByte(em[2:], 0)
	if sep < 8 {
		return nil, errors.New("bad RSA padding")
	}
	return em[2+sep+1:], nil
}

// mlState is one handshake's multilink exchange.
type mlState struct {
	on        bool   // we are doing the exchange
	peer      rsaPub // the peer's claimed key
	peerKey   []byte // ... encoded
	challenge []byte // acceptor: the challenge we sent, encrypted
	expect    []byte // acceptor: the challenge in clear
	openSyn   []byte // initiator: the challenge, encrypted for the peer
}

func (t *Unicast) mlEnabled() bool { return t.cfg.MaxLinks > 1 && t.cfg.MultiLinkKey != nil }

func mlExt(b []byte) *protocol.Extension {
	return &protocol.Extension{ID: extMultiLink, Enc: protocol.ExtZBuf, Buf: b}
}

func (t *Unicast) mlInitSyn() *protocol.Extension {
	if !t.mlEnabled() {
		return nil
	}
	t.ml.on = true
	return mlExt(t.cfg.MultiLinkKey.pub)
}

func (t *Unicast) mlInitAck(e *protocol.Extension) error {
	if !t.ml.on {
		return nil
	}
	if e == nil || e.Enc != protocol.ExtZBuf {
		t.ml.on = false // the peer does not do multilink
		return nil
	}
	r := codec.NewReader(e.Buf)
	peer, enc, err := readPubKey(r)
	if err != nil {
		return fmt.Errorf("%w: %w: INIT ACK: %v", ErrHandshake, errMultiLink, err)
	}
	ct, err := r.ReadZBytes()
	if err != nil {
		return fmt.Errorf("%w: %w: INIT ACK: %v", ErrHandshake, errMultiLink, err)
	}
	nonce, err := t.cfg.MultiLinkKey.decrypt(ct)
	if err != nil {
		return fmt.Errorf("%w: %w: %v", ErrHandshake, errMultiLink, err)
	}
	if t.ml.openSyn, err = peer.encrypt(nonce); err != nil {
		return fmt.Errorf("%w: %w: %v", ErrHandshake, errMultiLink, err)
	}
	t.ml.peer, t.ml.peerKey = peer, enc
	return nil
}

func (t *Unicast) mlOpenSyn() *protocol.Extension {
	if !t.ml.on {
		return nil
	}
	w := codec.NewWriter()
	w.WriteZBytes(t.ml.openSyn)
	return mlExt(w.Bytes())
}

func (t *Unicast) mlOpenAck(e *protocol.Extension) {
	if t.ml.on && e != nil {
		t.peerMLKey = t.ml.peerKey
	}
}

func (t *Unicast) mlAcceptInitSyn(e *protocol.Extension) error {
	if !t.mlEnabled() || e == nil || e.Enc != protocol.ExtZBuf {
		return nil
	}
	peer, enc, err := readPubKey(codec.NewReader(e.Buf))
	if err != nil {
		return fmt.Errorf("%w: %w: INIT SYN: %v", ErrHandshake, errMultiLink, err)
	}
	t.ml.expect = binary.LittleEndian.AppendUint64(nil, rand.Uint64())
	if t.ml.challenge, err = peer.encrypt(t.ml.expect); err != nil {
		return fmt.Errorf("%w: %w: %v", ErrHandshake, errMultiLink, err)
	}
	t.ml.on, t.ml.peer, t.ml.peerKey = true, peer, enc
	return nil
}

func (t *Unicast) mlAcceptInitAck() *protocol.Extension {
	if !t.ml.on {
		return nil
	}
	w := codec.NewWriter()
	w.WriteRaw(t.cfg.MultiLinkKey.pub)
	w.WriteZBytes(t.ml.challenge)
	return mlExt(w.Bytes())
}

func (t *Unicast) mlAcceptOpenSyn(e *protocol.Extension) error {
	if !t.ml.on {
		return nil
	}
	if e == nil || e.Enc != protocol.ExtZBuf {
		t.ml.on = false
		return nil
	}
	ct, err := codec.NewReader(e.Buf).ReadZBytes()
	if err != nil {
		return fmt.Errorf("%w: %w: OPEN SYN: %v", ErrHandshake, errMultiLink, err)
	}
	got, err := t.cfg.MultiLinkKey.decrypt(ct)
	if err != nil || !bytes.Equal(got, t.ml.expect) {
		return fmt.Errorf("%w: %w: the peer did not prove its key", ErrHandshake, errMultiLink)
	}
	t.peerMLKey = t.ml.peerKey
	return nil
}

func (t *Unicast) mlAcceptOpenAck() *protocol.Extension {
	if t.peerMLKey == nil {
		return nil
	}
	return &protocol.Extension{ID: extMultiLink, Enc: protocol.ExtUnit}
}

// MultiLink reports whether the multilink exchange succeeded, i.e. whether
// further links from the peer may join this transport.
func (t *Unicast) MultiLink() bool { return t.peerMLKey != nil }
