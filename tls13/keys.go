package tls13

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/binary"
	"hash"
)

// Cipher suites.
const (
	TLS_AES_128_GCM_SHA256 uint16 = 0x1301
	TLS_AES_256_GCM_SHA384 uint16 = 0x1302
)

type suite struct {
	id     uint16
	keyLen int
	hash   func() hash.Hash
}

var suites = []suite{
	{TLS_AES_128_GCM_SHA256, 16, sha256.New},
	{TLS_AES_256_GCM_SHA384, 32, sha512.New384},
}

func suiteByID(id uint16) *suite {
	for i := range suites {
		if suites[i].id == id {
			return &suites[i]
		}
	}
	return nil
}

func (s *suite) size() int { return s.hash().Size() }

// HKDF (RFC 5869) on crypto/hmac.
func (s *suite) extract(salt, ikm []byte) []byte {
	if salt == nil {
		salt = make([]byte, s.size())
	}
	if ikm == nil {
		ikm = make([]byte, s.size())
	}
	m := hmac.New(s.hash, salt)
	m.Write(ikm)
	return m.Sum(nil)
}

func (s *suite) expand(prk, info []byte, n int) []byte {
	m := hmac.New(s.hash, prk)
	var out, t []byte
	for ctr := byte(1); len(out) < n; ctr++ {
		m.Reset()
		m.Write(t)
		m.Write(info)
		m.Write([]byte{ctr})
		t = m.Sum(nil)
		out = append(out, t...)
	}
	return out[:n]
}

// expandLabel is HKDF-Expand-Label (RFC 8446 7.1).
func (s *suite) expandLabel(secret []byte, label string, context []byte, n int) []byte {
	info := make([]byte, 0, 4+6+len(label)+len(context))
	info = binary.BigEndian.AppendUint16(info, uint16(n))
	info = append(info, byte(6+len(label)))
	info = append(info, "tls13 "...)
	info = append(info, label...)
	info = append(info, byte(len(context)))
	info = append(info, context...)
	return s.expand(secret, info, n)
}

func (s *suite) deriveSecret(secret []byte, label string, transcript hash.Hash) []byte {
	var sum []byte
	if transcript == nil {
		sum = s.hash().Sum(nil)
	} else {
		sum = transcript.Sum(nil)
	}
	return s.expandLabel(secret, label, sum, s.size())
}

func (s *suite) finished(base []byte, transcript hash.Hash) []byte {
	m := hmac.New(s.hash, s.expandLabel(base, "finished", nil, s.size()))
	m.Write(transcript.Sum(nil))
	return m.Sum(nil)
}

// halfConn is one direction's record protection.
type halfConn struct {
	suite  *suite
	secret []byte
	aead   cipher.AEAD
	iv     []byte
	seq    uint64
}

func (h *halfConn) setSecret(s *suite, secret []byte) {
	h.suite, h.secret, h.seq = s, secret, 0
	block, err := aes.NewCipher(s.expandLabel(secret, "key", nil, s.keyLen))
	if err != nil {
		panic(err)
	}
	if h.aead, err = cipher.NewGCM(block); err != nil {
		panic(err)
	}
	h.iv = s.expandLabel(secret, "iv", nil, 12)
}

// update moves to the next traffic secret (KeyUpdate).
func (h *halfConn) update() {
	h.setSecret(h.suite, h.suite.expandLabel(h.secret, "traffic upd", nil, h.suite.size()))
}

func (h *halfConn) nonce() []byte {
	var n [12]byte
	copy(n[:], h.iv)
	for i := 0; i < 8; i++ {
		n[4+i] ^= byte(h.seq >> (56 - 8*i))
	}
	h.seq++
	return n[:]
}
