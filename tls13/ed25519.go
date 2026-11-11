//go:build !tinygo || tls13_ed25519

package tls13

import "crypto/ed25519"

// Ed25519 costs ~47 KB of flash and ~78 KB of RAM (precomputed tables) in
// TinyGo builds: there it is left out unless built with tls13_ed25519.
const haveEd25519 = true

func ed25519Verify(pub, msg, sig []byte) bool {
	return ed25519.Verify(ed25519.PublicKey(pub), msg, sig)
}

func ed25519Sign(priv, msg []byte) []byte { return ed25519.Sign(ed25519.PrivateKey(priv), msg) }

func ed25519FromSeed(seed []byte) []byte { return ed25519.NewKeyFromSeed(seed) }
