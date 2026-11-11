//go:build tinygo && !tls13_ed25519

package tls13

const haveEd25519 = false

func ed25519Verify(pub, msg, sig []byte) bool { return false }

func ed25519Sign(priv, msg []byte) []byte { panic(errNoEd25519) }

func ed25519FromSeed(seed []byte) []byte { panic(errNoEd25519) }
