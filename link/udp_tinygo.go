//go:build tinygo

package link

// TinyGo's ReadFromUDP doesn't report source addresses, which a UDP listener
// needs to tell its remote endpoints apart. Dialing UDP works.
const udpCanListen = false
