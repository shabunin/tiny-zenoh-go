//go:build tinygo

package zenoh

import "net"

// TinyGo's net package cannot join multicast groups, so a TinyGo peer does
// not answer SCOUTs; it still sends them and connects to whoever answers.
func listenMulticast(*net.Interface, *net.UDPAddr) (*net.UDPConn, error) {
	return nil, errNoMulticastListen
}
