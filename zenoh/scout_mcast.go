//go:build !tinygo

package zenoh

import "net"

func listenMulticast(ifi *net.Interface, group *net.UDPAddr) (*net.UDPConn, error) {
	return net.ListenMulticastUDP("udp4", ifi, group)
}
