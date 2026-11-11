//go:build (linux || darwin || freebsd) && !tinygo

package link

import (
	"net"
	"syscall"
)

func listenMulticastGroup(ifi *net.Interface, group *net.UDPAddr) (*net.UDPConn, error) {
	return net.ListenMulticastUDP("udp4", ifi, group)
}

// setMulticastOpts picks the outgoing interface and TTL of the send socket;
// the network stack loops our datagrams back to local group members.
func setMulticastOpts(c *net.UDPConn, src net.IP, ttl int) error {
	rc, err := c.SyscallConn()
	if err != nil {
		return err
	}
	var serr error
	err = rc.Control(func(fd uintptr) {
		var a [4]byte
		copy(a[:], src.To4())
		if serr = syscall.SetsockoptInet4Addr(int(fd), syscall.IPPROTO_IP, syscall.IP_MULTICAST_IF, a); serr != nil {
			return
		}
		if serr = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_IP, syscall.IP_MULTICAST_TTL, ttl); serr != nil {
			return
		}
		serr = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_IP, syscall.IP_MULTICAST_LOOP, 1)
	})
	if err != nil {
		return err
	}
	return serr
}
