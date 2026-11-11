//go:build !(linux || darwin || freebsd) || tinygo

package link

import (
	"errors"
	"net"
)

func listenMulticastGroup(ifi *net.Interface, group *net.UDPAddr) (*net.UDPConn, error) {
	return nil, errors.New("link: multicast groups are not supported on this platform")
}

func setMulticastOpts(*net.UDPConn, net.IP, int) error { return nil }
