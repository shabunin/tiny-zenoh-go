package link

import (
	"errors"
	"net"
	"strconv"

	"github.com/shabunin/gozenoh/transport"
)

// Multicast links, "udp/<group>:<port>#iface=<name>;ttl=<n>" (zenoh-rust
// zenoh-link-udp multicast.rs): datagrams go to the group from a unicast
// socket bound to the interface's address, and are read from a socket that
// joined the group. The interface defaults to the first up, multicast-capable,
// non-loopback one with an IPv4 address, which is also zenoh-rust's choice;
// nodes on one host only hear each other if they pick the same interface.

// MulticastMTU is the largest datagram a multicast link sends (as zenoh-rust).
const MulticastMTU = 0xffff - 8 - 40

// ErrNotMulticast is returned by ListenMulticast for a non-multicast locator.
var ErrNotMulticast = errors.New("link: not a multicast locator")

// IsMulticast reports whether locator is a udp/ locator with a multicast
// group address.
func IsMulticast(locator string) bool {
	l, err := Parse(locator)
	if err != nil || l.Protocol != "udp" {
		return false
	}
	host, _, err := net.SplitHostPort(l.Address)
	if err != nil {
		return false
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsMulticast()
}

type mcastLink struct {
	send  *net.UDPConn
	recv  *net.UDPConn
	group *net.UDPAddr
	self  string // our sender address, to skip our own datagrams
}

var _ transport.MulticastLink = (*mcastLink)(nil)

// ListenMulticast joins the group of a multicast udp/ locator.
func ListenMulticast(locator string) (transport.MulticastLink, error) {
	l, err := Parse(locator)
	if err != nil {
		return nil, err
	}
	if !IsMulticast(locator) {
		return nil, ErrNotMulticast
	}
	group, err := net.ResolveUDPAddr("udp", l.Address)
	if err != nil {
		return nil, err
	}
	if group.IP.To4() == nil {
		return nil, errors.New("link: IPv6 multicast is not supported")
	}
	cfg := l.ConfigValues()
	ifi, src, err := multicastInterface(cfg["iface"])
	if err != nil {
		return nil, err
	}
	send, err := net.ListenUDP("udp4", &net.UDPAddr{IP: src})
	if err != nil {
		return nil, err
	}
	ttl := 1
	if v, ok := cfg["ttl"]; ok {
		if ttl, err = strconv.Atoi(v); err != nil || ttl < 0 || ttl > 255 {
			_ = send.Close()
			return nil, errors.New("link: bad multicast ttl " + strconv.Quote(v))
		}
	}
	if err := setMulticastOpts(send, src, ttl); err != nil {
		_ = send.Close()
		return nil, err
	}
	recv, err := listenMulticastGroup(ifi, group)
	if err != nil {
		_ = send.Close()
		return nil, err
	}
	_ = recv.SetReadBuffer(4 << 20)
	return &mcastLink{send: send, recv: recv, group: group, self: send.LocalAddr().String()}, nil
}

// multicastInterface finds the named interface (or the default one) and its
// IPv4 address.
func multicastInterface(name string) (*net.Interface, net.IP, error) {
	ifs, err := net.Interfaces()
	if err != nil {
		return nil, nil, err
	}
	for i := range ifs {
		ifi := &ifs[i]
		if name != "" && name != "auto" {
			if ifi.Name != name {
				continue
			}
		} else if ifi.Flags&net.FlagUp == 0 || ifi.Flags&net.FlagMulticast == 0 || ifi.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, err := ifi.Addrs()
		if err != nil {
			continue
		}
		for _, a := range addrs {
			if ipn, ok := a.(*net.IPNet); ok && ipn.IP.To4() != nil {
				return ifi, ipn.IP.To4(), nil
			}
		}
	}
	if name != "" && name != "auto" {
		return nil, nil, errors.New("link: no IPv4 address on interface " + name)
	}
	return nil, nil, errors.New("link: no multicast interface")
}

func (m *mcastLink) ReadFrom(b []byte) (int, string, error) {
	for {
		n, src, err := m.recv.ReadFromUDP(b)
		if err != nil {
			return 0, "", err
		}
		if s := src.String(); s != m.self {
			return n, s, nil
		}
	}
}

func (m *mcastLink) Write(b []byte) (int, error) { return m.send.WriteToUDP(b, m.group) }

func (m *mcastLink) Close() error {
	err := m.send.Close()
	if e := m.recv.Close(); err == nil {
		err = e
	}
	return err
}

func (m *mcastLink) MTU() int { return MulticastMTU }

// LocalAddr is the address our datagrams come from.
func (m *mcastLink) LocalAddr() string { return m.self }

// DefaultMulticastInterface names the interface ListenMulticast uses when the
// locator does not say.
func DefaultMulticastInterface() (string, error) {
	ifi, _, err := multicastInterface("")
	if err != nil {
		return "", err
	}
	return ifi.Name, nil
}
