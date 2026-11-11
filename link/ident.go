package link

import "net"

// Identity of a link's remote end and local side, as used by access control
// subjects (zenoh-rust: cert_common_names, interfaces).

// PeerCommonName returns the common name of the certificate the remote end
// of l presented (TLS, or any link with a PeerCommonName method), or "".
func PeerCommonName(l any) string {
	if p, ok := l.(interface{ PeerCommonName() string }); ok {
		return p.PeerCommonName()
	}
	return tlsCommonName(l)
}

// LocalInterfaces returns the names of the network interfaces holding the
// local address of l, if it has one.
func LocalInterfaces(l any) []string {
	c, ok := l.(interface{ LocalAddr() net.Addr })
	if !ok {
		return nil
	}
	var ip net.IP
	switch a := c.LocalAddr().(type) {
	case *net.TCPAddr:
		ip = a.IP
	case *net.UDPAddr:
		ip = a.IP
	default:
		return nil
	}
	ifs, err := net.Interfaces()
	if err != nil {
		return nil
	}
	var out []string
	for _, ifc := range ifs {
		addrs, err := ifc.Addrs()
		if err != nil {
			continue
		}
		for _, a := range addrs {
			if n, ok := a.(*net.IPNet); ok && n.IP.Equal(ip) {
				out = append(out, ifc.Name)
				break
			}
		}
	}
	return out
}
