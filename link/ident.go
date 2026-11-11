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

// Endpoints returns the local and remote locators of l, a link made with
// protocol proto, as far as it knows its addresses ("" otherwise).
func Endpoints(l any, proto string) (src, dst string) {
	if c, ok := l.(interface{ LocalAddr() net.Addr }); ok {
		if a := c.LocalAddr(); a != nil && a.String() != "" {
			src = proto + "/" + a.String()
		}
	}
	if c, ok := l.(interface{ RemoteAddr() net.Addr }); ok {
		if a := c.RemoteAddr(); a != nil && a.String() != "" {
			dst = proto + "/" + a.String()
		}
	}
	if c, ok := l.(interface{ Locators() (string, string) }); ok && src == "" && dst == "" {
		src, dst = c.Locators()
	}
	return src, dst
}
