package link

import (
	"fmt"
	"strconv"
	"strings"
)

// vsock address names (linux/vm_sockets.h).
const (
	vmaddrCIDHypervisor = 0
	vmaddrCIDLocal      = 1
	vmaddrCIDHost       = 2
	vmaddrAny           = 0xFFFFFFFF // VMADDR_CID_ANY, VMADDR_PORT_ANY
)

// parseVsock parses "<cid>:<port>" as zenoh-rust's get_vsock_addr does.
func parseVsock(addr string) (cid, port uint32, err error) {
	c, p, ok := strings.Cut(addr, ":")
	if !ok || strings.Contains(p, ":") {
		return 0, 0, fmt.Errorf("link: incorrect vsock address %q", addr)
	}
	switch strings.ToUpper(c) {
	case "VMADDR_CID_HYPERVISOR":
		cid = vmaddrCIDHypervisor
	case "VMADDR_CID_HOST":
		cid = vmaddrCIDHost
	case "VMADDR_CID_LOCAL":
		cid = vmaddrCIDLocal
	case "VMADDR_CID_ANY", "-1":
		cid = vmaddrAny
	default:
		n, err := strconv.ParseUint(c, 10, 32)
		if err != nil {
			return 0, 0, fmt.Errorf("link: incorrect vsock cid %q", c)
		}
		cid = uint32(n)
	}
	switch strings.ToUpper(p) {
	case "VMADDR_PORT_ANY", "-1":
		port = vmaddrAny
	default:
		n, err := strconv.ParseUint(p, 10, 32)
		if err != nil {
			return 0, 0, fmt.Errorf("link: incorrect vsock port %q", p)
		}
		port = uint32(n)
	}
	return cid, port, nil
}
