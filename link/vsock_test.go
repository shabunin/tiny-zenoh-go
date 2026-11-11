package link

import (
	"strconv"
	"testing"
	"time"
)

func TestParseVsock(t *testing.T) {
	for _, c := range []struct {
		in        string
		cid, port uint32
		bad       bool
	}{
		{in: "3:1234", cid: 3, port: 1234},
		{in: "VMADDR_CID_ANY:VMADDR_PORT_ANY", cid: vmaddrAny, port: vmaddrAny},
		{in: "-1:-1", cid: vmaddrAny, port: vmaddrAny},
		{in: "vmaddr_cid_local:7", cid: 1, port: 7},
		{in: "VMADDR_CID_HOST:7", cid: 2, port: 7},
		{in: "VMADDR_CID_HYPERVISOR:7", cid: 0, port: 7},
		{in: "3", bad: true},
		{in: "x:1", bad: true},
		{in: "1:2:3", bad: true},
	} {
		cid, port, err := parseVsock(c.in)
		if (err != nil) != c.bad || (!c.bad && (cid != c.cid || port != c.port)) {
			t.Errorf("%s: %d %d %v", c.in, cid, port, err)
		}
	}
}

// TestVsockLoopback needs AF_VSOCK with the vsock_loopback module (CID 1).
func TestVsockLoopback(t *testing.T) {
	ln, err := Listen("vsock/VMADDR_CID_ANY:VMADDR_PORT_ANY")
	if err != nil {
		t.Skip(err)
	}
	defer ln.Close()
	_, port, _ := parseVsock(ln.Locator().Address)
	if port == vmaddrAny {
		t.Fatalf("listener locator %s", ln.Locator())
	}
	echo(t, ln)
	c, err := Dial("vsock/VMADDR_CID_LOCAL:"+strconv.FormatUint(uint64(port), 10), time.Second)
	if err != nil {
		t.Skip(err) // no vsock_loopback
	}
	defer c.Close()
	roundTrip(t, c, []byte("over vsock"), make([]byte, 30000))
	if _, err := Dial("vsock/1:1", time.Second); err == nil {
		t.Error("dial to a closed vsock port succeeded")
	}
}
