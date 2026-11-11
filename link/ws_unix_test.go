package link

import (
	"bufio"
	"bytes"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/shabunin/gozenoh/transport"
)

func TestWebSocket(t *testing.T) {
	ln, err := Listen("ws/127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	echo(t, ln)
	c, err := Dial(ln.Locator().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, ok := c.(transport.PacketLink); !ok {
		t.Fatal("ws link is not a PacketLink")
	}
	roundTrip(t, c, []byte("a"), bytes.Repeat([]byte{1}, 200), bytes.Repeat([]byte{2}, 65535))
}

// TestWebSocketControlAndFragments: an unmasked server side receiving a
// fragmented message with a ping in the middle answers the ping and
// reassembles the message.
func TestWebSocketControlAndFragments(t *testing.T) {
	a, b := net.Pipe()
	srv := &wsConn{c: a, br: bufio.NewReader(a)}
	srv.hsOnce.Do(func() {}) // skip the HTTP upgrade
	cli := &wsConn{c: b, br: bufio.NewReader(b), client: true}
	cli.hsOnce.Do(func() {})
	go func() {
		// first fragment (binary, no FIN), ping, continuation with FIN
		_ = cli.writeRaw(0x02, []byte("hel"))
		_ = cli.writeFrame(wsPing, []byte("p"))
		_ = cli.writeRaw(0x80|wsCont, []byte("lo"))
	}()
	got := make(chan string, 1)
	go func() {
		buf := make([]byte, 16)
		n, err := srv.Read(buf)
		if err != nil {
			got <- err.Error()
			return
		}
		got <- string(buf[:n])
	}()
	// the client must see the pong
	m, err := cli.readControlForTest()
	if err != nil || m != wsPong {
		t.Fatalf("expected pong, got %#x %v", m, err)
	}
	if s := <-got; s != "hello" {
		t.Fatalf("reassembled %q", s)
	}
}

// writeRaw writes a masked client frame with an explicit first header byte.
func (w *wsConn) writeRaw(b0 byte, payload []byte) error {
	mask := [4]byte{1, 2, 3, 4}
	f := []byte{b0, 0x80 | byte(len(payload))}
	f = append(f, mask[:]...)
	for i, c := range payload {
		f = append(f, c^mask[i&3])
	}
	_, err := w.c.Write(f)
	return err
}

func (w *wsConn) readControlForTest() (byte, error) {
	var h [2]byte
	if _, err := w.br.Read(h[:1]); err != nil {
		return 0, err
	}
	if _, err := w.br.Read(h[1:]); err != nil {
		return 0, err
	}
	p := make([]byte, h[1]&0x7f)
	_, err := w.br.Read(p)
	return h[0] & 0x0f, err
}

func TestUnixSocket(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("no unix sockets")
	}
	dir, err := os.MkdirTemp("", "gzu")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "z.sock")
	loc := "unixsock-stream/" + path

	// a stale socket file from a dead process is replaced
	stale, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	stale.(*net.UnixListener).SetUnlinkOnClose(false)
	stale.Close()

	ln, err := Listen(loc)
	if err != nil {
		t.Fatal(err)
	}
	if ln.Locator().String() != loc {
		t.Errorf("locator %s", ln.Locator())
	}
	if _, err := Listen(loc); err == nil {
		t.Fatal("second listener on the same path succeeded")
	}
	echo(t, ln)
	c, err := Dial(loc, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	roundTrip(t, c, []byte("over a unix socket"))
	c.Close()
	ln.Close()
	for _, p := range []string{path, path + ".lock"} {
		if _, err := os.Stat(p); err == nil {
			t.Errorf("%s left behind", p)
		}
	}
}
