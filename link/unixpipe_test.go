//go:build unix && !tinygo

package link

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestUnixPipe(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "zp")
	loc := "unixpipe/" + path

	if _, err := Dial(loc, time.Second); err == nil {
		t.Fatal("dial without a listener succeeded")
	}
	// A stale uplink FIFO with nobody holding it is not a listener either.
	if err := syscall.Mkfifo(path+"_uplink", 0o600); err != nil {
		t.Fatal(err)
	}
	stale, _ := os.OpenFile(path+"_uplink", os.O_RDWR, 0)
	if _, err := Dial(loc, time.Second); err == nil {
		t.Fatal("dial to a stale FIFO succeeded")
	}
	stale.Close()

	ln, err := Listen(loc + "#file_mask=384")
	if err != nil {
		t.Fatal(err)
	}
	if ln.Locator().String() != loc {
		t.Errorf("locator %s", ln.Locator())
	}
	if _, err := Listen(loc); err == nil {
		t.Fatal("second listener on the same path succeeded")
	}
	if fi, err := os.Stat(path + "_uplink"); err != nil || fi.Mode()&os.ModeNamedPipe == 0 {
		t.Fatalf("uplink FIFO: %v %v", fi, err)
	}
	for range 2 { // two links over one listener
		echo(t, ln)
		c, err := Dial(loc, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		roundTrip(t, c, []byte("over a named pipe"), bytes.Repeat([]byte{7}, 4000))
		c.Close()
	}
	ln.Close()
	// The dialer's FIFOs and the listener's go with them; the accepted side
	// never sees EOF on a FIFO (its session lease would close it).
	m, _ := filepath.Glob(path + "_downlink*")
	if _, err := os.Stat(path + "_uplink"); err == nil || len(m) != 0 {
		t.Errorf("left behind: uplink %v, downlinks %v", err, m)
	}
}

// TestUnixPipeWire plays zenoh-rust's connector by hand against our
// listener: invitation, confirmation on the dedicated downlink, final
// confirmation on the dedicated uplink.
func TestUnixPipeWire(t *testing.T) {
	path := filepath.Join(t.TempDir(), "zp")
	ln, err := Listen("unixpipe/" + path)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	accepted := make(chan error, 1)
	go func() {
		c, err := ln.Accept()
		if err == nil {
			_, err = c.Write([]byte("hi"))
		}
		accepted <- err
	}()
	const suffix = 12345
	down, up := pipeNames(path, suffix)
	if down != path+"_downlink12345" || up != path+"_uplink12345" {
		t.Fatalf("names %s %s", down, up)
	}
	r, err := openPipeR(down, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	req, err := os.OpenFile(path+"_uplink", os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	inv := []byte{0xDE, 0xAD, 0xBE, 0xEF, 0, 0, 0, 0}
	binary.NativeEndian.PutUint32(inv[4:], suffix)
	_, _ = req.Write(inv)
	req.Close()
	if err := pipeExpect(r, suffix, time.Now().Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	w, err := os.OpenFile(up, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	_, _ = w.Write(inv)
	if err := <-accepted; err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 2)
	_ = r.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := r.Read(buf); err != nil || string(buf) != "hi" {
		t.Fatalf("read %q %v", buf, err)
	}
}
