//go:build linux && !tinygo

package zenoh

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// ptyPair connects two pseudo-terminals with socat, like a null-modem cable.
func ptyPair(t *testing.T) (string, string) {
	t.Helper()
	if _, err := exec.LookPath("socat"); err != nil {
		t.Skip("socat not installed")
	}
	dir := t.TempDir()
	a, b := filepath.Join(dir, "a"), filepath.Join(dir, "b")
	cmd := exec.Command("socat", "pty,raw,echo=0,link="+a, "pty,raw,echo=0,link="+b)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	for deadline := time.Now().Add(3 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		_, ea := os.Stat(a)
		_, eb := os.Stat(b)
		if ea == nil && eb == nil {
			return a, b
		}
		if time.Now().After(deadline) {
			t.Fatal("socat did not create the ptys")
		}
	}
}

func TestSerialGoRouterClient(t *testing.T) {
	pa, pb := ptyPair(t)
	r, err := Open(Config{
		Mode:     ModeRouter,
		Listen:   []string{"tcp/127.0.0.1:0", "serial/" + pa + "#baudrate=115200"},
		Scouting: ScoutingConfig{Disable: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	c, err := Open(Config{Connect: []string{"serial/" + pb + "#baudrate=115200"}})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	tcp, err := Open(Config{Connect: []string{r.Locators()[0]}})
	if err != nil {
		t.Fatal(err)
	}
	defer tcp.Close()

	pfx := uniq(t)
	sink := newSink()
	sub, _ := tcp.DeclareSubscriber(pfx+"/**", sink.handler)
	defer sub.Undeclare()
	q, _ := c.DeclareQueryable(pfx+"/q", func(q *Query) { _ = q.Reply(q.KeyExpr(), []byte("from-serial")) })
	defer q.Undeclare()
	settle()

	big := bytes.Repeat([]byte("0123456789abcdef"), 1280) // 20 KiB: fragmented over 1500-byte frames
	_ = c.Put(pfx+"/small", []byte("hi"))
	_ = c.Put(pfx+"/big", big)
	got := sink.wait(t, 2)
	if string(got[0].Payload) != "hi" || !bytes.Equal(got[1].Payload, big) {
		t.Errorf("got %q and %d bytes", got[0].Payload, len(got[1].Payload))
	}
	replies, _ := tcp.Get(pfx + "/q")
	n := 0
	for rep := range replies {
		if s, err := rep.Result(); err != nil || string(s.Payload) != "from-serial" {
			t.Errorf("reply %v %q", err, s.Payload)
		}
		n++
	}
	if n != 1 {
		t.Errorf("%d replies", n)
	}

	// The serial client goes away and a new one takes the port over.
	c.Close()
	c2, err := Open(Config{Connect: []string{"serial/" + pb + "#baudrate=115200"}})
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	settle()
	_ = c2.Put(pfx+"/again", []byte("back"))
	if got := sink.wait(t, 1); string(got[len(got)-1].Payload) != "back" {
		t.Errorf("got %q", got[len(got)-1].Payload)
	}
}
