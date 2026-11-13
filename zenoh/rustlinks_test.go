//go:build linux && !tinygo

package zenoh

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/shabunin/tiny-zenoh-go/link"
)

// Interop over links the zenohd image and the Python binding lack
// (unixpipe, vsock) needs a zenoh-rust peer built with them: ZENOH_RS is
// the zrs binary built in zenoh/testdata/zrs (cargo build --release).

type rsProc struct {
	t     *testing.T
	cmd   *exec.Cmd
	lines chan map[string]any
}

func startRS(t *testing.T, cfg, role, key string, extra ...string) *rsProc {
	t.Helper()
	bin := os.Getenv("ZENOH_RS")
	if bin == "" {
		t.Skip("ZENOH_RS not set (zenoh-rust test peer from zenoh/testdata/zrs)")
	}
	cmd := exec.Command(bin, append([]string{cfg, role, key}, extra...)...)
	cmd.Stderr = os.Stderr
	out, _ := cmd.StdoutPipe()
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	p := &rsProc{t: t, cmd: cmd, lines: make(chan map[string]any, 256)}
	go func() {
		sc := bufio.NewScanner(out)
		for sc.Scan() {
			var m map[string]any
			if json.Unmarshal(sc.Bytes(), &m) == nil {
				p.lines <- m
			}
		}
		close(p.lines)
	}()
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	p.want("ready")
	return p
}

// want waits for a line carrying field f.
func (p *rsProc) want(f string) map[string]any {
	p.t.Helper()
	deadline := time.After(10 * time.Second)
	for {
		select {
		case m, ok := <-p.lines:
			if !ok {
				p.t.Fatalf("zrs exited waiting for %q", f)
			}
			if _, has := m[f]; has {
				return m
			}
		case <-deadline:
			p.t.Fatalf("zrs: timeout waiting for %q", f)
		}
	}
}

func rustLinkLocators(t *testing.T) map[string]func() string {
	if os.Getenv("ZENOH_RS") == "" {
		t.Skip("ZENOH_RS not set (zenoh-rust test peer from zenoh/testdata/zrs)")
	}
	return map[string]func() string{
		"unixpipe": func() string { return "unixpipe/" + filepath.Join(t.TempDir(), "zp") },
		"vsock": func() string {
			if _, err := os.Stat("/dev/vsock"); err != nil {
				t.Skip("no /dev/vsock")
			}
			loc := fmt.Sprintf("vsock/VMADDR_CID_LOCAL:%d", 20000+rand.IntN(30000))
			// /dev/vsock alone doesn't mean local vsock works: that needs
			// the vsock_loopback module (CI runners lack it).
			ln, err := link.Listen(loc)
			if err != nil {
				t.Skipf("vsock loopback unavailable (modprobe vsock_loopback): %v", err)
			}
			_ = ln.Close()
			return loc
		},
	}
}

// TestRustLinkRustListens: a Rust peer listens, a Go peer dials it.
func TestRustLinkRustListens(t *testing.T) {
	for name, loc := range rustLinkLocators(t) {
		t.Run(name, func(t *testing.T) {
			l := loc()
			cfg := fmt.Sprintf(`{mode:"peer",listen:{endpoints:[%q]},scouting:{multicast:{enabled:false}}}`, l)
			sub := startRS(t, cfg, "sub", "rl/**")
			_ = sub
			s, err := Open(Config{Mode: ModePeer, Connect: []string{l}, Listen: []string{},
				Scouting: ScoutingConfig{Disable: true}})
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			time.Sleep(300 * time.Millisecond)
			if err := s.Put("rl/go", []byte("hello "+name)); err != nil {
				t.Fatal(err)
			}
			if m := sub.want("sub"); m["sub"] != "rl/go" || m["payload"] != "hello "+name {
				t.Fatalf("Rust sub got %v", m)
			}
		})
	}
}

// TestRustLinkGoListens: a Go router listens; a Rust client publishes to
// a Go subscriber and queries a Go queryable over the link, and a second
// Rust client serves a queryable to Go.
func TestRustLinkGoListens(t *testing.T) {
	for name, loc := range rustLinkLocators(t) {
		t.Run(name, func(t *testing.T) {
			r, err := Open(Config{Mode: ModeRouter, Listen: []string{loc()}, Scouting: ScoutingConfig{Disable: true}})
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			l := r.Locators()[0]
			got := make(chan Sample, 16)
			sub, err := r.DeclareSubscriber("rl/**", func(s Sample) { got <- s })
			if err != nil {
				t.Fatal(err)
			}
			defer sub.Undeclare()
			q, err := r.DeclareQueryable("rl/go/q", func(q *Query) {
				_ = q.Reply("rl/go/q", []byte("go over "+name))
			})
			if err != nil {
				t.Fatal(err)
			}
			defer q.Undeclare()

			cfg := fmt.Sprintf(`{mode:"client",connect:{endpoints:[%q]},scouting:{multicast:{enabled:false}}}`, l)
			startRS(t, cfg, "put", "rl/rust", "from rust")
			select {
			case s := <-got:
				if s.KeyExpr != "rl/rust" || string(s.Payload) != "from rust" {
					t.Fatalf("Go sub got %s %q", s.KeyExpr, s.Payload)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("no sample from the Rust client")
			}

			get := startRS(t, cfg, "get", "rl/go/q")
			if m := get.want("reply"); m["reply"] != "rl/go/q" || m["payload"] != "go over "+name {
				t.Fatalf("Rust get got %v", m)
			}

			startRS(t, cfg, "qbl", "rl/rs/q")
			time.Sleep(300 * time.Millisecond)
			replies, err := r.Get("rl/rs/q", WithPayload([]byte("x"), EncodingTextPlain))
			if err != nil {
				t.Fatal(err)
			}
			n := 0
			for rep := range replies {
				if rep.Sample == nil || string(rep.Sample.Payload) != "rust:x" {
					t.Fatalf("reply %+v", rep)
				}
				n++
			}
			if n != 1 {
				t.Fatalf("%d replies from the Rust queryable", n)
			}
		})
	}
}
