//go:build unix

package main

import (
	"bufio"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"testing"
	"time"
)

// builtTZD is the tzd binary, built once per test binary.
var builtTZD = struct {
	path string
	err  error
}{}

func init() {
	teardown = append(teardown, func() {
		if builtTZD.path != "" {
			_ = os.RemoveAll(filepath.Dir(builtTZD.path))
		}
	})
}

func tzdBinary(t *testing.T) string {
	t.Helper()
	if testing.Short() {
		t.Skip("builds the binary")
	}
	if builtTZD.path == "" && builtTZD.err == nil {
		dir, err := os.MkdirTemp("", "tzd-bin")
		if err != nil {
			t.Fatal(err)
		}
		builtTZD.path = filepath.Join(dir, "tzd")
		out, err := exec.Command("go", "build", "-o", builtTZD.path, ".").CombinedOutput()
		if err != nil {
			builtTZD.err = errors.New(string(out))
		}
	}
	if builtTZD.err != nil {
		t.Fatalf("go build: %v", builtTZD.err)
	}
	return builtTZD.path
}

// proc is a tzd process; stderr is collected in the background.
type proc struct {
	cmd    *exec.Cmd
	stderr *syncBuf
	exited chan error
}

func startBinary(t *testing.T, args ...string) (*proc, string) {
	t.Helper()
	cmd := exec.Command(tzdBinary(t), args...)
	cmd.Env = append(os.Environ(), "TMPDIR="+t.TempDir()) // SIGUSR2's heap profile
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	p := &proc{cmd: cmd, stderr: &syncBuf{}, exited: make(chan error, 1)}
	cmd.Stderr = p.stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	lines := make(chan string, 16)
	go func() {
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			lines <- sc.Text()
		}
		close(lines)
		p.exited <- cmd.Wait()
	}()
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		for range lines {
		}
	})
	select {
	case l := <-lines:
		if !strings.Contains(l, " listening on ") {
			t.Fatalf("first line %q", l)
		}
		go func() {
			for range lines {
			}
		}()
		return p, l
	case <-time.After(10 * time.Second):
		t.Fatalf("no listening line; stderr:\n%s", p.stderr.String())
	}
	return nil, ""
}

func (p *proc) waitStderr(t *testing.T, re string) string {
	t.Helper()
	rx := regexp.MustCompile(re)
	deadline := time.Now().Add(5 * time.Second)
	for {
		if m := rx.FindString(p.stderr.String()); m != "" {
			return m
		}
		if time.Now().After(deadline) {
			t.Fatalf("stderr lacks %s:\n%s", re, p.stderr.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (p *proc) waitExit(t *testing.T) int {
	t.Helper()
	select {
	case err := <-p.exited:
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			return ee.ExitCode()
		}
		if err != nil {
			t.Fatal(err)
		}
		return 0
	case <-time.After(10 * time.Second):
		t.Fatal("tzd did not exit")
		return -1
	}
}

// The real binary: the debug signals, SIGTERM and SIGINT stop it cleanly,
// exit statuses.
func TestBinarySignals(t *testing.T) {
	p, _ := startBinary(t, args("--cfg", `plugins/storage_manager/storages/s:{key_expr:"bin/**",volume:"memory"}`)...)
	_ = p.cmd.Process.Signal(syscall.SIGUSR1)
	p.waitStderr(t, `goroutine \d+ \[`)
	_ = p.cmd.Process.Signal(syscall.SIGUSR2)
	m := p.waitStderr(t, `mem: heap profile in \S+`)
	if fi, err := os.Stat(strings.TrimPrefix(m, "mem: heap profile in ")); err != nil || fi.Size() == 0 {
		t.Fatalf("heap profile: %v", err)
	}
	p.waitStderr(t, `mem: sys [0-9.]+ MiB; .* goroutines \d+`)
	_ = p.cmd.Process.Signal(syscall.SIGTERM)
	if code := p.waitExit(t); code != 0 {
		t.Fatalf("SIGTERM: exit %d; stderr:\n%s", code, p.stderr.String())
	}

	q, _ := startBinary(t, args()...)
	_ = q.cmd.Process.Signal(syscall.SIGINT)
	if code := q.waitExit(t); code != 0 {
		t.Fatalf("SIGINT: exit %d", code)
	}

	for _, c := range []struct {
		args []string
		code int
	}{{[]string{"--version"}, 0}, {[]string{"--bogus"}, 2}, {[]string{"-l", "nope/1", "--no-multicast-scouting"}, 1}} {
		err := exec.Command(tzdBinary(t), c.args...).Run()
		code := 0
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			code = ee.ExitCode()
		} else if err != nil {
			t.Fatal(err)
		}
		if code != c.code {
			t.Errorf("tzd %v: exit %d, want %d", c.args, code, c.code)
		}
	}
}
