// Package leaktest checks that tests leave no goroutines of this module
// running: sessions, transports, links and plugins must stop everything
// they started once closed.
//
// A package opts in with
//
//	func TestMain(m *testing.M) { leaktest.Main(m) }
//
// and a single test can check itself with Check(t) at its start.
package leaktest

import (
	"fmt"
	"os"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"
)

// Wait is how long goroutines get to finish after the tests (closes are
// partly asynchronous: lingering closes, redial loops noticing shutdown).
var Wait = 10 * time.Second

// ours marks goroutines worth reporting: running or started by code of this
// module or the libraries it drives.
var ours = []string{
	"github.com/shabunin/tiny-zenoh-go/",
	"github.com/quic-go/quic-go",
}

// Main runs the tests, then the teardown functions (for state shared by
// the tests, such as a router opened once), and then fails the binary if
// goroutines of this module are still running after Wait. LEAKTEST=off
// skips the check; LEAKTEST_WAIT (a duration) overrides Wait.
func Main(m *testing.M, teardown ...func()) {
	code := m.Run()
	for _, f := range teardown {
		f()
	}
	if d, err := time.ParseDuration(os.Getenv("LEAKTEST_WAIT")); err == nil {
		Wait = d
	}
	if code == 0 && os.Getenv("LEAKTEST") != "off" {
		if left := settle(nil, Wait); len(left) > 0 {
			fmt.Fprintf(os.Stderr, "leaktest: %d goroutine(s) still running after the tests:\n\n%s\n",
				len(left), strings.Join(left, "\n\n"))
			code = 1
		}
	}
	os.Exit(code)
}

// Check records the goroutines running now and, when t ends, fails it if
// new goroutines of this module are still running after Wait.
func Check(t testing.TB) {
	t.Helper()
	before := map[string]bool{}
	for _, g := range goroutines() {
		before[id(g)] = true
	}
	t.Cleanup(func() {
		if left := settle(before, Wait); len(left) > 0 {
			t.Errorf("%d goroutine(s) still running after the test:\n\n%s", len(left), strings.Join(left, "\n\n"))
		}
	})
}

// settle waits up to d for the module's goroutines (other than those in
// skip) to finish and returns the stacks of those still running.
func settle(skip map[string]bool, d time.Duration) []string {
	deadline := time.Now().Add(d)
	for wait := time.Millisecond; ; wait = min(2*wait, 100*time.Millisecond) {
		var left []string
		for _, g := range goroutines() {
			if !skip[id(g)] && interesting(g) {
				left = append(left, g)
			}
		}
		if len(left) == 0 || time.Now().After(deadline) {
			sort.Strings(left)
			return left
		}
		time.Sleep(wait)
	}
}

func goroutines() []string {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}
	all := strings.Split(string(buf), "\n\n")
	// The first one is ours (the caller).
	if len(all) > 0 {
		all = all[1:]
	}
	return all
}

// id is a goroutine's "goroutine N" header word pair.
func id(g string) string {
	f := strings.Fields(g)
	if len(f) < 2 {
		return g
	}
	return f[0] + " " + f[1]
}

func interesting(g string) bool {
	if strings.Contains(g, "/internal/leaktest.") {
		return false
	}
	for _, s := range ours {
		if strings.Contains(g, s) {
			return true
		}
	}
	return false
}
