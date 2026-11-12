package zenoh

import (
	"sync/atomic"
	"testing"
	"time"
)

// TestClientReconnects: a client whose router goes away connects again when
// one is back on its endpoint, and its subscriber, liveliness token and
// publisher work through the new router (seen in the lab: clients of a
// router cut off for longer than the lease stayed disconnected for good).
func TestClientReconnects(t *testing.T) {
	r1 := openRouter(t)
	loc := r1.Locators()[0]
	c := clientAt(t, loc)
	var got atomic.Int32
	if _, err := c.DeclareSubscriber("rc/to-c", func(Sample) { got.Add(1) }); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Liveliness().DeclareToken("rc/alive/c"); err != nil {
		t.Fatal(err)
	}
	pub, err := c.DeclarePublisher("rc/from-c")
	if err != nil {
		t.Fatal(err)
	}

	_ = r1.Close()
	time.Sleep(300 * time.Millisecond) // the client notices
	r2, err := Open(Config{Mode: ModeRouter, Listen: []string{loc}, Scouting: ScoutingConfig{Disable: true}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r2.Close() })
	d := clientAt(t, loc)
	var heard atomic.Int32
	if _, err := d.DeclareSubscriber("rc/from-c", func(Sample) { heard.Add(1) }); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(8 * time.Second)
	wait := func(what string, cond func() bool) {
		t.Helper()
		for !cond() {
			if time.Now().After(deadline) {
				t.Fatalf("timeout waiting for %s", what)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	wait("the client's subscriber through the new router", func() bool {
		_ = d.Put("rc/to-c", []byte("x"))
		return got.Load() > 0
	})
	wait("the client's publisher through the new router", func() bool {
		_ = pub.Put([]byte("y"))
		return heard.Load() > 0
	})
	wait("the client's token through the new router", func() bool {
		replies, err := d.Liveliness().Get("rc/alive/**", WithTimeout(time.Second))
		if err != nil {
			return false
		}
		n := 0
		for range replies {
			n++
		}
		return n > 0
	})
}
