package zenoh

import (
	"testing"
	"time"

	"github.com/shabunin/tiny-zenoh-go/internal/leaktest"
)

// drainWithin reads ch until it is closed, failing after d; it returns how
// many values it read.
func drainWithin[T any](t *testing.T, ch <-chan T, d time.Duration) int {
	t.Helper()
	n := 0
	deadline := time.After(d)
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return n
			}
			n++
		case <-deadline:
			t.Fatalf("channel still open after %v (%d read)", d, n)
		}
	}
}

func TestFifoCloseDelivers(t *testing.T) {
	leaktest.Check(t)
	f := newFifo[int](nil)
	for i := range 5 {
		f.push(i)
	}
	f.close()
	if n := drainWithin(t, f.out, time.Second); n != 5 {
		t.Fatalf("read %d of 5 after close", n)
	}
}

// Nobody reads: abort (or the session ending) must still end the goroutine.
func TestFifoAbortUnread(t *testing.T) {
	leaktest.Check(t)
	f := newFifo[int](nil)
	for i := range 5 {
		f.push(i)
	}
	time.Sleep(10 * time.Millisecond) // run is blocked sending the first
	f.abort()
	f.push(9) // after abort: ignored, no panic
	if n := drainWithin(t, f.out, time.Second); n > 1 {
		t.Fatalf("read %d after abort, want at most the one in flight", n)
	}

	stop := make(chan struct{})
	g := newFifo[int](stop)
	g.push(1)
	close(stop)
	drainWithin(t, g.out, time.Second)
}

// An undeclared channel subscriber whose reader stopped reading releases
// its goroutine and closes the channel.
func TestChanSubscriberUndeclareUnread(t *testing.T) {
	s := openPeer(t)
	// After openPeer: the session's goroutines don't count, and the check
	// runs before its Close (which would end the subscriber's goroutine too).
	leaktest.Check(t)
	sub, ch, err := s.DeclareSubscriberChan("fifo/**")
	if err != nil {
		t.Fatal(err)
	}
	for i := range 10 {
		if err := s.Put("fifo/a", []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("no sample")
	}
	if err := sub.Undeclare(); err != nil {
		t.Fatal(err)
	}
	if n := drainWithin(t, ch, time.Second); n > 1 {
		t.Fatalf("read %d after Undeclare, want at most the one in flight", n)
	}
}
