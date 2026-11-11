package zenoh

import (
	"strconv"
	"testing"
)

// faceQoS reports whether s's (single) face negotiated the QoS extension.
func faceQoS(t *testing.T, s *Session) bool {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, f := range s.faces {
		<-f.ready
		if f.tr != nil {
			return f.tr.QoS()
		}
	}
	t.Fatal("no face")
	return false
}

func TestQoSRustPriorities(t *testing.T) {
	pfx := uniq(t)
	py := startPeer(t, true, "sub", pfx+"/**", "3")
	s := openSession(t)
	if !faceQoS(t, s) {
		t.Error("QoS not negotiated with the router")
	}
	prios := []Priority{PriorityRealTime, PriorityDataLow, PriorityBackground}
	for _, p := range prios {
		if err := s.Put(pfx+"/go", []byte("x"), WithPriority(p)); err != nil {
			t.Fatal(err)
		}
	}
	for _, p := range prios {
		if e := py.next(); e.Priority != int(p) {
			t.Errorf("rust got priority %d, want %d (%+v)", e.Priority, p, e)
		}
	}

	sink := newSink()
	sub, _ := s.DeclareSubscriber(pfx+"/rust", sink.handler)
	defer sub.Undeclare()
	settle()
	startPeer(t, false, "prio", pfx+"/rust", "REAL_TIME", "INTERACTIVE_HIGH", "DATA_HIGH", "BACKGROUND")
	for _, smp := range sink.wait(t, 4) {
		if want, _ := strconv.Atoi(string(smp.Payload)); int(smp.Priority) != want {
			t.Errorf("go got priority %d, want %d", smp.Priority, want)
		}
	}
}

func TestQoSDisabled(t *testing.T) {
	pfx := uniq(t)
	py := startPeer(t, true, "sub", pfx, "1")
	s, err := Open(Config{Connect: []string{routerLocator(t)}, DisableQoS: true})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if faceQoS(t, s) {
		t.Error("QoS negotiated although disabled")
	}
	// The priority still travels in the message's own QoS extension.
	if err := s.Put(pfx, []byte("x"), WithPriority(PriorityInteractiveLow)); err != nil {
		t.Fatal(err)
	}
	if e := py.next(); e.Priority != int(PriorityInteractiveLow) {
		t.Errorf("rust got priority %d", e.Priority)
	}
}
