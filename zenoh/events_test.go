package zenoh

import (
	"encoding/json"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"
)

// evRec records the events a listener heard.
type evRec struct {
	mu    sync.Mutex
	evs   []string // "T+ <zid>", "T- <zid>", "L+ <zid> <src> <dst>", ...
	ts    []TransportEvent
	links []LinkEvent
}

func (r *evRec) onT(ev TransportEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.evs = append(r.evs, map[SampleKind]string{SampleKindPut: "T+ ", SampleKindDelete: "T- "}[ev.Kind]+ev.Transport.ZID.String())
	r.ts = append(r.ts, ev)
}

func (r *evRec) onL(ev LinkEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.evs = append(r.evs, map[SampleKind]string{SampleKindPut: "L+ ", SampleKindDelete: "L- "}[ev.Kind]+ev.Link.ZID.String())
	r.links = append(r.links, ev)
}

func (r *evRec) get() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.evs...)
}

func (r *evRec) waitN(t *testing.T, n int) []string {
	t.Helper()
	waitFor(t, fmt.Sprintf("%d events", n), func() bool { return len(r.get()) >= n })
	time.Sleep(50 * time.Millisecond) // nothing more should come
	return r.get()
}

// listen declares a transport and a link listener recording into one evRec.
func listenEvents(t *testing.T, s *Session, opts ...EventsOption) *evRec {
	t.Helper()
	r := &evRec{}
	lt, err := s.DeclareTransportEventsListener(r.onT, opts...)
	if err != nil {
		t.Fatal(err)
	}
	ll, err := s.DeclareLinkEventsListener(r.onL, opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lt.Undeclare(); _ = ll.Undeclare() })
	return r
}

func evClient(t *testing.T, loc string) *Session {
	t.Helper()
	c, err := Open(Config{Connect: []string{loc}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestTransportEvents(t *testing.T) {
	r := openRouter(t)
	ev := listenEvents(t, r)
	a := evClient(t, r.Locators()[0])
	az := a.ZID().String()

	got := ev.waitN(t, 2)
	if want := []string{"T+ " + az, "L+ " + az}; !slices.Equal(got, want) {
		t.Fatalf("open: got %q, want %q", got, want)
	}
	ti := ev.ts[0].Transport
	if ti.WhatAmI != WhatAmIClient || ti.Proto != "tcp" || !ti.QoS || ti.Multicast || ti.Links != 1 {
		t.Errorf("transport %+v", ti)
	}
	k := ev.links[0].Link
	if k.Src != r.Locators()[0] || k.Dst == "" || k.MTU != 65480 || !k.Streamed || k.Reliability != Reliable ||
		k.Priorities == nil || *k.Priorities != (PriorityRange{0, 7}) || k.Group != "" || k.Multicast {
		t.Errorf("link %+v", k)
	}
	if links := r.Links(); len(links) != 1 || links[0].Src != k.Src || links[0].Dst != k.Dst {
		t.Errorf("Links() = %+v", links)
	}
	// The client sees the router, its link the other way round.
	cl := a.Links()
	if len(cl) != 1 || cl[0].Src != k.Dst || cl[0].Dst != k.Src || !cl[0].ZID.Equal(r.ZID()) {
		t.Errorf("client Links() = %+v", cl)
	}

	// A late listener with history hears what is up.
	late := listenEvents(t, r, WithHistory(true))
	if got := late.get(); !slices.Equal(got, []string{"T+ " + az, "L+ " + az}) {
		t.Errorf("history: %q", got)
	}
	none := listenEvents(t, r)
	if got := none.get(); len(got) != 0 {
		t.Errorf("no history asked, got %q", got)
	}

	// Closing: the link goes before the transport.
	cev := listenEvents(t, a)
	_ = a.Close()
	got = ev.waitN(t, 4)
	if want := []string{"T+ " + az, "L+ " + az, "L- " + az, "T- " + az}; !slices.Equal(got, want) {
		t.Fatalf("close: got %q, want %q", got, want)
	}
	if !ev.ts[1].Transport.ZID.Equal(a.ZID()) || ev.links[1].Link.Src != k.Src {
		t.Errorf("delete events %+v %+v", ev.ts[1], ev.links[1])
	}
	if len(r.Links()) != 0 {
		t.Errorf("Links() after close: %+v", r.Links())
	}
	rz := r.ZID().String()
	if got := cev.get(); !slices.Equal(got, []string{"L- " + rz, "T- " + rz}) {
		t.Errorf("client's own events on close: %q", got)
	}
}

// TestTransportEventsRouterGone: a client hears its router go.
func TestTransportEventsRouterGone(t *testing.T) {
	r := openRouter(t)
	a := evClient(t, r.Locators()[0])
	ev := listenEvents(t, a, WithHistory(true))
	rz := r.ZID().String()
	if got := ev.get(); !slices.Equal(got, []string{"T+ " + rz, "L+ " + rz}) {
		t.Fatalf("history %q", got)
	}
	_ = r.Close()
	if got := ev.waitN(t, 4); !slices.Equal(got[2:], []string{"L- " + rz, "T- " + rz}) {
		t.Errorf("router gone: %q", got)
	}
}

func TestLinkEventsFilter(t *testing.T) {
	r := openRouter(t)
	a := evClient(t, r.Locators()[0])
	b := evClient(t, r.Locators()[0])
	waitFor(t, "two transports", func() bool { return len(r.Transports()) == 2 })
	var ta TransportInfo
	for _, ti := range r.Transports() {
		if ti.ZID.Equal(a.ZID()) {
			ta = ti
		}
	}
	ev := &evRec{}
	l, err := r.DeclareLinkEventsListener(ev.onL, WithHistory(true), WithLinkTransport(ta))
	if err != nil {
		t.Fatal(err)
	}
	az := a.ZID().String()
	if got := ev.get(); !slices.Equal(got, []string{"L+ " + az}) {
		t.Errorf("filtered history %q", got)
	}
	_ = b.Close()
	_ = a.Close()
	if got := ev.waitN(t, 2); !slices.Equal(got, []string{"L+ " + az, "L- " + az}) {
		t.Errorf("filtered %q", got)
	}
	if err := l.Undeclare(); err != nil {
		t.Fatal(err)
	}
	if err := l.Undeclare(); err == nil {
		t.Error("second Undeclare succeeded")
	}
	c := evClient(t, r.Locators()[0])
	waitFor(t, "c", func() bool { return len(r.Transports()) == 1 })
	_ = c.Close()
	time.Sleep(100 * time.Millisecond)
	if got := ev.get(); len(got) != 2 {
		t.Errorf("events after Undeclare: %q", got)
	}
}

// TestTransportEventsConsistent: listeners declared (with history) while
// clients come and go see, per client, Put then Delete, never twice, and
// end up agreeing with Transports.
func TestTransportEventsConsistent(t *testing.T) {
	r := openRouter(t)
	loc := r.Locators()[0]
	var recs []*evRec
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := Open(Config{Connect: []string{loc}})
			if err != nil {
				t.Error(err)
				return
			}
			time.Sleep(time.Duration(i*7) * time.Millisecond)
			if i%2 == 0 {
				_ = c.Close()
			} else {
				t.Cleanup(func() { _ = c.Close() })
			}
		}()
		rec := &evRec{}
		l, err := r.DeclareTransportEventsListener(rec.onT, WithHistory(true))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = l.Undeclare() })
		recs = append(recs, rec)
		time.Sleep(3 * time.Millisecond)
	}
	wg.Wait()
	waitFor(t, "4 clients left", func() bool { return len(r.Transports()) == 4 })
	time.Sleep(200 * time.Millisecond)
	for i, rec := range recs {
		up := map[string]bool{}
		for _, e := range rec.get() {
			z := e[3:]
			if put := e[:2] == "T+"; put == up[z] {
				t.Fatalf("listener %d: %q out of order in %q", i, e, rec.get())
			} else {
				up[z] = put
			}
		}
		n := 0
		for _, v := range up {
			if v {
				n++
			}
		}
		if n != 4 {
			t.Errorf("listener %d ends with %d transports up: %q", i, n, rec.get())
		}
	}
}

func TestTransportEventsSessionClosed(t *testing.T) {
	r := openRouter(t)
	_ = r.Close()
	if _, err := r.DeclareTransportEventsListener(func(TransportEvent) {}); err != ErrSessionClosed {
		t.Errorf("got %v", err)
	}
}

func TestTransportEventsMulticast(t *testing.T) {
	group := mcastLocator(t)
	a := openMcastPeer(t, group)
	ev := listenEvents(t, a)
	b := openMcastPeer(t, group)
	bz := b.ZID().String()
	if got := ev.waitN(t, 2); !slices.Equal(got, []string{"T+ " + bz, "L+ " + bz}) {
		t.Fatalf("join: %q", got)
	}
	ti, k := ev.ts[0].Transport, ev.links[0].Link
	if !ti.Multicast || ti.WhatAmI != WhatAmIPeer || ti.Proto != "udp" {
		t.Errorf("transport %+v", ti)
	}
	if !k.Multicast || k.Group == "" || k.Dst != k.Group || k.Src == "" || k.Streamed || k.MTU == 0 {
		t.Errorf("link %+v", k)
	}
	if !slices.ContainsFunc(a.Transports(), func(t TransportInfo) bool { return t.Multicast && t.ZID.Equal(b.ZID()) }) {
		t.Errorf("Transports() misses the member: %+v", a.Transports())
	}
	_ = b.Close() // says CLOSE to the group
	if got := ev.waitN(t, 4); !slices.Equal(got[2:], []string{"L- " + bz, "T- " + bz}) {
		t.Errorf("leave: %q", got)
	}
}

// TestTransportEventsRust: a Rust peer and a Go peer report the same
// transport and link (mirrored) as they connect and part.
func TestTransportEventsRust(t *testing.T) {
	g, err := Open(Config{Mode: ModePeer, Listen: []string{"tcp/127.0.0.1:0"}, Scouting: ScoutingConfig{Disable: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	ev := listenEvents(t, g)
	conf, _ := json.Marshal(map[string]any{"mode": "peer", "scouting/multicast/enabled": false,
		"listen/endpoints": []string{}, "connect/endpoints": g.Locators()})

	// Rust goes: the Go side hears it.
	rs := startRust(t, string(conf), false, "events", "30")
	rz := rs.next().ZID
	var rt, rl peerEvent
	for rt.Ev == "" || rl.Ev == "" {
		switch e := rs.next(); e.Ev {
		case "transport":
			rt = e
		case "link":
			rl = e
		}
	}
	got := ev.waitN(t, 2)
	if !slices.Equal(got, []string{"T+ " + rz, "L+ " + rz}) {
		t.Fatalf("go side: %q", got)
	}
	gt, gl := ev.ts[0].Transport, ev.links[0].Link
	if rt.Kind != "PUT" || rt.ZID != g.ZID().String() || rt.WhatAmI != "peer" || !rt.QoS || rt.Multicast {
		t.Errorf("rust transport %+v", rt)
	}
	if gt.WhatAmI != WhatAmIPeer || !gt.QoS {
		t.Errorf("go transport %+v", gt)
	}
	if rl.Src != gl.Dst || rl.Dst != gl.Src || rl.MTU != gl.MTU || rl.Streamed != gl.Streamed || rl.Group != nil ||
		len(rl.Priorities) != 2 || gl.Priorities == nil ||
		rl.Priorities[0] != int(gl.Priorities.Start) || rl.Priorities[1] != int(gl.Priorities.End) ||
		rl.Reliability == nil || *rl.Reliability != "Reliability.RELIABLE" || gl.Reliability != Reliable {
		t.Errorf("links differ:\nrust %+v\ngo   %+v", rl, gl)
	}
	_ = rs.cmd.Process.Kill()
	if got := ev.waitN(t, 4); !slices.Equal(got[2:], []string{"L- " + rz, "T- " + rz}) {
		t.Errorf("rust gone: %q", got)
	}

	// Go goes: the Rust side hears it.
	rs = startRust(t, string(conf), false, "events", "30")
	rs.next() // zid
	for n := 0; n < 2; {
		if e := rs.next(); e.Ev != "" && e.Kind == "PUT" {
			n++
		}
	}
	_ = g.Close()
	// zenoh-rust reports the transport closed, without removing its links
	// first (we do: see DeclareLinkEventsListener).
	for {
		if e := rs.next(); e.Kind == "DELETE" && e.Ev == "transport" {
			if e.ZID != g.ZID().String() {
				t.Errorf("rust heard %+v", e)
			}
			break
		}
	}
}
