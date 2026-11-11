package zenoh

import (
	"bytes"
	"fmt"
	"slices"
	"strings"
	"testing"
)

// QUIC variants (multistream, mixed reliability, datagrams only), Go to Go
// and against zenoh-rust both ways.

type quicVariant struct {
	name  string
	meta  string // locator metadata
	prio  bool   // per-priority streams
	links int    // transport links (2 with a datagram companion)
	lossy bool   // reliable traffic itself rides on datagrams
}

var quicVariants = []quicVariant{
	{name: "single", meta: "multistream=0", links: 1},
	{name: "multistream", meta: "", prio: true, links: 1}, // the default
	{name: "mixed", meta: "multistream=0;mixed_rel=1", links: 2},
	{name: "multi-mixed", meta: "mixed_rel=1", prio: true, links: 2},
	{name: "datagram", meta: "rel=0", links: 1, lossy: true},
}

func withMeta(loc, meta string) string {
	if meta == "" {
		return loc
	}
	if i := strings.IndexAny(loc, "?#"); i >= 0 {
		loc = loc[:i]
	}
	return loc + "?" + meta
}

func checkQUICTransport(t *testing.T, s *Session, v quicVariant) {
	t.Helper()
	var ti TransportInfo
	waitFor(t, "transport", func() bool {
		ts := s.Transports()
		if len(ts) == 0 {
			return false
		}
		ti = ts[0]
		return ti.Links == v.links
	})
	if ti.Proto != "quic" || ti.PriorityStreams != v.prio || !ti.QoS {
		t.Fatalf("transport %+v, want priority streams %v", ti, v.prio)
	}
}

var quicPrios = []Priority{PriorityRealTime, PriorityInteractiveHigh, PriorityDataHigh, PriorityData,
	PriorityDataLow, PriorityBackground}

// checkPrios checks that every priority arrived times times, in whatever
// order: separate streams don't keep the order across priorities.
func checkPrios(t *testing.T, got []int, times int, v quicVariant) {
	t.Helper()
	if v.lossy {
		return
	}
	var want []int
	for range times {
		for _, p := range quicPrios {
			want = append(want, int(p))
		}
	}
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Errorf("priorities %v, want %v", got, want)
	}
}

func TestQUICVariantsGo(t *testing.T) {
	c := linkCases(t)[2]
	for _, v := range quicVariants {
		t.Run(v.name, func(t *testing.T) {
			r, err := Open(Config{Mode: ModeRouter, Listen: []string{withMeta("quic/127.0.0.1:0", v.meta)},
				TLS: c.srv, Scouting: ScoutingConfig{Disable: true}})
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			loc := withMeta(r.Locators()[0], v.meta)
			a, b := goClientOn(t, c, loc), goClientOn(t, c, loc)
			checkQUICTransport(t, a, v)
			checkQUICTransport(t, b, v)
			checkMesh(t, a, b)

			pfx := uniq(t)
			sink := newSink()
			sub, _ := b.DeclareSubscriber(pfx+"/**", sink.handler)
			defer sub.Undeclare()
			settle()
			// Every priority, reliable then best effort.
			for _, rel := range []Reliability{Reliable, BestEffort} {
				for _, p := range quicPrios {
					if err := a.Put(fmt.Sprintf("%s/p%d", pfx, p), []byte("x"), WithPriority(p), WithReliability(rel)); err != nil {
						t.Fatal(err)
					}
				}
			}
			var prios []int
			for _, smp := range sink.wait(t, 2*len(quicPrios)) {
				prios = append(prios, int(smp.Priority))
			}
			checkPrios(t, prios, 2, v)
			// A payload far larger than a batch: FRAGMENTs.
			big := bytes.Repeat([]byte("0123456789"), 30000)
			if err := a.Put(pfx+"/big", big); err != nil {
				t.Fatal(err)
			}
			if got := sink.wait(t, 1); !bytes.Equal(got[len(got)-1].Payload, big) {
				t.Errorf("big payload: got %d bytes", len(got[len(got)-1].Payload))
			}
		})
	}
}

// TestQUICVariantsRust: a Rust peer dials a Go peer's QUIC listener with
// the same variant, and both publish at every priority, reliably and best
// effort, to the other.
func TestQUICVariantsRust(t *testing.T) {
	c := linkCases(t)[2]
	for _, v := range quicVariants {
		t.Run(v.name, func(t *testing.T) {
			p, err := Open(Config{Mode: ModePeer, Listen: []string{withMeta("quic/127.0.0.1:0", v.meta)},
				TLS: mergeTLS(c), Scouting: ScoutingConfig{Disable: true}})
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			loc := withMeta(p.Locators()[0], v.meta)
			pfx := uniq(t)

			// Go -> Rust.
			n := 2 * len(quicPrios)
			rs := startRust(t, rustConf(c, "peer", "", loc), true, "sub", pfx+"/g/**", fmt.Sprint(n))
			checkQUICTransport(t, p, v)
			waitFor(t, "rust subscriber", func() bool {
				p.mu.Lock()
				defer p.mu.Unlock()
				for _, f := range p.faces {
					if len(f.subs) > 0 {
						return true
					}
				}
				return false
			})
			for _, rel := range []Reliability{Reliable, BestEffort} {
				for _, pr := range quicPrios {
					_ = p.Put(fmt.Sprintf("%s/g/%d", pfx, pr), []byte(fmt.Sprint(pr)), WithPriority(pr), WithReliability(rel))
				}
			}
			var prios []int
			for range n {
				prios = append(prios, rs.next().Priority)
			}
			checkPrios(t, prios, 2, v)

			// Rust -> Go: priorities on a reliable publisher, then a
			// best-effort one (QUIC datagrams with mixed reliability).
			sink := newSink()
			sub, _ := p.DeclareSubscriber(pfx+"/r/**", sink.handler)
			defer sub.Undeclare()
			startRust(t, rustConf(c, "peer", "", loc), false, "prio", pfx+"/r/prio",
				"REAL_TIME", "INTERACTIVE_HIGH", "DATA_HIGH", "DATA", "DATA_LOW", "BACKGROUND")
			prios = prios[:0]
			for _, smp := range sink.wait(t, len(quicPrios)) {
				prios = append(prios, int(smp.Priority))
			}
			checkPrios(t, prios, 1, v)
			startRust(t, rustConf(c, "peer", "", loc), false, "bepub", pfx+"/r/be", "DATA_LOW", "5")
			got := sink.wait(t, 5)
			if s := string(got[len(got)-1].Payload); !strings.HasPrefix(s, "be-") {
				t.Errorf("best effort: got %q", s)
			}
		})
	}
}

// TestQUICVariantsGoDialsRust: a Go client dials a Rust router's QUIC
// listener (the Rust side picks the variant) and publishes to a Rust
// subscriber on that router.
func TestQUICVariantsGoDialsRust(t *testing.T) {
	c := linkCases(t)[2]
	for _, v := range quicVariants {
		t.Run(v.name, func(t *testing.T) {
			loc := withMeta(freePort(t, "quic"), v.meta)
			pfx := uniq(t)
			n := 2 * len(quicPrios)
			rs := startRust(t, rustConf(c, "router", loc, ""), true, "sub", pfx+"/**", fmt.Sprint(n))
			s := goClientOn(t, c, loc)
			checkQUICTransport(t, s, v)
			for _, rel := range []Reliability{Reliable, BestEffort} {
				for _, pr := range quicPrios {
					_ = s.Put(fmt.Sprintf("%s/%d", pfx, pr), []byte("x"), WithPriority(pr), WithReliability(rel))
				}
			}
			var prios []int
			for range n {
				prios = append(prios, rs.next().Priority)
			}
			checkPrios(t, prios, 2, v)
		})
	}
}
