package zenoh

import (
	"slices"
	"strconv"
	"testing"

	"github.com/shabunin/tiny-zenoh-go/protocol"
)

// The advanced subscriber's ordering, without a network: samples fed to it
// as its subscription, history and recovery replies would, and what reaches
// the handler and the miss listeners.

type orderRig struct {
	as     *AdvancedSubscriber
	got    []string
	misses []uint32
}

func newOrderRig(maxDepth, global int, recov *RecoveryConfig) *orderRig {
	r := &orderRig{}
	r.as = &AdvancedSubscriber{maxDepth: maxDepth, global: global, recov: recov,
		seq: map[srcID]*seqSource{}, ts: map[string]*tsSource{}, misses: map[*MissListener]func(Miss){}}
	r.as.cb = func(s Sample) { r.got = append(r.got, string(s.Payload)) }
	r.as.misses[&MissListener{r.as}] = func(m Miss) { r.misses = append(r.misses, m.Count) }
	return r
}

var orderZID, _ = protocol.NewZID([]byte{0x0a, 0x0b})

func (r *orderRig) seq(sns ...uint32) {
	r.as.mu.Lock()
	defer r.as.mu.Unlock()
	for _, sn := range sns {
		r.as.handleLocked(Sample{Payload: []byte(strconv.Itoa(int(sn))),
			SourceInfo: &SourceInfo{ZID: orderZID, EID: 1, SN: sn}})
	}
}

func (r *orderRig) stamped(times ...uint64) {
	r.as.mu.Lock()
	defer r.as.mu.Unlock()
	for _, tm := range times {
		r.as.handleLocked(Sample{Payload: []byte("t" + strconv.Itoa(int(tm))),
			Timestamp: &Timestamp{Time: tm, ID: orderZID}})
	}
}

func (r *orderRig) expect(t *testing.T, got []string, misses ...uint32) {
	t.Helper()
	if !slices.Equal(r.got, got) || !slices.Equal(r.misses, misses) {
		t.Fatalf("delivered %q misses %v, want %q misses %v", r.got, r.misses, got, misses)
	}
}

func TestAdvancedOrderSequenced(t *testing.T) {
	// Live, no recovery: gaps are reported and skipped, late and repeated
	// samples dropped.
	r := newOrderRig(1, 0, nil)
	r.seq(1, 2, 4, 3, 4, 5)
	r.expect(t, []string{"1", "2", "4", "5"}, 1)

	// With recovery, samples after a gap wait for the missing ones.
	r = newOrderRig(1, 0, &RecoveryConfig{})
	r.seq(1, 3, 4, 2)
	r.expect(t, []string{"1", "2", "3", "4"})

	// While the initial history queries run, samples are held until
	// maxDepth of them are pending, then delivered in order.
	r = newOrderRig(3, 1, nil)
	r.seq(5, 3)
	r.expect(t, nil)
	r.seq(4)
	r.expect(t, []string{"3", "4", "5"})

	// ... unless only the newest one is wanted.
	r = newOrderRig(1, 1, nil)
	r.seq(9, 10)
	r.expect(t, []string{"9", "10"})

	// When the queries are done, what is pending goes out in order, gaps
	// reported.
	r = newOrderRig(10, 1, nil)
	r.seq(7, 9, 12)
	r.as.mu.Lock()
	for _, st := range r.as.seq {
		r.as.flushSequencedLocked(st)
	}
	r.as.mu.Unlock()
	r.expect(t, []string{"7", "9", "12"}, 1, 2)
}

func TestAdvancedOrderTimestamped(t *testing.T) {
	// Live: older or equal timestamps dropped.
	r := newOrderRig(1, 0, nil)
	r.stamped(20, 10, 20, 30)
	r.expect(t, []string{"t20", "t30"})

	// During history: held in timestamp order (repeats once), flushed at
	// maxDepth.
	r = newOrderRig(3, 1, nil)
	r.stamped(30, 10, 30)
	r.expect(t, nil)
	r.stamped(20)
	r.expect(t, []string{"t10", "t20", "t30"})

	// A recovery query finishing flushes its source's pending samples.
	r = newOrderRig(10, 0, nil)
	id := string(orderZID.Bytes())
	r.as.ts[id] = &tsSource{queries: 1}
	r.stamped(50, 40)
	r.expect(t, nil)
	r.as.mu.Lock()
	r.as.tsDone(id)()
	r.as.tsDone("unknown")()
	r.as.mu.Unlock()
	r.expect(t, []string{"t40", "t50"})
}

func TestAdvancedOrderMisc(t *testing.T) {
	// Neither source info nor timestamp: delivered as is.
	r := newOrderRig(1, 0, nil)
	r.as.mu.Lock()
	r.as.handleLocked(Sample{Payload: []byte("plain")})
	r.as.mu.Unlock()
	r.expect(t, []string{"plain"})

	// Undeclared miss listeners hear nothing more.
	r = newOrderRig(1, 0, nil)
	for l := range r.as.misses {
		l.Undeclare()
	}
	r.seq(1, 5)
	r.expect(t, []string{"1", "5"})

	// After Undeclare (cb nil) nothing is delivered.
	r = newOrderRig(1, 0, nil)
	r.as.cb = nil
	r.seq(1)
	r.stamped(1)
	if len(r.got) != 0 {
		t.Fatalf("delivered %q after undeclare", r.got)
	}
}
