// Package agent is the device side of the board tests: a scripted run of
// zenoh features against a host harness (examples/boardtest/host.py, built
// on zenoh-rust) that plays the other side of each step. Device and harness
// talk over zenoh itself: the device reports on <prefix>/status, the
// harness answers on <prefix>/host. It has no TinyGo-specific code, so the
// same run can be tried on a PC first (examples/boardtest/sim).
package agent

import (
	"errors"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/shabunin/tiny-zenoh-go/zenoh"
)

// ErrNoHarness means no harness subscribed to the run's status in time.
var ErrNoHarness = errors.New("no harness")

// Steps names the steps in order, as the harness expects them.
var Steps = []string{"query", "match", "advpub", "advsub", "live"}

// Run runs every step on s under prefix (board/<name>/<profile>), logging
// with logf, and returns how many passed.
func Run(s *zenoh.Session, prefix string, logf func(string)) (passed int, err error) {
	a := &agent{s: s, p: prefix, logf: logf, host: make(chan string, 16)}
	if a.status, err = s.DeclarePublisher(prefix + "/status"); err != nil {
		return 0, err
	}
	defer a.status.Undeclare()
	sub, err := s.DeclareSubscriber(prefix+"/host", func(smp zenoh.Sample) {
		select {
		case a.host <- string(smp.Payload):
		default:
		}
	})
	if err != nil {
		return 0, err
	}
	defer sub.Undeclare()

	// Wait until the harness is subscribed to our status.
	m := make(chan bool, 4)
	ml, err := a.status.DeclareMatchingListener(func(st zenoh.MatchingStatus) { m <- st.Matching })
	if err != nil {
		return 0, err
	}
	if !waitBool(m, true, 30*time.Second) {
		_ = ml.Undeclare()
		return 0, ErrNoHarness
	}
	_ = ml.Undeclare()
	runtime.GC()
	var ms0 runtime.MemStats
	runtime.ReadMemStats(&ms0)
	a.say("start " + strings.Join(Steps, ","))
	a.say("heap at start: inuse=" + strconv.FormatUint(ms0.HeapInuse, 10))
	if err := a.expect("start", 10*time.Second); err != nil {
		return 0, err
	}
	for _, t := range s.Transports() {
		a.say("transport " + t.WhatAmI.String() + " " + t.ZID.String() + " " + t.Proto +
			" batch=" + strconv.Itoa(t.BatchSize) + " qos=" + strconv.FormatBool(t.QoS) +
			" lowlatency=" + strconv.FormatBool(t.LowLatency) + " lz4=" + strconv.FormatBool(t.Compression))
	}

	for _, step := range []func() error{a.query, a.match, a.advPub, a.advSub, a.live} {
		name := Steps[passed]
		t0 := time.Now()
		if err := step(); err != nil {
			a.say("FAIL " + name + ": " + err.Error())
			return passed, err
		}
		passed++
		a.say("PASS " + name + " " + time.Since(t0).Round(time.Millisecond).String())
	}
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	a.say("DONE " + strconv.Itoa(passed) + " heap_inuse=" + strconv.FormatUint(ms.HeapInuse, 10) +
		" heap_sys=" + strconv.FormatUint(ms.HeapSys, 10))
	return passed, nil
}

type agent struct {
	s      *zenoh.Session
	p      string
	logf   func(string)
	status *zenoh.Publisher
	host   chan string
}

func (a *agent) say(msg string) {
	a.logf(msg)
	_ = a.status.Put([]byte(msg), zenoh.WithCongestionControl(zenoh.CongestionBlock))
}

// expect waits for the harness to send want.
func (a *agent) expect(want string, d time.Duration) error {
	deadline := time.After(d)
	for {
		select {
		case got := <-a.host:
			if got == want {
				return nil
			}
			if strings.HasPrefix(got, "FAIL") {
				return errors.New("harness: " + got)
			}
			a.logf("ignored from harness: " + got)
		case <-deadline:
			return errors.New("timeout waiting for harness " + strconv.Quote(want))
		case <-a.s.Done():
			return zenoh.ErrSessionClosed
		}
	}
}

func waitBool(c chan bool, want bool, d time.Duration) bool {
	deadline := time.After(d)
	for {
		select {
		case v := <-c:
			if v == want {
				return true
			}
		case <-deadline:
			return false
		}
	}
}

func matchFail(want bool) error {
	return errors.New("matching never became " + strconv.FormatBool(want))
}

// query: a querier sees the harness's queryable come and go (matching
// status) and gets an answer from it.
func (a *agent) query() error {
	q, err := a.s.DeclareQuerier(a.p+"/q", zenoh.WithTimeout(5*time.Second))
	if err != nil {
		return err
	}
	defer q.Undeclare()
	m := make(chan bool, 8)
	if _, err := q.DeclareMatchingListener(func(st zenoh.MatchingStatus) { m <- st.Matching }); err != nil {
		return err
	}
	a.say("query: ready")
	if !waitBool(m, true, 10*time.Second) {
		return matchFail(true)
	}
	replies, err := q.Get("x=1")
	if err != nil {
		return err
	}
	var got []string
	for r := range replies {
		smp, err := r.Result()
		if err != nil {
			return err
		}
		got = append(got, smp.KeyExpr+"="+string(smp.Payload))
	}
	if want := a.p + "/q=pong x=1"; len(got) != 1 || got[0] != want {
		return errors.New("replies " + strings.Join(got, ";") + ", want " + want)
	}
	a.say("query: answered")
	if !waitBool(m, false, 10*time.Second) {
		return matchFail(false)
	}
	return nil
}

// match: a publisher waits for the harness's subscriber before publishing.
func (a *agent) match() error {
	pub, err := a.s.DeclarePublisher(a.p + "/m")
	if err != nil {
		return err
	}
	defer pub.Undeclare()
	m := make(chan bool, 8)
	if _, err := pub.DeclareMatchingListener(func(st zenoh.MatchingStatus) { m <- st.Matching }); err != nil {
		return err
	}
	a.say("match: ready")
	if !waitBool(m, true, 10*time.Second) {
		return matchFail(true)
	}
	if err := pub.Put([]byte("m-1"), zenoh.WithAttachment([]byte("att"))); err != nil {
		return err
	}
	return a.expect("match: got m-1 att", 10*time.Second)
}

// advPub: an advanced publisher (cache, sequence numbers, heartbeat,
// liveliness) serves a late zenoh-ext subscriber its history, then live
// samples, then a heartbeat-only subscriber.
func (a *agent) advPub() error {
	ap, err := a.s.DeclareAdvancedPublisher(a.p+"/adv", zenoh.AdvancedPublisherConfig{
		Cache:              &zenoh.CacheConfig{MaxSamples: 10},
		MissDetection:      &zenoh.MissDetectionConfig{Heartbeat: 500 * time.Millisecond},
		PublisherDetection: true,
	})
	if err != nil {
		return err
	}
	defer ap.Undeclare()
	for i := 0; i < 3; i++ {
		if err := ap.Put([]byte("adv-" + strconv.Itoa(i))); err != nil {
			return err
		}
	}
	a.say("advpub: ready")
	if err := a.expect("advpub: more", 15*time.Second); err != nil {
		return err
	}
	for i := 3; i < 5; i++ {
		if err := ap.Put([]byte("adv-" + strconv.Itoa(i))); err != nil {
			return err
		}
	}
	return a.expect("advpub: ok", 20*time.Second)
}

// advSub: an advanced subscriber fetches the history of a zenoh-ext
// publisher it detects through liveliness, then gets live samples in order.
func (a *agent) advSub() error {
	a.say("advsub: ready")
	if err := a.expect("advsub: go", 10*time.Second); err != nil {
		return err
	}
	got := make(chan string, 16)
	var misses uint32
	sub, err := a.s.DeclareAdvancedSubscriber(a.p+"/hist", func(smp zenoh.Sample) {
		select {
		case got <- string(smp.Payload):
		default:
		}
	}, zenoh.AdvancedSubscriberConfig{
		History:  &zenoh.HistoryConfig{DetectLatePublishers: true},
		Recovery: &zenoh.RecoveryConfig{Heartbeat: true},
	})
	if err != nil {
		return err
	}
	defer sub.Undeclare()
	sub.DeclareMissListener(func(m zenoh.Miss) { misses += m.Count })
	collect := func(n int) ([]string, error) {
		var out []string
		deadline := time.After(10 * time.Second)
		for len(out) < n {
			select {
			case p := <-got:
				out = append(out, p)
			case <-deadline:
				return out, errors.New("got [" + strings.Join(out, ",") + "]")
			}
		}
		return out, nil
	}
	first, err := collect(3)
	if err != nil {
		return err
	}
	a.say("advsub: got " + strings.Join(first, ","))
	if err := a.expect("advsub: sent", 10*time.Second); err != nil {
		return err
	}
	rest, err := collect(2)
	if err != nil {
		return err
	}
	all := strings.Join(append(first, rest...), ",")
	if all != "h-0,h-1,h-2,h-3,h-4" || misses != 0 {
		return errors.New("got " + all + " misses=" + strconv.Itoa(int(misses)))
	}
	return nil
}

// live: the harness sees our liveliness token, we see the harness's.
func (a *agent) live() error {
	tok, err := a.s.Liveliness().DeclareToken(a.p + "/alive")
	if err != nil {
		return err
	}
	a.say("live: ready")
	if err := a.expect("live: seen", 10*time.Second); err != nil {
		_ = tok.Undeclare()
		return err
	}
	gone := make(chan bool, 4)
	ls, err := a.s.Liveliness().DeclareSubscriber(a.p+"/host-alive", func(smp zenoh.Sample) {
		gone <- smp.Kind == zenoh.SampleKindDelete
	}, true)
	if err != nil {
		return err
	}
	defer ls.Undeclare()
	if !waitBool(gone, false, 10*time.Second) {
		return errors.New("harness token not seen")
	}
	replies, err := a.s.Liveliness().Get(a.p+"/host-alive", zenoh.WithTimeout(5*time.Second))
	if err != nil {
		return err
	}
	n := 0
	for r := range replies {
		if r.Sample != nil {
			n++
		}
	}
	if n != 1 {
		return errors.New("liveliness get: " + strconv.Itoa(n) + " replies")
	}
	_ = tok.Undeclare()
	a.say("live: undeclared")
	if !waitBool(gone, true, 10*time.Second) {
		return errors.New("harness token never went away")
	}
	return nil
}
