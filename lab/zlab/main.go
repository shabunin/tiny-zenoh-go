// Command zlab is the traffic agent of the containerlab scenarios: a
// publisher of numbered, timestamped samples at a fixed rate, and a
// subscriber that reports what arrived as JSON: losses, duplicates,
// reordering, the longest silence and the latency (the lab's containers
// share the host clock).
//
//	zlab pub -e tcp/10.0.1.1:7447 -k lab/flow -rate 1000 -d 30s
//	zlab sub -e tcp/10.0.3.1:7447 -k lab/flow -d 35s
package main

import (
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/shabunin/tiny-zenoh-go/zenoh"
)

type list []string

func (l *list) String() string     { return strings.Join(*l, ",") }
func (l *list) Set(v string) error { *l = append(*l, v); return nil }

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: zlab pub|sub [flags]")
		os.Exit(2)
	}
	cmd := os.Args[1]
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	var connect list
	fs.Var(&connect, "e", "locator to connect to (repeatable)")
	mode := fs.String("mode", "client", "client or peer")
	key := fs.String("k", "lab/flow", "key expression")
	rate := fs.Int("rate", 1000, "pub: samples per second")
	size := fs.Int("size", 16, "pub: payload size (at least 16)")
	dur := fs.Duration("d", 30*time.Second, "how long to run")
	besteffort := fs.Bool("best-effort", false, "pub: best-effort reliability")
	_ = fs.Parse(os.Args[2:])

	cfg := zenoh.Config{Connect: connect, Scouting: zenoh.ScoutingConfig{Disable: true}}
	if *mode == "peer" {
		cfg.Mode = zenoh.ModePeer
	}
	var s *zenoh.Session
	var err error
	for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(500 * time.Millisecond) {
		if s, err = zenoh.Open(cfg); err == nil || time.Now().After(deadline) {
			break
		}
	}
	if err != nil {
		fatal(err)
	}
	defer s.Close()
	switch cmd {
	case "pub":
		pub(s, *key, *rate, max(*size, 16), *dur, *besteffort)
	case "sub":
		sub(s, *key, *dur)
	default:
		fatal(fmt.Errorf("unknown command %q", cmd))
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "zlab:", err)
	os.Exit(1)
}

func pub(s *zenoh.Session, key string, rate, size int, dur time.Duration, besteffort bool) {
	var opts []zenoh.PutOption
	if besteffort {
		opts = append(opts, zenoh.WithReliability(zenoh.BestEffort))
	}
	p, err := s.DeclarePublisher(key, opts...)
	if err != nil {
		fatal(err)
	}
	buf := make([]byte, size)
	start := time.Now()
	var seq, errs uint64
	period := time.Second / time.Duration(rate)
	for next := start; time.Since(start) < dur; next = next.Add(period) {
		if d := time.Until(next); d > 0 {
			time.Sleep(d)
		}
		binary.BigEndian.PutUint64(buf[0:], seq)
		binary.BigEndian.PutUint64(buf[8:], uint64(time.Now().UnixNano()))
		if p.Put(buf) != nil {
			errs++
		}
		seq++
	}
	out(map[string]any{"role": "pub", "sent": seq, "errors": errs, "secs": time.Since(start).Seconds()})
}

// gap is a run of samples that never came.
type gap struct {
	From, To uint64 // sequence numbers, inclusive
}

func sub(s *zenoh.Session, key string, dur time.Duration) {
	var mu sync.Mutex
	seen := map[uint64]bool{}
	var lat []time.Duration
	var dups, reordered, maxSeq uint64
	var last time.Time
	var maxSilence time.Duration
	var silenceAt float64
	start := time.Now()
	perSec := map[int]int{}
	_, err := s.DeclareSubscriber(key, func(smp zenoh.Sample) {
		now := time.Now()
		if len(smp.Payload) < 16 {
			return
		}
		seq := binary.BigEndian.Uint64(smp.Payload)
		sent := time.Unix(0, int64(binary.BigEndian.Uint64(smp.Payload[8:])))
		mu.Lock()
		defer mu.Unlock()
		if seen[seq] {
			dups++
			return
		}
		seen[seq] = true
		if seq < maxSeq {
			reordered++
		}
		maxSeq = max(maxSeq, seq)
		lat = append(lat, now.Sub(sent))
		if !last.IsZero() && now.Sub(last) > maxSilence {
			maxSilence, silenceAt = now.Sub(last), last.Sub(start).Seconds()
		}
		last = now
		perSec[int(now.Sub(start)/time.Second)]++
	})
	if err != nil {
		fatal(err)
	}
	time.Sleep(dur)
	mu.Lock()
	defer mu.Unlock()
	// Losses: holes between the first and the last sample received.
	var gaps []gap
	var lost uint64
	if len(seen) > 0 {
		var first uint64 = maxSeq
		for q := range seen {
			first = min(first, q)
		}
		for q := first; q <= maxSeq; q++ {
			if seen[q] {
				continue
			}
			from := q
			for q+1 <= maxSeq && !seen[q+1] {
				q++
			}
			lost += q - from + 1
			gaps = append(gaps, gap{From: from, To: q})
		}
	}
	slices.Sort(lat)
	pct := func(p float64) float64 {
		if len(lat) == 0 {
			return 0
		}
		return float64(lat[min(len(lat)-1, int(p*float64(len(lat))))].Microseconds()) / 1000
	}
	secs := make([]int, 0, len(perSec))
	for i := range int(dur / time.Second) {
		secs = append(secs, perSec[i])
	}
	if len(gaps) > 20 {
		gaps = gaps[:20]
	}
	out(map[string]any{
		"role": "sub", "received": len(seen), "max_seq": maxSeq, "lost": lost, "duplicates": dups,
		"reordered": reordered, "gaps": gaps,
		"max_silence_ms": maxSilence.Milliseconds(), "max_silence_at_s": silenceAt,
		"latency_ms": map[string]float64{"p50": pct(0.5), "p99": pct(0.99), "max": pct(1)},
		"per_second": secs,
	})
}

func out(v any) {
	b, _ := json.Marshal(v)
	fmt.Println(string(b))
}
