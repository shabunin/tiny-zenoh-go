package zenoh

import (
	"errors"
	"strings"
	"testing"
	"time"
)

// Reply consolidation: a queryable answers c/x three times out of
// timestamp order (t2, t1, t3) and c/y once (t1).
func TestQueryConsolidation(t *testing.T) {
	s := openPeer(t)
	t1, t2, t3 := s.NewTimestamp(), s.NewTimestamp(), s.NewTimestamp()
	q, err := s.DeclareQueryable("cons/**", func(q *Query) {
		for _, r := range []struct {
			key, name string
			ts        Timestamp
		}{{"cons/x", "t2", t2}, {"cons/x", "t1", t1}, {"cons/x", "t3", t3}, {"cons/y", "t1", t1}} {
			if err := q.Reply(r.key, []byte(r.name), WithTimestamp(r.ts)); err != nil {
				t.Error(err)
			}
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	defer q.Undeclare()
	get := func(params string, opts ...GetOption) string {
		t.Helper()
		replies, err := s.Get("cons/**"+params, append(opts, WithTimeout(2*time.Second))...)
		if err != nil {
			t.Fatal(err)
		}
		var got []string
		for r := range replies {
			smp, err := r.Result()
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, strings.TrimPrefix(smp.KeyExpr, "cons/")+"="+string(smp.Payload))
		}
		return strings.Join(got, " ")
	}
	for _, c := range []struct {
		name   string
		params string
		mode   *ConsolidationMode
		want   string
	}{
		{"none", "", ptr(ConsolidationNone), "x=t2 x=t1 x=t3 y=t1"},
		{"monotonic drops what is older than the last", "", ptr(ConsolidationMonotonic), "x=t2 x=t3 y=t1"},
		{"latest keeps the newest per key", "", ptr(ConsolidationLatest), "x=t3 y=t1"},
		{"auto is latest", "", nil, "x=t3 y=t1"},
		{"auto with a time range is none", "?_time=[..]", nil, "x=t2 x=t1 x=t3 y=t1"},
	} {
		var opts []GetOption
		if c.mode != nil {
			opts = append(opts, WithConsolidation(*c.mode))
		}
		if got := get(c.params, opts...); got != c.want {
			t.Errorf("%s: %q, want %q", c.name, got, c.want)
		}
	}
}

func ptr[T any](v T) *T { return &v }

func TestReplyErrorResult(t *testing.T) {
	s := openPeer(t)
	q, err := s.DeclareQueryable("rerr", func(q *Query) { _ = q.ReplyErr([]byte("no such thing"), Encoding{}) })
	if err != nil {
		t.Fatal(err)
	}
	defer q.Undeclare()
	replies, err := s.Get("rerr", WithTimeout(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for r := range replies {
		n++
		_, err := r.Result()
		var re *ReplyError
		if !errors.As(err, &re) || !strings.Contains(err.Error(), "no such thing") {
			t.Fatalf("Result: %v", err)
		}
	}
	if n != 1 {
		t.Fatalf("%d replies", n)
	}
}

// The API refuses invalid key expressions and configurations, and calls on
// a closed session.
func TestAPIArgumentErrors(t *testing.T) {
	s := openPeer(t)
	const bad = "a//b"
	nop := func(Sample) {}
	checks := map[string]error{}
	_, checks["Get"] = s.Get(bad)
	_, checks["Liveliness.DeclareToken"] = s.Liveliness().DeclareToken(bad)
	_, checks["Liveliness.DeclareSubscriber"] = s.Liveliness().DeclareSubscriber(bad, nop, false)
	_, checks["Liveliness.Get"] = s.Liveliness().Get(bad)
	_, checks["DeclareAdvancedPublisher key"] = s.DeclareAdvancedPublisher(bad, AdvancedPublisherConfig{})
	_, checks["DeclareAdvancedPublisher metadata"] = s.DeclareAdvancedPublisher("ok", AdvancedPublisherConfig{Metadata: bad})
	_, checks["DeclareAdvancedPublisher cache"] = s.DeclareAdvancedPublisher("ok", AdvancedPublisherConfig{Cache: &CacheConfig{MaxSamples: -1}})
	_, checks["DeclareAdvancedSubscriber key"] = s.DeclareAdvancedSubscriber(bad, nop, AdvancedSubscriberConfig{})
	for name, err := range checks {
		if err == nil {
			t.Errorf("%s accepted %q", name, bad)
		}
	}
	_ = s.Close()
	if _, err := s.Get("ok"); !errors.Is(err, ErrSessionClosed) {
		t.Errorf("Get on a closed session: %v", err)
	}
	if _, err := s.Liveliness().Get("ok"); !errors.Is(err, ErrSessionClosed) {
		t.Errorf("Liveliness.Get on a closed session: %v", err)
	}
}
