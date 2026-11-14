package zenoh

import (
	"testing"
	"time"
)

// The "_time" selector parameter: both forms, every unit and date layout,
// and what must be refused.
func TestParseTimeRangeForms(t *testing.T) {
	for _, c := range []struct{ in, out string }{
		// [start;duration]
		{"[now(-1h);30m]", "[now(-3600s)..now(-1800s)]"},
		{"]now();5s[", "]now()..now(5s)["},
		{"[2024-01-02T00:00:00Z;1d]", "[2024-01-02T00:00:00Z..2024-01-03T00:00:00Z]"},
		// units (none: seconds)
		{"[now(1u)..]", "[now(0.000001s)..]"},
		{"[now(250ms)..]", "[now(0.25s)..]"},
		{"[now(2)..]", "[now(2s)..]"},
		{"[now(1d)..]", "[now(86400s)..]"},
		{"[now(-1w)..]", "[now(-604800s)..]"},
		{"[..now()]", "[..now()]"},
		// date layouts
		{"[2024-01-02T03:04:05.5..]", "[2024-01-02T03:04:05.5Z..]"},
		{"[2024-01-02 03:04:05+01:00..]", "[2024-01-02T02:04:05Z..]"},
		{"[2024-01-02 03:04:05..]", "[2024-01-02T03:04:05Z..]"},
		{"[2024-01-02T03:04Z..]", "[2024-01-02T03:04:00Z..]"},
		{"[2024-01-02..2024-01-03[", "[2024-01-02T00:00:00Z..2024-01-03T00:00:00Z["},
	} {
		r, err := ParseTimeRange(c.in)
		if err != nil {
			t.Errorf("%s: %v", c.in, err)
			continue
		}
		if r.String() != c.out {
			t.Errorf("%s -> %s, want %s", c.in, r, c.out)
		}
	}
	for _, bad := range []string{
		"", "[]", "[..", "now()..",
		"[now()5s]",      // neither .. nor ;
		"[;5s]",          // a duration needs a start
		"[now();]",       // ... and a duration
		"[now();5x]",     // unknown unit
		"[bogus;5s]",     // bad start
		"[bogus..]",      // bad start
		"[..bogus]",      // bad end
		"[now(x)..]",     // bad offset
		"[now(-1y)..]",   // unknown unit
		"[2024-13-45..]", // not a date
	} {
		if r, err := ParseTimeRange(bad); err == nil {
			t.Errorf("%q accepted as %s", bad, r)
		}
	}
	// A duration range from an absolute start contains what it should.
	r, _ := ParseTimeRange("[2024-01-02T00:00:00Z;1h[")
	at := time.Date(2024, 1, 2, 0, 30, 0, 0, time.UTC)
	if !r.Contains(at) || r.Contains(at.Add(30*time.Minute)) || r.Contains(at.Add(-31*time.Minute)) {
		t.Errorf("%s: Contains wrong", r)
	}
}
