package influxdb

import (
	"regexp"
	"testing"
	"time"

	"github.com/shabunin/tiny-zenoh-go/zenoh"
)

var (
	safeV1 = regexp.MustCompile(`^('[-0-9TZ:.+]+'|now\(\)[-+][0-9]+ns)$`)
	safeV2 = regexp.MustCompile(`^(-?[0-9]+ns|[-0-9TZ:.+]+)$`)
)

// FuzzTimeRange: a query's _time parameter (zenoh.Param and
// zenoh.ParseTimeRange on the selector of a remote query) only ever puts
// a time or a duration into the InfluxQL and Flux queries, and the range
// prints back to a range that parses the same.
func FuzzTimeRange(f *testing.F) {
	for _, s := range []string{"_time=[now(-5s)..]", "a=b;_time=[now(-1.5s)..now()]", "_time=]now(-2m)..now(1h)[",
		"_time=[2024-01-02T03:04:05Z..]", "_time=[..]", "_time=[2024-01-02;1h]", "_time=[now(-1e300s)..now(NaN)]",
		"_time=[0001-01-01..9999-12-31T23:59:59.999999999Z]", "_time=[now(1w)..now(-2u)]", "_anyke;_time=now(-5s)",
		"_time=[2024-01-02 03:04:05.5+02:00..2024-01-02T15:04Z]", "_time=[now()..now(-0ms)["} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, params string) {
		tr, err := timeRange(params)
		if err != nil || tr == nil {
			return
		}
		tr.Contains(time.Now())
		for _, b := range []zenoh.TimeBound{tr.Start, tr.End} {
			if !b.Bounded {
				continue
			}
			if e := timeExprV1(b); !safeV1.MatchString(e) {
				t.Fatalf("%q: InfluxQL time %q", params, e)
			}
			for _, plus1 := range []bool{false, true} {
				if e := boundV2(b, plus1); !safeV2.MatchString(e) {
					t.Fatalf("%q: Flux time %q", params, e)
				}
			}
		}
		s := tr.String()
		back, err := zenoh.ParseTimeRange(s)
		if err != nil || back.String() != s {
			t.Fatalf("%q: %s parses to %s, %v", params, s, back, err)
		}
	})
}
