package zenoh

import (
	"errors"
	"strconv"
	"strings"
	"time"
)

// TimeRange is the "_time" selector parameter (zenoh-util time_range.rs):
// "[start..end]", each side inclusive ('[' start, ']' end) or exclusive
// (']' start, '[' end) and empty when unbounded; a time is RFC 3339 or
// now(), now(-5s), now(1.5h) (units u, ms, s, m, h, d, w; none = seconds).
// "[start;duration]" is accepted too.
type TimeRange struct {
	Start, End TimeBound
}

// TimeBound is one end of a TimeRange.
type TimeBound struct {
	Bounded   bool
	Exclusive bool
	Now       bool          // relative to the time of evaluation...
	Offset    time.Duration // ...plus Offset
	At        time.Time     // absolute, when !Now
}

var errTimeRange = errors.New("zenoh: invalid time range")

// SinceNow returns the range [now(-age)..], as zenoh-rust's max_age.
func SinceNow(age time.Duration) TimeRange {
	return TimeRange{Start: TimeBound{Bounded: true, Now: true, Offset: -age}}
}

func (b TimeBound) resolve(now time.Time) time.Time {
	if b.Now {
		return now.Add(b.Offset)
	}
	return b.At
}

// Contains reports whether t is in the range, now-relative bounds being
// evaluated at the time of the call.
func (r TimeRange) Contains(t time.Time) bool {
	now := time.Now()
	if r.Start.Bounded {
		s := r.Start.resolve(now)
		if s.After(t) || (r.Start.Exclusive && s.Equal(t)) {
			return false
		}
	}
	if r.End.Bounded {
		e := r.End.resolve(now)
		if e.Before(t) || (r.End.Exclusive && e.Equal(t)) {
			return false
		}
	}
	return true
}

func (b TimeBound) expr() string {
	if !b.Now {
		return b.At.UTC().Format(time.RFC3339Nano)
	}
	if b.Offset == 0 {
		return "now()"
	}
	return "now(" + strconv.FormatFloat(b.Offset.Seconds(), 'f', -1, 64) + "s)"
}

// String renders the range in zenoh's syntax.
func (r TimeRange) String() string {
	var sb strings.Builder
	switch {
	case !r.Start.Bounded:
		sb.WriteString("[..")
	case r.Start.Exclusive:
		sb.WriteString("]" + r.Start.expr() + "..")
	default:
		sb.WriteString("[" + r.Start.expr() + "..")
	}
	switch {
	case !r.End.Bounded:
		sb.WriteString("]")
	case r.End.Exclusive:
		sb.WriteString(r.End.expr() + "[")
	default:
		sb.WriteString(r.End.expr() + "]")
	}
	return sb.String()
}

// ParseTimeRange parses zenoh's time range syntax.
func ParseTimeRange(s string) (TimeRange, error) {
	var r TimeRange
	if len(s) < 4 {
		return r, errTimeRange
	}
	var exclStart, exclEnd bool
	switch s[0] {
	case '[':
	case ']':
		exclStart = true
	default:
		return r, errTimeRange
	}
	switch s[len(s)-1] {
	case ']':
	case '[':
		exclEnd = true
	default:
		return r, errTimeRange
	}
	body := s[1 : len(s)-1]
	var err error
	if a, b, ok := strings.Cut(body, ".."); ok {
		if r.Start, err = parseTimeBound(a, exclStart); err != nil {
			return r, err
		}
		r.End, err = parseTimeBound(b, exclEnd)
		return r, err
	}
	a, d, ok := strings.Cut(body, ";")
	if !ok {
		return r, errTimeRange
	}
	if r.Start, err = parseTimeBound(a, exclStart); err != nil {
		return r, err
	}
	if !r.Start.Bounded {
		return r, errTimeRange
	}
	secs, err := parseDurationSecs(d)
	if err != nil {
		return r, err
	}
	r.End = r.Start
	r.End.Exclusive = exclEnd
	add := time.Duration(secs * float64(time.Second))
	if r.End.Now {
		r.End.Offset += add
	} else {
		r.End.At = r.End.At.Add(add)
	}
	return r, nil
}

func parseTimeBound(s string, exclusive bool) (TimeBound, error) {
	if s == "" {
		return TimeBound{}, nil
	}
	b := TimeBound{Bounded: true, Exclusive: exclusive}
	if strings.HasPrefix(s, "now(") && strings.HasSuffix(s, ")") {
		b.Now = true
		if in := s[4 : len(s)-1]; in != "" {
			neg := in[0] == '-'
			if neg {
				in = in[1:]
			}
			secs, err := parseDurationSecs(in)
			if err != nil {
				return b, err
			}
			if neg {
				secs = -secs
			}
			b.Offset = time.Duration(secs * float64(time.Second))
		}
		return b, nil
	}
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02T15:04:05.999999999", "2006-01-02 15:04:05.999999999Z07:00",
		"2006-01-02 15:04:05.999999999", "2006-01-02T15:04Z07:00", "2006-01-02"} {
		if t, err := time.Parse(layout, s); err == nil {
			b.At = t
			return b, nil
		}
	}
	return b, errTimeRange
}

func parseDurationSecs(s string) (float64, error) {
	mult := 1.0
	switch {
	case strings.HasSuffix(s, "ms"):
		s, mult = s[:len(s)-2], 0.001
	case strings.HasSuffix(s, "u"):
		s, mult = s[:len(s)-1], 0.000001
	case strings.HasSuffix(s, "s"):
		s = s[:len(s)-1]
	case strings.HasSuffix(s, "m"):
		s, mult = s[:len(s)-1], 60
	case strings.HasSuffix(s, "h"):
		s, mult = s[:len(s)-1], 3600
	case strings.HasSuffix(s, "d"):
		s, mult = s[:len(s)-1], 86400
	case strings.HasSuffix(s, "w"):
		s, mult = s[:len(s)-1], 604800
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, errTimeRange
	}
	return f * mult, nil
}
