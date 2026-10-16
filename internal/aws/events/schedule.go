package events

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Schedule is a parsed schedule expression.
type Schedule interface {
	// Next returns the first run strictly after t, in t's location, or the
	// zero time when there is none.
	Next(t time.Time) time.Time
}

// ParseSchedule parses rate(N unit), cron(min hour dom month dow year) and
// at(yyyy-mm-ddThh:mm:ss) with AWS semantics. Rules take rate and cron;
// EventBridge Scheduler takes all three.
func ParseSchedule(expr string) (Schedule, error) {
	kind, arg, ok := strings.Cut(strings.TrimSpace(expr), "(")
	arg, closed := strings.CutSuffix(arg, ")")
	if ok && closed {
		switch kind {
		case "rate":
			return parseRate(arg)
		case "cron":
			return parseCron(arg)
		case "at":
			if t, err := time.Parse("2006-01-02T15:04:05", strings.TrimSpace(arg)); err == nil {
				return at(t), nil
			}
		}
	}
	return nil, fmt.Errorf("invalid schedule expression %q", expr)
}

// at runs once; its wall-clock time is read in the location of Next's argument.
type at time.Time

func (a at) Next(t time.Time) time.Time {
	w := time.Time(a)
	n := time.Date(w.Year(), w.Month(), w.Day(), w.Hour(), w.Minute(), w.Second(), 0, t.Location())
	if n.After(t) {
		return n
	}
	return time.Time{}
}

type rate time.Duration

func (r rate) Next(t time.Time) time.Time { return t.Add(time.Duration(r)) }

var rateUnits = map[string]time.Duration{"minute": time.Minute, "hour": time.Hour, "day": 24 * time.Hour}

// parseRate takes "1 minute" or "N minutes|hours|days"; AWS rejects the other plural forms.
func parseRate(arg string) (Schedule, error) {
	if f := strings.Fields(arg); len(f) == 2 {
		n, err := strconv.Atoi(f[0])
		unit, plural := strings.CutSuffix(f[1], "s")
		if d, ok := rateUnits[unit]; ok && err == nil && n > 0 && plural == (n > 1) {
			return rate(time.Duration(n) * d), nil
		}
	}
	return nil, fmt.Errorf("invalid rate %q: want rate(1 minute) or rate(N minutes|hours|days)", arg)
}

// cron is an AWS cron expression. Days match on day-of-week when it is set,
// else on day-of-month (or its last day for "L").
type cron struct {
	min, hour, dom, month, dow, year []bool // indexed by value
	lastDay                          bool
}

var (
	monthNames = strings.Fields("JAN FEB MAR APR MAY JUN JUL AUG SEP OCT NOV DEC")
	dayNames   = strings.Fields("SUN MON TUE WED THU FRI SAT")
)

// parseCron supports *, ?, numbers, names, ranges, lists, steps and L in
// day-of-month; W and # are rejected.
func parseCron(arg string) (Schedule, error) {
	f := strings.Fields(arg)
	if len(f) != 6 {
		return nil, fmt.Errorf("cron needs 6 fields (minutes hours day-of-month month day-of-week year), got %d", len(f))
	}
	if (f[2] == "?") == (f[4] == "?") {
		return nil, fmt.Errorf("cron: exactly one of day-of-month and day-of-week must be ?")
	}
	var err error
	field := func(s string, lo, hi int, names []string) []bool {
		if err != nil {
			return nil
		}
		var set []bool
		set, err = parseField(s, lo, hi, names)
		return set
	}
	c := &cron{lastDay: f[2] == "L"}
	c.min = field(f[0], 0, 59, nil)
	c.hour = field(f[1], 0, 23, nil)
	if f[2] != "?" && !c.lastDay {
		c.dom = field(f[2], 1, 31, nil)
	}
	c.month = field(f[3], 1, 12, monthNames)
	if f[4] != "?" {
		c.dow = field(f[4], 1, 7, dayNames)
	}
	c.year = field(f[5], 1970, 2199, nil)
	if err != nil {
		return nil, fmt.Errorf("cron: %w", err)
	}
	return c, nil
}

func parseField(s string, lo, hi int, names []string) ([]bool, error) {
	set := make([]bool, hi+1)
	for _, part := range strings.Split(s, ",") {
		span, stepText, hasStep := strings.Cut(part, "/")
		step := 1
		if hasStep {
			n, err := strconv.Atoi(stepText)
			if err != nil || n < 1 {
				return nil, fmt.Errorf("invalid step in %q", part)
			}
			step = n
		}
		a, b := lo, hi
		if span != "*" {
			from, to, isRange := strings.Cut(span, "-")
			var err error
			if a, err = cronValue(from, lo, hi, names); err != nil {
				return nil, err
			}
			b = a
			switch {
			case isRange:
				if b, err = cronValue(to, lo, hi, names); err != nil || b < a {
					return nil, fmt.Errorf("invalid range %q", span)
				}
			case hasStep:
				b = hi
			}
		}
		for v := a; v <= b; v += step {
			set[v] = true
		}
	}
	return set, nil
}

func cronValue(s string, lo, hi int, names []string) (int, error) {
	for i, n := range names {
		if strings.EqualFold(s, n) {
			return lo + i, nil
		}
	}
	v, err := strconv.Atoi(s)
	if err != nil || v < lo || v > hi {
		return 0, fmt.Errorf("invalid value %q (want %d-%d)", s, lo, hi)
	}
	return v, nil
}

func (c *cron) day(t time.Time) bool {
	switch {
	case c.dow != nil:
		return c.dow[int(t.Weekday())+1]
	case c.lastDay:
		return t.AddDate(0, 0, 1).Day() == 1
	}
	return c.dom[t.Day()]
}

// Next skips whole years, months, days and hours that cannot match before
// stepping through minutes.
func (c *cron) Next(t time.Time) time.Time {
	loc := t.Location()
	t = t.Truncate(time.Minute).Add(time.Minute)
	for t.Year() <= 2199 {
		var n time.Time
		switch y, m, d := t.Date(); {
		case !c.year[y]:
			n = time.Date(y+1, 1, 1, 0, 0, 0, 0, loc)
		case !c.month[m]:
			n = time.Date(y, m+1, 1, 0, 0, 0, 0, loc)
		case !c.day(t):
			n = time.Date(y, m, d+1, 0, 0, 0, 0, loc)
		case !c.hour[t.Hour()]:
			n = time.Date(y, m, d, t.Hour()+1, 0, 0, 0, loc)
		case !c.min[t.Minute()]:
			n = t.Add(time.Minute)
		default:
			return t
		}
		if !n.After(t) { // a DST change can map a wall-clock time backwards
			n = t.Add(time.Minute)
		}
		t = n
	}
	return time.Time{}
}
