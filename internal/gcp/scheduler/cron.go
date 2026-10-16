package scheduler

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// cron is a parsed unix-cron schedule: minute hour day-of-month month day-of-week.
// ponytail: no @hourly-style macros and no App Engine "every 5 minutes" syntax.
type cron struct {
	min, hour, dom, month, dow uint64 // bit sets
	domAny, dowAny             bool   // field was "*": when both are restricted, either one matching is enough
	loc                        *time.Location
}

var (
	monthNames = []string{"jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"}
	dowNames   = []string{"sun", "mon", "tue", "wed", "thu", "fri", "sat"}
)

func parseCron(expr string, loc *time.Location) (*cron, error) {
	f := strings.Fields(expr)
	if len(f) != 5 {
		return nil, fmt.Errorf("want 5 fields (minute hour day-of-month month day-of-week), got %d", len(f))
	}
	c := &cron{loc: loc, domAny: strings.HasPrefix(f[2], "*"), dowAny: strings.HasPrefix(f[4], "*")}
	var err error
	for i, p := range []struct {
		field  string
		lo, hi int
		names  []string
		offset int // value of names[0]
		set    *uint64
	}{{f[0], 0, 59, nil, 0, &c.min}, {f[1], 0, 23, nil, 0, &c.hour}, {f[2], 1, 31, nil, 0, &c.dom},
		{f[3], 1, 12, monthNames, 1, &c.month}, {f[4], 0, 7, dowNames, 0, &c.dow}} {
		if *p.set, err = parseField(p.field, p.lo, p.hi, p.names, p.offset); err != nil {
			return nil, fmt.Errorf("field %d (%q): %w", i+1, p.field, err)
		}
	}
	if c.dow&(1<<7) != 0 { // 7 is Sunday too
		c.dow |= 1
	}
	return c, nil
}

// parseField reads a comma list of *, N, A-B, optionally with /step.
func parseField(s string, lo, hi int, names []string, offset int) (uint64, error) {
	val := func(v string) (int, error) {
		for i, n := range names {
			if strings.EqualFold(v, n) {
				return i + offset, nil
			}
		}
		n, err := strconv.Atoi(v)
		if err != nil || n < lo || n > hi {
			return 0, fmt.Errorf("%q is not in %d-%d", v, lo, hi)
		}
		return n, nil
	}
	var set uint64
	for _, item := range strings.Split(s, ",") {
		rng, step, hasStep := strings.Cut(item, "/")
		n := 1
		if hasStep {
			var err error
			if n, err = strconv.Atoi(step); err != nil || n < 1 {
				return 0, fmt.Errorf("bad step %q", step)
			}
		}
		a, b := lo, hi
		if rng != "*" {
			from, to, isRange := strings.Cut(rng, "-")
			var err error
			if a, err = val(from); err != nil {
				return 0, err
			}
			b = a
			if isRange {
				if b, err = val(to); err != nil {
					return 0, err
				}
			} else if hasStep {
				b = hi // "5/15" runs from 5 to the end
			}
			if a > b {
				return 0, fmt.Errorf("range %q runs backwards", item)
			}
		}
		for v := a; v <= b; v += n {
			set |= 1 << v
		}
	}
	return set, nil
}

func (c *cron) dayOK(t time.Time) bool {
	dom, dow := c.dom&(1<<t.Day()) != 0, c.dow&(1<<int(t.Weekday())) != 0
	switch {
	case c.domAny:
		return dow
	case c.dowAny:
		return dom
	}
	return dom || dow
}

// next returns the first matching minute strictly after t, or the zero time.
func (c *cron) next(t time.Time) time.Time {
	t = t.In(c.loc)
	t = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), 0, 0, c.loc).Add(time.Minute)
	for limit := t.Year() + 8; t.Year() <= limit; {
		var n time.Time
		switch {
		case c.month&(1<<int(t.Month())) == 0:
			n = time.Date(t.Year(), t.Month()+1, 1, 0, 0, 0, 0, c.loc)
		case !c.dayOK(t):
			n = time.Date(t.Year(), t.Month(), t.Day()+1, 0, 0, 0, 0, c.loc)
		case c.hour&(1<<t.Hour()) == 0:
			n = time.Date(t.Year(), t.Month(), t.Day(), t.Hour()+1, 0, 0, 0, c.loc)
		case c.min&(1<<t.Minute()) == 0:
			n = t.Add(time.Minute)
		default:
			return t
		}
		if !n.After(t) { // a daylight-saving gap can make Date step back
			n = t.Add(time.Hour)
		}
		t = n
	}
	return time.Time{}
}
