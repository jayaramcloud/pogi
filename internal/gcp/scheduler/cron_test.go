package scheduler

import (
	"testing"
	"time"
)

func TestCronNext(t *testing.T) {
	ny, _ := time.LoadLocation("America/New_York")
	utc := time.UTC
	at := func(loc *time.Location, y int, m time.Month, d, h, min int) time.Time {
		return time.Date(y, m, d, h, min, 0, 0, loc)
	}
	for _, tc := range []struct {
		expr string
		loc  *time.Location
		from time.Time
		want time.Time
	}{
		{"* * * * *", utc, at(utc, 2026, 1, 1, 0, 0), at(utc, 2026, 1, 1, 0, 1)},
		{"*/15 * * * *", utc, at(utc, 2026, 1, 1, 10, 14), at(utc, 2026, 1, 1, 10, 15)},
		{"0 9 * * mon-fri", utc, at(utc, 2026, 1, 2, 10, 0), at(utc, 2026, 1, 5, 9, 0)},  // Friday after 9 → Monday
		{"30 2 1 jan,jul *", utc, at(utc, 2026, 1, 1, 3, 0), at(utc, 2026, 7, 1, 2, 30)}, // names and lists
		{"0 0 29 2 *", utc, at(utc, 2026, 1, 1, 0, 0), at(utc, 2028, 2, 29, 0, 0)},       // leap day
		{"0 12 13 * fri", utc, at(utc, 2026, 1, 1, 0, 0), at(utc, 2026, 1, 2, 12, 0)},    // day-of-month OR day-of-week
		{"0 0 * * 7", utc, at(utc, 2026, 1, 1, 0, 0), at(utc, 2026, 1, 4, 0, 0)},         // 7 is Sunday
		{"10-20/5 8 * * *", utc, at(utc, 2026, 1, 1, 8, 10), at(utc, 2026, 1, 1, 8, 15)}, // range with step
		{"0 9 * * *", ny, at(utc, 2026, 7, 1, 12, 0), at(ny, 2026, 7, 1, 9, 0)},          // 9:00 in New York, EDT
		{"30 2 * * *", ny, at(ny, 2026, 3, 7, 3, 0), at(ny, 2026, 3, 9, 2, 30)},          // 2:30 does not exist on the spring-forward day: skipped
	} {
		c, err := parseCron(tc.expr, tc.loc)
		if err != nil {
			t.Fatalf("%q: %v", tc.expr, err)
		}
		if got := c.next(tc.from); !got.Equal(tc.want) {
			t.Errorf("%q after %v = %v, want %v", tc.expr, tc.from, got, tc.want)
		}
	}
	for _, bad := range []string{"", "* * * *", "60 * * * *", "* 24 * * *", "* * 0 * *", "* * * 13 *", "* * * * 8", "5-1 * * * *", "*/0 * * * *", "a * * * *"} {
		if _, err := parseCron(bad, time.UTC); err == nil {
			t.Errorf("%q parsed", bad)
		}
	}
}
