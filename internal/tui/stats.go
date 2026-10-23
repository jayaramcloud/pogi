package tui

// Numbers worked out from the request log the TUI holds, and the small
// formatters every tab shares.

import (
	"cmp"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

type svcStat struct {
	Calls, Errors int
	AvgMs         float64
	Last          time.Time
}

type errCount struct {
	Code, Service string
	N             int
}

type stats struct {
	Total, Errors, ChaosErrors int
	Slowest                    core.Entry
	Svc                        map[string]svcStat
	PerSec                     [60]int // calls per second, the last 60 seconds, oldest first
	Top                        []errCount
}

// summarize works only from the calls it is given. Errors are status 400 and up.
func summarize(reqs []core.Entry, now time.Time) stats {
	s := stats{Svc: map[string]svcStat{}}
	sum := map[string]float64{}
	top := map[[2]string]int{}
	for _, e := range reqs {
		s.Total++
		if e.Ms > s.Slowest.Ms || s.Total == 1 {
			s.Slowest = e
		}
		v := s.Svc[e.Service]
		v.Calls++
		sum[e.Service] += e.Ms
		if e.Time.After(v.Last) {
			v.Last = e.Time
		}
		if e.Status >= 400 {
			s.Errors++
			v.Errors++
			if e.Chaos {
				s.ChaosErrors++
			}
			code := e.Error
			if code == "" {
				code = "HTTP " + strconv.Itoa(e.Status)
			}
			top[[2]string{code, e.Service}]++
		}
		s.Svc[e.Service] = v
		if age := now.Sub(e.Time); age >= 0 && age < 60*time.Second {
			s.PerSec[59-int(age/time.Second)]++
		}
	}
	for k, v := range s.Svc {
		v.AvgMs = sum[k] / float64(v.Calls)
		s.Svc[k] = v
	}
	for k, n := range top {
		s.Top = append(s.Top, errCount{k[0], k[1], n})
	}
	slices.SortFunc(s.Top, func(a, b errCount) int {
		return cmp.Or(cmp.Compare(b.N, a.N), cmp.Compare(a.Code, b.Code), cmp.Compare(a.Service, b.Service))
	})
	if len(s.Top) > 5 {
		s.Top = s.Top[:5]
	}
	return s
}

// ago: one unit, rounded down.
func ago(d time.Duration) string {
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", max(0, int(d/time.Second)))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d/time.Minute))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh", int(d/time.Hour))
	}
	return fmt.Sprintf("%dd", int(d/(24*time.Hour)))
}

func uptime(secs int64) string {
	d := time.Duration(secs) * time.Second
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", secs)
	case d < time.Hour:
		return fmt.Sprintf("%dm %02ds", int(d/time.Minute), int(d%time.Minute/time.Second))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh %dm", int(d/time.Hour), int(d%time.Hour/time.Minute))
	}
	return fmt.Sprintf("%dd %dh", int(d/(24*time.Hour)), int(d%(24*time.Hour)/time.Hour))
}

// humanSize uses 1024 as the base.
func humanSize(n int64) string {
	if n < 1024 {
		return fmt.Sprintf("%d B", n)
	}
	f := float64(n)
	for _, u := range []string{"KB", "MB", "GB"} {
		f /= 1024
		if f < 1024 || u == "GB" {
			return fmt.Sprintf("%.1f %s", f, u)
		}
	}
	return ""
}

// shortName is the text after the last / or : (the name inside an ARN or a resource path).
func shortName(s string) string {
	if s == "" {
		return "–"
	}
	return s[strings.LastIndexAny(s, "/:")+1:]
}

// tilde shows the home folder as ~.
func tilde(path string) string {
	if home, err := os.UserHomeDir(); err == nil && home != "" && (path == home || strings.HasPrefix(path, home+"/")) {
		return "~" + path[len(home):]
	}
	return path
}

func orDash(s string) string { return or(s, "–") }
