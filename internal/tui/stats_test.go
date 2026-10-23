package tui

import (
	"testing"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

func TestSummarize(t *testing.T) {
	now := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
	at := func(age time.Duration) time.Time { return now.Add(-age) }
	reqs := []core.Entry{
		{Service: "sqs", Status: 400, Error: "ThrottlingException", Chaos: true, Ms: 800, Time: at(0)},                                     // newest bucket
		{Service: "sqs", Status: 400, Error: "ThrottlingException", Chaos: true, Ms: 900, Time: at(59*time.Second + 900*time.Millisecond)}, // oldest bucket
		{Service: "sqs", Status: 200, Ms: 100, Time: at(60 * time.Second)},                                                                 // just outside the window
		{Service: "s3", Status: 404, Error: "NoSuchKey", Ms: 1, Time: at(5 * time.Second)},
		{Service: "iam", Status: 400, Error: "ThrottlingException", Ms: 2, Time: at(5 * time.Second)},
		{Service: "s3", Status: 200, Ms: 3, Time: at(3 * time.Second)},
		{Service: "s3", Status: 500, Ms: 3, Time: at(-time.Minute)}, // in the future: not in the window
	}
	s := summarize(reqs, now)
	if s.Total != 7 || s.Errors != 5 || s.ChaosErrors != 2 || s.Slowest.Ms != 900 {
		t.Errorf("totals = %d %d %d slowest %v", s.Total, s.Errors, s.ChaosErrors, s.Slowest.Ms)
	}
	if v := s.Svc["sqs"]; v.Calls != 3 || v.Errors != 2 || v.AvgMs != 600 || !v.Last.Equal(now) {
		t.Errorf("sqs = %+v", v)
	}
	if s.PerSec[59] != 1 || s.PerSec[0] != 1 || s.PerSec[54] != 2 || s.PerSec[56] != 1 {
		t.Errorf("per second = %v", s.PerSec)
	}
	sum := 0
	for _, n := range s.PerSec {
		sum += n
	}
	if sum != 5 { // the 60 s old call and the future call are left out
		t.Errorf("window holds %d calls, want 5", sum)
	}
	want := []errCount{{"ThrottlingException", "sqs", 2}, {"HTTP 500", "s3", 1}, {"NoSuchKey", "s3", 1}, {"ThrottlingException", "iam", 1}}
	if len(s.Top) != len(want) {
		t.Fatalf("top = %+v", s.Top)
	}
	for i := range want {
		if s.Top[i] != want[i] {
			t.Errorf("top[%d] = %+v, want %+v", i, s.Top[i], want[i])
		}
	}
	if e := summarize(nil, now); e.Total != 0 || len(e.Top) != 0 || len(e.Svc) != 0 {
		t.Errorf("empty input = %+v", e)
	}
}

func TestSummarizeKeepsFiveTopErrors(t *testing.T) {
	var reqs []core.Entry
	for _, c := range []string{"A", "B", "C", "D", "E", "F", "G"} {
		reqs = append(reqs, core.Entry{Service: "x", Status: 400, Error: c})
	}
	if s := summarize(reqs, time.Now()); len(s.Top) != 5 || s.Top[0].Code != "A" {
		t.Errorf("top = %+v", s.Top)
	}
}

func TestFormats(t *testing.T) {
	for d, want := range map[time.Duration]string{4 * time.Second: "4s", 59*time.Second + 900*time.Millisecond: "59s", 12*time.Minute + 40*time.Second: "12m",
		3*time.Hour + 59*time.Minute: "3h", 49 * time.Hour: "2d", -time.Second: "0s"} {
		if got := ago(d); got != want {
			t.Errorf("ago(%v) = %q, want %q", d, got, want)
		}
	}
	for secs, want := range map[int64]string{32: "32s", 724: "12m 04s", 3*3600 + 12*60: "3h 12m", 2*86400 + 4*3600: "2d 4h"} {
		if got := uptime(secs); got != want {
			t.Errorf("uptime(%d) = %q, want %q", secs, got, want)
		}
	}
	for n, want := range map[int64]string{512: "512 B", 1024: "1.0 KB", 22733: "22.2 KB", 1572864: "1.5 MB", 3 << 30: "3.0 GB"} {
		if got := humanSize(n); got != want {
			t.Errorf("humanSize(%d) = %q, want %q", n, got, want)
		}
	}
	for in, want := range map[string]string{"arn:aws:sqs:us-east-1:000000000000:orders": "orders", "projects/p/topics/t": "t", "plain": "plain", "": "–"} {
		if got := shortName(in); got != want {
			t.Errorf("shortName(%q) = %q, want %q", in, got, want)
		}
	}
	t.Setenv("HOME", "/srv/me")
	if got := tilde("/srv/me/.pogi/snapshots"); got != "~/.pogi/snapshots" {
		t.Errorf("tilde = %q", got)
	}
	if got := tilde("/srv/mework"); got != "/srv/mework" {
		t.Errorf("tilde cut a folder that only starts with the home name: %q", got)
	}
}

func TestLevelOf(t *testing.T) {
	for in, want := range map[string]string{"ERROR upstream timed out": "ERROR", "[WARN] disk low": "WARN", "info: started": "INFO", "[debug]: x": "DEBUG",
		"NOTICE\tx": "NOTICE", "FATAL": "FATAL", "hello world": "–", "": "–", "ERRORS happened": "–"} {
		if got := levelOf(in); got != want {
			t.Errorf("levelOf(%q) = %q, want %q", in, got, want)
		}
	}
}
