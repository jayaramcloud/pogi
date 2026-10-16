package core

import (
	"context"
	"math/rand/v2"
	"path"
	"strings"
	"sync"
	"time"
)

// Entry is one recorded API call.
type Entry struct {
	ID         int64             `json:"id"`
	Time       time.Time         `json:"time"`
	Cloud      string            `json:"cloud"`
	Service    string            `json:"service"`
	Op         string            `json:"op"`
	Method     string            `json:"method"`
	Path       string            `json:"path"`
	Status     int               `json:"status"`
	Ms         float64           `json:"ms"`
	Account    string            `json:"account,omitempty"`
	Region     string            `json:"region,omitempty"`
	RequestID  string            `json:"requestId,omitempty"`
	AccessKey  string            `json:"accessKey,omitempty"`
	Principal  string            `json:"principal,omitempty"`
	Error      string            `json:"error,omitempty"`
	Chaos      bool              `json:"chaos,omitempty"`
	IAM        string            `json:"iam,omitempty"`   // "allow" or "deny: why" when enforcement is on
	Trace      string            `json:"trace,omitempty"` // the flow (Ctx.Trace) this request belongs to
	ReqHeaders map[string]string `json:"reqHeaders,omitempty"`
	ReqBody    string            `json:"reqBody,omitempty"`
	RespBody   string            `json:"respBody,omitempty"`
}

// Summary drops headers and bodies for list views.
func (e Entry) Summary() Entry {
	e.ReqHeaders, e.ReqBody, e.RespBody = nil, "", ""
	return e
}

// RequestLog is a bounded, waitable log of recent calls.
type RequestLog struct {
	mu    sync.Mutex
	max   int
	buf   []Entry
	next  int64
	total map[string]int // per service
	wake  chan struct{}
}

// NewRequestLog keeps the last max entries.
func NewRequestLog(max int) *RequestLog {
	return &RequestLog{max: max, total: map[string]int{}, wake: make(chan struct{})}
}

// Add appends an entry and wakes waiters.
func (l *RequestLog) Add(e Entry) Entry {
	l.mu.Lock()
	l.next++
	e.ID = l.next
	l.buf = append(l.buf, e)
	if len(l.buf) > l.max {
		l.buf = append([]Entry(nil), l.buf[len(l.buf)-l.max:]...)
	}
	l.total[e.Service]++
	close(l.wake)
	l.wake = make(chan struct{})
	l.mu.Unlock()
	return e
}

// Query returns summaries newer than after (newest last), filtered.
// q matches service, op, path or error code; errorsOnly keeps status >= 400.
func (l *RequestLog) Query(after int64, limit int, service, q string, errorsOnly bool) []Entry {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []Entry
	for i := len(l.buf) - 1; i >= 0 && (limit <= 0 || len(out) < limit); i-- {
		e := l.buf[i]
		if e.ID <= after {
			break
		}
		if service != "" && e.Service != service || errorsOnly && e.Status < 400 {
			continue
		}
		if q != "" && !strings.Contains(strings.ToLower(e.Service+" "+e.Op+" "+e.Path+" "+e.Error), strings.ToLower(q)) {
			continue
		}
		out = append(out, e.Summary())
	}
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out
}

// Get returns one full entry.
func (l *RequestLog) Get(id int64) (Entry, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for i := len(l.buf) - 1; i >= 0; i-- {
		if l.buf[i].ID == id {
			return l.buf[i], true
		}
	}
	return Entry{}, false
}

// Totals returns request counts per service since start.
func (l *RequestLog) Totals() map[string]int {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make(map[string]int, len(l.total))
	for k, v := range l.total {
		out[k] = v
	}
	return out
}

// Wait blocks until an entry newer than after exists, ctx ends or d passes.
func (l *RequestLog) Wait(ctx context.Context, after int64, d time.Duration) {
	l.mu.Lock()
	if l.next > after {
		l.mu.Unlock()
		return
	}
	wake := l.wake
	l.mu.Unlock()
	select {
	case <-wake:
	case <-ctx.Done():
	case <-time.After(d):
	}
}

// Rule is a chaos rule: matching calls get latency and/or an injected error.
type Rule struct {
	ID          string  `json:"id"`
	Cloud       string  `json:"cloud,omitempty"`
	Service     string  `json:"service,omitempty"`   // glob, empty = any
	Op          string  `json:"op,omitempty"`        // glob
	Region      string  `json:"region,omitempty"`    // glob
	Principal   string  `json:"principal,omitempty"` // glob on Ctx.Principal
	Path        string  `json:"path,omitempty"`      // glob on Ctx.Path
	Probability float64 `json:"probability"`         // 0..1, 0 means always
	LatencyMs   int     `json:"latencyMs,omitempty"`
	Status      int     `json:"status,omitempty"` // >0 injects an error
	Code        string  `json:"code,omitempty"`
	Message     string  `json:"message,omitempty"`
	Hits        int     `json:"hits"`
}

// Chaos holds fault-injection rules.
type Chaos struct {
	mu    sync.Mutex
	rules []*Rule
}

// Add stores a rule and returns it with an ID.
func (ch *Chaos) Add(r Rule) Rule {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	if r.ID == "" {
		r.ID = RandHex(4)
	}
	if r.Status > 0 && r.Code == "" {
		r.Code = map[bool]string{true: "ThrottlingException", false: "InternalFailure"}[r.Status == 429]
	}
	if r.Status > 0 && r.Message == "" {
		r.Message = "injected by pogi chaos rule " + r.ID
	}
	ch.rules = append(ch.rules, &r)
	return r
}

// List returns a copy of all rules.
func (ch *Chaos) List() []Rule {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	out := make([]Rule, len(ch.rules))
	for i, r := range ch.rules {
		out[i] = *r
	}
	return out
}

// Remove deletes a rule by ID ("" clears all).
func (ch *Chaos) Remove(id string) bool {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	if id == "" {
		ch.rules = nil
		return true
	}
	for i, r := range ch.rules {
		if r.ID == id {
			ch.rules = append(ch.rules[:i], ch.rules[i+1:]...)
			return true
		}
	}
	return false
}

// Apply sleeps for matching latency rules and returns an injected error, if any.
func (ch *Chaos) Apply(c *Ctx) *Error {
	ch.mu.Lock()
	var delay time.Duration
	var fail *Error
	for _, r := range ch.rules {
		if !glob(r.Cloud, c.Cloud) || !glob(r.Service, c.Service) || !glob(r.Op, c.Op) || !glob(r.Region, c.Region) ||
			!glob(r.Principal, c.Principal) || !glob(r.Path, c.Path) {
			continue
		}
		if r.Probability > 0 && rand.Float64() >= r.Probability {
			continue
		}
		r.Hits++
		delay += time.Duration(r.LatencyMs) * time.Millisecond
		if r.Status > 0 && fail == nil {
			fail = &Error{r.Status, r.Code, r.Message}
		}
	}
	ch.mu.Unlock()
	time.Sleep(delay)
	return fail
}

func glob(pattern, s string) bool {
	if pattern == "" || pattern == "*" {
		return true
	}
	ok, _ := path.Match(strings.ToLower(pattern), strings.ToLower(s))
	return ok
}
