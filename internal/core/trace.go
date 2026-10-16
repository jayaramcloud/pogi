package core

import (
	"net/http"
	"strings"
	"sync"
	"time"
)

// A trace ties together everything one call set off: the gateway gives every
// request one (Ctx.Trace), services copy it into the messages they deliver
// (Message.Trace) and Lambda hands it to the function runtime, so the calls
// the function makes join the same flow.

// NewTrace returns a fresh AWS-style trace id, 1-<8 hex>-<24 hex>.
func NewTrace() string { return "1-" + RandHex(4) + "-" + RandHex(12) }

// TraceFrom reads the trace a request carries: the Root of X-Amzn-Trace-Id,
// else the id of traceparent or X-Cloud-Trace-Context. Without one it makes
// a new trace.
func TraceFrom(h http.Header) string {
	if t := TraceRoot(h.Get("X-Amzn-Trace-Id")); t != "" {
		return t
	}
	if t := TraceFromTraceparent(h.Get("Traceparent")); t != "" {
		return t
	}
	if id, _, _ := strings.Cut(h.Get("X-Cloud-Trace-Context"), "/"); isHex32(id) {
		return "1-" + id[:8] + "-" + id[8:]
	}
	return NewTrace()
}

func isHex32(s string) bool {
	if len(s) != 32 {
		return false
	}
	for _, r := range s {
		if !(r >= '0' && r <= '9' || r >= 'a' && r <= 'f') {
			return false
		}
	}
	return true
}

// TraceFromTraceparent reads the trace of a W3C traceparent value ("00-<32 hex>-<16 hex>-01"), or "".
func TraceFromTraceparent(v string) string {
	if p := strings.Split(v, "-"); len(p) >= 3 && isHex32(p[1]) {
		return "1-" + p[1][:8] + "-" + p[1][8:]
	}
	return ""
}

// Traceparent is the W3C traceparent that carries a trace to the next hop, or "" for a trace that has no such form.
func Traceparent(trace string) string {
	if t, ok := strings.CutPrefix(trace, "1-"); ok && len(t) == 33 {
		if id := strings.Replace(t, "-", "", 1); isHex32(id) {
			return "00-" + id + "-" + RandHex(8) + "-01"
		}
	}
	return ""
}

// TraceRoot returns the Root of an X-Amzn-Trace-Id style value
// ("Root=1-...;Parent=...;Sampled=1"), or "".
func TraceRoot(v string) string {
	for _, part := range strings.Split(v, ";") {
		if k, val, ok := strings.Cut(strings.TrimSpace(part), "="); ok && strings.EqualFold(k, "Root") {
			return val
		}
	}
	return ""
}

// TraceHeader is the X-Amzn-Trace-Id value that carries a trace to the next hop.
func TraceHeader(trace string) string {
	return "Root=" + trace + ";Parent=" + RandHex(8) + ";Sampled=1"
}

// Span kinds.
const (
	SpanDeliver = "deliver" // a message handed from one resource to another
	SpanInvoke  = "invoke"  // pogi ran a function
	SpanRedrive = "redrive" // a message moved to a dead-letter queue
	SpanNotify  = "notify"  // a notification to a subscriber or endpoint
)

// Span is one step of a flow that is not an HTTP request to pogi.
type Span struct {
	Trace   string    `json:"trace"`
	Time    time.Time `json:"time"`
	Ms      float64   `json:"ms"`
	Service string    `json:"service"` // the service that did the work (the target's)
	Op      string    `json:"op"`
	From    string    `json:"from,omitempty"` // resource ids: ARNs or full resource names
	To      string    `json:"to,omitempty"`
	Kind    string    `json:"kind"`
	Err     string    `json:"err,omitempty"`
}

// SpanLog is a bounded log of recent spans.
type SpanLog struct {
	mu  sync.Mutex
	max int
	buf []Span
}

// NewSpanLog keeps the last max spans.
func NewSpanLog(max int) *SpanLog { return &SpanLog{max: max} }

// Add records a span.
func (l *SpanLog) Add(s Span) {
	l.mu.Lock()
	l.buf = append(l.buf, s)
	if len(l.buf) > l.max {
		l.buf = append([]Span(nil), l.buf[len(l.buf)-l.max:]...)
	}
	l.mu.Unlock()
}

// All returns the spans, oldest first.
func (l *SpanLog) All() []Span {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]Span(nil), l.buf...)
}

// Span records a step that began at start. With neither a trace nor a
// sender it records nothing: such traffic is part of no flow and no edge.
func (h *Hub) Span(kind, service, op, from, to, trace string, start time.Time, err error) {
	if h == nil || trace == "" && from == "" {
		return
	}
	s := Span{Trace: trace, Time: start, Ms: float64(time.Since(start).Microseconds()) / 1000, Service: service, Op: op, From: from, To: to, Kind: kind}
	if err != nil {
		s.Err = err.Error()
	}
	h.Spans.Add(s)
}

// Link is a wire between two resources, for the console's map: From and To
// are resource ids (ARNs or full resource names).
type Link struct {
	From, To string
	Kind     string // "configured", or "dlq" for a dead-letter redrive
	Label    string // what the wire is: subscription, notification, event source, target, redrive
}

// Linker services list the wires their resources are configured with.
type Linker interface{ Links() []Link }
