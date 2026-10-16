package core

import (
	"net/http"
	"regexp"
	"testing"
)

func TestTraceFrom(t *testing.T) {
	for name, tc := range map[string]struct {
		h    http.Header
		want string
	}{
		"amzn":        {http.Header{"X-Amzn-Trace-Id": {"Self=1-aa-bb;Root=1-5f84c7a1-31d9a2b4c5e6f7a8b9c0d1e2;Parent=53995c3f42cd8ad8;Sampled=1"}}, "1-5f84c7a1-31d9a2b4c5e6f7a8b9c0d1e2"},
		"traceparent": {http.Header{"Traceparent": {"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"}}, "1-0af76519-16cd43dd8448eb211c80319c"},
		"gcp":         {http.Header{"X-Cloud-Trace-Context": {"105445aa7843bc8bf206b12000100000/1;o=1"}}, "1-105445aa-7843bc8bf206b12000100000"},
	} {
		if got := TraceFrom(tc.h); got != tc.want {
			t.Errorf("%s: TraceFrom = %q, want %q", name, got, tc.want)
		}
	}
	if got := TraceFrom(http.Header{}); !regexp.MustCompile(`^1-[0-9a-f]{8}-[0-9a-f]{24}$`).MatchString(got) {
		t.Errorf("generated trace %q", got)
	}
}

func TestDeliverRecordsSpan(t *testing.T) {
	h := NewHub(Config{DataDir: t.TempDir()}, nil)
	h.OnDeliver("sqs", func(string, *Message) error { return nil })
	q := "arn:aws:sqs:us-east-1:000000000000:q"
	h.Deliver(q, &Message{Body: "x", Trace: "1-a-b", From: "arn:aws:sns:us-east-1:000000000000:t"})
	h.Deliver(q, &Message{Body: "no flow"}) // neither a trace nor a sender: no span
	got := h.Spans.All()
	if len(got) != 1 || got[0].Trace != "1-a-b" || got[0].To != q || got[0].Kind != SpanDeliver || got[0].Service != "sqs" {
		t.Fatalf("spans = %+v", got)
	}
}

func TestTraceparentRoundTrip(t *testing.T) {
	tr := NewTrace()
	tp := Traceparent(tr)
	if !regexp.MustCompile(`^00-[0-9a-f]{32}-[0-9a-f]{16}-01$`).MatchString(tp) || TraceFromTraceparent(tp) != tr {
		t.Fatalf("%s -> %s -> %s", tr, tp, TraceFromTraceparent(tp))
	}
	if Traceparent("custom") != "" {
		t.Error("a trace without a W3C form has no traceparent")
	}
}
