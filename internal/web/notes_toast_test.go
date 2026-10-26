package web

import (
	"strings"
	"testing"
)

func TestToastsFor(t *testing.T) {
	bad := noteRow{note: note{Kind: "bad", Title: "S3 PutObject failed", Text: "500", Href: "/_pogi/requests/1"}}
	warn := noteRow{note: note{Kind: "warn", Title: "Chaos fault injected"}}
	if got := toastsFor(nil); len(got) != 0 {
		t.Fatalf("no notes: %v", got)
	}
	if got := toastsFor([]noteRow{bad}); len(got) != 1 || got[0]["href"] != "/_pogi/requests/1" || !strings.Contains(got[0]["msg"], "S3 PutObject failed") {
		t.Fatalf("one note toasts as it is: %v", got)
	}
	got := toastsFor([]noteRow{bad, bad, bad})
	if len(got) != 1 || got[0]["msg"] != "3 calls failed · " || got[0]["kind"] != "bad" || got[0]["href"] != "#bell" {
		t.Fatalf("same kind: %v", got)
	}
	if got := toastsFor([]noteRow{bad, warn}); got[0]["msg"] != "2 new notifications · " || got[0]["kind"] != "warn" {
		t.Fatalf("mixed kinds: %v", got)
	}
}
