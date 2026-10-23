package lambda

import (
	"testing"
	"time"
)

// A function that does not start in time says which part was slow, and when each part got ready.
func TestWaitingSaysWhatWasSlow(t *testing.T) {
	born := time.Now().Add(-10 * time.Second)
	in := &instance{born: born}
	if got := in.waiting(); got != ": its runtime never asked for an invocation" {
		t.Errorf("no runtime yet: %q", got)
	}
	in.asked.Store(born.Add(9 * time.Second).UnixNano())
	in.ext.init(2)
	in.ext.list = map[string]*extension{"a": {name: "rec", registered: born.Add(2 * time.Second), ready: born.Add(9500 * time.Millisecond)}}
	want := ": its runtime asked for an invocation after 9s; 2 extensions started, 1 registered; rec registered after 2s and finished its init after 9.5s"
	if got := in.waiting(); got != want {
		t.Errorf("slow extension:\n got %q\nwant %q", got, want)
	}
}
