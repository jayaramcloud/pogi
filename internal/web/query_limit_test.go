package web

import "testing"

// The console's Firestore limit must fit an int32: 4294967296 used to wrap to 0 and 4294967297 to 1.
func TestQueryLimit(t *testing.T) {
	for v, want := range map[string]struct {
		n  int32
		ok bool
	}{"10": {10, true}, "2147483647": {2147483647, true}, "": {0, false}, "0": {0, false}, "-5": {0, false}, "4294967297": {0, false}, "abc": {0, false}} {
		if n, ok := queryLimit(v); ok != want.ok || ok && n != want.n {
			t.Errorf("queryLimit(%q) = %d, %v; want %d, %v", v, n, ok, want.n, want.ok)
		}
	}
}
