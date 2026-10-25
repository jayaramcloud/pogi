package logging

import "testing"

// A severity number that does not fit LogSeverity (an int32) is not a severity: it must not wrap to another one,
// as "4294967297" wrapped to DEBUG (1).
func TestSeverityRankRange(t *testing.T) {
	for v, want := range map[string]struct {
		n  int
		ok bool
	}{"ERROR": {500, true}, "error": {500, true}, "200": {200, true}, "2147483647": {2147483647, true}, "4294967297": {0, false}, "x": {0, false}} {
		if n, ok := severityRank(v); ok != want.ok || ok && n != want.n {
			t.Errorf("severityRank(%q) = %d, %v; want %d, %v", v, n, ok, want.n, want.ok)
		}
	}
}
