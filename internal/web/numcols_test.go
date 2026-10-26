package web

import (
	"testing"

	"github.com/nrndev/pogi/internal/core"
)

// Sizes and counts line up on the right; an account ID and a word do not.
func TestNumColsSizes(t *testing.T) {
	tb := core.Table{Columns: []string{"Name", "Objects", "Size", "Account", "Region"}}
	rows := [][]string{{"a", "1", "5 B", "000000000000", "us-east-1"}, {"b", "20", "1.2 MiB", "000000000000", "eu"}}
	got := numCols(tb, rows)
	want := []bool{false, true, true, false, false}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("column %s: got %v, want %v", tb.Columns[i], got[i], want[i])
		}
	}
}
