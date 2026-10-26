package web

import (
	"testing"

	"github.com/nrndev/pogi/internal/core"
)

func TestGlobalTableIgnoresRegion(t *testing.T) {
	sc := scope{Account: "000000000000", Region: "us-east-1", Project: all, Subscription: all}
	cols := []string{"Name", "Region", "Account"}
	other := []string{"b", "ap-south-1", "000000000000"}
	if sc.keeps(core.Table{Columns: cols}, other) {
		t.Error("a regional table should hide a row in another region")
	}
	g := core.Table{Columns: cols, Global: true}
	if !sc.keeps(g, other) {
		t.Error("a global table should keep a row in another region")
	}
	if sc.keeps(g, []string{"b", "ap-south-1", "111111111111"}) {
		t.Error("a global table should still drop a row in another account")
	}
}
