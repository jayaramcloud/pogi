package cost

import (
	"strings"
	"testing"
)

func TestListSizes(t *testing.T) {
	db := testDB(t)
	mappings["aws_test_sized"] = mapping{service: "Test", sizes: "test/vm/%s", price: func(r *res) {}}
	mappings["aws_test_nodata"] = mapping{service: "Test", sizes: "nodata/vm/%s", price: func(r *res) {}}
	defer delete(mappings, "aws_test_sized")
	defer delete(mappings, "aws_test_nodata")
	got, region, err := ListSizes(db, SizeQuery{Type: "aws_test_sized", Region: "us-east-1"})
	if err != nil || region != "us-east-1" || len(got) != 2 || got[0].Name != "small" {
		t.Fatalf("all sizes: %v %q %v", got, region, err)
	}
	if got, _, _ = ListSizes(db, SizeQuery{Type: "aws_test_sized", Region: "us-east-1", VCPU: 2}); len(got) != 1 || got[0].Name != "big" {
		t.Fatalf("vcpu filter: %v", got)
	}
	for q, want := range map[SizeQuery]string{
		{Type: "aws_nothing"}:                                      "Types with one: ",
		{Type: "aws_test_sized", Region: "mars"}:                   "unknown test region",
		{Type: "aws_test_sized", Region: "us-east-1", MemGB: 1000}: "no aws_test_sized sizes match",
		{Type: "aws_test_sized", Region: "us-east-1", OS: "beos"}:  "use linux or windows",
		{Type: "aws_test_sized", Region: "us-east-1", VCPU: -1}:    "must not be negative",
		{Type: "aws_test_nodata"}:                                  "pogi has no nodata prices yet",
	} {
		if _, _, err := ListSizes(db, q); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%+v: want %q, got %v", q, want, err)
		}
	}
}
