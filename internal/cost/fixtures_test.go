package cost

import (
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/nrndev/pogi/internal/terraform"
)

var expectedTotal = regexp.MustCompile(`\*\*Expected monthly total: \$([0-9,.]+)\*\*`)

// TestFixtureTotals prices each examples/cost folder through the real parser and the embedded
// prices, and checks the total against the one in its EXPECTED.md, which was worked out by hand
// from the price list. A price update that moves a total by more than 1% fails here until
// someone checks that fixture again.
func TestFixtureTotals(t *testing.T) {
	dirs, err := filepath.Glob("../../examples/cost/*")
	if err != nil || len(dirs) == 0 {
		t.Fatalf("no fixtures found: %v", err)
	}
	for _, dir := range dirs {
		t.Run(filepath.Base(dir), func(t *testing.T) {
			md, err := os.ReadFile(filepath.Join(dir, "EXPECTED.md"))
			if err != nil {
				t.Fatal(err)
			}
			m := expectedTotal.FindSubmatch(md)
			if m == nil {
				t.Skip("EXPECTED.md has no checked total yet")
			}
			want, err := strconv.ParseFloat(strings.ReplaceAll(string(m[1]), ",", ""), 64)
			if err != nil {
				t.Fatal(err)
			}
			cfg, err := terraform.Load(dir, nil)
			if err != nil {
				t.Fatal(err)
			}
			var opt Options
			if b, err := os.ReadFile(filepath.Join(dir, "usage.yaml")); err == nil {
				if opt.Usage, err = ReadUsage(b); err != nil {
					t.Fatal(err)
				}
			}
			e, err := Price(cfg, opt)
			if err != nil {
				t.Fatal(err)
			}
			if math.Abs(e.Total-want) > 0.01*want {
				t.Errorf("total $%.2f, EXPECTED.md says $%.2f: check the fixture against the price list again", e.Total, want)
			}
			for _, r := range e.Rows {
				if r.Status == Needs || r.Status == Unpriced {
					t.Errorf("%s is %s: %v (a fixture should price fully)", r.Address, r.Status, r.Notes)
				}
			}
		})
	}
}
