package cost

import (
	"cmp"
	"fmt"
	"slices"
	"strings"
)

// SizeQuery picks sizes of one resource type.
type SizeQuery struct {
	Type   string  // "aws_instance"
	OS     string  // "linux" (the default) or "windows"
	Region string  // "" means the cloud's usual region
	VCPU   float64 // at least this many
	MemGB  float64 // at least this much
	Old    bool    // include older generations
}

// ListSizes returns the sizes that match q, cheapest first, and the region used. An unknown type
// or region is an error that lists what is known.
func ListSizes(db *DB, q SizeQuery) ([]SizePrice, string, error) {
	if q.OS != "" && q.OS != "linux" && q.OS != "windows" {
		return nil, "", fmt.Errorf("os %q: use linux or windows", q.OS)
	}
	if q.VCPU < 0 || q.MemGB < 0 {
		return nil, "", fmt.Errorf("vCPU and memory must not be negative")
	}
	col, ok := SizeColumn(q.Type, q.OS)
	if !ok {
		return nil, "", fmt.Errorf("%q has no size list. Types with one: %s", q.Type, strings.Join(SizeTypes(), ", "))
	}
	cloud, _, _ := strings.Cut(col, "/")
	region := cmp.Or(q.Region, DefaultRegion[cloud])
	regions := db.Regions(cloud)
	if len(regions) == 0 {
		return nil, "", fmt.Errorf("pogi has no %s prices yet, so it has no %s sizes to list", cmp.Or(CloudName[cloud], cloud), q.Type)
	}
	if !slices.Contains(regions, region) {
		return nil, "", fmt.Errorf("unknown %s region %q. Some regions: %s", cloud, region, strings.Join(regions[:min(8, len(regions))], ", "))
	}
	var out []SizePrice
	for _, s := range db.Sizes(col, region) {
		if (s.Old && !q.Old) || s.VCPU < q.VCPU || s.MemGB < q.MemGB {
			continue
		}
		out = append(out, s)
	}
	if len(out) == 0 {
		return nil, "", fmt.Errorf("no %s sizes match in %s", q.Type, region)
	}
	return out, region, nil
}
