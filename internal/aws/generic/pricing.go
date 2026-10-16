package generic

import (
	"encoding/base64"
	"slices"
	"strconv"
	"strings"

	"github.com/nrndev/pogi/internal/aws/pricelist"
)

// Pricing: the Price List Query API over the embedded price list. Nothing is
// stored; GetProducts filters the products and returns them as price-list JSON.
func init() {
	Override("pricing", "DescribeServices", func(c *Call) (map[string]any, error) {
		var out []any
		for _, s := range pricelist.Services() {
			if code := str(c.In["ServiceCode"]); code == "" || code == s.Code {
				out = append(out, map[string]any{"ServiceCode": s.Code, "AttributeNames": costStrs(s.AttributeNames())})
			}
		}
		if len(out) == 0 {
			return nil, c.NotFound("Service code %s not found", str(c.In["ServiceCode"]))
		}
		out, next := costPageOf(out, c.In["NextToken"], c.In["MaxResults"], 100)
		return map[string]any{"Services": out, "NextToken": next, "FormatVersion": "aws_v1"}, nil
	})
	Override("pricing", "GetAttributeValues", func(c *Call) (map[string]any, error) {
		s := pricelist.Lookup(str(c.In["ServiceCode"]))
		if s == nil {
			return nil, c.NotFound("Service code %s not found", str(c.In["ServiceCode"]))
		}
		if !slices.Contains(s.AttributeNames(), str(c.In["AttributeName"])) {
			return nil, c.NotFound("Attribute %s not found for service %s", str(c.In["AttributeName"]), s.Code)
		}
		set := map[string]bool{}
		for _, p := range s.Products() {
			set[p.Attrs[str(c.In["AttributeName"])]] = true
		}
		var vals []any
		for _, v := range costSortedKeys(set) {
			if v != "" {
				vals = append(vals, map[string]any{"Value": v})
			}
		}
		vals, next := costPageOf(vals, c.In["NextToken"], c.In["MaxResults"], 100)
		return map[string]any{"AttributeValues": vals, "NextToken": next}, nil
	})
	Override("pricing", "GetProducts", func(c *Call) (map[string]any, error) {
		s := pricelist.Lookup(str(c.In["ServiceCode"]))
		if s == nil {
			return nil, c.NotFound("Service code %s not found", str(c.In["ServiceCode"]))
		}
		filters, _ := c.In["Filters"].([]any)
		var items []any
	next:
		for _, p := range s.Products() {
			for _, f := range filters {
				fm, _ := f.(map[string]any)
				have, want := p.Attrs[str(fm["Field"])], str(fm["Value"])
				ok := false
				switch str(fm["Type"]) {
				case "CONTAINS":
					ok = strings.Contains(have, want)
				case "ANY_OF":
					ok = slices.Contains(strings.Split(want, ","), have)
				case "NONE_OF":
					ok = !slices.Contains(strings.Split(want, ","), have)
				default: // TERM_MATCH, EQUALS
					ok = have == want
				}
				if !ok {
					continue next
				}
			}
			items = append(items, p.JSON())
		}
		items, nextTok := costPageOf(items, c.In["NextToken"], c.In["MaxResults"], 100)
		return map[string]any{"PriceList": items, "NextToken": nextTok, "FormatVersion": "aws_v1"}, nil
	})
	// One price list per region of the service, as files nothing serves (ponytail: GetPriceListFileUrl has no file behind it).
	Override("pricing", "ListPriceLists", func(c *Call) (map[string]any, error) {
		if pricelist.Lookup(str(c.In["ServiceCode"])) == nil {
			return nil, c.NotFound("Service code %s not found", str(c.In["ServiceCode"]))
		}
		var out []any
		for _, r := range pricelist.Regions() {
			if want := str(c.In["RegionCode"]); want == "" || want == r {
				out = append(out, map[string]any{"RegionCode": r, "CurrencyCode": str(c.In["CurrencyCode"]), "FileFormats": []any{"json", "csv"},
					"PriceListArn": "arn:aws:pricing:::price-list/aws/" + str(c.In["ServiceCode"]) + "/" + str(c.In["CurrencyCode"]) + "/" + pricelist.Version() + "/" + r})
			}
		}
		out, next := costPageOf(out, c.In["NextToken"], c.In["MaxResults"], 100)
		return map[string]any{"PriceLists": out, "NextToken": next}, nil
	})
}

func costStrs(l []string) []any {
	out := make([]any, len(l))
	for i, s := range l {
		out[i] = s
	}
	return out
}

func costSortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

// pageOf returns the items a NextToken (an offset, as the engine's own lists make it) and a
// limit select, and the token of the page after.
func costPageOf(items []any, token, limit any, def int) ([]any, any) {
	off := 0
	if b, err := base64.RawURLEncoding.DecodeString(str(token)); err == nil {
		off, _ = strconv.Atoi(string(b))
	}
	n, _ := strconv.Atoi(str(limit))
	if n <= 0 {
		n = def
	}
	off = min(max(off, 0), len(items))
	if off+n >= len(items) {
		return items[off:], nil
	}
	return items[off : off+n], base64.RawURLEncoding.EncodeToString([]byte(strconv.Itoa(off + n)))
}
