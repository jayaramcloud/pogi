// Package pricelist is pogi's small embedded AWS price list: on-demand prices
// for a dozen EC2 instance types, S3, Lambda, DynamoDB, RDS and SQS, in a few
// regions. The Pricing API serves it as price-list JSON, and Cost Explorer,
// Budgets and the Pricing Calculator price usage with it.
//
// prices.json holds one price per product for us-east-1 and a multiplier per
// region, so a product in another region costs the base price times that
// region's multiplier (the real list has an independent price per region).
package pricelist

import (
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"
	"sync"
)

//go:embed prices.json
var raw []byte

// Region is one AWS region of the list.
type Region struct {
	Code   string  `json:"-"`
	Name   string  `json:"name"`   // the "location" attribute
	Prefix string  `json:"prefix"` // of usage types outside us-east-1 (EUW1-BoxUsage:t3.micro)
	Mult   float64 `json:"mult"`
}

// Service is one service of the list.
type Service struct {
	Code   string `json:"code"`
	Name   string `json:"name"` // as Cost Explorer calls it
	Family string `json:"family"`
	Unit   string `json:"unit"`

	Attrs map[string]string `json:"attrs"`
	Specs []prodSpec        `json:"products"`
}

type prodSpec struct {
	UsageType string            `json:"usagetype"`
	Operation string            `json:"operation"`
	Family    string            `json:"family"`
	Unit      string            `json:"unit"`
	Price     float64           `json:"price"`
	Attrs     map[string]string `json:"attrs"`
	Desc      string            `json:"desc"`
}

// Product is one price-list entry: a product in a region with its on-demand price.
type Product struct {
	Service   string
	SKU       string
	Region    string
	Family    string
	UsageType string // with the region prefix
	Operation string
	Unit      string
	Price     float64
	Desc      string
	Attrs     map[string]string // every filterable attribute
}

var (
	once      sync.Once
	regions   map[string]Region
	services  []*Service
	published string
)

func load() {
	once.Do(func() {
		var doc struct {
			Published string            `json:"published"`
			Regions   map[string]Region `json:"regions"`
			Services  []*Service        `json:"services"`
		}
		if err := json.Unmarshal(raw, &doc); err != nil {
			panic("pricelist: " + err.Error())
		}
		regions, services, published = doc.Regions, doc.Services, doc.Published
		for c, r := range regions {
			r.Code = c
			regions[c] = r
		}
	})
}

// Version is the publication stamp of the list, as in price-list ARNs.
func Version() string {
	load()
	return strings.NewReplacer("-", "", ":", "", "T", "", "Z", "").Replace(published)
}

// Regions returns the region codes of the list, sorted.
func Regions() []string {
	load()
	out := make([]string, 0, len(regions))
	for c := range regions {
		out = append(out, c)
	}
	sort.Strings(out)
	return out
}

// RegionOf returns a region of the list.
func RegionOf(code string) (Region, bool) {
	load()
	r, ok := regions[code]
	return r, ok
}

// Prefix is the usage-type prefix of a region ("" for us-east-1, EUW1 for eu-west-1).
func Prefix(region string) string {
	load()
	return regions[region].Prefix
}

// SplitUsageType finds the region a usage type names (EUW1-BoxUsage:t3.micro is
// eu-west-1's BoxUsage:t3.micro); a usage type without a prefix is us-east-1's.
func SplitUsageType(ut string) (region, base string) {
	load()
	if p, rest, ok := strings.Cut(ut, "-"); ok {
		for c, r := range regions {
			if r.Prefix != "" && r.Prefix == p {
				return c, rest
			}
		}
	}
	return "us-east-1", ut
}

// Services returns every service of the list, sorted by code.
func Services() []*Service {
	load()
	out := slices.Clone(services)
	sort.Slice(out, func(i, j int) bool { return out[i].Code < out[j].Code })
	return out
}

// Lookup finds a service of the list by its code.
func Lookup(code string) *Service {
	load()
	for _, s := range services {
		if s.Code == code {
			return s
		}
	}
	return nil
}

// ByName finds a service by Cost Explorer's name for it.
func ByName(name string) *Service {
	load()
	for _, s := range services {
		if s.Name == name {
			return s
		}
	}
	return nil
}

// Products returns every product of the service in every region, sorted by SKU.
func (s *Service) Products() []Product {
	load()
	var out []Product
	for _, c := range Regions() {
		for _, p := range s.Specs {
			out = append(out, s.product(p, regions[c]))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].SKU < out[j].SKU })
	return out
}

func (s *Service) product(p prodSpec, r Region) Product {
	ut := p.UsageType
	if r.Prefix != "" {
		ut = r.Prefix + "-" + ut
	}
	family := p.Family
	if family == "" {
		family = s.Family
	}
	unit := p.Unit
	if unit == "" {
		unit = s.Unit
	}
	attrs := map[string]string{"servicecode": s.Code, "location": r.Name, "locationType": "AWS Region", "regionCode": r.Code,
		"usagetype": ut, "operation": p.Operation, "productFamily": family}
	for k, v := range s.Attrs {
		attrs[k] = v
	}
	for k, v := range p.Attrs {
		attrs[k] = v
	}
	h := sha256.Sum256([]byte(s.Code + "|" + r.Code + "|" + p.UsageType + "|" + p.Operation))
	return Product{Service: s.Code, SKU: strings.ToUpper(hex.EncodeToString(h[:8])), Region: r.Code, Family: family, UsageType: ut,
		Operation: p.Operation, Unit: unit, Price: math.Round(p.Price*r.Mult*1e10) / 1e10, Desc: p.Desc, Attrs: attrs}
}

// Price finds the on-demand price of a usage type (with or without a region
// prefix) in a region ("" takes the region the prefix names). operation, when
// not empty, tells apart products that share a usage type (RDS engines). A
// region the list does not have is priced as us-east-1.
func (s *Service) Price(region, usageType, operation string) (Product, bool) {
	load()
	pr, base := SplitUsageType(usageType)
	if region == "" || base != usageType {
		region = pr
	}
	r, ok := regions[region]
	if !ok {
		r = regions["us-east-1"]
		r.Code = region
	}
	for _, p := range s.Specs {
		if p.UsageType == base && (operation == "" || p.Operation == operation) {
			return s.product(p, r), true
		}
	}
	return Product{}, false
}

// AttributeNames lists the attributes the products of a service have, sorted.
func (s *Service) AttributeNames() []string {
	set := map[string]bool{}
	for _, p := range s.Products() {
		for k := range p.Attrs {
			set[k] = true
		}
	}
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// JSON renders the product as a price-list item, the string GetProducts returns.
func (p Product) JSON() string {
	load()
	rate := fmt.Sprintf("%s.%s", p.SKU, "6YS6EN2CT7")
	term := p.SKU + ".JRTCKXETXF"
	item := map[string]any{
		"product":         map[string]any{"productFamily": p.Family, "attributes": p.Attrs, "sku": p.SKU},
		"serviceCode":     p.Service,
		"version":         Version(),
		"publicationDate": published,
		"terms": map[string]any{"OnDemand": map[string]any{term: map[string]any{
			"sku": p.SKU, "effectiveDate": published, "offerTermCode": "JRTCKXETXF", "termAttributes": map[string]any{},
			"priceDimensions": map[string]any{rate: map[string]any{
				"rateCode": rate, "description": p.Desc, "beginRange": "0", "endRange": "Inf", "unit": p.Unit, "appliesTo": []any{},
				"pricePerUnit": map[string]string{"USD": fmt.Sprintf("%.10f", p.Price)},
			}},
		}}},
	}
	b, _ := json.Marshal(item)
	return string(b)
}
