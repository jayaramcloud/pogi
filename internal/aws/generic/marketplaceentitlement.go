package generic

import (
	"slices"
	"time"
)

// Marketplace Entitlement: GetEntitlements lists the Entitlement resources
// seeded for the account (Service.SeedResource): there are none until a test or a
// seeding step adds them, and then the answer is the empty list. A seeded
// entitlement keeps what the API returns for it: ProductCode, Dimension,
// CustomerIdentifier, CustomerAWSAccountId, LicenseArn, ExpirationDate and a
// Value of IntegerValue, DoubleValue, BooleanValue or StringValue. Filter
// narrows by CUSTOMER_IDENTIFIER and DIMENSION. The service speaks Smithy
// RPCv2 CBOR, as the Go SDK does for it.
func init() {
	Override("marketplace-entitlement", "GetEntitlements", func(c *Call) (map[string]any, error) {
		product := str(c.In["ProductCode"])
		if product == "" {
			return nil, c.Invalid("ProductCode is required")
		}
		filter := costCmap(c.In["Filter"])
		var items []any
		for _, r := range c.List("Entitlement") {
			if str(r.Data["ProductCode"]) != product {
				continue
			}
			ok := true
			for name, field := range map[string]string{"CUSTOMER_IDENTIFIER": "CustomerIdentifier", "DIMENSION": "Dimension"} {
				if want := asList(filter[name]); len(want) > 0 {
					ok = ok && slices.ContainsFunc(want, func(v any) bool { return str(v) == str(r.Data[field]) })
				}
			}
			if ok {
				items = append(items, r.Data)
			}
		}
		page, next := costPageOf(items, c.In["NextToken"], c.In["MaxResults"], 25)
		return map[string]any{"Entitlements": costOrEmpty(page), "NextToken": next}, nil
	})
}

// Seed stores a resource of a type under its key (the identifiers joined with "/"),
// replacing one that is there. data is in the model's member names, as the API
// returns it: it is how tests and seeding give a service something that no API
// call of its own can create (GetEntitlements has no create).
func (s *Service) SeedResource(account, region, typ, key string, data map[string]any) {
	now := time.Now().UTC()
	r := &Resource{Type: typ, Key: key, ID: key, Account: account, Region: s.region(region), Data: data, Created: now, Updated: now}
	s.mu.Lock()
	s.st.Res[r.skey()] = r
	s.mu.Unlock()
}
