package generic_test

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/marketplaceentitlementservice"
	"github.com/nrndev/pogi/internal/aws/generic"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestMarketplaceEntitlement(t *testing.T) {
	h, url := start(t, "marketplace-entitlement")
	ctx := context.Background()
	me := marketplaceentitlementservice.NewFromConfig(awstest.Config(t, url))
	get := func(in *marketplaceentitlementservice.GetEntitlementsInput) *marketplaceentitlementservice.GetEntitlementsOutput {
		out, err := me.GetEntitlements(ctx, in)
		if err != nil {
			t.Fatalf("GetEntitlements: %v", err)
		}
		return out
	}

	// nothing seeded: the empty list
	if out := get(&marketplaceentitlementservice.GetEntitlementsInput{ProductCode: aws.String("prod1")}); out.Entitlements == nil || len(out.Entitlements) != 0 {
		t.Fatalf("unseeded: %+v", out)
	}

	// seeded entitlements, over CBOR: values of each kind, a timestamp, filters and pages
	exp := time.Now().UTC().AddDate(1, 0, 0).Truncate(time.Second)
	svc := h.Service("marketplace-entitlement").(*generic.Service)
	seed := func(key, product, customer, dim string, value map[string]any) {
		svc.SeedResource("000000000000", "us-east-1", "Entitlement", key, map[string]any{"ProductCode": product, "CustomerIdentifier": customer, "Dimension": dim,
			"CustomerAWSAccountId": "123456789012", "Value": value, "ExpirationDate": exp.Format(time.RFC3339)})
	}
	seed("1", "prod1", "c1", "users", map[string]any{"IntegerValue": 10.0})
	seed("2", "prod1", "c1", "premium", map[string]any{"BooleanValue": true})
	seed("3", "prod1", "c2", "users", map[string]any{"StringValue": "gold"})
	seed("4", "prod2", "c1", "users", map[string]any{"DoubleValue": 0.5})

	all := get(&marketplaceentitlementservice.GetEntitlementsInput{ProductCode: aws.String("prod1")})
	if len(all.Entitlements) != 3 {
		t.Fatalf("prod1: %+v", all.Entitlements)
	}
	e := all.Entitlements[0]
	if aws.ToString(e.Dimension) != "users" || aws.ToString(e.CustomerIdentifier) != "c1" || aws.ToString(e.CustomerAWSAccountId) != "123456789012" || e.Value == nil ||
		aws.ToInt32(e.Value.IntegerValue) != 10 || e.ExpirationDate == nil || !e.ExpirationDate.Equal(exp) {
		t.Fatalf("entitlement: %+v (%+v)", e, e.Value)
	}
	if b := all.Entitlements[1].Value; b == nil || !aws.ToBool(b.BooleanValue) {
		t.Fatalf("boolean: %+v", all.Entitlements[1])
	}
	if s := all.Entitlements[2].Value; s == nil || aws.ToString(s.StringValue) != "gold" {
		t.Fatalf("string: %+v", all.Entitlements[2])
	}
	if d := get(&marketplaceentitlementservice.GetEntitlementsInput{ProductCode: aws.String("prod2")}); len(d.Entitlements) != 1 {
		t.Fatalf("prod2: %+v", d)
	} else if dv := d.Entitlements[0].Value; dv == nil || aws.ToFloat64(dv.DoubleValue) != 0.5 {
		t.Fatalf("double: %+v", d.Entitlements[0])
	}
	f := get(&marketplaceentitlementservice.GetEntitlementsInput{ProductCode: aws.String("prod1"), Filter: map[string][]string{"CUSTOMER_IDENTIFIER": {"c2"}}})
	if len(f.Entitlements) != 1 || aws.ToString(f.Entitlements[0].CustomerIdentifier) != "c2" {
		t.Fatalf("by customer: %+v", f.Entitlements)
	}
	f = get(&marketplaceentitlementservice.GetEntitlementsInput{ProductCode: aws.String("prod1"), Filter: map[string][]string{"DIMENSION": {"users"}, "CUSTOMER_IDENTIFIER": {"c1"}}})
	if len(f.Entitlements) != 1 || aws.ToString(f.Entitlements[0].Dimension) != "users" {
		t.Fatalf("by both: %+v", f.Entitlements)
	}
	var n, pages int
	p := marketplaceentitlementservice.NewGetEntitlementsPaginator(me, &marketplaceentitlementservice.GetEntitlementsInput{ProductCode: aws.String("prod1"), MaxResults: aws.Int32(2)})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			t.Fatal(err)
		}
		n, pages = n+len(page.Entitlements), pages+1
	}
	if n != 3 || pages != 2 {
		t.Fatalf("paged: %d entitlements in %d pages", n, pages)
	}

	// a missing product code is an error
	if _, err := me.GetEntitlements(ctx, &marketplaceentitlementservice.GetEntitlementsInput{ProductCode: aws.String("")}); err == nil {
		t.Fatal("an empty product code was accepted")
	}
}
