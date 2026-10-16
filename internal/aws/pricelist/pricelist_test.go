package pricelist

import (
	"encoding/json"
	"testing"
)

func TestLookupAndJSON(t *testing.T) {
	ec2 := Lookup("AmazonEC2")
	p, ok := ec2.Price("us-east-1", "BoxUsage:t3.micro", "")
	if !ok || p.Price != 0.0104 || p.Attrs["instanceType"] != "t3.micro" || p.Attrs["location"] != "US East (N. Virginia)" {
		t.Fatalf("t3.micro: %v %+v", ok, p)
	}
	// a region prefix on the usage type names the region
	eu, ok := ec2.Price("", "EUW1-BoxUsage:t3.micro", "")
	if !ok || eu.Region != "eu-west-1" || eu.UsageType != "EUW1-BoxUsage:t3.micro" || eu.Price <= p.Price {
		t.Fatalf("eu: %v %+v", ok, eu)
	}
	// the operation tells apart the engines of an RDS usage type
	rds := Lookup("AmazonRDS")
	my, _ := rds.Price("us-east-1", "InstanceUsage:db.t3.micro", "CreateDBInstance:0002")
	pg, _ := rds.Price("us-east-1", "InstanceUsage:db.t3.micro", "CreateDBInstance:0014")
	if my.Price != 0.017 || pg.Price != 0.018 {
		t.Fatalf("rds: %v %v", my.Price, pg.Price)
	}
	if _, ok := ec2.Price("us-east-1", "BoxUsage:nope", ""); ok {
		t.Fatal("unknown usage type priced")
	}
	var item struct {
		Product struct {
			SKU        string
			Attributes map[string]string
		}
		Terms struct {
			OnDemand map[string]struct {
				PriceDimensions map[string]struct {
					PricePerUnit map[string]string
					Unit         string
				}
			}
		}
	}
	if err := json.Unmarshal([]byte(p.JSON()), &item); err != nil || item.Product.SKU != p.SKU {
		t.Fatalf("json: %v %s", err, p.JSON())
	}
	for _, term := range item.Terms.OnDemand {
		for _, d := range term.PriceDimensions {
			if d.PricePerUnit["USD"] != "0.0104000000" || d.Unit != "Hrs" {
				t.Fatalf("dimension: %+v", d)
			}
		}
	}
	if n := len(ec2.Products()); n != 12*len(Regions()) {
		t.Fatalf("ec2 products: %d", n)
	}
}
