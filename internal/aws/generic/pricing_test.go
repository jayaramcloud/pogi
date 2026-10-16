package generic_test

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/pricing"
	"github.com/aws/aws-sdk-go-v2/service/pricing/types"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestPricing(t *testing.T) {
	_, url := start(t, "pricing")
	ctx := context.Background()
	c := pricing.NewFromConfig(awstest.Config(t, url))

	// every service of the list, then one
	all, err := c.DescribeServices(ctx, &pricing.DescribeServicesInput{})
	if err != nil {
		t.Fatal(err)
	}
	var codes []string
	for _, s := range all.Services {
		codes = append(codes, aws.ToString(s.ServiceCode))
	}
	for _, want := range []string{"AmazonEC2", "AmazonS3", "AWSLambda", "AmazonDynamoDB", "AmazonRDS", "AWSQueueService"} {
		if !slices.Contains(codes, want) {
			t.Fatalf("services %v lack %s", codes, want)
		}
	}
	one, err := c.DescribeServices(ctx, &pricing.DescribeServicesInput{ServiceCode: aws.String("AmazonEC2")})
	if err != nil || len(one.Services) != 1 || !slices.Contains(one.Services[0].AttributeNames, "instanceType") {
		t.Fatalf("ec2: %v %+v", err, one)
	}

	vals, err := c.GetAttributeValues(ctx, &pricing.GetAttributeValuesInput{ServiceCode: aws.String("AmazonEC2"), AttributeName: aws.String("instanceType")})
	if err != nil {
		t.Fatal(err)
	}
	var types_ []string
	for _, v := range vals.AttributeValues {
		types_ = append(types_, aws.ToString(v.Value))
	}
	if len(types_) != 12 || !slices.Contains(types_, "m5.large") {
		t.Fatalf("instance types: %v", types_)
	}

	// TERM_MATCH filters select one product, rendered as a price-list item
	term := func(f, v string) types.Filter {
		return types.Filter{Type: types.FilterTypeTermMatch, Field: aws.String(f), Value: aws.String(v)}
	}
	got, err := c.GetProducts(ctx, &pricing.GetProductsInput{ServiceCode: aws.String("AmazonEC2"),
		Filters: []types.Filter{term("instanceType", "t3.micro"), term("regionCode", "us-east-1")}})
	if err != nil || len(got.PriceList) != 1 {
		t.Fatalf("products: %v %v", err, got)
	}
	var item struct {
		Product struct{ Attributes map[string]string }
		Terms   struct {
			OnDemand map[string]struct {
				PriceDimensions map[string]struct {
					Unit         string
					PricePerUnit map[string]string
				}
			}
		}
	}
	if err := json.Unmarshal([]byte(got.PriceList[0]), &item); err != nil || item.Product.Attributes["location"] != "US East (N. Virginia)" {
		t.Fatalf("item: %v %s", err, got.PriceList[0])
	}
	for _, o := range item.Terms.OnDemand {
		for _, d := range o.PriceDimensions {
			if d.Unit != "Hrs" || d.PricePerUnit["USD"] != "0.0104000000" {
				t.Fatalf("price: %+v", d)
			}
		}
	}

	// paging
	var n int
	p := pricing.NewGetProductsPaginator(c, &pricing.GetProductsInput{ServiceCode: aws.String("AmazonS3"), MaxResults: aws.Int32(5)})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			t.Fatal(err)
		}
		n += len(page.PriceList)
	}
	if want := 4 * 7; n != want {
		t.Fatalf("s3 products: %d, want %d", n, want)
	}

	lists, err := c.ListPriceLists(ctx, &pricing.ListPriceListsInput{ServiceCode: aws.String("AmazonS3"), CurrencyCode: aws.String("USD"), RegionCode: aws.String("eu-west-1"),
		EffectiveDate: aws.Time(time.Now())})
	if err != nil || len(lists.PriceLists) != 1 || aws.ToString(lists.PriceLists[0].RegionCode) != "eu-west-1" {
		t.Fatalf("price lists: %v %+v", err, lists)
	}

	// an unknown service is a typed not-found error
	_, err = c.GetProducts(ctx, &pricing.GetProductsInput{ServiceCode: aws.String("Nope")})
	var nf *types.NotFoundException
	if !errors.As(err, &nf) {
		t.Fatalf("not found: %T %v", err, err)
	}
}
