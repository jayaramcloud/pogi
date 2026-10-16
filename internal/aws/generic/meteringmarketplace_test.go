package generic_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/marketplacemetering"
	"github.com/aws/aws-sdk-go-v2/service/marketplacemetering/types"
	"github.com/nrndev/pogi/internal/aws/generic"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestMarketplaceMetering(t *testing.T) {
	h, url := start(t, "meteringmarketplace")
	ctx := context.Background()
	mm := marketplacemetering.NewFromConfig(awstest.Config(t, url))
	now := time.Now()

	// a record per product, dimension and hour: the same again is the same record
	meter := func(qty int32, ts time.Time) (*marketplacemetering.MeterUsageOutput, error) {
		return mm.MeterUsage(ctx, &marketplacemetering.MeterUsageInput{ProductCode: aws.String("prod1"), UsageDimension: aws.String("requests"), UsageQuantity: aws.Int32(qty), Timestamp: aws.Time(ts)})
	}
	first, err := meter(5, now)
	if err != nil || aws.ToString(first.MeteringRecordId) == "" {
		t.Fatalf("meter: %v %+v", err, first)
	}
	same, err := meter(5, now)
	if err != nil || aws.ToString(same.MeteringRecordId) != aws.ToString(first.MeteringRecordId) {
		t.Fatalf("same record: %v %+v", err, same)
	}
	var dup *types.DuplicateRequestException
	if _, err := meter(6, now); !errors.As(err, &dup) {
		t.Fatalf("another quantity in the same hour: %T %v", err, err)
	}
	var old *types.TimestampOutOfBoundsException
	if _, err := meter(1, now.Add(-7*time.Hour)); !errors.As(err, &old) {
		t.Fatalf("old timestamp: %T %v", err, err)
	}
	if _, err := mm.MeterUsage(ctx, &marketplacemetering.MeterUsageInput{ProductCode: aws.String("prod1"), UsageDimension: aws.String("seats"), UsageQuantity: aws.Int32(3), Timestamp: aws.Time(now),
		UsageAllocations: []types.UsageAllocation{{AllocatedUsageQuantity: aws.Int32(1)}, {AllocatedUsageQuantity: aws.Int32(1)}}}); err == nil {
		t.Fatal("allocations that do not add up were taken")
	} else {
		var bad *types.InvalidUsageAllocationsException
		if !errors.As(err, &bad) {
			t.Fatalf("allocations: %T %v", err, err)
		}
	}
	if _, err := mm.MeterUsage(ctx, &marketplacemetering.MeterUsageInput{ProductCode: aws.String("prod1"), UsageDimension: aws.String("seats"), UsageQuantity: aws.Int32(3), Timestamp: aws.Time(now),
		UsageAllocations: []types.UsageAllocation{{AllocatedUsageQuantity: aws.Int32(1), Tags: []types.Tag{{Key: aws.String("team"), Value: aws.String("a")}}}, {AllocatedUsageQuantity: aws.Int32(2)}}}); err != nil {
		t.Fatal(err)
	}

	// a batch: new records, a repeat, a duplicate (the same hour with another quantity), a record with no customer, and one too old to take
	rec := func(customer string, qty int32, ts time.Time) types.UsageRecord {
		r := types.UsageRecord{Dimension: aws.String("requests"), Quantity: aws.Int32(qty), Timestamp: aws.Time(ts)}
		if customer != "" {
			r.CustomerIdentifier = aws.String(customer)
		}
		return r
	}
	b, err := mm.BatchMeterUsage(ctx, &marketplacemetering.BatchMeterUsageInput{ProductCode: aws.String("prod1"), UsageRecords: []types.UsageRecord{
		rec("c1", 2, now), rec("c2", 4, now), rec("c1", 2, now), rec("c2", 9, now), rec("", 1, now), rec("c3", 1, now.Add(-8*time.Hour))}})
	if err != nil || len(b.Results) != 5 || len(b.UnprocessedRecords) != 1 {
		t.Fatalf("batch: %v %+v", err, b)
	}
	for i, want := range []types.UsageRecordResultStatus{types.UsageRecordResultStatusSuccess, types.UsageRecordResultStatusSuccess, types.UsageRecordResultStatusSuccess, types.UsageRecordResultStatusDuplicateRecord, types.UsageRecordResultStatusCustomerNotSubscribed} {
		if b.Results[i].Status != want {
			t.Fatalf("result %d: %+v", i, b.Results[i])
		}
	}
	if aws.ToString(b.Results[2].MeteringRecordId) != aws.ToString(b.Results[0].MeteringRecordId) || aws.ToString(b.Results[3].MeteringRecordId) != aws.ToString(b.Results[1].MeteringRecordId) || aws.ToString(b.UnprocessedRecords[0].CustomerIdentifier) != "c3" {
		t.Fatalf("batch ids: %+v", b)
	}

	// the records are there: 2 from MeterUsage, 2 from the batch
	if n := len(h.Service("meteringmarketplace").(*generic.Service).List("000000000000", "us-east-1", "MeteringRecord")); n != 4 {
		t.Fatalf("%d records", n)
	}

	// customers and registration
	c, err := mm.ResolveCustomer(ctx, &marketplacemetering.ResolveCustomerInput{RegistrationToken: aws.String("pogi:prod1:cust-9:123456789012")})
	if err != nil || aws.ToString(c.CustomerIdentifier) != "cust-9" || aws.ToString(c.ProductCode) != "prod1" || aws.ToString(c.CustomerAWSAccountId) != "123456789012" {
		t.Fatalf("resolve: %v %+v", err, c)
	}
	c1, _ := mm.ResolveCustomer(ctx, &marketplacemetering.ResolveCustomerInput{RegistrationToken: aws.String("some-token")})
	c2, _ := mm.ResolveCustomer(ctx, &marketplacemetering.ResolveCustomerInput{RegistrationToken: aws.String("some-token")})
	if aws.ToString(c1.CustomerIdentifier) == "" || aws.ToString(c1.CustomerIdentifier) != aws.ToString(c2.CustomerIdentifier) || len(aws.ToString(c1.CustomerAWSAccountId)) != 12 {
		t.Fatalf("hashed customer: %+v %+v", c1, c2)
	}
	var expired *types.ExpiredTokenException
	if _, err := mm.ResolveCustomer(ctx, &marketplacemetering.ResolveCustomerInput{RegistrationToken: aws.String("expired-1")}); !errors.As(err, &expired) {
		t.Fatalf("expired token: %T %v", err, err)
	}
	r, err := mm.RegisterUsage(ctx, &marketplacemetering.RegisterUsageInput{ProductCode: aws.String("prod1"), PublicKeyVersion: aws.Int32(1), Nonce: aws.String("n")})
	if err != nil || strings.Count(aws.ToString(r.Signature), ".") != 2 || r.PublicKeyRotationTimestamp == nil {
		t.Fatalf("register: %v %+v", err, r)
	}
}
