package generic_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/bcmpricingcalculator"
	"github.com/aws/aws-sdk-go-v2/service/bcmpricingcalculator/types"
)

func TestBCMPricingCalculator(t *testing.T) {
	_, cfg := startCost(t, "bcm-pricing-calculator")
	ctx := context.Background()
	pc := bcmpricingcalculator.NewFromConfig(cfg)

	// a workload estimate priced from the price list
	w, err := pc.CreateWorkloadEstimate(ctx, &bcmpricingcalculator.CreateWorkloadEstimateInput{Name: aws.String("w1"), Tags: map[string]string{"team": "a"}})
	if err != nil || aws.ToString(w.Id) == "" || w.Status != types.WorkloadEstimateStatusValid || aws.ToFloat64(w.TotalCost) != 0 || w.RateType != types.WorkloadEstimateRateTypeBeforeDiscounts {
		t.Fatalf("create: %v %+v", err, w)
	}
	usage := func(key, svc, ut, op string, amount float64) types.BatchCreateWorkloadEstimateUsageEntry {
		return types.BatchCreateWorkloadEstimateUsageEntry{Key: aws.String(key), ServiceCode: aws.String(svc), UsageType: aws.String(ut), Operation: aws.String(op), UsageAccountId: aws.String(costAcct), Amount: aws.Float64(amount)}
	}
	added, err := pc.BatchCreateWorkloadEstimateUsage(ctx, &bcmpricingcalculator.BatchCreateWorkloadEstimateUsageInput{WorkloadEstimateId: w.Id, Usage: []types.BatchCreateWorkloadEstimateUsageEntry{
		usage("hours", "AmazonEC2", "BoxUsage:t3.micro", "RunInstances", 730),
		usage("puts", "AmazonS3", "Requests-Tier1", "PutObject", 1e6),
		usage("nope", "AmazonNope", "Usage", "Op", 1),
		usage("hours", "AmazonEC2", "BoxUsage:t3.small", "RunInstances", 1),
	}})
	if err != nil || len(added.Items) != 2 || len(added.Errors) != 2 || added.Errors[0].ErrorCode != types.BatchCreateWorkloadEstimateUsageCodeBadRequest ||
		added.Errors[1].ErrorCode != types.BatchCreateWorkloadEstimateUsageCodeConflict {
		t.Fatalf("batch create: %v %+v", err, added)
	}
	if it := added.Items[0]; !costNear(aws.ToFloat64(it.Cost), 730*0.0104) || aws.ToString(it.Location) != "US East (N. Virginia)" || aws.ToString(it.Quantity.Unit) != "Hrs" || it.Status != types.WorkloadEstimateCostStatusValid {
		t.Fatalf("item: %+v", it)
	}
	g, err := pc.GetWorkloadEstimate(ctx, &bcmpricingcalculator.GetWorkloadEstimateInput{Identifier: w.Id})
	if err != nil || !costNear(aws.ToFloat64(g.TotalCost), 730*0.0104+5) || aws.ToString(g.Name) != "w1" {
		t.Fatalf("get: %v %+v", err, g)
	}
	one, err := pc.ListWorkloadEstimateUsage(ctx, &bcmpricingcalculator.ListWorkloadEstimateUsageInput{WorkloadEstimateId: w.Id,
		Filters: []types.ListUsageFilter{{Name: types.ListUsageFilterNameServiceCode, Values: []string{"AmazonS3"}, MatchOption: types.MatchOptionEquals}}})
	if err != nil || len(one.Items) != 1 || aws.ToString(one.Items[0].UsageType) != "Requests-Tier1" {
		t.Fatalf("filtered usage: %v %+v", err, one)
	}
	paged, err := pc.ListWorkloadEstimateUsage(ctx, &bcmpricingcalculator.ListWorkloadEstimateUsageInput{WorkloadEstimateId: w.Id, MaxResults: aws.Int32(1)})
	if err != nil || len(paged.Items) != 1 || paged.NextToken == nil {
		t.Fatalf("paged usage: %v %+v", err, paged)
	}
	upd, err := pc.BatchUpdateWorkloadEstimateUsage(ctx, &bcmpricingcalculator.BatchUpdateWorkloadEstimateUsageInput{WorkloadEstimateId: w.Id, Usage: []types.BatchUpdateWorkloadEstimateUsageEntry{
		{Id: added.Items[0].Id, Amount: aws.Float64(1460)}, {Id: aws.String("00000000-0000-0000-0000-000000000000"), Amount: aws.Float64(1)}}})
	if err != nil || len(upd.Items) != 1 || len(upd.Errors) != 1 || upd.Errors[0].ErrorCode != types.WorkloadEstimateUpdateUsageErrorCodeNotFound {
		t.Fatalf("batch update: %v %+v", err, upd)
	}
	if g, _ = pc.GetWorkloadEstimate(ctx, &bcmpricingcalculator.GetWorkloadEstimateInput{Identifier: w.Id}); !costNear(aws.ToFloat64(g.TotalCost), 1460*0.0104+5) {
		t.Fatalf("total after update %v", aws.ToFloat64(g.TotalCost))
	}
	del, err := pc.BatchDeleteWorkloadEstimateUsage(ctx, &bcmpricingcalculator.BatchDeleteWorkloadEstimateUsageInput{WorkloadEstimateId: w.Id, Ids: []string{aws.ToString(added.Items[0].Id)}})
	if err != nil || len(del.Errors) != 0 {
		t.Fatalf("batch delete: %v %+v", err, del)
	}
	if g, _ = pc.GetWorkloadEstimate(ctx, &bcmpricingcalculator.GetWorkloadEstimateInput{Identifier: w.Id}); !costNear(aws.ToFloat64(g.TotalCost), 5) {
		t.Fatalf("total after delete %v", aws.ToFloat64(g.TotalCost))
	}
	l, err := pc.ListWorkloadEstimates(ctx, &bcmpricingcalculator.ListWorkloadEstimatesInput{})
	if err != nil || len(l.Items) != 1 || aws.ToString(l.Items[0].Name) != "w1" {
		t.Fatalf("list: %v %+v", err, l)
	}
	if u, err := pc.UpdateWorkloadEstimate(ctx, &bcmpricingcalculator.UpdateWorkloadEstimateInput{Identifier: w.Id, Name: aws.String("w2")}); err != nil || aws.ToString(u.Name) != "w2" || !costNear(aws.ToFloat64(u.TotalCost), 5) {
		t.Fatalf("update: %v %+v", err, u)
	}
	arn := "arn:aws:bcm-pricing-calculator::" + costAcct + ":workload-estimate/" + aws.ToString(w.Id)
	if _, err := pc.TagResource(ctx, &bcmpricingcalculator.TagResourceInput{Arn: aws.String(arn), Tags: map[string]string{"env": "dev"}}); err != nil {
		t.Fatal(err)
	}
	tags, err := pc.ListTagsForResource(ctx, &bcmpricingcalculator.ListTagsForResourceInput{Arn: aws.String(arn)})
	if err != nil || len(tags.Tags) != 2 {
		t.Fatalf("tags: %v %+v", err, tags)
	}
	if _, err := pc.UntagResource(ctx, &bcmpricingcalculator.UntagResourceInput{Arn: aws.String(arn), TagKeys: []string{"team"}}); err != nil {
		t.Fatal(err)
	}
	if tags, _ = pc.ListTagsForResource(ctx, &bcmpricingcalculator.ListTagsForResourceInput{Arn: aws.String(arn)}); len(tags.Tags) != 1 || tags.Tags["env"] != "dev" {
		t.Fatalf("tags after: %+v", tags.Tags)
	}
	if _, err := pc.DeleteWorkloadEstimate(ctx, &bcmpricingcalculator.DeleteWorkloadEstimateInput{Identifier: w.Id}); err != nil {
		t.Fatal(err)
	}
	var nf *types.ResourceNotFoundException
	if _, err := pc.GetWorkloadEstimate(ctx, &bcmpricingcalculator.GetWorkloadEstimateInput{Identifier: w.Id}); !errors.As(err, &nf) {
		t.Fatalf("deleted estimate: %T %v", err, err)
	}
	if _, err := pc.BatchCreateWorkloadEstimateUsage(ctx, &bcmpricingcalculator.BatchCreateWorkloadEstimateUsageInput{WorkloadEstimateId: w.Id,
		Usage: []types.BatchCreateWorkloadEstimateUsageEntry{usage("k", "AmazonS3", "Requests-Tier1", "PutObject", 1)}}); !errors.As(err, &nf) {
		t.Fatalf("usage of a deleted estimate: %T %v", err, err)
	}

	// a bill scenario with usage modifications, and the estimate made from it
	sc, err := pc.CreateBillScenario(ctx, &bcmpricingcalculator.CreateBillScenarioInput{Name: aws.String("s1")})
	if err != nil || sc.Status != types.BillScenarioStatusReady || sc.BillInterval == nil {
		t.Fatalf("scenario: %v %+v", err, sc)
	}
	hour := time.Now().UTC().Truncate(time.Hour)
	mods, err := pc.BatchCreateBillScenarioUsageModification(ctx, &bcmpricingcalculator.BatchCreateBillScenarioUsageModificationInput{BillScenarioId: sc.Id, UsageModifications: []types.BatchCreateBillScenarioUsageModificationEntry{
		{Key: aws.String("eu"), ServiceCode: aws.String("AmazonEC2"), UsageType: aws.String("EUW1-BoxUsage:t3.micro"), Operation: aws.String("RunInstances"), UsageAccountId: aws.String(costAcct),
			Amounts: []types.UsageAmount{{StartHour: aws.Time(hour), Amount: aws.Float64(100)}, {StartHour: aws.Time(hour.Add(time.Hour)), Amount: aws.Float64(100)}}},
		{Key: aws.String("bad"), ServiceCode: aws.String("AmazonNope"), UsageType: aws.String("x"), Operation: aws.String("y"), UsageAccountId: aws.String(costAcct)}}})
	if err != nil || len(mods.Items) != 1 || len(mods.Errors) != 1 || len(mods.Items[0].Quantities) != 2 || aws.ToString(mods.Items[0].Location) != "EU (Ireland)" {
		t.Fatalf("modifications: %v %+v", err, mods)
	}
	cm, err := pc.BatchCreateBillScenarioCommitmentModification(ctx, &bcmpricingcalculator.BatchCreateBillScenarioCommitmentModificationInput{BillScenarioId: sc.Id,
		CommitmentModifications: []types.BatchCreateBillScenarioCommitmentModificationEntry{{Key: aws.String("sp"), UsageAccountId: aws.String(costAcct),
			CommitmentAction: &types.BillScenarioCommitmentModificationActionMemberNegateSavingsPlanAction{Value: types.NegateSavingsPlanAction{SavingsPlanId: aws.String("00000000-0000-0000-0000-000000000000")}}}}})
	if err != nil || len(cm.Items) != 1 {
		t.Fatalf("commitments: %v %+v", err, cm)
	}
	lm, err := pc.ListBillScenarioUsageModifications(ctx, &bcmpricingcalculator.ListBillScenarioUsageModificationsInput{BillScenarioId: sc.Id})
	if err != nil || len(lm.Items) != 1 {
		t.Fatalf("list modifications: %v %+v", err, lm)
	}
	if lc, err := pc.ListBillScenarioCommitmentModifications(ctx, &bcmpricingcalculator.ListBillScenarioCommitmentModificationsInput{BillScenarioId: sc.Id}); err != nil || len(lc.Items) != 1 {
		t.Fatalf("list commitments: %v %+v", err, lc)
	}
	if _, err := pc.BatchUpdateBillScenarioUsageModification(ctx, &bcmpricingcalculator.BatchUpdateBillScenarioUsageModificationInput{BillScenarioId: sc.Id, UsageModifications: []types.BatchUpdateBillScenarioUsageModificationEntry{
		{Id: mods.Items[0].Id, Group: aws.String("web"), Amounts: []types.UsageAmount{{StartHour: aws.Time(hour), Amount: aws.Float64(300)}}}}}); err != nil {
		t.Fatal(err)
	}
	be, err := pc.CreateBillEstimate(ctx, &bcmpricingcalculator.CreateBillEstimateInput{BillScenarioId: sc.Id, Name: aws.String("e1")})
	if err != nil || be.Status != types.BillEstimateStatusComplete || be.CostSummary == nil {
		t.Fatalf("estimate: %v %+v", err, be)
	}
	want := 300 * 0.0104 * 1.1 // eu-west-1 costs 10% over us-east-1 in the list
	if got := aws.ToFloat64(be.CostSummary.TotalCostDifference.EstimatedCost.Amount); !costNear(got, want) || len(be.CostSummary.ServiceCostDifferences) != 1 {
		t.Fatalf("estimated cost %v, want %v: %+v", got, want, be.CostSummary)
	}
	items, err := pc.ListBillEstimateLineItems(ctx, &bcmpricingcalculator.ListBillEstimateLineItemsInput{BillEstimateId: be.Id,
		Filters: []types.ListBillEstimateLineItemsFilter{{Name: types.ListBillEstimateLineItemsFilterNameServiceCode, Values: []string{"AmazonEC2"}, MatchOption: types.MatchOptionEquals}}})
	if err != nil || len(items.Items) != 1 || !costNear(aws.ToFloat64(items.Items[0].EstimatedCost.Amount), want) || aws.ToString(items.Items[0].UsageType) != "EUW1-BoxUsage:t3.micro" {
		t.Fatalf("line items: %v %+v", err, items)
	}
	if in, err := pc.ListBillEstimateInputUsageModifications(ctx, &bcmpricingcalculator.ListBillEstimateInputUsageModificationsInput{BillEstimateId: be.Id}); err != nil || len(in.Items) != 1 {
		t.Fatalf("input modifications: %v %+v", err, in)
	}
	if got, err := pc.GetBillEstimate(ctx, &bcmpricingcalculator.GetBillEstimateInput{Identifier: be.Id}); err != nil || aws.ToString(got.Name) != "e1" {
		t.Fatalf("get estimate: %v %+v", err, got)
	}
	if le, err := pc.ListBillEstimates(ctx, &bcmpricingcalculator.ListBillEstimatesInput{}); err != nil || len(le.Items) != 1 {
		t.Fatalf("list estimates: %v %+v", err, le)
	}
	if ls, err := pc.ListBillScenarios(ctx, &bcmpricingcalculator.ListBillScenariosInput{}); err != nil || len(ls.Items) != 1 {
		t.Fatalf("list scenarios: %v %+v", err, ls)
	}
	if u, err := pc.UpdateBillScenario(ctx, &bcmpricingcalculator.UpdateBillScenarioInput{Identifier: sc.Id, Name: aws.String("s2")}); err != nil || aws.ToString(u.Name) != "s2" {
		t.Fatalf("update scenario: %v %+v", err, u)
	}
	if u, err := pc.UpdateBillEstimate(ctx, &bcmpricingcalculator.UpdateBillEstimateInput{Identifier: be.Id, Name: aws.String("e2")}); err != nil || aws.ToString(u.Name) != "e2" {
		t.Fatalf("update estimate: %v %+v", err, u)
	}
	if _, err := pc.BatchDeleteBillScenarioUsageModification(ctx, &bcmpricingcalculator.BatchDeleteBillScenarioUsageModificationInput{BillScenarioId: sc.Id, Ids: []string{aws.ToString(mods.Items[0].Id)}}); err != nil {
		t.Fatal(err)
	}
	if _, err := pc.BatchDeleteBillScenarioCommitmentModification(ctx, &bcmpricingcalculator.BatchDeleteBillScenarioCommitmentModificationInput{BillScenarioId: sc.Id, Ids: []string{aws.ToString(cm.Items[0].Id)}}); err != nil {
		t.Fatal(err)
	}
	if _, err := pc.DeleteBillEstimate(ctx, &bcmpricingcalculator.DeleteBillEstimateInput{Identifier: be.Id}); err != nil {
		t.Fatal(err)
	}
	if _, err := pc.DeleteBillScenario(ctx, &bcmpricingcalculator.DeleteBillScenarioInput{Identifier: sc.Id}); err != nil {
		t.Fatal(err)
	}
	for _, err := range []error{
		func() error {
			_, err := pc.GetBillScenario(ctx, &bcmpricingcalculator.GetBillScenarioInput{Identifier: sc.Id})
			return err
		}(),
		func() error {
			_, err := pc.GetBillEstimate(ctx, &bcmpricingcalculator.GetBillEstimateInput{Identifier: be.Id})
			return err
		}(),
		func() error {
			_, err := pc.CreateBillEstimate(ctx, &bcmpricingcalculator.CreateBillEstimateInput{BillScenarioId: sc.Id, Name: aws.String("again")})
			return err
		}(),
	} {
		if !errors.As(err, &nf) {
			t.Fatalf("after deleting: %T %v", err, err)
		}
	}
}
