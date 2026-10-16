package generic_test

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/securityhub"
	"github.com/aws/aws-sdk-go-v2/service/securityhub/types"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestSecurityHub(t *testing.T) {
	_, base := start(t, "securityhub")
	ctx := context.Background()
	c := securityhub.NewFromConfig(awstest.Config(t, base))

	// Not enabled yet.
	if _, err := c.DescribeHub(ctx, &securityhub.DescribeHubInput{}); !errors.As(err, new(*types.InvalidAccessException)) {
		t.Fatalf("describe before enable: %T %v", err, err)
	}
	if _, err := c.EnableSecurityHub(ctx, &securityhub.EnableSecurityHubInput{Tags: map[string]string{"env": "dev"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.EnableSecurityHub(ctx, &securityhub.EnableSecurityHubInput{}); !errors.As(err, new(*types.ResourceConflictException)) {
		t.Fatalf("enable twice: %T %v", err, err)
	}
	h, err := c.DescribeHub(ctx, &securityhub.DescribeHubInput{})
	if err != nil || aws.ToString(h.HubArn) != "arn:aws:securityhub:us-east-1:000000000000:hub/default" || aws.ToString(h.SubscribedAt) == "" || !aws.ToBool(h.AutoEnableControls) {
		t.Fatalf("describe hub: %v %+v", err, h)
	}
	if _, err := c.UpdateSecurityHubConfiguration(ctx, &securityhub.UpdateSecurityHubConfigurationInput{AutoEnableControls: aws.Bool(false), ControlFindingGenerator: types.ControlFindingGeneratorStandardControl}); err != nil {
		t.Fatal(err)
	}
	h, _ = c.DescribeHub(ctx, &securityhub.DescribeHubInput{})
	if aws.ToBool(h.AutoEnableControls) || h.ControlFindingGenerator != types.ControlFindingGeneratorStandardControl {
		t.Fatalf("update hub: %+v", h)
	}
	tags, err := c.ListTagsForResource(ctx, &securityhub.ListTagsForResourceInput{ResourceArn: h.HubArn})
	if err != nil || tags.Tags["env"] != "dev" {
		t.Fatalf("tags: %v %+v", err, tags)
	}
	if _, err := c.TagResource(ctx, &securityhub.TagResourceInput{ResourceArn: h.HubArn, Tags: map[string]string{"team": "sec"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.UntagResource(ctx, &securityhub.UntagResourceInput{ResourceArn: h.HubArn, TagKeys: []string{"env"}}); err != nil {
		t.Fatal(err)
	}
	if tags, _ = c.ListTagsForResource(ctx, &securityhub.ListTagsForResourceInput{ResourceArn: h.HubArn}); len(tags.Tags) != 1 || tags.Tags["team"] != "sec" {
		t.Fatalf("tags after: %+v", tags.Tags)
	}

	// Standards: two are on by default; more can be enabled and disabled.
	en, err := c.GetEnabledStandards(ctx, &securityhub.GetEnabledStandardsInput{})
	if err != nil || len(en.StandardsSubscriptions) != 2 {
		t.Fatalf("default standards: %v %+v", err, en)
	}
	all, err := c.DescribeStandards(ctx, &securityhub.DescribeStandardsInput{})
	if err != nil || len(all.Standards) != 5 {
		t.Fatalf("standards: %v %+v", err, all)
	}
	var pci *string
	for _, s := range all.Standards {
		if aws.ToString(s.Name) == "PCI DSS" {
			pci = s.StandardsArn
		}
	}
	bs, err := c.BatchEnableStandards(ctx, &securityhub.BatchEnableStandardsInput{StandardsSubscriptionRequests: []types.StandardsSubscriptionRequest{{StandardsArn: pci}}})
	if err != nil || len(bs.StandardsSubscriptions) != 1 || bs.StandardsSubscriptions[0].StandardsStatus != types.StandardsStatusReady {
		t.Fatalf("enable standard: %v %+v", err, bs)
	}
	if _, err := c.BatchEnableStandards(ctx, &securityhub.BatchEnableStandardsInput{StandardsSubscriptionRequests: []types.StandardsSubscriptionRequest{{StandardsArn: aws.String("arn:aws:securityhub:::bogus")}}}); !errors.As(err, new(*types.InvalidInputException)) {
		t.Fatalf("bogus standard: %T %v", err, err)
	}
	if _, err := c.BatchDisableStandards(ctx, &securityhub.BatchDisableStandardsInput{StandardsSubscriptionArns: []string{aws.ToString(bs.StandardsSubscriptions[0].StandardsSubscriptionArn)}}); err != nil {
		t.Fatal(err)
	}
	if en, _ = c.GetEnabledStandards(ctx, &securityhub.GetEnabledStandardsInput{}); len(en.StandardsSubscriptions) != 2 {
		t.Fatalf("after disable: %+v", en)
	}

	// Products.
	ps, err := c.EnableImportFindingsForProduct(ctx, &securityhub.EnableImportFindingsForProductInput{ProductArn: aws.String("arn:aws:securityhub:us-east-1::product/aws/guardduty")})
	if err != nil {
		t.Fatal(err)
	}
	if l, err := c.ListEnabledProductsForImport(ctx, &securityhub.ListEnabledProductsForImportInput{}); err != nil || len(l.ProductSubscriptions) != 1 || l.ProductSubscriptions[0] != aws.ToString(ps.ProductSubscriptionArn) {
		t.Fatalf("products: %v %+v", err, l)
	}
	if _, err := c.DisableImportFindingsForProduct(ctx, &securityhub.DisableImportFindingsForProductInput{ProductSubscriptionArn: ps.ProductSubscriptionArn}); err != nil {
		t.Fatal(err)
	}

	// Action targets: create, describe, list, update, delete, not found.
	at, err := c.CreateActionTarget(ctx, &securityhub.CreateActionTargetInput{Name: aws.String("notify"), Id: aws.String("notify1"), Description: aws.String("send a message")})
	if err != nil || aws.ToString(at.ActionTargetArn) != "arn:aws:securityhub:us-east-1:000000000000:action/custom/notify1" {
		t.Fatalf("action target: %v %+v", err, at)
	}
	d, err := c.DescribeActionTargets(ctx, &securityhub.DescribeActionTargetsInput{ActionTargetArns: []string{aws.ToString(at.ActionTargetArn)}})
	if err != nil || len(d.ActionTargets) != 1 || aws.ToString(d.ActionTargets[0].Name) != "notify" {
		t.Fatalf("describe action targets: %v %+v", err, d)
	}
	if d, err = c.DescribeActionTargets(ctx, &securityhub.DescribeActionTargetsInput{}); err != nil || len(d.ActionTargets) != 1 {
		t.Fatalf("list action targets: %v %+v", err, d)
	}
	if _, err := c.UpdateActionTarget(ctx, &securityhub.UpdateActionTargetInput{ActionTargetArn: at.ActionTargetArn, Description: aws.String("page someone")}); err != nil {
		t.Fatal(err)
	}
	if d, _ = c.DescribeActionTargets(ctx, &securityhub.DescribeActionTargetsInput{}); aws.ToString(d.ActionTargets[0].Description) != "page someone" {
		t.Fatalf("update action target: %+v", d.ActionTargets)
	}
	if _, err := c.DeleteActionTarget(ctx, &securityhub.DeleteActionTargetInput{ActionTargetArn: at.ActionTargetArn}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.DeleteActionTarget(ctx, &securityhub.DeleteActionTargetInput{ActionTargetArn: at.ActionTargetArn}); !errors.As(err, new(*types.ResourceNotFoundException)) {
		t.Fatalf("delete twice: %T %v", err, err)
	}

	// Insights.
	in, err := c.CreateInsight(ctx, &securityhub.CreateInsightInput{Name: aws.String("open"), GroupByAttribute: aws.String("ResourceId"),
		Filters: &types.AwsSecurityFindingFilters{RecordState: []types.StringFilter{{Value: aws.String("ACTIVE"), Comparison: types.StringFilterComparisonEquals}}}})
	if err != nil {
		t.Fatal(err)
	}
	gi, err := c.GetInsights(ctx, &securityhub.GetInsightsInput{InsightArns: []string{aws.ToString(in.InsightArn)}})
	if err != nil || len(gi.Insights) != 1 || aws.ToString(gi.Insights[0].GroupByAttribute) != "ResourceId" || len(gi.Insights[0].Filters.RecordState) != 1 {
		t.Fatalf("insights: %v %+v", err, gi)
	}
	if _, err := c.UpdateInsight(ctx, &securityhub.UpdateInsightInput{InsightArn: in.InsightArn, Name: aws.String("open findings")}); err != nil {
		t.Fatal(err)
	}
	if gi, _ = c.GetInsights(ctx, &securityhub.GetInsightsInput{}); len(gi.Insights) != 1 || aws.ToString(gi.Insights[0].Name) != "open findings" {
		t.Fatalf("update insight: %+v", gi.Insights)
	}
	if _, err := c.DeleteInsight(ctx, &securityhub.DeleteInsightInput{InsightArn: in.InsightArn}); err != nil {
		t.Fatal(err)
	}

	// Finding aggregator.
	fa, err := c.CreateFindingAggregator(ctx, &securityhub.CreateFindingAggregatorInput{RegionLinkingMode: aws.String("ALL_REGIONS")})
	if err != nil || aws.ToString(fa.FindingAggregatorArn) == "" {
		t.Fatalf("aggregator: %v %+v", err, fa)
	}
	if g, err := c.GetFindingAggregator(ctx, &securityhub.GetFindingAggregatorInput{FindingAggregatorArn: fa.FindingAggregatorArn}); err != nil || aws.ToString(g.RegionLinkingMode) != "ALL_REGIONS" {
		t.Fatalf("get aggregator: %v %+v", err, g)
	}
	if _, err := c.DeleteFindingAggregator(ctx, &securityhub.DeleteFindingAggregatorInput{FindingAggregatorArn: fa.FindingAggregatorArn}); err != nil {
		t.Fatal(err)
	}

	// Disabling removes the hub.
	if _, err := c.DisableSecurityHub(ctx, &securityhub.DisableSecurityHubInput{}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.DescribeHub(ctx, &securityhub.DescribeHubInput{}); !errors.As(err, new(*types.InvalidAccessException)) {
		t.Fatalf("describe after disable: %T %v", err, err)
	}
	if _, err := c.DisableSecurityHub(ctx, &securityhub.DisableSecurityHubInput{}); !errors.As(err, new(*types.InvalidAccessException)) {
		t.Fatalf("disable twice: %T %v", err, err)
	}
}
