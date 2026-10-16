package generic_test

import (
	"bytes"
	"context"
	"errors"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/costexplorer"
	"github.com/aws/aws-sdk-go-v2/service/costexplorer/types"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/smithy-go"
	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/awstest"
)

// startCost runs pogi with the named services (and no container engine) and returns its SDK config.
func startCost(t *testing.T, names ...string) (*pogi.Running, aws.Config) {
	t.Helper()
	s := pogi.Start(t, pogi.Config{Services: names, DockerHost: "tcp://127.0.0.1:1"})
	return s, awstest.Config(t, s.URL)
}

func costF(t *testing.T, s *string) float64 {
	t.Helper()
	v, err := strconv.ParseFloat(aws.ToString(s), 64)
	if err != nil {
		t.Fatalf("not a number: %q", aws.ToString(s))
	}
	return v
}

func costNear(a, b float64) bool { return math.Abs(a-b) < 1e-9 }

// today is the UTC date, and the period around it that a test asks about.
func costPeriod() *types.DateInterval {
	d := time.Now().UTC().Truncate(24 * time.Hour)
	return &types.DateInterval{Start: aws.String(d.AddDate(0, 0, -1).Format("2006-01-02")), End: aws.String(d.AddDate(0, 0, 2).Format("2006-01-02"))}
}

func TestCostExplorer(t *testing.T) {
	_, cfg := startCost(t, "ce", "s3", "sqs", "ec2")
	ctx := context.Background()
	ce := costexplorer.NewFromConfig(cfg)

	// make some usage: S3 (1 bucket + 3 puts at the Tier1 rate, 2 gets at Tier2), SQS (queue + 4 sends), an EC2 instance
	s3c := s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true })
	if _, err := s3c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("cost-b")}); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"a", "b", "c"} {
		if _, err := s3c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("cost-b"), Key: aws.String(k), Body: bytes.NewReader([]byte("x"))}); err != nil {
			t.Fatal(err)
		}
	}
	for _, k := range []string{"a", "b"} {
		o, err := s3c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("cost-b"), Key: aws.String(k)})
		if err != nil {
			t.Fatal(err)
		}
		o.Body.Close()
	}
	sq := sqs.NewFromConfig(cfg)
	q, err := sq.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("cost-q")})
	if err != nil {
		t.Fatal(err)
	}
	for range 4 {
		if _, err := sq.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: q.QueueUrl, MessageBody: aws.String("m")}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := ec2.NewFromConfig(cfg).RunInstances(ctx, &ec2.RunInstancesInput{ImageId: aws.String("ami-0abcdef1234567890"), InstanceType: ec2types.InstanceTypeT3Micro,
		MinCount: aws.Int32(1), MaxCount: aws.Int32(1)}); err != nil {
		t.Fatal(err)
	}

	// grouped by usage type: every unit is priced from the price list
	by := func(key string, filter *types.Expression) map[string][2]float64 {
		out, err := ce.GetCostAndUsage(ctx, &costexplorer.GetCostAndUsageInput{TimePeriod: costPeriod(), Granularity: types.GranularityDaily,
			Metrics: []string{"UnblendedCost", "UsageQuantity"}, GroupBy: []types.GroupDefinition{{Type: types.GroupDefinitionTypeDimension, Key: aws.String(key)}}, Filter: filter})
		if err != nil {
			t.Fatal(err)
		}
		if len(out.ResultsByTime) != 3 || len(out.GroupDefinitions) != 1 {
			t.Fatalf("results: %d, definitions: %d", len(out.ResultsByTime), len(out.GroupDefinitions))
		}
		sum := map[string][2]float64{}
		for _, r := range out.ResultsByTime {
			if len(r.Total) != 0 {
				t.Fatalf("a grouped result has no total: %v", r.Total)
			}
			for _, g := range r.Groups {
				v := sum[g.Keys[0]]
				sum[g.Keys[0]] = [2]float64{v[0] + costF(t, g.Metrics["UnblendedCost"].Amount), v[1] + costF(t, g.Metrics["UsageQuantity"].Amount)}
			}
		}
		return sum
	}
	u := by("USAGE_TYPE", nil)
	for ut, want := range map[string][2]float64{"Requests-Tier1": {4 * 0.000005, 4}, "Requests-Tier2": {2 * 0.0000004, 2}, "Requests": {5 * 0.0000004, 5}} {
		if got := u[ut]; !costNear(got[0], want[0]) || !costNear(got[1], want[1]) {
			t.Errorf("%s = %v, want %v (all: %v)", ut, got, want, u)
		}
	}
	if g := u["BoxUsage:t3.micro"]; g[1] <= 0 || g[0] <= 0 || math.Abs(g[0]/g[1]-0.0104) > 1e-3 {
		t.Errorf("instance hours %v", g)
	}
	svc := by("SERVICE", nil)
	for _, name := range []string{"Amazon Simple Storage Service", "Amazon Simple Queue Service", "Amazon Elastic Compute Cloud - Compute"} {
		if _, ok := svc[name]; !ok {
			t.Errorf("no %s in %v", name, svc)
		}
	}

	// an expression filter, and a total without groups
	filter := &types.Expression{Dimensions: &types.DimensionValues{Key: types.DimensionService, Values: []string{"Amazon Simple Queue Service"}}}
	out, err := ce.GetCostAndUsage(ctx, &costexplorer.GetCostAndUsageInput{TimePeriod: costPeriod(), Granularity: types.GranularityMonthly,
		Metrics: []string{"UnblendedCost"}, Filter: filter})
	if err != nil {
		t.Fatal(err)
	}
	var sum float64
	for _, r := range out.ResultsByTime {
		sum += costF(t, r.Total["UnblendedCost"].Amount)
		if aws.ToString(r.Total["UnblendedCost"].Unit) != "USD" {
			t.Fatalf("unit %v", r.Total)
		}
	}
	if !costNear(sum, 5*0.0000004) {
		t.Fatalf("sqs total %v", sum)
	}
	if by("REGION", filter)["us-east-1"][1] != 5 {
		t.Fatal("grouped by region")
	}

	// dimension values come from the bill
	dv, err := ce.GetDimensionValues(ctx, &costexplorer.GetDimensionValuesInput{TimePeriod: costPeriod(), Dimension: types.DimensionService, SearchString: aws.String("storage")})
	if err != nil || len(dv.DimensionValues) != 1 || aws.ToString(dv.DimensionValues[0].Value) != "Amazon Simple Storage Service" || aws.ToInt32(dv.TotalSize) != 1 {
		t.Fatalf("dimension values: %v %+v", err, dv)
	}

	// a forecast over the next three days, then as a month
	tomorrow := time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, 1)
	fc, err := ce.GetCostForecast(ctx, &costexplorer.GetCostForecastInput{Granularity: types.GranularityDaily, Metric: types.MetricUnblendedCost,
		TimePeriod: &types.DateInterval{Start: aws.String(tomorrow.Format("2006-01-02")), End: aws.String(tomorrow.AddDate(0, 0, 3).Format("2006-01-02"))}})
	if err != nil || len(fc.ForecastResultsByTime) != 3 || costF(t, fc.Total.Amount) < 0 {
		t.Fatalf("forecast: %v %+v", err, fc)
	}
	var mean float64
	for _, r := range fc.ForecastResultsByTime {
		mean += costF(t, r.MeanValue)
		if costF(t, r.PredictionIntervalLowerBound) > costF(t, r.MeanValue) || costF(t, r.PredictionIntervalUpperBound) < costF(t, r.MeanValue) {
			t.Fatalf("interval: %+v", r)
		}
	}
	if !costNear(mean, costF(t, fc.Total.Amount)) {
		t.Fatalf("total %v is not the sum %v", *fc.Total.Amount, mean)
	}
	if _, err := ce.GetCostForecast(ctx, &costexplorer.GetCostForecastInput{Granularity: types.GranularityDaily, Metric: types.MetricUnblendedCost, TimePeriod: costPeriod()}); err == nil {
		t.Fatal("a forecast of the past succeeded")
	}

	// bad requests
	var ae smithy.APIError
	_, err = ce.GetCostAndUsage(ctx, &costexplorer.GetCostAndUsageInput{TimePeriod: &types.DateInterval{Start: aws.String("2026-02-01"), End: aws.String("2026-01-01")},
		Granularity: types.GranularityDaily, Metrics: []string{"UnblendedCost"}})
	if !errors.As(err, &ae) || ae.ErrorCode() != "ValidationException" {
		t.Fatalf("reversed period: %v", err)
	}
	if _, err = ce.GetCostAndUsage(ctx, &costexplorer.GetCostAndUsageInput{TimePeriod: costPeriod(), Granularity: types.GranularityDaily, Metrics: []string{"Bogus"}}); err == nil {
		t.Fatal("unknown metric accepted")
	}
}

func TestCostExplorerResources(t *testing.T) {
	_, cfg := startCost(t, "ce")
	ctx := context.Background()
	ce := costexplorer.NewFromConfig(cfg)

	// cost category: create, describe, list, update, tag, delete, not found
	rules := []types.CostCategoryRule{{Value: aws.String("prod"), Rule: &types.Expression{Dimensions: &types.DimensionValues{Key: types.DimensionLinkedAccount, Values: []string{"000000000000"}}}}}
	cc, err := ce.CreateCostCategoryDefinition(ctx, &costexplorer.CreateCostCategoryDefinitionInput{Name: aws.String("env"), RuleVersion: types.CostCategoryRuleVersionCostCategoryExpressionV1,
		Rules: rules, ResourceTags: []types.ResourceTag{{Key: aws.String("team"), Value: aws.String("a")}}})
	if err != nil || aws.ToString(cc.CostCategoryArn) == "" || aws.ToString(cc.EffectiveStart) == "" {
		t.Fatalf("create: %v %+v", err, cc)
	}
	arn := cc.CostCategoryArn
	d, err := ce.DescribeCostCategoryDefinition(ctx, &costexplorer.DescribeCostCategoryDefinitionInput{CostCategoryArn: arn})
	if err != nil || aws.ToString(d.CostCategory.Name) != "env" || len(d.CostCategory.Rules) != 1 || aws.ToString(d.CostCategory.Rules[0].Value) != "prod" {
		t.Fatalf("describe: %v %+v", err, d)
	}
	l, err := ce.ListCostCategoryDefinitions(ctx, &costexplorer.ListCostCategoryDefinitionsInput{})
	if err != nil || len(l.CostCategoryReferences) != 1 || l.CostCategoryReferences[0].NumberOfRules != 1 {
		t.Fatalf("list: %v %+v", err, l)
	}
	names, err := ce.GetCostCategories(ctx, &costexplorer.GetCostCategoriesInput{TimePeriod: costPeriod()})
	if err != nil || len(names.CostCategoryNames) != 1 || names.CostCategoryNames[0] != "env" {
		t.Fatalf("names: %v %+v", err, names)
	}
	rules = append(rules, types.CostCategoryRule{Value: aws.String("dev"), Rule: rules[0].Rule})
	if _, err := ce.UpdateCostCategoryDefinition(ctx, &costexplorer.UpdateCostCategoryDefinitionInput{CostCategoryArn: arn, RuleVersion: types.CostCategoryRuleVersionCostCategoryExpressionV1, Rules: rules}); err != nil {
		t.Fatal(err)
	}
	d, _ = ce.DescribeCostCategoryDefinition(ctx, &costexplorer.DescribeCostCategoryDefinitionInput{CostCategoryArn: arn})
	if len(d.CostCategory.Rules) != 2 {
		t.Fatalf("after update: %+v", d.CostCategory)
	}
	if _, err := ce.TagResource(ctx, &costexplorer.TagResourceInput{ResourceArn: arn, ResourceTags: []types.ResourceTag{{Key: aws.String("k"), Value: aws.String("v")}}}); err != nil {
		t.Fatal(err)
	}
	tags, err := ce.ListTagsForResource(ctx, &costexplorer.ListTagsForResourceInput{ResourceArn: arn})
	if err != nil || len(tags.ResourceTags) != 2 {
		t.Fatalf("tags: %v %+v", err, tags)
	}
	if _, err := ce.UntagResource(ctx, &costexplorer.UntagResourceInput{ResourceArn: arn, ResourceTagKeys: []string{"team"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := ce.DeleteCostCategoryDefinition(ctx, &costexplorer.DeleteCostCategoryDefinitionInput{CostCategoryArn: arn}); err != nil {
		t.Fatal(err)
	}
	var nf *types.ResourceNotFoundException
	if _, err := ce.DescribeCostCategoryDefinition(ctx, &costexplorer.DescribeCostCategoryDefinitionInput{CostCategoryArn: arn}); !errors.As(err, &nf) {
		t.Fatalf("not found: %T %v", err, err)
	}

	// anomaly monitor and subscription
	m, err := ce.CreateAnomalyMonitor(ctx, &costexplorer.CreateAnomalyMonitorInput{AnomalyMonitor: &types.AnomalyMonitor{MonitorName: aws.String("svc"),
		MonitorType: types.MonitorTypeDimensional, MonitorDimension: types.MonitorDimensionService}})
	if err != nil || aws.ToString(m.MonitorArn) == "" {
		t.Fatalf("monitor: %v %+v", err, m)
	}
	got, err := ce.GetAnomalyMonitors(ctx, &costexplorer.GetAnomalyMonitorsInput{})
	if err != nil || len(got.AnomalyMonitors) != 1 || aws.ToString(got.AnomalyMonitors[0].MonitorName) != "svc" || got.AnomalyMonitors[0].MonitorDimension != types.MonitorDimensionService {
		t.Fatalf("monitors: %v %+v", err, got)
	}
	if _, err := ce.UpdateAnomalyMonitor(ctx, &costexplorer.UpdateAnomalyMonitorInput{MonitorArn: m.MonitorArn, MonitorName: aws.String("renamed")}); err != nil {
		t.Fatal(err)
	}
	got, _ = ce.GetAnomalyMonitors(ctx, &costexplorer.GetAnomalyMonitorsInput{MonitorArnList: []string{aws.ToString(m.MonitorArn)}})
	if len(got.AnomalyMonitors) != 1 || aws.ToString(got.AnomalyMonitors[0].MonitorName) != "renamed" {
		t.Fatalf("renamed: %+v", got)
	}
	var um *types.UnknownMonitorException
	if _, err := ce.CreateAnomalySubscription(ctx, &costexplorer.CreateAnomalySubscriptionInput{AnomalySubscription: &types.AnomalySubscription{SubscriptionName: aws.String("s"),
		Frequency: types.AnomalySubscriptionFrequencyDaily, MonitorArnList: []string{"arn:aws:ce::000000000000:anomalymonitor/none"}, Subscribers: []types.Subscriber{{Address: aws.String("a@bb.cc"), Type: types.SubscriberTypeEmail}}}}); !errors.As(err, &um) {
		t.Fatalf("subscription of a missing monitor: %T %v", err, err)
	}
	s, err := ce.CreateAnomalySubscription(ctx, &costexplorer.CreateAnomalySubscriptionInput{AnomalySubscription: &types.AnomalySubscription{SubscriptionName: aws.String("s"),
		Frequency: types.AnomalySubscriptionFrequencyDaily, MonitorArnList: []string{aws.ToString(m.MonitorArn)}, Threshold: aws.Float64(10),
		Subscribers: []types.Subscriber{{Address: aws.String("a@bb.cc"), Type: types.SubscriberTypeEmail}}}})
	if err != nil {
		t.Fatal(err)
	}
	subs, err := ce.GetAnomalySubscriptions(ctx, &costexplorer.GetAnomalySubscriptionsInput{MonitorArn: m.MonitorArn})
	if err != nil || len(subs.AnomalySubscriptions) != 1 || aws.ToString(subs.AnomalySubscriptions[0].SubscriptionName) != "s" {
		t.Fatalf("subscriptions: %v %+v", err, subs)
	}
	if _, err := ce.UpdateAnomalySubscription(ctx, &costexplorer.UpdateAnomalySubscriptionInput{SubscriptionArn: s.SubscriptionArn, Frequency: types.AnomalySubscriptionFrequencyWeekly}); err != nil {
		t.Fatal(err)
	}
	subs, _ = ce.GetAnomalySubscriptions(ctx, &costexplorer.GetAnomalySubscriptionsInput{})
	if subs.AnomalySubscriptions[0].Frequency != types.AnomalySubscriptionFrequencyWeekly {
		t.Fatalf("frequency: %+v", subs.AnomalySubscriptions[0])
	}
	if _, err := ce.DeleteAnomalySubscription(ctx, &costexplorer.DeleteAnomalySubscriptionInput{SubscriptionArn: s.SubscriptionArn}); err != nil {
		t.Fatal(err)
	}
	var us *types.UnknownSubscriptionException
	if _, err := ce.DeleteAnomalySubscription(ctx, &costexplorer.DeleteAnomalySubscriptionInput{SubscriptionArn: s.SubscriptionArn}); !errors.As(err, &us) {
		t.Fatalf("deleted subscription: %T %v", err, err)
	}
	if _, err := ce.DeleteAnomalyMonitor(ctx, &costexplorer.DeleteAnomalyMonitorInput{MonitorArn: m.MonitorArn}); err != nil {
		t.Fatal(err)
	}
	if _, err := ce.UpdateAnomalyMonitor(ctx, &costexplorer.UpdateAnomalyMonitorInput{MonitorArn: m.MonitorArn, MonitorName: aws.String("x")}); !errors.As(err, &um) {
		t.Fatalf("deleted monitor: %T %v", err, err)
	}
}
