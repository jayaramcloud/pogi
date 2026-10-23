package generic_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/xray"
	"github.com/aws/aws-sdk-go-v2/service/xray/types"
	"github.com/nrndev/pogi/internal/awstest"
)

func xrayTraceID(at time.Time, n int) string {
	return fmt.Sprintf("1-%08x-%024x", at.Unix(), n)
}

func xraySeg(trace string, id int, extra map[string]any) string {
	m := map[string]any{"id": fmt.Sprintf("%016x", id), "trace_id": trace, "name": "web", "start_time": 1.7e9, "end_time": 1.7e9 + 2}
	for k, v := range extra {
		if v == nil {
			delete(m, k)
		} else {
			m[k] = v
		}
	}
	b, _ := json.Marshal(m)
	return string(b)
}

func TestXRayTraces(t *testing.T) {
	_, url := start(t, "xray")
	ctx := context.Background()
	c := xray.NewFromConfig(awstest.Config(t, url))
	now := time.Now()
	tid := xrayTraceID(now, 1)

	docs := []string{
		xraySeg(tid, 1, map[string]any{"fault": true, "http": map[string]any{"request": map[string]any{"url": "https://x.test/a", "method": "GET"}, "response": map[string]any{"status": 500}},
			"subsegments": []any{map[string]any{"name": "db", "start_time": 1.7e9, "end_time": 1.7e9 + 1, "throttle": true}}}),
		xraySeg(tid, 2, map[string]any{"parent_id": fmt.Sprintf("%016x", 1), "type": "subsegment", "name": "call", "start_time": 1.7e9 + 1, "end_time": 1.7e9 + 3}),
		"not json",
		`[1]`,
		xraySeg(tid, 3, map[string]any{"id": "xyz"}),
		xraySeg(tid, 4, map[string]any{"trace_id": "1-short"}),
		xraySeg(tid, 5, map[string]any{"name": nil}),
		xraySeg(tid, 6, map[string]any{"start_time": "soon"}),
		xraySeg(tid, 7, map[string]any{"end_time": nil}),
		xraySeg(tid, 8, map[string]any{"name": strings.Repeat("a", 64*1024)}),
	}
	out, err := c.PutTraceSegments(ctx, &xray.PutTraceSegmentsInput{TraceSegmentDocuments: docs})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.UnprocessedTraceSegments) != 8 {
		t.Fatalf("want 8 refused, got %+v", out.UnprocessedTraceSegments)
	}
	for _, u := range out.UnprocessedTraceSegments {
		if aws.ToString(u.ErrorCode) == "" || aws.ToString(u.Message) == "" {
			t.Fatalf("incomplete: %+v", u)
		}
	}
	// A segment still in progress makes the trace partial.
	if _, err := c.PutTraceSegments(ctx, &xray.PutTraceSegmentsInput{TraceSegmentDocuments: []string{xraySeg(tid, 7, map[string]any{"parent_id": fmt.Sprintf("%016x", 1), "end_time": nil, "in_progress": true})}}); err != nil {
		t.Fatal(err)
	}
	win := &xray.GetTraceSummariesInput{StartTime: aws.Time(now.Add(-time.Hour)), EndTime: aws.Time(now.Add(time.Hour))}
	sum, err := c.GetTraceSummaries(ctx, win)
	if err != nil || len(sum.TraceSummaries) != 1 {
		t.Fatalf("summaries: %v %+v", err, sum)
	}
	s := sum.TraceSummaries[0]
	if aws.ToString(s.Id) != tid || !aws.ToBool(s.HasFault) || !aws.ToBool(s.HasThrottle) || aws.ToBool(s.HasError) || !aws.ToBool(s.IsPartial) ||
		aws.ToFloat64(s.Duration) != 3 || aws.ToFloat64(s.ResponseTime) != 2 {
		t.Fatalf("summary: %+v", s)
	}
	if s.Http == nil || aws.ToString(s.Http.HttpURL) != "https://x.test/a" || aws.ToString(s.Http.HttpMethod) != "GET" || aws.ToInt32(s.Http.HttpStatus) != 500 {
		t.Fatalf("http: %+v", s.Http)
	}
	// Sending segment 7 again, finished, replaces it and ends the partial state.
	if _, err := c.PutTraceSegments(ctx, &xray.PutTraceSegmentsInput{TraceSegmentDocuments: []string{xraySeg(tid, 7, map[string]any{"parent_id": fmt.Sprintf("%016x", 1)})}}); err != nil {
		t.Fatal(err)
	}
	if sum, _ = c.GetTraceSummaries(ctx, win); aws.ToBool(sum.TraceSummaries[0].IsPartial) {
		t.Fatal("still partial")
	}

	got, err := c.BatchGetTraces(ctx, &xray.BatchGetTracesInput{TraceIds: []string{tid, xrayTraceID(now, 99)}})
	if err != nil || len(got.Traces) != 1 || len(got.Traces[0].Segments) != 3 {
		t.Fatalf("batch get: %v %+v", err, got)
	}
	if len(got.UnprocessedTraceIds) != 1 || got.UnprocessedTraceIds[0] != xrayTraceID(now, 99) {
		t.Fatalf("unprocessed: %v", got.UnprocessedTraceIds)
	}
	if !strings.Contains(aws.ToString(got.Traces[0].Segments[0].Document), `"name":"web"`) || aws.ToString(got.Traces[0].Segments[0].Id) != fmt.Sprintf("%016x", 1) {
		t.Fatalf("segment: %+v", got.Traces[0].Segments[0])
	}
	if _, err := c.BatchGetTraces(ctx, &xray.BatchGetTracesInput{TraceIds: []string{"a", "b", "c", "d", "e", "f"}}); err == nil {
		t.Fatal("six trace IDs accepted")
	}

	// Window: a trace made two hours ago is outside it, and a reversed window is refused.
	old := xrayTraceID(now.Add(-2*time.Hour), 2)
	if _, err := c.PutTraceSegments(ctx, &xray.PutTraceSegmentsInput{TraceSegmentDocuments: []string{xraySeg(old, 1, nil)}}); err != nil {
		t.Fatal(err)
	}
	if sum, _ = c.GetTraceSummaries(ctx, win); len(sum.TraceSummaries) != 1 {
		t.Fatalf("window: %+v", sum.TraceSummaries)
	}
	wide, _ := c.GetTraceSummaries(ctx, &xray.GetTraceSummariesInput{StartTime: aws.Time(now.Add(-3 * time.Hour)), EndTime: aws.Time(now.Add(time.Hour))})
	if len(wide.TraceSummaries) != 2 || aws.ToString(wide.TraceSummaries[0].Id) != old {
		t.Fatalf("wide window: %+v", wide.TraceSummaries)
	}
	if _, err := c.GetTraceSummaries(ctx, &xray.GetTraceSummariesInput{StartTime: win.EndTime, EndTime: win.StartTime}); err == nil {
		t.Fatal("reversed window accepted")
	}
	if _, err := c.GetTraceSummaries(ctx, &xray.GetTraceSummariesInput{EndTime: win.EndTime}); err == nil {
		t.Fatal("missing StartTime accepted")
	}

	// A filter is refused, never ignored.
	win.FilterExpression = aws.String(`service("web")`)
	if _, err := c.GetTraceSummaries(ctx, win); err == nil || !strings.Contains(err.Error(), "InvalidRequestException") || !strings.Contains(err.Error(), "filter expressions") {
		t.Fatalf("filter: %v", err)
	}
}

func TestXRayPagingAndCap(t *testing.T) {
	_, url := start(t, "xray")
	ctx := context.Background()
	c := xray.NewFromConfig(awstest.Config(t, url))
	now := time.Now()
	for i := 0; i < 1010; i += 50 {
		var docs []string
		for j := i; j < i+50 && j < 1010; j++ {
			docs = append(docs, xraySeg(xrayTraceID(now, j+1), 1, nil))
		}
		if out, err := c.PutTraceSegments(ctx, &xray.PutTraceSegmentsInput{TraceSegmentDocuments: docs}); err != nil || len(out.UnprocessedTraceSegments) != 0 {
			t.Fatalf("put: %v %+v", err, out)
		}
	}
	in := &xray.GetTraceSummariesInput{StartTime: aws.Time(now.Add(-time.Hour)), EndTime: aws.Time(now.Add(time.Hour))}
	seen, pages := map[string]bool{}, 0
	for {
		out, err := c.GetTraceSummaries(ctx, in)
		if err != nil {
			t.Fatal(err)
		}
		pages++
		for _, s := range out.TraceSummaries {
			seen[aws.ToString(s.Id)] = true
		}
		if out.NextToken == nil {
			break
		}
		in.NextToken = out.NextToken
	}
	// The oldest 10 were dropped to stay at the cap of 1000.
	if len(seen) != 1000 || pages != 10 {
		t.Fatalf("saw %d traces in %d pages", len(seen), pages)
	}
	in.NextToken = aws.String("bogus")
	if _, err := c.GetTraceSummaries(ctx, in); err == nil {
		t.Fatal("bad token accepted")
	}
	if _, err := c.PutTraceSegments(ctx, &xray.PutTraceSegmentsInput{}); err == nil {
		t.Fatal("empty put accepted")
	}
}

func TestXRaySampling(t *testing.T) {
	_, url := start(t, "xray")
	ctx := context.Background()
	c := xray.NewFromConfig(awstest.Config(t, url))

	rules, err := c.GetSamplingRules(ctx, &xray.GetSamplingRulesInput{})
	if err != nil || len(rules.SamplingRuleRecords) != 1 {
		t.Fatalf("rules: %v %+v", err, rules)
	}
	d := rules.SamplingRuleRecords[0].SamplingRule
	if aws.ToString(d.RuleName) != "Default" || aws.ToInt32(d.Priority) != 10000 || d.FixedRate != 0.05 || d.ReservoirSize != 1 ||
		aws.ToString(d.ServiceName) != "*" || aws.ToString(d.URLPath) != "*" || aws.ToString(d.ResourceARN) != "*" || aws.ToString(d.RuleARN) == "" {
		t.Fatalf("default rule: %+v", d)
	}

	mk := func(name string, rate float64, prio int32) *xray.CreateSamplingRuleInput {
		return &xray.CreateSamplingRuleInput{SamplingRule: &types.SamplingRule{RuleName: aws.String(name), ResourceARN: aws.String("*"), Priority: aws.Int32(prio), FixedRate: rate,
			ReservoirSize: 2, ServiceName: aws.String("api"), ServiceType: aws.String("*"), Host: aws.String("*"), HTTPMethod: aws.String("GET"), URLPath: aws.String("/*"), Version: aws.Int32(1)},
			Tags: []types.Tag{{Key: aws.String("env"), Value: aws.String("dev")}}}
	}
	made, err := c.CreateSamplingRule(ctx, mk("api", 0.5, 100))
	if err != nil || aws.ToString(made.SamplingRuleRecord.SamplingRule.RuleARN) == "" {
		t.Fatalf("create: %v %+v", err, made)
	}
	for name, in := range map[string]*xray.CreateSamplingRuleInput{"duplicate": mk("api", 0.5, 100), "Default": mk("Default", 0.5, 100), "rate": mk("hi", 1.5, 100), "priority": mk("p", 0.5, 0)} {
		if _, err := c.CreateSamplingRule(ctx, in); err == nil {
			t.Fatalf("%s create succeeded", name)
		}
	}
	if rules, _ = c.GetSamplingRules(ctx, &xray.GetSamplingRulesInput{}); len(rules.SamplingRuleRecords) != 2 || aws.ToString(rules.SamplingRuleRecords[1].SamplingRule.RuleName) != "api" {
		t.Fatalf("rules: %+v", rules.SamplingRuleRecords)
	}
	tags, err := c.ListTagsForResource(ctx, &xray.ListTagsForResourceInput{ResourceARN: made.SamplingRuleRecord.SamplingRule.RuleARN})
	if err != nil || len(tags.Tags) != 1 {
		t.Fatalf("tags: %v %+v", err, tags)
	}

	// Default can be updated; the change sticks.
	up, err := c.UpdateSamplingRule(ctx, &xray.UpdateSamplingRuleInput{SamplingRuleUpdate: &types.SamplingRuleUpdate{RuleName: aws.String("Default"), FixedRate: aws.Float64(0.2)}})
	if err != nil || up.SamplingRuleRecord.SamplingRule.FixedRate != 0.2 || aws.ToInt32(up.SamplingRuleRecord.SamplingRule.Priority) != 10000 {
		t.Fatalf("update default: %v %+v", err, up)
	}
	if _, err := c.UpdateSamplingRule(ctx, &xray.UpdateSamplingRuleInput{SamplingRuleUpdate: &types.SamplingRuleUpdate{RuleName: aws.String("nope")}}); err == nil {
		t.Fatal("update of unknown rule succeeded")
	}
	if _, err := c.UpdateSamplingRule(ctx, &xray.UpdateSamplingRuleInput{SamplingRuleUpdate: &types.SamplingRuleUpdate{RuleName: aws.String("api"), FixedRate: aws.Float64(2)}}); err == nil {
		t.Fatal("bad rate accepted")
	}
	if _, err := c.DeleteSamplingRule(ctx, &xray.DeleteSamplingRuleInput{RuleName: aws.String("Default")}); err == nil || !strings.Contains(err.Error(), "Default") {
		t.Fatalf("delete Default: %v", err)
	}

	tg, err := c.GetSamplingTargets(ctx, &xray.GetSamplingTargetsInput{SamplingStatisticsDocuments: []types.SamplingStatisticsDocument{
		{RuleName: aws.String("api"), ClientID: aws.String("c1c1c1c1c1c1c1c1c1c1c1c1"), RequestCount: 10, SampledCount: 5, Timestamp: aws.Time(time.Now())},
		{RuleName: aws.String("Default"), ClientID: aws.String("c1c1c1c1c1c1c1c1c1c1c1c1"), Timestamp: aws.Time(time.Now())},
		{RuleName: aws.String("ghost"), ClientID: aws.String("c1c1c1c1c1c1c1c1c1c1c1c1"), Timestamp: aws.Time(time.Now())},
	}})
	if err != nil || len(tg.SamplingTargetDocuments) != 2 || len(tg.UnprocessedStatistics) != 1 || aws.ToString(tg.UnprocessedStatistics[0].RuleName) != "ghost" {
		t.Fatalf("targets: %v %+v", err, tg)
	}
	a := tg.SamplingTargetDocuments[0]
	if aws.ToString(a.RuleName) != "api" || a.FixedRate != 0.5 || aws.ToInt32(a.ReservoirQuota) != 2 || aws.ToInt32(a.Interval) != 10 || a.ReservoirQuotaTTL == nil || !a.ReservoirQuotaTTL.After(time.Now()) {
		t.Fatalf("target: %+v", a)
	}
	if tg.SamplingTargetDocuments[1].FixedRate != 0.2 {
		t.Fatalf("default target: %+v", tg.SamplingTargetDocuments[1])
	}

	if _, err := c.DeleteSamplingRule(ctx, &xray.DeleteSamplingRuleInput{RuleARN: made.SamplingRuleRecord.SamplingRule.RuleARN}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.DeleteSamplingRule(ctx, &xray.DeleteSamplingRuleInput{RuleName: aws.String("api")}); err == nil {
		t.Fatal("second delete succeeded")
	}
}

func TestXRayGroupsAndEncryption(t *testing.T) {
	_, url := start(t, "xray")
	ctx := context.Background()
	c := xray.NewFromConfig(awstest.Config(t, url))
	g, err := c.CreateGroup(ctx, &xray.CreateGroupInput{GroupName: aws.String("errors"), FilterExpression: aws.String("fault")})
	if err != nil || aws.ToString(g.Group.GroupName) != "errors" || aws.ToString(g.Group.GroupARN) == "" {
		t.Fatalf("group: %v %+v", err, g)
	}
	if _, err := c.CreateGroup(ctx, &xray.CreateGroupInput{GroupName: aws.String("errors")}); err == nil {
		t.Fatal("duplicate group")
	}
	if _, err := c.GetGroup(ctx, &xray.GetGroupInput{GroupName: aws.String("ghost")}); err == nil {
		t.Fatal("unknown group found")
	}
	if _, err := c.PutEncryptionConfig(ctx, &xray.PutEncryptionConfigInput{Type: types.EncryptionTypeNone}); err != nil {
		t.Fatal(err)
	}
	e, err := c.GetEncryptionConfig(ctx, &xray.GetEncryptionConfigInput{})
	if err != nil || e.EncryptionConfig.Type != types.EncryptionTypeNone || e.EncryptionConfig.Status != types.EncryptionStatusActive {
		t.Fatalf("encryption: %v %+v", err, e)
	}
	if _, err := c.PutEncryptionConfig(ctx, &xray.PutEncryptionConfigInput{Type: types.EncryptionTypeKms}); err == nil {
		t.Fatal("KMS without a key accepted")
	}
	k, err := c.PutEncryptionConfig(ctx, &xray.PutEncryptionConfigInput{Type: types.EncryptionTypeKms, KeyId: aws.String("alias/xray")})
	if err != nil || aws.ToString(k.EncryptionConfig.KeyId) != "alias/xray" {
		t.Fatalf("kms: %v %+v", err, k)
	}
}
