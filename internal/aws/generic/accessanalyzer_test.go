package generic_test

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/accessanalyzer"
	"github.com/aws/aws-sdk-go-v2/service/accessanalyzer/types"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestAccessAnalyzer(t *testing.T) {
	_, base := start(t, "accessanalyzer")
	ctx := context.Background()
	c := accessanalyzer.NewFromConfig(awstest.Config(t, base))

	a, err := c.CreateAnalyzer(ctx, &accessanalyzer.CreateAnalyzerInput{AnalyzerName: aws.String("acct"), Type: types.TypeAccount, Tags: map[string]string{"env": "dev"},
		ArchiveRules: []types.InlineArchiveRule{{RuleName: aws.String("known"), Filter: map[string]types.Criterion{"principal.AWS": {Eq: []string{"111111111111"}}}}}})
	if err != nil || aws.ToString(a.Arn) != "arn:aws:access-analyzer:us-east-1:000000000000:analyzer/acct" {
		t.Fatalf("create: %v %+v", err, a)
	}
	if _, err := c.CreateAnalyzer(ctx, &accessanalyzer.CreateAnalyzerInput{AnalyzerName: aws.String("acct"), Type: types.TypeAccount}); !errors.As(err, new(*types.ConflictException)) {
		t.Fatalf("duplicate: %T %v", err, err)
	}
	g, err := c.GetAnalyzer(ctx, &accessanalyzer.GetAnalyzerInput{AnalyzerName: aws.String("acct")})
	if err != nil || aws.ToString(g.Analyzer.Name) != "acct" || g.Analyzer.Status != types.AnalyzerStatusActive || g.Analyzer.Type != types.TypeAccount || g.Analyzer.Tags["env"] != "dev" || g.Analyzer.CreatedAt == nil {
		t.Fatalf("get: %v %+v", err, g)
	}
	if _, err := c.CreateAnalyzer(ctx, &accessanalyzer.CreateAnalyzerInput{AnalyzerName: aws.String("org"), Type: types.TypeOrganization}); err != nil {
		t.Fatal(err)
	}
	l, err := c.ListAnalyzers(ctx, &accessanalyzer.ListAnalyzersInput{})
	if err != nil || len(l.Analyzers) != 2 {
		t.Fatalf("list: %v %+v", err, l)
	}
	if l, _ = c.ListAnalyzers(ctx, &accessanalyzer.ListAnalyzersInput{Type: types.TypeOrganization}); len(l.Analyzers) != 1 || aws.ToString(l.Analyzers[0].Name) != "org" {
		t.Fatalf("list by type: %+v", l.Analyzers)
	}
	if _, err := c.UpdateAnalyzer(ctx, &accessanalyzer.UpdateAnalyzerInput{AnalyzerName: aws.String("org"),
		Configuration: &types.AnalyzerConfigurationMemberUnusedAccess{Value: types.UnusedAccessConfiguration{UnusedAccessAge: aws.Int32(90)}}}); err != nil {
		t.Fatal(err)
	}
	tags, err := c.ListTagsForResource(ctx, &accessanalyzer.ListTagsForResourceInput{ResourceArn: a.Arn})
	if err != nil || tags.Tags["env"] != "dev" {
		t.Fatalf("tags: %v %+v", err, tags)
	}
	if _, err := c.TagResource(ctx, &accessanalyzer.TagResourceInput{ResourceArn: a.Arn, Tags: map[string]string{"team": "sec"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.UntagResource(ctx, &accessanalyzer.UntagResourceInput{ResourceArn: a.Arn, TagKeys: []string{"env"}}); err != nil {
		t.Fatal(err)
	}
	if tags, _ = c.ListTagsForResource(ctx, &accessanalyzer.ListTagsForResourceInput{ResourceArn: a.Arn}); len(tags.Tags) != 1 || tags.Tags["team"] != "sec" {
		t.Fatalf("tags after: %+v", tags.Tags)
	}

	// Archive rules: the inline one, then create, get, list, update, delete.
	if _, err := c.CreateArchiveRule(ctx, &accessanalyzer.CreateArchiveRuleInput{AnalyzerName: aws.String("acct"), RuleName: aws.String("internal"),
		Filter: map[string]types.Criterion{"isPublic": {Eq: []string{"false"}}}}); err != nil {
		t.Fatal(err)
	}
	r, err := c.GetArchiveRule(ctx, &accessanalyzer.GetArchiveRuleInput{AnalyzerName: aws.String("acct"), RuleName: aws.String("internal")})
	if err != nil || aws.ToString(r.ArchiveRule.RuleName) != "internal" || len(r.ArchiveRule.Filter["isPublic"].Eq) != 1 || r.ArchiveRule.CreatedAt == nil {
		t.Fatalf("get rule: %v %+v", err, r)
	}
	lr, err := c.ListArchiveRules(ctx, &accessanalyzer.ListArchiveRulesInput{AnalyzerName: aws.String("acct")})
	if err != nil || len(lr.ArchiveRules) != 2 {
		t.Fatalf("list rules: %v %+v", err, lr)
	}
	if _, err := c.UpdateArchiveRule(ctx, &accessanalyzer.UpdateArchiveRuleInput{AnalyzerName: aws.String("acct"), RuleName: aws.String("internal"),
		Filter: map[string]types.Criterion{"isPublic": {Eq: []string{"true"}}}}); err != nil {
		t.Fatal(err)
	}
	if r, _ = c.GetArchiveRule(ctx, &accessanalyzer.GetArchiveRuleInput{AnalyzerName: aws.String("acct"), RuleName: aws.String("internal")}); r.ArchiveRule.Filter["isPublic"].Eq[0] != "true" {
		t.Fatalf("update rule: %+v", r.ArchiveRule)
	}
	if _, err := c.DeleteArchiveRule(ctx, &accessanalyzer.DeleteArchiveRuleInput{AnalyzerName: aws.String("acct"), RuleName: aws.String("internal")}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.GetArchiveRule(ctx, &accessanalyzer.GetArchiveRuleInput{AnalyzerName: aws.String("acct"), RuleName: aws.String("internal")}); !errors.As(err, new(*types.ResourceNotFoundException)) {
		t.Fatalf("rule after delete: %T %v", err, err)
	}
	if lr, _ = c.ListArchiveRules(ctx, &accessanalyzer.ListArchiveRulesInput{AnalyzerName: aws.String("acct")}); len(lr.ArchiveRules) != 1 {
		t.Fatalf("rules after delete: %+v", lr.ArchiveRules)
	}

	if _, err := c.DeleteAnalyzer(ctx, &accessanalyzer.DeleteAnalyzerInput{AnalyzerName: aws.String("acct")}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.GetAnalyzer(ctx, &accessanalyzer.GetAnalyzerInput{AnalyzerName: aws.String("acct")}); !errors.As(err, new(*types.ResourceNotFoundException)) {
		t.Fatalf("get after delete: %T %v", err, err)
	}
	if _, err := c.ListArchiveRules(ctx, &accessanalyzer.ListArchiveRulesInput{AnalyzerName: aws.String("acct")}); !errors.As(err, new(*types.ResourceNotFoundException)) {
		t.Fatalf("rules of a deleted analyzer: %T %v", err, err)
	}
}
