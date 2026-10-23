package generic_test

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/resiliencehub"
	"github.com/aws/aws-sdk-go-v2/service/resiliencehub/types"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestResilienceHub(t *testing.T) {
	_, url := start(t, "resiliencehub")
	ctx := context.Background()
	c := resiliencehub.NewFromConfig(awstest.Config(t, url))

	p, err := c.CreateResiliencyPolicy(ctx, &resiliencehub.CreateResiliencyPolicyInput{PolicyName: aws.String("pol"), Tier: types.ResiliencyPolicyTierImportant,
		Policy: map[string]types.FailurePolicy{"AZ": {RpoInSecs: 3600, RtoInSecs: 3600}, "Hardware": {RpoInSecs: 3600, RtoInSecs: 3600}, "Software": {RpoInSecs: 3600, RtoInSecs: 3600}}})
	if err != nil || aws.ToString(p.Policy.PolicyArn) == "" {
		t.Fatalf("policy: %v %+v", err, p)
	}
	a, err := c.CreateApp(ctx, &resiliencehub.CreateAppInput{Name: aws.String("shop"), PolicyArn: p.Policy.PolicyArn, Tags: map[string]string{"env": "dev"}})
	if err != nil {
		t.Fatal(err)
	}
	arn := a.App.AppArn
	g, err := c.DescribeApp(ctx, &resiliencehub.DescribeAppInput{AppArn: arn})
	if err != nil || aws.ToString(g.App.Name) != "shop" || g.App.Tags["env"] != "dev" {
		t.Fatalf("describe: %v %+v", err, g)
	}
	l, err := c.ListApps(ctx, &resiliencehub.ListAppsInput{})
	if err != nil || len(l.AppSummaries) != 1 {
		t.Fatalf("list: %v %+v", err, l)
	}
	if _, err := c.DeleteApp(ctx, &resiliencehub.DeleteAppInput{AppArn: arn}); err != nil {
		t.Fatal(err)
	}
	var nf *types.ResourceNotFoundException
	if _, err := c.DescribeApp(ctx, &resiliencehub.DescribeAppInput{AppArn: arn}); !errors.As(err, &nf) {
		t.Fatalf("not found: %T %v", err, err)
	}
	if _, err := c.DeleteApp(ctx, &resiliencehub.DeleteAppInput{AppArn: arn}); !errors.As(err, &nf) {
		t.Fatalf("second delete: %T %v", err, err)
	}
	if _, err := c.CreateApp(ctx, &resiliencehub.CreateAppInput{}); err == nil {
		t.Fatal("empty create succeeded")
	}
}
