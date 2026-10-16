package generic_test

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/controlcatalog"
	"github.com/aws/aws-sdk-go-v2/service/controlcatalog/types"
	"github.com/nrndev/pogi/internal/awstest"
)

// The catalog is read-only: it is browsed, narrowed by domain and objective, and a control is read by ARN.
func TestControlCatalog(t *testing.T) {
	_, base := start(t, "controlcatalog")
	ctx := context.Background()
	c := controlcatalog.NewFromConfig(awstest.Config(t, base))

	ds, err := c.ListDomains(ctx, &controlcatalog.ListDomainsInput{})
	if err != nil || len(ds.Domains) != 2 || aws.ToString(ds.Domains[0].Name) != "Security" || ds.Domains[0].CreateTime == nil {
		t.Fatalf("domains: %v %+v", err, ds)
	}
	os, err := c.ListObjectives(ctx, &controlcatalog.ListObjectivesInput{ObjectiveFilter: &types.ObjectiveFilter{Domains: []types.DomainResourceFilter{{Arn: ds.Domains[0].Arn}}}})
	if err != nil || len(os.Objectives) != 2 || aws.ToString(os.Objectives[0].Domain.Arn) != aws.ToString(ds.Domains[0].Arn) {
		t.Fatalf("objectives: %v %+v", err, os)
	}
	all, _ := c.ListObjectives(ctx, &controlcatalog.ListObjectivesInput{})
	if len(all.Objectives) != 3 {
		t.Fatalf("all objectives: %+v", all.Objectives)
	}
	cc, err := c.ListCommonControls(ctx, &controlcatalog.ListCommonControlsInput{CommonControlFilter: &types.CommonControlFilter{Objectives: []types.ObjectiveResourceFilter{{Arn: os.Objectives[1].Arn}}}})
	if err != nil || len(cc.CommonControls) != 1 || aws.ToString(cc.CommonControls[0].Objective.Arn) != aws.ToString(os.Objectives[1].Arn) {
		t.Fatalf("common controls: %v %+v", err, cc)
	}
	cs, err := c.ListControls(ctx, &controlcatalog.ListControlsInput{})
	if err != nil || len(cs.Controls) == 0 {
		t.Fatalf("controls: %v %+v", err, cs)
	}
	g, err := c.GetControl(ctx, &controlcatalog.GetControlInput{ControlArn: cs.Controls[0].Arn})
	if err != nil || aws.ToString(g.Arn) != aws.ToString(cs.Controls[0].Arn) || g.Behavior == "" || g.RegionConfiguration == nil || g.Implementation == nil {
		t.Fatalf("get control: %v %+v", err, g)
	}
	if _, err := c.GetControl(ctx, &controlcatalog.GetControlInput{ControlArn: aws.String("arn:aws:controlcatalog:::control/nope")}); !errors.As(err, new(*types.ResourceNotFoundException)) {
		t.Fatalf("unknown control: %T %v", err, err)
	}
	if m, err := c.ListControlMappings(ctx, &controlcatalog.ListControlMappingsInput{}); err != nil || m.ControlMappings == nil {
		t.Fatalf("mappings: %v %+v", err, m)
	}
}
