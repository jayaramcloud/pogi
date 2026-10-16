package generic_test

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/inspector2"
	"github.com/aws/aws-sdk-go-v2/service/inspector2/types"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestInspector2(t *testing.T) {
	_, base := start(t, "inspector2")
	ctx := context.Background()
	c := inspector2.NewFromConfig(awstest.Config(t, base))
	const acct = "000000000000"

	st, err := c.BatchGetAccountStatus(ctx, &inspector2.BatchGetAccountStatusInput{})
	if err != nil || len(st.Accounts) != 1 || st.Accounts[0].State.Status != types.StatusDisabled {
		t.Fatalf("status before enable: %v %+v", err, st)
	}
	en, err := c.Enable(ctx, &inspector2.EnableInput{ResourceTypes: []types.ResourceScanType{types.ResourceScanTypeEc2, types.ResourceScanTypeEcr}})
	if err != nil || len(en.Accounts) != 1 || en.Accounts[0].Status != types.StatusEnabled || en.Accounts[0].ResourceStatus.Ec2 != types.StatusEnabled || en.Accounts[0].ResourceStatus.Lambda != types.StatusDisabled {
		t.Fatalf("enable: %v %+v", err, en)
	}
	st, err = c.BatchGetAccountStatus(ctx, &inspector2.BatchGetAccountStatusInput{AccountIds: []string{acct, "111111111111"}})
	if err != nil || len(st.Accounts) != 2 || st.Accounts[0].State.Status != types.StatusEnabled || st.Accounts[0].ResourceState.Ecr.Status != types.StatusEnabled || st.Accounts[1].State.Status != types.StatusDisabled {
		t.Fatalf("status: %v %+v", err, st)
	}
	if _, err := c.Enable(ctx, &inspector2.EnableInput{ResourceTypes: []types.ResourceScanType{"BOGUS"}}); err == nil {
		t.Fatal("bogus resource type accepted")
	}
	dis, err := c.Disable(ctx, &inspector2.DisableInput{ResourceTypes: []types.ResourceScanType{types.ResourceScanTypeEc2}})
	if err != nil || dis.Accounts[0].Status != types.StatusEnabled || dis.Accounts[0].ResourceStatus.Ec2 != types.StatusDisabled || dis.Accounts[0].ResourceStatus.Ecr != types.StatusEnabled {
		t.Fatalf("disable ec2: %v %+v", err, dis)
	}
	if dis, err = c.Disable(ctx, &inspector2.DisableInput{}); err != nil || dis.Accounts[0].Status != types.StatusDisabled {
		t.Fatalf("disable all: %v %+v", err, dis)
	}
	if p, err := c.ListAccountPermissions(ctx, &inspector2.ListAccountPermissionsInput{}); err != nil || len(p.Permissions) == 0 {
		t.Fatalf("permissions: %v %+v", err, p)
	}

	// Suppression filters.
	crit := &types.FilterCriteria{Severity: []types.StringFilter{{Comparison: types.StringComparisonEquals, Value: aws.String("LOW")}}}
	f, err := c.CreateFilter(ctx, &inspector2.CreateFilterInput{Name: aws.String("quiet"), Action: types.FilterActionSuppress, FilterCriteria: crit, Description: aws.String("low severity"),
		Tags: map[string]string{"env": "dev"}})
	if err != nil || aws.ToString(f.Arn) == "" {
		t.Fatalf("create filter: %v %+v", err, f)
	}
	l, err := c.ListFilters(ctx, &inspector2.ListFiltersInput{})
	if err != nil || len(l.Filters) != 1 || aws.ToString(l.Filters[0].Name) != "quiet" || aws.ToString(l.Filters[0].OwnerId) != acct || len(l.Filters[0].Criteria.Severity) != 1 || l.Filters[0].Tags["env"] != "dev" {
		t.Fatalf("list filters: %v %+v", err, l)
	}
	if l, _ = c.ListFilters(ctx, &inspector2.ListFiltersInput{Action: types.FilterActionNone}); len(l.Filters) != 0 {
		t.Fatalf("filter by action: %+v", l.Filters)
	}
	if _, err := c.UpdateFilter(ctx, &inspector2.UpdateFilterInput{FilterArn: f.Arn, Description: aws.String("lows")}); err != nil {
		t.Fatal(err)
	}
	if l, _ = c.ListFilters(ctx, &inspector2.ListFiltersInput{Arns: []string{aws.ToString(f.Arn)}}); len(l.Filters) != 1 || aws.ToString(l.Filters[0].Description) != "lows" || aws.ToString(l.Filters[0].Name) != "quiet" {
		t.Fatalf("update filter: %+v", l.Filters)
	}
	if _, err := c.TagResource(ctx, &inspector2.TagResourceInput{ResourceArn: f.Arn, Tags: map[string]string{"team": "sec"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.UntagResource(ctx, &inspector2.UntagResourceInput{ResourceArn: f.Arn, TagKeys: []string{"env"}}); err != nil {
		t.Fatal(err)
	}
	tags, err := c.ListTagsForResource(ctx, &inspector2.ListTagsForResourceInput{ResourceArn: f.Arn})
	if err != nil || len(tags.Tags) != 1 || tags.Tags["team"] != "sec" {
		t.Fatalf("tags: %v %+v", err, tags)
	}
	if _, err := c.DeleteFilter(ctx, &inspector2.DeleteFilterInput{Arn: f.Arn}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.DeleteFilter(ctx, &inspector2.DeleteFilterInput{Arn: f.Arn}); !errors.As(err, new(*types.ResourceNotFoundException)) {
		t.Fatalf("delete twice: %T %v", err, err)
	}
	if _, err := c.UpdateFilter(ctx, &inspector2.UpdateFilterInput{FilterArn: f.Arn, Name: aws.String("x")}); !errors.As(err, new(*types.ResourceNotFoundException)) {
		t.Fatalf("update deleted: %T %v", err, err)
	}
}
