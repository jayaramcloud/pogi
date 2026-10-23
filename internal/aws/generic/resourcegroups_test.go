package generic_test

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/resourcegroups"
	"github.com/aws/aws-sdk-go-v2/service/resourcegroups/types"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestResourceGroups(t *testing.T) {
	_, url := start(t, "resource-groups")
	ctx := context.Background()
	c := resourcegroups.NewFromConfig(awstest.Config(t, url))

	q := &types.ResourceQuery{Type: types.QueryTypeTagFilters10, Query: aws.String(`{"ResourceTypeFilters":["AWS::AllSupported"],"TagFilters":[{"Key":"env","Values":["dev"]}]}`)}
	o, err := c.CreateGroup(ctx, &resourcegroups.CreateGroupInput{Name: aws.String("dev"), Description: aws.String("d"), ResourceQuery: q, Tags: map[string]string{"team": "a"}})
	if err != nil {
		t.Fatal(err)
	}
	arn := aws.ToString(o.Group.GroupArn)
	if arn != "arn:aws:resource-groups:us-east-1:000000000000:group/dev" || aws.ToString(o.Group.Name) != "dev" || o.Tags["team"] != "a" {
		t.Fatalf("create: %+v", o)
	}
	_, err = c.CreateGroup(ctx, &resourcegroups.CreateGroupInput{Name: aws.String("dev"), ResourceQuery: q})
	var bad *types.BadRequestException
	if !errors.As(err, &bad) {
		t.Fatalf("duplicate: %T %v", err, err)
	}
	g, err := c.GetGroup(ctx, &resourcegroups.GetGroupInput{Group: aws.String("dev")})
	if err != nil || aws.ToString(g.Group.GroupArn) != arn || aws.ToString(g.Group.Description) != "d" {
		t.Fatalf("get: %v %+v", err, g)
	}
	gq, err := c.GetGroupQuery(ctx, &resourcegroups.GetGroupQueryInput{Group: aws.String("dev")})
	if err != nil || aws.ToString(gq.GroupQuery.ResourceQuery.Query) != aws.ToString(q.Query) {
		t.Fatalf("query: %v %+v", err, gq)
	}
	if _, err := c.UpdateGroup(ctx, &resourcegroups.UpdateGroupInput{Group: aws.String("dev"), Description: aws.String("e")}); err != nil {
		t.Fatal(err)
	}
	tg, err := c.GetTags(ctx, &resourcegroups.GetTagsInput{Arn: aws.String(arn)})
	if err != nil || tg.Tags["team"] != "a" {
		t.Fatalf("tags: %v %+v", err, tg)
	}
	if _, err := c.Tag(ctx, &resourcegroups.TagInput{Arn: aws.String(arn), Tags: map[string]string{"x": "y"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Untag(ctx, &resourcegroups.UntagInput{Arn: aws.String(arn), Keys: []string{"team"}}); err != nil {
		t.Fatal(err)
	}
	tg, _ = c.GetTags(ctx, &resourcegroups.GetTagsInput{Arn: aws.String(arn)})
	if len(tg.Tags) != 1 || tg.Tags["x"] != "y" {
		t.Fatalf("tags after: %+v", tg.Tags)
	}
	l, err := c.ListGroups(ctx, &resourcegroups.ListGroupsInput{})
	if err != nil || len(l.GroupIdentifiers) != 1 || aws.ToString(l.GroupIdentifiers[0].GroupName) != "dev" {
		t.Fatalf("list: %v %+v", err, l)
	}
	if _, err := c.DeleteGroup(ctx, &resourcegroups.DeleteGroupInput{Group: aws.String("dev")}); err != nil {
		t.Fatal(err)
	}
	var nf *types.NotFoundException
	if _, err := c.GetGroup(ctx, &resourcegroups.GetGroupInput{Group: aws.String("dev")}); !errors.As(err, &nf) {
		t.Fatalf("not found: %T %v", err, err)
	}
	if _, err := c.DeleteGroup(ctx, &resourcegroups.DeleteGroupInput{Group: aws.String("dev")}); !errors.As(err, &nf) {
		t.Fatalf("second delete: %T %v", err, err)
	}
	if _, err := c.CreateGroup(ctx, &resourcegroups.CreateGroupInput{}); err == nil {
		t.Fatal("empty create succeeded")
	}
}
