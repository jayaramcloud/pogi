package generic_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/workspaces"
	"github.com/aws/aws-sdk-go-v2/service/workspaces/types"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestWorkSpaces(t *testing.T) {
	_, url := start(t, "workspaces")
	ctx := context.Background()
	c := workspaces.NewFromConfig(awstest.Config(t, url))

	o, err := c.CreateIpGroup(ctx, &workspaces.CreateIpGroupInput{
		GroupName: aws.String("office"), GroupDesc: aws.String("hq"),
		UserRules: []types.IpRuleItem{{IpRule: aws.String("10.0.0.0/16"), RuleDesc: aws.String("lan")}},
		Tags:      []types.Tag{{Key: aws.String("env"), Value: aws.String("dev")}},
	})
	if err != nil || aws.ToString(o.GroupId) == "" {
		t.Fatalf("create: %v %+v", err, o)
	}
	d, err := c.DescribeIpGroups(ctx, &workspaces.DescribeIpGroupsInput{GroupIds: []string{*o.GroupId}})
	if err != nil || len(d.Result) != 1 || aws.ToString(d.Result[0].GroupName) != "office" || len(d.Result[0].UserRules) != 1 {
		t.Fatalf("describe: %v %+v", err, d)
	}
	tg, err := c.DescribeTags(ctx, &workspaces.DescribeTagsInput{ResourceId: o.GroupId})
	if err != nil || len(tg.TagList) != 1 || aws.ToString(tg.TagList[0].Key) != "env" {
		t.Fatalf("tags: %v %+v", err, tg)
	}
	if _, err := c.UpdateRulesOfIpGroup(ctx, &workspaces.UpdateRulesOfIpGroupInput{GroupId: o.GroupId,
		UserRules: []types.IpRuleItem{{IpRule: aws.String("10.1.0.0/16")}, {IpRule: aws.String("10.2.0.0/16")}}}); err != nil {
		t.Fatal(err)
	}
	if d, _ := c.DescribeIpGroups(ctx, &workspaces.DescribeIpGroupsInput{GroupIds: []string{*o.GroupId}}); len(d.Result) != 1 || len(d.Result[0].UserRules) != 2 {
		t.Fatalf("rules not replaced: %+v", d)
	}
	if _, err := c.CreateIpGroup(ctx, &workspaces.CreateIpGroupInput{}); err == nil {
		t.Fatal("empty create succeeded")
	}
	if _, err := c.DeleteIpGroup(ctx, &workspaces.DeleteIpGroupInput{GroupId: o.GroupId}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.DeleteIpGroup(ctx, &workspaces.DeleteIpGroupInput{GroupId: o.GroupId}); err == nil {
		t.Fatal("second delete succeeded")
	}
	if d, err := c.DescribeIpGroups(ctx, &workspaces.DescribeIpGroupsInput{}); err != nil || len(d.Result) != 0 {
		t.Fatalf("after delete: %v %+v", err, d)
	}
}
