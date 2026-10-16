package generic_test

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/detective"
	"github.com/aws/aws-sdk-go-v2/service/detective/types"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestDetective(t *testing.T) {
	_, base := start(t, "detective")
	ctx := context.Background()
	c := detective.NewFromConfig(awstest.Config(t, base))

	g, err := c.CreateGraph(ctx, &detective.CreateGraphInput{Tags: map[string]string{"env": "dev"}})
	if err != nil || aws.ToString(g.GraphArn) == "" {
		t.Fatalf("create: %v %+v", err, g)
	}
	if _, err := c.CreateGraph(ctx, &detective.CreateGraphInput{}); !errors.As(err, new(*types.ConflictException)) {
		t.Fatalf("second graph: %T %v", err, err)
	}
	l, err := c.ListGraphs(ctx, &detective.ListGraphsInput{})
	if err != nil || len(l.GraphList) != 1 || aws.ToString(l.GraphList[0].Arn) != aws.ToString(g.GraphArn) || l.GraphList[0].CreatedTime == nil {
		t.Fatalf("list: %v %+v", err, l)
	}
	tags, err := c.ListTagsForResource(ctx, &detective.ListTagsForResourceInput{ResourceArn: g.GraphArn})
	if err != nil || tags.Tags["env"] != "dev" {
		t.Fatalf("tags: %v %+v", err, tags)
	}
	if _, err := c.TagResource(ctx, &detective.TagResourceInput{ResourceArn: g.GraphArn, Tags: map[string]string{"team": "sec"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.UntagResource(ctx, &detective.UntagResourceInput{ResourceArn: g.GraphArn, TagKeys: []string{"env"}}); err != nil {
		t.Fatal(err)
	}
	if tags, _ = c.ListTagsForResource(ctx, &detective.ListTagsForResourceInput{ResourceArn: g.GraphArn}); len(tags.Tags) != 1 || tags.Tags["team"] != "sec" {
		t.Fatalf("tags after: %+v", tags.Tags)
	}

	// Members: invited, listed, fetched, removed.
	cm, err := c.CreateMembers(ctx, &detective.CreateMembersInput{GraphArn: g.GraphArn, Accounts: []types.Account{
		{AccountId: aws.String("111111111111"), EmailAddress: aws.String("a@example.com")}, {AccountId: aws.String("222222222222"), EmailAddress: aws.String("b@example.com")}}})
	if err != nil || len(cm.Members) != 2 || cm.Members[0].Status != types.MemberStatusInvited {
		t.Fatalf("create members: %v %+v", err, cm)
	}
	lm, err := c.ListMembers(ctx, &detective.ListMembersInput{GraphArn: g.GraphArn})
	if err != nil || len(lm.MemberDetails) != 2 {
		t.Fatalf("list members: %v %+v", err, lm)
	}
	gm, err := c.GetMembers(ctx, &detective.GetMembersInput{GraphArn: g.GraphArn, AccountIds: []string{"111111111111", "999999999999"}})
	if err != nil || len(gm.MemberDetails) != 1 || aws.ToString(gm.MemberDetails[0].EmailAddress) != "a@example.com" || len(gm.UnprocessedAccounts) != 1 {
		t.Fatalf("get members: %v %+v", err, gm)
	}
	if _, err := c.DeleteMembers(ctx, &detective.DeleteMembersInput{GraphArn: g.GraphArn, AccountIds: []string{"111111111111"}}); err != nil {
		t.Fatal(err)
	}
	if lm, _ = c.ListMembers(ctx, &detective.ListMembersInput{GraphArn: g.GraphArn}); len(lm.MemberDetails) != 1 {
		t.Fatalf("members after delete: %+v", lm)
	}

	if _, err := c.DeleteGraph(ctx, &detective.DeleteGraphInput{GraphArn: g.GraphArn}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ListMembers(ctx, &detective.ListMembersInput{GraphArn: g.GraphArn}); !errors.As(err, new(*types.ResourceNotFoundException)) {
		t.Fatalf("list members after delete: %T %v", err, err)
	}
	if _, err := c.DeleteGraph(ctx, &detective.DeleteGraphInput{GraphArn: g.GraphArn}); !errors.As(err, new(*types.ResourceNotFoundException)) {
		t.Fatalf("delete twice: %T %v", err, err)
	}
	if l, _ = c.ListGraphs(ctx, &detective.ListGraphsInput{}); len(l.GraphList) != 0 {
		t.Fatalf("graph left: %+v", l)
	}
}
