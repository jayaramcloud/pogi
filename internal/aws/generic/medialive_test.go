package generic_test

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/medialive"
	"github.com/aws/aws-sdk-go-v2/service/medialive/types"
	"github.com/nrndev/pogi/internal/aws/generic"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestMediaLive(t *testing.T) {
	prev := generic.SetLifecycleDelay(0) // states settle at once
	t.Cleanup(func() { generic.SetLifecycleDelay(prev) })
	_, url := start(t, "medialive")
	ctx := context.Background()
	c := medialive.NewFromConfig(awstest.Config(t, url))

	o, err := c.CreateChannel(ctx, &medialive.CreateChannelInput{Name: aws.String("live"), RoleArn: aws.String("arn:aws:iam::000000000000:role/ml"),
		ChannelClass: types.ChannelClassSinglePipeline, Tags: map[string]string{"env": "dev"}})
	if err != nil {
		t.Fatal(err)
	}
	ch := o.Channel
	if aws.ToString(ch.Name) != "live" || aws.ToString(ch.Id) == "" || ch.State != types.ChannelStateIdle {
		t.Fatalf("channel: %+v", ch)
	}
	g, err := c.DescribeChannel(ctx, &medialive.DescribeChannelInput{ChannelId: ch.Id})
	if err != nil || aws.ToString(g.Name) != "live" || g.Tags["env"] != "dev" {
		t.Fatalf("describe: %v %+v", err, g)
	}
	l, err := c.ListChannels(ctx, &medialive.ListChannelsInput{})
	if err != nil || len(l.Channels) != 1 {
		t.Fatalf("list: %v %+v", err, l)
	}
	if _, err := c.CreateInputSecurityGroup(ctx, &medialive.CreateInputSecurityGroupInput{WhitelistRules: []types.InputWhitelistRuleCidr{{Cidr: aws.String("10.0.0.0/16")}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.DeleteChannel(ctx, &medialive.DeleteChannelInput{ChannelId: ch.Id}); err != nil {
		t.Fatal(err)
	}
	// a deleted channel stays readable in state DELETED, which Terraform waits for
	if d, err := c.DescribeChannel(ctx, &medialive.DescribeChannelInput{ChannelId: ch.Id}); err != nil || d.State != types.ChannelStateDeleted {
		t.Fatalf("after delete: %v %+v", err, d)
	}
	var nf *types.NotFoundException
	if _, err := c.DescribeChannel(ctx, &medialive.DescribeChannelInput{ChannelId: aws.String("999")}); !errors.As(err, &nf) {
		t.Fatalf("not found: %T %v", err, err)
	}
	if _, err := c.DeleteChannel(ctx, &medialive.DeleteChannelInput{ChannelId: aws.String("999")}); !errors.As(err, &nf) {
		t.Fatalf("delete of an unknown channel: %T %v", err, err)
	}
	if _, err := c.DescribeChannel(ctx, &medialive.DescribeChannelInput{}); err == nil {
		t.Fatal("describe without an id succeeded")
	}
}
