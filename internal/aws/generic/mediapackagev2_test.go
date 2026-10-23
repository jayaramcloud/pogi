package generic_test

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/mediapackagev2"
	"github.com/aws/aws-sdk-go-v2/service/mediapackagev2/types"
	"github.com/aws/smithy-go"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestMediaPackageV2(t *testing.T) {
	_, url := start(t, "mediapackagev2")
	ctx := context.Background()
	c := mediapackagev2.NewFromConfig(awstest.Config(t, url))

	g, err := c.CreateChannelGroup(ctx, &mediapackagev2.CreateChannelGroupInput{ChannelGroupName: aws.String("grp"), Tags: map[string]string{"env": "dev"}})
	if err != nil || aws.ToString(g.Arn) == "" {
		t.Fatalf("group: %v %+v", err, g)
	}
	ch, err := c.CreateChannel(ctx, &mediapackagev2.CreateChannelInput{ChannelGroupName: aws.String("grp"), ChannelName: aws.String("ch")})
	if err != nil || aws.ToString(ch.ChannelName) != "ch" {
		t.Fatalf("channel: %v %+v", err, ch)
	}
	if _, err := c.CreateOriginEndpoint(ctx, &mediapackagev2.CreateOriginEndpointInput{ChannelGroupName: aws.String("grp"), ChannelName: aws.String("ch"),
		OriginEndpointName: aws.String("ep"), ContainerType: types.ContainerTypeTs}); err != nil {
		t.Fatal(err)
	}
	gg, err := c.GetChannelGroup(ctx, &mediapackagev2.GetChannelGroupInput{ChannelGroupName: aws.String("grp")})
	if err != nil || aws.ToString(gg.ChannelGroupName) != "grp" || gg.Tags["env"] != "dev" {
		t.Fatalf("get: %v %+v", err, gg)
	}
	l, err := c.ListChannels(ctx, &mediapackagev2.ListChannelsInput{ChannelGroupName: aws.String("grp")})
	if err != nil || len(l.Items) != 1 {
		t.Fatalf("list: %v %+v", err, l)
	}
	if _, err := c.DeleteChannelGroup(ctx, &mediapackagev2.DeleteChannelGroupInput{ChannelGroupName: aws.String("grp")}); err != nil {
		t.Fatal(err)
	}
	var nf *types.ResourceNotFoundException
	if _, err := c.GetChannelGroup(ctx, &mediapackagev2.GetChannelGroupInput{ChannelGroupName: aws.String("grp")}); !errors.As(err, &nf) {
		t.Fatalf("not found: %T %v", err, err)
	}
	var ae smithy.APIError
	if _, err := c.DeleteChannelGroup(ctx, &mediapackagev2.DeleteChannelGroupInput{ChannelGroupName: aws.String("grp")}); !errors.As(err, &ae) || ae.ErrorCode() != "ResourceNotFoundException" {
		t.Fatalf("second delete: %T %v", err, err)
	}
	if _, err := c.CreateChannelGroup(ctx, &mediapackagev2.CreateChannelGroupInput{}); err == nil {
		t.Fatal("empty create succeeded")
	}
}
