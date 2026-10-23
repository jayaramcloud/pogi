package generic_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ivs"
	"github.com/aws/aws-sdk-go-v2/service/ivs/types"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestIVS(t *testing.T) {
	_, url := start(t, "ivs")
	ctx := context.Background()
	c := ivs.NewFromConfig(awstest.Config(t, url))

	rec, err := c.CreateRecordingConfiguration(ctx, &ivs.CreateRecordingConfigurationInput{Name: aws.String("rec"),
		DestinationConfiguration: &types.DestinationConfiguration{S3: &types.S3DestinationConfiguration{BucketName: aws.String("recordings")}}})
	if err != nil || rec.RecordingConfiguration.State != types.RecordingConfigurationStateActive || aws.ToString(rec.RecordingConfiguration.Arn) == "" {
		t.Fatalf("recording: %v %+v", err, rec)
	}
	if _, err := c.CreateRecordingConfiguration(ctx, &ivs.CreateRecordingConfigurationInput{Name: aws.String("bad")}); err == nil {
		t.Fatal("recording without a destination succeeded")
	}

	ch, err := c.CreateChannel(ctx, &ivs.CreateChannelInput{Name: aws.String("live"), LatencyMode: types.ChannelLatencyModeLowLatency,
		RecordingConfigurationArn: rec.RecordingConfiguration.Arn, Tags: map[string]string{"env": "dev"}})
	if err != nil || ch.Channel == nil || aws.ToString(ch.Channel.Name) != "live" || ch.StreamKey == nil || aws.ToString(ch.StreamKey.Value) == "" {
		t.Fatalf("channel: %v %+v", err, ch)
	}
	g, err := c.GetChannel(ctx, &ivs.GetChannelInput{Arn: ch.Channel.Arn})
	if err != nil || aws.ToString(g.Channel.RecordingConfigurationArn) != aws.ToString(rec.RecordingConfiguration.Arn) || g.Channel.LatencyMode != types.ChannelLatencyModeLowLatency {
		t.Fatalf("get: %v %+v", err, g)
	}
	if g.Channel.Type != types.ChannelTypeStandardChannelType || aws.ToString(g.Channel.PlaybackUrl) == "" || aws.ToString(g.Channel.IngestEndpoint) == "" {
		t.Fatalf("defaults: %+v", g.Channel)
	}
	ks, err := c.ListStreamKeys(ctx, &ivs.ListStreamKeysInput{ChannelArn: ch.Channel.Arn})
	if err != nil || len(ks.StreamKeys) != 1 || aws.ToString(ks.StreamKeys[0].Arn) != aws.ToString(ch.StreamKey.Arn) {
		t.Fatalf("stream keys: %v %+v", err, ks)
	}
	l, err := c.ListChannels(ctx, &ivs.ListChannelsInput{})
	if err != nil || len(l.Channels) != 1 {
		t.Fatalf("list: %v %+v", err, l)
	}
	if _, err := c.GetChannel(ctx, &ivs.GetChannelInput{Arn: aws.String("arn:aws:ivs:us-east-1:000000000000:channel/nope")}); err == nil {
		t.Fatal("unknown channel read")
	}
	if _, err := c.CreateChannel(ctx, &ivs.CreateChannelInput{Name: aws.String("x"), Type: "BOGUS"}); err == nil {
		t.Fatal("bad channel type succeeded")
	}
	tags, err := c.ListTagsForResource(ctx, &ivs.ListTagsForResourceInput{ResourceArn: ch.Channel.Arn})
	if err != nil || tags.Tags["env"] != "dev" {
		t.Fatalf("tags: %v %+v", err, tags)
	}
	if _, err := c.DeleteChannel(ctx, &ivs.DeleteChannelInput{Arn: ch.Channel.Arn}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.DeleteChannel(ctx, &ivs.DeleteChannelInput{Arn: ch.Channel.Arn}); err == nil {
		t.Fatal("second delete succeeded")
	}
	if _, err := c.DeleteRecordingConfiguration(ctx, &ivs.DeleteRecordingConfigurationInput{Arn: rec.RecordingConfiguration.Arn}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.GetRecordingConfiguration(ctx, &ivs.GetRecordingConfigurationInput{Arn: rec.RecordingConfiguration.Arn}); err == nil {
		t.Fatal("deleted recording still read")
	}
}
