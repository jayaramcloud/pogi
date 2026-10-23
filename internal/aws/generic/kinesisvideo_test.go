package generic_test

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesisvideo"
	"github.com/aws/aws-sdk-go-v2/service/kinesisvideo/types"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestKinesisVideo(t *testing.T) {
	_, url := start(t, "kinesisvideo")
	ctx := context.Background()
	c := kinesisvideo.NewFromConfig(awstest.Config(t, url))

	o, err := c.CreateStream(ctx, &kinesisvideo.CreateStreamInput{StreamName: aws.String("cam"), DataRetentionInHours: aws.Int32(24), Tags: map[string]string{"env": "dev"}})
	if err != nil || aws.ToString(o.StreamARN) == "" {
		t.Fatalf("create: %v %+v", err, o)
	}
	d, err := c.DescribeStream(ctx, &kinesisvideo.DescribeStreamInput{StreamName: aws.String("cam")})
	if err != nil || d.StreamInfo.Status != types.StatusActive || aws.ToInt32(d.StreamInfo.DataRetentionInHours) != 24 {
		t.Fatalf("describe: %v %+v", err, d)
	}
	if _, err := c.CreateStream(ctx, &kinesisvideo.CreateStreamInput{StreamName: aws.String("cam")}); err == nil {
		t.Fatal("duplicate create succeeded")
	}
	ch, err := c.CreateSignalingChannel(ctx, &kinesisvideo.CreateSignalingChannelInput{ChannelName: aws.String("sig")})
	if err != nil || aws.ToString(ch.ChannelARN) == "" {
		t.Fatalf("channel: %v %+v", err, ch)
	}
	l, err := c.ListStreams(ctx, &kinesisvideo.ListStreamsInput{})
	if err != nil || len(l.StreamInfoList) != 1 {
		t.Fatalf("list: %v %+v", err, l)
	}
	if _, err := c.DeleteStream(ctx, &kinesisvideo.DeleteStreamInput{StreamARN: o.StreamARN, CurrentVersion: aws.String("stale")}); err == nil {
		t.Fatal("delete with a stale version succeeded")
	}
	if _, err := c.DeleteStream(ctx, &kinesisvideo.DeleteStreamInput{StreamARN: o.StreamARN, CurrentVersion: d.StreamInfo.Version}); err != nil {
		t.Fatal(err)
	}
	var nf *types.ResourceNotFoundException
	if _, err := c.DescribeStream(ctx, &kinesisvideo.DescribeStreamInput{StreamName: aws.String("cam")}); !errors.As(err, &nf) {
		t.Fatalf("not found: %T %v", err, err)
	}
	if _, err := c.DeleteStream(ctx, &kinesisvideo.DeleteStreamInput{StreamARN: o.StreamARN}); !errors.As(err, &nf) {
		t.Fatalf("second delete: %T %v", err, err)
	}
}
