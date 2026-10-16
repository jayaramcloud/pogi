package s3_test

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3control"
	"github.com/aws/aws-sdk-go-v2/service/s3control/types"
	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/awstest"
)

// The AWS Terraform provider reads and writes bucket tags through S3 Control.
func TestControlBucketTags(t *testing.T) {
	s := pogi.Start(t)
	ctx := context.Background()
	cfg := awstest.Config(t, s.URL)
	s3c := s3sdk.NewFromConfig(cfg, func(o *s3sdk.Options) { o.UsePathStyle = true })
	if _, err := s3c.CreateBucket(ctx, &s3sdk.CreateBucketInput{Bucket: aws.String("tagged")}); err != nil {
		t.Fatal(err)
	}
	// S3 Control prefixes the host with the account id, so it needs a name, not an IP
	ctl := s3control.NewFromConfig(awstest.Config(t, strings.Replace(s.URL, "127.0.0.1", "localhost", 1)))
	arn, acct := aws.String("arn:aws:s3:::tagged"), aws.String("000000000000")
	if _, err := ctl.TagResource(ctx, &s3control.TagResourceInput{AccountId: acct, ResourceArn: arn,
		Tags: []types.Tag{{Key: aws.String("a"), Value: aws.String("1")}, {Key: aws.String("b"), Value: aws.String("2")}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := ctl.UntagResource(ctx, &s3control.UntagResourceInput{AccountId: acct, ResourceArn: arn, TagKeys: []string{"b"}}); err != nil {
		t.Fatal(err)
	}
	out, err := ctl.ListTagsForResource(ctx, &s3control.ListTagsForResourceInput{AccountId: acct, ResourceArn: arn})
	if err != nil || len(out.Tags) != 1 || *out.Tags[0].Key != "a" {
		t.Fatalf("tags = %+v, %v", out, err)
	}
	// the same tag set as GetBucketTagging
	bt, err := s3c.GetBucketTagging(ctx, &s3sdk.GetBucketTaggingInput{Bucket: aws.String("tagged")})
	if err != nil || len(bt.TagSet) != 1 || *bt.TagSet[0].Value != "1" {
		t.Fatalf("bucket tagging = %+v, %v", bt, err)
	}
	// floci #557: the error is wrapped in <ErrorResponse>, so the SDK reads its code
	if _, err := ctl.ListTagsForResource(ctx, &s3control.ListTagsForResourceInput{AccountId: acct, ResourceArn: aws.String("arn:aws:s3:::nope")}); code(err) != "NoSuchBucket" {
		t.Fatalf("missing bucket: %v", err)
	}
}
