package s3_test

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	s3svc "github.com/nrndev/pogi/internal/aws/s3"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/core"
)

// TestJournalReplay replays the wal-mode journal into a fresh service: the
// state must come out the same, multipart uploads included.
func TestJournalReplay(t *testing.T) {
	var svc *s3svc.Service
	_, url := core.ServeTest(t, func(h *core.Hub) core.Service { svc = s3svc.New(h); return svc })
	check := core.JournalTest(t, svc, s3svc.New(nil))
	c := s3.NewFromConfig(awstest.Config(t, url), func(o *s3.Options) { o.UsePathStyle = true })
	b := func(name string) *string { return aws.String(name) }
	put := func(bucket, key, body string) {
		must(c.PutObject(ctx, &s3.PutObjectInput{Bucket: b(bucket), Key: b(key), Body: strings.NewReader(body)}))
	}

	must(c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: b("docs")}))
	must(c.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{Bucket: b("docs"),
		VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled}}))
	put("docs", "k", "v1")
	put("docs", "k", "v2")
	must(c.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: b("docs"), Key: b("k")})) // a delete marker
	put("docs", "t", "tagged")
	must(c.PutObjectTagging(ctx, &s3.PutObjectTaggingInput{Bucket: b("docs"), Key: b("t"),
		Tagging: &types.Tagging{TagSet: []types.Tag{{Key: b("team"), Value: b("core")}}}}))

	must(c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: b("plain")}))
	must(c.PutBucketTagging(ctx, &s3.PutBucketTaggingInput{Bucket: b("plain"),
		Tagging: &types.Tagging{TagSet: []types.Tag{{Key: b("env"), Value: b("dev")}}}}))
	put("plain", "a", "A")
	must(c.CopyObject(ctx, &s3.CopyObjectInput{Bucket: b("plain"), Key: b("copy"), CopySource: b("plain/a")}))
	must(c.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: b("plain"), Delete: &types.Delete{Objects: []types.ObjectIdentifier{{Key: b("a")}}}}))
	for _, key := range []string{"big", "aborted", "open"} {
		up := must(c.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: b("plain"), Key: b(key)}))
		part := must(c.UploadPart(ctx, &s3.UploadPartInput{Bucket: b("plain"), Key: b(key), UploadId: up.UploadId,
			PartNumber: aws.Int32(1), Body: strings.NewReader("part")}))
		switch key {
		case "big":
			must(c.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{Bucket: b("plain"), Key: b(key), UploadId: up.UploadId,
				MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{{PartNumber: aws.Int32(1), ETag: part.ETag}}}}))
		case "aborted":
			must(c.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: b("plain"), Key: b(key), UploadId: up.UploadId}))
		}
	}
	must(c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: b("tmp")}))
	must(c.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: b("tmp")}))
	check()
}
