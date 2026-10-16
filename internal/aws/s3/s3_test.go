package s3_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/smithy-go"
	iamsvc "github.com/nrndev/pogi/internal/aws/iam"
	s3svc "github.com/nrndev/pogi/internal/aws/s3"
	sqssvc "github.com/nrndev/pogi/internal/aws/sqs"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/core"
)

var ctx = context.Background()

func setup(t *testing.T) (*s3.Client, *sqs.Client, string) {
	_, url := core.ServeTest(t,
		func(h *core.Hub) core.Service { return s3svc.New(h) },
		func(h *core.Hub) core.Service { return sqssvc.New(h) })
	cfg := awstest.Config(t, url)
	return s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true }), sqs.NewFromConfig(cfg), url
}

func code(err error) string {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		return ae.ErrorCode()
	}
	return fmt.Sprint(err)
}

// must panics on error: the failing test shows the error and stack.
func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func TestBucketsAndObjects(t *testing.T) {
	c, _, _ := setup(t)
	must(c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("docs")}))
	must(c.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String("docs")}))
	lb := must(c.ListBuckets(ctx, &s3.ListBucketsInput{}))
	if len(lb.Buckets) != 1 || *lb.Buckets[0].Name != "docs" {
		t.Fatalf("buckets = %+v", lb.Buckets)
	}
	if _, err := c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("Bad_Name")}); code(err) != "InvalidBucketName" {
		t.Fatalf("want InvalidBucketName, got %v", err)
	}

	// The SDK sends a CRC32 checksum by default and validates it on download.
	must(c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("docs"), Key: aws.String("a/b c.txt"),
		Body: strings.NewReader("hello pogi"), ContentType: aws.String("text/plain"), Metadata: map[string]string{"owner": "me"}}))
	get := must(c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("docs"), Key: aws.String("a/b c.txt"), ChecksumMode: types.ChecksumModeEnabled}))
	body, _ := io.ReadAll(get.Body)
	if string(body) != "hello pogi" || *get.ContentType != "text/plain" || get.Metadata["owner"] != "me" || get.ChecksumCRC32 == nil {
		t.Fatalf("get = %q %v %v crc=%v", body, *get.ContentType, get.Metadata, get.ChecksumCRC32)
	}

	rng := must(c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("docs"), Key: aws.String("a/b c.txt"), Range: aws.String("bytes=6-")}))
	if b, _ := io.ReadAll(rng.Body); string(b) != "pogi" {
		t.Fatalf("range = %q", b)
	}

	_, err := c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("docs"), Key: aws.String("missing")})
	var nsk *types.NoSuchKey
	if !errors.As(err, &nsk) {
		t.Fatalf("want NoSuchKey, got %v", err)
	}

	for _, k := range []string{"a/1", "a/2", "b/1", "top"} {
		must(c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("docs"), Key: aws.String(k), Body: strings.NewReader(k)}))
	}
	list := must(c.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String("docs"), Delimiter: aws.String("/")}))
	if len(list.CommonPrefixes) != 2 || len(list.Contents) != 1 || *list.Contents[0].Key != "top" {
		t.Fatalf("delimited list = %d prefixes, %d contents", len(list.CommonPrefixes), len(list.Contents))
	}
	var keys []string
	p := s3.NewListObjectsV2Paginator(c, &s3.ListObjectsV2Input{Bucket: aws.String("docs"), MaxKeys: aws.Int32(2)})
	for p.HasMorePages() {
		page := must(p.NextPage(ctx))
		for _, o := range page.Contents {
			keys = append(keys, *o.Key)
		}
	}
	if strings.Join(keys, ",") != "a/1,a/2,a/b c.txt,b/1,top" {
		t.Fatalf("paginated keys = %v", keys)
	}

	must(c.CopyObject(ctx, &s3.CopyObjectInput{Bucket: aws.String("docs"), Key: aws.String("copy"), CopySource: aws.String("docs/a/b c.txt")}))
	cp := must(c.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String("docs"), Key: aws.String("copy")}))
	if *cp.ContentLength != 10 || cp.Metadata["owner"] != "me" {
		t.Fatalf("copy head = %+v", cp)
	}

	del := must(c.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: aws.String("docs"), Delete: &types.Delete{
		Objects: []types.ObjectIdentifier{{Key: aws.String("a/1")}, {Key: aws.String("a/2")}}}}))
	if len(del.Deleted) != 2 {
		t.Fatalf("deleted = %d", len(del.Deleted))
	}
	if _, err := c.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String("docs")}); code(err) != "BucketNotEmpty" {
		t.Fatalf("want BucketNotEmpty, got %v", err)
	}
}

func TestConditionalWritesAndTags(t *testing.T) {
	c, _, _ := setup(t)
	must(c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("tf-state")}))
	put := &s3.PutObjectInput{Bucket: aws.String("tf-state"), Key: aws.String("lock"), Body: strings.NewReader("x"), IfNoneMatch: aws.String("*")}
	must(c.PutObject(ctx, put))
	put.Body = strings.NewReader("y")
	if _, err := c.PutObject(ctx, put); code(err) != "PreconditionFailed" {
		t.Fatalf("want PreconditionFailed, got %v", err)
	}
	must(c.PutObjectTagging(ctx, &s3.PutObjectTaggingInput{Bucket: aws.String("tf-state"), Key: aws.String("lock"),
		Tagging: &types.Tagging{TagSet: []types.Tag{{Key: aws.String("env"), Value: aws.String("dev")}}}}))
	tags := must(c.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{Bucket: aws.String("tf-state"), Key: aws.String("lock")}))
	if len(tags.TagSet) != 1 || *tags.TagSet[0].Value != "dev" {
		t.Fatalf("tags = %+v", tags.TagSet)
	}
	// Terraform reads every bucket sub-configuration; unset ones must be the documented "not found" codes.
	if _, err := c.GetBucketCors(ctx, &s3.GetBucketCorsInput{Bucket: aws.String("tf-state")}); code(err) != "NoSuchCORSConfiguration" {
		t.Fatalf("cors: %v", err)
	}
	if _, err := c.GetBucketPolicy(ctx, &s3.GetBucketPolicyInput{Bucket: aws.String("tf-state")}); code(err) != "NoSuchBucketPolicy" {
		t.Fatalf("policy: %v", err)
	}
	must(c.GetBucketEncryption(ctx, &s3.GetBucketEncryptionInput{Bucket: aws.String("tf-state")}))
	must(c.GetBucketAccelerateConfiguration(ctx, &s3.GetBucketAccelerateConfigurationInput{Bucket: aws.String("tf-state")}))
	must(c.GetBucketRequestPayment(ctx, &s3.GetBucketRequestPaymentInput{Bucket: aws.String("tf-state")}))
	must(c.GetBucketAcl(ctx, &s3.GetBucketAclInput{Bucket: aws.String("tf-state")}))
	must(c.PutBucketCors(ctx, &s3.PutBucketCorsInput{Bucket: aws.String("tf-state"), CORSConfiguration: &types.CORSConfiguration{
		CORSRules: []types.CORSRule{{AllowedMethods: []string{"GET"}, AllowedOrigins: []string{"*"}}}}}))
	cors := must(c.GetBucketCors(ctx, &s3.GetBucketCorsInput{Bucket: aws.String("tf-state")}))
	if len(cors.CORSRules) != 1 {
		t.Fatalf("cors rules = %+v", cors.CORSRules)
	}
	loc := must(c.GetBucketLocation(ctx, &s3.GetBucketLocationInput{Bucket: aws.String("tf-state")}))
	if loc.LocationConstraint != "" {
		t.Fatalf("us-east-1 location should be empty, got %q", loc.LocationConstraint)
	}
}

func TestVersioning(t *testing.T) {
	c, _, _ := setup(t)
	b := aws.String("ver")
	must(c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: b}))
	must(c.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{Bucket: b,
		VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled}}))
	v1 := must(c.PutObject(ctx, &s3.PutObjectInput{Bucket: b, Key: aws.String("k"), Body: strings.NewReader("one")}))
	must(c.PutObject(ctx, &s3.PutObjectInput{Bucket: b, Key: aws.String("k"), Body: strings.NewReader("two")}))
	old := must(c.GetObject(ctx, &s3.GetObjectInput{Bucket: b, Key: aws.String("k"), VersionId: v1.VersionId}))
	if body, _ := io.ReadAll(old.Body); string(body) != "one" {
		t.Fatalf("v1 = %q", body)
	}
	dm := must(c.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: b, Key: aws.String("k")}))
	if !aws.ToBool(dm.DeleteMarker) {
		t.Fatal("expected a delete marker")
	}
	if _, err := c.GetObject(ctx, &s3.GetObjectInput{Bucket: b, Key: aws.String("k")}); code(err) != "NoSuchKey" {
		t.Fatalf("after delete: %v", err)
	}
	vs := must(c.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{Bucket: b}))
	if len(vs.Versions) != 2 || len(vs.DeleteMarkers) != 1 {
		t.Fatalf("versions=%d markers=%d", len(vs.Versions), len(vs.DeleteMarkers))
	}
}

func TestMultipart(t *testing.T) {
	c, _, _ := setup(t)
	b, k := aws.String("big"), aws.String("blob.bin")
	must(c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: b}))
	up := must(c.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: b, Key: k, ChecksumAlgorithm: types.ChecksumAlgorithmCrc32}))
	part1 := bytes.Repeat([]byte("a"), 5<<20)
	part2 := []byte("tail")
	var parts []types.CompletedPart
	for i, data := range [][]byte{part1, part2} {
		res := must(c.UploadPart(ctx, &s3.UploadPartInput{Bucket: b, Key: k, UploadId: up.UploadId, PartNumber: aws.Int32(int32(i + 1)),
			Body: bytes.NewReader(data), ChecksumAlgorithm: types.ChecksumAlgorithmCrc32}))
		parts = append(parts, types.CompletedPart{ETag: res.ETag, PartNumber: aws.Int32(int32(i + 1)), ChecksumCRC32: res.ChecksumCRC32})
	}
	lp := must(c.ListParts(ctx, &s3.ListPartsInput{Bucket: b, Key: k, UploadId: up.UploadId}))
	if len(lp.Parts) != 2 {
		t.Fatalf("parts = %d", len(lp.Parts))
	}
	done := must(c.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{Bucket: b, Key: k, UploadId: up.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: parts}}))
	if !strings.HasSuffix(strings.Trim(*done.ETag, `"`), "-2") {
		t.Fatalf("etag = %s", *done.ETag)
	}
	get := must(c.GetObject(ctx, &s3.GetObjectInput{Bucket: b, Key: k, ChecksumMode: types.ChecksumModeEnabled}))
	body, _ := io.ReadAll(get.Body)
	if len(body) != len(part1)+len(part2) || !bytes.HasSuffix(body, part2) {
		t.Fatalf("assembled %d bytes", len(body))
	}
	small := must(c.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: b, Key: k}))
	p1 := must(c.UploadPart(ctx, &s3.UploadPartInput{Bucket: b, Key: k, UploadId: small.UploadId, PartNumber: aws.Int32(1), Body: strings.NewReader("x")}))
	p2 := must(c.UploadPart(ctx, &s3.UploadPartInput{Bucket: b, Key: k, UploadId: small.UploadId, PartNumber: aws.Int32(2), Body: strings.NewReader("y")}))
	_, err := c.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{Bucket: b, Key: k, UploadId: small.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{{ETag: p1.ETag, PartNumber: aws.Int32(1)}, {ETag: p2.ETag, PartNumber: aws.Int32(2)}}}})
	if code(err) != "EntityTooSmall" {
		t.Fatalf("want EntityTooSmall, got %v", err)
	}
	must(c.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: b, Key: k, UploadId: small.UploadId}))
}

// Parts under the upload limit can add up to a file over it: CompleteMultipartUpload refuses that.
func TestMultipartOverLimit(t *testing.T) {
	c, _, _ := setup(t)
	b, k := aws.String("big"), aws.String("huge.bin")
	must(c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: b}))
	up := must(c.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: b, Key: k}))
	var parts []types.CompletedPart
	for i, size := range []int{5 << 20, 5 << 20, 1 << 20} {
		res := must(c.UploadPart(ctx, &s3.UploadPartInput{Bucket: b, Key: k, UploadId: up.UploadId, PartNumber: aws.Int32(int32(i + 1)),
			Body: bytes.NewReader(make([]byte, size))}))
		parts = append(parts, types.CompletedPart{ETag: res.ETag, PartNumber: aws.Int32(int32(i + 1))})
	}
	_, err := c.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{Bucket: b, Key: k, UploadId: up.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: parts}})
	if code(err) != "EntityTooLarge" || !strings.Contains(err.Error(), "limited to 10 MB (this one is 11 MB)") {
		t.Fatalf("want EntityTooLarge with the caution, got %v", err)
	}
}

func TestPresignAndChunkedUploads(t *testing.T) {
	c, _, url := setup(t)
	b := aws.String("web")
	must(c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: b}))

	ps := s3.NewPresignClient(c)
	put := must(ps.PresignPutObject(ctx, &s3.PutObjectInput{Bucket: b, Key: aws.String("up.txt")}))
	req, _ := http.NewRequest(http.MethodPut, put.URL, strings.NewReader("presigned!"))
	res := must(http.DefaultClient.Do(req))
	res.Body.Close()
	getURL := must(ps.PresignGetObject(ctx, &s3.GetObjectInput{Bucket: b, Key: aws.String("up.txt")}))
	res = must(http.Get(getURL.URL))
	if body, _ := io.ReadAll(res.Body); string(body) != "presigned!" {
		t.Fatalf("presigned get = %q", body)
	}

	// What the JS SDK v3 sends: aws-chunked framing with a trailing CRC32.
	chunked := "5\r\nhello\r\n6\r\n world\r\n0\r\nx-amz-checksum-crc32:DUoRhQ==\r\n\r\n"
	req, _ = http.NewRequest(http.MethodPut, url+"/web/chunked.txt", strings.NewReader(chunked))
	req.Header.Set("Content-Encoding", "aws-chunked")
	req.Header.Set("X-Amz-Content-Sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER")
	req.Header.Set("X-Amz-Trailer", "x-amz-checksum-crc32")
	awstest.Sign(t, req, "s3")
	res = must(http.DefaultClient.Do(req))
	if res.StatusCode != 200 {
		b, _ := io.ReadAll(res.Body)
		t.Fatalf("chunked put: %d %s", res.StatusCode, b)
	}
	got := must(c.GetObject(ctx, &s3.GetObjectInput{Bucket: b, Key: aws.String("chunked.txt"), ChecksumMode: types.ChecksumModeEnabled}))
	if body, _ := io.ReadAll(got.Body); string(body) != "hello world" {
		t.Fatalf("chunked body = %q", body)
	}
}

func TestNotificationsToSQS(t *testing.T) {
	c, q, _ := setup(t)
	b := aws.String("events")
	must(c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: b}))
	qu := must(q.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("uploads")}))
	must(c.PutBucketNotificationConfiguration(ctx, &s3.PutBucketNotificationConfigurationInput{Bucket: b,
		NotificationConfiguration: &types.NotificationConfiguration{QueueConfigurations: []types.QueueConfiguration{{
			QueueArn: aws.String("arn:aws:sqs:us-east-1:000000000000:uploads"), Events: []types.Event{"s3:ObjectCreated:*"},
			Filter: &types.NotificationConfigurationFilter{Key: &types.S3KeyFilter{FilterRules: []types.FilterRule{{Name: "suffix", Value: aws.String(".jpg")}}}},
		}}}}))
	must(c.PutObject(ctx, &s3.PutObjectInput{Bucket: b, Key: aws.String("skip.txt"), Body: strings.NewReader("x")}))
	must(c.PutObject(ctx, &s3.PutObjectInput{Bucket: b, Key: aws.String("cat.jpg"), Body: strings.NewReader("meow")}))
	msgs := must(q.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: qu.QueueUrl, MaxNumberOfMessages: 10, WaitTimeSeconds: 1}))
	var events []string
	for _, m := range msgs.Messages {
		var rec struct {
			Event   string
			Records []struct {
				EventName string
				S3        struct{ Object struct{ Key string } }
			}
		}
		json.Unmarshal([]byte(*m.Body), &rec)
		if rec.Event != "" {
			events = append(events, rec.Event)
		}
		for _, r := range rec.Records {
			events = append(events, r.EventName+":"+r.S3.Object.Key)
		}
	}
	if strings.Join(events, ",") != "s3:TestEvent,ObjectCreated:Put:cat.jpg" {
		t.Fatalf("events = %v", events)
	}
}

func TestPresignExpired(t *testing.T) {
	_, _, url := setup(t)
	old := time.Now().UTC().Add(-2 * time.Hour).Format("20060102T150405Z")
	res := must(http.Get(url + "/x/y?X-Amz-Signature=abc&X-Amz-Date=" + old + "&X-Amz-Expires=60"))
	if res.StatusCode != 403 {
		t.Fatalf("status = %d", res.StatusCode)
	}
}

// iamUser makes an IAM user with one inline policy and returns an S3 client signed as that user.
func iamUser(t *testing.T, url, name string, statements ...string) *s3.Client {
	ic := iam.NewFromConfig(awstest.Config(t, url))
	must(ic.CreateUser(ctx, &iam.CreateUserInput{UserName: aws.String(name)}))
	must(ic.PutUserPolicy(ctx, &iam.PutUserPolicyInput{UserName: aws.String(name), PolicyName: aws.String("p"),
		PolicyDocument: aws.String(`{"Version":"2012-10-17","Statement":[` + strings.Join(statements, ",") + `]}`)}))
	k := must(ic.CreateAccessKey(ctx, &iam.CreateAccessKeyInput{UserName: aws.String(name)}))
	cfg := awstest.ConfigAs(t, url, *k.AccessKey.AccessKeyId, *k.AccessKey.SecretAccessKey, "")
	return s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true })
}

func allow(action, resource string, cond ...string) string {
	c := ""
	if len(cond) > 0 {
		c = `,"Condition":` + cond[0]
	}
	return `{"Effect":"Allow","Action":` + action + `,"Resource":` + resource + c + `}`
}

func TestIAMEnforcement(t *testing.T) {
	h, url := core.ServeTest(t,
		func(h *core.Hub) core.Service { return iamsvc.New(h) },
		func(h *core.Hub) core.Service { return s3svc.New(h) })
	h.Cfg.IAM, h.Cfg.ValidateSigs = "strict", false // the empty-Delete request below is unsigned
	admin := s3.NewFromConfig(awstest.Config(t, url), func(o *s3.Options) { o.UsePathStyle = true })
	for _, b := range []string{"secret", "dest", "demo", "vers"} {
		must(admin.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(b)}))
	}
	put := func(b, k string, tagging *string) *s3.PutObjectOutput {
		return must(admin.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(b), Key: aws.String(k), Body: strings.NewReader("x"), Tagging: tagging}))
	}
	denied := func(what string, err error) {
		t.Helper()
		if code(err) != "AccessDenied" {
			t.Errorf("%s: want AccessDenied, got %v", what, err)
		}
	}
	put("secret", "k", nil)

	// floci #2333: CopyObject and UploadPartCopy need read access on the source.
	w := iamUser(t, url, "writer", allow(`"s3:PutObject"`, `"arn:aws:s3:::dest/*"`))
	_, err := w.CopyObject(ctx, &s3.CopyObjectInput{Bucket: aws.String("dest"), Key: aws.String("x"), CopySource: aws.String("secret/k")})
	denied("CopyObject without read on the source", err)
	mp := must(w.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: aws.String("dest"), Key: aws.String("m")}))
	_, err = w.UploadPartCopy(ctx, &s3.UploadPartCopyInput{Bucket: aws.String("dest"), Key: aws.String("m"), UploadId: mp.UploadId, PartNumber: aws.Int32(1), CopySource: aws.String("secret/k")})
	denied("UploadPartCopy without read on the source", err)
	both := iamUser(t, url, "both", allow(`["s3:PutObject","s3:GetObject"]`, `["arn:aws:s3:::dest/*","arn:aws:s3:::secret/*"]`))
	must(both.CopyObject(ctx, &s3.CopyObjectInput{Bucket: aws.String("dest"), Key: aws.String("x"), CopySource: aws.String("secret/k")}))

	// floci #5716: IAM action names. Deleting a version, deleting bucket tags and
	// writing tags with an object each need their own action.
	must(admin.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{Bucket: aws.String("vers"),
		VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled}}))
	v := put("vers", "k", nil)
	d := iamUser(t, url, "deleter", allow(`"s3:DeleteObject"`, `"arn:aws:s3:::vers/*"`))
	_, err = d.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String("vers"), Key: aws.String("k"), VersionId: v.VersionId})
	denied("DeleteObject of a version needs s3:DeleteObjectVersion", err)
	must(d.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String("vers"), Key: aws.String("k")}))
	tg := iamUser(t, url, "tagger", allow(`"s3:PutBucketTagging"`, `"arn:aws:s3:::demo"`))
	must(tg.DeleteBucketTagging(ctx, &s3.DeleteBucketTaggingInput{Bucket: aws.String("demo")}))
	p := iamUser(t, url, "putter", allow(`"s3:PutObject"`, `"arn:aws:s3:::demo/*"`))
	_, err = p.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("demo"), Key: aws.String("t"), Body: strings.NewReader("x"), Tagging: aws.String("a=b")})
	denied("PutObject with tags needs s3:PutObjectTagging", err)

	// floci #3945: object tag condition keys.
	put("demo", "pub", aws.String("class=public"))
	put("demo", "priv", nil)
	tu := iamUser(t, url, "tagu",
		allow(`"s3:GetObject"`, `"arn:aws:s3:::demo/*"`, `{"StringEquals":{"s3:ExistingObjectTag/class":"public"}}`),
		allow(`["s3:PutObject","s3:PutObjectTagging"]`, `"arn:aws:s3:::demo/*"`, `{"StringEquals":{"s3:RequestObjectTag/class":"public"}}`))
	must(tu.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("demo"), Key: aws.String("pub")}))
	_, err = tu.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("demo"), Key: aws.String("priv")})
	denied("GetObject of an untagged object", err)
	must(tu.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("demo"), Key: aws.String("n"), Body: strings.NewReader("x"), Tagging: aws.String("class=public")}))
	_, err = tu.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("demo"), Key: aws.String("n2"), Body: strings.NewReader("x"), Tagging: aws.String("class=secret")})
	denied("PutObject with the wrong tag", err)

	// floci #5405: DeleteObjects is checked key by key and answers 200.
	put("demo", "allowed/a", nil)
	put("demo", "protected/b", nil)
	du := iamUser(t, url, "du", allow(`"s3:DeleteObject"`, `"arn:aws:s3:::demo/allowed/*"`))
	out := must(du.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: aws.String("demo"), Delete: &types.Delete{
		Objects: []types.ObjectIdentifier{{Key: aws.String("allowed/a")}, {Key: aws.String("protected/b")}}}}))
	if len(out.Deleted) != 1 || *out.Deleted[0].Key != "allowed/a" || len(out.Errors) != 1 || *out.Errors[0].Key != "protected/b" || *out.Errors[0].Code != "AccessDenied" {
		t.Fatalf("DeleteObjects = deleted %v, errors %v", out.Deleted, out.Errors)
	}
	must(admin.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String("demo"), Key: aws.String("protected/b")}))

	// floci #2198: an empty Delete element is MalformedXML.
	res := must(http.Post(url+"/demo?delete", "application/xml", strings.NewReader(`<Delete xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></Delete>`)))
	if body, _ := io.ReadAll(res.Body); res.StatusCode != 400 || !strings.Contains(string(body), "MalformedXML") {
		t.Errorf("empty DeleteObjects: %d %s", res.StatusCode, body)
	}
}

// floci #2598: Object Lock retention and legal hold are enforced.
func TestObjectLock(t *testing.T) {
	c, _, _ := setup(t)
	locked := func(what string, err error) {
		t.Helper()
		if code(err) != "AccessDenied" {
			t.Errorf("%s: want AccessDenied, got %v", what, err)
		}
	}
	must(c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("lock"), ObjectLockEnabledForBucket: aws.Bool(true)}))
	must(c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("plain")}))
	b := aws.String("lock")
	put := func(k string, mode types.ObjectLockMode, d time.Duration) *string {
		in := &s3.PutObjectInput{Bucket: b, Key: aws.String(k), Body: strings.NewReader("x"), ObjectLockMode: mode}
		if mode != "" {
			in.ObjectLockRetainUntilDate = aws.Time(time.Now().Add(d))
		}
		return must(c.PutObject(ctx, in)).VersionId
	}
	retain := func(k string, v *string, mode types.ObjectLockRetentionMode, d time.Duration, bypass bool) error {
		_, err := c.PutObjectRetention(ctx, &s3.PutObjectRetentionInput{Bucket: b, Key: aws.String(k), VersionId: v, BypassGovernanceRetention: aws.Bool(bypass),
			Retention: &types.ObjectLockRetention{Mode: mode, RetainUntilDate: aws.Time(time.Now().Add(d))}})
		return err
	}
	del := func(k string, v *string, bypass bool) error {
		_, err := c.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: b, Key: aws.String(k), VersionId: v, BypassGovernanceRetention: aws.Bool(bypass)})
		return err
	}

	// COMPLIANCE: cannot become GOVERNANCE, cannot shrink, cannot be deleted; can grow.
	v := put("comp", types.ObjectLockModeCompliance, time.Hour)
	locked("COMPLIANCE to GOVERNANCE", retain("comp", v, types.ObjectLockRetentionModeGovernance, 2*time.Hour, false))
	locked("shortening COMPLIANCE", retain("comp", v, types.ObjectLockRetentionModeCompliance, time.Minute, true))
	locked("deleting a COMPLIANCE version", del("comp", v, true))
	if err := retain("comp", v, types.ObjectLockRetentionModeCompliance, 2*time.Hour, false); err != nil {
		t.Errorf("extending COMPLIANCE: %v", err)
	}
	got := must(c.GetObjectRetention(ctx, &s3.GetObjectRetentionInput{Bucket: b, Key: aws.String("comp"), VersionId: v}))
	if got.Retention.Mode != types.ObjectLockRetentionModeCompliance || time.Until(*got.Retention.RetainUntilDate) < 90*time.Minute {
		t.Errorf("retention = %+v", got.Retention)
	}
	// A delete without a version only adds a delete marker.
	must(c.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: b, Key: aws.String("comp")}))

	// GOVERNANCE: the bypass header lets you shorten, remove and delete.
	v = put("gov", types.ObjectLockModeGovernance, time.Hour)
	locked("deleting a GOVERNANCE version", del("gov", v, false))
	locked("shortening GOVERNANCE", retain("gov", v, types.ObjectLockRetentionModeGovernance, time.Minute, false))
	if err := retain("gov", v, types.ObjectLockRetentionModeGovernance, time.Minute, true); err != nil {
		t.Errorf("shortening GOVERNANCE with bypass: %v", err)
	}
	if err := del("gov", v, true); err != nil {
		t.Errorf("deleting GOVERNANCE with bypass: %v", err)
	}

	// Legal hold blocks deletes until it is switched off.
	v = put("hold", "", 0)
	must(c.PutObjectLegalHold(ctx, &s3.PutObjectLegalHoldInput{Bucket: b, Key: aws.String("hold"), VersionId: v, LegalHold: &types.ObjectLockLegalHold{Status: types.ObjectLockLegalHoldStatusOn}}))
	locked("deleting a version on legal hold", del("hold", v, true))
	res := must(c.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: b, Delete: &types.Delete{Objects: []types.ObjectIdentifier{{Key: aws.String("hold"), VersionId: v}}}}))
	if len(res.Errors) != 1 || *res.Errors[0].Code != "AccessDenied" {
		t.Errorf("DeleteObjects on legal hold: %+v", res)
	}
	must(c.PutObjectLegalHold(ctx, &s3.PutObjectLegalHoldInput{Bucket: b, Key: aws.String("hold"), VersionId: v, LegalHold: &types.ObjectLockLegalHold{Status: types.ObjectLockLegalHoldStatusOff}}))
	if err := del("hold", v, false); err != nil {
		t.Errorf("deleting after the hold is off: %v", err)
	}

	// Lock settings need a bucket with Object Lock, and the bucket default applies.
	_, err := c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("plain"), Key: aws.String("k"), Body: strings.NewReader("x"), ObjectLockMode: types.ObjectLockModeGovernance, ObjectLockRetainUntilDate: aws.Time(time.Now().Add(time.Hour))})
	if code(err) != "InvalidRequest" {
		t.Errorf("lock headers on a bucket without Object Lock: %v", err)
	}
	must(c.PutObjectLockConfiguration(ctx, &s3.PutObjectLockConfigurationInput{Bucket: b, ObjectLockConfiguration: &types.ObjectLockConfiguration{
		ObjectLockEnabled: types.ObjectLockEnabledEnabled, Rule: &types.ObjectLockRule{DefaultRetention: &types.DefaultRetention{Mode: types.ObjectLockRetentionModeGovernance, Days: aws.Int32(1)}}}}))
	v = put("dflt", "", 0)
	locked("deleting a version under the default retention", del("dflt", v, false))
	if _, err := c.GetObjectRetention(ctx, &s3.GetObjectRetentionInput{Bucket: b, Key: aws.String("dflt"), VersionId: v}); err != nil {
		t.Errorf("default retention not applied: %v", err)
	}
}

// floci #5137: lifecycle rules are applied by the sweep, and x-amz-expiration shows the date.
func TestLifecycleSweep(t *testing.T) {
	h, url := core.ServeTest(t, func(h *core.Hub) core.Service { return s3svc.New(h) })
	svc := h.Service("s3").(*s3svc.Service)
	c := s3.NewFromConfig(awstest.Config(t, url), func(o *s3.Options) { o.UsePathStyle = true })
	b := aws.String("life")
	must(c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: b}))
	put := func(k string) *s3.PutObjectOutput {
		return must(c.PutObject(ctx, &s3.PutObjectInput{Bucket: b, Key: aws.String(k), Body: strings.NewReader("x")}))
	}
	exists := func(k string) bool {
		_, err := c.HeadObject(ctx, &s3.HeadObjectInput{Bucket: b, Key: aws.String(k)})
		return err == nil
	}
	rule := func(id, prefix string, exp *types.LifecycleExpiration) types.LifecycleRule {
		return types.LifecycleRule{ID: aws.String(id), Status: types.ExpirationStatusEnabled, Filter: &types.LifecycleRuleFilter{Prefix: aws.String(prefix)}, Expiration: exp}
	}
	lifecycle := func(rules ...types.LifecycleRule) {
		must(c.PutBucketLifecycleConfiguration(ctx, &s3.PutBucketLifecycleConfigurationInput{Bucket: b, LifecycleConfiguration: &types.BucketLifecycleConfiguration{Rules: rules}}))
	}

	// Expiration by Days: the header names the rule and the day; the sweep honours the clock.
	put("logs/a")
	put("keep/b")
	lifecycle(rule("r", "logs/", &types.LifecycleExpiration{Days: aws.Int32(1)}))
	h1 := must(c.HeadObject(ctx, &s3.HeadObjectInput{Bucket: b, Key: aws.String("logs/a")}))
	if e := aws.ToString(h1.Expiration); !strings.Contains(e, `rule-id="r"`) || !strings.Contains(e, "expiry-date=") {
		t.Errorf("x-amz-expiration = %q", e)
	}
	if h2 := must(c.HeadObject(ctx, &s3.HeadObjectInput{Bucket: b, Key: aws.String("keep/b")})); h2.Expiration != nil {
		t.Errorf("an object no rule covers has x-amz-expiration %q", *h2.Expiration)
	}
	svc.Sweep(time.Now())
	if !exists("logs/a") {
		t.Error("expired before its day")
	}
	svc.Sweep(time.Now().Add(49 * time.Hour))
	if exists("logs/a") || !exists("keep/b") {
		t.Errorf("after the sweep: logs/a=%v keep/b=%v", exists("logs/a"), exists("keep/b"))
	}

	// A date in the past is applied by the background sweep itself.
	put("old/c")
	lifecycle(rule("d", "old/", &types.LifecycleExpiration{Date: aws.Time(time.Now().Add(-48 * time.Hour).Truncate(24 * time.Hour))}))
	time.Sleep(2500 * time.Millisecond)
	if exists("old/c") {
		t.Error("the background sweep did not expire an object past the rule's date")
	}

	// Versioned bucket: expiry adds a delete marker; noncurrent versions go by their own rule.
	must(c.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{Bucket: b, VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled}}))
	v1 := put("ver/x").VersionId
	v2 := put("ver/x").VersionId
	lifecycle(types.LifecycleRule{ID: aws.String("n"), Status: types.ExpirationStatusEnabled, Filter: &types.LifecycleRuleFilter{Prefix: aws.String("ver/")},
		NoncurrentVersionExpiration: &types.NoncurrentVersionExpiration{NoncurrentDays: aws.Int32(1)},
		Expiration:                  &types.LifecycleExpiration{Days: aws.Int32(5)}})
	svc.Sweep(time.Now().Add(49 * time.Hour))
	if _, err := c.HeadObject(ctx, &s3.HeadObjectInput{Bucket: b, Key: aws.String("ver/x"), VersionId: v1}); err == nil {
		t.Error("noncurrent version survived NoncurrentDays")
	}
	must(c.HeadObject(ctx, &s3.HeadObjectInput{Bucket: b, Key: aws.String("ver/x"), VersionId: v2}))
	svc.Sweep(time.Now().Add(7 * 24 * time.Hour))
	if exists("ver/x") {
		t.Error("the current version was not expired (delete marker expected)")
	}
	if n := len(must(c.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{Bucket: b, Prefix: aws.String("ver/")})).DeleteMarkers); n != 1 {
		t.Errorf("%d delete markers, want 1", n)
	}

	// Incomplete multipart uploads are aborted.
	must(c.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: b, Key: aws.String("up/big")}))
	lifecycle(types.LifecycleRule{ID: aws.String("a"), Status: types.ExpirationStatusEnabled, Filter: &types.LifecycleRuleFilter{Prefix: aws.String("up/")},
		AbortIncompleteMultipartUpload: &types.AbortIncompleteMultipartUpload{DaysAfterInitiation: aws.Int32(1)}})
	svc.Sweep(time.Now().Add(49 * time.Hour))
	if n := len(must(c.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{Bucket: b})).Uploads); n != 0 {
		t.Errorf("%d incomplete uploads left", n)
	}
}

// floci #964: a bucket with a website configuration is served on the website endpoint.
func TestWebsite(t *testing.T) {
	c, _, url := setup(t)
	b := aws.String("site")
	must(c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: b}))
	put := func(k, body string) {
		must(c.PutObject(ctx, &s3.PutObjectInput{Bucket: b, Key: aws.String(k), Body: strings.NewReader(body), ContentType: aws.String("text/html")}))
	}
	get := func(path string) (int, string, string) {
		req, _ := http.NewRequest("GET", url+path, nil)
		req.Host = "site.s3-website.localhost"
		res := must((&http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}).Do(req))
		defer res.Body.Close()
		body, _ := io.ReadAll(res.Body)
		return res.StatusCode, string(body), res.Header.Get("Location")
	}
	put("index.html", "<h1>home</h1>")
	put("docs/index.html", "<h1>docs</h1>")
	if st, _, _ := get("/"); st != 404 {
		t.Errorf("no website configuration: %d", st)
	}
	must(c.PutBucketWebsite(ctx, &s3.PutBucketWebsiteInput{Bucket: b, WebsiteConfiguration: &types.WebsiteConfiguration{
		IndexDocument: &types.IndexDocument{Suffix: aws.String("index.html")}, ErrorDocument: &types.ErrorDocument{Key: aws.String("error.html")}}}))
	if st, body, _ := get("/"); st != 200 || !strings.Contains(body, "home") {
		t.Errorf("index: %d %q", st, body)
	}
	if st, body, _ := get("/docs/"); st != 200 || !strings.Contains(body, "docs") {
		t.Errorf("folder index: %d %q", st, body)
	}
	if st, _, loc := get("/docs"); st != 302 || loc != "/docs/" {
		t.Errorf("folder without slash: %d %q", st, loc)
	}
	if st, body, _ := get("/missing"); st != 404 || !strings.Contains(body, "NoSuchKey") {
		t.Errorf("missing page without an error document: %d %q", st, body)
	}
	put("error.html", "<h1>custom 404</h1>")
	if st, body, _ := get("/missing"); st != 404 || !strings.Contains(body, "custom 404") {
		t.Errorf("error document: %d %q", st, body)
	}
}

// floci #4243, #3481: bucket policies are evaluated next to identity policies, in the
// same account and across accounts, including the source bucket of a copy.
func TestBucketPolicyEvaluation(t *testing.T) {
	h, url := core.ServeTest(t,
		func(h *core.Hub) core.Service { return iamsvc.New(h) },
		func(h *core.Hub) core.Service { return s3svc.New(h) })
	h.Cfg.IAM = "strict"
	path := func(o *s3.Options) { o.UsePathStyle = true }
	admin := s3.NewFromConfig(awstest.Config(t, url), path)
	other := s3.NewFromConfig(awstest.ConfigAs(t, url, "111111111111", "x", ""), path)
	for _, b := range []string{"demo", "secret", "dest"} {
		must(admin.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(b)}))
	}
	must(other.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("xacct")}))
	must(other.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("xacct"), Key: aws.String("k"), Body: strings.NewReader("x")}))
	must(admin.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("secret"), Key: aws.String("k"), Body: strings.NewReader("x")}))
	setPolicy := func(c *s3.Client, bucket, doc string) {
		must(c.PutBucketPolicy(ctx, &s3.PutBucketPolicyInput{Bucket: aws.String(bucket), Policy: aws.String(doc)}))
	}
	pol := func(effect, principal, action, resource string) string {
		return `{"Version":"2012-10-17","Statement":[{"Effect":"` + effect + `","Principal":` + principal + `,"Action":"` + action + `","Resource":"` + resource + `"}]}`
	}
	alice := iamUser(t, url, "alice", allow(`"s3:ListAllMyBuckets"`, `"*"`))
	put := func(c *s3.Client, bucket string) error {
		_, err := c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String("n"), Body: strings.NewReader("x")})
		return err
	}

	// Same account: an Allow in the bucket policy is enough; naming only the account is not.
	if code(put(alice, "demo")) != "AccessDenied" {
		t.Error("no policy anywhere must be denied")
	}
	setPolicy(admin, "demo", pol("Allow", `{"AWS":"arn:aws:iam::000000000000:root"}`, "s3:PutObject", "arn:aws:s3:::demo/*"))
	if code(put(alice, "demo")) != "AccessDenied" {
		t.Error("a policy naming only the account root delegates to identity policies")
	}
	setPolicy(admin, "demo", pol("Allow", `{"AWS":"arn:aws:iam::000000000000:user/alice"}`, "s3:PutObject", "arn:aws:s3:::demo/*"))
	if err := put(alice, "demo"); err != nil {
		t.Errorf("same-account user allowed by the bucket policy: %v", err)
	}
	// A Deny in the bucket policy beats an identity Allow.
	bob := iamUser(t, url, "bob", allow(`"s3:*"`, `"*"`))
	setPolicy(admin, "demo", pol("Deny", `"*"`, "s3:PutObject", "arn:aws:s3:::demo/*"))
	if code(put(bob, "demo")) != "AccessDenied" {
		t.Error("a bucket policy Deny must win")
	}

	// A Deny on the copy source stops CopyObject.
	setPolicy(admin, "secret", pol("Deny", `"*"`, "s3:GetObject", "arn:aws:s3:::secret/*"))
	_, err := bob.CopyObject(ctx, &s3.CopyObjectInput{Bucket: aws.String("dest"), Key: aws.String("c"), CopySource: aws.String("secret/k")})
	if code(err) != "AccessDenied" {
		t.Errorf("CopyObject from a source the bucket policy denies: %v", err)
	}

	// Cross account: the identity policy and the bucket policy must both allow.
	carol := iamUser(t, url, "carol", allow(`"s3:GetObject"`, `"arn:aws:s3:::xacct/*"`))
	get := func() error {
		_, err := carol.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("xacct"), Key: aws.String("k")})
		return err
	}
	if code(get()) != "AccessDenied" {
		t.Error("cross-account access without a bucket policy must be denied")
	}
	setPolicy(other, "xacct", pol("Allow", `{"AWS":"arn:aws:iam::000000000000:root"}`, "s3:GetObject", "arn:aws:s3:::xacct/*"))
	if err := get(); err != nil {
		t.Errorf("cross-account access allowed on both sides: %v", err)
	}
	dave := iamUser(t, url, "dave", allow(`"s3:ListAllMyBuckets"`, `"*"`))
	_, err = dave.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("xacct"), Key: aws.String("k")})
	if code(err) != "AccessDenied" {
		t.Errorf("cross-account access with only a bucket policy Allow: %v", err)
	}
}

// send makes one plain HTTP request (no signature) and returns status and body.
func send(t *testing.T, method, url, body string, hdr ...string) (int, string) {
	t.Helper()
	req, _ := http.NewRequest(method, url, strings.NewReader(body))
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	res := must(http.DefaultClient.Do(req))
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	return res.StatusCode, string(b)
}

// postForm sends a browser form upload with the given policy conditions and file body.
func postForm(t *testing.T, url, conditions, file string) (int, string) {
	t.Helper()
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	w.WriteField("key", "k")
	if conditions != "" {
		w.WriteField("policy", base64.StdEncoding.EncodeToString([]byte(`{"expiration":"2099-01-01T00:00:00Z","conditions":[`+conditions+`]}`)))
	}
	fw, _ := w.CreateFormFile("file", "f.txt")
	fw.Write([]byte(file))
	w.Close()
	return send(t, "POST", url, buf.String(), "Content-Type", w.FormDataContentType())
}

func TestAnonymousRequestsNeedAPublicPolicy(t *testing.T) {
	h, url := core.ServeTest(t, func(h *core.Hub) core.Service { return s3svc.New(h) })
	h.Cfg.ValidateSigs = true
	c := s3.NewFromConfig(awstest.Config(t, url), func(o *s3.Options) { o.UsePathStyle = true })
	must(c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("bkt")}))
	must(c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("bkt"), Key: aws.String("k"), Body: strings.NewReader("x")}))
	// floci #2318, #2335, #2334: with signatures validated, no credentials means no access.
	for _, r := range [][3]string{{"GET", "/bkt/k", ""}, {"PUT", "/bkt/new", "x"}, {"DELETE", "/bkt/k", ""}, {"PUT", "/anon", ""},
		{"PUT", "/bkt?policy", `{"Statement":[]}`}, {"PUT", "/bkt?versioning", `<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`}} {
		if st, _ := send(t, r[0], url+r[1], r[2]); st != 403 {
			t.Errorf("anonymous %s %s: %d, want 403", r[0], r[1], st)
		}
	}
	if st, _ := postForm(t, url+"/bkt", "", "x"); st != 403 {
		t.Errorf("anonymous form upload without a policy: %d, want 403", st)
	}
	// A bucket policy that opens GetObject to everyone lets reads in, nothing else.
	must(c.PutBucketPolicy(ctx, &s3.PutBucketPolicyInput{Bucket: aws.String("bkt"),
		Policy: aws.String(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bkt/*"}]}`)}))
	if st, _ := send(t, "GET", url+"/bkt/k", ""); st != 200 {
		t.Errorf("anonymous GET of a public object: %d", st)
	}
	if st, _ := send(t, "PUT", url+"/bkt/new", "x"); st != 403 {
		t.Errorf("anonymous PUT on a read-only public bucket: %d", st)
	}
}

func TestPostPolicyConditionsAndPresignedQuery(t *testing.T) {
	h, url := core.ServeTest(t, func(h *core.Hub) core.Service { return s3svc.New(h) })
	h.Cfg.ValidateSigs = false // the requests below are unsigned
	c := s3.NewFromConfig(awstest.Config(t, url), func(o *s3.Options) { o.UsePathStyle = true })
	must(c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("bkt")}))
	// floci #5138: a form upload must satisfy the policy's conditions.
	for _, tc := range []struct {
		size int
		want int
		code string
	}{{5, 400, "EntityTooSmall"}, {15, 204, ""}, {30, 400, "EntityTooLarge"}} {
		st, body := postForm(t, url+"/bkt", `["content-length-range",10,20],["starts-with","$key","k"]`, strings.Repeat("x", tc.size))
		if st != tc.want || !strings.Contains(body, tc.code) {
			t.Errorf("size %d: %d %s, want %d %s", tc.size, st, body, tc.want, tc.code)
		}
	}
	if st, _ := postForm(t, url+"/bkt", `["eq","$key","other"]`, "x"); st != 403 {
		t.Errorf("eq condition that does not hold: %d, want 403", st)
	}
	// floci #4752, #3608: a presigned URL carries metadata and tags in the query.
	if st, body := send(t, "PUT", url+"/bkt/q?x-amz-meta-source=presigned&x-amz-tagging=a%3Db%26c%3Dd", "body"); st != 200 {
		t.Fatalf("put: %d %s", st, body)
	}
	head := must(c.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String("bkt"), Key: aws.String("q")}))
	tg := must(c.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{Bucket: aws.String("bkt"), Key: aws.String("q")}))
	if head.Metadata["source"] != "presigned" || len(tg.TagSet) != 2 {
		t.Errorf("metadata %v, tags %v", head.Metadata, tg.TagSet)
	}
	// floci #4188: so does the checksum, and it is verified against the body.
	if st, _ := send(t, "PUT", url+"/bkt/c?x-amz-checksum-crc32=AAAAAA%3D%3D", "real content"); st != 400 {
		t.Errorf("query checksum mismatch: %d, want 400", st)
	}
}

func TestObjectDetails(t *testing.T) {
	c, _, _ := setup(t)
	b := aws.String("bkt")
	must(c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: b}))
	must(c.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{Bucket: b,
		VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled}}))
	body := func(k string) string {
		return string(must(io.ReadAll(must(c.GetObject(ctx, &s3.GetObjectInput{Bucket: b, Key: aws.String(k)})).Body)))
	}

	// floci #748: copying an old version onto its own key restores it.
	v1 := must(c.PutObject(ctx, &s3.PutObjectInput{Bucket: b, Key: aws.String("k"), Body: strings.NewReader("v1")}))
	must(c.PutObject(ctx, &s3.PutObjectInput{Bucket: b, Key: aws.String("k"), Body: strings.NewReader("v2")}))
	must(c.CopyObject(ctx, &s3.CopyObjectInput{Bucket: b, Key: aws.String("k"), CopySource: aws.String("bkt/k?versionId=" + *v1.VersionId)}))
	if got := body("k"); got != "v1" {
		t.Errorf("rollback copy: %q", got)
	}
	if _, err := c.CopyObject(ctx, &s3.CopyObjectInput{Bucket: b, Key: aws.String("k"), CopySource: aws.String("bkt/k")}); code(err) != "InvalidRequest" {
		t.Errorf("copy onto itself without a version: %v", err)
	}

	// floci #1767: x-amz-grant-* on PutObject and CopyObject show up in GetObjectAcl.
	all := aws.String(`uri="http://acs.amazonaws.com/groups/global/AllUsers"`)
	must(c.PutObject(ctx, &s3.PutObjectInput{Bucket: b, Key: aws.String("g"), Body: strings.NewReader("x"), GrantRead: all}))
	must(c.CopyObject(ctx, &s3.CopyObjectInput{Bucket: b, Key: aws.String("g2"), CopySource: aws.String("bkt/g"), GrantRead: all}))
	for _, k := range []string{"g", "g2"} {
		acl := must(c.GetObjectAcl(ctx, &s3.GetObjectAclInput{Bucket: b, Key: aws.String(k)}))
		if len(acl.Grants) != 2 || acl.Grants[1].Permission != types.PermissionRead || !strings.HasSuffix(aws.ToString(acl.Grants[1].Grantee.URI), "AllUsers") {
			t.Errorf("%s grants = %+v", k, acl.Grants)
		}
	}

	// floci #1950: a wrong checksum on CompleteMultipartUpload is refused.
	up := must(c.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: b, Key: aws.String("m"),
		ChecksumAlgorithm: types.ChecksumAlgorithmCrc32, ChecksumType: types.ChecksumTypeFullObject}))
	p := must(c.UploadPart(ctx, &s3.UploadPartInput{Bucket: b, Key: aws.String("m"), UploadId: up.UploadId, PartNumber: aws.Int32(1),
		Body: strings.NewReader("hello"), ChecksumAlgorithm: types.ChecksumAlgorithmCrc32}))
	done := &s3.CompleteMultipartUploadInput{Bucket: b, Key: aws.String("m"), UploadId: up.UploadId, ChecksumType: types.ChecksumTypeFullObject,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{{PartNumber: aws.Int32(1), ETag: p.ETag, ChecksumCRC32: p.ChecksumCRC32}}}}
	done.ChecksumCRC32 = aws.String("AQIDBA==")
	if _, err := c.CompleteMultipartUpload(ctx, done); code(err) != "BadDigest" {
		t.Errorf("wrong FULL_OBJECT checksum: %v", err)
	}
	done.ChecksumCRC32 = p.ChecksumCRC32 // one part: the object's CRC32 is the part's
	must(c.CompleteMultipartUpload(ctx, done))

	// floci #30: GetObjectAttributes reads every x-amz-object-attributes header and answers PartsCount.
	at := must(c.GetObjectAttributes(ctx, &s3.GetObjectAttributesInput{Bucket: b, Key: aws.String("m"),
		ObjectAttributes: []types.ObjectAttributes{types.ObjectAttributesEtag, types.ObjectAttributesObjectSize, types.ObjectAttributesObjectParts}}))
	if at.ETag == nil || aws.ToInt64(at.ObjectSize) != 5 || at.ObjectParts == nil || aws.ToInt32(at.ObjectParts.TotalPartsCount) != 1 {
		t.Errorf("attributes = %+v", at)
	}
}

// The low-severity floci fixes, one check each.
func TestLowSeverityFlociFixes(t *testing.T) {
	c, _, url := setup(t)
	b := aws.String("bkt")
	must(c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: b}))
	hdr := func(method, path, body string, h ...string) (int, http.Header, string) {
		req, _ := http.NewRequest(method, url+path, strings.NewReader(body))
		for i := 0; i+1 < len(h); i += 2 {
			req.Header.Set(h[i], h[i+1])
		}
		res := must(http.DefaultClient.Do(req))
		defer res.Body.Close()
		out, _ := io.ReadAll(res.Body)
		return res.StatusCode, res.Header, string(out)
	}

	// floci #5433: a new bucket has all four public access blocks on.
	pab := must(c.GetPublicAccessBlock(ctx, &s3.GetPublicAccessBlockInput{Bucket: b})).PublicAccessBlockConfiguration
	if !aws.ToBool(pab.BlockPublicAcls) || !aws.ToBool(pab.IgnorePublicAcls) || !aws.ToBool(pab.BlockPublicPolicy) || !aws.ToBool(pab.RestrictPublicBuckets) {
		t.Errorf("public access block = %+v", pab)
	}

	// floci #4167 and #74: an unknown or default (us-east-1) LocationConstraint is refused.
	for _, loc := range []types.BucketLocationConstraint{"polygondwanaland-west-1", "us-east-1"} {
		_, err := c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("loc"), CreateBucketConfiguration: &types.CreateBucketConfiguration{LocationConstraint: loc}})
		if code(err) != "InvalidLocationConstraint" {
			t.Errorf("LocationConstraint %s: %v", loc, err)
		}
	}

	// floci #4595: bucket errors name the bucket.
	for _, path := range []string{"/nobucket/k", "/bkt?tagging", "/BAD_name"} {
		method := map[bool]string{true: "PUT", false: "GET"}[path == "/BAD_name"]
		if _, _, body := hdr(method, path, ""); !strings.Contains(body, "<BucketName>") {
			t.Errorf("%s %s: no <BucketName> in %s", method, path, body)
		}
	}

	// floci #4595: BucketNotEmpty too.
	must(c.PutObject(ctx, &s3.PutObjectInput{Bucket: b, Key: aws.String("k"), Body: strings.NewReader("x")}))
	if _, _, body := hdr("DELETE", "/bkt", ""); !strings.Contains(body, "<BucketName>bkt</BucketName>") {
		t.Errorf("BucketNotEmpty: %s", body)
	}

	// floci #858 and #2144: a plain PutObject stores a CRC64NVME checksum, and the response names the KMS key.
	if _, h, _ := hdr("PUT", "/bkt/plain", "x"); h.Get("x-amz-checksum-crc64nvme") == "" {
		t.Errorf("plain PutObject: no CRC64NVME checksum")
	}
	key := "arn:aws:kms:us-east-1:000000000000:key/1234abcd-12ab-34cd-56ef-1234567890ab"
	put := must(c.PutObject(ctx, &s3.PutObjectInput{Bucket: b, Key: aws.String("kms"), Body: strings.NewReader("x"),
		ServerSideEncryption: types.ServerSideEncryptionAwsKms, SSEKMSKeyId: aws.String(key)}))
	if aws.ToString(put.SSEKMSKeyId) != key {
		t.Errorf("put: key %q", aws.ToString(put.SSEKMSKeyId))
	}

	// floci #1141: a suffix range on an empty object is a plain 200.
	must(c.PutObject(ctx, &s3.PutObjectInput{Bucket: b, Key: aws.String("empty"), Body: strings.NewReader("")}))
	if st, _, _ := hdr("GET", "/bkt/empty", "", "Range", "bytes=-1"); st != 200 {
		t.Errorf("suffix range on an empty object: %d", st)
	}

	// floci #918: response-* overrides are refused without credentials (a request with credentials may use them).
	if st, _, _ := hdr("GET", "/bkt/k?response-content-type=text/evil", ""); st != 400 {
		t.Errorf("anonymous response-content-type: %d", st)
	}
	if st, _, _ := hdr("GET", "/bkt/k?response-content-type=text/plain", "", "Authorization", "AWS4-HMAC-SHA256 Credential=test/20260101/us-east-1/s3/aws4_request"); st != 200 {
		t.Errorf("signed response-content-type: %d", st)
	}

	// floci #1231: a key made of dot segments is not a key.
	if st, _, body := hdr("PUT", "/bkt/../x/object", "x"); st != 400 || !strings.Contains(body, "InvalidURI") {
		t.Errorf("../ key: %d %s", st, body)
	}

	// floci #1766: a canned ACL shows in GetObjectAcl, and private clears it.
	must(c.PutObjectAcl(ctx, &s3.PutObjectAclInput{Bucket: b, Key: aws.String("k"), ACL: types.ObjectCannedACLPublicRead}))
	public := func() bool {
		for _, g := range must(c.GetObjectAcl(ctx, &s3.GetObjectAclInput{Bucket: b, Key: aws.String("k")})).Grants {
			if g.Grantee != nil && strings.HasSuffix(aws.ToString(g.Grantee.URI), "AllUsers") && g.Permission == types.PermissionRead {
				return true
			}
		}
		return false
	}
	if !public() {
		t.Errorf("public-read is not in GetObjectAcl")
	}
	must(c.PutObjectAcl(ctx, &s3.PutObjectAclInput{Bucket: b, Key: aws.String("k"), ACL: types.ObjectCannedACLPrivate}))
	if public() {
		t.Errorf("private left the public grant")
	}
	if _, err := c.PutObjectAcl(ctx, &s3.PutObjectAclInput{Bucket: b, Key: aws.String("missing"), ACL: types.ObjectCannedACLPrivate}); code(err) != "NoSuchKey" {
		t.Errorf("PutObjectAcl on a missing key: %v", err)
	}

	// floci #2607: a browser POST to a versioned bucket answers x-amz-version-id.
	must(c.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{Bucket: b, VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled}}))
	var form bytes.Buffer
	mw := multipart.NewWriter(&form)
	mw.WriteField("key", "posted")
	fw, _ := mw.CreateFormFile("file", "f.txt")
	fw.Write([]byte("data"))
	mw.Close()
	if _, h, _ := hdr("POST", "/bkt", form.String(), "Content-Type", mw.FormDataContentType()); h.Get("x-amz-version-id") == "" {
		t.Errorf("POST upload to a versioned bucket has no x-amz-version-id")
	}

	// floci #5636 and #5635: CompleteMultipartUpload needs a part, and an expected size must match.
	up := must(c.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: b, Key: aws.String("m")}))
	p := must(c.UploadPart(ctx, &s3.UploadPartInput{Bucket: b, Key: aws.String("m"), UploadId: up.UploadId, PartNumber: aws.Int32(1), Body: strings.NewReader("data")}))
	for _, body := range []string{"", "<CompleteMultipartUpload/>"} {
		if st, _, resp := hdr("POST", "/bkt/m?uploadId="+*up.UploadId, body); st != 400 || !strings.Contains(resp, "<Code>InvalidRequest</Code>") {
			t.Errorf("complete with body %q: %d %s", body, st, resp)
		}
	}
	done := &s3.CompleteMultipartUploadInput{Bucket: b, Key: aws.String("m"), UploadId: up.UploadId, MpuObjectSize: aws.Int64(5),
		MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{{PartNumber: aws.Int32(1), ETag: p.ETag}}}}
	if _, err := c.CompleteMultipartUpload(ctx, done); code(err) != "InvalidRequest" {
		t.Errorf("wrong MpuObjectSize: %v", err)
	}
	done.MpuObjectSize = aws.Int64(4)
	must(c.CompleteMultipartUpload(ctx, done))
}

// floci #5369: a bucket CORS rule that names the origin also allows credentials.
func TestCORSAllowCredentials(t *testing.T) {
	c, _, url := setup(t)
	must(c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("bkt")}))
	must(c.PutBucketCors(ctx, &s3.PutBucketCorsInput{Bucket: aws.String("bkt"), CORSConfiguration: &types.CORSConfiguration{CORSRules: []types.CORSRule{
		{AllowedOrigins: []string{"http://localhost:3000"}, AllowedMethods: []string{"GET"}}}}}))
	req, _ := http.NewRequest("OPTIONS", url+"/bkt/k", nil)
	req.Header.Set("Origin", "http://localhost:3000")
	req.Header.Set("Access-Control-Request-Method", "GET")
	res := must(http.DefaultClient.Do(req))
	res.Body.Close()
	if res.Header.Get("Access-Control-Allow-Origin") != "http://localhost:3000" || res.Header.Get("Access-Control-Allow-Credentials") != "true" {
		t.Errorf("preflight headers: %v", res.Header)
	}
}

// A paging parameter that is negative or too big for 32 bits is a 400; one above 1000 is capped.
func TestPagingParameters(t *testing.T) {
	c, _, url := setup(t)
	must(c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("pages")}))
	_, err := c.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String("pages"), MaxKeys: aws.Int32(-1)})
	if code(err) != "InvalidArgument" {
		t.Fatalf("max-keys=-1: %v", err)
	}
	must(c.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String("pages"), MaxKeys: aws.Int32(5000)}))
	for _, q := range []string{"max-keys=2147483648", "max-keys=abc", "uploads&max-uploads=-1", "versions&max-keys=abc"} {
		req, _ := http.NewRequest("GET", url+"/pages?"+q, nil)
		awstest.Sign(t, req, "s3")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != 400 {
			t.Errorf("%s: status %d", q, resp.StatusCode)
		}
	}
}
