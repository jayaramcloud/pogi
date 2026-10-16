package firehose_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/firehose"
	"github.com/aws/aws-sdk-go-v2/service/firehose/types"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	firehosesvc "github.com/nrndev/pogi/internal/aws/firehose"
	kinesissvc "github.com/nrndev/pogi/internal/aws/kinesis"
	s3svc "github.com/nrndev/pogi/internal/aws/s3"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/core"
)

var ctx = context.Background()

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

type env struct {
	fh  *firehose.Client
	s3  *s3.Client
	kin *kinesis.Client
	hub *core.Hub
}

func setup(t *testing.T) env {
	h, url := core.ServeTest(t,
		func(h *core.Hub) core.Service { return s3svc.New(h) },
		func(h *core.Hub) core.Service { return kinesissvc.New(h) },
		func(h *core.Hub) core.Service { return firehosesvc.New(h) })
	cfg := awstest.Config(t, url)
	return env{firehose.NewFromConfig(cfg), s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true }), kinesis.NewFromConfig(cfg), h}
}

func (e env) flush() { e.hub.Service("firehose").(*firehosesvc.Service).Flush() }

// objects returns key → body for everything in a bucket.
func (e env) objects(t *testing.T, bucket string) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, o := range must(e.s3.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket)})).Contents {
		get := must(e.s3.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: o.Key}))
		b, _ := io.ReadAll(get.Body)
		out[*o.Key] = string(b)
	}
	return out
}

func s3Dest(bucket, prefix string) *types.ExtendedS3DestinationConfiguration {
	return &types.ExtendedS3DestinationConfiguration{BucketARN: aws.String("arn:aws:s3:::" + bucket), RoleARN: aws.String("arn:aws:iam::000000000000:role/firehose"), Prefix: aws.String(prefix)}
}

var objectKey = regexp.MustCompile(`^raw/\d{4}/\d{2}/\d{2}/\d{2}/clicks-1-\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2}-[0-9a-f-]{36}$`)

func TestDirectPutToS3(t *testing.T) {
	e := setup(t)
	must(e.s3.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("logs")}))
	dest := s3Dest("logs", "raw/")
	dest.ErrorOutputPrefix = aws.String("errors/")
	out := must(e.fh.CreateDeliveryStream(ctx, &firehose.CreateDeliveryStreamInput{DeliveryStreamName: aws.String("clicks"), ExtendedS3DestinationConfiguration: dest}))
	if *out.DeliveryStreamARN != "arn:aws:firehose:us-east-1:000000000000:deliverystream/clicks" {
		t.Fatalf("arn = %s", *out.DeliveryStreamARN)
	}
	must(e.fh.PutRecord(ctx, &firehose.PutRecordInput{DeliveryStreamName: aws.String("clicks"), Record: &types.Record{Data: []byte("a\n")}}))
	batch := must(e.fh.PutRecordBatch(ctx, &firehose.PutRecordBatchInput{DeliveryStreamName: aws.String("clicks"),
		Records: []types.Record{{Data: []byte("b\n")}, {Data: []byte("c\n")}}}))
	if *batch.FailedPutCount != 0 || len(batch.RequestResponses) != 2 || batch.RequestResponses[1].RecordId == nil {
		t.Fatalf("batch = %+v", batch)
	}
	if err := e.hub.Deliver(*out.DeliveryStreamARN, &core.Message{Body: "d\n"}); err != nil {
		t.Fatal(err)
	}
	if got := e.objects(t, "logs"); len(got) != 0 {
		t.Fatalf("nothing is due before 5 MiB or 300 s, got %v", got)
	}
	e.flush()
	got := e.objects(t, "logs")
	if len(got) != 1 {
		t.Fatalf("objects = %v", got)
	}
	for k, body := range got {
		if !objectKey.MatchString(k) || body != "a\nb\nc\nd\n" {
			t.Fatalf("object %q = %q", k, body)
		}
	}
	e.flush() // nothing buffered: no empty object
	if n := len(e.objects(t, "logs")); n != 1 {
		t.Fatalf("objects after an idle flush = %d", n)
	}
}

func TestGzipOnInterval(t *testing.T) {
	e := setup(t)
	must(e.s3.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("zipped")}))
	dest := s3Dest("zipped", "gz/")
	dest.CompressionFormat = types.CompressionFormatGzip
	dest.BufferingHints = &types.BufferingHints{IntervalInSeconds: aws.Int32(1), SizeInMBs: aws.Int32(1)}
	must(e.fh.CreateDeliveryStream(ctx, &firehose.CreateDeliveryStreamInput{DeliveryStreamName: aws.String("zip"), ExtendedS3DestinationConfiguration: dest}))
	must(e.fh.PutRecordBatch(ctx, &firehose.PutRecordBatchInput{DeliveryStreamName: aws.String("zip"),
		Records: []types.Record{{Data: []byte("hello ")}, {Data: []byte("world")}}}))
	// No Flush: the background loop delivers once the 1 s interval passes.
	var got map[string]string
	for deadline := time.Now().Add(5 * time.Second); len(got) == 0 && time.Now().Before(deadline); time.Sleep(100 * time.Millisecond) {
		got = e.objects(t, "zipped")
	}
	if len(got) != 1 {
		t.Fatalf("objects = %v", got)
	}
	for k, body := range got {
		zr, err := gzip.NewReader(bytes.NewReader([]byte(body)))
		if err != nil {
			t.Fatal(err)
		}
		plain, _ := io.ReadAll(zr)
		if !regexp.MustCompile(`^gz/.*\.gz$`).MatchString(k) || string(plain) != "hello world" {
			t.Fatalf("object %q = %q", k, plain)
		}
	}
}

func TestHTTPEndpoint(t *testing.T) {
	e := setup(t)
	type request struct {
		Key     string
		Records []string
	}
	var mu sync.Mutex
	var reqs []request
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			RequestID string `json:"requestId"`
			Records   []struct {
				Data []byte `json:"data"`
			} `json:"records"`
		}
		json.NewDecoder(r.Body).Decode(&body)
		req := request{Key: r.Header.Get("X-Amz-Firehose-Access-Key")}
		for _, rec := range body.Records {
			req.Records = append(req.Records, string(rec.Data))
		}
		mu.Lock()
		reqs = append(reqs, req)
		mu.Unlock()
		json.NewEncoder(w).Encode(map[string]any{"requestId": body.RequestID, "timestamp": time.Now().UnixMilli()})
	}))
	defer srv.Close()
	must(e.fh.CreateDeliveryStream(ctx, &firehose.CreateDeliveryStreamInput{DeliveryStreamName: aws.String("hook"),
		HttpEndpointDestinationConfiguration: &types.HttpEndpointDestinationConfiguration{
			EndpointConfiguration: &types.HttpEndpointConfiguration{Url: aws.String(srv.URL), Name: aws.String("mine"), AccessKey: aws.String("s3cr3t")},
			S3Configuration:       &types.S3DestinationConfiguration{BucketARN: aws.String("arn:aws:s3:::backup"), RoleARN: aws.String("arn:aws:iam::000000000000:role/firehose")},
		}}))
	must(e.fh.PutRecordBatch(ctx, &firehose.PutRecordBatchInput{DeliveryStreamName: aws.String("hook"),
		Records: []types.Record{{Data: []byte(`{"n":1}`)}, {Data: []byte(`{"n":2}`)}}}))
	e.flush()
	mu.Lock()
	defer mu.Unlock()
	if len(reqs) != 1 || reqs[0].Key != "s3cr3t" || len(reqs[0].Records) != 2 || reqs[0].Records[1] != `{"n":2}` {
		t.Fatalf("endpoint got %+v", reqs)
	}
	d := must(e.fh.DescribeDeliveryStream(ctx, &firehose.DescribeDeliveryStreamInput{DeliveryStreamName: aws.String("hook")})).DeliveryStreamDescription
	h := d.Destinations[0].HttpEndpointDestinationDescription
	if h == nil || *h.EndpointConfiguration.Url != srv.URL || *h.S3DestinationDescription.BucketARN != "arn:aws:s3:::backup" || *h.BufferingHints.IntervalInSeconds != 300 {
		t.Fatalf("http destination = %+v", h)
	}
}

func TestKinesisSource(t *testing.T) {
	e := setup(t)
	must(e.s3.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("fromkinesis")}))
	must(e.kin.CreateStream(ctx, &kinesis.CreateStreamInput{StreamName: aws.String("src"), ShardCount: aws.Int32(1)}))
	streamARN := aws.String("arn:aws:kinesis:us-east-1:000000000000:stream/src")
	must(e.kin.PutRecord(ctx, &kinesis.PutRecordInput{StreamARN: streamARN, PartitionKey: aws.String("p"), Data: []byte("old")}))
	time.Sleep(20 * time.Millisecond) // records older than the Firehose stream are not read
	must(e.fh.CreateDeliveryStream(ctx, &firehose.CreateDeliveryStreamInput{DeliveryStreamName: aws.String("pipe"),
		DeliveryStreamType:                 types.DeliveryStreamTypeKinesisStreamAsSource,
		KinesisStreamSourceConfiguration:   &types.KinesisStreamSourceConfiguration{KinesisStreamARN: streamARN, RoleARN: aws.String("arn:aws:iam::000000000000:role/firehose")},
		ExtendedS3DestinationConfiguration: s3Dest("fromkinesis", ""),
	}))
	for _, d := range []string{"k1|", "k2|"} {
		must(e.kin.PutRecord(ctx, &kinesis.PutRecordInput{StreamARN: streamARN, PartitionKey: aws.String("p"), Data: []byte(d)}))
	}
	e.flush()
	e.flush() // checkpoints hold: nothing is read twice
	got := e.objects(t, "fromkinesis")
	if len(got) != 1 {
		t.Fatalf("objects = %v", got)
	}
	for _, body := range got {
		if body != "k1|k2|" {
			t.Fatalf("delivered %q", body)
		}
	}
	_, err := e.fh.PutRecord(ctx, &firehose.PutRecordInput{DeliveryStreamName: aws.String("pipe"), Record: &types.Record{Data: []byte("x")}})
	var bad *types.InvalidArgumentException
	if !errors.As(err, &bad) {
		t.Fatalf("PutRecord on a Kinesis-sourced stream: want InvalidArgumentException, got %v", err)
	}
	src := must(e.fh.DescribeDeliveryStream(ctx, &firehose.DescribeDeliveryStreamInput{DeliveryStreamName: aws.String("pipe")})).DeliveryStreamDescription.Source
	if *src.KinesisStreamSourceDescription.KinesisStreamARN != *streamARN {
		t.Fatalf("source = %+v", src)
	}
}

func TestLifecycle(t *testing.T) {
	e := setup(t)
	name := aws.String("plain")
	must(e.fh.CreateDeliveryStream(ctx, &firehose.CreateDeliveryStreamInput{DeliveryStreamName: name,
		S3DestinationConfiguration: &types.S3DestinationConfiguration{BucketARN: aws.String("arn:aws:s3:::b"), RoleARN: aws.String("arn:aws:iam::000000000000:role/r")},
		Tags:                       []types.Tag{{Key: aws.String("team"), Value: aws.String("data")}}}))
	_, err := e.fh.CreateDeliveryStream(ctx, &firehose.CreateDeliveryStreamInput{DeliveryStreamName: name, ExtendedS3DestinationConfiguration: s3Dest("b", "")})
	var inUse *types.ResourceInUseException
	if !errors.As(err, &inUse) {
		t.Fatalf("want ResourceInUseException, got %v", err)
	}
	d := must(e.fh.DescribeDeliveryStream(ctx, &firehose.DescribeDeliveryStreamInput{DeliveryStreamName: name})).DeliveryStreamDescription
	dst := d.Destinations[0]
	if d.DeliveryStreamStatus != types.DeliveryStreamStatusActive || d.DeliveryStreamType != types.DeliveryStreamTypeDirectPut || *d.VersionId != "1" ||
		*dst.DestinationId != "destinationId-000000000001" || *dst.S3DestinationDescription.BucketARN != "arn:aws:s3:::b" ||
		dst.ExtendedS3DestinationDescription == nil || dst.S3DestinationDescription.CompressionFormat != types.CompressionFormatUncompressed ||
		*dst.S3DestinationDescription.BufferingHints.SizeInMBs != 5 {
		t.Fatalf("describe = %+v %+v", d, dst.S3DestinationDescription)
	}
	if l := must(e.fh.ListDeliveryStreams(ctx, &firehose.ListDeliveryStreamsInput{})); len(l.DeliveryStreamNames) != 1 || *l.HasMoreDeliveryStreams {
		t.Fatalf("list = %+v", l)
	}

	upd := &firehose.UpdateDestinationInput{DeliveryStreamName: name, CurrentDeliveryStreamVersionId: aws.String("7"), DestinationId: dst.DestinationId,
		ExtendedS3DestinationUpdate: &types.ExtendedS3DestinationUpdate{Prefix: aws.String("new/"), BufferingHints: &types.BufferingHints{IntervalInSeconds: aws.Int32(60)}}}
	_, err = e.fh.UpdateDestination(ctx, upd)
	var conflict *types.ConcurrentModificationException
	if !errors.As(err, &conflict) {
		t.Fatalf("stale version: want ConcurrentModificationException, got %v", err)
	}
	upd.CurrentDeliveryStreamVersionId = d.VersionId
	must(e.fh.UpdateDestination(ctx, upd))
	d = must(e.fh.DescribeDeliveryStream(ctx, &firehose.DescribeDeliveryStreamInput{DeliveryStreamName: name})).DeliveryStreamDescription
	ext := d.Destinations[0].ExtendedS3DestinationDescription
	if *d.VersionId != "2" || *ext.Prefix != "new/" || *ext.BucketARN != "arn:aws:s3:::b" || *ext.BufferingHints.IntervalInSeconds != 60 ||
		*ext.BufferingHints.SizeInMBs != 5 || d.LastUpdateTimestamp == nil {
		t.Fatalf("after update = %+v %+v", d, ext)
	}

	must(e.fh.TagDeliveryStream(ctx, &firehose.TagDeliveryStreamInput{DeliveryStreamName: name, Tags: []types.Tag{{Key: aws.String("env"), Value: aws.String("dev")}}}))
	must(e.fh.UntagDeliveryStream(ctx, &firehose.UntagDeliveryStreamInput{DeliveryStreamName: name, TagKeys: []string{"team"}}))
	if tags := must(e.fh.ListTagsForDeliveryStream(ctx, &firehose.ListTagsForDeliveryStreamInput{DeliveryStreamName: name})); len(tags.Tags) != 1 || *tags.Tags[0].Key != "env" {
		t.Fatalf("tags = %+v", tags.Tags)
	}
	must(e.fh.StartDeliveryStreamEncryption(ctx, &firehose.StartDeliveryStreamEncryptionInput{DeliveryStreamName: name}))
	if put := must(e.fh.PutRecord(ctx, &firehose.PutRecordInput{DeliveryStreamName: name, Record: &types.Record{Data: []byte("x")}})); !*put.Encrypted {
		t.Fatal("records on an encrypted stream report Encrypted")
	}
	enc := must(e.fh.DescribeDeliveryStream(ctx, &firehose.DescribeDeliveryStreamInput{DeliveryStreamName: name})).DeliveryStreamDescription.DeliveryStreamEncryptionConfiguration
	if enc.Status != types.DeliveryStreamEncryptionStatusEnabled || enc.KeyType != types.KeyTypeAwsOwnedCmk {
		t.Fatalf("encryption = %+v", enc)
	}

	must(e.fh.DeleteDeliveryStream(ctx, &firehose.DeleteDeliveryStreamInput{DeliveryStreamName: name}))
	var nf *types.ResourceNotFoundException
	if _, err = e.fh.DescribeDeliveryStream(ctx, &firehose.DescribeDeliveryStreamInput{DeliveryStreamName: name}); !errors.As(err, &nf) {
		t.Fatalf("describe deleted: want ResourceNotFoundException, got %v", err)
	}
	if _, err = e.fh.PutRecord(ctx, &firehose.PutRecordInput{DeliveryStreamName: name, Record: &types.Record{Data: []byte("x")}}); !errors.As(err, &nf) {
		t.Fatalf("put to deleted: want ResourceNotFoundException, got %v", err)
	}
}

// Records stay buffered while delivery fails, and survive a snapshot.
func TestBufferSurvivesFailureAndSnapshot(t *testing.T) {
	e := setup(t)
	must(e.fh.CreateDeliveryStream(ctx, &firehose.CreateDeliveryStreamInput{DeliveryStreamName: aws.String("later"), ExtendedS3DestinationConfiguration: s3Dest("notyet", "")}))
	must(e.fh.PutRecord(ctx, &firehose.PutRecordInput{DeliveryStreamName: aws.String("later"), Record: &types.Record{Data: []byte("kept")}}))
	e.flush() // fails: no bucket yet
	snap := must(e.hub.Snapshot())
	e.hub.Reset()
	if err := e.hub.Restore(snap); err != nil {
		t.Fatal(err)
	}
	if rows := e.hub.Resources("firehose")["firehose"][0].Rows; len(rows) != 1 || rows[0][3] != "s3://notyet/" || rows[0][4] != "1" {
		t.Fatalf("console rows = %v", rows)
	}
	must(e.s3.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("notyet")}))
	e.flush()
	got := e.objects(t, "notyet")
	if len(got) != 1 {
		t.Fatalf("objects = %v", got)
	}
	for _, body := range got {
		if body != "kept" {
			t.Fatalf("delivered %q", body)
		}
	}
}

// floci #1857: !{timestamp:…} and !{firehose:random-string} in a prefix are filled in, with no extra date folders.
// floci #2200: an ExtendedS3 destination reports S3BackupMode Disabled when none was set.
func TestPrefixExpressionsAndBackupMode(t *testing.T) {
	e := setup(t)
	must(e.s3.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("expr")}))
	must(e.fh.CreateDeliveryStream(ctx, &firehose.CreateDeliveryStreamInput{DeliveryStreamName: aws.String("dyn"),
		ExtendedS3DestinationConfiguration: s3Dest("expr", "d/!{timestamp:yyyy/MM/dd}/!{firehose:random-string}/")}))
	must(e.fh.PutRecord(ctx, &firehose.PutRecordInput{DeliveryStreamName: aws.String("dyn"), Record: &types.Record{Data: []byte("a\n")}}))
	e.flush()
	re := regexp.MustCompile(`^d/\d{4}/\d{2}/\d{2}/[A-Za-z0-9]{11}/dyn-1-`)
	got := e.objects(t, "expr")
	if len(got) != 1 {
		t.Fatalf("objects = %v", got)
	}
	for k := range got {
		if !re.MatchString(k) {
			t.Fatalf("key %q does not match %s", k, re)
		}
	}
	d := must(e.fh.DescribeDeliveryStream(ctx, &firehose.DescribeDeliveryStreamInput{DeliveryStreamName: aws.String("dyn")}))
	if x := d.DeliveryStreamDescription.Destinations[0].ExtendedS3DestinationDescription; x == nil || x.S3BackupMode != types.S3BackupModeDisabled {
		t.Fatalf("S3BackupMode = %+v", x)
	}
}
