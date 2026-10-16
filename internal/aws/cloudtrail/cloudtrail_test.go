package cloudtrail_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudtrail"
	"github.com/aws/aws-sdk-go-v2/service/cloudtrail/types"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	cloudtrailsvc "github.com/nrndev/pogi/internal/aws/cloudtrail"
	logssvc "github.com/nrndev/pogi/internal/aws/logs"
	s3svc "github.com/nrndev/pogi/internal/aws/s3"
	sqssvc "github.com/nrndev/pogi/internal/aws/sqs"
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

type clients struct {
	ct   *cloudtrail.Client
	s3   *s3.Client
	sqs  *sqs.Client
	logs *cloudwatchlogs.Client
	svc  *cloudtrailsvc.Service
}

func setup(t *testing.T) clients {
	var svc *cloudtrailsvc.Service
	_, url := core.ServeTest(t, func(h *core.Hub) core.Service { return s3svc.New(h) },
		func(h *core.Hub) core.Service { return sqssvc.New(h) },
		func(h *core.Hub) core.Service { return logssvc.New(h) },
		func(h *core.Hub) core.Service { svc = cloudtrailsvc.New(h); return svc })
	cfg := awstest.Config(t, url)
	return clients{cloudtrail.NewFromConfig(cfg), s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true }),
		sqs.NewFromConfig(cfg), cloudwatchlogs.NewFromConfig(cfg), svc}
}

func lookup(t *testing.T, c *cloudtrail.Client, key types.LookupAttributeKey, value string) []types.Event {
	t.Helper()
	in := &cloudtrail.LookupEventsInput{LookupAttributes: []types.LookupAttribute{{AttributeKey: key, AttributeValue: aws.String(value)}}}
	return must(c.LookupEvents(ctx, in)).Events
}

func names(evs []types.Event) []string {
	var out []string
	for _, e := range evs {
		out = append(out, *e.EventName)
	}
	return out
}

func TestLookupEvents(t *testing.T) {
	c := setup(t)
	must(c.s3.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("logs")}))
	must(c.s3.ListBuckets(ctx, &s3.ListBucketsInput{}))
	q := must(c.sqs.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("orders")})).QueueUrl
	must(c.sqs.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: q, MessageBody: aws.String("hi")}))
	if _, err := c.sqs.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String("nope")}); err == nil {
		t.Fatal("want an error")
	}

	evs := lookup(t, c.ct, types.LookupAttributeKeyEventName, "SendMessage")
	if len(evs) != 1 {
		t.Fatalf("SendMessage events %+v", evs)
	}
	e := evs[0]
	if *e.EventSource != "sqs.amazonaws.com" || *e.ReadOnly != "false" || *e.AccessKeyId != "test" || *e.Username != "root" ||
		time.Since(*e.EventTime) > time.Minute || len(*e.EventId) != 36 {
		t.Fatalf("event %+v", e)
	}
	var rec struct {
		EventVersion, EventSource, EventName, AWSRegion, RequestID, EventID, EventType, RecipientAccountID, UserAgent string
		ReadOnly, ManagementEvent                                                                                     bool
		UserIdentity                                                                                                  struct{ Type, PrincipalID, ARN, AccountID, AccessKeyID string }
		RequestParameters                                                                                             struct{ MessageBody, QueueUrl string }
		ResponseElements                                                                                              struct{ MessageId string }
	}
	if err := json.Unmarshal([]byte(*e.CloudTrailEvent), &rec); err != nil {
		t.Fatal(err)
	}
	id := rec.UserIdentity
	if rec.EventVersion != "1.09" || rec.EventName != "SendMessage" || rec.AWSRegion != "us-east-1" || rec.EventID != *e.EventId ||
		rec.EventType != "AwsApiCall" || !rec.ManagementEvent || rec.ReadOnly || rec.RecipientAccountID != "000000000000" || rec.RequestID == "" ||
		!strings.Contains(rec.UserAgent, "aws-sdk-go-v2") || id.Type != "Root" || id.ARN != "arn:aws:iam::000000000000:root" ||
		id.AccountID != "000000000000" || id.AccessKeyID != "test" || rec.RequestParameters.MessageBody != "hi" ||
		rec.RequestParameters.QueueUrl != *q || rec.ResponseElements.MessageId == "" {
		t.Fatalf("record %s", *e.CloudTrailEvent)
	}
	if !slices.ContainsFunc(e.Resources, func(r types.Resource) bool { return *r.ResourceType == "AWS::SQS::Queue" && *r.ResourceName == *q }) {
		t.Fatalf("resources %+v", e.Resources)
	}

	if got := names(lookup(t, c.ct, types.LookupAttributeKeyEventSource, "s3.amazonaws.com")); !slices.Equal(got, []string{"ListBuckets", "CreateBucket"}) {
		t.Fatalf("S3 events newest first: %v", got)
	}
	if got := names(lookup(t, c.ct, types.LookupAttributeKeyReadOnly, "true")); !slices.Equal(got, []string{"GetQueueUrl", "ListBuckets"}) {
		t.Fatalf("read-only events %v", got)
	}
	if got := names(lookup(t, c.ct, types.LookupAttributeKeyResourceName, "orders")); !slices.Equal(got, []string{"CreateQueue"}) {
		t.Fatalf("events about the queue %v", got)
	}
	if got := names(lookup(t, c.ct, types.LookupAttributeKeyResourceName, "logs")); !slices.Equal(got, []string{"CreateBucket"}) {
		t.Fatalf("events about the bucket %v", got)
	}
	failed := lookup(t, c.ct, types.LookupAttributeKeyEventName, "GetQueueUrl")
	if len(failed) != 1 || !strings.Contains(*failed[0].CloudTrailEvent, `"errorCode":"QueueDoesNotExist"`) ||
		!strings.Contains(*failed[0].CloudTrailEvent, `"errorMessage":"The specified queue does not exist`) {
		t.Fatalf("failed call %+v", failed)
	}
	if got := lookup(t, c.ct, types.LookupAttributeKeyEventId, *e.EventId); len(got) != 1 || *got[0].EventName != "SendMessage" {
		t.Fatalf("by id %+v", got)
	}

	// pages, newest first, and a time window
	var all []string
	in := &cloudtrail.LookupEventsInput{MaxResults: aws.Int32(2), LookupAttributes: []types.LookupAttribute{{AttributeKey: types.LookupAttributeKeyUsername, AttributeValue: aws.String("root")}}}
	for p := cloudtrail.NewLookupEventsPaginator(c.ct, in); p.HasMorePages(); {
		page := must(p.NextPage(ctx))
		if len(page.Events) > 2 {
			t.Fatalf("page of %d", len(page.Events))
		}
		all = append(all, names(page.Events)...)
	}
	if len(all) < 6 || all[len(all)-1] != "CreateBucket" || slices.Index(all, "SendMessage") > slices.Index(all, "CreateQueue") {
		t.Fatalf("all pages %v", all)
	}
	old := must(c.ct.LookupEvents(ctx, &cloudtrail.LookupEventsInput{EndTime: aws.Time(time.Now().Add(-time.Hour))}))
	if len(old.Events) != 0 {
		t.Fatalf("nothing is an hour old: %v", names(old.Events))
	}
	_, err := c.ct.LookupEvents(ctx, &cloudtrail.LookupEventsInput{StartTime: aws.Time(time.Now()), EndTime: aws.Time(time.Now().Add(-time.Hour))})
	if e := (*types.InvalidTimeRangeException)(nil); !errors.As(err, &e) {
		t.Fatalf("want InvalidTimeRangeException, got %v", err)
	}

	// the history outlives the request log's 1000 calls
	for range 1001 {
		must(c.sqs.ListQueues(ctx, &sqs.ListQueuesInput{}))
	}
	if got := lookup(t, c.ct, types.LookupAttributeKeyEventName, "CreateBucket"); len(got) != 1 {
		t.Fatalf("CreateBucket after 1001 more calls: %+v", got)
	}
}

func TestTrails(t *testing.T) {
	c := setup(t)
	must(c.s3.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("trail-logs")}))
	_, err := c.ct.CreateTrail(ctx, &cloudtrail.CreateTrailInput{Name: aws.String("main"), S3BucketName: aws.String("missing")})
	if e := (*types.S3BucketDoesNotExistException)(nil); !errors.As(err, &e) {
		t.Fatalf("want S3BucketDoesNotExistException, got %v", err)
	}
	group := "arn:aws:logs:us-east-1:000000000000:log-group:audit:*"
	tr := must(c.ct.CreateTrail(ctx, &cloudtrail.CreateTrailInput{Name: aws.String("main"), S3BucketName: aws.String("trail-logs"),
		S3KeyPrefix: aws.String("audit"), IsMultiRegionTrail: aws.Bool(true), EnableLogFileValidation: aws.Bool(true),
		CloudWatchLogsLogGroupArn: aws.String(group), CloudWatchLogsRoleArn: aws.String("arn:aws:iam::000000000000:role/trail"),
		TagsList: []types.Tag{{Key: aws.String("team"), Value: aws.String("audit")}}}))
	if *tr.TrailARN != "arn:aws:cloudtrail:us-east-1:000000000000:trail/main" || !*tr.IsMultiRegionTrail || !*tr.LogFileValidationEnabled || !*tr.IncludeGlobalServiceEvents {
		t.Fatalf("trail %+v", tr)
	}
	_, err = c.ct.CreateTrail(ctx, &cloudtrail.CreateTrailInput{Name: aws.String("main"), S3BucketName: aws.String("trail-logs")})
	if e := (*types.TrailAlreadyExistsException)(nil); !errors.As(err, &e) {
		t.Fatalf("want TrailAlreadyExistsException, got %v", err)
	}
	if st := must(c.ct.GetTrailStatus(ctx, &cloudtrail.GetTrailStatusInput{Name: aws.String("main")})); *st.IsLogging {
		t.Fatal("a new trail does not log")
	}
	q := must(c.sqs.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("jobs")})).QueueUrl
	must(c.ct.StartLogging(ctx, &cloudtrail.StartLoggingInput{Name: tr.TrailARN}))
	must(c.sqs.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: q, MessageBody: aws.String("logged")}))
	must(c.sqs.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: q}))

	c.svc.Tick(time.Now())
	records := delivered(t, c, 1)
	if !slices.Contains(records, "SendMessage") || !slices.Contains(records, "GetQueueAttributes") || slices.Contains(records, "CreateQueue") {
		t.Fatalf("the first file has the calls since StartLogging: %v", records)
	}
	st := must(c.ct.GetTrailStatus(ctx, &cloudtrail.GetTrailStatusInput{Name: aws.String("main")}))
	if !*st.IsLogging || st.LatestDeliveryTime == nil || st.StartLoggingTime == nil || st.LatestDeliveryError != nil || st.LatestCloudWatchLogsDeliveryTime == nil {
		t.Fatalf("status %+v", st)
	}
	logged := must(c.logs.GetLogEvents(ctx, &cloudwatchlogs.GetLogEventsInput{LogGroupName: aws.String("audit"),
		LogStreamName: aws.String("000000000000_CloudTrail_us-east-1")})).Events
	if len(logged) != len(records) || !strings.Contains(*logged[0].Message, `"eventVersion":"1.09"`) {
		t.Fatalf("CloudWatch Logs got %d of %d records", len(logged), len(records))
	}

	// write-only selectors leave reads out
	must(c.ct.PutEventSelectors(ctx, &cloudtrail.PutEventSelectorsInput{TrailName: aws.String("main"),
		EventSelectors: []types.EventSelector{{ReadWriteType: types.ReadWriteTypeWriteOnly, IncludeManagementEvents: aws.Bool(true)}}}))
	sel := must(c.ct.GetEventSelectors(ctx, &cloudtrail.GetEventSelectorsInput{TrailName: aws.String("main")}))
	if len(sel.EventSelectors) != 1 || sel.EventSelectors[0].ReadWriteType != types.ReadWriteTypeWriteOnly {
		t.Fatalf("selectors %+v", sel)
	}
	must(c.sqs.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: q, MessageBody: aws.String("again")}))
	must(c.sqs.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: q}))
	c.svc.Tick(time.Now().Add(time.Minute))
	if records = delivered(t, c, 2); !slices.Contains(records, "SendMessage") || slices.Contains(records, "GetQueueAttributes") {
		t.Fatalf("write-only file: %v", records)
	}

	if got := must(c.ct.DescribeTrails(ctx, &cloudtrail.DescribeTrailsInput{})).TrailList; len(got) != 1 || !*got[0].HasCustomEventSelectors ||
		*got[0].CloudWatchLogsLogGroupArn != group || *got[0].S3KeyPrefix != "audit" {
		t.Fatalf("describe %+v", got)
	}
	if got := must(c.ct.ListTrails(ctx, &cloudtrail.ListTrailsInput{})).Trails; len(got) != 1 || *got[0].HomeRegion != "us-east-1" {
		t.Fatalf("list %+v", got)
	}
	tags := must(c.ct.ListTags(ctx, &cloudtrail.ListTagsInput{ResourceIdList: []string{*tr.TrailARN}})).ResourceTagList
	if len(tags) != 1 || len(tags[0].TagsList) != 1 || *tags[0].TagsList[0].Value != "audit" {
		t.Fatalf("tags %+v", tags)
	}
	_, err = c.ct.GetInsightSelectors(ctx, &cloudtrail.GetInsightSelectorsInput{TrailName: aws.String("main")})
	if e := (*types.InsightNotEnabledException)(nil); !errors.As(err, &e) {
		t.Fatalf("want InsightNotEnabledException, got %v", err)
	}
	must(c.ct.StopLogging(ctx, &cloudtrail.StopLoggingInput{Name: aws.String("main")}))
	must(c.ct.DeleteTrail(ctx, &cloudtrail.DeleteTrailInput{Name: aws.String("main")}))
	_, err = c.ct.GetTrail(ctx, &cloudtrail.GetTrailInput{Name: aws.String("main")})
	if e := (*types.TrailNotFoundException)(nil); !errors.As(err, &e) {
		t.Fatalf("want TrailNotFoundException, got %v", err)
	}

	// event data stores are metadata
	ds := must(c.ct.CreateEventDataStore(ctx, &cloudtrail.CreateEventDataStoreInput{Name: aws.String("lake")}))
	if ds.Status != types.EventDataStoreStatusEnabled || !*ds.TerminationProtectionEnabled || *ds.RetentionPeriod != 2557 || len(ds.AdvancedEventSelectors) != 1 {
		t.Fatalf("event data store %+v", ds)
	}
	_, err = c.ct.DeleteEventDataStore(ctx, &cloudtrail.DeleteEventDataStoreInput{EventDataStore: ds.EventDataStoreArn})
	if e := (*types.EventDataStoreTerminationProtectedException)(nil); !errors.As(err, &e) {
		t.Fatalf("want EventDataStoreTerminationProtectedException, got %v", err)
	}
	must(c.ct.UpdateEventDataStore(ctx, &cloudtrail.UpdateEventDataStoreInput{EventDataStore: ds.EventDataStoreArn, TerminationProtectionEnabled: aws.Bool(false)}))
	must(c.ct.DeleteEventDataStore(ctx, &cloudtrail.DeleteEventDataStoreInput{EventDataStore: ds.EventDataStoreArn}))
	if got := must(c.ct.ListEventDataStores(ctx, &cloudtrail.ListEventDataStoresInput{})).EventDataStores; len(got) != 0 {
		t.Fatalf("stores left %+v", got)
	}
}

var logKey = regexp.MustCompile(`^audit/AWSLogs/000000000000/CloudTrail/us-east-1/\d{4}/\d\d/\d\d/000000000000_CloudTrail_us-east-1_\d{8}T\d{4}Z_[A-Za-z0-9]{16}\.json\.gz$`)

// delivered checks that the trail's bucket holds n log files and returns
// the event names in the newest one.
func delivered(t *testing.T, c clients, n int) []string {
	t.Helper()
	objs := must(c.s3.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String("trail-logs")})).Contents
	if len(objs) != n {
		t.Fatalf("%d log files, want %d", len(objs), n)
	}
	newest := slices.MaxFunc(objs, func(a, b s3types.Object) int { return strings.Compare(*a.Key, *b.Key) }) // keys hold the time
	if !logKey.MatchString(*newest.Key) {
		t.Fatalf("log file key %s", *newest.Key)
	}
	body := must(io.ReadAll(must(c.s3.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("trail-logs"), Key: newest.Key})).Body))
	var file struct{ Records []struct{ EventName string } }
	if err := json.NewDecoder(must(gzip.NewReader(bytes.NewReader(body)))).Decode(&file); err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, r := range file.Records {
		out = append(out, r.EventName)
	}
	return out
}
