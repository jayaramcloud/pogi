package generic_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudcontrol"
	"github.com/aws/aws-sdk-go-v2/service/cloudcontrol/types"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/efs"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/smithy-go"

	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestCloudControl(t *testing.T) {
	s := pogi.Start(t)
	ctx := context.Background()
	cfg := awstest.Config(t, s.URL)
	cc := cloudcontrol.NewFromConfig(cfg)
	apiCode := func(err error) string {
		var ae smithy.APIError
		if errors.As(err, &ae) {
			return ae.ErrorCode()
		}
		return ""
	}

	// An S3 bucket made through Cloud Control is a real bucket.
	cr, err := cc.CreateResource(ctx, &cloudcontrol.CreateResourceInput{TypeName: aws.String("AWS::S3::Bucket"), DesiredState: aws.String(`{"BucketName":"cc-bucket"}`)})
	if err != nil {
		t.Fatal(err)
	}
	ev := cr.ProgressEvent
	if ev.Operation != types.OperationCreate || aws.ToString(ev.RequestToken) == "" || aws.ToString(ev.Identifier) != "cc-bucket" {
		t.Fatalf("progress: %+v", ev)
	}
	if _, err := s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true }).HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String("cc-bucket")}); err != nil {
		t.Fatalf("bucket is not there: %v", err)
	}
	// the request goes IN_PROGRESS then SUCCESS, and the waiter agrees
	st, err := cc.GetResourceRequestStatus(ctx, &cloudcontrol.GetResourceRequestStatusInput{RequestToken: ev.RequestToken})
	if err != nil || st.ProgressEvent.OperationStatus != types.OperationStatusInProgress && st.ProgressEvent.OperationStatus != types.OperationStatusSuccess {
		t.Fatalf("status: %v %+v", err, st)
	}
	if err := cloudcontrol.NewResourceRequestSuccessWaiter(cc, func(o *cloudcontrol.ResourceRequestSuccessWaiterOptions) {
		o.MinDelay, o.MaxDelay = 100*time.Millisecond, 500*time.Millisecond
	}).Wait(ctx, &cloudcontrol.GetResourceRequestStatusInput{RequestToken: ev.RequestToken}, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	g, err := cc.GetResource(ctx, &cloudcontrol.GetResourceInput{TypeName: aws.String("AWS::S3::Bucket"), Identifier: aws.String("cc-bucket")})
	if err != nil {
		t.Fatal(err)
	}
	var props map[string]any
	json.Unmarshal([]byte(aws.ToString(g.ResourceDescription.Properties)), &props)
	if props["BucketName"] != "cc-bucket" || props["Arn"] == nil {
		t.Fatalf("properties: %v", props)
	}

	// An SQS queue is updated by a JSON patch, and SQS sees it.
	q, err := cc.CreateResource(ctx, &cloudcontrol.CreateResourceInput{TypeName: aws.String("AWS::SQS::Queue"), DesiredState: aws.String(`{"QueueName":"cc-queue","VisibilityTimeout":30}`)})
	if err != nil {
		t.Fatal(err)
	}
	qurl := aws.ToString(q.ProgressEvent.Identifier)
	sq := sqs.NewFromConfig(cfg)
	vt := func() string {
		a, err := sq.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: aws.String(qurl), AttributeNames: []sqstypes.QueueAttributeName{"VisibilityTimeout"}})
		if err != nil {
			t.Fatal(err)
		}
		return a.Attributes["VisibilityTimeout"]
	}
	if vt() != "30" {
		t.Fatalf("visibility timeout %s", vt())
	}
	u, err := cc.UpdateResource(ctx, &cloudcontrol.UpdateResourceInput{TypeName: aws.String("AWS::SQS::Queue"), Identifier: aws.String(qurl),
		PatchDocument: aws.String(`[{"op":"replace","path":"/VisibilityTimeout","value":60},{"op":"add","path":"/DelaySeconds","value":5}]`)})
	if err != nil || u.ProgressEvent.Operation != types.OperationUpdate {
		t.Fatalf("update: %v %+v", err, u)
	}
	if vt() != "60" {
		t.Fatalf("visibility timeout after patch %s", vt())
	}
	g, _ = cc.GetResource(ctx, &cloudcontrol.GetResourceInput{TypeName: aws.String("AWS::SQS::Queue"), Identifier: aws.String(qurl)})
	json.Unmarshal([]byte(aws.ToString(g.ResourceDescription.Properties)), &props)
	if props["VisibilityTimeout"] != 60.0 || props["DelaySeconds"] != 5.0 || props["QueueName"] != "cc-queue" {
		t.Fatalf("properties after patch: %v", props)
	}
	// a patch that cannot apply is refused
	if _, err := cc.UpdateResource(ctx, &cloudcontrol.UpdateResourceInput{TypeName: aws.String("AWS::SQS::Queue"), Identifier: aws.String(qurl),
		PatchDocument: aws.String(`[{"op":"replace","path":"/Nope","value":1}]`)}); apiCode(err) != "InvalidRequestException" {
		t.Fatalf("bad patch: %v", err)
	}

	// list, and request listing
	l, err := cc.ListResources(ctx, &cloudcontrol.ListResourcesInput{TypeName: aws.String("AWS::SQS::Queue")})
	if err != nil || len(l.ResourceDescriptions) != 1 || aws.ToString(l.ResourceDescriptions[0].Identifier) != qurl {
		t.Fatalf("list: %v %+v", err, l)
	}
	rr, err := cc.ListResourceRequests(ctx, &cloudcontrol.ListResourceRequestsInput{ResourceRequestStatusFilter: &types.ResourceRequestStatusFilter{Operations: []types.Operation{types.OperationUpdate}}})
	if err != nil || len(rr.ResourceRequestStatusSummaries) != 1 {
		t.Fatalf("requests: %v %+v", err, rr)
	}

	// delete
	if _, err := cc.DeleteResource(ctx, &cloudcontrol.DeleteResourceInput{TypeName: aws.String("AWS::SQS::Queue"), Identifier: aws.String(qurl)}); err != nil {
		t.Fatal(err)
	}
	if _, err := sq.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: aws.String(qurl)}); err == nil {
		t.Fatal("queue survived the delete")
	}
	if _, err := cc.GetResource(ctx, &cloudcontrol.GetResourceInput{TypeName: aws.String("AWS::SQS::Queue"), Identifier: aws.String(qurl)}); apiCode(err) != "ResourceNotFoundException" {
		t.Fatalf("get deleted: %v", err)
	}
	if _, err := cc.DeleteResource(ctx, &cloudcontrol.DeleteResourceInput{TypeName: aws.String("AWS::S3::Bucket"), Identifier: aws.String("cc-bucket")}); err != nil {
		t.Fatal(err)
	}
	if _, err := cc.GetResourceRequestStatus(ctx, &cloudcontrol.GetResourceRequestStatusInput{RequestToken: aws.String("nope")}); apiCode(err) != "RequestTokenNotFoundException" {
		t.Fatalf("unknown token: %v", err)
	}

	// A type that maps onto an embedded model runs on the generic engine: EFS sees it.
	f, err := cc.CreateResource(ctx, &cloudcontrol.CreateResourceInput{TypeName: aws.String("AWS::EFS::FileSystem"), DesiredState: aws.String(`{"PerformanceMode":"generalPurpose"}`)})
	if err != nil {
		t.Fatal(err)
	}
	fid := aws.ToString(f.ProgressEvent.Identifier)
	if fid == "" {
		t.Fatalf("efs create: %+v", f.ProgressEvent)
	}
	if d, err := efs.NewFromConfig(cfg).DescribeFileSystems(ctx, &efs.DescribeFileSystemsInput{FileSystemId: aws.String(fid)}); err != nil || len(d.FileSystems) != 1 {
		t.Fatalf("efs does not see it: %v", err)
	}
	if l, err := cc.ListResources(ctx, &cloudcontrol.ListResourcesInput{TypeName: aws.String("AWS::EFS::FileSystem")}); err != nil || len(l.ResourceDescriptions) != 1 {
		t.Fatalf("efs list: %v", err)
	}
	if _, err := cc.DeleteResource(ctx, &cloudcontrol.DeleteResourceInput{TypeName: aws.String("AWS::EFS::FileSystem"), Identifier: aws.String(fid)}); err != nil {
		t.Fatal(err)
	}
	if _, err := cc.GetResource(ctx, &cloudcontrol.GetResourceInput{TypeName: aws.String("AWS::EFS::FileSystem"), Identifier: aws.String(fid)}); apiCode(err) != "ResourceNotFoundException" {
		t.Fatalf("efs get deleted: %v", err)
	}

	// unknown types
	if _, err := cc.CreateResource(ctx, &cloudcontrol.CreateResourceInput{TypeName: aws.String("AWS::Nope::Thing"), DesiredState: aws.String(`{}`)}); apiCode(err) != "UnsupportedActionException" {
		t.Fatalf("unknown type: %v", err)
	}
}

// floci #2043: ListResources finds queues and log groups made outside Cloud Control.
func TestCloudControlListsOutsideResources(t *testing.T) {
	ctx := context.Background()
	cfg := awstest.Config(t, pogi.Start(t).URL)
	if _, err := sqs.NewFromConfig(cfg).CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("q1")}); err != nil {
		t.Fatal(err)
	}
	if _, err := cloudwatchlogs.NewFromConfig(cfg).CreateLogGroup(ctx, &cloudwatchlogs.CreateLogGroupInput{LogGroupName: aws.String("/lg1")}); err != nil {
		t.Fatal(err)
	}
	cc := cloudcontrol.NewFromConfig(cfg)
	for _, typ := range []string{"AWS::SQS::Queue", "AWS::Logs::LogGroup"} {
		l, err := cc.ListResources(ctx, &cloudcontrol.ListResourcesInput{TypeName: aws.String(typ)})
		if err != nil || len(l.ResourceDescriptions) != 1 {
			t.Errorf("%s: %v %+v", typ, err, l)
		}
	}
}
