package generic_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/connect"
	"github.com/aws/aws-sdk-go-v2/service/connect/types"

	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestConnect(t *testing.T) {
	s := pogi.Start(t)
	ctx := context.Background()
	c := connect.NewFromConfig(awstest.Config(t, s.URL))
	lifecycle(t, time.Hour)

	ci, err := c.CreateInstance(ctx, &connect.CreateInstanceInput{IdentityManagementType: types.DirectoryTypeConnectManaged, InstanceAlias: aws.String("acme-support"),
		InboundCallsEnabled: aws.Bool(true), OutboundCallsEnabled: aws.Bool(false), Tags: map[string]string{"team": "cx"}})
	if err != nil || aws.ToString(ci.Id) == "" || !strings.HasSuffix(aws.ToString(ci.Arn), ":instance/"+aws.ToString(ci.Id)) {
		t.Fatalf("create: %v %+v", err, ci)
	}
	id := aws.ToString(ci.Id)
	if _, err := c.CreateInstance(ctx, &connect.CreateInstanceInput{IdentityManagementType: types.DirectoryTypeConnectManaged, InstanceAlias: aws.String("acme-support"),
		InboundCallsEnabled: aws.Bool(true), OutboundCallsEnabled: aws.Bool(true)}); err == nil {
		t.Fatal("duplicate alias")
	}
	get := func() *types.Instance {
		d, err := c.DescribeInstance(ctx, &connect.DescribeInstanceInput{InstanceId: aws.String(id)})
		if err != nil {
			t.Fatal(err)
		}
		return d.Instance
	}
	in := get()
	if in.InstanceStatus != types.InstanceStatusCreationInProgress || aws.ToString(in.InstanceAlias) != "acme-support" || !aws.ToBool(in.InboundCallsEnabled) || aws.ToBool(in.OutboundCallsEnabled) ||
		aws.ToString(in.InstanceAccessUrl) != "https://acme-support.my.connect.localhost" || aws.ToString(in.ServiceRole) == "" || in.Tags["team"] != "cx" {
		t.Fatalf("creating: %+v", in)
	}
	lifecycle(t, 0)
	if in := get(); in.InstanceStatus != types.InstanceStatusActive {
		t.Fatalf("active: %+v", in)
	}
	if l, err := c.ListInstances(ctx, &connect.ListInstancesInput{}); err != nil || len(l.InstanceSummaryList) != 1 || l.InstanceSummaryList[0].InstanceStatus != types.InstanceStatusActive {
		t.Fatalf("list: %v %+v", err, l)
	}
	// attributes: changed, and read back
	if _, err := c.UpdateInstanceAttribute(ctx, &connect.UpdateInstanceAttributeInput{InstanceId: aws.String(id), AttributeType: types.InstanceAttributeTypeOutboundCalls, Value: aws.String("true")}); err != nil {
		t.Fatal(err)
	}
	if !aws.ToBool(get().OutboundCallsEnabled) {
		t.Fatalf("outbound not enabled: %+v", get())
	}
	if a, err := c.DescribeInstanceAttribute(ctx, &connect.DescribeInstanceAttributeInput{InstanceId: aws.String(id), AttributeType: types.InstanceAttributeTypeOutboundCalls}); err != nil || aws.ToString(a.Attribute.Value) != "true" {
		t.Fatalf("attribute: %v %+v", err, a)
	}

	// hours of operation and a queue that uses them
	h, err := c.CreateHoursOfOperation(ctx, &connect.CreateHoursOfOperationInput{InstanceId: aws.String(id), Name: aws.String("business"), TimeZone: aws.String("UTC"),
		Config: []types.HoursOfOperationConfig{{Day: types.HoursOfOperationDaysMonday, StartTime: &types.HoursOfOperationTimeSlice{Hours: aws.Int32(9), Minutes: aws.Int32(0)}, EndTime: &types.HoursOfOperationTimeSlice{Hours: aws.Int32(17), Minutes: aws.Int32(0)}}}})
	if err != nil || aws.ToString(h.HoursOfOperationId) == "" {
		t.Fatalf("hours: %v %+v", err, h)
	}
	q, err := c.CreateQueue(ctx, &connect.CreateQueueInput{InstanceId: aws.String(id), Name: aws.String("support"), HoursOfOperationId: h.HoursOfOperationId, Description: aws.String("tier 1"),
		Tags: map[string]string{"q": "1"}})
	if err != nil || aws.ToString(q.QueueId) == "" || aws.ToString(q.QueueArn) == "" {
		t.Fatalf("queue: %v %+v", err, q)
	}
	dq, err := c.DescribeQueue(ctx, &connect.DescribeQueueInput{InstanceId: aws.String(id), QueueId: q.QueueId})
	if err != nil || aws.ToString(dq.Queue.Name) != "support" || aws.ToString(dq.Queue.HoursOfOperationId) != aws.ToString(h.HoursOfOperationId) || dq.Queue.Status != types.QueueStatusEnabled {
		t.Fatalf("describe queue: %v %+v", err, dq)
	}
	if _, err := c.UpdateQueueName(ctx, &connect.UpdateQueueNameInput{InstanceId: aws.String(id), QueueId: q.QueueId, Name: aws.String("support-2")}); err != nil {
		t.Fatal(err)
	}
	if dq, _ := c.DescribeQueue(ctx, &connect.DescribeQueueInput{InstanceId: aws.String(id), QueueId: q.QueueId}); aws.ToString(dq.Queue.Name) != "support-2" {
		t.Fatalf("renamed queue: %+v", dq.Queue)
	}
	if l, err := c.ListQueues(ctx, &connect.ListQueuesInput{InstanceId: aws.String(id)}); err != nil || len(l.QueueSummaryList) != 1 {
		t.Fatalf("list queues: %v %+v", err, l)
	}
	if _, err := c.TagResource(ctx, &connect.TagResourceInput{ResourceArn: q.QueueArn, Tags: map[string]string{"x": "y"}}); err != nil {
		t.Fatal(err)
	}
	if l, err := c.ListTagsForResource(ctx, &connect.ListTagsForResourceInput{ResourceArn: q.QueueArn}); err != nil || len(l.Tags) != 2 {
		t.Fatalf("tags: %v %+v", err, l)
	}
	if _, err := c.UntagResource(ctx, &connect.UntagResourceInput{ResourceArn: q.QueueArn, TagKeys: []string{"q"}}); err != nil {
		t.Fatal(err)
	}
	if l, _ := c.ListTagsForResource(ctx, &connect.ListTagsForResourceInput{ResourceArn: q.QueueArn}); len(l.Tags) != 1 || l.Tags["x"] != "y" {
		t.Fatalf("tags after untag: %+v", l)
	}
	if _, err := c.DeleteQueue(ctx, &connect.DeleteQueueInput{InstanceId: aws.String(id), QueueId: q.QueueId}); err != nil {
		t.Fatal(err)
	}
	var nf *types.ResourceNotFoundException
	if _, err := c.DescribeQueue(ctx, &connect.DescribeQueueInput{InstanceId: aws.String(id), QueueId: q.QueueId}); !errors.As(err, &nf) {
		t.Fatalf("describe deleted queue: %v", err)
	}

	// delete the instance
	if _, err := c.DeleteInstance(ctx, &connect.DeleteInstanceInput{InstanceId: aws.String(id)}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.DescribeInstance(ctx, &connect.DescribeInstanceInput{InstanceId: aws.String(id)}); !errors.As(err, &nf) {
		t.Fatalf("describe deleted instance: %v", err)
	}
}
