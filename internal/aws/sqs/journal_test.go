package sqs_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqssvc "github.com/nrndev/pogi/internal/aws/sqs"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/core"
)

// TestJournalReplay replays the wal-mode journal into a fresh service: the
// state must come out the same.
func TestJournalReplay(t *testing.T) {
	var svc *sqssvc.Service
	_, url := core.ServeTest(t, func(h *core.Hub) core.Service { svc = sqssvc.New(h); return svc })
	check := core.JournalTest(t, svc, sqssvc.New(nil))
	c := sqs.NewFromConfig(awstest.Config(t, url))

	must(c.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("dead")}))
	q := must(c.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("jobs"), Attributes: map[string]string{
		"RedrivePolicy": `{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:000000000000:dead","maxReceiveCount":1}`}}))
	fifo := must(c.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("jobs.fifo"),
		Attributes: map[string]string{"FifoQueue": "true", "ContentBasedDeduplication": "true"}}))
	for _, b := range []string{"a", "b", "c", "a"} { // the second "a" is a FIFO duplicate
		must(c.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: fifo.QueueUrl, MessageBody: aws.String(b), MessageGroupId: aws.String("g")}))
	}
	for _, b := range []string{"a", "b", "c"} {
		must(c.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: q.QueueUrl, MessageBody: aws.String(b)}))
	}
	got := must(c.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: q.QueueUrl, MaxNumberOfMessages: 2}))
	must(c.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{QueueUrl: q.QueueUrl, ReceiptHandle: got.Messages[0].ReceiptHandle}))
	must(c.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: q.QueueUrl, ReceiptHandle: got.Messages[1].ReceiptHandle}))
	must(c.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: q.QueueUrl, MaxNumberOfMessages: 10})) // a goes to dead
	must(c.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: fifo.QueueUrl}))
	must(c.TagQueue(ctx, &sqs.TagQueueInput{QueueUrl: q.QueueUrl, Tags: map[string]string{"team": "core"}}))
	must(c.SetQueueAttributes(ctx, &sqs.SetQueueAttributesInput{QueueUrl: q.QueueUrl, Attributes: map[string]string{"DelaySeconds": "1"}}))
	tmp := must(c.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("tmp")}))
	must(c.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: tmp.QueueUrl, MessageBody: aws.String("x")}))
	must(c.DeleteQueue(ctx, &sqs.DeleteQueueInput{QueueUrl: tmp.QueueUrl}))
	check()
}
