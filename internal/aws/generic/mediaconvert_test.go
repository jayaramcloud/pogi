package generic_test

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/mediaconvert"
	"github.com/aws/aws-sdk-go-v2/service/mediaconvert/types"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestMediaConvert(t *testing.T) {
	_, url := start(t, "mediaconvert")
	ctx := context.Background()
	c := mediaconvert.NewFromConfig(awstest.Config(t, url))

	q, err := c.CreateQueue(ctx, &mediaconvert.CreateQueueInput{Name: aws.String("q"), Description: aws.String("batch"), Tags: map[string]string{"env": "dev"}})
	if err != nil || aws.ToString(q.Queue.Name) != "q" || aws.ToString(q.Queue.Arn) == "" || q.Queue.Status != types.QueueStatusActive {
		t.Fatalf("create: %v %+v", err, q)
	}
	if _, err := c.CreateQueue(ctx, &mediaconvert.CreateQueueInput{Name: aws.String("q")}); err == nil {
		t.Fatal("duplicate queue succeeded")
	}
	if _, err := c.CreateQueue(ctx, &mediaconvert.CreateQueueInput{}); err == nil {
		t.Fatal("queue without a name succeeded")
	}
	if a := aws.ToString(q.Queue.Arn); !strings.HasSuffix(a, ":queues/q") {
		t.Fatalf("arn: %s", a)
	}
	g, err := c.GetQueue(ctx, &mediaconvert.GetQueueInput{Name: aws.String("q")})
	if err != nil || aws.ToString(g.Queue.Description) != "batch" || g.Queue.Arn == nil {
		t.Fatalf("get: %v %+v", err, g)
	}
	u, err := c.UpdateQueue(ctx, &mediaconvert.UpdateQueueInput{Name: aws.String("q"), Description: aws.String("night")})
	if err != nil || aws.ToString(u.Queue.Description) != "night" {
		t.Fatalf("update: %v %+v", err, u)
	}
	l, err := c.ListQueues(ctx, &mediaconvert.ListQueuesInput{})
	if err != nil || len(l.Queues) != 1 {
		t.Fatalf("list: %v %+v", err, l)
	}
	tags, err := c.ListTagsForResource(ctx, &mediaconvert.ListTagsForResourceInput{Arn: q.Queue.Arn})
	if err != nil || tags.ResourceTags.Tags["env"] != "dev" {
		t.Fatalf("tags: %v %+v", err, tags)
	}
	if _, err := c.DeleteQueue(ctx, &mediaconvert.DeleteQueueInput{Name: aws.String("q")}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.GetQueue(ctx, &mediaconvert.GetQueueInput{Name: aws.String("q")}); err == nil {
		t.Fatal("deleted queue still read")
	}
	if _, err := c.DeleteQueue(ctx, &mediaconvert.DeleteQueueInput{Name: aws.String("q")}); err == nil {
		t.Fatal("second delete succeeded")
	}
}
