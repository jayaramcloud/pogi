package generic_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfn "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/aws/aws-sdk-go-v2/service/serverlessapplicationrepository"
	sqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestServerlessRepo(t *testing.T) {
	_, url := start(t, "serverlessrepo")
	ctx := context.Background()
	c := serverlessapplicationrepository.NewFromConfig(awstest.Config(t, url))

	if _, err := c.CreateApplication(ctx, &serverlessapplicationrepository.CreateApplicationInput{Name: aws.String("app")}); err == nil {
		t.Fatal("create without an author succeeded")
	}
	_, err := c.GetApplication(ctx, &serverlessapplicationrepository.GetApplicationInput{ApplicationId: aws.String("arn:aws:serverlessrepo:us-east-1:000000000000:applications/none")})
	kCode(t, err, "NotFoundException")
	_, err = c.DeleteApplication(ctx, &serverlessapplicationrepository.DeleteApplicationInput{ApplicationId: aws.String("arn:aws:serverlessrepo:us-east-1:000000000000:applications/none")})
	kCode(t, err, "NotFoundException")

	a, err := c.CreateApplication(ctx, &serverlessapplicationrepository.CreateApplicationInput{
		Name: aws.String("hello"), Author: aws.String("acme"), Description: aws.String("says hello"),
		SemanticVersion: aws.String("1.0.0"), TemplateBody: aws.String(`{"Resources":{}}`), Labels: []string{"demo"},
	})
	if err != nil || !strings.Contains(aws.ToString(a.ApplicationId), ":applications/") || aws.ToString(a.Name) != "hello" {
		t.Fatalf("create: %v %+v", err, a)
	}
	g, err := c.GetApplication(ctx, &serverlessapplicationrepository.GetApplicationInput{ApplicationId: a.ApplicationId})
	if err != nil || aws.ToString(g.Author) != "acme" || aws.ToString(g.Description) != "says hello" {
		t.Fatalf("get: %v %+v", err, g)
	}
	l, err := c.ListApplications(ctx, &serverlessapplicationrepository.ListApplicationsInput{})
	if err != nil || len(l.Applications) != 1 {
		t.Fatalf("list: %v %+v", err, l)
	}
	if _, err := c.DeleteApplication(ctx, &serverlessapplicationrepository.DeleteApplicationInput{ApplicationId: a.ApplicationId}); err != nil {
		t.Fatal(err)
	}
	_, err = c.GetApplication(ctx, &serverlessapplicationrepository.GetApplicationInput{ApplicationId: a.ApplicationId})
	kCode(t, err, "NotFoundException")
}

// A change set made from an application version is a real CloudFormation change set: executing it
// creates the stack's resources, and the stack carries AWS's serverlessrepo tags.
func TestServerlessRepoDeploy(t *testing.T) {
	cfg := awstest.Config(t, pogi.Start(t).URL)
	ctx := context.Background()
	c := serverlessapplicationrepository.NewFromConfig(cfg)
	f := cfn.NewFromConfig(cfg)

	a, err := c.CreateApplication(ctx, &serverlessapplicationrepository.CreateApplicationInput{
		Name: aws.String("queue"), Author: aws.String("acme"), Description: aws.String("a queue"), SemanticVersion: aws.String("1.0.0"),
		TemplateBody: aws.String(`{"Resources":{"Q":{"Type":"AWS::SQS::Queue","Properties":{"QueueName":"sar-queue"}}}}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.CreateCloudFormationChangeSet(ctx, &serverlessapplicationrepository.CreateCloudFormationChangeSetInput{
		ApplicationId: aws.String("arn:aws:serverlessrepo:us-east-1:000000000000:applications/none"), StackName: aws.String("s")})
	kCode(t, err, "NotFoundException")

	cs, err := c.CreateCloudFormationChangeSet(ctx, &serverlessapplicationrepository.CreateCloudFormationChangeSetInput{
		ApplicationId: a.ApplicationId, StackName: aws.String("sar-stack")})
	if err != nil || aws.ToString(cs.StackId) == "" || aws.ToString(cs.ChangeSetId) == "" || aws.ToString(cs.SemanticVersion) != "1.0.0" {
		t.Fatalf("change set: %v %+v", err, cs)
	}
	if _, err := f.ExecuteChangeSet(ctx, &cfn.ExecuteChangeSetInput{ChangeSetName: cs.ChangeSetId, StackName: cs.StackId}); err != nil {
		t.Fatal(err)
	}
	var tags map[string]string
	for range 100 {
		d, err := f.DescribeStacks(ctx, &cfn.DescribeStacksInput{StackName: cs.StackId})
		if err != nil {
			t.Fatal(err)
		}
		if st := d.Stacks[0].StackStatus; st == "CREATE_COMPLETE" {
			tags = map[string]string{}
			for _, tg := range d.Stacks[0].Tags {
				tags[aws.ToString(tg.Key)] = aws.ToString(tg.Value)
			}
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if tags["serverlessrepo:applicationId"] != aws.ToString(a.ApplicationId) || tags["serverlessrepo:semanticVersion"] != "1.0.0" {
		t.Fatalf("stack tags: %v", tags)
	}
	if _, err := sqs.NewFromConfig(cfg).GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String("sar-queue")}); err != nil {
		t.Fatalf("the queue was not made: %v", err)
	}
}
