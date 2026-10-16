package iam_test

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sts"

	iamsvc "github.com/nrndev/pogi/internal/aws/iam"
	sqssvc "github.com/nrndev/pogi/internal/aws/sqs"
	stssvc "github.com/nrndev/pogi/internal/aws/sts"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/core"
)

const bobPolicy = `{"Version":"2012-10-17","Statement":[
 {"Effect":"Allow","Action":["sqs:CreateQueue","sqs:Send*","sqs:DeleteQueue"],"Resource":"arn:aws:sqs:*:*:allowed-*"},
 {"Effect":"Allow","Action":"sqs:GetQueueUrl","Resource":"*","Condition":{"StringEquals":{"aws:username":"bob"}}},
 {"Effect":"Deny","Action":"sqs:DeleteQueue","Resource":"*"}]}`

func TestEnforcement(t *testing.T) {
	ctx := context.Background()
	h, url := core.ServeTest(t,
		func(h *core.Hub) core.Service { return iamsvc.New(h) },
		func(h *core.Hub) core.Service { return sqssvc.New(h) },
		func(h *core.Hub) core.Service { return stssvc.New(h) })
	h.Cfg.IAM = "strict"
	root := awstest.Config(t, url)
	admin := iam.NewFromConfig(root)

	if _, err := admin.CreateUser(ctx, &iam.CreateUserInput{UserName: aws.String("bob")}); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.PutUserPolicy(ctx, &iam.PutUserPolicyInput{UserName: aws.String("bob"), PolicyName: aws.String("p"), PolicyDocument: aws.String(bobPolicy)}); err != nil {
		t.Fatal(err)
	}
	key, err := admin.CreateAccessKey(ctx, &iam.CreateAccessKeyInput{UserName: aws.String("bob")})
	if err != nil {
		t.Fatal(err)
	}
	bobCfg := awstest.ConfigAs(t, url, *key.AccessKey.AccessKeyId, *key.AccessKey.SecretAccessKey, "")
	bob := sqs.NewFromConfig(bobCfg)
	if _, err := sts.NewFromConfig(bobCfg).GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{}); err != nil {
		t.Fatalf("GetCallerIdentity needs no permission: %v", err)
	}

	q, err := bob.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("allowed-1")})
	if err != nil {
		t.Fatalf("allowed create: %v", err)
	}
	if _, err := bob.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: q.QueueUrl, MessageBody: aws.String("hi")}); err != nil {
		t.Fatalf("allowed send: %v", err)
	}
	if _, err := bob.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String("allowed-1")}); err != nil {
		t.Fatalf("condition on aws:username: %v", err)
	}
	for name, call := range map[string]func() error{
		"other resource": func() error {
			_, err := bob.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("other")})
			return err
		},
		"explicit deny": func() error {
			_, err := bob.DeleteQueue(ctx, &sqs.DeleteQueueInput{QueueUrl: q.QueueUrl})
			return err
		},
		"no statement": func() error {
			_, err := bob.ListQueues(ctx, &sqs.ListQueuesInput{})
			return err
		},
	} {
		if msg := callErr(call); !strings.Contains(msg, "is not authorized to perform") {
			t.Errorf("%s: want AccessDenied, got %q", name, msg)
		}
	}

	// roles: assumed-role sessions get the role's policies
	trust := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"sts:AssumeRole"}]}`
	role, err := admin.CreateRole(ctx, &iam.CreateRoleInput{RoleName: aws.String("lister"), AssumeRolePolicyDocument: aws.String(trust)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admin.AttachRolePolicy(ctx, &iam.AttachRolePolicyInput{RoleName: aws.String("lister"), PolicyArn: aws.String("arn:aws:iam::aws:policy/AmazonSQSReadOnlyAccess")}); err != nil {
		t.Fatal(err)
	}
	creds, err := sts.NewFromConfig(root).AssumeRole(ctx, &sts.AssumeRoleInput{RoleArn: role.Role.Arn, RoleSessionName: aws.String("ss")})
	if err != nil {
		t.Fatal(err)
	}
	c := creds.Credentials
	lister := sqs.NewFromConfig(awstest.ConfigAs(t, url, *c.AccessKeyId, *c.SecretAccessKey, *c.SessionToken))
	if _, err := lister.ListQueues(ctx, &sqs.ListQueuesInput{}); err != nil {
		t.Fatalf("role allows ListQueues: %v", err)
	}
	if _, err := lister.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("allowed-2")}); err == nil {
		t.Fatal("read-only role created a queue")
	}

	// soft mode lets everything through and records the decision
	h.Cfg.IAM = "soft"
	if _, err := bob.ListQueues(ctx, &sqs.ListQueuesInput{}); err != nil {
		t.Fatalf("soft mode blocked: %v", err)
	}
	if e := h.Requests.Query(0, 1, "sqs", "", false); len(e) != 1 || !strings.HasPrefix(e[0].IAM, "deny sqs:ListQueues") {
		t.Fatalf("decision not logged: %+v", e)
	}

	// the simulator shares the evaluator
	sim, err := admin.SimulatePrincipalPolicy(ctx, &iam.SimulatePrincipalPolicyInput{
		PolicySourceArn: aws.String("arn:aws:iam::000000000000:user/bob"), ActionNames: []string{"sqs:SendMessage", "sqs:DeleteQueue", "s3:GetObject"},
		ResourceArns: []string{"arn:aws:sqs:us-east-1:000000000000:allowed-9"}})
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, r := range sim.EvaluationResults {
		got = append(got, string(r.EvalDecision))
	}
	if strings.Join(got, ",") != "allowed,explicitDeny,implicitDeny" {
		t.Fatalf("simulation: %v", got)
	}
}

func callErr(f func() error) string {
	if err := f(); err != nil {
		return err.Error()
	}
	return ""
}
