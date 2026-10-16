package iam_test

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	iamsvc "github.com/nrndev/pogi/internal/aws/iam"
	sqssvc "github.com/nrndev/pogi/internal/aws/sqs"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/core"
)

// Off mode records what a user calls, says what strict would do, and the
// generated policy can be attached (v1, then v2) and undone.
func TestRecorderGenerateAttach(t *testing.T) {
	ctx := context.Background()
	h, url := core.ServeTest(t,
		func(h *core.Hub) core.Service { return iamsvc.New(h) },
		func(h *core.Hub) core.Service { return sqssvc.New(h) })
	h.Cfg.IAM = "off"
	admin := iam.NewFromConfig(awstest.Config(t, url))
	if _, err := admin.CreateUser(ctx, &iam.CreateUserInput{UserName: aws.String("ci")}); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.PutUserPolicy(ctx, &iam.PutUserPolicyInput{UserName: aws.String("ci"), PolicyName: aws.String("p"),
		PolicyDocument: aws.String(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"sqs:CreateQueue","Resource":"*"}]}`)}); err != nil {
		t.Fatal(err)
	}
	key, _ := admin.CreateAccessKey(ctx, &iam.CreateAccessKeyInput{UserName: aws.String("ci")})
	ci := sqs.NewFromConfig(awstest.ConfigAs(t, url, *key.AccessKey.AccessKeyId, *key.AccessKey.SecretAccessKey, ""))
	q, err := ci.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("jobs")})
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if _, err := ci.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: q.QueueUrl, MessageBody: aws.String("x")}); err != nil {
			t.Fatalf("off mode must allow: %v", err)
		}
	}

	user := "arn:aws:iam::000000000000:user/ci"
	find := func() (p iamsvc.Principal) {
		for _, x := range iamsvc.Recorded(h) {
			if x.ARN == user {
				return x
			}
		}
		t.Fatalf("%s not recorded", user)
		return
	}
	p := find()
	counts := map[string]int{}
	for _, c := range p.Actions {
		counts[c.Action] = c.Count
	}
	if counts["sqs:SendMessage"] != 3 || counts["sqs:CreateQueue"] != 1 {
		t.Fatalf("counts: %v", counts)
	}
	if d := p.Diff(); len(d) != 1 || d[0] != "sqs:SendMessage on jobs" {
		t.Fatalf("diff: %v", d)
	}
	if got := p.Policy(); !strings.Contains(got, `"sqs:SendMessage"`) || !strings.Contains(got, `"Resource": "arn:aws:sqs:us-east-1:000000000000:jobs"`) {
		t.Fatalf("policy: %s", got)
	}
	if tf := p.Terraform(); !strings.Contains(tf, `resource "aws_iam_policy" "pogi_ci"`) || !strings.Contains(tf, "jsonencode(") {
		t.Fatalf("terraform: %s", tf)
	}
	if e := h.Requests.Query(0, 1, "sqs", "", false); len(e) != 1 || e[0].IAM != "" {
		t.Fatalf("off mode must not log a decision: %+v", e)
	}

	a, err := iamsvc.Attach(h, user)
	if err != nil || a.Version != "v1" || a.Prev != "" || !a.Linked {
		t.Fatalf("attach v1: %+v %v", a, err)
	}
	if n := len(find().WouldDeny()); n != 0 {
		t.Fatalf("%d calls still would deny after attach", n)
	}
	// a new call, a second version that keeps the first one's actions
	if _, err := ci.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: q.QueueUrl}); err != nil {
		t.Fatal(err)
	}
	a2, err := iamsvc.Attach(h, user)
	if err != nil || a2.Version != "v2" || a2.Prev != "v1" || a2.Linked {
		t.Fatalf("attach v2: %+v %v", a2, err)
	}
	vs, _ := admin.ListPolicyVersions(ctx, &iam.ListPolicyVersionsInput{PolicyArn: aws.String(a.Arn)})
	if len(vs.Versions) != 2 {
		t.Fatalf("versions: %d", len(vs.Versions))
	}
	if a.Arn != "arn:aws:iam::000000000000:policy/pogi-ci" {
		t.Fatalf("policy arn %s", a.Arn)
	}
	if err := iamsvc.Undo(h, a2); err != nil {
		t.Fatal(err)
	}
	pol, _ := admin.GetPolicy(ctx, &iam.GetPolicyInput{PolicyArn: aws.String(a.Arn)})
	if *pol.Policy.DefaultVersionId != "v1" {
		t.Fatalf("undo v2: default is %s", *pol.Policy.DefaultVersionId)
	}
	if err := iamsvc.Undo(h, a); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.GetPolicy(ctx, &iam.GetPolicyInput{PolicyArn: aws.String(a.Arn)}); err == nil {
		t.Fatal("undo v1 should delete the policy")
	}
	if l, _ := admin.ListAttachedUserPolicies(ctx, &iam.ListAttachedUserPoliciesInput{UserName: aws.String("ci")}); len(l.AttachedPolicies) != 0 {
		t.Fatalf("still attached: %v", l.AttachedPolicies)
	}
}
