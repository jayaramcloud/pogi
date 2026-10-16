package web_test

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/nrndev/pogi/internal/awstest"
)

// The IAM page: recorded calls with the strict verdict, the generated policy, attach with undo,
// and the enforcement switch.
func TestIAMPage(t *testing.T) {
	ctx := context.Background()
	c := start(t, "iam", "sqs")
	want(t, c.get("/_pogi/iam"), "No calls recorded yet")

	admin := iam.NewFromConfig(awstest.Config(t, c.base))
	admin.CreateUser(ctx, &iam.CreateUserInput{UserName: aws.String("ci")})
	admin.PutUserPolicy(ctx, &iam.PutUserPolicyInput{UserName: aws.String("ci"), PolicyName: aws.String("p"),
		PolicyDocument: aws.String(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"sqs:CreateQueue","Resource":"*"}]}`)})
	key, err := admin.CreateAccessKey(ctx, &iam.CreateAccessKeyInput{UserName: aws.String("ci")})
	if err != nil {
		t.Fatal(err)
	}
	q := sqs.NewFromConfig(awstest.ConfigAs(t, c.base, *key.AccessKey.AccessKeyId, *key.AccessKey.SecretAccessKey, ""))
	queue, _ := q.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("audit")})
	q.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: queue.QueueUrl, MessageBody: aws.String("x")})

	page := c.get("/_pogi/iam")
	want(t, page, `/_pogi/iam/attach?principal=arn%3aaws%3aiam%3a%3a`) // escaped once, not twice
	want(t, page, "user/ci", "sqs:SendMessage", "would deny", "sqs:CreateQueue", "allowed", "arn:aws:sqs:us-east-1:000000000000:audit", "Attach to user/ci")
	want(t, c.get("/_pogi/iam?fmt=tf"), `resource &#34;aws_iam_policy&#34; &#34;pogi_ci&#34;`)
	want(t, c.get("/_pogi/iam/policy?principal=arn:aws:iam::000000000000:user/ci"), `"sqs:SendMessage"`)
	// an unknown principal is a 404, not another principal's policy
	if res, err := http.Get(c.base + "/_pogi/iam/policy?principal=arn:aws:iam::000000000000:user/nobody"); err != nil {
		t.Fatal(err)
	} else if b, _ := io.ReadAll(res.Body); res.StatusCode != 404 || !strings.Contains(string(b), "nothing recorded for arn:aws:iam::000000000000:user/nobody") {
		t.Fatalf("unknown principal = %d: %s", res.StatusCode, b)
	}
	// the sidebar badge counts the would-deny calls in soft mode only
	wantNot(t, page, "would deny</em>")

	// the attach drawer shows the diff
	want(t, c.get("/_pogi/iam/attach?principal=arn:aws:iam::000000000000:user/ci"), "+1 action", "sqs:SendMessage on audit", "pogi-ci", "v1")

	body, final := c.post("/_pogi/iam/attach", url.Values{"principal": {"arn:aws:iam::000000000000:user/ci"}})
	if final != "/_pogi/iam" || strings.Contains(body, `class="flash"`) {
		t.Fatalf("attach ended on %s: %s", final, flash(body))
	}
	want(t, body, "Attached v1 of pogi-ci", "Undo", "Nothing to attach")
	if l, _ := admin.ListAttachedUserPolicies(ctx, &iam.ListAttachedUserPoliciesInput{UserName: aws.String("ci")}); len(l.AttachedPolicies) != 1 {
		t.Fatalf("attached: %v", l.AttachedPolicies)
	}

	body, _ = c.post("/_pogi/iam/undo", url.Values{"principal": {"arn:aws:iam::000000000000:user/ci"}, "policy": {"pogi-ci"},
		"arn": {"arn:aws:iam::000000000000:policy/pogi-ci"}, "attached": {"v1"}, "linked": {"1"}})
	want(t, body, "would deny")
	if l, _ := admin.ListAttachedUserPolicies(ctx, &iam.ListAttachedUserPoliciesInput{UserName: aws.String("ci")}); len(l.AttachedPolicies) != 0 {
		t.Fatalf("still attached after undo: %v", l.AttachedPolicies)
	}

	// the mode switch, and the badge it brings
	body, _ = c.post("/_pogi/iam/mode", url.Values{"mode": {"soft"}})
	want(t, body, "1 would deny", "Soft: every call is allowed")
	_, final = c.post("/_pogi/iam/mode", url.Values{"mode": {"strict"}, "to": {"/_pogi/requests?show=iam"}})
	if final != "/_pogi/requests" {
		t.Fatalf("mode switch ended on %s", final)
	}
	if _, err := q.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: queue.QueueUrl}); err == nil {
		t.Fatal("strict mode let an unallowed call through")
	}
	c.post("/_pogi/iam/restart", url.Values{})
	want(t, c.get("/_pogi/iam"), "Strict: calls without an allowing policy fail")
}
