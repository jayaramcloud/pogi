package iam_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/iam/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/smithy-go"
	"golang.org/x/crypto/ssh"

	iamsvc "github.com/nrndev/pogi/internal/aws/iam"
	sqssvc "github.com/nrndev/pogi/internal/aws/sqs"
	stssvc "github.com/nrndev/pogi/internal/aws/sts"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/core"
)

func code(err error) string {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		return ae.ErrorCode()
	}
	return ""
}

// floci #4985: SSH public key operations.
func TestSSHPublicKeys(t *testing.T) {
	ctx := context.Background()
	_, endpoint := serve(t)
	c := iam.NewFromConfig(awstest.Config(t, endpoint))
	if _, err := c.CreateUser(ctx, &iam.CreateUserInput{UserName: aws.String("u")}); err != nil {
		t.Fatal(err)
	}
	pub, _, _ := ed25519.GenerateKey(rand.Reader)
	sp, _ := ssh.NewPublicKey(pub)
	body := strings.TrimSpace(string(ssh.MarshalAuthorizedKey(sp))) + " u@host"

	up, err := c.UploadSSHPublicKey(ctx, &iam.UploadSSHPublicKeyInput{UserName: aws.String("u"), SSHPublicKeyBody: aws.String(body)})
	if err != nil {
		t.Fatal(err)
	}
	k := up.SSHPublicKey
	if k.Status != types.StatusTypeActive || aws.ToString(k.Fingerprint) != ssh.FingerprintLegacyMD5(sp) || !strings.HasPrefix(aws.ToString(k.SSHPublicKeyId), "APKA") {
		t.Fatalf("key = %+v", k)
	}
	if _, err := c.UploadSSHPublicKey(ctx, &iam.UploadSSHPublicKeyInput{UserName: aws.String("u"), SSHPublicKeyBody: aws.String(body)}); code(err) != "DuplicateSSHPublicKey" {
		t.Errorf("duplicate upload: %q", code(err))
	}
	if _, err := c.UploadSSHPublicKey(ctx, &iam.UploadSSHPublicKeyInput{UserName: aws.String("u"), SSHPublicKeyBody: aws.String("not a key")}); code(err) != "InvalidPublicKey" {
		t.Errorf("bad key: %q", code(err))
	}
	for enc, want := range map[types.EncodingType]string{types.EncodingTypeSsh: "ssh-ed25519 ", types.EncodingTypePem: "-----BEGIN PUBLIC KEY-----"} {
		g, err := c.GetSSHPublicKey(ctx, &iam.GetSSHPublicKeyInput{UserName: aws.String("u"), SSHPublicKeyId: k.SSHPublicKeyId, Encoding: enc})
		if err != nil || !strings.HasPrefix(aws.ToString(g.SSHPublicKey.SSHPublicKeyBody), want) {
			t.Errorf("Get %s: %v %+v", enc, err, g)
		}
	}
	l, err := c.ListSSHPublicKeys(ctx, &iam.ListSSHPublicKeysInput{UserName: aws.String("u")})
	if err != nil || len(l.SSHPublicKeys) != 1 || aws.ToString(l.SSHPublicKeys[0].UserName) != "u" {
		t.Fatalf("list: %v %+v", err, l)
	}
	if _, err := c.UpdateSSHPublicKey(ctx, &iam.UpdateSSHPublicKeyInput{UserName: aws.String("u"), SSHPublicKeyId: k.SSHPublicKeyId, Status: types.StatusTypeInactive}); err != nil {
		t.Fatal(err)
	}
	if l, _ := c.ListSSHPublicKeys(ctx, &iam.ListSSHPublicKeysInput{UserName: aws.String("u")}); l.SSHPublicKeys[0].Status != types.StatusTypeInactive {
		t.Error("status not updated")
	}
	if _, err := c.DeleteUser(ctx, &iam.DeleteUserInput{UserName: aws.String("u")}); code(err) != "DeleteConflict" {
		t.Errorf("DeleteUser with an SSH key: %q", code(err))
	}
	if _, err := c.DeleteSSHPublicKey(ctx, &iam.DeleteSSHPublicKeyInput{UserName: aws.String("u"), SSHPublicKeyId: k.SSHPublicKeyId}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.GetSSHPublicKey(ctx, &iam.GetSSHPublicKeyInput{UserName: aws.String("u"), SSHPublicKeyId: k.SSHPublicKeyId, Encoding: types.EncodingTypeSsh}); code(err) != "NoSuchEntity" {
		t.Errorf("get after delete: %q", code(err))
	}
}

// floci #3584: a permissions boundary limits what a user or an assumed role may do.
func TestPermissionsBoundary(t *testing.T) {
	ctx := context.Background()
	h, url := core.ServeTest(t,
		func(h *core.Hub) core.Service { return iamsvc.New(h) },
		func(h *core.Hub) core.Service { return sqssvc.New(h) },
		func(h *core.Hub) core.Service { return stssvc.New(h) })
	h.Cfg.IAM = "strict"
	admin := iam.NewFromConfig(awstest.Config(t, url))
	doc := func(effect, action string) *string {
		return aws.String(`{"Version":"2012-10-17","Statement":[{"Effect":"` + effect + `","Action":"` + action + `","Resource":"*"}]}`)
	}
	mk := func(name string, d *string) *string {
		p, err := admin.CreatePolicy(ctx, &iam.CreatePolicyInput{PolicyName: aws.String(name), PolicyDocument: d})
		if err != nil {
			t.Fatal(err)
		}
		return p.Policy.Arn
	}
	s3Only, sqsOK := mk("s3only", doc("Allow", "s3:*")), mk("sqsok", doc("Allow", "sqs:*"))
	denySQS := mk("denysqs", aws.String(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"},{"Effect":"Deny","Action":"sqs:CreateQueue","Resource":"*"}]}`))

	admin.CreateUser(ctx, &iam.CreateUserInput{UserName: aws.String("u"), PermissionsBoundary: s3Only})
	admin.PutUserPolicy(ctx, &iam.PutUserPolicyInput{UserName: aws.String("u"), PolicyName: aws.String("p"), PolicyDocument: doc("Allow", "*")})
	k, _ := admin.CreateAccessKey(ctx, &iam.CreateAccessKeyInput{UserName: aws.String("u")})
	q := sqs.NewFromConfig(awstest.ConfigAs(t, url, *k.AccessKey.AccessKeyId, *k.AccessKey.SecretAccessKey, ""))
	create := func(n string) error {
		_, err := q.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(n)})
		return err
	}
	if err := create("a"); err == nil || !strings.Contains(err.Error(), "no permissions boundary allows the sqs:CreateQueue action") {
		t.Fatalf("s3-only boundary: %v", err)
	}
	admin.PutUserPermissionsBoundary(ctx, &iam.PutUserPermissionsBoundaryInput{UserName: aws.String("u"), PermissionsBoundary: sqsOK})
	if err := create("b"); err != nil {
		t.Fatalf("sqs boundary: %v", err)
	}
	admin.PutUserPermissionsBoundary(ctx, &iam.PutUserPermissionsBoundaryInput{UserName: aws.String("u"), PermissionsBoundary: denySQS})
	if err := create("c"); err == nil || !strings.Contains(err.Error(), "explicit deny in a permissions boundary") {
		t.Fatalf("deny in boundary: %v", err)
	}
	admin.DeleteUserPermissionsBoundary(ctx, &iam.DeleteUserPermissionsBoundaryInput{UserName: aws.String("u")})
	if err := create("d"); err != nil {
		t.Fatalf("no boundary: %v", err)
	}

	// An assumed role is limited by the role's boundary.
	if _, err := admin.CreateRole(ctx, &iam.CreateRoleInput{RoleName: aws.String("r"), AssumeRolePolicyDocument: aws.String(trust), PermissionsBoundary: s3Only}); err != nil {
		t.Fatal(err)
	}
	admin.PutRolePolicy(ctx, &iam.PutRolePolicyInput{RoleName: aws.String("r"), PolicyName: aws.String("p"), PolicyDocument: doc("Allow", "*")})
	as, err := sts.NewFromConfig(awstest.Config(t, url)).AssumeRole(ctx, &sts.AssumeRoleInput{RoleArn: aws.String("arn:aws:iam::000000000000:role/r"), RoleSessionName: aws.String("ss")})
	if err != nil {
		t.Fatal(err)
	}
	rq := sqs.NewFromConfig(awstest.ConfigAs(t, url, *as.Credentials.AccessKeyId, *as.Credentials.SecretAccessKey, *as.Credentials.SessionToken))
	if _, err := rq.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("e")}); err == nil || !strings.Contains(err.Error(), "permissions boundary") {
		t.Fatalf("role boundary: %v", err)
	}
}

// floci #4487: service last accessed reports list the services a policy allows
// and when the entity last called each.
func TestServiceLastAccessed(t *testing.T) {
	ctx := context.Background()
	_, url := core.ServeTest(t,
		func(h *core.Hub) core.Service { return iamsvc.New(h) },
		func(h *core.Hub) core.Service { return sqssvc.New(h) },
		func(h *core.Hub) core.Service { return stssvc.New(h) })
	admin := iam.NewFromConfig(awstest.Config(t, url))
	report := func(arn *string) []types.ServiceLastAccessed {
		t.Helper()
		j, err := admin.GenerateServiceLastAccessedDetails(ctx, &iam.GenerateServiceLastAccessedDetailsInput{Arn: arn})
		if err != nil {
			t.Fatal(err)
		}
		d, err := admin.GetServiceLastAccessedDetails(ctx, &iam.GetServiceLastAccessedDetailsInput{JobId: j.JobId})
		if err != nil || d.JobStatus != types.JobStatusTypeCompleted {
			t.Fatalf("Get: %v %+v", err, d)
		}
		return d.ServicesLastAccessed
	}
	u, _ := admin.CreateUser(ctx, &iam.CreateUserInput{UserName: aws.String("u")})
	admin.PutUserPolicy(ctx, &iam.PutUserPolicyInput{UserName: aws.String("u"), PolicyName: aws.String("p"),
		PolicyDocument: aws.String(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["sqs:*","s3:Get*"],"Resource":"*"}]}`)})
	svcs := report(u.User.Arn)
	if len(svcs) != 2 || aws.ToString(svcs[0].ServiceNamespace) != "s3" || aws.ToString(svcs[1].ServiceNamespace) != "sqs" || svcs[1].LastAuthenticated != nil {
		t.Fatalf("before any call: %+v", svcs)
	}
	k, _ := admin.CreateAccessKey(ctx, &iam.CreateAccessKeyInput{UserName: aws.String("u")})
	q := sqs.NewFromConfig(awstest.ConfigAs(t, url, *k.AccessKey.AccessKeyId, *k.AccessKey.SecretAccessKey, ""))
	if _, err := q.ListQueues(ctx, &sqs.ListQueuesInput{}); err != nil {
		t.Fatal(err)
	}
	svcs = report(u.User.Arn)
	if svcs[1].LastAuthenticated == nil || aws.ToString(svcs[1].LastAuthenticatedEntity) != *u.User.Arn || aws.ToInt32(svcs[1].TotalAuthenticatedEntities) != 1 || svcs[0].LastAuthenticated != nil {
		t.Fatalf("after an sqs call: %+v", svcs)
	}

	admin.CreateUser(ctx, &iam.CreateUserInput{UserName: aws.String("root2")})
	admin.PutUserPolicy(ctx, &iam.PutUserPolicyInput{UserName: aws.String("root2"), PolicyName: aws.String("p"), PolicyDocument: allowAll})
	r2, _ := admin.GetUser(ctx, &iam.GetUserInput{UserName: aws.String("root2")})
	if all := report(r2.User.Arn); len(all) != 3 { // iam, sqs and sts are what this test server runs
		t.Errorf("Action * lists %d services, want every service the server runs (3)", len(all))
	}
	if _, err := admin.GenerateServiceLastAccessedDetails(ctx, &iam.GenerateServiceLastAccessedDetailsInput{Arn: aws.String("arn:aws:iam::000000000000:user/ghost")}); code(err) != "NoSuchEntity" {
		t.Errorf("unknown user: %q", code(err))
	}
	if _, err := admin.GetServiceLastAccessedDetails(ctx, &iam.GetServiceLastAccessedDetailsInput{JobId: aws.String("00000000-0000-0000-0000-000000000000")}); code(err) != "NoSuchEntity" {
		t.Errorf("unknown job: %q", code(err))
	}
}

var allowAll = aws.String(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`)
