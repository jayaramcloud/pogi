package sts_test

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/smithy-go"
	iamsvc "github.com/nrndev/pogi/internal/aws/iam"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	stssvc "github.com/nrndev/pogi/internal/aws/sts"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/core"
)

func TestIdentitiesAndAssumeRole(t *testing.T) {
	ctx := context.Background()
	_, url := core.ServeTest(t, func(h *core.Hub) core.Service { return stssvc.New(h) })
	c := sts.NewFromConfig(awstest.Config(t, url))
	id, err := c.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil || *id.Account != "000000000000" || *id.Arn != "arn:aws:iam::000000000000:root" {
		t.Fatalf("default identity = %+v %v", id, err)
	}
	other := sts.NewFromConfig(awstest.ConfigAs(t, url, "111122223333", "test", ""))
	if id, _ := other.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{}); *id.Account != "111122223333" {
		t.Fatalf("12-digit key account = %s", *id.Account)
	}
	role, err := c.AssumeRole(ctx, &sts.AssumeRoleInput{RoleArn: aws.String("arn:aws:iam::444455556666:role/deployer"), RoleSessionName: aws.String("ci")})
	if err != nil {
		t.Fatal(err)
	}
	cr := role.Credentials
	assumed := sts.NewFromConfig(awstest.ConfigAs(t, url, *cr.AccessKeyId, *cr.SecretAccessKey, *cr.SessionToken))
	id, err = assumed.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil || *id.Account != "444455556666" || *id.Arn != "arn:aws:sts::444455556666:assumed-role/deployer/ci" {
		t.Fatalf("assumed identity = %+v %v", id, err)
	}
	if _, err := c.AssumeRole(ctx, &sts.AssumeRoleInput{RoleArn: aws.String("nope"), RoleSessionName: aws.String("x")}); err == nil {
		t.Fatal("invalid role ARN must fail")
	}
}

// floci #1934 #4392 #3205: expired credentials stop working, a missing role
// cannot be assumed, an expired web identity token is refused, and
// DurationSeconds has a 900 second floor.
func TestExpiryMissingRoleAndShortSession(t *testing.T) {
	ctx := context.Background()
	h, url := core.ServeTest(t, func(h *core.Hub) core.Service { return iamsvc.New(h) }, func(h *core.Hub) core.Service { return stssvc.New(h) })
	h.Cfg.ValidateSigs = true
	root := awstest.Config(t, url)
	c := sts.NewFromConfig(root)
	code := func(err error) string {
		var ae smithy.APIError
		if errors.As(err, &ae) {
			return ae.ErrorCode()
		}
		return ""
	}

	if _, err := c.GetSessionToken(ctx, &sts.GetSessionTokenInput{DurationSeconds: aws.Int32(1)}); code(err) != "ValidationError" {
		t.Errorf("DurationSeconds=1: %v", err)
	}
	if _, err := c.AssumeRole(ctx, &sts.AssumeRoleInput{RoleArn: aws.String("arn:aws:iam::000000000000:role/never-created"), RoleSessionName: aws.String("ss")}); code(err) != "AccessDenied" {
		t.Errorf("missing role: %v", err)
	}

	// credentials past their Expires time are refused
	h.AddIdentity("ASIAEXPIRED", core.Identity{Account: "000000000000", ARN: "arn:aws:sts::000000000000:assumed-role/r/s", Secret: "secret", Expires: time.Now().Add(-time.Minute)})
	expired := sts.NewFromConfig(awstest.ConfigAs(t, url, "ASIAEXPIRED", "secret", "tok"))
	if _, err := expired.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{}); code(err) != "ExpiredToken" {
		t.Errorf("expired credentials: %v", err)
	}

	trust := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Federated":"*"},"Action":"sts:AssumeRoleWithWebIdentity"}]}`
	if _, err := iam.NewFromConfig(root).CreateRole(ctx, &iam.CreateRoleInput{RoleName: aws.String("web"), AssumeRolePolicyDocument: aws.String(trust)}); err != nil {
		t.Fatal(err)
	}
	jwt := func(exp time.Time) string {
		e := base64.RawURLEncoding.EncodeToString
		return e([]byte(`{"alg":"none"}`)) + "." + e([]byte(fmt.Sprintf(`{"sub":"u","exp":%d}`, exp.Unix()))) + "."
	}
	web := func(tok string) error {
		_, err := c.AssumeRoleWithWebIdentity(ctx, &sts.AssumeRoleWithWebIdentityInput{RoleArn: aws.String("arn:aws:iam::000000000000:role/web"), RoleSessionName: aws.String("ss"), WebIdentityToken: aws.String(tok)})
		return err
	}
	if err := web(jwt(time.Now().Add(-time.Hour))); code(err) != "ExpiredTokenException" {
		t.Errorf("expired web identity token: %v", err)
	}
	if err := web(jwt(time.Now().Add(time.Hour))); err != nil {
		t.Errorf("fresh web identity token: %v", err)
	}
}
