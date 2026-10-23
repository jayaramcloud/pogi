package generic_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/signer"
	"github.com/aws/aws-sdk-go-v2/service/signer/types"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestSigner(t *testing.T) {
	_, url := start(t, "signer")
	ctx := context.Background()
	c := signer.NewFromConfig(awstest.Config(t, url))

	o, err := c.PutSigningProfile(ctx, &signer.PutSigningProfileInput{
		ProfileName: aws.String("lambda_profile"), PlatformId: aws.String("AWSLambda-SHA384-ECDSA"),
		SignatureValidityPeriod: &types.SignatureValidityPeriod{Type: types.ValidityTypeDays, Value: 135},
		Tags:                    map[string]string{"env": "dev"},
	})
	if err != nil || aws.ToString(o.Arn) == "" {
		t.Fatalf("put: %v %+v", err, o)
	}
	g, err := c.GetSigningProfile(ctx, &signer.GetSigningProfileInput{ProfileName: aws.String("lambda_profile")})
	if err != nil || g.Status != types.SigningProfileStatusActive || aws.ToString(g.PlatformId) != "AWSLambda-SHA384-ECDSA" ||
		g.SignatureValidityPeriod == nil || g.SignatureValidityPeriod.Value != 135 || g.Tags["env"] != "dev" {
		t.Fatalf("get: %v %+v", err, g)
	}
	l, err := c.ListSigningProfiles(ctx, &signer.ListSigningProfilesInput{})
	if err != nil || len(l.Profiles) != 1 {
		t.Fatalf("list: %v %+v", err, l)
	}
	if _, err := c.PutSigningProfile(ctx, &signer.PutSigningProfileInput{}); err == nil {
		t.Fatal("empty put succeeded")
	}
	if _, err := c.GetSigningProfile(ctx, &signer.GetSigningProfileInput{ProfileName: aws.String("nope")}); err == nil {
		t.Fatal("unknown profile found")
	}
	if _, err := c.CancelSigningProfile(ctx, &signer.CancelSigningProfileInput{ProfileName: aws.String("lambda_profile")}); err != nil {
		t.Fatal(err)
	}
}
