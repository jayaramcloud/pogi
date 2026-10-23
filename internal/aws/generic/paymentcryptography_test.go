package generic_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	pc "github.com/aws/aws-sdk-go-v2/service/paymentcryptography"
	"github.com/aws/aws-sdk-go-v2/service/paymentcryptography/types"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestPaymentCryptography(t *testing.T) {
	_, url := start(t, "payment-cryptography")
	ctx := context.Background()
	c := pc.NewFromConfig(awstest.Config(t, url))

	o, err := c.CreateKey(ctx, &pc.CreateKeyInput{Exportable: aws.Bool(true), Tags: []types.Tag{{Key: aws.String("env"), Value: aws.String("dev")}},
		KeyAttributes: &types.KeyAttributes{KeyAlgorithm: types.KeyAlgorithmTdes3key, KeyClass: types.KeyClassSymmetricKey,
			KeyUsage: types.KeyUsageTr31P0PinEncryptionKey, KeyModesOfUse: &types.KeyModesOfUse{Encrypt: true, Decrypt: true, Wrap: true, Unwrap: true}}})
	if err != nil {
		t.Fatal(err)
	}
	k := o.Key
	if !strings.HasPrefix(aws.ToString(k.KeyArn), "arn:aws:payment-cryptography:us-east-1:000000000000:key/") || k.KeyState != types.KeyStateCreateComplete || !aws.ToBool(k.Enabled) {
		t.Fatalf("key: %s %v %v", aws.ToString(k.KeyArn), k.KeyState, aws.ToBool(k.Enabled))
	}
	g, err := c.GetKey(ctx, &pc.GetKeyInput{KeyIdentifier: k.KeyArn})
	if err != nil || g.Key.KeyAttributes == nil || g.Key.KeyAttributes.KeyUsage != types.KeyUsageTr31P0PinEncryptionKey {
		t.Fatalf("get: %v %+v", err, g)
	}
	a, err := c.CreateAlias(ctx, &pc.CreateAliasInput{AliasName: aws.String("alias/pin"), KeyArn: k.KeyArn})
	if err != nil || aws.ToString(a.Alias.AliasName) != "alias/pin" {
		t.Fatalf("alias: %v %+v", err, a)
	}
	l, err := c.ListKeys(ctx, &pc.ListKeysInput{})
	if err != nil || len(l.Keys) != 1 {
		t.Fatalf("list: %v %+v", err, l)
	}
	if _, err := c.DeleteKey(ctx, &pc.DeleteKeyInput{KeyIdentifier: k.KeyArn}); err != nil {
		t.Fatal(err)
	}
	if p, err := c.GetKey(ctx, &pc.GetKeyInput{KeyIdentifier: k.KeyArn}); err != nil || p.Key.KeyState != types.KeyStateDeletePending || aws.ToBool(p.Key.Enabled) || p.Key.DeletePendingTimestamp == nil {
		t.Fatalf("after delete: %v %+v", err, p)
	}
	if _, err := c.DeleteKey(ctx, &pc.DeleteKeyInput{KeyIdentifier: k.KeyArn, DeleteKeyInDays: aws.Int32(1)}); err == nil {
		t.Fatal("DeleteKeyInDays 1 accepted")
	}
	var nf *types.ResourceNotFoundException
	if _, err := c.GetKey(ctx, &pc.GetKeyInput{KeyIdentifier: aws.String("arn:aws:payment-cryptography:us-east-1:000000000000:key/nosuchkey")}); !errors.As(err, &nf) {
		t.Fatalf("not found: %T %v", err, err)
	}
	if _, err := c.CreateKey(ctx, &pc.CreateKeyInput{}); err == nil {
		t.Fatal("empty create succeeded")
	}
}
