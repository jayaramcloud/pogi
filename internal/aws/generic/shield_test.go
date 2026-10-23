package generic_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/shield"
	"github.com/aws/aws-sdk-go-v2/service/shield/types"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestShield(t *testing.T) {
	_, url := start(t, "shield")
	ctx := context.Background()
	c := shield.NewFromConfig(awstest.Config(t, url))

	var nf *types.ResourceNotFoundException
	if _, err := c.DescribeSubscription(ctx, &shield.DescribeSubscriptionInput{}); !errors.As(err, &nf) {
		t.Fatalf("describe before subscribing: %T %v", err, err)
	}
	st, err := c.GetSubscriptionState(ctx, &shield.GetSubscriptionStateInput{})
	if err != nil || st.SubscriptionState != types.SubscriptionStateInactive {
		t.Fatalf("state: %v %+v", err, st)
	}
	if _, err := c.CreateSubscription(ctx, &shield.CreateSubscriptionInput{}); err != nil {
		t.Fatal(err)
	}
	var ex *types.ResourceAlreadyExistsException
	if _, err := c.CreateSubscription(ctx, &shield.CreateSubscriptionInput{}); !errors.As(err, &ex) {
		t.Fatalf("second subscribe: %T %v", err, err)
	}
	d, err := c.DescribeSubscription(ctx, &shield.DescribeSubscriptionInput{})
	if err != nil {
		t.Fatal(err)
	}
	s := d.Subscription
	if s.AutoRenew != types.AutoRenewEnabled || s.TimeCommitmentInSeconds != 365*24*3600 || s.StartTime == nil || s.EndTime == nil ||
		!strings.HasPrefix(aws.ToString(s.SubscriptionArn), "arn:aws:shield::000000000000:subscription/") {
		t.Fatalf("subscription: %+v", s)
	}
	if _, err := c.UpdateSubscription(ctx, &shield.UpdateSubscriptionInput{AutoRenew: types.AutoRenewDisabled}); err != nil {
		t.Fatal(err)
	}
	d, _ = c.DescribeSubscription(ctx, &shield.DescribeSubscriptionInput{})
	if d.Subscription.AutoRenew != types.AutoRenewDisabled {
		t.Fatalf("auto renew: %v", d.Subscription.AutoRenew)
	}
	st, _ = c.GetSubscriptionState(ctx, &shield.GetSubscriptionStateInput{})
	if st.SubscriptionState != types.SubscriptionStateActive {
		t.Fatalf("state: %v", st.SubscriptionState)
	}

	eip := "arn:aws:ec2:us-east-1:000000000000:eip-allocation/eipalloc-0123456789abcdef0"
	p, err := c.CreateProtection(ctx, &shield.CreateProtectionInput{Name: aws.String("eip"), ResourceArn: aws.String(eip), Tags: []types.Tag{{Key: aws.String("env"), Value: aws.String("dev")}}})
	if err != nil || aws.ToString(p.ProtectionId) == "" {
		t.Fatalf("protect: %v %+v", err, p)
	}
	id := aws.ToString(p.ProtectionId)
	if _, err := c.CreateProtection(ctx, &shield.CreateProtectionInput{Name: aws.String("again"), ResourceArn: aws.String(eip)}); !errors.As(err, &ex) {
		t.Fatalf("protect twice: %T %v", err, err)
	}
	g, err := c.DescribeProtection(ctx, &shield.DescribeProtectionInput{ProtectionId: aws.String(id)})
	if err != nil || aws.ToString(g.Protection.Id) != id || aws.ToString(g.Protection.ResourceArn) != eip || aws.ToString(g.Protection.Name) != "eip" ||
		aws.ToString(g.Protection.ProtectionArn) != "arn:aws:shield::000000000000:protection/"+id {
		t.Fatalf("describe: %v id %v name %v res %v arn %v", err, aws.ToString(g.Protection.Id), aws.ToString(g.Protection.Name), aws.ToString(g.Protection.ResourceArn), aws.ToString(g.Protection.ProtectionArn))
	}
	g, err = c.DescribeProtection(ctx, &shield.DescribeProtectionInput{ResourceArn: aws.String(eip)})
	if err != nil || aws.ToString(g.Protection.Id) != id {
		t.Fatalf("describe by resource: %v %+v", err, g)
	}
	l, err := c.ListProtections(ctx, &shield.ListProtectionsInput{})
	if err != nil || len(l.Protections) != 1 {
		t.Fatalf("list: %v %+v", err, l)
	}
	tg, err := c.ListTagsForResource(ctx, &shield.ListTagsForResourceInput{ResourceARN: g.Protection.ProtectionArn})
	if err != nil || len(tg.Tags) != 1 {
		t.Fatalf("tags: %v %+v", err, tg)
	}
	if _, err := c.CreateProtection(ctx, &shield.CreateProtectionInput{Name: aws.String("bad"), ResourceArn: aws.String("not-an-arn")}); err == nil {
		t.Fatal("protect of a non-ARN succeeded")
	}
	if _, err := c.DeleteProtection(ctx, &shield.DeleteProtectionInput{ProtectionId: aws.String(id)}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.DescribeProtection(ctx, &shield.DescribeProtectionInput{ProtectionId: aws.String(id)}); !errors.As(err, &nf) {
		t.Fatalf("not found: %T %v", err, err)
	}
	if _, err := c.DeleteProtection(ctx, &shield.DeleteProtectionInput{ProtectionId: aws.String(id)}); !errors.As(err, &nf) {
		t.Fatalf("second delete: %T %v", err, err)
	}
	if _, err := c.DescribeProtection(ctx, &shield.DescribeProtectionInput{}); err == nil {
		t.Fatal("describe with no id succeeded")
	}
	// a subscription lasts its year: real Shield refuses to end it early
	var lk *types.LockedSubscriptionException
	if _, err := c.DeleteSubscription(ctx, &shield.DeleteSubscriptionInput{}); !errors.As(err, &lk) {
		t.Fatalf("delete subscription: %T %v", err, err)
	}
}
