package generic_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/computeoptimizer"
	coptypes "github.com/aws/aws-sdk-go-v2/service/computeoptimizer/types"
	"github.com/aws/aws-sdk-go-v2/service/costoptimizationhub"
	"github.com/aws/aws-sdk-go-v2/service/costoptimizationhub/types"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestComputeOptimizer(t *testing.T) {
	_, url := start(t, "compute-optimizer")
	ctx := context.Background()
	c := computeoptimizer.NewFromConfig(awstest.Config(t, url))

	g, err := c.GetEnrollmentStatus(ctx, &computeoptimizer.GetEnrollmentStatusInput{})
	if err != nil || g.Status != coptypes.StatusInactive {
		t.Fatalf("before: %v %+v", err, g)
	}
	if _, err := c.UpdateEnrollmentStatus(ctx, &computeoptimizer.UpdateEnrollmentStatusInput{}); err == nil {
		t.Fatal("update without a status succeeded")
	}
	if _, err := c.UpdateEnrollmentStatus(ctx, &computeoptimizer.UpdateEnrollmentStatusInput{Status: coptypes.StatusPending}); err == nil {
		t.Fatal("update to Pending succeeded")
	}
	if _, err := c.UpdateEnrollmentStatus(ctx, &computeoptimizer.UpdateEnrollmentStatusInput{Status: coptypes.StatusActive, IncludeMemberAccounts: true}); err != nil {
		t.Fatal(err)
	}
	g, err = c.GetEnrollmentStatus(ctx, &computeoptimizer.GetEnrollmentStatusInput{})
	if err != nil || g.Status != coptypes.StatusActive || !g.MemberAccountsEnrolled || g.LastUpdatedTimestamp == nil {
		t.Fatalf("after: %v %+v", err, g)
	}
}

func TestCostOptimizationHub(t *testing.T) {
	_, url := start(t, "cost-optimization-hub")
	ctx := context.Background()
	c := costoptimizationhub.NewFromConfig(awstest.Config(t, url))

	p, err := c.GetPreferences(ctx, &costoptimizationhub.GetPreferencesInput{})
	if err != nil || p.SavingsEstimationMode != types.SavingsEstimationModeBeforeDiscounts || p.MemberAccountDiscountVisibility != types.MemberAccountDiscountVisibilityAll {
		t.Fatalf("defaults: %v %+v", err, p)
	}
	if _, err := c.UpdatePreferences(ctx, &costoptimizationhub.UpdatePreferencesInput{SavingsEstimationMode: "Nope"}); err == nil {
		t.Fatal("a bad mode was accepted")
	}
	if _, err := c.UpdateEnrollmentStatus(ctx, &costoptimizationhub.UpdateEnrollmentStatusInput{Status: "Pending"}); err == nil {
		t.Fatal("a bad status was accepted")
	}
	l, err := c.ListEnrollmentStatuses(ctx, &costoptimizationhub.ListEnrollmentStatusesInput{})
	if err != nil || len(l.Items) != 0 {
		t.Fatalf("before: %v %+v", err, l)
	}
	if _, err := c.UpdateEnrollmentStatus(ctx, &costoptimizationhub.UpdateEnrollmentStatusInput{Status: types.EnrollmentStatusActive}); err != nil {
		t.Fatal(err)
	}
	l, err = c.ListEnrollmentStatuses(ctx, &costoptimizationhub.ListEnrollmentStatusesInput{})
	if err != nil || len(l.Items) != 1 || l.Items[0].Status != types.EnrollmentStatusActive || aws.ToString(l.Items[0].AccountId) != "000000000000" {
		t.Fatalf("after: %v %+v", err, l)
	}
	if _, err := c.UpdatePreferences(ctx, &costoptimizationhub.UpdatePreferencesInput{SavingsEstimationMode: types.SavingsEstimationModeAfterDiscounts}); err != nil {
		t.Fatal(err)
	}
	p, err = c.GetPreferences(ctx, &costoptimizationhub.GetPreferencesInput{})
	if err != nil || p.SavingsEstimationMode != types.SavingsEstimationModeAfterDiscounts || p.MemberAccountDiscountVisibility != types.MemberAccountDiscountVisibilityAll {
		t.Fatalf("updated: %v %+v", err, p)
	}
}
