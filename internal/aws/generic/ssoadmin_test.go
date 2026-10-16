package generic_test

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ssoadmin"
	"github.com/aws/aws-sdk-go-v2/service/ssoadmin/types"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestSSOAdmin(t *testing.T) {
	_, base := startWith(t, "sso-admin")
	ctx := context.Background()
	c := ssoadmin.NewFromConfig(awstest.Config(t, base))
	in, err := c.ListInstances(ctx, &ssoadmin.ListInstancesInput{})
	if err != nil || len(in.Instances) != 1 {
		t.Fatalf("instances: %v %+v", err, in)
	}
	inst := in.Instances[0].InstanceArn

	ps, err := c.CreatePermissionSet(ctx, &ssoadmin.CreatePermissionSetInput{InstanceArn: inst, Name: aws.String("Dev"), Description: aws.String("developers"), SessionDuration: aws.String("PT2H"),
		Tags: []types.Tag{{Key: aws.String("env"), Value: aws.String("dev")}}})
	if err != nil {
		t.Fatal(err)
	}
	arn := ps.PermissionSet.PermissionSetArn
	if aws.ToString(ps.PermissionSet.Name) != "Dev" || aws.ToString(arn) == "" {
		t.Fatalf("create: %+v", ps.PermissionSet)
	}
	d, err := c.DescribePermissionSet(ctx, &ssoadmin.DescribePermissionSetInput{InstanceArn: inst, PermissionSetArn: arn})
	if err != nil || aws.ToString(d.PermissionSet.Description) != "developers" || aws.ToString(d.PermissionSet.SessionDuration) != "PT2H" {
		t.Fatalf("describe: %v %+v", err, d)
	}
	l, err := c.ListPermissionSets(ctx, &ssoadmin.ListPermissionSetsInput{InstanceArn: inst})
	if err != nil || len(l.PermissionSets) != 1 || l.PermissionSets[0] != aws.ToString(arn) {
		t.Fatalf("list: %v %+v", err, l)
	}
	if _, err := c.UpdatePermissionSet(ctx, &ssoadmin.UpdatePermissionSetInput{InstanceArn: inst, PermissionSetArn: arn, Description: aws.String("devs")}); err != nil {
		t.Fatal(err)
	}
	d, _ = c.DescribePermissionSet(ctx, &ssoadmin.DescribePermissionSetInput{InstanceArn: inst, PermissionSetArn: arn})
	if aws.ToString(d.PermissionSet.Description) != "devs" {
		t.Fatalf("update: %+v", d.PermissionSet)
	}
	tags, err := c.ListTagsForResource(ctx, &ssoadmin.ListTagsForResourceInput{InstanceArn: inst, ResourceArn: arn})
	if err != nil || len(tags.Tags) != 1 {
		t.Fatalf("tags: %v %+v", err, tags)
	}
	if _, err := c.TagResource(ctx, &ssoadmin.TagResourceInput{InstanceArn: inst, ResourceArn: arn, Tags: []types.Tag{{Key: aws.String("team"), Value: aws.String("a")}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.UntagResource(ctx, &ssoadmin.UntagResourceInput{InstanceArn: inst, ResourceArn: arn, TagKeys: []string{"env"}}); err != nil {
		t.Fatal(err)
	}
	tags, _ = c.ListTagsForResource(ctx, &ssoadmin.ListTagsForResourceInput{InstanceArn: inst, ResourceArn: arn})
	if len(tags.Tags) != 1 || aws.ToString(tags.Tags[0].Key) != "team" {
		t.Fatalf("tags after: %+v", tags.Tags)
	}

	// Policies: inline, AWS-managed, customer-managed and the boundary.
	if _, err := c.PutInlinePolicyToPermissionSet(ctx, &ssoadmin.PutInlinePolicyToPermissionSetInput{InstanceArn: inst, PermissionSetArn: arn, InlinePolicy: aws.String(`{"Version":"2012-10-17","Statement":[]}`)}); err != nil {
		t.Fatal(err)
	}
	ip, err := c.GetInlinePolicyForPermissionSet(ctx, &ssoadmin.GetInlinePolicyForPermissionSetInput{InstanceArn: inst, PermissionSetArn: arn})
	if err != nil || aws.ToString(ip.InlinePolicy) != `{"Version":"2012-10-17","Statement":[]}` {
		t.Fatalf("inline: %v %+v", err, ip)
	}
	for range 20 { // the inline policy is keyed by the permission set's ARN but is not the permission set: its tags are not the set's
		if tags, err = c.ListTagsForResource(ctx, &ssoadmin.ListTagsForResourceInput{InstanceArn: inst, ResourceArn: arn}); err != nil || len(tags.Tags) != 1 {
			t.Fatalf("tags after the inline policy: %v %+v", err, tags)
		}
	}
	const managed = "arn:aws:iam::aws:policy/ReadOnlyAccess"
	if _, err := c.AttachManagedPolicyToPermissionSet(ctx, &ssoadmin.AttachManagedPolicyToPermissionSetInput{InstanceArn: inst, PermissionSetArn: arn, ManagedPolicyArn: aws.String(managed)}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.AttachManagedPolicyToPermissionSet(ctx, &ssoadmin.AttachManagedPolicyToPermissionSetInput{InstanceArn: inst, PermissionSetArn: arn, ManagedPolicyArn: aws.String(managed)}); !errors.As(err, new(*types.ConflictException)) {
		t.Fatalf("attach twice: %T %v", err, err)
	}
	mp, err := c.ListManagedPoliciesInPermissionSet(ctx, &ssoadmin.ListManagedPoliciesInPermissionSetInput{InstanceArn: inst, PermissionSetArn: arn})
	if err != nil || len(mp.AttachedManagedPolicies) != 1 || aws.ToString(mp.AttachedManagedPolicies[0].Name) != "ReadOnlyAccess" {
		t.Fatalf("managed: %v %+v", err, mp)
	}
	ref := &types.CustomerManagedPolicyReference{Name: aws.String("MyPolicy"), Path: aws.String("/team/")}
	if _, err := c.AttachCustomerManagedPolicyReferenceToPermissionSet(ctx, &ssoadmin.AttachCustomerManagedPolicyReferenceToPermissionSetInput{InstanceArn: inst, PermissionSetArn: arn, CustomerManagedPolicyReference: ref}); err != nil {
		t.Fatal(err)
	}
	cm, err := c.ListCustomerManagedPolicyReferencesInPermissionSet(ctx, &ssoadmin.ListCustomerManagedPolicyReferencesInPermissionSetInput{InstanceArn: inst, PermissionSetArn: arn})
	if err != nil || len(cm.CustomerManagedPolicyReferences) != 1 || aws.ToString(cm.CustomerManagedPolicyReferences[0].Name) != "MyPolicy" {
		t.Fatalf("customer managed: %v %+v", err, cm)
	}
	if _, err := c.DetachManagedPolicyFromPermissionSet(ctx, &ssoadmin.DetachManagedPolicyFromPermissionSetInput{InstanceArn: inst, PermissionSetArn: arn, ManagedPolicyArn: aws.String(managed)}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.DetachCustomerManagedPolicyReferenceFromPermissionSet(ctx, &ssoadmin.DetachCustomerManagedPolicyReferenceFromPermissionSetInput{InstanceArn: inst, PermissionSetArn: arn, CustomerManagedPolicyReference: ref}); err != nil {
		t.Fatal(err)
	}
	mp, _ = c.ListManagedPoliciesInPermissionSet(ctx, &ssoadmin.ListManagedPoliciesInPermissionSetInput{InstanceArn: inst, PermissionSetArn: arn})
	if len(mp.AttachedManagedPolicies) != 0 {
		t.Fatalf("detach: %+v", mp)
	}
	if _, err := c.PutPermissionsBoundaryToPermissionSet(ctx, &ssoadmin.PutPermissionsBoundaryToPermissionSetInput{InstanceArn: inst, PermissionSetArn: arn,
		PermissionsBoundary: &types.PermissionsBoundary{ManagedPolicyArn: aws.String(managed)}}); err != nil {
		t.Fatal(err)
	}
	pb, err := c.GetPermissionsBoundaryForPermissionSet(ctx, &ssoadmin.GetPermissionsBoundaryForPermissionSetInput{InstanceArn: inst, PermissionSetArn: arn})
	if err != nil || aws.ToString(pb.PermissionsBoundary.ManagedPolicyArn) != managed {
		t.Fatalf("boundary: %v %+v", err, pb)
	}

	// An assignment succeeds at once, is listed, and provisions a role in the account.
	const acct = "210987654321"
	as := &ssoadmin.CreateAccountAssignmentInput{InstanceArn: inst, PermissionSetArn: arn, PrincipalType: types.PrincipalTypeGroup, PrincipalId: aws.String("g-1"), TargetType: types.TargetTypeAwsAccount, TargetId: aws.String(acct)}
	ca, err := c.CreateAccountAssignment(ctx, as)
	if err != nil || ca.AccountAssignmentCreationStatus.Status != types.StatusValuesSucceeded {
		t.Fatalf("assign: %v %+v", err, ca)
	}
	st, err := c.DescribeAccountAssignmentCreationStatus(ctx, &ssoadmin.DescribeAccountAssignmentCreationStatusInput{InstanceArn: inst, AccountAssignmentCreationRequestId: ca.AccountAssignmentCreationStatus.RequestId})
	if err != nil || st.AccountAssignmentCreationStatus.Status != types.StatusValuesSucceeded || aws.ToString(st.AccountAssignmentCreationStatus.TargetId) != acct {
		t.Fatalf("assignment status: %v %+v", err, st)
	}
	la, err := c.ListAccountAssignments(ctx, &ssoadmin.ListAccountAssignmentsInput{InstanceArn: inst, AccountId: aws.String(acct), PermissionSetArn: arn})
	if err != nil || len(la.AccountAssignments) != 1 || aws.ToString(la.AccountAssignments[0].PrincipalId) != "g-1" {
		t.Fatalf("list assignments: %v %+v", err, la)
	}
	lp, err := c.ListAccountAssignmentsForPrincipal(ctx, &ssoadmin.ListAccountAssignmentsForPrincipalInput{InstanceArn: inst, PrincipalId: aws.String("g-1"), PrincipalType: types.PrincipalTypeGroup})
	if err != nil || len(lp.AccountAssignments) != 1 {
		t.Fatalf("assignments for principal: %v %+v", err, lp)
	}
	ac, err := c.ListAccountsForProvisionedPermissionSet(ctx, &ssoadmin.ListAccountsForProvisionedPermissionSetInput{InstanceArn: inst, PermissionSetArn: arn})
	if err != nil || len(ac.AccountIds) != 1 || ac.AccountIds[0] != acct {
		t.Fatalf("provisioned accounts: %v %+v", err, ac)
	}
	pa, err := c.ListPermissionSetsProvisionedToAccount(ctx, &ssoadmin.ListPermissionSetsProvisionedToAccountInput{InstanceArn: inst, AccountId: aws.String(acct)})
	if err != nil || len(pa.PermissionSets) != 1 {
		t.Fatalf("provisioned sets: %v %+v", err, pa)
	}
	pr, err := c.ProvisionPermissionSet(ctx, &ssoadmin.ProvisionPermissionSetInput{InstanceArn: inst, PermissionSetArn: arn, TargetType: types.ProvisionTargetTypeAllProvisionedAccounts})
	if err != nil {
		t.Fatal(err)
	}
	if ps, err := c.DescribePermissionSetProvisioningStatus(ctx, &ssoadmin.DescribePermissionSetProvisioningStatusInput{InstanceArn: inst, ProvisionPermissionSetRequestId: pr.PermissionSetProvisioningStatus.RequestId}); err != nil || ps.PermissionSetProvisioningStatus.Status != types.StatusValuesSucceeded {
		t.Fatalf("provisioning status: %v %+v", err, ps)
	}
	if _, err := c.DeleteAccountAssignment(ctx, &ssoadmin.DeleteAccountAssignmentInput{InstanceArn: inst, PermissionSetArn: arn, PrincipalType: as.PrincipalType, PrincipalId: as.PrincipalId, TargetType: as.TargetType, TargetId: as.TargetId}); err != nil {
		t.Fatal(err)
	}
	if la, _ = c.ListAccountAssignments(ctx, &ssoadmin.ListAccountAssignmentsInput{InstanceArn: inst, AccountId: aws.String(acct), PermissionSetArn: arn}); len(la.AccountAssignments) != 0 {
		t.Fatalf("assignment left: %+v", la)
	}

	if _, err := c.DeletePermissionSet(ctx, &ssoadmin.DeletePermissionSetInput{InstanceArn: inst, PermissionSetArn: arn}); err != nil {
		t.Fatal(err)
	}
	_, err = c.DescribePermissionSet(ctx, &ssoadmin.DescribePermissionSetInput{InstanceArn: inst, PermissionSetArn: arn})
	if !errors.As(err, new(*types.ResourceNotFoundException)) {
		t.Fatalf("describe after delete: %T %v", err, err)
	}
}
