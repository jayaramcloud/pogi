package organizations_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	orgs "github.com/aws/aws-sdk-go-v2/service/organizations"
	"github.com/aws/aws-sdk-go-v2/service/organizations/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/smithy-go"
	iamsvc "github.com/nrndev/pogi/internal/aws/iam"
	"github.com/nrndev/pogi/internal/aws/organizations"
	s3svc "github.com/nrndev/pogi/internal/aws/s3"
	stssvc "github.com/nrndev/pogi/internal/aws/sts"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/core"
)

var ctx = context.Background()

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func code(err error) string {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		return ae.ErrorCode()
	}
	return fmt.Sprint(err)
}

func serve(t *testing.T) (*core.Hub, string) {
	return core.ServeTest(t,
		func(h *core.Hub) core.Service { return organizations.New(h) },
		func(h *core.Hub) core.Service { return iamsvc.New(h) },
		func(h *core.Hub) core.Service { return stssvc.New(h) },
		func(h *core.Hub) core.Service { return s3svc.New(h) })
}

// newAccount creates a member account and returns its id.
func newAccount(t *testing.T, c *orgs.Client, name string) string {
	st := must(c.CreateAccount(ctx, &orgs.CreateAccountInput{Email: aws.String(name + "@example.com"), AccountName: aws.String(name)})).CreateAccountStatus
	if st.State != types.CreateAccountStateInProgress {
		t.Fatalf("create state = %s", st.State)
	}
	st = must(c.DescribeCreateAccountStatus(ctx, &orgs.DescribeCreateAccountStatusInput{CreateAccountRequestId: st.Id})).CreateAccountStatus
	if st.State != types.CreateAccountStateSucceeded || len(*st.AccountId) != 12 {
		t.Fatalf("status = %+v", st)
	}
	return *st.AccountId
}

// assume returns credentials for the member account's admin role.
func assume(t *testing.T, url string, mgmt aws.Config, account string) aws.Config {
	out := must(sts.NewFromConfig(mgmt).AssumeRole(ctx, &sts.AssumeRoleInput{
		RoleArn: aws.String("arn:aws:iam::" + account + ":role/OrganizationAccountAccessRole"), RoleSessionName: aws.String("admin")}))
	cr := out.Credentials
	return awstest.ConfigAs(t, url, *cr.AccessKeyId, *cr.SecretAccessKey, *cr.SessionToken)
}

// adminUser makes an IAM user with AdministratorAccess and returns its credentials.
func adminUser(t *testing.T, url string, cfg aws.Config, name string) aws.Config {
	c := iam.NewFromConfig(cfg)
	must(c.CreateUser(ctx, &iam.CreateUserInput{UserName: aws.String(name)}))
	must(c.AttachUserPolicy(ctx, &iam.AttachUserPolicyInput{UserName: aws.String(name), PolicyArn: aws.String("arn:aws:iam::aws:policy/AdministratorAccess")}))
	k := must(c.CreateAccessKey(ctx, &iam.CreateAccessKeyInput{UserName: aws.String(name)})).AccessKey
	return awstest.ConfigAs(t, url, *k.AccessKeyId, *k.SecretAccessKey, "")
}

func TestOrganization(t *testing.T) {
	_, url := serve(t)
	mgmt := awstest.Config(t, url)
	c := orgs.NewFromConfig(mgmt)

	if _, err := c.DescribeOrganization(ctx, &orgs.DescribeOrganizationInput{}); code(err) != "AWSOrganizationsNotInUseException" {
		t.Fatalf("before create: %v", err)
	}
	org := must(c.CreateOrganization(ctx, &orgs.CreateOrganizationInput{FeatureSet: types.OrganizationFeatureSetAll})).Organization
	if *org.MasterAccountId != "000000000000" || !strings.HasPrefix(*org.Id, "o-") || *org.Arn != "arn:aws:organizations::000000000000:organization/"+*org.Id {
		t.Fatalf("organization = %+v", org)
	}
	if _, err := c.CreateOrganization(ctx, &orgs.CreateOrganizationInput{}); code(err) != "AlreadyInOrganizationException" {
		t.Fatalf("second create: %v", err)
	}
	root := must(c.ListRoots(ctx, &orgs.ListRootsInput{})).Roots[0]
	scps := must(c.ListPoliciesForTarget(ctx, &orgs.ListPoliciesForTargetInput{TargetId: root.Id, Filter: types.PolicyTypeServiceControlPolicy})).Policies
	if len(root.PolicyTypes) != 0 || len(scps) != 1 || *scps[0].Id != "p-FullAWSAccess" || !scps[0].AwsManaged {
		t.Fatalf("root = %+v, SCPs = %+v", root, scps)
	}

	// OUs
	ou := must(c.CreateOrganizationalUnit(ctx, &orgs.CreateOrganizationalUnitInput{ParentId: root.Id, Name: aws.String("dev")})).OrganizationalUnit
	if _, err := c.CreateOrganizationalUnit(ctx, &orgs.CreateOrganizationalUnitInput{ParentId: root.Id, Name: aws.String("dev")}); code(err) != "DuplicateOrganizationalUnitException" {
		t.Fatalf("duplicate OU: %v", err)
	}
	if _, err := c.CreateOrganizationalUnit(ctx, &orgs.CreateOrganizationalUnitInput{ParentId: aws.String("ou-nope-12345678"), Name: aws.String("x")}); code(err) != "ParentNotFoundException" {
		t.Fatalf("bad parent: %v", err)
	}
	must(c.UpdateOrganizationalUnit(ctx, &orgs.UpdateOrganizationalUnitInput{OrganizationalUnitId: ou.Id, Name: aws.String("development")}))
	if got := must(c.DescribeOrganizationalUnit(ctx, &orgs.DescribeOrganizationalUnitInput{OrganizationalUnitId: ou.Id})).OrganizationalUnit; *got.Name != "development" {
		t.Fatalf("renamed OU = %+v", got)
	}

	// accounts: usable at once through the admin role
	acct := newAccount(t, c, "dev")
	if accts := must(c.ListAccounts(ctx, &orgs.ListAccountsInput{})).Accounts; len(accts) != 2 {
		t.Fatalf("accounts = %+v", accts)
	}
	member := assume(t, url, mgmt, acct)
	role := must(iam.NewFromConfig(member).ListAttachedRolePolicies(ctx, &iam.ListAttachedRolePoliciesInput{RoleName: aws.String("OrganizationAccountAccessRole")}))
	if len(role.AttachedPolicies) != 1 || *role.AttachedPolicies[0].PolicyName != "AdministratorAccess" {
		t.Fatalf("admin role policies = %+v", role.AttachedPolicies)
	}
	if id := must(sts.NewFromConfig(member).GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})); *id.Account != acct {
		t.Fatalf("assumed into %s", *id.Account)
	}
	// a member can see its organization but not manage it
	if got := must(orgs.NewFromConfig(member).DescribeOrganization(ctx, &orgs.DescribeOrganizationInput{})).Organization; *got.Id != *org.Id {
		t.Fatalf("member sees %+v", got)
	}
	if _, err := orgs.NewFromConfig(member).ListAccounts(ctx, &orgs.ListAccountsInput{}); code(err) != "AccessDeniedException" {
		t.Fatalf("member ListAccounts: %v", err)
	}

	must(c.MoveAccount(ctx, &orgs.MoveAccountInput{AccountId: &acct, SourceParentId: root.Id, DestinationParentId: ou.Id}))
	if got := must(c.ListAccountsForParent(ctx, &orgs.ListAccountsForParentInput{ParentId: ou.Id})).Accounts; len(got) != 1 || *got[0].Id != acct {
		t.Fatalf("OU accounts = %+v", got)
	}
	if p := must(c.ListParents(ctx, &orgs.ListParentsInput{ChildId: &acct})).Parents; *p[0].Id != *ou.Id || p[0].Type != types.ParentTypeOrganizationalUnit {
		t.Fatalf("parents = %+v", p)
	}
	if ch := must(c.ListChildren(ctx, &orgs.ListChildrenInput{ParentId: root.Id, ChildType: types.ChildTypeOrganizationalUnit})).Children; len(ch) != 1 || *ch[0].Id != *ou.Id {
		t.Fatalf("children = %+v", ch)
	}
	if _, err := c.DeleteOrganizationalUnit(ctx, &orgs.DeleteOrganizationalUnitInput{OrganizationalUnitId: ou.Id}); code(err) != "OrganizationalUnitNotEmptyException" {
		t.Fatalf("delete non-empty OU: %v", err)
	}
	if _, err := c.DescribeAccount(ctx, &orgs.DescribeAccountInput{AccountId: aws.String("123456789012")}); code(err) != "AccountNotFoundException" {
		t.Fatalf("unknown account: %v", err)
	}

	// policies
	deny := `{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"ec2:*","Resource":"*"}]}`
	p := must(c.CreatePolicy(ctx, &orgs.CreatePolicyInput{Name: aws.String("no-ec2"), Description: aws.String("d"), Content: &deny,
		Type: types.PolicyTypeServiceControlPolicy})).Policy.PolicySummary
	if _, err := c.AttachPolicy(ctx, &orgs.AttachPolicyInput{PolicyId: p.Id, TargetId: &acct}); code(err) != "PolicyTypeNotEnabledException" {
		t.Fatalf("attach before enabling: %v", err)
	}
	r := must(c.EnablePolicyType(ctx, &orgs.EnablePolicyTypeInput{RootId: root.Id, PolicyType: types.PolicyTypeServiceControlPolicy})).Root
	if len(r.PolicyTypes) != 1 || r.PolicyTypes[0].Status != types.PolicyTypeStatusEnabled {
		t.Fatalf("root = %+v", r)
	}
	must(c.AttachPolicy(ctx, &orgs.AttachPolicyInput{PolicyId: p.Id, TargetId: &acct}))
	if _, err := c.AttachPolicy(ctx, &orgs.AttachPolicyInput{PolicyId: p.Id, TargetId: &acct}); code(err) != "DuplicatePolicyAttachmentException" {
		t.Fatalf("duplicate attachment: %v", err)
	}
	if tg := must(c.ListTargetsForPolicy(ctx, &orgs.ListTargetsForPolicyInput{PolicyId: p.Id})).Targets; len(tg) != 1 || *tg[0].TargetId != acct || tg[0].Type != types.TargetTypeAccount {
		t.Fatalf("targets = %+v", tg)
	}
	if _, err := c.DeletePolicy(ctx, &orgs.DeletePolicyInput{PolicyId: p.Id}); code(err) != "PolicyInUseException" {
		t.Fatalf("delete attached policy: %v", err)
	}
	must(c.DetachPolicy(ctx, &orgs.DetachPolicyInput{PolicyId: p.Id, TargetId: &acct}))
	if _, err := c.DetachPolicy(ctx, &orgs.DetachPolicyInput{PolicyId: aws.String("p-FullAWSAccess"), TargetId: &acct}); code(err) != "ConstraintViolationException" {
		t.Fatalf("detach last SCP: %v", err)
	}
	// disabling SCPs detaches them, leaving FullAWSAccess alone
	must(c.AttachPolicy(ctx, &orgs.AttachPolicyInput{PolicyId: p.Id, TargetId: &acct}))
	must(c.DetachPolicy(ctx, &orgs.DetachPolicyInput{PolicyId: aws.String("p-FullAWSAccess"), TargetId: &acct}))
	must(c.DisablePolicyType(ctx, &orgs.DisablePolicyTypeInput{RootId: root.Id, PolicyType: types.PolicyTypeServiceControlPolicy}))
	if got := must(c.ListPoliciesForTarget(ctx, &orgs.ListPoliciesForTargetInput{TargetId: &acct, Filter: types.PolicyTypeServiceControlPolicy})).Policies; len(got) != 1 || *got[0].Id != "p-FullAWSAccess" {
		t.Fatalf("after disabling = %+v", got)
	}
	must(c.UpdatePolicy(ctx, &orgs.UpdatePolicyInput{PolicyId: p.Id, Name: aws.String("no-compute")}))
	must(c.DeletePolicy(ctx, &orgs.DeletePolicyInput{PolicyId: p.Id}))

	// tag policies merge from the root down
	must(c.EnablePolicyType(ctx, &orgs.EnablePolicyTypeInput{RootId: root.Id, PolicyType: types.PolicyTypeTagPolicy}))
	for target, doc := range map[string]string{
		*root.Id: `{"tags":{"env":{"tag_key":{"@@assign":"env"}},"team":{"tag_key":{"@@assign":"Team"}}}}`,
		acct:     `{"tags":{"team":{"tag_key":{"@@assign":"team"}}}}`,
	} {
		tp := must(c.CreatePolicy(ctx, &orgs.CreatePolicyInput{Name: aws.String("tags-" + target), Description: aws.String("t"), Content: aws.String(doc),
			Type: types.PolicyTypeTagPolicy})).Policy.PolicySummary
		must(c.AttachPolicy(ctx, &orgs.AttachPolicyInput{PolicyId: tp.Id, TargetId: aws.String(target)}))
	}
	eff := must(c.DescribeEffectivePolicy(ctx, &orgs.DescribeEffectivePolicyInput{PolicyType: types.EffectivePolicyTypeTagPolicy, TargetId: &acct})).EffectivePolicy
	if want := `{"tags":{"env":{"tag_key":{"@@assign":"env"}},"team":{"tag_key":{"@@assign":"team"}}}}`; *eff.PolicyContent != want {
		t.Fatalf("effective = %s", *eff.PolicyContent)
	}

	// delegated administrators, trusted services, tags
	must(c.RegisterDelegatedAdministrator(ctx, &orgs.RegisterDelegatedAdministratorInput{AccountId: &acct, ServicePrincipal: aws.String("guardduty.amazonaws.com")}))
	if _, err := c.RegisterDelegatedAdministrator(ctx, &orgs.RegisterDelegatedAdministratorInput{AccountId: &acct, ServicePrincipal: aws.String("guardduty.amazonaws.com")}); code(err) != "AccountAlreadyRegisteredException" {
		t.Fatalf("register twice: %v", err)
	}
	if d := must(c.ListDelegatedAdministrators(ctx, &orgs.ListDelegatedAdministratorsInput{})).DelegatedAdministrators; len(d) != 1 || *d[0].Id != acct {
		t.Fatalf("delegated = %+v", d)
	}
	must(c.EnableAWSServiceAccess(ctx, &orgs.EnableAWSServiceAccessInput{ServicePrincipal: aws.String("config.amazonaws.com")}))
	if e := must(c.ListAWSServiceAccessForOrganization(ctx, &orgs.ListAWSServiceAccessForOrganizationInput{})).EnabledServicePrincipals; len(e) != 1 {
		t.Fatalf("service access = %+v", e)
	}
	must(c.TagResource(ctx, &orgs.TagResourceInput{ResourceId: ou.Id, Tags: []types.Tag{{Key: aws.String("env"), Value: aws.String("dev")}}}))
	if tags := must(c.ListTagsForResource(ctx, &orgs.ListTagsForResourceInput{ResourceId: ou.Id})).Tags; len(tags) != 1 || *tags[0].Value != "dev" {
		t.Fatalf("tags = %+v", tags)
	}

	// invitations: the invited account accepts and joins
	h := must(c.InviteAccountToOrganization(ctx, &orgs.InviteAccountToOrganizationInput{Target: &types.HandshakeParty{Id: aws.String("111122223333"), Type: types.HandshakePartyTypeAccount}})).Handshake
	invited := orgs.NewFromConfig(awstest.ConfigAs(t, url, "111122223333", "test", ""))
	if got := must(invited.ListHandshakesForAccount(ctx, &orgs.ListHandshakesForAccountInput{})).Handshakes; len(got) != 1 || *got[0].Id != *h.Id {
		t.Fatalf("invitations = %+v", got)
	}
	if got := must(invited.AcceptHandshake(ctx, &orgs.AcceptHandshakeInput{HandshakeId: h.Id})).Handshake; got.State != types.HandshakeStateAccepted {
		t.Fatalf("accepted = %+v", got)
	}
	if a := must(c.DescribeAccount(ctx, &orgs.DescribeAccountInput{AccountId: aws.String("111122223333")})).Account; a.JoinedMethod != types.AccountJoinedMethodInvited {
		t.Fatalf("invited account = %+v", a)
	}
	must(invited.LeaveOrganization(ctx, &orgs.LeaveOrganizationInput{}))

	// closing and removing accounts, then deleting the organization
	if _, err := c.DeleteOrganization(ctx, &orgs.DeleteOrganizationInput{}); code(err) != "OrganizationNotEmptyException" {
		t.Fatalf("delete with members: %v", err)
	}
	must(c.CloseAccount(ctx, &orgs.CloseAccountInput{AccountId: &acct}))
	if a := must(c.DescribeAccount(ctx, &orgs.DescribeAccountInput{AccountId: &acct})).Account; a.Status != types.AccountStatusSuspended {
		t.Fatalf("closed account = %+v", a)
	}
	must(c.RemoveAccountFromOrganization(ctx, &orgs.RemoveAccountFromOrganizationInput{AccountId: &acct}))
	must(c.DeleteOrganization(ctx, &orgs.DeleteOrganizationInput{}))
}

// An SCP on a member account denies the call in strict mode even to an
// administrator; the management account is never restricted.
func TestSCPEnforcement(t *testing.T) {
	h, url := serve(t)
	mgmt := awstest.Config(t, url)
	c := orgs.NewFromConfig(mgmt)
	must(c.CreateOrganization(ctx, &orgs.CreateOrganizationInput{}))
	root := must(c.ListRoots(ctx, &orgs.ListRootsInput{})).Roots[0].Id
	must(c.EnablePolicyType(ctx, &orgs.EnablePolicyTypeInput{RootId: root, PolicyType: types.PolicyTypeServiceControlPolicy}))
	acct := newAccount(t, c, "sandbox")
	h.Cfg.IAM = "strict"

	dev := s3.NewFromConfig(adminUser(t, url, assume(t, url, mgmt, acct), "dev"), func(o *s3.Options) { o.UsePathStyle = true })
	ops := s3.NewFromConfig(adminUser(t, url, mgmt, "ops"), func(o *s3.Options) { o.UsePathStyle = true })
	create := func(c *s3.Client, bucket string) error {
		_, err := c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
		return err
	}
	if err := create(dev, "dev-before"); err != nil {
		t.Fatalf("FullAWSAccess allows: %v", err)
	}
	scp := func(name, doc string) *string {
		return must(c.CreatePolicy(ctx, &orgs.CreatePolicyInput{Name: aws.String(name), Description: aws.String("d"), Content: &doc,
			Type: types.PolicyTypeServiceControlPolicy})).Policy.PolicySummary.Id
	}
	attach := func(policy, target *string) {
		must(c.AttachPolicy(ctx, &orgs.AttachPolicyInput{PolicyId: policy, TargetId: target}))
	}
	detach := func(policy, target *string) {
		must(c.DetachPolicy(ctx, &orgs.DetachPolicyInput{PolicyId: policy, TargetId: target}))
	}
	full := aws.String("p-FullAWSAccess")

	// every level must allow: an allow-list replacing FullAWSAccess on the account
	allow := scp("list-only", `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:List*","Resource":"*"}]}`)
	attach(allow, &acct)
	detach(full, &acct)
	if err := create(dev, "dev-implicit"); !strings.Contains(fmt.Sprint(err), "because no service control policy allows the s3:CreateBucket action") {
		t.Fatalf("implicit SCP deny: %v", err)
	}
	if _, err := dev.ListBuckets(ctx, &s3.ListBucketsInput{}); err != nil {
		t.Fatalf("allow-listed: %v", err)
	}
	attach(full, &acct)
	detach(allow, &acct)

	deny := scp("no-buckets", `{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"s3:CreateBucket","Resource":"*"}]}`)
	attach(deny, &acct)
	err := create(dev, "dev-denied")
	if code(err) != "AccessDenied" || !strings.Contains(err.Error(), "with an explicit deny in a service control policy") {
		t.Fatalf("SCP deny: %v", err)
	}
	if _, err := dev.ListBuckets(ctx, &s3.ListBucketsInput{}); err != nil {
		t.Fatalf("list still allowed: %v", err)
	}
	// SCPs never restrict the management account, even attached to the root
	attach(deny, root)
	if err := create(ops, "ops-bucket"); err != nil {
		t.Fatalf("management account restricted: %v", err)
	}
	detach(deny, root)

	// soft mode only logs the decision
	h.Cfg.IAM = "soft"
	if err := create(dev, "dev-soft"); err != nil {
		t.Fatalf("soft mode blocked: %v", err)
	}
	if e := h.Requests.Query(0, 1, "s3", "", false); len(e) != 1 || e[0].IAM != "deny s3:CreateBucket: with an explicit deny in a service control policy" {
		t.Fatalf("decision not logged: %+v", e)
	}
	h.Cfg.IAM = "strict"
	detach(deny, &acct)
	if err := create(dev, "dev-after"); err != nil {
		t.Fatalf("detaching restores access: %v", err)
	}
}
