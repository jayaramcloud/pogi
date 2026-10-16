package iam_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	elb "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2"
	elbtypes "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2/types"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/iam/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/smithy-go"
	elbv2svc "github.com/nrndev/pogi/internal/aws/elbv2"
	iamsvc "github.com/nrndev/pogi/internal/aws/iam"
	sqssvc "github.com/nrndev/pogi/internal/aws/sqs"
	stssvc "github.com/nrndev/pogi/internal/aws/sts"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/core"
)

const (
	trust  = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}`
	readS3 = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*"}]}`
	basic  = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
)

func serve(t *testing.T) (*iamsvc.Service, string) {
	var svc *iamsvc.Service
	_, endpoint := core.ServeTest(t,
		func(h *core.Hub) core.Service { svc = iamsvc.New(h); return svc },
		func(h *core.Hub) core.Service { return stssvc.New(h) })
	return svc, endpoint
}

func is[T error](err error) bool {
	var e T
	return errors.As(err, &e)
}

func decoded(t *testing.T, doc *string) string {
	t.Helper()
	s, err := url.QueryUnescape(aws.ToString(doc))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestRoleTerraformFlow(t *testing.T) {
	ctx := context.Background()
	svc, endpoint := serve(t)
	c := iam.NewFromConfig(awstest.Config(t, endpoint))
	role, err := c.CreateRole(ctx, &iam.CreateRoleInput{RoleName: aws.String("app"), Path: aws.String("/svc/"),
		AssumeRolePolicyDocument: aws.String(trust), MaxSessionDuration: aws.Int32(7200),
		Tags: []types.Tag{{Key: aws.String("env"), Value: aws.String("dev")}}})
	if err != nil {
		t.Fatal(err)
	}
	r := role.Role
	if *r.Arn != "arn:aws:iam::000000000000:role/svc/app" || !strings.HasPrefix(*r.RoleId, "AROA") || len(*r.RoleId) != 21 || *r.MaxSessionDuration != 7200 {
		t.Fatalf("role = %+v", r)
	}
	if _, err := c.CreateRole(ctx, &iam.CreateRoleInput{RoleName: aws.String("APP"), AssumeRolePolicyDocument: aws.String(trust)}); !is[*types.EntityAlreadyExistsException](err) {
		t.Fatalf("duplicate role (names are case-insensitive) err = %v", err)
	}
	if _, err := c.CreateRole(ctx, &iam.CreateRoleInput{RoleName: aws.String("bad"), AssumeRolePolicyDocument: aws.String(readS3)}); !is[*types.MalformedPolicyDocumentException](err) {
		t.Fatalf("trust policy without Principal err = %v", err)
	}
	got, err := c.GetRole(ctx, &iam.GetRoleInput{RoleName: aws.String("app")})
	if err != nil || decoded(t, got.Role.AssumeRolePolicyDocument) != trust || got.Role.RoleLastUsed == nil || len(got.Role.Tags) != 1 {
		t.Fatalf("GetRole = %+v %v", got, err)
	}

	pol, err := c.CreatePolicy(ctx, &iam.CreatePolicyInput{PolicyName: aws.String("read-s3"), PolicyDocument: aws.String(readS3)})
	if err != nil || *pol.Policy.Arn != "arn:aws:iam::000000000000:policy/read-s3" || *pol.Policy.DefaultVersionId != "v1" {
		t.Fatalf("CreatePolicy = %+v %v", pol, err)
	}
	if _, err := c.CreatePolicy(ctx, &iam.CreatePolicyInput{PolicyName: aws.String("bad"), PolicyDocument: aws.String(`{"Statement":"nope"}`)}); !is[*types.MalformedPolicyDocumentException](err) {
		t.Fatalf("malformed policy err = %v", err)
	}
	for _, arn := range []string{*pol.Policy.Arn, basic} {
		if _, err := c.AttachRolePolicy(ctx, &iam.AttachRolePolicyInput{RoleName: aws.String("app"), PolicyArn: aws.String(arn)}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := c.AttachRolePolicy(ctx, &iam.AttachRolePolicyInput{RoleName: aws.String("app"), PolicyArn: aws.String("arn:aws:iam::000000000000:policy/NoSuchPolicy")}); !is[*types.NoSuchEntityException](err) {
		t.Fatalf("unknown customer managed policy err = %v", err)
	}
	// AWS has 1,000+ managed policies; ones pogi has no document for still exist (granting nothing)
	if _, err := c.GetPolicy(ctx, &iam.GetPolicyInput{PolicyArn: aws.String("arn:aws:iam::aws:policy/service-role/AWSSomethingNew")}); err != nil {
		t.Fatalf("unlisted AWS managed policy: %v", err)
	}
	if _, err := c.PutRolePolicy(ctx, &iam.PutRolePolicyInput{RoleName: aws.String("app"), PolicyName: aws.String("inline"), PolicyDocument: aws.String(readS3)}); err != nil {
		t.Fatal(err)
	}
	att, err := c.ListAttachedRolePolicies(ctx, &iam.ListAttachedRolePoliciesInput{RoleName: aws.String("app")})
	if err != nil || len(att.AttachedPolicies) != 2 || *att.AttachedPolicies[1].PolicyName != "AWSLambdaBasicExecutionRole" {
		t.Fatalf("attached = %+v %v", att, err)
	}
	inl, err := c.ListRolePolicies(ctx, &iam.ListRolePoliciesInput{RoleName: aws.String("app")})
	if err != nil || len(inl.PolicyNames) != 1 {
		t.Fatalf("inline = %+v %v", inl, err)
	}
	rp, err := c.GetRolePolicy(ctx, &iam.GetRolePolicyInput{RoleName: aws.String("app"), PolicyName: aws.String("inline")})
	if err != nil || *rp.RoleName != "app" || decoded(t, rp.PolicyDocument) != readS3 {
		t.Fatalf("GetRolePolicy = %+v %v", rp, err)
	}
	mp, err := c.GetPolicy(ctx, &iam.GetPolicyInput{PolicyArn: aws.String(basic)})
	if err != nil || *mp.Policy.AttachmentCount != 1 || *mp.Policy.Path != "/service-role/" {
		t.Fatalf("AWS managed policy = %+v %v", mp, err)
	}
	mv, err := c.GetPolicyVersion(ctx, &iam.GetPolicyVersionInput{PolicyArn: aws.String(basic), VersionId: mp.Policy.DefaultVersionId})
	if err != nil || !strings.Contains(decoded(t, mv.PolicyVersion.Document), "logs:PutLogEvents") {
		t.Fatalf("AWS managed policy version = %+v %v", mv, err)
	}
	ents, err := c.ListEntitiesForPolicy(ctx, &iam.ListEntitiesForPolicyInput{PolicyArn: pol.Policy.Arn})
	if err != nil || len(ents.PolicyRoles) != 1 || *ents.PolicyRoles[0].RoleName != "app" {
		t.Fatalf("entities = %+v %v", ents, err)
	}
	for _, p := range []string{*r.Arn, "arn:aws:sts::000000000000:assumed-role/app/session"} {
		if docs := svc.PoliciesFor(p); len(docs) != 3 || docs[0] != readS3 || !strings.Contains(docs[2], "logs:CreateLogGroup") {
			t.Fatalf("PoliciesFor(%s) = %v", p, docs)
		}
	}

	// Terraform destroy: the role and the policy cannot go while attached.
	if _, err := c.DeleteRole(ctx, &iam.DeleteRoleInput{RoleName: aws.String("app")}); !is[*types.DeleteConflictException](err) {
		t.Fatalf("DeleteRole with policies err = %v", err)
	}
	if _, err := c.DeletePolicy(ctx, &iam.DeletePolicyInput{PolicyArn: pol.Policy.Arn}); !is[*types.DeleteConflictException](err) {
		t.Fatalf("DeletePolicy while attached err = %v", err)
	}
	for _, arn := range []string{*pol.Policy.Arn, basic} {
		if _, err := c.DetachRolePolicy(ctx, &iam.DetachRolePolicyInput{RoleName: aws.String("app"), PolicyArn: aws.String(arn)}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := c.DetachRolePolicy(ctx, &iam.DetachRolePolicyInput{RoleName: aws.String("app"), PolicyArn: aws.String(basic)}); !is[*types.NoSuchEntityException](err) {
		t.Fatalf("detach twice err = %v", err)
	}
	if _, err := c.DeleteRolePolicy(ctx, &iam.DeleteRolePolicyInput{RoleName: aws.String("app"), PolicyName: aws.String("inline")}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.DeleteRole(ctx, &iam.DeleteRoleInput{RoleName: aws.String("app")}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.GetRole(ctx, &iam.GetRoleInput{RoleName: aws.String("app")}); !is[*types.NoSuchEntityException](err) {
		t.Fatalf("GetRole after delete err = %v", err)
	}
	if _, err := c.DeletePolicy(ctx, &iam.DeletePolicyInput{PolicyArn: pol.Policy.Arn}); err != nil {
		t.Fatal(err)
	}
}

func TestUsersGroupsAndAccessKeys(t *testing.T) {
	ctx := context.Background()
	svc, endpoint := serve(t)
	c := iam.NewFromConfig(awstest.Config(t, endpoint))
	u, err := c.CreateUser(ctx, &iam.CreateUserInput{UserName: aws.String("alice"), Path: aws.String("/eng/")})
	if err != nil || *u.User.Arn != "arn:aws:iam::000000000000:user/eng/alice" || !strings.HasPrefix(*u.User.UserId, "AIDA") {
		t.Fatalf("CreateUser = %+v %v", u, err)
	}
	g, err := c.CreateGroup(ctx, &iam.CreateGroupInput{GroupName: aws.String("devs")})
	if err != nil || !strings.HasPrefix(*g.Group.GroupId, "AGPA") {
		t.Fatalf("CreateGroup = %+v %v", g, err)
	}
	must := func(_ any, err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(c.AddUserToGroup(ctx, &iam.AddUserToGroupInput{GroupName: aws.String("devs"), UserName: aws.String("alice")}))
	must(c.AttachGroupPolicy(ctx, &iam.AttachGroupPolicyInput{GroupName: aws.String("devs"), PolicyArn: aws.String("arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")}))
	must(c.PutUserPolicy(ctx, &iam.PutUserPolicyInput{UserName: aws.String("alice"), PolicyName: aws.String("own"), PolicyDocument: aws.String(readS3)}))
	gg, err := c.GetGroup(ctx, &iam.GetGroupInput{GroupName: aws.String("devs")})
	if err != nil || len(gg.Users) != 1 || *gg.Users[0].UserName != "alice" || *gg.Group.GroupName != "devs" {
		t.Fatalf("GetGroup = %+v %v", gg, err)
	}
	gs, err := c.ListGroupsForUser(ctx, &iam.ListGroupsForUserInput{UserName: aws.String("alice")})
	if err != nil || len(gs.Groups) != 1 {
		t.Fatalf("ListGroupsForUser = %+v %v", gs, err)
	}
	if docs := svc.PoliciesFor(*u.User.Arn); len(docs) != 2 || docs[0] != readS3 || !strings.Contains(docs[1], "s3:Get*") {
		t.Fatalf("PoliciesFor(user) = %v", docs)
	}

	key, err := c.CreateAccessKey(ctx, &iam.CreateAccessKeyInput{UserName: aws.String("alice")})
	if err != nil {
		t.Fatal(err)
	}
	k := key.AccessKey
	if !strings.HasPrefix(*k.AccessKeyId, "AKIA") || len(*k.AccessKeyId) != 20 || len(*k.SecretAccessKey) != 40 || k.Status != types.StatusTypeActive {
		t.Fatalf("access key = %+v", k)
	}
	// Requests signed with the new key run as alice.
	as := awstest.ConfigAs(t, endpoint, *k.AccessKeyId, *k.SecretAccessKey, "")
	id, err := sts.NewFromConfig(as).GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil || *id.Arn != *u.User.Arn || *id.UserId != *u.User.UserId || *id.Account != "000000000000" {
		t.Fatalf("caller identity = %+v %v", id, err)
	}
	me, err := iam.NewFromConfig(as).GetUser(ctx, &iam.GetUserInput{})
	if err != nil || *me.User.UserName != "alice" {
		t.Fatalf("GetUser as alice = %+v %v", me, err)
	}
	root, err := c.GetUser(ctx, &iam.GetUserInput{})
	if err != nil || *root.User.Arn != "arn:aws:iam::000000000000:root" {
		t.Fatalf("GetUser as root = %+v %v", root, err)
	}

	// Persisted keys keep resolving after a restart.
	saved, err := svc.Save()
	if err != nil {
		t.Fatal(err)
	}
	svc2, endpoint2 := serve(t)
	if err := svc2.Load(saved); err != nil {
		t.Fatal(err)
	}
	id, err = sts.NewFromConfig(awstest.ConfigAs(t, endpoint2, *k.AccessKeyId, *k.SecretAccessKey, "")).GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil || *id.Arn != *u.User.Arn {
		t.Fatalf("identity after Load = %+v %v", id, err)
	}

	if _, err := c.DeleteUser(ctx, &iam.DeleteUserInput{UserName: aws.String("alice")}); !is[*types.DeleteConflictException](err) {
		t.Fatalf("DeleteUser with keys err = %v", err)
	}
	must(c.UpdateAccessKey(ctx, &iam.UpdateAccessKeyInput{UserName: aws.String("alice"), AccessKeyId: k.AccessKeyId, Status: types.StatusTypeInactive}))
	keys, err := c.ListAccessKeys(ctx, &iam.ListAccessKeysInput{UserName: aws.String("alice")})
	if err != nil || len(keys.AccessKeyMetadata) != 1 || keys.AccessKeyMetadata[0].Status != types.StatusTypeInactive || keys.AccessKeyMetadata[0].UserName == nil {
		t.Fatalf("ListAccessKeys = %+v %v", keys, err)
	}
	lu, err := c.GetAccessKeyLastUsed(ctx, &iam.GetAccessKeyLastUsedInput{AccessKeyId: k.AccessKeyId})
	if err != nil || *lu.UserName != "alice" || *lu.AccessKeyLastUsed.ServiceName != "N/A" {
		t.Fatalf("GetAccessKeyLastUsed = %+v %v", lu, err)
	}
	must(c.DeleteAccessKey(ctx, &iam.DeleteAccessKeyInput{UserName: aws.String("alice"), AccessKeyId: k.AccessKeyId}))
	must(c.DeleteUserPolicy(ctx, &iam.DeleteUserPolicyInput{UserName: aws.String("alice"), PolicyName: aws.String("own")}))
	if _, err := c.DeleteGroup(ctx, &iam.DeleteGroupInput{GroupName: aws.String("devs")}); !is[*types.DeleteConflictException](err) {
		t.Fatalf("DeleteGroup with members err = %v", err)
	}
	must(c.RemoveUserFromGroup(ctx, &iam.RemoveUserFromGroupInput{GroupName: aws.String("devs"), UserName: aws.String("alice")}))
	must(c.DetachGroupPolicy(ctx, &iam.DetachGroupPolicyInput{GroupName: aws.String("devs"), PolicyArn: aws.String("arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")}))
	must(c.DeleteGroup(ctx, &iam.DeleteGroupInput{GroupName: aws.String("devs")}))
	must(c.DeleteUser(ctx, &iam.DeleteUserInput{UserName: aws.String("alice")}))
	if _, err := c.GetUser(ctx, &iam.GetUserInput{UserName: aws.String("alice")}); !is[*types.NoSuchEntityException](err) {
		t.Fatalf("GetUser after delete err = %v", err)
	}
}

func TestPolicyVersionsAndPagination(t *testing.T) {
	ctx := context.Background()
	_, endpoint := serve(t)
	c := iam.NewFromConfig(awstest.Config(t, endpoint))
	p, err := c.CreatePolicy(ctx, &iam.CreatePolicyInput{PolicyName: aws.String("versions"), PolicyDocument: aws.String(readS3)})
	if err != nil {
		t.Fatal(err)
	}
	arn := p.Policy.Arn
	for i := range 4 {
		v, err := c.CreatePolicyVersion(ctx, &iam.CreatePolicyVersionInput{PolicyArn: arn, PolicyDocument: aws.String(readS3), SetAsDefault: i == 3})
		if err != nil || *v.PolicyVersion.VersionId != fmt.Sprintf("v%d", i+2) {
			t.Fatalf("CreatePolicyVersion = %+v %v", v, err)
		}
	}
	if _, err := c.CreatePolicyVersion(ctx, &iam.CreatePolicyVersionInput{PolicyArn: arn, PolicyDocument: aws.String(readS3)}); !is[*types.LimitExceededException](err) {
		t.Fatalf("sixth version err = %v", err)
	}
	if gp, err := c.GetPolicy(ctx, &iam.GetPolicyInput{PolicyArn: arn}); err != nil || *gp.Policy.DefaultVersionId != "v5" {
		t.Fatalf("GetPolicy = %+v %v", gp, err)
	}
	if _, err := c.DeletePolicyVersion(ctx, &iam.DeletePolicyVersionInput{PolicyArn: arn, VersionId: aws.String("v5")}); !is[*types.DeleteConflictException](err) {
		t.Fatalf("delete default version err = %v", err)
	}
	if _, err := c.SetDefaultPolicyVersion(ctx, &iam.SetDefaultPolicyVersionInput{PolicyArn: arn, VersionId: aws.String("v1")}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.DeletePolicy(ctx, &iam.DeletePolicyInput{PolicyArn: arn}); !is[*types.DeleteConflictException](err) {
		t.Fatalf("DeletePolicy with versions err = %v", err)
	}
	for _, v := range []string{"v2", "v3", "v4", "v5"} {
		if _, err := c.DeletePolicyVersion(ctx, &iam.DeletePolicyVersionInput{PolicyArn: arn, VersionId: aws.String(v)}); err != nil {
			t.Fatal(err)
		}
	}
	vs, err := c.ListPolicyVersions(ctx, &iam.ListPolicyVersionsInput{PolicyArn: arn})
	if err != nil || len(vs.Versions) != 1 || !vs.Versions[0].IsDefaultVersion {
		t.Fatalf("ListPolicyVersions = %+v %v", vs, err)
	}
	if v, err := c.CreatePolicyVersion(ctx, &iam.CreatePolicyVersionInput{PolicyArn: arn, PolicyDocument: aws.String(readS3)}); err != nil || *v.PolicyVersion.VersionId != "v6" {
		t.Fatalf("version ids are never reused: %+v %v", v, err)
	}

	for i := range 5 {
		if _, err := c.CreateUser(ctx, &iam.CreateUserInput{UserName: aws.String(fmt.Sprintf("user%d", i))}); err != nil {
			t.Fatal(err)
		}
	}
	first, err := c.ListUsers(ctx, &iam.ListUsersInput{MaxItems: aws.Int32(2)})
	if err != nil || len(first.Users) != 2 || !first.IsTruncated || first.Marker == nil {
		t.Fatalf("first page = %+v %v", first, err)
	}
	pager := iam.NewListUsersPaginator(c, &iam.ListUsersInput{MaxItems: aws.Int32(2)})
	users, pages := 0, 0
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			t.Fatal(err)
		}
		users, pages = users+len(page.Users), pages+1
	}
	if users != 5 || pages != 3 {
		t.Fatalf("paginated %d users in %d pages", users, pages)
	}
	local, err := c.ListPolicies(ctx, &iam.ListPoliciesInput{Scope: types.PolicyScopeTypeLocal})
	if err != nil || len(local.Policies) != 1 {
		t.Fatalf("local policies = %+v %v", local, err)
	}
	awsPolicies, err := c.ListPolicies(ctx, &iam.ListPoliciesInput{Scope: types.PolicyScopeTypeAws})
	if err != nil || len(awsPolicies.Policies) < 17 {
		t.Fatalf("AWS managed policies = %d %v", len(awsPolicies.Policies), err)
	}
	if attached, err := c.ListPolicies(ctx, &iam.ListPoliciesInput{OnlyAttached: true}); err != nil || len(attached.Policies) != 0 {
		t.Fatalf("attached policies = %+v %v", attached, err)
	}
	// Every embedded AWS managed document passes the customer policy validator.
	for _, p := range awsPolicies.Policies {
		v, err := c.GetPolicyVersion(ctx, &iam.GetPolicyVersionInput{PolicyArn: p.Arn, VersionId: p.DefaultVersionId})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.CreatePolicy(ctx, &iam.CreatePolicyInput{PolicyName: aws.String("copy-" + *p.PolicyName), PolicyDocument: aws.String(decoded(t, v.PolicyVersion.Document))}); err != nil {
			t.Fatalf("AWS managed %s: %v", *p.Arn, err)
		}
	}
}

func TestInstanceProfilesProvidersAndAccount(t *testing.T) {
	ctx := context.Background()
	_, endpoint := serve(t)
	c := iam.NewFromConfig(awstest.Config(t, endpoint))
	must := func(_ any, err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(c.CreateRole(ctx, &iam.CreateRoleInput{RoleName: aws.String("ec2"), AssumeRolePolicyDocument: aws.String(trust)}))
	ip, err := c.CreateInstanceProfile(ctx, &iam.CreateInstanceProfileInput{InstanceProfileName: aws.String("ec2-profile")})
	if err != nil || *ip.InstanceProfile.Arn != "arn:aws:iam::000000000000:instance-profile/ec2-profile" || !strings.HasPrefix(*ip.InstanceProfile.InstanceProfileId, "AIPA") {
		t.Fatalf("CreateInstanceProfile = %+v %v", ip, err)
	}
	add := &iam.AddRoleToInstanceProfileInput{InstanceProfileName: aws.String("ec2-profile"), RoleName: aws.String("ec2")}
	must(c.AddRoleToInstanceProfile(ctx, add))
	if _, err := c.AddRoleToInstanceProfile(ctx, add); !is[*types.LimitExceededException](err) {
		t.Fatalf("second role err = %v", err)
	}
	got, err := c.GetInstanceProfile(ctx, &iam.GetInstanceProfileInput{InstanceProfileName: aws.String("ec2-profile")})
	if err != nil || len(got.InstanceProfile.Roles) != 1 || *got.InstanceProfile.Roles[0].RoleName != "ec2" {
		t.Fatalf("GetInstanceProfile = %+v %v", got, err)
	}
	if fr, err := c.ListInstanceProfilesForRole(ctx, &iam.ListInstanceProfilesForRoleInput{RoleName: aws.String("ec2")}); err != nil || len(fr.InstanceProfiles) != 1 {
		t.Fatalf("ListInstanceProfilesForRole = %+v %v", fr, err)
	}
	if _, err := c.DeleteRole(ctx, &iam.DeleteRoleInput{RoleName: aws.String("ec2")}); !is[*types.DeleteConflictException](err) {
		t.Fatalf("DeleteRole in a profile err = %v", err)
	}
	if _, err := c.DeleteInstanceProfile(ctx, &iam.DeleteInstanceProfileInput{InstanceProfileName: aws.String("ec2-profile")}); !is[*types.DeleteConflictException](err) {
		t.Fatalf("DeleteInstanceProfile with a role err = %v", err)
	}
	must(c.RemoveRoleFromInstanceProfile(ctx, &iam.RemoveRoleFromInstanceProfileInput{InstanceProfileName: aws.String("ec2-profile"), RoleName: aws.String("ec2")}))
	must(c.DeleteInstanceProfile(ctx, &iam.DeleteInstanceProfileInput{InstanceProfileName: aws.String("ec2-profile")}))

	oidc, err := c.CreateOpenIDConnectProvider(ctx, &iam.CreateOpenIDConnectProviderInput{Url: aws.String("https://token.actions.githubusercontent.com"),
		ClientIDList: []string{"sts.amazonaws.com"}, ThumbprintList: []string{strings.Repeat("a", 40)}})
	if err != nil || *oidc.OpenIDConnectProviderArn != "arn:aws:iam::000000000000:oidc-provider/token.actions.githubusercontent.com" {
		t.Fatalf("CreateOpenIDConnectProvider = %+v %v", oidc, err)
	}
	must(c.AddClientIDToOpenIDConnectProvider(ctx, &iam.AddClientIDToOpenIDConnectProviderInput{OpenIDConnectProviderArn: oidc.OpenIDConnectProviderArn, ClientID: aws.String("other")}))
	op, err := c.GetOpenIDConnectProvider(ctx, &iam.GetOpenIDConnectProviderInput{OpenIDConnectProviderArn: oidc.OpenIDConnectProviderArn})
	if err != nil || *op.Url != "token.actions.githubusercontent.com" || len(op.ClientIDList) != 2 || len(op.ThumbprintList) != 1 {
		t.Fatalf("GetOpenIDConnectProvider = %+v %v", op, err)
	}
	must(c.DeleteOpenIDConnectProvider(ctx, &iam.DeleteOpenIDConnectProviderInput{OpenIDConnectProviderArn: oidc.OpenIDConnectProviderArn}))
	if ops, err := c.ListOpenIDConnectProviders(ctx, &iam.ListOpenIDConnectProvidersInput{}); err != nil || len(ops.OpenIDConnectProviderList) != 0 {
		t.Fatalf("ListOpenIDConnectProviders = %+v %v", ops, err)
	}

	certPEM := selfSigned(t)
	up, err := c.UploadServerCertificate(ctx, &iam.UploadServerCertificateInput{ServerCertificateName: aws.String("web"), CertificateBody: aws.String(certPEM), PrivateKey: aws.String("unused")})
	if err != nil || up.ServerCertificateMetadata.Expiration == nil {
		t.Fatalf("UploadServerCertificate = %+v %v", up, err)
	}
	if sc, err := c.GetServerCertificate(ctx, &iam.GetServerCertificateInput{ServerCertificateName: aws.String("web")}); err != nil || *sc.ServerCertificate.CertificateBody != certPEM {
		t.Fatalf("GetServerCertificate = %+v %v", sc, err)
	}
	if _, err := c.UploadServerCertificate(ctx, &iam.UploadServerCertificateInput{ServerCertificateName: aws.String("junk"), CertificateBody: aws.String("junk"), PrivateKey: aws.String("x")}); !is[*types.MalformedCertificateException](err) {
		t.Fatalf("junk certificate err = %v", err)
	}

	if _, err := c.GetAccountPasswordPolicy(ctx, &iam.GetAccountPasswordPolicyInput{}); !is[*types.NoSuchEntityException](err) {
		t.Fatalf("password policy before set err = %v", err)
	}
	must(c.UpdateAccountPasswordPolicy(ctx, &iam.UpdateAccountPasswordPolicyInput{MinimumPasswordLength: aws.Int32(12), RequireSymbols: true, MaxPasswordAge: aws.Int32(90)}))
	pp, err := c.GetAccountPasswordPolicy(ctx, &iam.GetAccountPasswordPolicyInput{})
	if err != nil || *pp.PasswordPolicy.MinimumPasswordLength != 12 || !pp.PasswordPolicy.RequireSymbols || !pp.PasswordPolicy.ExpirePasswords {
		t.Fatalf("password policy = %+v %v", pp, err)
	}
	must(c.CreateAccountAlias(ctx, &iam.CreateAccountAliasInput{AccountAlias: aws.String("my-org")}))
	if al, err := c.ListAccountAliases(ctx, &iam.ListAccountAliasesInput{}); err != nil || len(al.AccountAliases) != 1 {
		t.Fatalf("ListAccountAliases = %+v %v", al, err)
	}

	slr, err := c.CreateServiceLinkedRole(ctx, &iam.CreateServiceLinkedRoleInput{AWSServiceName: aws.String("autoscaling.amazonaws.com")})
	if err != nil || *slr.Role.RoleName != "AWSServiceRoleForAutoScaling" || *slr.Role.Path != "/aws-service-role/autoscaling.amazonaws.com/" {
		t.Fatalf("CreateServiceLinkedRole = %+v %v", slr, err)
	}
	sum, err := c.GetAccountSummary(ctx, &iam.GetAccountSummaryInput{})
	if err != nil || sum.SummaryMap["Roles"] != 2 || sum.SummaryMap["UsersQuota"] != 5000 {
		t.Fatalf("GetAccountSummary = %+v %v", sum, err)
	}
	must(c.PutRolePolicy(ctx, &iam.PutRolePolicyInput{RoleName: aws.String("ec2"), PolicyName: aws.String("p"), PolicyDocument: aws.String(readS3)}))
	det, err := c.GetAccountAuthorizationDetails(ctx, &iam.GetAccountAuthorizationDetailsInput{Filter: []types.EntityType{types.EntityTypeRole}})
	if err != nil || len(det.RoleDetailList) != 2 || len(det.UserDetailList) != 0 {
		t.Fatalf("GetAccountAuthorizationDetails = %+v %v", det, err)
	}
	if rd := det.RoleDetailList[1]; *rd.RoleName != "ec2" || len(rd.RolePolicyList) != 1 || decoded(t, rd.RolePolicyList[0].PolicyDocument) != readS3 {
		t.Fatalf("role detail = %+v", rd)
	}
	sim, err := c.SimulatePrincipalPolicy(ctx, &iam.SimulatePrincipalPolicyInput{PolicySourceArn: slr.Role.Arn, ActionNames: []string{"s3:GetObject"}})
	if err != nil || len(sim.EvaluationResults) != 1 || sim.EvaluationResults[0].EvalDecision != types.PolicyEvaluationDecisionTypeImplicitDeny {
		t.Fatalf("SimulatePrincipalPolicy = %+v %v", sim, err)
	}
	del, err := c.DeleteServiceLinkedRole(ctx, &iam.DeleteServiceLinkedRoleInput{RoleName: slr.Role.RoleName})
	if err != nil {
		t.Fatal(err)
	}
	st, err := c.GetServiceLinkedRoleDeletionStatus(ctx, &iam.GetServiceLinkedRoleDeletionStatusInput{DeletionTaskId: del.DeletionTaskId})
	if err != nil || st.Status != types.DeletionTaskStatusTypeSucceeded {
		t.Fatalf("deletion status = %+v %v", st, err)
	}
}

func selfSigned(t *testing.T) string {
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "example.com"}, NotBefore: time.Now(), NotAfter: time.Now().Add(time.Hour)}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
}

func apiCode(err error) string {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		return ae.ErrorCode()
	}
	return ""
}

// floci #4484 #4407: tag keys are case-insensitive and cannot repeat in one
// request; at most 50 tags, 128-char keys, 256-char values, no aws: keys, no
// illegal characters.
func TestTagRules(t *testing.T) {
	ctx := context.Background()
	_, endpoint := serve(t)
	c := iam.NewFromConfig(awstest.Config(t, endpoint))
	must(c.CreateUser(ctx, &iam.CreateUserInput{UserName: aws.String("u")}))
	tag := func(k, v string) types.Tag { return types.Tag{Key: aws.String(k), Value: aws.String(v)} }
	tagUser := func(tags ...types.Tag) string {
		_, err := c.TagUser(ctx, &iam.TagUserInput{UserName: aws.String("u"), Tags: tags})
		return apiCode(err)
	}
	var many []types.Tag
	for i := 0; i < 51; i++ {
		many = append(many, tag(fmt.Sprintf("k%d", i), "v"))
	}
	for name, tc := range map[string]struct {
		tags []types.Tag
		want string
	}{
		"51 tags":        {many, "LimitExceeded"},
		"duplicate keys": {[]types.Tag{tag("a", "1"), tag("a", "2")}, "InvalidInput"},
		"Env and env":    {[]types.Tag{tag("Env", "1"), tag("env", "2")}, "InvalidInput"},
		"129-char key":   {[]types.Tag{tag(strings.Repeat("x", 129), "v")}, "InvalidInput"},
		"257-char value": {[]types.Tag{tag("k", strings.Repeat("x", 257))}, "InvalidInput"},
		"aws: key":       {[]types.Tag{tag("AWS:reserved", "v")}, "InvalidInput"},
		"illegal key":    {[]types.Tag{tag("bad key!#", "v")}, "InvalidInput"},
		"valid":          {[]types.Tag{tag("Cost", "1"), tag("team:a/b=c+d@e", "")}, ""},
	} {
		if got := tagUser(tc.tags...); got != tc.want {
			t.Errorf("%s: %q, want %q", name, got, tc.want)
		}
	}
	tagUser(tag("cost", "2")) // replaces Cost, not a second tag
	l := must(c.ListUserTags(ctx, &iam.ListUserTagsInput{UserName: aws.String("u")}))
	if len(l.Tags) != 2 || *l.Tags[0].Value != "2" {
		t.Errorf("tags after Cost then cost = %+v", l.Tags)
	}
}

// floci #4668 #4629: a denied JSON-protocol request answers 400
// AccessDeniedException, and aws:PrincipalIsAWSService is "false" for IAM callers.
func TestDenialStatusAndPrincipalIsAWSService(t *testing.T) {
	ctx := context.Background()
	h, url := core.ServeTest(t, func(h *core.Hub) core.Service { return iamsvc.New(h) }, func(h *core.Hub) core.Service { return sqssvc.New(h) })
	h.Cfg.IAM = "strict"
	admin := iam.NewFromConfig(awstest.Config(t, url))
	queues := func(user, policy string) *sqs.Client {
		must(admin.CreateUser(ctx, &iam.CreateUserInput{UserName: aws.String(user)}))
		if policy != "" {
			must(admin.PutUserPolicy(ctx, &iam.PutUserPolicyInput{UserName: aws.String(user), PolicyName: aws.String("p"), PolicyDocument: aws.String(policy)}))
		}
		k := must(admin.CreateAccessKey(ctx, &iam.CreateAccessKeyInput{UserName: aws.String(user)}))
		return sqs.NewFromConfig(awstest.ConfigAs(t, url, *k.AccessKey.AccessKeyId, *k.AccessKey.SecretAccessKey, ""))
	}
	_, err := queues("nobody", "").CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("q")})
	var re *awshttp.ResponseError
	if !errors.As(err, &re) || re.HTTPStatusCode() != 400 || apiCode(err) != "AccessDeniedException" {
		t.Errorf("JSON-protocol denial: %v", err)
	}
	allow := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"sqs:*","Resource":"*","Condition":{"Bool":{"aws:PrincipalIsAWSService":"false"}}}]}`
	if _, err := queues("u", allow).CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("q1")}); err != nil {
		t.Errorf("Allow with PrincipalIsAWSService=false: %v", err)
	}
	deny := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"sqs:*","Resource":"*"},{"Effect":"Deny","Action":"sqs:*","Resource":"*","Condition":{"Bool":{"aws:PrincipalIsAWSService":"false"}}}]}`
	if _, err := queues("u2", deny).CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("q2")}); err == nil {
		t.Error("Deny with PrincipalIsAWSService=false did not apply")
	}
}

// floci #5087: a server certificate a listener uses cannot be deleted, and a
// listener cannot use a certificate IAM does not have.
func TestServerCertificateInUse(t *testing.T) {
	ctx := context.Background()
	_, url := core.ServeTest(t, func(h *core.Hub) core.Service { return iamsvc.New(h) }, func(h *core.Hub) core.Service { return elbv2svc.New(h) })
	cfg := awstest.Config(t, url)
	c, lb := iam.NewFromConfig(cfg), elb.NewFromConfig(cfg)
	up := must(c.UploadServerCertificate(ctx, &iam.UploadServerCertificateInput{ServerCertificateName: aws.String("web"), CertificateBody: aws.String(selfSigned(t)), PrivateKey: aws.String("unused")}))
	arn := up.ServerCertificateMetadata.Arn
	l := must(lb.CreateLoadBalancer(ctx, &elb.CreateLoadBalancerInput{Name: aws.String("lb"), Subnets: []string{"subnet-1", "subnet-2"}})).LoadBalancers[0]
	tg := must(lb.CreateTargetGroup(ctx, &elb.CreateTargetGroupInput{Name: aws.String("tg"), Protocol: elbtypes.ProtocolEnumHttp, Port: aws.Int32(80), VpcId: aws.String("vpc-1"), TargetType: elbtypes.TargetTypeEnumIp})).TargetGroups[0]
	listen := func(cert *string, port int32) error {
		_, err := lb.CreateListener(ctx, &elb.CreateListenerInput{LoadBalancerArn: l.LoadBalancerArn, Protocol: elbtypes.ProtocolEnumHttps, Port: aws.Int32(port),
			Certificates:   []elbtypes.Certificate{{CertificateArn: cert}},
			DefaultActions: []elbtypes.Action{{Type: elbtypes.ActionTypeEnumForward, TargetGroupArn: tg.TargetGroupArn}}})
		return err
	}
	if err := listen(aws.String(strings.Replace(*arn, "/web", "/missing", 1)), 4431); apiCode(err) != "CertificateNotFound" {
		t.Errorf("listener with a missing certificate: %v", err)
	}
	if err := listen(arn, 4432); err != nil {
		t.Fatal(err)
	}
	if _, err := c.DeleteServerCertificate(ctx, &iam.DeleteServerCertificateInput{ServerCertificateName: aws.String("web")}); apiCode(err) != "DeleteConflict" {
		t.Errorf("delete of a certificate in use: %v", err)
	}
	if _, err := c.GetServerCertificate(ctx, &iam.GetServerCertificateInput{ServerCertificateName: aws.String("web")}); err != nil {
		t.Errorf("the certificate in use was removed: %v", err)
	}
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}
