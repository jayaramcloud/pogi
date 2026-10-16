package generic_test

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/identitystore"
	"github.com/aws/aws-sdk-go-v2/service/sso"
	ssotypes "github.com/aws/aws-sdk-go-v2/service/sso/types"
	"github.com/aws/aws-sdk-go-v2/service/ssoadmin"
	"github.com/aws/aws-sdk-go-v2/service/ssoadmin/types"
	"github.com/aws/aws-sdk-go-v2/service/ssooidc"
	oidctypes "github.com/aws/aws-sdk-go-v2/service/ssooidc/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/nrndev/pogi/internal/aws/generic"
	awsiam "github.com/nrndev/pogi/internal/aws/iam"
	awssts "github.com/nrndev/pogi/internal/aws/sts"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/core"
	gcpoauth "github.com/nrndev/pogi/internal/gcp/oauth"
)

// startWith starts generic services plus IAM and STS, for tests that mint credentials or roles.
func startWith(t *testing.T, names ...string) (*core.Hub, string) {
	t.Helper()
	builds := []func(*core.Hub) core.Service{
		func(h *core.Hub) core.Service { return awsiam.New(h) },
		func(h *core.Hub) core.Service { return awssts.New(h) },
		func(h *core.Hub) core.Service { return gcpoauth.New(h) }, // it too answers POST /token, and comes first in pogi
	}
	for _, n := range names {
		builds = append(builds, func(h *core.Hub) core.Service { return generic.New(h, n) })
	}
	return core.ServeTest(t, builds...)
}

// TestSSOSignIn drives the calls `aws sso login` and a profile with sso_session make: register, device
// authorization, polling, the portal, then the minted credentials against STS.
func TestSSOSignIn(t *testing.T) {
	_, base := startWith(t, "sso-oidc", "sso", "sso-admin", "identitystore")
	ctx := context.Background()
	cfg := awstest.Config(t, base)
	const acct = "123456789012"

	// An administrator sets up a user, a permission set and an assignment.
	admin, ids := ssoadmin.NewFromConfig(cfg), identitystore.NewFromConfig(cfg)
	in, err := admin.ListInstances(ctx, &ssoadmin.ListInstancesInput{})
	if err != nil || len(in.Instances) != 1 {
		t.Fatalf("instances: %v %+v", err, in)
	}
	inst := in.Instances[0]
	if !strings.HasPrefix(aws.ToString(inst.InstanceArn), "arn:aws:sso:::instance/ssoins-") {
		t.Fatalf("instance arn %s", aws.ToString(inst.InstanceArn))
	}
	ps, err := admin.CreatePermissionSet(ctx, &ssoadmin.CreatePermissionSetInput{InstanceArn: inst.InstanceArn, Name: aws.String("AdminAccess")})
	if err != nil {
		t.Fatal(err)
	}
	psArn := ps.PermissionSet.PermissionSetArn
	if _, err := admin.AttachManagedPolicyToPermissionSet(ctx, &ssoadmin.AttachManagedPolicyToPermissionSetInput{InstanceArn: inst.InstanceArn, PermissionSetArn: psArn,
		ManagedPolicyArn: aws.String("arn:aws:iam::aws:policy/AdministratorAccess")}); err != nil {
		t.Fatal(err)
	}
	user, err := ids.CreateUser(ctx, &identitystore.CreateUserInput{IdentityStoreId: inst.IdentityStoreId, UserName: aws.String("jdoe")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admin.CreateAccountAssignment(ctx, &ssoadmin.CreateAccountAssignmentInput{InstanceArn: inst.InstanceArn, PermissionSetArn: psArn,
		PrincipalType: types.PrincipalTypeUser, PrincipalId: user.UserId, TargetType: types.TargetTypeAwsAccount, TargetId: aws.String(acct)}); err != nil {
		t.Fatal(err)
	}
	roleName := ""
	roles, err := iam.NewFromConfig(awstest.ConfigAs(t, base, acct, "x", "")).ListRoles(ctx, &iam.ListRolesInput{PathPrefix: aws.String("/aws-reserved/sso.amazonaws.com/")})
	if err != nil || len(roles.Roles) != 1 {
		t.Fatalf("sso role: %v %+v", err, roles)
	}
	roleName = aws.ToString(roles.Roles[0].RoleName)
	if !strings.HasPrefix(roleName, "AWSReservedSSO_AdminAccess_") {
		t.Fatalf("role %s", roleName)
	}

	att, err := iam.NewFromConfig(awstest.ConfigAs(t, base, acct, "x", "")).ListAttachedRolePolicies(ctx, &iam.ListAttachedRolePoliciesInput{RoleName: aws.String(roleName)})
	if err != nil || len(att.AttachedPolicies) != 1 || aws.ToString(att.AttachedPolicies[0].PolicyName) != "AdministratorAccess" {
		t.Fatalf("role policies: %v %+v", err, att)
	}

	// The device flow.
	oidc := ssooidc.NewFromConfig(cfg)
	reg, err := oidc.RegisterClient(ctx, &ssooidc.RegisterClientInput{ClientName: aws.String("botocore-client-x"), ClientType: aws.String("public"),
		Scopes: []string{"sso:account:access"}})
	if err != nil || aws.ToString(reg.ClientId) == "" || aws.ToString(reg.ClientSecret) == "" {
		t.Fatalf("register: %v %+v", err, reg)
	}
	if _, err := oidc.StartDeviceAuthorization(ctx, &ssooidc.StartDeviceAuthorizationInput{ClientId: reg.ClientId, ClientSecret: aws.String("wrong"), StartUrl: aws.String(base + "/start")}); !errors.As(err, new(*oidctypes.InvalidClientException)) {
		t.Fatalf("wrong secret: %T %v", err, err)
	}
	dev, err := oidc.StartDeviceAuthorization(ctx, &ssooidc.StartDeviceAuthorizationInput{ClientId: reg.ClientId, ClientSecret: reg.ClientSecret, StartUrl: aws.String(base + "/start")})
	if err != nil || !strings.HasPrefix(aws.ToString(dev.VerificationUriComplete), base+"/_pogi/sso/device?user_code=") {
		t.Fatalf("device: %v %+v", err, dev)
	}
	poll := func() (*ssooidc.CreateTokenOutput, error) {
		return oidc.CreateToken(ctx, &ssooidc.CreateTokenInput{ClientId: reg.ClientId, ClientSecret: reg.ClientSecret, GrantType: aws.String("urn:ietf:params:oauth:grant-type:device_code"), DeviceCode: dev.DeviceCode})
	}
	if _, err := poll(); !errors.As(err, new(*oidctypes.AuthorizationPendingException)) {
		t.Fatalf("first poll: %T %v", err, err)
	}
	res, err := http.Get(aws.ToString(dev.VerificationUriComplete) + "&user=jdoe") // the user opens the verification URL
	if err != nil {
		t.Fatal(err)
	}
	page, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode != 200 || !strings.Contains(string(page), "jdoe") {
		t.Fatalf("verification page: %d %s", res.StatusCode, page)
	}
	tok, err := poll()
	if err != nil || aws.ToString(tok.AccessToken) == "" || aws.ToString(tok.RefreshToken) == "" || tok.ExpiresIn == 0 {
		t.Fatalf("token: %v %+v", err, tok)
	}
	if _, err := poll(); !errors.As(err, new(*oidctypes.InvalidGrantException)) {
		t.Fatalf("device code used twice: %T %v", err, err)
	}
	if _, err := oidc.CreateToken(ctx, &ssooidc.CreateTokenInput{ClientId: reg.ClientId, ClientSecret: reg.ClientSecret, GrantType: aws.String("password")}); !errors.As(err, new(*oidctypes.UnsupportedGrantTypeException)) {
		t.Fatalf("bad grant: %T %v", err, err)
	}

	// The portal: accounts, roles, credentials.
	portal := sso.NewFromConfig(cfg)
	accts, err := portal.ListAccounts(ctx, &sso.ListAccountsInput{AccessToken: tok.AccessToken})
	if err != nil || len(accts.AccountList) != 1 || aws.ToString(accts.AccountList[0].AccountId) != acct {
		t.Fatalf("accounts: %v %+v", err, accts)
	}
	rl, err := portal.ListAccountRoles(ctx, &sso.ListAccountRolesInput{AccessToken: tok.AccessToken, AccountId: aws.String(acct)})
	if err != nil || len(rl.RoleList) != 1 || aws.ToString(rl.RoleList[0].RoleName) != "AdminAccess" {
		t.Fatalf("roles: %v %+v", err, rl)
	}
	if _, err := portal.GetRoleCredentials(ctx, &sso.GetRoleCredentialsInput{AccessToken: tok.AccessToken, AccountId: aws.String(acct), RoleName: aws.String("Nope")}); !errors.As(err, new(*ssotypes.ResourceNotFoundException)) {
		t.Fatalf("unassigned role: %T %v", err, err)
	}
	if _, err := portal.GetRoleCredentials(ctx, &sso.GetRoleCredentialsInput{AccessToken: aws.String("bogus"), AccountId: aws.String(acct), RoleName: aws.String("AdminAccess")}); !errors.As(err, new(*ssotypes.UnauthorizedException)) {
		t.Fatalf("bad token: %T %v", err, err)
	}
	rc, err := portal.GetRoleCredentials(ctx, &sso.GetRoleCredentialsInput{AccessToken: tok.AccessToken, AccountId: aws.String(acct), RoleName: aws.String("AdminAccess")})
	if err != nil {
		t.Fatal(err)
	}
	c := rc.RoleCredentials
	who, err := sts.NewFromConfig(awstest.ConfigAs(t, base, aws.ToString(c.AccessKeyId), aws.ToString(c.SecretAccessKey), aws.ToString(c.SessionToken))).GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		t.Fatal(err)
	}
	if want := "arn:aws:sts::" + acct + ":assumed-role/" + roleName + "/jdoe"; aws.ToString(who.Arn) != want || aws.ToString(who.Account) != acct {
		t.Fatalf("caller %s %s, want %s", aws.ToString(who.Arn), aws.ToString(who.Account), want)
	}
	if c.Expiration < 1e12 {
		t.Fatalf("expiration %d is not in milliseconds", c.Expiration)
	}

	// Refresh.
	ref, err := oidc.CreateToken(ctx, &ssooidc.CreateTokenInput{ClientId: reg.ClientId, ClientSecret: reg.ClientSecret, GrantType: aws.String("refresh_token"), RefreshToken: tok.RefreshToken})
	if err != nil || aws.ToString(ref.AccessToken) == aws.ToString(tok.AccessToken) {
		t.Fatalf("refresh: %v %+v", err, ref)
	}
	if _, err := portal.ListAccounts(ctx, &sso.ListAccountsInput{AccessToken: ref.AccessToken}); err != nil {
		t.Fatal(err)
	}

	// The authorization-code grant with PKCE, which `aws sso login` uses with an sso_session.
	verifier := strings.Repeat("v", 64)
	sum := sha256.Sum256([]byte(verifier))
	challenge := base64.RawURLEncoding.EncodeToString(sum[:])
	redirect := "http://127.0.0.1:8123/oauth/callback"
	authz := base + "/authorize?" + url.Values{"response_type": {"code"}, "client_id": {aws.ToString(reg.ClientId)}, "redirect_uri": {redirect}, "state": {"s1"},
		"code_challenge": {challenge}, "code_challenge_method": {"S256"}}.Encode()
	noFollow := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	res, err = noFollow.Get(authz)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	loc, _ := url.Parse(res.Header.Get("Location"))
	if res.StatusCode != 302 || !strings.HasPrefix(loc.String(), redirect) || loc.Query().Get("state") != "s1" || loc.Query().Get("code") == "" {
		t.Fatalf("authorize: %d %s", res.StatusCode, res.Header.Get("Location"))
	}
	exchange := func(verifier string) error {
		_, err := oidc.CreateToken(ctx, &ssooidc.CreateTokenInput{ClientId: reg.ClientId, ClientSecret: reg.ClientSecret, GrantType: aws.String("authorization_code"),
			Code: aws.String(loc.Query().Get("code")), RedirectUri: aws.String(redirect), CodeVerifier: aws.String(verifier)})
		return err
	}
	if err := exchange("wrong"); !errors.As(err, new(*oidctypes.InvalidGrantException)) {
		t.Fatalf("wrong verifier: %T %v", err, err)
	}
	if err := exchange(verifier); err != nil { // a failed attempt spends nothing
		t.Fatalf("code exchange: %v", err)
	}

	// The code only goes to a registered redirect: loopback with any port, or an exact match.
	reg2, err := oidc.RegisterClient(ctx, &ssooidc.RegisterClientInput{ClientName: aws.String("c2"), ClientType: aws.String("public"),
		RedirectUris: []string{"http://127.0.0.1/oauth/callback", "https://app.example.com/cb"}})
	if err != nil {
		t.Fatal(err)
	}
	status := func(client *string, redirect string) int {
		res, err := noFollow.Get(base + "/authorize?" + url.Values{"response_type": {"code"}, "client_id": {aws.ToString(client)}, "redirect_uri": {redirect}}.Encode())
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		return res.StatusCode
	}
	for redirect, want := range map[string]int{
		"http://127.0.0.1:5555/oauth/callback": 302, "https://app.example.com/cb": 302,
		"https://evil.example.com/cb": 400, "https://app.example.com/other": 400, "http://127.0.0.1:5555/other": 400,
	} {
		if got := status(reg2.ClientId, redirect); got != want {
			t.Errorf("registered client, %s: %d, want %d", redirect, got, want)
		}
	}
	// No registered URIs: loopback only.
	if got := status(reg.ClientId, "https://evil.example.com/cb"); got != 400 {
		t.Errorf("unregistered client, remote redirect: %d, want 400", got)
	}

	// Logout ends the session.
	if _, err := portal.Logout(ctx, &sso.LogoutInput{AccessToken: tok.AccessToken}); err != nil {
		t.Fatal(err)
	}
	if _, err := portal.ListAccounts(ctx, &sso.ListAccountsInput{AccessToken: tok.AccessToken}); !errors.As(err, new(*ssotypes.UnauthorizedException)) {
		t.Fatalf("after logout: %T %v", err, err)
	}

	// Removing the assignment removes the role.
	if _, err := admin.DeleteAccountAssignment(ctx, &ssoadmin.DeleteAccountAssignmentInput{InstanceArn: inst.InstanceArn, PermissionSetArn: psArn,
		PrincipalType: types.PrincipalTypeUser, PrincipalId: user.UserId, TargetType: types.TargetTypeAwsAccount, TargetId: aws.String(acct)}); err != nil {
		t.Fatal(err)
	}
	roles, _ = iam.NewFromConfig(awstest.ConfigAs(t, base, acct, "x", "")).ListRoles(ctx, &iam.ListRolesInput{PathPrefix: aws.String("/aws-reserved/sso.amazonaws.com/")})
	if len(roles.Roles) != 0 {
		t.Fatalf("role left behind: %+v", roles.Roles)
	}
}
