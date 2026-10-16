package cognitoidentity_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"slices"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ci "github.com/aws/aws-sdk-go-v2/service/cognitoidentity"
	"github.com/aws/aws-sdk-go-v2/service/cognitoidentity/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/smithy-go"
	"github.com/nrndev/pogi/internal/aws/cognito"
	"github.com/nrndev/pogi/internal/aws/cognitoidentity"
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

func wantErr(t *testing.T, err error, code string) {
	t.Helper()
	var ae smithy.APIError
	if !errors.As(err, &ae) || ae.ErrorCode() != code {
		t.Fatalf("want %s, got %v", code, err)
	}
}

// idp calls a Cognito user pools operation (its SDK is not a dependency).
func idp(t *testing.T, url, op string, in any) map[string]any {
	t.Helper()
	b, _ := json.Marshal(in)
	req, _ := http.NewRequest(http.MethodPost, url+"/", bytes.NewReader(b))
	req.Header.Set("X-Amz-Target", "AWSCognitoIdentityProviderService."+op)
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	awstest.Sign(t, req, "cognito-idp")
	res := must(http.DefaultClient.Do(req))
	defer res.Body.Close()
	var out map[string]any
	json.NewDecoder(res.Body).Decode(&out)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("%s: %v", op, out)
	}
	return out
}

// callerArn is the identity STS sees for credentials.
func callerArn(t *testing.T, url string, cr *types.Credentials) string {
	t.Helper()
	c := sts.NewFromConfig(awstest.ConfigAs(t, url, *cr.AccessKeyId, *cr.SecretKey, *cr.SessionToken))
	return *must(c.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})).Arn
}

func TestIdentityPools(t *testing.T) {
	_, url := core.ServeTest(t,
		func(h *core.Hub) core.Service { return cognito.New(h) },
		func(h *core.Hub) core.Service { return stssvc.New(h) },
		func(h *core.Hub) core.Service { return cognitoidentity.New(h) })
	c := ci.NewFromConfig(awstest.Config(t, url))

	// a user pool user signs in with USER_PASSWORD_AUTH
	userPool := idp(t, url, "CreateUserPool", map[string]any{"PoolName": "users"})["UserPool"].(map[string]any)["Id"].(string)
	client := idp(t, url, "CreateUserPoolClient", map[string]any{"UserPoolId": userPool, "ClientName": "app",
		"ExplicitAuthFlows": []string{"ALLOW_USER_PASSWORD_AUTH"}})["UserPoolClient"].(map[string]any)["ClientId"].(string)
	idp(t, url, "AdminCreateUser", map[string]any{"UserPoolId": userPool, "Username": "jane",
		"UserAttributes": []map[string]string{{"Name": "email", "Value": "jane@example.com"}}})
	idp(t, url, "AdminSetUserPassword", map[string]any{"UserPoolId": userPool, "Username": "jane", "Password": "Passw0rd!", "Permanent": true})
	auth := idp(t, url, "InitiateAuth", map[string]any{"AuthFlow": "USER_PASSWORD_AUTH", "ClientId": client,
		"AuthParameters": map[string]string{"USERNAME": "jane", "PASSWORD": "Passw0rd!"}})
	idToken := auth["AuthenticationResult"].(map[string]any)["IdToken"].(string)

	provider := "cognito-idp.us-east-1.amazonaws.com/" + userPool
	pool := must(c.CreateIdentityPool(ctx, &ci.CreateIdentityPoolInput{IdentityPoolName: aws.String("app pool"), AllowUnauthenticatedIdentities: true,
		CognitoIdentityProviders: []types.CognitoIdentityProvider{{ProviderName: aws.String(provider), ClientId: aws.String(client)}},
		DeveloperProviderName:    aws.String("login.example.app"), IdentityPoolTags: map[string]string{"team": "a"}}))
	poolID := *pool.IdentityPoolId
	must(c.SetIdentityPoolRoles(ctx, &ci.SetIdentityPoolRolesInput{IdentityPoolId: aws.String(poolID), Roles: map[string]string{
		"authenticated": "arn:aws:iam::000000000000:role/signed-in", "unauthenticated": "arn:aws:iam::000000000000:role/guest"}}))

	// GetId with the ID token: one identity per user, credentials of the authenticated role
	logins := map[string]string{provider: idToken}
	id := *must(c.GetId(ctx, &ci.GetIdInput{IdentityPoolId: aws.String(poolID), Logins: logins})).IdentityId
	if again := *must(c.GetId(ctx, &ci.GetIdInput{IdentityPoolId: aws.String(poolID), Logins: logins})).IdentityId; again != id {
		t.Fatalf("identity changed: %s then %s", id, again)
	}
	cr := must(c.GetCredentialsForIdentity(ctx, &ci.GetCredentialsForIdentityInput{IdentityId: aws.String(id), Logins: logins}))
	if arn := callerArn(t, url, cr.Credentials); arn != "arn:aws:sts::000000000000:assumed-role/signed-in/CognitoIdentityCredentials" || cr.Credentials.Expiration == nil {
		t.Fatalf("authenticated caller = %s", arn)
	}
	_, err := c.GetCredentialsForIdentity(ctx, &ci.GetCredentialsForIdentityInput{IdentityId: aws.String(id)})
	wantErr(t, err, "NotAuthorizedException") // an authenticated identity needs its login
	_, err = c.GetId(ctx, &ci.GetIdInput{IdentityPoolId: aws.String(poolID), Logins: map[string]string{provider: idToken[:len(idToken)-4] + "AAAA"}})
	wantErr(t, err, "NotAuthorizedException")
	desc := must(c.DescribeIdentity(ctx, &ci.DescribeIdentityInput{IdentityId: aws.String(id)}))
	if !slices.Equal(desc.Logins, []string{provider}) {
		t.Fatalf("logins = %v", desc.Logins)
	}

	// a rule on the token's claims picks another role
	must(c.SetIdentityPoolRoles(ctx, &ci.SetIdentityPoolRolesInput{IdentityPoolId: aws.String(poolID),
		Roles: map[string]string{"authenticated": "arn:aws:iam::000000000000:role/signed-in"},
		RoleMappings: map[string]types.RoleMapping{provider + ":" + client: {Type: types.RoleMappingTypeRules, AmbiguousRoleResolution: types.AmbiguousRoleResolutionTypeAuthenticatedRole,
			RulesConfiguration: &types.RulesConfigurationType{Rules: []types.MappingRule{{Claim: aws.String("email"), MatchType: types.MappingRuleMatchTypeContains,
				Value: aws.String("@example.com"), RoleARN: aws.String("arn:aws:iam::000000000000:role/staff")}}}}}}))
	cr = must(c.GetCredentialsForIdentity(ctx, &ci.GetCredentialsForIdentityInput{IdentityId: aws.String(id), Logins: logins}))
	if arn := callerArn(t, url, cr.Credentials); arn != "arn:aws:sts::000000000000:assumed-role/staff/CognitoIdentityCredentials" {
		t.Fatalf("rule-mapped caller = %s", arn)
	}
	roles := must(c.GetIdentityPoolRoles(ctx, &ci.GetIdentityPoolRolesInput{IdentityPoolId: aws.String(poolID)}))
	if len(roles.RoleMappings) != 1 || roles.Roles["unauthenticated"] != "" {
		t.Fatalf("roles = %+v", roles)
	}

	// unauthenticated identities: only while the pool allows them, and they need a role
	guest := *must(c.GetId(ctx, &ci.GetIdInput{IdentityPoolId: aws.String(poolID)})).IdentityId
	_, err = c.GetCredentialsForIdentity(ctx, &ci.GetCredentialsForIdentityInput{IdentityId: aws.String(guest)})
	wantErr(t, err, "InvalidIdentityPoolConfigurationException")
	must(c.SetIdentityPoolRoles(ctx, &ci.SetIdentityPoolRolesInput{IdentityPoolId: aws.String(poolID), Roles: map[string]string{
		"authenticated": "arn:aws:iam::000000000000:role/signed-in", "unauthenticated": "arn:aws:iam::000000000000:role/guest"}}))
	cr = must(c.GetCredentialsForIdentity(ctx, &ci.GetCredentialsForIdentityInput{IdentityId: aws.String(guest)}))
	if arn := callerArn(t, url, cr.Credentials); arn != "arn:aws:sts::000000000000:assumed-role/guest/CognitoIdentityCredentials" {
		t.Fatalf("guest caller = %s", arn)
	}
	upd := must(c.UpdateIdentityPool(ctx, &ci.UpdateIdentityPoolInput{IdentityPoolId: aws.String(poolID), IdentityPoolName: aws.String("app pool"),
		AllowUnauthenticatedIdentities: false, CognitoIdentityProviders: pool.CognitoIdentityProviders, DeveloperProviderName: pool.DeveloperProviderName}))
	if upd.IdentityPoolTags["team"] != "a" {
		t.Fatalf("tags lost on update: %v", upd.IdentityPoolTags)
	}
	_, err = c.GetId(ctx, &ci.GetIdInput{IdentityPoolId: aws.String(poolID)})
	wantErr(t, err, "NotAuthorizedException")
	_, err = c.GetCredentialsForIdentity(ctx, &ci.GetCredentialsForIdentityInput{IdentityId: aws.String(guest)})
	wantErr(t, err, "NotAuthorizedException")

	// OpenID tokens: STS takes them for AssumeRoleWithWebIdentity, the developer flow signs in with them
	tok := must(c.GetOpenIdToken(ctx, &ci.GetOpenIdTokenInput{IdentityId: aws.String(id), Logins: logins}))
	web := must(sts.NewFromConfig(awstest.Config(t, url)).AssumeRoleWithWebIdentity(ctx, &sts.AssumeRoleWithWebIdentityInput{
		RoleArn: aws.String("arn:aws:iam::000000000000:role/web"), RoleSessionName: aws.String("app"), WebIdentityToken: tok.Token}))
	if *web.AssumedRoleUser.Arn != "arn:aws:sts::000000000000:assumed-role/web/app" {
		t.Fatalf("web identity = %s", *web.AssumedRoleUser.Arn)
	}
	res := must(http.Get(url + "/_aws/cognito-identity/.well-known/jwks_uri"))
	var keys struct{ Keys []struct{ Kid string } }
	json.NewDecoder(res.Body).Decode(&keys)
	res.Body.Close()
	if len(keys.Keys) != 1 || keys.Keys[0].Kid == "" {
		t.Fatalf("jwks = %+v", keys)
	}
	dev := must(c.GetOpenIdTokenForDeveloperIdentity(ctx, &ci.GetOpenIdTokenForDeveloperIdentityInput{IdentityPoolId: aws.String(poolID),
		Logins: map[string]string{"login.example.app": "user-42"}}))
	found := must(c.LookupDeveloperIdentity(ctx, &ci.LookupDeveloperIdentityInput{IdentityPoolId: aws.String(poolID), DeveloperUserIdentifier: aws.String("user-42")}))
	if *found.IdentityId != *dev.IdentityId || !slices.Equal(found.DeveloperUserIdentifierList, []string{"user-42"}) {
		t.Fatalf("lookup = %+v", found)
	}
	cr = must(c.GetCredentialsForIdentity(ctx, &ci.GetCredentialsForIdentityInput{IdentityId: dev.IdentityId,
		Logins: map[string]string{"cognito-identity.amazonaws.com": *dev.Token}}))
	if arn := callerArn(t, url, cr.Credentials); arn != "arn:aws:sts::000000000000:assumed-role/signed-in/CognitoIdentityCredentials" {
		t.Fatalf("developer caller = %s", arn)
	}

	// listings, tags, deletion
	list := must(c.ListIdentities(ctx, &ci.ListIdentitiesInput{IdentityPoolId: aws.String(poolID), MaxResults: aws.Int32(60)}))
	if len(list.Identities) != 3 {
		t.Fatalf("identities = %d", len(list.Identities))
	}
	arn := "arn:aws:cognito-identity:us-east-1:000000000000:identitypool/" + poolID
	must(c.TagResource(ctx, &ci.TagResourceInput{ResourceArn: aws.String(arn), Tags: map[string]string{"env": "dev"}}))
	must(c.UntagResource(ctx, &ci.UntagResourceInput{ResourceArn: aws.String(arn), TagKeys: []string{"team"}}))
	if tags := must(c.ListTagsForResource(ctx, &ci.ListTagsForResourceInput{ResourceArn: aws.String(arn)})).Tags; len(tags) != 1 || tags["env"] != "dev" {
		t.Fatalf("tags = %v", tags)
	}
	must(c.DeleteIdentities(ctx, &ci.DeleteIdentitiesInput{IdentityIdsToDelete: []string{guest}}))
	_, err = c.DescribeIdentity(ctx, &ci.DescribeIdentityInput{IdentityId: aws.String(guest)})
	wantErr(t, err, "ResourceNotFoundException")
	pools := must(c.ListIdentityPools(ctx, &ci.ListIdentityPoolsInput{MaxResults: aws.Int32(10)}))
	if len(pools.IdentityPools) != 1 || *pools.IdentityPools[0].IdentityPoolName != "app pool" {
		t.Fatalf("pools = %+v", pools.IdentityPools)
	}
	must(c.DeleteIdentityPool(ctx, &ci.DeleteIdentityPoolInput{IdentityPoolId: aws.String(poolID)}))
	_, err = c.DescribeIdentityPool(ctx, &ci.DescribeIdentityPoolInput{IdentityPoolId: aws.String(poolID)})
	wantErr(t, err, "ResourceNotFoundException")
	_, err = c.CreateIdentityPool(ctx, &ci.CreateIdentityPoolInput{IdentityPoolName: aws.String("bad/name")})
	wantErr(t, err, "InvalidParameterException")
}
