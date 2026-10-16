package verifiedpermissions_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	vp "github.com/aws/aws-sdk-go-v2/service/verifiedpermissions"
	"github.com/aws/aws-sdk-go-v2/service/verifiedpermissions/types"
	"github.com/aws/smithy-go"
	cognitosvc "github.com/nrndev/pogi/internal/aws/cognito"
	"github.com/nrndev/pogi/internal/aws/verifiedpermissions"
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

const photoSchema = `{"PhotoApp": {
 "entityTypes": {
  "User": {"memberOfTypes": ["UserGroup"]},
  "UserGroup": {},
  "Photo": {"shape": {"type": "Record", "attributes": {
   "owner": {"type": "Entity", "name": "User"},
   "private": {"type": "Boolean"}}}}},
 "actions": {
  "ReadActions": {},
  "ViewPhoto": {"memberOf": [{"id": "ReadActions"}], "appliesTo": {"principalTypes": ["User"], "resourceTypes": ["Photo"]}},
  "DeletePhoto": {"appliesTo": {"principalTypes": ["User"], "resourceTypes": ["Photo"]}}}}}`

func user(id string) *types.EntityIdentifier {
	return &types.EntityIdentifier{EntityType: aws.String("PhotoApp::User"), EntityId: aws.String(id)}
}

func photo(id string) *types.EntityIdentifier {
	return &types.EntityIdentifier{EntityType: aws.String("PhotoApp::Photo"), EntityId: aws.String(id)}
}

func action(id string) *types.ActionIdentifier {
	return &types.ActionIdentifier{ActionType: aws.String("PhotoApp::Action"), ActionId: aws.String(id)}
}

// entities: alice owns a public and a private photo; bob is alice's friend.
var entities = &types.EntitiesDefinitionMemberEntityList{Value: []types.EntityItem{
	{Identifier: user("bob"), Parents: []types.EntityIdentifier{{EntityType: aws.String("PhotoApp::UserGroup"), EntityId: aws.String("friends")}}},
	{Identifier: photo("beach"), Attributes: map[string]types.AttributeValue{
		"owner": &types.AttributeValueMemberEntityIdentifier{Value: *user("alice")}, "private": &types.AttributeValueMemberBoolean{Value: false}}},
	{Identifier: photo("diary"), Attributes: map[string]types.AttributeValue{
		"owner": &types.AttributeValueMemberEntityIdentifier{Value: *user("alice")}, "private": &types.AttributeValueMemberBoolean{Value: true}}},
}}

func TestCedarPolicies(t *testing.T) {
	h, url := core.ServeTest(t,
		func(h *core.Hub) core.Service { return verifiedpermissions.New(h) },
		func(h *core.Hub) core.Service { return cognitosvc.New(h) })
	c := vp.NewFromConfig(awstest.Config(t, url))

	store := must(c.CreatePolicyStore(ctx, &vp.CreatePolicyStoreInput{ValidationSettings: &types.ValidationSettings{Mode: types.ValidationModeStrict},
		Description: aws.String("photos"), Tags: map[string]string{"app": "photos"}}))
	sid := store.PolicyStoreId
	if !strings.HasPrefix(*store.Arn, "arn:aws:verifiedpermissions::000000000000:policy-store/") {
		t.Fatalf("arn = %s", *store.Arn)
	}
	if got := must(c.GetPolicyStore(ctx, &vp.GetPolicyStoreInput{PolicyStoreId: sid})); got.ValidationSettings.Mode != types.ValidationModeStrict || *got.Description != "photos" {
		t.Fatalf("store = %+v", got)
	}
	sch := must(c.PutSchema(ctx, &vp.PutSchemaInput{PolicyStoreId: sid, Definition: &types.SchemaDefinitionMemberCedarJson{Value: photoSchema}}))
	if !slices.Equal(sch.Namespaces, []string{"PhotoApp"}) {
		t.Fatalf("namespaces = %v", sch.Namespaces)
	}
	if got := must(c.GetSchema(ctx, &vp.GetSchemaInput{PolicyStoreId: sid})); *got.Schema != photoSchema {
		t.Fatalf("schema = %s", *got.Schema)
	}

	static := func(name, stmt string) string {
		out := must(c.CreatePolicy(ctx, &vp.CreatePolicyInput{PolicyStoreId: sid, Name: aws.String(name),
			Definition: &types.PolicyDefinitionMemberStatic{Value: types.StaticPolicyDefinition{Statement: aws.String(stmt)}}}))
		return *out.PolicyId
	}
	owner := static("name/owner", `permit (principal, action == PhotoApp::Action::"ViewPhoto", resource) when { resource.owner == principal };`)
	friends := static("name/friends", `permit (principal in PhotoApp::UserGroup::"friends", action == PhotoApp::Action::"ViewPhoto", resource);`)
	private := static("name/private", `forbid (principal, action == PhotoApp::Action::"ViewPhoto", resource) when { resource.private } unless { resource.owner == principal };`)

	got := must(c.GetPolicy(ctx, &vp.GetPolicyInput{PolicyStoreId: sid, PolicyId: aws.String("name/friends")}))
	if *got.PolicyId != friends || got.Effect != types.PolicyEffectPermit || *got.Principal.EntityId != "friends" || *got.Actions[0].ActionId != "ViewPhoto" {
		t.Fatalf("policy = %+v", got)
	}
	if def := got.Definition.(*types.PolicyDefinitionDetailMemberStatic).Value; !strings.HasPrefix(*def.Statement, "permit (principal in") {
		t.Fatalf("definition = %+v", def)
	}

	authz := func(principal, act, resource string) (types.Decision, []string) {
		out := must(c.IsAuthorized(ctx, &vp.IsAuthorizedInput{PolicyStoreId: sid, Principal: user(principal), Action: action(act),
			Resource: photo(resource), Entities: entities}))
		var ids []string
		for _, p := range out.DeterminingPolicies {
			ids = append(ids, *p.PolicyId)
		}
		return out.Decision, ids
	}
	for _, tc := range []struct {
		principal, resource string
		want                types.Decision
		policies            []string
	}{
		{"alice", "diary", types.DecisionAllow, []string{owner}}, // the owner sees her private photo
		{"bob", "beach", types.DecisionAllow, []string{friends}}, // a friend sees a public photo
		{"bob", "diary", types.DecisionDeny, []string{private}},  // but not a private one
		{"carol", "beach", types.DecisionDeny, nil},              // nobody else: default deny
	} {
		if d, ids := authz(tc.principal, "ViewPhoto", tc.resource); d != tc.want || !slices.Equal(ids, tc.policies) {
			t.Errorf("%s views %s: %s %v, want %s %v", tc.principal, tc.resource, d, ids, tc.want, tc.policies)
		}
	}

	// a template linked for carol, through the ReadActions action group of the schema
	tpl := must(c.CreatePolicyTemplate(ctx, &vp.CreatePolicyTemplateInput{PolicyStoreId: sid, Description: aws.String("share"),
		Statement: aws.String(`permit (principal == ?principal, action in PhotoApp::Action::"ReadActions", resource == ?resource);`)})).PolicyTemplateId
	linked := must(c.CreatePolicy(ctx, &vp.CreatePolicyInput{PolicyStoreId: sid, Definition: &types.PolicyDefinitionMemberTemplateLinked{
		Value: types.TemplateLinkedPolicyDefinition{PolicyTemplateId: tpl, Principal: user("carol"), Resource: photo("beach")}}}))
	if linked.PolicyType != types.PolicyTypeTemplateLinked || *linked.Principal.EntityId != "carol" {
		t.Fatalf("linked = %+v", linked)
	}
	if d, ids := authz("carol", "ViewPhoto", "beach"); d != types.DecisionAllow || !slices.Equal(ids, []string{*linked.PolicyId}) {
		t.Fatalf("carol with the template: %s %v", d, ids)
	}
	list := must(c.ListPolicies(ctx, &vp.ListPoliciesInput{PolicyStoreId: sid, Filter: &types.PolicyFilter{
		Principal: &types.EntityReferenceMemberIdentifier{Value: *user("carol")}}})).Policies
	if len(list) != 1 || *list[0].PolicyId != *linked.PolicyId {
		t.Fatalf("carol's policies = %+v", list)
	}

	// a batch
	batch := must(c.BatchIsAuthorized(ctx, &vp.BatchIsAuthorizedInput{PolicyStoreId: sid, Entities: entities, Requests: []types.BatchIsAuthorizedInputItem{
		{Principal: user("alice"), Action: action("ViewPhoto"), Resource: photo("beach")},
		{Principal: user("bob"), Action: action("DeletePhoto"), Resource: photo("beach")}}}))
	if len(batch.Results) != 2 || batch.Results[0].Decision != types.DecisionAllow || batch.Results[1].Decision != types.DecisionDeny ||
		*batch.Results[1].Request.Principal.EntityId != "bob" {
		t.Fatalf("batch = %+v", batch.Results)
	}

	// errors: STRICT validation, Cedar syntax, names, unknown stores
	if _, err := c.CreatePolicy(ctx, &vp.CreatePolicyInput{PolicyStoreId: sid, Definition: &types.PolicyDefinitionMemberStatic{
		Value: types.StaticPolicyDefinition{Statement: aws.String(`permit (principal, action == PhotoApp::Action::"ViewPhoto", resource) when { resource.size > 3 };`)}}}); code(err) != "ValidationException" {
		t.Fatalf("schema validation: %v", err)
	}
	if _, err := c.CreatePolicy(ctx, &vp.CreatePolicyInput{PolicyStoreId: sid, Definition: &types.PolicyDefinitionMemberStatic{
		Value: types.StaticPolicyDefinition{Statement: aws.String(`permit (principal`)}}}); code(err) != "ValidationException" {
		t.Fatalf("syntax: %v", err)
	}
	if _, err := c.CreatePolicy(ctx, &vp.CreatePolicyInput{PolicyStoreId: sid, Name: aws.String("name/owner"), Definition: &types.PolicyDefinitionMemberStatic{
		Value: types.StaticPolicyDefinition{Statement: aws.String(`permit (principal, action, resource);`)}}}); code(err) != "ConflictException" {
		t.Fatalf("duplicate name: %v", err)
	}
	if _, err := c.GetPolicyStore(ctx, &vp.GetPolicyStoreInput{PolicyStoreId: aws.String("nope")}); code(err) != "ResourceNotFoundException" {
		t.Fatalf("unknown store: %v", err)
	}

	// updates keep the effect, principal and resource
	must(c.UpdatePolicy(ctx, &vp.UpdatePolicyInput{PolicyStoreId: sid, PolicyId: &friends, Definition: &types.UpdatePolicyDefinitionMemberStatic{
		Value: types.UpdateStaticPolicyDefinition{Statement: aws.String(`permit (principal in PhotoApp::UserGroup::"friends", action in [PhotoApp::Action::"ViewPhoto", PhotoApp::Action::"DeletePhoto"], resource);`)}}}))
	if d, ids := authz("bob", "DeletePhoto", "beach"); d != types.DecisionAllow || !slices.Equal(ids, []string{friends}) {
		t.Fatalf("after update: %s %v", d, ids)
	}
	if _, err := c.UpdatePolicy(ctx, &vp.UpdatePolicyInput{PolicyStoreId: sid, PolicyId: &friends, Definition: &types.UpdatePolicyDefinitionMemberStatic{
		Value: types.UpdateStaticPolicyDefinition{Statement: aws.String(`forbid (principal in PhotoApp::UserGroup::"friends", action, resource);`)}}}); code(err) != "ValidationException" {
		t.Fatalf("effect change: %v", err)
	}

	// deleting the template deletes its linked policy
	must(c.DeletePolicyTemplate(ctx, &vp.DeletePolicyTemplateInput{PolicyStoreId: sid, PolicyTemplateId: tpl}))
	if d, _ := authz("carol", "ViewPhoto", "beach"); d != types.DecisionDeny {
		t.Fatal("linked policy survived its template")
	}

	// tags
	must(c.TagResource(ctx, &vp.TagResourceInput{ResourceArn: store.Arn, Tags: map[string]string{"team": "web"}}))
	if tags := must(c.ListTagsForResource(ctx, &vp.ListTagsForResourceInput{ResourceArn: store.Arn})).Tags; len(tags) != 2 || tags["team"] != "web" {
		t.Fatalf("tags = %v", tags)
	}

	t.Run("tokens", func(t *testing.T) { tokens(t, h, c, sid) })
}

// tokens checks IsAuthorizedWithToken with a user of pogi's Cognito who is
// in the "friends" group.
func tokens(t *testing.T, h *core.Hub, c *vp.Client, sid *string) {
	call := func(op string, in, out any) {
		if err := h.Call("000000000000", "us-east-1", "AWSCognitoIdentityProviderService", op, in, out); err != nil {
			t.Fatalf("%s: %v", op, err)
		}
	}
	var pool struct{ UserPool struct{ Id, Arn string } }
	call("CreateUserPool", map[string]any{"PoolName": "photos"}, &pool)
	pid := pool.UserPool.Id
	var client struct{ UserPoolClient struct{ ClientId string } }
	call("CreateUserPoolClient", map[string]any{"UserPoolId": pid, "ClientName": "web", "ExplicitAuthFlows": []string{"ALLOW_USER_PASSWORD_AUTH"}}, &client)
	call("AdminCreateUser", map[string]any{"UserPoolId": pid, "Username": "dave", "MessageAction": "SUPPRESS"}, nil)
	call("AdminSetUserPassword", map[string]any{"UserPoolId": pid, "Username": "dave", "Password": "Passw0rd!", "Permanent": true}, nil)
	call("CreateGroup", map[string]any{"UserPoolId": pid, "GroupName": "friends"}, nil)
	call("AdminAddUserToGroup", map[string]any{"UserPoolId": pid, "Username": "dave", "GroupName": "friends"}, nil)
	var auth struct {
		AuthenticationResult struct{ IdToken, AccessToken string }
	}
	call("InitiateAuth", map[string]any{"AuthFlow": "USER_PASSWORD_AUTH", "ClientId": client.UserPoolClient.ClientId,
		"AuthParameters": map[string]string{"USERNAME": "dave", "PASSWORD": "Passw0rd!"}}, &auth)

	must(c.CreateIdentitySource(ctx, &vp.CreateIdentitySourceInput{PolicyStoreId: sid, PrincipalEntityType: aws.String("PhotoApp::User"),
		Configuration: &types.ConfigurationMemberCognitoUserPoolConfiguration{Value: types.CognitoUserPoolConfiguration{
			UserPoolArn: aws.String(pool.UserPool.Arn), ClientIds: []string{client.UserPoolClient.ClientId},
			GroupConfiguration: &types.CognitoGroupConfiguration{GroupEntityType: aws.String("PhotoApp::UserGroup")}}}}))
	del := must(c.CreatePolicy(ctx, &vp.CreatePolicyInput{PolicyStoreId: sid, Definition: &types.PolicyDefinitionMemberStatic{Value: types.StaticPolicyDefinition{
		Statement: aws.String(`permit (principal in PhotoApp::UserGroup::"` + pid + `|friends", action == PhotoApp::Action::"DeletePhoto", resource);`)}}})).PolicyId

	out := must(c.IsAuthorizedWithToken(ctx, &vp.IsAuthorizedWithTokenInput{PolicyStoreId: sid, IdentityToken: aws.String(auth.AuthenticationResult.IdToken),
		Action: action("DeletePhoto"), Resource: photo("beach"), Entities: entities}))
	if out.Decision != types.DecisionAllow || *out.DeterminingPolicies[0].PolicyId != *del ||
		*out.Principal.EntityType != "PhotoApp::User" || !strings.HasPrefix(*out.Principal.EntityId, pid+"|") {
		t.Fatalf("with ID token: %+v %+v", out.Decision, out.Principal)
	}
	batch := must(c.BatchIsAuthorizedWithToken(ctx, &vp.BatchIsAuthorizedWithTokenInput{PolicyStoreId: sid, AccessToken: aws.String(auth.AuthenticationResult.AccessToken),
		Entities: entities, Requests: []types.BatchIsAuthorizedWithTokenInputItem{
			{Action: action("DeletePhoto"), Resource: photo("beach")}, {Action: action("ViewPhoto"), Resource: photo("diary")}}}))
	if batch.Results[0].Decision != types.DecisionAllow || batch.Results[1].Decision != types.DecisionDeny || *batch.Principal.EntityId != *out.Principal.EntityId {
		t.Fatalf("with access token: %+v", batch.Results)
	}
	forged := auth.AuthenticationResult.IdToken[:len(auth.AuthenticationResult.IdToken)-4] + "AAAA"
	if _, err := c.IsAuthorizedWithToken(ctx, &vp.IsAuthorizedWithTokenInput{PolicyStoreId: sid, IdentityToken: &forged,
		Action: action("DeletePhoto"), Resource: photo("beach")}); code(err) != "ValidationException" {
		t.Fatalf("forged token: %v", err)
	}
}
