package graph_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/cloud"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/azure/arm"
)

var ctx = context.Background()

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

type env struct {
	base string
	http *http.Client
	co   azcore.ClientOptions
}

func start(t *testing.T) *env {
	t.Helper()
	srv := pogi.Start(t, pogi.Config{Services: []string{"arm", "entra", "graph"}})
	base := "https://localhost:" + srv.URL[strings.LastIndexByte(srv.URL, ':')+1:]
	ca, _, _ := srv.Hub.CertPaths()
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(must(os.ReadFile(ca)))
	hc := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, ServerName: "localhost"}}}
	return &env{base: base, http: hc, co: azcore.ClientOptions{Transport: hc, Cloud: cloud.Configuration{ActiveDirectoryAuthorityHost: base + "/"}}}
}

type obj = map[string]any

// call sends a Graph request with a bearer token and decodes the answer.
func (e *env) call(t *testing.T, method, path string, body any) (int, obj) {
	t.Helper()
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(must(json.Marshal(body)))
	}
	if strings.HasPrefix(path, "/") {
		path = e.base + path
	}
	req, _ := http.NewRequest(method, path, rd)
	req.Header.Set("Authorization", "Bearer x")
	req.Header.Set("Content-Type", "application/json")
	res, err := e.http.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	var out obj
	json.NewDecoder(res.Body).Decode(&out)
	return res.StatusCode, out
}

func (e *env) ok(t *testing.T, want int, method, path string, body any) obj {
	t.Helper()
	st, out := e.call(t, method, path, body)
	if st != want {
		t.Fatalf("%s %s: %d %v", method, path, st, out)
	}
	return out
}

func values(o obj) []obj {
	var out []obj
	for _, v := range o["value"].([]any) {
		out = append(out, v.(obj))
	}
	return out
}

func (e *env) credential(app, secret string) azcore.TokenCredential {
	return must(azidentity.NewClientSecretCredential(arm.TenantID, app, secret, &azidentity.ClientSecretCredentialOptions{ClientOptions: e.co, DisableInstanceDiscovery: true}))
}

func TestAppRegistrationSignsIn(t *testing.T) {
	e := start(t)
	app := e.ok(t, 201, "POST", "/v1.0/applications", obj{"displayName": "ci"})
	appID, appObj := app["appId"].(string), app["id"].(string)
	pw := e.ok(t, 200, "POST", "/v1.0/applications/"+appObj+"/addPassword", obj{"passwordCredential": obj{"displayName": "ci"}})
	secret := pw["secretText"].(string)
	if secret == "" || pw["hint"] != secret[:3] || pw["keyId"] == "" {
		t.Fatalf("%v", pw)
	}
	sp := e.ok(t, 201, "POST", "/v1.0/servicePrincipals", obj{"appId": appID})
	if sp["displayName"] != "ci" || sp["servicePrincipalType"] != "Application" {
		t.Fatalf("%v", sp)
	}
	e.ok(t, 400, "POST", "/v1.0/servicePrincipals", obj{"appId": appID}) // one per app

	// the application keeps the hint of the secret, never the secret
	got := e.ok(t, 200, "GET", "/v1.0/applications(appId='"+appID+"')", nil)
	creds := got["passwordCredentials"].([]any)
	if len(creds) != 1 || creds[0].(obj)["secretText"] != nil || creds[0].(obj)["hint"] != secret[:3] {
		t.Fatalf("%v", got)
	}

	// the secret signs in, as the service principal
	tok, err := e.credential(appID, secret).GetToken(ctx, policy.TokenRequestOptions{Scopes: []string{"https://graph.microsoft.com/.default"}})
	if err != nil {
		t.Fatal(err)
	}
	var claims obj
	json.Unmarshal(must(base64.RawURLEncoding.DecodeString(strings.Split(tok.Token, ".")[1])), &claims)
	if claims["oid"] != sp["id"] || claims["appid"] != appID || claims["aud"] != "https://graph.microsoft.com" {
		t.Fatalf("%v", claims)
	}
	// a wrong one is refused, with the error code of Entra
	for _, bad := range []string{"wrong", "not-a-secret"} {
		_, err = e.credential(appID, bad).GetToken(ctx, policy.TokenRequestOptions{Scopes: []string{"https://graph.microsoft.com/.default"}})
		if err == nil || !strings.Contains(err.Error(), "invalid_client") {
			t.Fatalf("secret %q: %v", bad, err)
		}
	}
	// a second secret works too, and a removed one stops
	pw2 := e.ok(t, 200, "POST", "/v1.0/applications/"+appObj+"/addPassword", obj{})
	if _, err = e.credential(appID, pw2["secretText"].(string)).GetToken(ctx, policy.TokenRequestOptions{Scopes: []string{"https://graph.microsoft.com/.default"}}); err != nil {
		t.Fatal(err)
	}
	e.ok(t, 204, "POST", "/v1.0/applications/"+appObj+"/removePassword", obj{"keyId": pw["keyId"]})
	if _, err = e.credential(appID, secret).GetToken(ctx, policy.TokenRequestOptions{Scopes: []string{"https://graph.microsoft.com/.default"}}); err == nil {
		t.Fatal("a removed secret still signs in")
	}
	// deleting the application and its service principal is clean
	e.ok(t, 204, "DELETE", "/v1.0/servicePrincipals/"+sp["id"].(string), nil)
	e.ok(t, 204, "DELETE", "/v1.0/applications/"+appObj, nil)
	e.ok(t, 404, "GET", "/v1.0/applications/"+appObj, nil)
}

func TestUsersAndGroups(t *testing.T) {
	e := start(t)
	if st, _ := e.rawNoAuth(t, "/v1.0/users"); st != 401 {
		t.Fatalf("no token: %d", st)
	}

	ann := e.ok(t, 201, "POST", "/v1.0/users", obj{"userPrincipalName": "ann@pogi.onmicrosoft.com", "displayName": "Ann Lee", "mailNickname": "ann",
		"accountEnabled": true, "passwordProfile": obj{"password": "Pw-12345", "forceChangePasswordNextSignIn": false}})
	if ann["id"] == "" || ann["displayName"] != "Ann Lee" || ann["passwordProfile"] != nil || ann["userType"] != "Member" {
		t.Fatalf("%v", ann)
	}
	annID := ann["id"].(string)
	e.ok(t, 400, "POST", "/v1.0/users", obj{"userPrincipalName": "ANN@pogi.onmicrosoft.com", "displayName": "Dup"})
	e.ok(t, 400, "POST", "/v1.0/users", obj{"displayName": "No upn"})
	e.ok(t, 400, "GET", "/v1.0/users?$skiptoken=-1", nil)
	for _, n := range []string{"Alan Ng", "Alice Roe", "Bob Fry", "Cy Dunn"} {
		e.ok(t, 201, "POST", "/v1.0/users", obj{"userPrincipalName": strings.ToLower(strings.Fields(n)[0]) + "@pogi.onmicrosoft.com", "displayName": n})
	}

	// read by object ID and by principal name; update; the change shows
	if got := e.ok(t, 200, "GET", "/v1.0/users/ann@pogi.onmicrosoft.com", nil); got["id"] != annID {
		t.Fatalf("%v", got)
	}
	e.ok(t, 204, "PATCH", "/v1.0/users/"+annID, obj{"jobTitle": "Engineer", "displayName": "Ann Q. Lee"})
	if got := e.ok(t, 200, "GET", "/v1.0/users/"+annID, nil); got["jobTitle"] != "Engineer" || got["displayName"] != "Ann Q. Lee" || got["userPrincipalName"] != "ann@pogi.onmicrosoft.com" {
		t.Fatalf("%v", got)
	}
	e.ok(t, 404, "GET", "/v1.0/users/00000000-0000-0000-0000-0000000000aa", nil)

	// $filter: eq, startswith, and the combination, with $select and $top
	names := func(o obj) []string {
		var out []string
		for _, u := range values(o) {
			out = append(out, u["displayName"].(string))
		}
		slices.Sort(out)
		return out
	}
	if got := names(e.ok(t, 200, "GET", `/v1.0/users?$filter=userPrincipalName%20eq%20'bob@pogi.onmicrosoft.com'`, nil)); !slices.Equal(got, []string{"Bob Fry"}) {
		t.Fatal(got)
	}
	if got := names(e.ok(t, 200, "GET", `/v1.0/users?$filter=startswith(displayName,'Al')`, nil)); !slices.Equal(got, []string{"Alan Ng", "Alice Roe"}) {
		t.Fatal(got)
	}
	if got := names(e.ok(t, 200, "GET", `/v1.0/users?$filter=startswith(displayName,'al')%20and%20userPrincipalName%20eq%20'alice@pogi.onmicrosoft.com'`, nil)); !slices.Equal(got, []string{"Alice Roe"}) {
		t.Fatal(got)
	}
	if got := values(e.ok(t, 200, "GET", `/v1.0/users?$filter=displayName%20eq%20'nobody'`, nil)); len(got) != 0 {
		t.Fatal(got)
	}
	e.ok(t, 400, "GET", `/v1.0/users?$filter=%28%28broken`, nil)

	page := e.ok(t, 200, "GET", `/v1.0/users?$filter=startswith(displayName,'Al')&$select=displayName&$top=1&$orderby=displayName&$count=true`, nil)
	first := values(page)
	if len(first) != 1 || first[0]["displayName"] != "Alan Ng" || first[0]["id"] == "" || first[0]["userPrincipalName"] != nil || len(first[0]) != 2 {
		t.Fatalf("%v", page)
	}
	if page["@odata.count"] != float64(2) || page["@odata.nextLink"] == nil {
		t.Fatalf("%v", page)
	}
	next := e.ok(t, 200, "GET", page["@odata.nextLink"].(string), nil)
	if v := values(next); len(v) != 1 || v[0]["displayName"] != "Alice Roe" || next["@odata.nextLink"] != nil {
		t.Fatalf("%v", next)
	}

	// groups: create, members (add, list, duplicate, remove), update, filter, delete
	g := e.ok(t, 201, "POST", "/v1.0/groups", obj{"displayName": "eng", "mailNickname": "eng", "securityEnabled": true, "description": "engineers"})
	gid := g["id"].(string)
	if g["securityEnabled"] != true || g["mailEnabled"] != false {
		t.Fatalf("%v", g)
	}
	e.ok(t, 400, "POST", "/v1.0/groups", obj{"description": "no name"})
	ref := func(id string) obj { return obj{"@odata.id": e.base + "/v1.0/directoryObjects/" + id} }
	e.ok(t, 204, "POST", "/v1.0/groups/"+gid+"/members/$ref", ref(annID))
	e.ok(t, 400, "POST", "/v1.0/groups/"+gid+"/members/$ref", ref(annID))
	e.ok(t, 404, "POST", "/v1.0/groups/"+gid+"/members/$ref", ref("00000000-0000-0000-0000-0000000000aa"))
	members := values(e.ok(t, 200, "GET", "/v1.0/groups/"+gid+"/members", nil))
	if len(members) != 1 || members[0]["id"] != annID || members[0]["@odata.type"] != "#microsoft.graph.user" {
		t.Fatalf("%v", members)
	}
	if refs := values(e.ok(t, 200, "GET", "/v1.0/groups/"+gid+"/members/$ref", nil)); len(refs) != 1 || !strings.HasSuffix(refs[0]["@odata.id"].(string), annID) {
		t.Fatalf("%v", refs)
	}
	e.ok(t, 204, "PATCH", "/v1.0/groups/"+gid, obj{"description": "all engineers"})
	if got := e.ok(t, 200, "GET", "/v1.0/groups?$filter=displayName%20eq%20'ENG'&$select=description", nil); values(got)[0]["description"] != "all engineers" {
		t.Fatalf("%v", got)
	}
	e.ok(t, 204, "DELETE", "/v1.0/groups/"+gid+"/members/"+annID+"/$ref", nil)
	if members = values(e.ok(t, 200, "GET", "/v1.0/groups/"+gid+"/members", nil)); len(members) != 0 {
		t.Fatalf("%v", members)
	}
	// a member that is deleted leaves its groups
	e.ok(t, 204, "POST", "/v1.0/groups/"+gid+"/members/$ref", ref(annID))
	e.ok(t, 204, "DELETE", "/v1.0/users/"+annID, nil)
	e.ok(t, 404, "GET", "/v1.0/users/"+annID, nil)
	if members = values(e.ok(t, 200, "GET", "/v1.0/groups/"+gid+"/members", nil)); len(members) != 0 {
		t.Fatalf("%v", members)
	}
	e.ok(t, 204, "DELETE", "/v1.0/groups/"+gid, nil)
	e.ok(t, 404, "GET", "/v1.0/groups/"+gid, nil)
}

func (e *env) rawNoAuth(t *testing.T, path string) (int, []byte) {
	t.Helper()
	res, err := e.http.Get(e.base + path)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	return res.StatusCode, must(io.ReadAll(res.Body))
}
