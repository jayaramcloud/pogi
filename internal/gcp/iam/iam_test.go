package iam_test

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"io"
	"math/big"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	adminapi "cloud.google.com/go/iam/admin/apiv1"
	"cloud.google.com/go/iam/admin/apiv1/adminpb"
	"cloud.google.com/go/iam/apiv1/iampb"
	credapi "cloud.google.com/go/iam/credentials/apiv1"
	credpb "cloud.google.com/go/iam/credentials/apiv1/credentialspb"
	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/gcp/gapi"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iam/v1"
	"google.golang.org/api/iamcredentials/v1"
	"google.golang.org/api/idtoken"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

var ctx = context.Background()

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func start(t *testing.T) *pogi.Running {
	t.Helper()
	return pogi.Start(t, pogi.Config{Services: []string{"gcpiam", "iamcredentials", "gcpoauth", "operations"}})
}

// rest is the IAM REST client Terraform's google provider uses.
func rest(t *testing.T, s *pogi.Running) *iam.Service {
	t.Helper()
	return must(iam.NewService(ctx, option.WithEndpoint(s.URL), option.WithoutAuthentication()))
}

func grpcOpts(s *pogi.Running) []option.ClientOption {
	return []option.ClientOption{option.WithEndpoint(strings.TrimPrefix(s.URL, "http://")), option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials()))}
}

func wantHTTP(t *testing.T, err error, want int) {
	t.Helper()
	var ge *googleapi.Error
	if err == nil || !asGoogle(err, &ge) || ge.Code != want {
		t.Fatalf("got %v, want HTTP %d", err, want)
	}
}

func asGoogle(err error, ge **googleapi.Error) bool {
	e, ok := err.(*googleapi.Error)
	*ge = e
	return ok
}

func TestServiceAccountsREST(t *testing.T) {
	s := start(t)
	svc := rest(t, s).Projects.ServiceAccounts
	sa := must(svc.Create("projects/proj", &iam.CreateServiceAccountRequest{AccountId: "my-svc",
		ServiceAccount: &iam.ServiceAccount{DisplayName: "My SA", Description: "first"}}).Do())
	if sa.Email != "my-svc@proj.iam.gserviceaccount.com" || sa.Name != "projects/proj/serviceAccounts/"+sa.Email ||
		sa.ProjectId != "proj" || len(sa.UniqueId) != 21 || sa.Oauth2ClientId != sa.UniqueId || sa.DisplayName != "My SA" {
		t.Fatalf("created %+v", sa)
	}
	_, err := svc.Create("projects/proj", &iam.CreateServiceAccountRequest{AccountId: "my-svc"}).Do()
	wantHTTP(t, err, 409)
	_, err = svc.Create("projects/proj", &iam.CreateServiceAccountRequest{AccountId: "Bad_Id"}).Do()
	wantHTTP(t, err, 400)

	got := must(svc.Get(sa.Name).Do())
	if got.UniqueId != sa.UniqueId || got.Description != "first" {
		t.Fatalf("get %+v", got)
	}
	if byID := must(svc.Get("projects/-/serviceAccounts/" + sa.UniqueId).Do()); byID.Email != sa.Email {
		t.Fatalf("by unique id: %+v", byID)
	}
	_, err = svc.Get("projects/proj/serviceAccounts/nobody@proj.iam.gserviceaccount.com").Do()
	wantHTTP(t, err, 404)
	must(svc.Create("projects/proj", &iam.CreateServiceAccountRequest{AccountId: "other-sa"}).Do())
	must(svc.Create("projects/elsewhere", &iam.CreateServiceAccountRequest{AccountId: "third-sa"}).Do())

	list := must(svc.List("projects/proj").Do())
	if len(list.Accounts) != 2 || list.Accounts[0].Email != "my-svc@proj.iam.gserviceaccount.com" {
		t.Fatalf("list %+v", list.Accounts)
	}
	if page := must(svc.List("projects/proj").PageSize(1).Do()); len(page.Accounts) != 1 || page.NextPageToken == "" {
		t.Fatalf("page %+v", page)
	}

	// what Terraform sends on update: a patch with a mask
	p := must(svc.Patch(sa.Name, &iam.PatchServiceAccountRequest{UpdateMask: "displayName,description",
		ServiceAccount: &iam.ServiceAccount{DisplayName: "Renamed", Description: ""}}).Do())
	if p.DisplayName != "Renamed" || p.Description != "" || string(p.Etag) == string(sa.Etag) {
		t.Fatalf("patched %+v", p)
	}
	must(svc.Update(sa.Name, &iam.ServiceAccount{DisplayName: "Put", Description: "d"}).Do())
	if g := must(svc.Get(sa.Name).Do()); g.DisplayName != "Put" || g.Description != "d" {
		t.Fatalf("after put %+v", g)
	}

	must(svc.Disable(sa.Name, &iam.DisableServiceAccountRequest{}).Do())
	if !must(svc.Get(sa.Name).Do()).Disabled {
		t.Fatal("not disabled")
	}
	must(svc.Enable(sa.Name, &iam.EnableServiceAccountRequest{}).Do())
	if must(svc.Get(sa.Name).Do()).Disabled {
		t.Fatal("still disabled")
	}

	must(svc.Delete(sa.Name).Do())
	_, err = svc.Get(sa.Name).Do()
	wantHTTP(t, err, 404)
	_, err = svc.Delete(sa.Name).Do()
	wantHTTP(t, err, 404)
	back := must(svc.Undelete("projects/-/serviceAccounts/"+sa.UniqueId, &iam.UndeleteServiceAccountRequest{}).Do())
	if back.RestoredAccount.Email != sa.Email || back.RestoredAccount.UniqueId != sa.UniqueId {
		t.Fatalf("undeleted %+v", back)
	}
}

func TestServiceAccountsGRPC(t *testing.T) {
	s := start(t)
	c := must(adminapi.NewIamClient(ctx, grpcOpts(s)...))
	defer c.Close()
	sa := must(c.CreateServiceAccount(ctx, &adminpb.CreateServiceAccountRequest{Name: "projects/proj", AccountId: "grpc-sa",
		ServiceAccount: &adminpb.ServiceAccount{DisplayName: "g"}}))
	if sa.Email != "grpc-sa@proj.iam.gserviceaccount.com" {
		t.Fatalf("created %v", sa)
	}
	_, err := c.CreateServiceAccount(ctx, &adminpb.CreateServiceAccountRequest{Name: "projects/proj", AccountId: "grpc-sa"})
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("duplicate: %v", err)
	}

	// IAM policy on the account: round trip, then a stale etag is refused.
	conn := must(grpc.NewClient(strings.TrimPrefix(s.URL, "http://"), grpc.WithTransportCredentials(insecure.NewCredentials())))
	defer conn.Close()
	c2 := adminpb.NewIAMClient(conn)
	pol := must(c2.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: sa.Name}))
	if len(pol.Bindings) != 0 || len(pol.Etag) == 0 {
		t.Fatalf("empty policy %v", pol)
	}
	set := must(c2.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{Resource: sa.Name, Policy: &iampb.Policy{Etag: pol.Etag,
		Bindings: []*iampb.Binding{{Role: "roles/iam.serviceAccountUser", Members: []string{"user:a@example.com"}}, {Role: "roles/none"}}}}))
	if len(set.Bindings) != 1 || string(set.Etag) == string(pol.Etag) {
		t.Fatalf("set %v", set)
	}
	if got := must(c2.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: sa.Name})); got.Bindings[0].Members[0] != "user:a@example.com" {
		t.Fatalf("get %v", got)
	}
	_, err = c2.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{Resource: sa.Name, Policy: &iampb.Policy{Etag: pol.Etag}})
	if status.Code(err) != codes.Aborted {
		t.Fatalf("stale etag: %v", err)
	}
	perms := must(c2.TestIamPermissions(ctx, &iampb.TestIamPermissionsRequest{Resource: sa.Name, Permissions: []string{"iam.serviceAccounts.get"}}))
	if len(perms.Permissions) != 1 {
		t.Fatalf("perms %v", perms)
	}
}

// keyFile creates a key through the REST client and returns its credentials file.
func keyFile(t *testing.T, s *pogi.Running, project, id string) (email string, file []byte, key *iam.ServiceAccountKey) {
	t.Helper()
	svc := rest(t, s).Projects.ServiceAccounts
	sa := must(svc.Create("projects/"+project, &iam.CreateServiceAccountRequest{AccountId: id}).Do())
	key = must(svc.Keys.Create(sa.Name, &iam.CreateServiceAccountKeyRequest{}).Do())
	return sa.Email, must(base64.StdEncoding.DecodeString(key.PrivateKeyData)), key
}

func TestKeysAndTokenSource(t *testing.T) {
	s := start(t)
	email, file, key := keyFile(t, s, "proj", "key-sa")
	var f map[string]string
	if err := json.Unmarshal(file, &f); err != nil {
		t.Fatal(err)
	}
	if f["type"] != "service_account" || f["client_email"] != email || f["project_id"] != "proj" || f["token_uri"] != s.URL+"/token" ||
		!strings.HasPrefix(f["private_key"], "-----BEGIN PRIVATE KEY-----") || !strings.HasSuffix(key.Name, "/keys/"+f["private_key_id"]) {
		t.Fatalf("key file %v", f)
	}
	if key.KeyAlgorithm != "KEY_ALG_RSA_2048" || key.PrivateKeyType != "TYPE_GOOGLE_CREDENTIALS_FILE" || key.ValidBeforeTime == "" {
		t.Fatalf("key %+v", key)
	}

	// the key file works with Google's own client library, and tokeninfo knows the token
	creds := must(google.CredentialsFromJSON(ctx, file, "https://www.googleapis.com/auth/cloud-platform"))
	tok := must(creds.TokenSource.Token())
	if !strings.HasPrefix(tok.AccessToken, "ya29.pogi-") || tok.TokenType != "Bearer" || time.Until(tok.Expiry) < 50*time.Minute {
		t.Fatalf("token %+v", tok)
	}
	info := tokeninfo(t, s, "access_token="+tok.AccessToken)
	if info["email"] != email || info["scope"] != "https://www.googleapis.com/auth/cloud-platform" {
		t.Fatalf("tokeninfo %v", info)
	}

	// Terraform reads the key back with its public half
	svc := rest(t, s).Projects.ServiceAccounts.Keys
	got := must(svc.Get(key.Name).PublicKeyType("TYPE_X509_PEM_FILE").Do())
	pub := must(base64.StdEncoding.DecodeString(got.PublicKeyData))
	blk, _ := pem.Decode(pub)
	if blk == nil || blk.Type != "CERTIFICATE" {
		t.Fatalf("public key data %q", pub)
	}
	if _, err := x509.ParseCertificate(blk.Bytes); err != nil {
		t.Fatal(err)
	}
	if got.ValidAfterTime == "" || got.KeyOrigin != "GOOGLE_PROVIDED" {
		t.Fatalf("key %+v", got)
	}
	list := must(svc.List("projects/proj/serviceAccounts/" + email).KeyTypes("USER_MANAGED").Do())
	if len(list.Keys) != 1 || list.Keys[0].Name != key.Name || list.Keys[0].KeyType != "USER_MANAGED" {
		t.Fatalf("user keys %+v", list.Keys)
	}
	if all := must(svc.List("projects/proj/serviceAccounts/" + email).Do()); len(all.Keys) != 2 {
		t.Fatalf("with the system key: %+v", all.Keys)
	}

	// a disabled key, a disabled account and a deleted key are each refused by the token endpoint
	refused := func(why string) {
		t.Helper()
		creds := must(google.CredentialsFromJSON(ctx, file, "scope"))
		if _, err := creds.TokenSource.Token(); err == nil || !strings.Contains(err.Error(), "invalid_grant") {
			t.Fatalf("%s: %v", why, err)
		}
	}
	must(svc.Disable(key.Name, &iam.DisableServiceAccountKeyRequest{}).Do())
	refused("disabled key")
	must(svc.Enable(key.Name, &iam.EnableServiceAccountKeyRequest{}).Do())
	must(google.CredentialsFromJSON(ctx, file, "scope")).TokenSource.Token()
	acct := "projects/proj/serviceAccounts/" + email
	must(rest(t, s).Projects.ServiceAccounts.Disable(acct, &iam.DisableServiceAccountRequest{}).Do())
	refused("disabled account")
	must(rest(t, s).Projects.ServiceAccounts.Enable(acct, &iam.EnableServiceAccountRequest{}).Do())
	must(svc.Delete(key.Name).Do())
	refused("deleted key")
	_, err := svc.Get(key.Name).Do()
	wantHTTP(t, err, 404)
}

func tokeninfo(t *testing.T, s *pogi.Running, query string) map[string]string {
	t.Helper()
	resp := must(http.Get(s.URL + "/oauth2/v3/tokeninfo?" + query))
	defer resp.Body.Close()
	var m map[string]string
	json.NewDecoder(resp.Body).Decode(&m)
	if resp.StatusCode != 200 {
		t.Fatalf("tokeninfo %d %v", resp.StatusCode, m)
	}
	return m
}

// toPogi sends every request, whatever its host, to pogi: it stands in for DNS
// so Google's validators can fetch https://www.googleapis.com/oauth2/v3/certs.
type toPogi struct{ host string }

func (p toPogi) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.URL.Scheme, r.URL.Host = "http", p.host
	return http.DefaultTransport.RoundTrip(r)
}

func validator(t *testing.T, s *pogi.Running) *idtoken.Validator {
	t.Helper()
	return must(idtoken.NewValidator(ctx, option.WithHTTPClient(&http.Client{Transport: toPogi{strings.TrimPrefix(s.URL, "http://")}})))
}

func TestIDTokens(t *testing.T) {
	s := start(t)
	email, file, _ := keyFile(t, s, "proj", "ident-sa")
	v := validator(t, s)

	// idtoken's own token source: a key-signed assertion with target_audience, exchanged at the token endpoint
	ts := must(idtoken.NewTokenSource(ctx, "https://my-service.example.com", option.WithCredentialsJSON(file)))
	tok := must(ts.Token())
	p, err := v.Validate(ctx, tok.AccessToken, "https://my-service.example.com")
	if err != nil {
		t.Fatalf("idtoken from the key: %v", err)
	}
	if p.Claims["email"] != email || p.Issuer != "https://accounts.google.com" {
		t.Fatalf("payload %+v", p)
	}
	if _, err := v.Validate(ctx, tok.AccessToken, "https://other.example.com"); err == nil {
		t.Fatal("wrong audience accepted")
	}

	// IAM Credentials' generateIdToken is signed by the same key
	gen := must(must(iamcredentials.NewService(ctx, option.WithEndpoint(s.URL), option.WithoutAuthentication())).Projects.ServiceAccounts.
		GenerateIdToken("projects/-/serviceAccounts/"+email, &iamcredentials.GenerateIdTokenRequest{Audience: "aud-2", IncludeEmail: true}).Do())
	if p, err := v.Validate(ctx, gen.Token, "aud-2"); err != nil || p.Claims["email"] != email {
		t.Fatalf("generateIdToken: %v %+v", err, p)
	}

	// and so is what Cloud Tasks and Cloud Scheduler attach
	if _, err := v.Validate(ctx, gapi.OIDCToken(email, "https://task.example.com"), "https://task.example.com"); err != nil {
		t.Fatalf("OIDC token of a Tasks delivery: %v", err)
	}
}

func TestIAMCredentials(t *testing.T) {
	s := start(t)
	email, _, _ := keyFile(t, s, "proj", "cred-sa")
	svc := must(iamcredentials.NewService(ctx, option.WithEndpoint(s.URL), option.WithoutAuthentication())).Projects.ServiceAccounts
	name := "projects/-/serviceAccounts/" + email

	at := must(svc.GenerateAccessToken(name, &iamcredentials.GenerateAccessTokenRequest{Scope: []string{"s1", "s2"}, Lifetime: "600s"}).Do())
	if info := tokeninfo(t, s, "access_token="+at.AccessToken); info["email"] != email || info["scope"] != "s1 s2" {
		t.Fatalf("tokeninfo %v", info)
	}
	if exp := must(time.Parse(time.RFC3339, at.ExpireTime)); time.Until(exp) > 11*time.Minute || time.Until(exp) < 9*time.Minute {
		t.Fatalf("expire %s", at.ExpireTime)
	}
	_, err := svc.GenerateAccessToken(name, &iamcredentials.GenerateAccessTokenRequest{Scope: []string{"s"}, Lifetime: "86400s"}).Do()
	wantHTTP(t, err, 400)
	_, err = svc.GenerateAccessToken(name, &iamcredentials.GenerateAccessTokenRequest{}).Do()
	wantHTTP(t, err, 400)
	_, err = svc.GenerateAccessToken("projects/-/serviceAccounts/ghost@proj.iam.gserviceaccount.com", &iamcredentials.GenerateAccessTokenRequest{Scope: []string{"s"}}).Do()
	wantHTTP(t, err, 404)

	// signJwt verifies with the JWK published for the account
	claims := `{"iss":"` + email + `","sub":"` + email + `","aud":"https://example.com","exp":` + "4102444800" + `}`
	sj := must(svc.SignJwt(name, &iamcredentials.SignJwtRequest{Payload: claims}).Do())
	jwks := fetch(t, s.URL+"/service_accounts/v1/metadata/jwk/"+email)
	var set struct{ Keys []map[string]string }
	json.Unmarshal(jwks, &set)
	var jwk map[string]string // the system key; the account's user key is published too
	for _, k := range set.Keys {
		if k["kid"] == sj.KeyId {
			jwk = k
		}
	}
	if len(set.Keys) != 2 || jwk == nil || jwk["alg"] != "RS256" || jwk["kty"] != "RSA" {
		t.Fatalf("jwk %s (signing key %s)", jwks, sj.KeyId)
	}
	n := new(big.Int).SetBytes(must(base64.RawURLEncoding.DecodeString(jwk["n"])))
	pub := &rsa.PublicKey{N: n, E: 65537}
	jwt := must(gapi.ParseJWT(sj.SignedJwt))
	if !jwt.Verify(pub) || jwt.Str("iss") != email || jwt.Header["kid"] != sj.KeyId {
		t.Fatalf("signed jwt %s", sj.SignedJwt)
	}
	_, err = svc.SignJwt(name, &iamcredentials.SignJwtRequest{Payload: "not json"}).Do()
	wantHTTP(t, err, 400)

	// signBlob verifies with the X.509 certificate published for the account
	blob := []byte("sign me")
	sb := must(svc.SignBlob(name, &iamcredentials.SignBlobRequest{Payload: base64.StdEncoding.EncodeToString(blob)}).Do())
	var certs map[string]string
	json.Unmarshal(fetch(t, s.URL+"/service_accounts/v1/metadata/x509/"+email), &certs)
	blk, _ := pem.Decode([]byte(certs[sb.KeyId]))
	if blk == nil {
		t.Fatalf("no certificate for key %s in %v", sb.KeyId, certs)
	}
	cert := must(x509.ParseCertificate(blk.Bytes))
	sig := must(base64.StdEncoding.DecodeString(sb.SignedBlob))
	sum := sha256.Sum256(blob)
	if err := rsa.VerifyPKCS1v15(cert.PublicKey.(*rsa.PublicKey), crypto.SHA256, sum[:], sig); err != nil {
		t.Fatalf("signBlob: %v", err)
	}
	if sb.KeyId != sj.KeyId {
		t.Fatalf("two system keys: %s and %s", sb.KeyId, sj.KeyId)
	}
	if resp := must(http.Get(s.URL + "/service_accounts/v1/metadata/x509/ghost@proj.iam.gserviceaccount.com")); resp.StatusCode != 404 {
		t.Fatalf("unknown account: %d", resp.StatusCode)
	}

	// the same over gRPC
	c := must(credapi.NewIamCredentialsClient(ctx, grpcOpts(s)...))
	defer c.Close()
	r := must(c.SignBlob(ctx, &credpb.SignBlobRequest{Name: name, Payload: blob}))
	if r.KeyId != sb.KeyId {
		t.Fatalf("grpc signBlob key %s", r.KeyId)
	}
	idt := must(c.GenerateIdToken(ctx, &credpb.GenerateIdTokenRequest{Name: name, Audience: "x"}))
	if j := must(gapi.ParseJWT(idt.Token)); j.Str("aud") != "x" || !j.Verify(gapi.PublicKey()) {
		t.Fatalf("grpc id token %v", j.Claims)
	}
}

func fetch(t *testing.T, url string) []byte {
	t.Helper()
	resp := must(http.Get(url))
	defer resp.Body.Close()
	return must(io.ReadAll(resp.Body))
}

func TestWorkloadIdentityImpersonation(t *testing.T) {
	// an external account's token is exchanged at STS, then traded for the service account's own access token
	s := start(t)
	email, _, _ := keyFile(t, s, "proj", "wif-sa")
	cfg := map[string]any{"type": "external_account", "audience": "//iam.googleapis.com/projects/1/locations/global/workloadIdentityPools/p/providers/pr",
		"subject_token_type": "urn:ietf:params:oauth:token-type:jwt", "token_url": s.URL + "/v1/token",
		"service_account_impersonation_url": s.URL + "/v1/projects/-/serviceAccounts/" + email + ":generateAccessToken",
		"credential_source":                 map[string]any{"file": writeTemp(t, "external-subject-token")}}
	b, _ := json.Marshal(cfg)
	creds := must(google.CredentialsFromJSON(ctx, b, "https://www.googleapis.com/auth/cloud-platform"))
	tok := must(creds.TokenSource.Token())
	if info := tokeninfo(t, s, "access_token="+tok.AccessToken); info["email"] != email {
		t.Fatalf("impersonated token %v", info)
	}
}

func TestRoles(t *testing.T) {
	s := start(t)
	svc := rest(t, s)
	r := must(svc.Roles.Get("roles/viewer").Do())
	if r.Name != "roles/viewer" || r.Title != "Viewer" || len(r.IncludedPermissions) == 0 || r.Stage != "GA" {
		t.Fatalf("viewer %+v", r)
	}
	for _, name := range []string{"roles/owner", "roles/editor", "roles/storage.objectViewer", "roles/iam.serviceAccountTokenCreator"} {
		must(svc.Roles.Get(name).Do())
	}
	if list := must(svc.Roles.List().Do()); len(list.Roles) < 30 || len(list.Roles[0].IncludedPermissions) != 0 {
		t.Fatalf("predefined list: %d roles, first %+v", len(list.Roles), list.Roles[0])
	}
	if list := must(svc.Roles.List().View("FULL").PageSize(2).Do()); len(list.Roles) != 2 || len(list.Roles[0].IncludedPermissions) == 0 || list.NextPageToken == "" {
		t.Fatalf("full page %+v", list)
	}
	_, err := svc.Roles.Get("roles/nope").Do()
	wantHTTP(t, err, 404)

	// custom roles: what google_project_iam_custom_role does
	p := svc.Projects.Roles
	role := must(p.Create("projects/proj", &iam.CreateRoleRequest{RoleId: "myRole",
		Role: &iam.Role{Title: "Mine", Description: "d", Stage: "GA", IncludedPermissions: []string{"storage.buckets.get"}}}).Do())
	if role.Name != "projects/proj/roles/myRole" || role.Stage != "GA" || len(role.IncludedPermissions) != 1 || role.Etag == "" {
		t.Fatalf("created %+v", role)
	}
	_, err = p.Create("projects/proj", &iam.CreateRoleRequest{RoleId: "myRole", Role: &iam.Role{}}).Do()
	wantHTTP(t, err, 409)
	_, err = p.Create("projects/proj", &iam.CreateRoleRequest{RoleId: "x", Role: &iam.Role{}}).Do()
	wantHTTP(t, err, 400)
	must(svc.Organizations.Roles.Create("organizations/1", &iam.CreateRoleRequest{RoleId: "orgRole", Role: &iam.Role{Title: "Org"}}).Do())
	up := must(p.Patch(role.Name, &iam.Role{Title: "Renamed", IncludedPermissions: []string{"storage.buckets.get", "storage.buckets.list"}}).UpdateMask("title,includedPermissions").Do())
	if up.Title != "Renamed" || len(up.IncludedPermissions) != 2 || up.Description != "d" || up.Etag == role.Etag {
		t.Fatalf("patched %+v", up)
	}
	_, err = p.Patch(role.Name, &iam.Role{Title: "x", Etag: role.Etag}).UpdateMask("title").Do() // stale etag
	wantHTTP(t, err, 409)
	if list := must(p.List("projects/proj").Do()); len(list.Roles) != 1 {
		t.Fatalf("project roles %+v", list.Roles)
	}
	if got := must(p.Get(role.Name).Do()); len(got.IncludedPermissions) != 2 {
		t.Fatalf("get %+v", got)
	}
	del := must(p.Delete(role.Name).Do())
	if !del.Deleted {
		t.Fatalf("deleted %+v", del)
	}
	if list := must(p.List("projects/proj").Do()); len(list.Roles) != 0 {
		t.Fatalf("deleted roles are listed: %+v", list.Roles)
	}
	if list := must(p.List("projects/proj").ShowDeleted(true).Do()); len(list.Roles) != 1 {
		t.Fatalf("show deleted: %+v", list.Roles)
	}
	if back := must(p.Undelete(role.Name, &iam.UndeleteRoleRequest{}).Do()); back.Deleted {
		t.Fatalf("undeleted %+v", back)
	}
}

func TestPersistence(t *testing.T) {
	s := start(t)
	_, file, key := keyFile(t, s, "proj", "keep-sa")
	idToken := gapi.OIDCToken("x@y", "aud")
	saved := must(s.Hub.Service("gcpiam").(core.Stateful).Save())
	gapi.SetSigningKey(must(rsa.GenerateKey(rand.Reader, 2048))) // a restart: the process forgets its key

	s2 := start(t)
	if err := s2.Hub.Service("gcpiam").(core.Stateful).Load(saved); err != nil {
		t.Fatal(err)
	}
	// the account and its key survive: the old credentials file still gets a token
	// (from the new server: the file names the old one, so point the endpoint at the new one)
	var f map[string]any
	json.Unmarshal(file, &f)
	f["token_uri"] = s2.URL + "/token"
	b, _ := json.Marshal(f)
	if _, err := must(google.CredentialsFromJSON(ctx, b, "scope")).TokenSource.Token(); err != nil {
		t.Fatalf("key after reload: %v", err)
	}
	if _, err := rest(t, s2).Projects.ServiceAccounts.Keys.Get(key.Name).Do(); err != nil {
		t.Fatal(err)
	}
	// and so does the ID-token key: tokens from before still verify against the new server's certs
	if _, err := validator(t, s2).Validate(ctx, idToken, "aud"); err != nil {
		t.Fatalf("id token after reload: %v", err)
	}
}

func writeTemp(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "subject-token")
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}
