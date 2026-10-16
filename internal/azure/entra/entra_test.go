package entra_test

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/cloud"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/msi/armmsi"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/resources/armresources"
	"github.com/nrndev/pogi"
	armsvc "github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/core"
)

var ctx = context.Background()

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

type env struct {
	srv  *pogi.Running
	base string // https://localhost:port
	http *http.Client
	co   azcore.ClientOptions
}

// start runs pogi with Resource Manager, Entra and Graph, and a client that
// trusts pogi's CA and whose authority host is pogi.
func start(t *testing.T) *env {
	t.Helper()
	srv := pogi.Start(t, pogi.Config{Services: []string{"arm", "entra", "graph"}})
	base := "https://localhost:" + srv.URL[strings.LastIndexByte(srv.URL, ':')+1:]
	ca, _, _ := srv.Hub.CertPaths()
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(must(os.ReadFile(ca)))
	hc := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, ServerName: "localhost"}}}
	co := azcore.ClientOptions{Transport: hc, Cloud: cloud.Configuration{ActiveDirectoryAuthorityHost: base + "/",
		Services: map[cloud.ServiceName]cloud.ServiceConfiguration{cloud.ResourceManager: {Endpoint: base, Audience: base}}}}
	return &env{srv: srv, base: base, http: hc, co: co}
}

// call sends JSON (or a form, when body is url.Values) and decodes a JSON answer.
func (e *env) call(t *testing.T, method, path string, body any) (int, map[string]any) {
	t.Helper()
	var rd io.Reader
	ctype := "application/json"
	switch b := body.(type) {
	case nil:
	case url.Values:
		rd, ctype = strings.NewReader(b.Encode()), "application/x-www-form-urlencoded"
	default:
		rd = bytes.NewReader(must(json.Marshal(b)))
	}
	req, _ := http.NewRequest(method, e.base+path, rd)
	req.Header.Set("Authorization", "Bearer x")
	req.Header.Set("Content-Type", ctype)
	res, err := e.http.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	var out map[string]any
	json.NewDecoder(res.Body).Decode(&out)
	return res.StatusCode, out
}

// claims checks a token's RS256 signature against the discovery JWKS and returns its claims.
func (e *env) claims(t *testing.T, tok string) map[string]any {
	t.Helper()
	p := strings.Split(tok, ".")
	if len(p) != 3 {
		t.Fatalf("not a JWT: %q", tok)
	}
	var hdr struct{ Alg, Kid string }
	json.Unmarshal(must(base64.RawURLEncoding.DecodeString(p[0])), &hdr)
	if hdr.Alg != "RS256" {
		t.Fatalf("alg %q", hdr.Alg)
	}
	st, jwks := e.call(t, "GET", "/"+armsvc.TenantID+"/discovery/v2.0/keys", nil)
	if st != 200 {
		t.Fatal(st)
	}
	for _, k := range jwks["keys"].([]any) {
		k := k.(map[string]any)
		if k["kid"] != hdr.Kid {
			continue
		}
		pub := &rsa.PublicKey{N: new(big.Int).SetBytes(must(base64.RawURLEncoding.DecodeString(k["n"].(string)))),
			E: int(new(big.Int).SetBytes(must(base64.RawURLEncoding.DecodeString(k["e"].(string)))).Int64())}
		sum := sha256.Sum256([]byte(p[0] + "." + p[1]))
		if err := rsa.VerifyPKCS1v15(pub, crypto.SHA256, sum[:], must(base64.RawURLEncoding.DecodeString(p[2]))); err != nil {
			t.Fatalf("signature: %v", err)
		}
		var c map[string]any
		json.Unmarshal(must(base64.RawURLEncoding.DecodeString(p[1])), &c)
		if exp, _ := c["exp"].(float64); int64(exp) <= time.Now().Unix() {
			t.Fatal("expired")
		}
		return c
	}
	t.Fatalf("no key %q in the JWKS", hdr.Kid)
	return nil
}

func (e *env) token(t *testing.T, cred azcore.TokenCredential, scope string) map[string]any {
	t.Helper()
	tok, err := cred.GetToken(ctx, policy.TokenRequestOptions{Scopes: []string{scope}})
	if err != nil {
		t.Fatal(err)
	}
	return e.claims(t, tok.Token)
}

type roundTripper func(*http.Request) (*http.Response, error)

func (f roundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestClientSecretCredential(t *testing.T) {
	e := start(t)
	scope := e.base + "/.default"
	newCred := func(app, secret string) azcore.TokenCredential {
		return must(azidentity.NewClientSecretCredential(armsvc.TenantID, app, secret,
			&azidentity.ClientSecretCredentialOptions{ClientOptions: e.co, DisableInstanceDiscovery: true}))
	}

	// a client the directory doesn't know gets a token for the tenant
	c := e.token(t, newCred("some-app", "any"), scope)
	if c["aud"] != e.base || c["iss"] != "https://sts.windows.net/"+armsvc.TenantID+"/" || c["tid"] != armsvc.TenantID || c["appid"] != "some-app" || c["oid"] == "" || c["oid"] == nil {
		t.Fatalf("%v", c)
	}

	// a registered one gets its service principal's object ID
	_, app := e.call(t, "POST", "/v1.0/applications", map[string]any{"displayName": "tokens"})
	_, sp := e.call(t, "POST", "/v1.0/servicePrincipals", map[string]any{"appId": app["appId"]})
	_, pw := e.call(t, "POST", "/v1.0/applications/"+app["id"].(string)+"/addPassword", map[string]any{})
	c = e.token(t, newCred(app["appId"].(string), pw["secretText"].(string)), "https://management.azure.com/.default")
	if c["aud"] != "https://management.azure.com" || c["oid"] != sp["id"] || c["sub"] != sp["id"] || c["appid"] != app["appId"] || c["tid"] != armsvc.TenantID || c["ver"] != "1.0" {
		t.Fatalf("%v", c)
	}
	// a tenant named by GUID in the authority is the tenant of the token
	other := "11111111-2222-3333-4444-555555555555"
	cred := must(azidentity.NewClientSecretCredential(other, "some-app", "x", &azidentity.ClientSecretCredentialOptions{ClientOptions: e.co, DisableInstanceDiscovery: true}))
	if c := e.token(t, cred, scope); c["tid"] != other || c["iss"] != "https://sts.windows.net/"+other+"/" {
		t.Fatalf("%v", c)
	}
	// the discovery document names pogi's endpoints and the same JWKS
	_, doc := e.call(t, "GET", "/"+armsvc.TenantID+"/v2.0/.well-known/openid-configuration", nil)
	if doc["token_endpoint"] != e.base+"/"+armsvc.TenantID+"/oauth2/v2.0/token" || doc["jwks_uri"] != e.base+"/"+armsvc.TenantID+"/discovery/v2.0/keys" {
		t.Fatalf("%v", doc)
	}
}

// the IMDS and App Service token endpoints, through azidentity.
func TestManagedIdentityCredential(t *testing.T) {
	e := start(t)
	cred := must(azidentity.NewClientSecretCredential(armsvc.TenantID, "app", "secret", &azidentity.ClientSecretCredentialOptions{ClientOptions: e.co, DisableInstanceDiscovery: true}))
	opts := &arm.ClientOptions{ClientOptions: e.co}
	must(must(armresources.NewResourceGroupsClient(core.AzureSubscription, cred, opts)).CreateOrUpdate(ctx, "ids", armresources.ResourceGroup{Location: to.Ptr("eastus")}, nil))
	ident := must(must(armmsi.NewUserAssignedIdentitiesClient(core.AzureSubscription, cred, opts)).CreateOrUpdate(ctx, "ids", "worker", armmsi.Identity{Location: to.Ptr("eastus")}, nil)).Identity
	clientID, principalID := *ident.Properties.ClientID, *ident.Properties.PrincipalID

	// each source is asked for the system-assigned identity, a user-assigned one by client ID, and one that doesn't exist
	// azidentity has no override for the IMDS address, 169.254.169.254, so that one is redirected by the transport.
	imds := &http.Client{Transport: roundTripper(func(r *http.Request) (*http.Response, error) {
		if r.URL.Host == "169.254.169.254" {
			r = r.Clone(r.Context())
			r.URL.Scheme, r.URL.Host = "https", strings.TrimPrefix(e.base, "https://")
		}
		return e.http.Transport.RoundTrip(r)
	})}
	sources := map[string]func(t *testing.T) azcore.ClientOptions{
		"app service": func(t *testing.T) azcore.ClientOptions {
			t.Setenv("IDENTITY_ENDPOINT", e.base+"/msi/token")
			t.Setenv("IDENTITY_HEADER", "secret-header")
			return e.co
		},
		"imds": func(t *testing.T) azcore.ClientOptions {
			t.Setenv("IDENTITY_ENDPOINT", "") // any other source in the environment would win
			t.Setenv("IDENTITY_HEADER", "")
			t.Setenv("MSI_ENDPOINT", "")
			t.Setenv("IMDS_ENDPOINT", "")
			co := e.co
			co.Transport = imds
			return co
		},
	}
	for name, setup := range sources {
		t.Run(name, func(t *testing.T) {
			co := setup(t)
			get := func(id azidentity.ManagedIDKind) (map[string]any, error) {
				mc, err := azidentity.NewManagedIdentityCredential(&azidentity.ManagedIdentityCredentialOptions{ClientOptions: co, ID: id})
				if err != nil {
					t.Fatal(err)
				}
				tok, err := mc.GetToken(ctx, policy.TokenRequestOptions{Scopes: []string{"https://management.azure.com/.default"}})
				if err != nil {
					return nil, err
				}
				return e.claims(t, tok.Token), nil
			}
			sys, err := get(nil)
			if err != nil {
				t.Fatal(err)
			}
			if sys["aud"] != "https://management.azure.com" || sys["tid"] != armsvc.TenantID || sys["oid"] == "" || sys["xms_mirid"] != nil {
				t.Fatalf("%v", sys)
			}
			user, err := get(azidentity.ClientID(clientID))
			if err != nil {
				t.Fatal(err)
			}
			if user["oid"] != principalID || user["appid"] != clientID || user["xms_mirid"] != *ident.ID || user["tid"] != armsvc.TenantID || user["oid"] == sys["oid"] {
				t.Fatalf("%v", user)
			}
			if _, err := get(azidentity.ClientID("00000000-0000-0000-0000-00000000dead")); err == nil {
				t.Fatal("an unknown identity got a token")
			}
		})
	}
}

// Microsoft.ManagedIdentity/userAssignedIdentities in the Resource Manager, through the official client.
func TestUserAssignedIdentityProvider(t *testing.T) {
	e := start(t)
	cred := must(azidentity.NewClientSecretCredential(armsvc.TenantID, "app", "secret", &azidentity.ClientSecretCredentialOptions{ClientOptions: e.co, DisableInstanceDiscovery: true}))
	opts := &arm.ClientOptions{ClientOptions: e.co}
	must(must(armresources.NewResourceGroupsClient(core.AzureSubscription, cred, opts)).CreateOrUpdate(ctx, "ids", armresources.ResourceGroup{Location: to.Ptr("eastus")}, nil))
	c := must(armmsi.NewUserAssignedIdentitiesClient(core.AzureSubscription, cred, opts))

	created := must(c.CreateOrUpdate(ctx, "ids", "worker", armmsi.Identity{Location: to.Ptr("westus2"), Tags: map[string]*string{"a": to.Ptr("b")}}, nil))
	p := created.Properties
	if *created.Name != "worker" || *created.Type != "Microsoft.ManagedIdentity/userAssignedIdentities" || *created.Location != "westus2" || *created.Tags["a"] != "b" ||
		*created.ID != "/subscriptions/"+core.AzureSubscription+"/resourceGroups/ids/providers/Microsoft.ManagedIdentity/userAssignedIdentities/worker" {
		t.Fatalf("%+v", created)
	}
	for _, id := range []*string{p.ClientID, p.PrincipalID, p.TenantID} {
		if id == nil || *id == "" {
			t.Fatalf("%+v", p)
		}
	}
	if *p.TenantID != armsvc.TenantID || *p.ClientID == *p.PrincipalID {
		t.Fatalf("%+v", p)
	}
	// a second PUT updates it and keeps its IDs: they are what tokens were issued to
	again := must(c.CreateOrUpdate(ctx, "ids", "worker", armmsi.Identity{Location: to.Ptr("westus2"), Tags: map[string]*string{"a": to.Ptr("c")}}, nil))
	if *again.Properties.ClientID != *p.ClientID || *again.Properties.PrincipalID != *p.PrincipalID || *again.Tags["a"] != "c" {
		t.Fatalf("%+v", again)
	}
	if got := must(c.Get(ctx, "ids", "worker", nil)); *got.Properties.ClientID != *p.ClientID {
		t.Fatalf("%+v", got)
	}
	must(c.CreateOrUpdate(ctx, "ids", "other", armmsi.Identity{Location: to.Ptr("eastus")}, nil))
	var names []string
	for pg := c.NewListByResourceGroupPager("ids", nil); pg.More(); {
		for _, i := range must(pg.NextPage(ctx)).Value {
			names = append(names, *i.Name)
		}
	}
	if strings.Join(names, ",") != "other,worker" {
		t.Fatal(names)
	}
	names = nil
	for pg := c.NewListBySubscriptionPager(nil); pg.More(); {
		for _, i := range must(pg.NextPage(ctx)).Value {
			names = append(names, *i.Name)
		}
	}
	if strings.Join(names, ",") != "other,worker" {
		t.Fatal(names)
	}
	must(c.Delete(ctx, "ids", "worker", nil))
	if _, err := c.Get(ctx, "ids", "worker", nil); err == nil {
		t.Fatal("still there")
	}
}

// password (ROPC), refresh token and device code, with the official clients and raw requests.
func TestUserGrants(t *testing.T) {
	e := start(t)
	st, _ := e.call(t, "POST", "/v1.0/users", map[string]any{"userPrincipalName": "ann@pogi.onmicrosoft.com", "displayName": "Ann",
		"passwordProfile": map[string]any{"password": "Sup3r-secret"}})
	if st != 201 {
		t.Fatal(st)
	}
	graphScope := "https://graph.microsoft.com/User.Read"

	t.Run("password", func(t *testing.T) {
		cred := must(azidentity.NewUsernamePasswordCredential(armsvc.TenantID, "client-1", "ann@pogi.onmicrosoft.com", "Sup3r-secret",
			&azidentity.UsernamePasswordCredentialOptions{ClientOptions: e.co, DisableInstanceDiscovery: true}))
		c := e.token(t, cred, graphScope)
		if c["aud"] != "https://graph.microsoft.com" || c["upn"] != "ann@pogi.onmicrosoft.com" || c["appid"] != "client-1" || c["tid"] != armsvc.TenantID || !strings.Contains(c["scp"].(string), "User.Read") {
			t.Fatalf("%v", c)
		}
		bad := must(azidentity.NewUsernamePasswordCredential(armsvc.TenantID, "client-1", "ann@pogi.onmicrosoft.com", "wrong",
			&azidentity.UsernamePasswordCredentialOptions{ClientOptions: e.co, DisableInstanceDiscovery: true}))
		if _, err := bad.GetToken(ctx, policy.TokenRequestOptions{Scopes: []string{graphScope}}); err == nil || !strings.Contains(err.Error(), "AADSTS50126") {
			t.Fatalf("wrong password: %v", err)
		}
	})

	t.Run("refresh token", func(t *testing.T) {
		form := url.Values{"grant_type": {"password"}, "client_id": {"client-1"}, "username": {"ann@pogi.onmicrosoft.com"}, "password": {"Sup3r-secret"},
			"scope": {"openid offline_access " + graphScope}}
		st, first := e.call(t, "POST", "/"+armsvc.TenantID+"/oauth2/v2.0/token", form)
		rt, _ := first["refresh_token"].(string)
		if st != 200 || rt == "" || first["id_token"] == nil {
			t.Fatalf("%d %v", st, first)
		}
		st, next := e.call(t, "POST", "/"+armsvc.TenantID+"/oauth2/v2.0/token", url.Values{"grant_type": {"refresh_token"}, "client_id": {"client-1"}, "refresh_token": {rt}})
		if st != 200 {
			t.Fatalf("%d %v", st, next)
		}
		c := e.claims(t, next["access_token"].(string))
		if c["upn"] != "ann@pogi.onmicrosoft.com" || c["aud"] != "https://graph.microsoft.com" || c["oid"] != e.claims(t, first["access_token"].(string))["oid"] {
			t.Fatalf("%v", c)
		}
		st, bad := e.call(t, "POST", "/"+armsvc.TenantID+"/oauth2/v2.0/token", url.Values{"grant_type": {"refresh_token"}, "client_id": {"client-1"}, "refresh_token": {"nope"}})
		if st != 400 || bad["error"] != "invalid_grant" {
			t.Fatalf("%d %v", st, bad)
		}
	})

	t.Run("device code", func(t *testing.T) {
		var prompt azidentity.DeviceCodeMessage
		cred := must(azidentity.NewDeviceCodeCredential(&azidentity.DeviceCodeCredentialOptions{ClientOptions: e.co, TenantID: armsvc.TenantID, ClientID: "client-2",
			DisableInstanceDiscovery: true, UserPrompt: func(_ context.Context, m azidentity.DeviceCodeMessage) error { prompt = m; return nil }}))
		c := e.token(t, cred, graphScope)
		if prompt.UserCode == "" || !strings.HasPrefix(prompt.VerificationURL, e.base) {
			t.Fatalf("%+v", prompt)
		}
		if c["upn"] == "" || c["upn"] == nil || c["appid"] != "client-2" || c["aud"] != "https://graph.microsoft.com" {
			t.Fatalf("%v", c)
		}
		// a device code that wasn't issued by pogi is expired
		st, bad := e.call(t, "POST", "/"+armsvc.TenantID+"/oauth2/v2.0/token", url.Values{"grant_type": {"urn:ietf:params:oauth:grant-type:device_code"}, "client_id": {"client-2"}, "device_code": {"bogus"}})
		if st != 400 || bad["error"] != "expired_token" {
			t.Fatalf("%d %v", st, bad)
		}
	})
}
