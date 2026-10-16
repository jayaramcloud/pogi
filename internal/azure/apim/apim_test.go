package apim_test

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/cloud"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/apimanagement/armapimanagement/v2"
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

const sub, rg, svc = core.AzureSubscription, "apim-rg", "shop"

// seen is what the backend last received.
type seen struct {
	Method, Path, Query, Body string
	Header                    http.Header
}

type env struct {
	t    *testing.T
	srv  *pogi.Running
	co   azcore.ClientOptions
	cred azcore.TokenCredential
	opts *arm.ClientOptions

	mu      sync.Mutex
	last    seen
	backend *httptest.Server
}

// start runs pogi with the resource manager, Entra and API Management, a backend, and a service "shop" in a resource group.
func start(t *testing.T) *env {
	t.Helper()
	srv := pogi.Start(t, pogi.Config{Services: []string{"arm", "entra", "apim"}, CORSOrigins: []string{"https://app.example.test"}})
	base := "https://localhost:" + srv.URL[strings.LastIndexByte(srv.URL, ':')+1:]
	ca, _, _ := srv.Hub.CertPaths()
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(must(os.ReadFile(ca)))
	hc := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, ServerName: "localhost"}}}
	co := azcore.ClientOptions{Transport: hc, Cloud: cloud.Configuration{ActiveDirectoryAuthorityHost: base + "/",
		Services: map[cloud.ServiceName]cloud.ServiceConfiguration{cloud.ResourceManager: {Endpoint: base, Audience: base}}}}
	e := &env{t: t, srv: srv, co: co, opts: &arm.ClientOptions{ClientOptions: co}}
	e.cred = must(azidentity.NewClientSecretCredential(armsvc.TenantID, "app", "secret", &azidentity.ClientSecretCredentialOptions{ClientOptions: co, DisableInstanceDiscovery: true}))
	e.backend = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		e.mu.Lock()
		e.last = seen{r.Method, r.URL.Path, r.URL.RawQuery, string(b), r.Header.Clone()}
		e.mu.Unlock()
		w.Header().Set("X-Backend", "yes")
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"path": r.URL.Path})
	}))
	t.Cleanup(e.backend.Close)

	must(armresources.NewResourceGroupsClient(sub, e.cred, e.opts)).CreateOrUpdate(ctx, rg, armresources.ResourceGroup{Location: to.Ptr("eastus")}, nil)
	p := must(e.services().BeginCreateOrUpdate(ctx, rg, svc, armapimanagement.ServiceResource{Location: to.Ptr("eastus"),
		SKU:        &armapimanagement.ServiceSKUProperties{Name: to.Ptr(armapimanagement.SKUTypeDeveloper), Capacity: to.Ptr(int32(1))},
		Properties: &armapimanagement.ServiceProperties{PublisherEmail: to.Ptr("ops@example.test"), PublisherName: to.Ptr("ops")}}, nil))
	must(p.PollUntilDone(ctx, nil))
	return e
}

func (e *env) services() *armapimanagement.ServiceClient {
	return must(armapimanagement.NewServiceClient(sub, e.cred, e.opts))
}
func (e *env) apis() *armapimanagement.APIClient {
	return must(armapimanagement.NewAPIClient(sub, e.cred, e.opts))
}
func (e *env) ops() *armapimanagement.APIOperationClient {
	return must(armapimanagement.NewAPIOperationClient(sub, e.cred, e.opts))
}
func (e *env) subs() *armapimanagement.SubscriptionClient {
	return must(armapimanagement.NewSubscriptionClient(sub, e.cred, e.opts))
}

func xml(inbound string) armapimanagement.PolicyContract {
	return armapimanagement.PolicyContract{Properties: &armapimanagement.PolicyContractProperties{Format: to.Ptr(armapimanagement.PolicyContentFormatXML),
		Value: to.Ptr("<policies><inbound>" + inbound + "</inbound><backend><base/></backend><outbound><base/></outbound><on-error><base/></on-error></policies>")}}
}

// api creates an API of the service at path; it needs no key unless keyed.
func (e *env) api(id, path, serviceURL string, keyed bool) {
	e.t.Helper()
	p := must(e.apis().BeginCreateOrUpdate(ctx, rg, svc, id, armapimanagement.APICreateOrUpdateParameter{Properties: &armapimanagement.APICreateOrUpdateProperties{
		DisplayName: to.Ptr(id), Path: to.Ptr(path), ServiceURL: to.Ptr(serviceURL), SubscriptionRequired: to.Ptr(keyed),
		Protocols: []*armapimanagement.Protocol{to.Ptr(armapimanagement.ProtocolHTTPS)}}}, nil))
	must(p.PollUntilDone(ctx, nil))
}

func (e *env) op(api, id, method, template string) {
	e.t.Helper()
	must(e.ops().CreateOrUpdate(ctx, rg, svc, api, id, armapimanagement.OperationContract{Properties: &armapimanagement.OperationContractProperties{
		DisplayName: to.Ptr(id), Method: to.Ptr(method), URLTemplate: to.Ptr(template)}}, nil))
}

func (e *env) apiPolicy(api string, p armapimanagement.PolicyContract) {
	e.t.Helper()
	must(must(armapimanagement.NewAPIPolicyClient(sub, e.cred, e.opts)).CreateOrUpdate(ctx, rg, svc, api, armapimanagement.PolicyIDNamePolicy, p, nil))
}

func (e *env) opPolicy(api, op string, p armapimanagement.PolicyContract) {
	e.t.Helper()
	// the operation policy client is the one the SDK keeps for it
	must(must(armapimanagement.NewAPIOperationPolicyClient(sub, e.cred, e.opts)).CreateOrUpdate(ctx, rg, svc, api, op, armapimanagement.PolicyIDNamePolicy, p, nil))
}

// call goes through the gateway: by host name, or by path when pathForm is set.
func (e *env) call(method, path, key string, pathForm bool, hdr ...string) (*http.Response, string) {
	e.t.Helper()
	port := e.srv.URL[strings.LastIndexByte(e.srv.URL, ':')+1:]
	u := e.srv.URL + path
	if pathForm {
		u = e.srv.URL + "/_azure/apim/" + svc + path
	}
	req, _ := http.NewRequest(method, u, nil)
	if !pathForm {
		req.Host = svc + ".azure-api.localhost:" + port
	}
	if key != "" {
		req.Header.Set("Ocp-Apim-Subscription-Key", key)
	}
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Add(hdr[i], hdr[i+1])
	}
	res := must(http.DefaultClient.Do(req))
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	return res, string(b)
}

func (e *env) lastSeen() seen {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.last
}

func status(t *testing.T, res *http.Response, body string, want int) {
	t.Helper()
	if res.StatusCode != want {
		t.Fatalf("status = %d %s, want %d", res.StatusCode, body, want)
	}
}

func errCode(err error) string {
	var re *azcore.ResponseError
	if err != nil && !asResponseError(err, &re) {
		return err.Error()
	}
	if re == nil {
		return ""
	}
	return re.ErrorCode
}

func asResponseError(err error, re **azcore.ResponseError) bool {
	for err != nil {
		if r, ok := err.(*azcore.ResponseError); ok {
			*re = r
			return true
		}
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}

// The management plane through the SDK: create, get, list, update, delete, with tags and keys, and the typed not found.
func TestManagementPlane(t *testing.T) {
	e := start(t)

	got := must(e.services().Get(ctx, rg, svc, nil))
	if g := *got.Properties.GatewayURL; !strings.Contains(g, svc+".azure-api") && !strings.Contains(g, "/_azure/apim/"+svc) {
		t.Fatalf("gatewayUrl = %q", g)
	}
	if *got.Properties.ProvisioningState != "Succeeded" || *got.SKU.Name != armapimanagement.SKUTypeDeveloper {
		t.Fatalf("service = %+v %+v", got.Properties, got.SKU)
	}
	n := 0
	for pg := e.services().NewListByResourceGroupPager(rg, nil); pg.More(); {
		n += len(must(pg.NextPage(ctx)).Value)
	}
	if n != 1 {
		t.Fatalf("services in the group = %d", n)
	}

	// an API with operations, a schema, and an update
	e.api("orders", "orders", e.backend.URL, true)
	e.op("orders", "get-order", "GET", "/{id}")
	e.op("orders", "list", "GET", "/")
	if op := must(e.ops().Get(ctx, rg, svc, "orders", "get-order", nil)); *op.Properties.URLTemplate != "/{id}" || *op.Properties.Method != "GET" {
		t.Fatalf("operation = %+v", op.Properties)
	}
	n = 0
	for pg := e.ops().NewListByAPIPager(rg, svc, "orders", nil); pg.More(); {
		n += len(must(pg.NextPage(ctx)).Value)
	}
	if n != 2 {
		t.Fatalf("operations = %d", n)
	}
	e.api("orders", "orders", e.backend.URL+"/v2", true) // update
	if a := must(e.apis().Get(ctx, rg, svc, "orders", nil)); *a.Properties.ServiceURL != e.backend.URL+"/v2" || *a.Properties.Path != "orders" {
		t.Fatalf("api = %+v", a.Properties)
	}
	sp := must(must(armapimanagement.NewAPISchemaClient(sub, e.cred, e.opts)).BeginCreateOrUpdate(ctx, rg, svc, "orders", "s1", armapimanagement.SchemaContract{
		Properties: &armapimanagement.SchemaContractProperties{ContentType: to.Ptr("application/vnd.oai.openapi.components+json"),
			Document: &armapimanagement.SchemaDocumentProperties{Value: to.Ptr(`{"definitions":{}}`)}}}, nil))
	must(sp.PollUntilDone(ctx, nil))

	// policies at the service, API and operation levels
	polc := must(armapimanagement.NewPolicyClient(sub, e.cred, e.opts))
	must(polc.CreateOrUpdate(ctx, rg, svc, armapimanagement.PolicyIDNamePolicy, xml(""), nil))
	if p := must(polc.Get(ctx, rg, svc, armapimanagement.PolicyIDNamePolicy, nil)); !strings.Contains(*p.Properties.Value, "<policies>") {
		t.Fatalf("policy = %+v", p.Properties)
	}
	_, err := polc.CreateOrUpdate(ctx, rg, svc, armapimanagement.PolicyIDNamePolicy, armapimanagement.PolicyContract{Properties: &armapimanagement.PolicyContractProperties{Value: to.Ptr("<inbound>")}}, nil)
	if errCode(err) != "ValidationError" {
		t.Fatalf("an invalid policy document: %v", err)
	}
	e.apiPolicy("orders", xml(`<base/>`))
	e.opPolicy("orders", "get-order", xml(`<base/>`))

	// backends, named values, products with their APIs and policy
	bc := must(armapimanagement.NewBackendClient(sub, e.cred, e.opts))
	must(bc.CreateOrUpdate(ctx, rg, svc, "be", armapimanagement.BackendContract{Properties: &armapimanagement.BackendContractProperties{
		URL: to.Ptr(e.backend.URL), Protocol: to.Ptr(armapimanagement.BackendProtocolHTTP)}}, nil))
	if b := must(bc.Get(ctx, rg, svc, "be", nil)); *b.Properties.URL != e.backend.URL {
		t.Fatalf("backend = %+v", b.Properties)
	}
	nvc := must(armapimanagement.NewNamedValueClient(sub, e.cred, e.opts))
	nv := must(nvc.BeginCreateOrUpdate(ctx, rg, svc, "token", armapimanagement.NamedValueCreateContract{Properties: &armapimanagement.NamedValueCreateContractProperties{
		DisplayName: to.Ptr("token"), Value: to.Ptr("s3cret"), Secret: to.Ptr(true)}}, nil))
	must(nv.PollUntilDone(ctx, nil))
	if g := must(nvc.Get(ctx, rg, svc, "token", nil)); g.Properties.Value != nil {
		t.Fatalf("a secret named value shows its value: %v", *g.Properties.Value)
	}
	if v := must(nvc.ListValue(ctx, rg, svc, "token", nil)); *v.Value != "s3cret" {
		t.Fatalf("listValue = %v", v.Value)
	}
	pc := must(armapimanagement.NewProductClient(sub, e.cred, e.opts))
	must(pc.CreateOrUpdate(ctx, rg, svc, "gold", armapimanagement.ProductContract{Properties: &armapimanagement.ProductContractProperties{
		DisplayName: to.Ptr("Gold"), State: to.Ptr(armapimanagement.ProductStatePublished)}}, nil))
	pac := must(armapimanagement.NewProductAPIClient(sub, e.cred, e.opts))
	if a := must(pac.CreateOrUpdate(ctx, rg, svc, "gold", "orders", nil)); *a.Name != "orders" {
		t.Fatalf("product api = %+v", a)
	}
	n = 0
	for pg := pac.NewListByProductPager(rg, svc, "gold", nil); pg.More(); {
		n += len(must(pg.NextPage(ctx)).Value)
	}
	if n != 1 {
		t.Fatalf("product apis = %d", n)
	}
	if _, err := pac.CreateOrUpdate(ctx, rg, svc, "gold", "nope", nil); errCode(err) != "ResourceNotFound" {
		t.Fatalf("linking a missing API: %v", err)
	}
	must(must(armapimanagement.NewProductPolicyClient(sub, e.cred, e.opts)).CreateOrUpdate(ctx, rg, svc, "gold", armapimanagement.PolicyIDNamePolicy, xml(`<base/>`), nil))

	// subscriptions: keys come from listSecrets, and regenerate changes them
	sc := e.subs()
	made := must(sc.CreateOrUpdate(ctx, rg, svc, "sub1", armapimanagement.SubscriptionCreateParameters{Properties: &armapimanagement.SubscriptionCreateParameterProperties{
		DisplayName: to.Ptr("Sub 1"), Scope: to.Ptr("/products/gold"), State: to.Ptr(armapimanagement.SubscriptionStateActive)}}, nil))
	if made.Properties.PrimaryKey != nil {
		t.Fatal("a subscription shows its key")
	}
	keys := must(sc.ListSecrets(ctx, rg, svc, "sub1", nil))
	if len(*keys.PrimaryKey) < 16 || *keys.PrimaryKey == *keys.SecondaryKey {
		t.Fatalf("keys = %v %v", *keys.PrimaryKey, *keys.SecondaryKey)
	}
	must(sc.RegeneratePrimaryKey(ctx, rg, svc, "sub1", nil))
	if again := must(sc.ListSecrets(ctx, rg, svc, "sub1", nil)); *again.PrimaryKey == *keys.PrimaryKey || *again.SecondaryKey != *keys.SecondaryKey {
		t.Fatalf("regenerated keys = %v %v", *again.PrimaryKey, *again.SecondaryKey)
	}
	n = 0
	for pg := sc.NewListPager(rg, svc, nil); pg.More(); {
		n += len(must(pg.NextPage(ctx)).Value) // sub1 and the built-in master
	}
	if n != 2 {
		t.Fatalf("subscriptions = %d", n)
	}

	// deleting: an API goes out of its products; then everything answers with the typed not found
	must(e.apis().Delete(ctx, rg, svc, "orders", "*", nil))
	if _, err := e.apis().Get(ctx, rg, svc, "orders", nil); errCode(err) != "ResourceNotFound" {
		t.Fatalf("deleted API: %v", err)
	}
	if _, err := e.ops().Get(ctx, rg, svc, "orders", "list", nil); errCode(err) != "ResourceNotFound" {
		t.Fatalf("operation of a deleted API: %v", err)
	}
	n = 0
	for pg := pac.NewListByProductPager(rg, svc, "gold", nil); pg.More(); {
		n += len(must(pg.NextPage(ctx)).Value)
	}
	if n != 0 {
		t.Fatalf("product apis after the API was deleted = %d", n)
	}
	must(sc.Delete(ctx, rg, svc, "sub1", "*", nil))
	if _, err := sc.Get(ctx, rg, svc, "sub1", nil); errCode(err) != "ResourceNotFound" {
		t.Fatalf("deleted subscription: %v", err)
	}
	dp := must(e.services().BeginDelete(ctx, rg, svc, nil))
	must(dp.PollUntilDone(ctx, nil))
	if _, err := e.services().Get(ctx, rg, svc, nil); errCode(err) != "ResourceNotFound" {
		t.Fatalf("deleted service: %v", err)
	}
}

// A request finds its API and operation, passes the key check, runs the policies of every level in order, and reaches the
// backend with set-header and rewrite-uri applied.
func TestGatewayRoutesWithPolicies(t *testing.T) {
	e := start(t)
	e.api("shop", "shop", e.backend.URL+"/v2", true)
	e.op("shop", "get-item", "GET", "/items/{id}")
	e.op("shop", "add-item", "POST", "/items")
	pc := must(armapimanagement.NewPolicyClient(sub, e.cred, e.opts))
	must(pc.CreateOrUpdate(ctx, rg, svc, armapimanagement.PolicyIDNamePolicy, xml(
		`<set-header name="X-Order" exists-action="append"><value>global</value></set-header>
		 <set-header name="X-Token" exists-action="override"><value>{{token}}</value></set-header>`), nil))
	nv := must(must(armapimanagement.NewNamedValueClient(sub, e.cred, e.opts)).BeginCreateOrUpdate(ctx, rg, svc, "token", armapimanagement.NamedValueCreateContract{
		Properties: &armapimanagement.NamedValueCreateContractProperties{DisplayName: to.Ptr("token"), Value: to.Ptr("abc"), Secret: to.Ptr(true)}}, nil))
	must(nv.PollUntilDone(ctx, nil))
	// a product subscription also runs the product's policy
	prods := must(armapimanagement.NewProductClient(sub, e.cred, e.opts))
	must(prods.CreateOrUpdate(ctx, rg, svc, "gold", armapimanagement.ProductContract{Properties: &armapimanagement.ProductContractProperties{DisplayName: to.Ptr("Gold")}}, nil))
	must(must(armapimanagement.NewProductAPIClient(sub, e.cred, e.opts)).CreateOrUpdate(ctx, rg, svc, "gold", "shop", nil))
	must(must(armapimanagement.NewProductPolicyClient(sub, e.cred, e.opts)).CreateOrUpdate(ctx, rg, svc, "gold", armapimanagement.PolicyIDNamePolicy, xml(
		`<base/><set-header name="X-Order" exists-action="append"><value>product</value></set-header>`), nil))
	e.apiPolicy("shop", xml(`<base/><set-header name="X-Order" exists-action="append"><value>api</value></set-header>`))
	e.opPolicy("shop", "get-item", xml(`<base/>
		<set-header name="X-Order" exists-action="append"><value>operation</value></set-header>
		<set-query-parameter name="debug" exists-action="override"><value>1</value></set-query-parameter>
		<rewrite-uri template="/things/{id}"/>`))
	// this operation's policy has no <base/>: nothing of the levels above runs
	e.opPolicy("shop", "add-item", xml(`<set-header name="X-Order" exists-action="override"><value>alone</value></set-header>
		<set-body>@(context.Request.Headers.GetValueOrDefault("X", "x"))</set-body>`))
	// the outbound pipeline: set-header on the response
	e.apiPolicy("shop", armapimanagement.PolicyContract{Properties: &armapimanagement.PolicyContractProperties{Format: to.Ptr(armapimanagement.PolicyContentFormatXML),
		Value: to.Ptr(`<policies><inbound><base/><set-header name="X-Order" exists-action="append"><value>api</value></set-header></inbound><backend><base/></backend>
			<outbound><base/><set-header name="X-Backend" exists-action="delete"/><set-header name="X-Gateway" exists-action="override"><value>apim</value></set-header></outbound></policies>`)}})

	subs := e.subs()
	must(subs.CreateOrUpdate(ctx, rg, svc, "gold-sub", armapimanagement.SubscriptionCreateParameters{Properties: &armapimanagement.SubscriptionCreateParameterProperties{
		DisplayName: to.Ptr("gold"), Scope: to.Ptr("/products/gold"), State: to.Ptr(armapimanagement.SubscriptionStateActive)}}, nil))
	goldKey := *must(subs.ListSecrets(ctx, rg, svc, "gold-sub", nil)).PrimaryKey
	master := *must(subs.ListSecrets(ctx, rg, svc, "master", nil)).PrimaryKey

	// no key, a wrong key: 401
	for _, key := range []string{"", "nope"} {
		res, body := e.call("GET", "/shop/items/7", key, false)
		status(t, res, body, 401)
		if !strings.Contains(body, "subscription key") {
			t.Fatalf("401 body = %s", body)
		}
	}
	// the master key (all APIs) runs global, API and operation policies, in that order, with rewrite-uri and the query
	// parameter applied; the host form and the path form are the same gateway
	for _, pathForm := range []bool{false, true} {
		res, body := e.call("GET", "/shop/items/7?x=1", master, pathForm)
		status(t, res, body, 200)
		got := e.lastSeen()
		if got.Path != "/v2/things/7" || got.Query != "debug=1&x=1" {
			t.Fatalf("the backend saw %s ?%s", got.Path, got.Query)
		}
		if h := got.Header.Values("X-Order"); strings.Join(h, ",") != "global,api,operation" || got.Header.Get("X-Token") != "abc" {
			t.Fatalf("the backend saw X-Order=%v X-Token=%q", h, got.Header.Get("X-Token"))
		}
		if res.Header.Get("X-Gateway") != "apim" || res.Header.Get("X-Backend") != "" || !strings.Contains(body, "/v2/things/7") {
			t.Fatalf("response: %v %s", res.Header, body)
		}
	}
	// the key goes in the query as well, and a product key adds the product's policy
	res, body := e.call("GET", "/shop/items/9?subscription-key="+goldKey, "", false)
	status(t, res, body, 200)
	if h := e.lastSeen().Header.Values("X-Order"); strings.Join(h, ",") != "global,product,api,operation" {
		t.Fatalf("X-Order = %v", h)
	}
	// an operation policy without <base/> replaces the levels above, and set-body writes the request body
	res, body = e.call("POST", "/shop/items", master, false, "X", "7")
	status(t, res, body, 200)
	if got := e.lastSeen(); got.Method != "POST" || got.Path != "/v2/items" || got.Body != "7" || strings.Join(got.Header.Values("X-Order"), ",") != "alone" {
		t.Fatalf("the backend saw %+v", got)
	}
	// unknown paths and operations are 404, and a suspended subscription is refused
	for _, p := range []string{"/nowhere", "/shop/other"} {
		res, body = e.call("GET", p, master, false)
		status(t, res, body, 404)
	}
	must(subs.CreateOrUpdate(ctx, rg, svc, "gold-sub", armapimanagement.SubscriptionCreateParameters{Properties: &armapimanagement.SubscriptionCreateParameterProperties{
		DisplayName: to.Ptr("gold"), Scope: to.Ptr("/products/gold"), State: to.Ptr(armapimanagement.SubscriptionStateSuspended)}}, nil))
	res, body = e.call("GET", "/shop/items/9", goldKey, false)
	status(t, res, body, 401)
	// an API that is not in the product is out of the product key's scope
	e.api("open", "open", e.backend.URL, false)
	e.op("open", "all", "GET", "/*")
	res, body = e.call("GET", "/open/a/b", "", false) // no key needed there
	status(t, res, body, 200)
	if e.lastSeen().Path != "/a/b" {
		t.Fatalf("wildcard operation forwarded %s", e.lastSeen().Path)
	}
}

func TestRateLimit(t *testing.T) {
	e := start(t)
	e.api("shop", "shop", e.backend.URL, true)
	e.op("shop", "hello", "GET", "/hello")
	e.apiPolicy("shop", xml(`<rate-limit calls="3" renewal-period="60"/>
		<rate-limit-by-key calls="1" renewal-period="60" counter-key="@(context.Request.Headers.GetValueOrDefault(&quot;X-User&quot;,&quot;anon&quot;))"/>`))
	master := *must(e.subs().ListSecrets(ctx, rg, svc, "master", nil)).PrimaryKey
	// the by-key limit lets one call per user through: alice, bob, then alice is over, while the per-subscription limit is at 3
	for i, c := range []struct {
		user string
		want int
	}{{"alice", 200}, {"bob", 200}, {"alice", 429}} {
		res, body := e.call("GET", "/shop/hello", master, false, "X-User", c.user)
		if res.StatusCode != c.want {
			t.Fatalf("call %d (%s) = %d %s, want %d", i, c.user, res.StatusCode, body, c.want)
		}
		if c.want == 429 && (res.Header.Get("Retry-After") == "" || !strings.Contains(body, "Rate limit is exceeded")) {
			t.Fatalf("429 = %v %s", res.Header, body)
		}
	}
	// the third call of the subscription was counted (the 429 of alice was refused by the by-key limit after it); the next is over
	res, body := e.call("GET", "/shop/hello", master, false, "X-User", "carol")
	status(t, res, body, 429)
}

// An Entra token from pogi passes validate-jwt through the openid-config of pogi's own tenant, a tampered one does not.
func TestValidateJWT(t *testing.T) {
	e := start(t)
	e.api("secure", "secure", e.backend.URL, false)
	e.op("secure", "get", "GET", "/data")
	port := e.srv.URL[strings.LastIndexByte(e.srv.URL, ':')+1:]
	configs := map[string]string{
		"pogi's address":     "http://127.0.0.1:" + port + "/" + armsvc.TenantID + "/v2.0/.well-known/openid-configuration",
		"Entra's own host":   "https://login.microsoftonline.com/" + armsvc.TenantID + "/v2.0/.well-known/openid-configuration",
		"an unreachable one": "http://127.0.0.1:1/nothing",
	}
	tok := must(e.cred.GetToken(ctx, policy.TokenRequestOptions{Scopes: []string{"api://pogi-secure/.default"}})).Token
	bad := tok[:len(tok)-4] + "AAAA"
	if bad == tok {
		bad = tok[:len(tok)-4] + "BBBB"
	}
	for name, cfg := range configs {
		e.apiPolicy("secure", xml(`<validate-jwt header-name="Authorization" failed-validation-httpcode="401" failed-validation-error-message="Unauthorized">
			<openid-config url="`+cfg+`"/><audiences><audience>api://pogi-secure</audience></audiences>
			<required-claims><claim name="tid" match="all"><value>`+armsvc.TenantID+`</value></claim></required-claims></validate-jwt>`))
		for _, c := range []struct {
			what, auth string
			want       int
		}{{"no token", "", 401}, {"garbage", "Bearer not.a.jwt", 401}, {"a bad signature", "Bearer " + bad, 401}, {"the scheme missing", tok, 401}, {"a good token", "Bearer " + tok, 200}} {
			if name == "an unreachable one" && c.want == 200 {
				c.want = 401 // the keys cannot be read: nothing passes
			}
			var hdr []string
			if c.auth != "" {
				hdr = []string{"Authorization", c.auth}
			}
			res, body := e.call("GET", "/secure/data", "", false, hdr...)
			if res.StatusCode != c.want {
				t.Fatalf("%s, %s: %d %s, want %d", name, c.what, res.StatusCode, body, c.want)
			}
			if c.want == 401 && !strings.Contains(res.Header.Get("WWW-Authenticate"), "Bearer") {
				t.Fatalf("%s, %s: no WWW-Authenticate header", name, c.what)
			}
		}
	}
	// another audience and an issuer the policy does not name are refused
	for _, in := range []string{`<audiences><audience>api://other</audience></audiences>`, `<issuers><issuer>https://example.test/</issuer></issuers>`} {
		e.apiPolicy("secure", xml(`<validate-jwt><openid-config url="`+configs["pogi's address"]+`"/>`+in+`</validate-jwt>`))
		res, body := e.call("GET", "/secure/data", "", false, "Authorization", "Bearer "+tok)
		status(t, res, body, 401)
	}
	// a symmetric key: an HS256 token signed with it passes
	e.apiPolicy("secure", xml(`<validate-jwt token-value="@(context.Request.Headers.GetValueOrDefault(&quot;X-Jwt&quot;,&quot;&quot;))"><issuer-signing-keys><key>c2VjcmV0LWtleQ==</key></issuer-signing-keys></validate-jwt>`))
	hs := hs256(`{"exp":9999999999}`, "secret-key")
	res, body := e.call("GET", "/secure/data", "", false, "X-Jwt", hs)
	status(t, res, body, 200)
	res, body = e.call("GET", "/secure/data", "", false, "X-Jwt", hs256(`{"exp":9999999999}`, "other-key"))
	status(t, res, body, 401)
	res, body = e.call("GET", "/secure/data", "", false, "X-Jwt", hs256(`{"exp":1}`, "secret-key"))
	status(t, res, body, 401)
}

// mock-response needs no backend; return-response, set-body, cors, and an expression pogi cannot evaluate.
func TestMockAndReturnResponse(t *testing.T) {
	e := start(t)
	e.api("mocks", "mocks", "", false) // no serviceUrl
	e.op("mocks", "mocked", "GET", "/pet")
	must(e.ops().CreateOrUpdate(ctx, rg, svc, "mocks", "mocked", armapimanagement.OperationContract{Properties: &armapimanagement.OperationContractProperties{
		DisplayName: to.Ptr("mocked"), Method: to.Ptr("GET"), URLTemplate: to.Ptr("/pet"),
		Responses: []*armapimanagement.ResponseContract{{StatusCode: to.Ptr(int32(200)), Representations: []*armapimanagement.RepresentationContract{{
			ContentType: to.Ptr("application/json"), Examples: map[string]*armapimanagement.ParameterExampleContract{"default": {Value: map[string]any{"name": "rex"}}}}}}}}}, nil))
	e.opPolicy("mocks", "mocked", xml(`<mock-response status-code="200" content-type="application/json"/>`))
	e.op("mocks", "teapot", "GET", "/teapot")
	e.opPolicy("mocks", "teapot", xml(`<base/><return-response><set-status code="418" reason="teapot"/><set-header name="X-Tea" exists-action="override"><value>earl grey</value></set-header>
		<set-body>@(context.Request.Headers.GetValueOrDefault("X-Name", "anon"))</set-body></return-response>`))
	e.op("mocks", "bad", "GET", "/bad")
	e.opPolicy("mocks", "bad", xml(`<set-body>@(DateTime.Now.ToString())</set-body>`))
	e.apiPolicy("mocks", xml(`<cors allow-credentials="false"><allowed-origins><origin>https://app.example.test</origin></allowed-origins>
		<allowed-methods preflight-result-max-age="300"><method>GET</method><method>POST</method></allowed-methods><allowed-headers><header>*</header></allowed-headers></cors>`))

	res, body := e.call("GET", "/mocks/pet", "", false)
	status(t, res, body, 200)
	if res.Header.Get("Content-Type") != "application/json" || strings.ReplaceAll(body, " ", "") != `{"name":"rex"}` {
		t.Fatalf("mock = %v %s", res.Header, body)
	}
	if got := e.lastSeen(); got.Method != "" {
		t.Fatalf("the backend was called: %+v", got)
	}
	res, body = e.call("GET", "/mocks/teapot", "", false, "X-Name", "ann", "Origin", "https://app.example.test")
	status(t, res, body, 418)
	if body != "ann" || res.Header.Get("X-Tea") != "earl grey" || res.Header.Get("Access-Control-Allow-Origin") != "https://app.example.test" {
		t.Fatalf("return-response = %v %q", res.Header, body)
	}
	res, body = e.call("GET", "/mocks/teapot", "", false, "Origin", "https://evil.example.test")
	if res.StatusCode != 403 || res.Header.Get("Access-Control-Allow-Origin") != "" { // refused at the front door, before the policy runs
		t.Fatalf("a foreign origin: %d %v %q", res.StatusCode, res.Header, body)
	}
	// a preflight has no operation of its own: the API's cors policy answers
	res, body = e.call("OPTIONS", "/mocks/teapot", "", false, "Origin", "https://app.example.test", "Access-Control-Request-Method", "POST")
	status(t, res, body, 200)
	if res.Header.Get("Access-Control-Allow-Origin") != "https://app.example.test" || res.Header.Get("Access-Control-Allow-Methods") != "GET, POST" || res.Header.Get("Access-Control-Max-Age") != "300" {
		t.Fatalf("preflight = %v", res.Header)
	}
	res, body = e.call("GET", "/mocks/bad", "", false)
	status(t, res, body, 500)
	if !strings.Contains(body, "not supported") {
		t.Fatalf("an expression pogi cannot evaluate: %s", body)
	}
}

// the services come to exist and go away with their resources, and the console sees them
func TestConsoleAndState(t *testing.T) {
	e := start(t)
	e.api("shop", "shop", e.backend.URL, true)
	svcs := e.srv.Hub.Service("apim")
	tabs := svcs.(core.Browsable).Resources()
	if len(tabs) != 3 || len(tabs[0].Rows) != 1 || len(tabs[1].Rows) != 1 {
		t.Fatalf("tables = %+v", tabs)
	}
	if got := svcs.(core.Inspectable).Inspect("APIs", tabs[1].Rows[0][0]); got == nil {
		t.Fatal("inspect found nothing")
	}
	b := must(svcs.(core.Stateful).Save())
	svcs.(core.Stateful).Reset()
	if r := svcs.(core.Browsable).Resources(); len(r[0].Rows) != 0 {
		t.Fatal("reset left the services")
	}
	if err := svcs.(core.Stateful).Load(b); err != nil {
		t.Fatal(err)
	}
	if r := svcs.(core.Browsable).Resources(); len(r[1].Rows) != 1 {
		t.Fatal("load lost the APIs")
	}
}

// hs256 makes an HS256 JWT with the claims (JSON) signed with key.
func hs256(claims, key string) string {
	enc := base64.RawURLEncoding.EncodeToString
	msg := enc([]byte(`{"alg":"HS256","typ":"JWT"}`)) + "." + enc([]byte(claims))
	m := hmac.New(sha256.New, []byte(key))
	m.Write([]byte(msg))
	return msg + "." + enc(m.Sum(nil))
}
