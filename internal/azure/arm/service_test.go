package arm_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	azarm "github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/cloud"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/resources/armresources"
	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/azure/arm"
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
	opts *azarm.ClientOptions
	cred azcore.TokenCredential
}

// start runs pogi with the Resource Manager and the sign-in services, and a
// client that trusts pogi's CA and signs in with client credentials.
func start(t *testing.T, extra ...string) *env {
	t.Helper()
	srv := pogi.Start(t, pogi.Config{Services: append([]string{"arm", "entra", "graph"}, extra...)})
	base := "https://localhost:" + srv.URL[strings.LastIndexByte(srv.URL, ':')+1:]
	ca, _, _ := srv.Hub.CertPaths()
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(must(os.ReadFile(ca)))
	hc := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, ServerName: "localhost"}}}
	co := azcore.ClientOptions{Transport: hc, Cloud: cloud.Configuration{ActiveDirectoryAuthorityHost: base + "/",
		Services: map[cloud.ServiceName]cloud.ServiceConfiguration{cloud.ResourceManager: {Endpoint: base, Audience: base}}}}
	cred := must(azidentity.NewClientSecretCredential(arm.TenantID, "app", "secret",
		&azidentity.ClientSecretCredentialOptions{ClientOptions: co, DisableInstanceDiscovery: true}))
	return &env{srv: srv, base: base, http: hc, opts: &azarm.ClientOptions{ClientOptions: co}, cred: cred}
}

func (e *env) factory(sub string) *armresources.ClientFactory {
	return must(armresources.NewClientFactory(sub, e.cred, e.opts))
}

// get fetches a path as JSON.
func (e *env) do(t *testing.T, method, path, body string) (int, map[string]any, http.Header) {
	t.Helper()
	req, _ := http.NewRequest(method, e.base+path, strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer x")
	req.Header.Set("Content-Type", "application/json")
	res, err := e.http.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	var out map[string]any
	json.Unmarshal(b, &out)
	return res.StatusCode, out, res.Header
}

func wantErr(t *testing.T, err error, status int, code string) {
	t.Helper()
	var re *azcore.ResponseError
	if !errors.As(err, &re) || re.StatusCode != status || re.ErrorCode != code {
		t.Fatalf("want %d %s, got %v", status, code, err)
	}
}

var sub = core.AzureSubscription

func TestResourceGroups(t *testing.T) {
	e := start(t)
	rgs := e.factory(sub).NewResourceGroupsClient()

	// the default group is there, and a new one is created, read, updated and listed
	if got := must(rgs.CheckExistence(ctx, core.AzureResourceGroup, nil)); !got.Success {
		t.Fatal("default group missing")
	}
	created := must(rgs.CreateOrUpdate(ctx, "demo", armresources.ResourceGroup{Location: to.Ptr("westeurope"), Tags: map[string]*string{"env": to.Ptr("test")}}, nil))
	if *created.Name != "demo" || *created.Location != "westeurope" || *created.Properties.ProvisioningState != "Succeeded" || *created.Tags["env"] != "test" {
		t.Fatalf("%+v", created)
	}
	if *created.ID != "/subscriptions/"+sub+"/resourceGroups/demo" {
		t.Fatal(*created.ID)
	}
	patched := must(rgs.Update(ctx, "demo", armresources.ResourceGroupPatchable{Tags: map[string]*string{"env": to.Ptr("prod"), "team": to.Ptr("x")}}, nil))
	if len(patched.Tags) != 2 || *patched.Tags["env"] != "prod" || *patched.Location != "westeurope" {
		t.Fatalf("%+v", patched)
	}
	if got := must(rgs.Get(ctx, "demo", nil)); *got.Tags["team"] != "x" {
		t.Fatal("tags lost")
	}
	_, err := rgs.CreateOrUpdate(ctx, "demo", armresources.ResourceGroup{Location: to.Ptr("eastus")}, nil)
	wantErr(t, err, 409, "InvalidResourceGroupLocation")
	_, err = rgs.CreateOrUpdate(ctx, "nolocation", armresources.ResourceGroup{}, nil)
	wantErr(t, err, 400, "LocationRequired")
	pager, names := rgs.NewListPager(nil), []string{}
	for pager.More() {
		for _, g := range must(pager.NextPage(ctx)).Value {
			names = append(names, *g.Name)
		}
	}
	if strings.Join(names, ",") != "demo,pogi" {
		t.Fatal(names)
	}

	// deleting waits for the long-running operation, and the group is gone
	poller := must(rgs.BeginDelete(ctx, "demo", nil))
	must(poller.PollUntilDone(ctx, &azruntimePoll))
	if got := must(rgs.CheckExistence(ctx, "demo", nil)); got.Success {
		t.Fatal("still there")
	}
	_, err = rgs.Get(ctx, "demo", nil)
	wantErr(t, err, 404, "ResourceGroupNotFound")
	if _, err = e.factory("11111111-1111-1111-1111-111111111111").NewResourceGroupsClient().Get(ctx, "pogi", nil); err == nil {
		t.Fatal("a new subscription has no groups")
	}
}

func TestGenericResources(t *testing.T) {
	e := start(t)
	f := e.factory(sub)
	must(f.NewResourceGroupsClient().CreateOrUpdate(ctx, "g", armresources.ResourceGroup{Location: to.Ptr("eastus")}, nil))
	c := f.NewClient()

	// no Provider for Microsoft.Widgets: the resource is stored as sent
	poller := must(c.BeginCreateOrUpdate(ctx, "g", "Microsoft.Widgets", "", "widgets", "w1", "2023-01-01", armresources.GenericResource{
		Location: to.Ptr("eastus"), Tags: map[string]*string{"a": to.Ptr("b")}, Properties: map[string]any{"size": 3}, SKU: &armresources.SKU{Name: to.Ptr("S1")}}, nil))
	res := must(poller.PollUntilDone(ctx, &azruntimePoll))
	if *res.Name != "w1" || *res.Type != "Microsoft.Widgets/widgets" || *res.ID != "/subscriptions/"+sub+"/resourceGroups/g/providers/Microsoft.Widgets/widgets/w1" {
		t.Fatalf("%+v", res)
	}
	props := res.Properties.(map[string]any)
	if props["provisioningState"] != "Succeeded" || props["size"] != float64(3) {
		t.Fatal(props)
	}
	got := must(c.Get(ctx, "g", "Microsoft.Widgets", "", "widgets", "w1", "2023-01-01", nil))
	if *got.SKU.Name != "S1" || *got.Tags["a"] != "b" {
		t.Fatalf("%+v", got)
	}
	// a child needs its parent
	_, err := c.BeginCreateOrUpdate(ctx, "g", "Microsoft.Widgets", "widgets/nope", "parts", "p1", "2023-01-01", armresources.GenericResource{Location: to.Ptr("eastus")}, nil)
	wantErr(t, err, 404, "ResourceNotFound")
	must(must(c.BeginCreateOrUpdate(ctx, "g", "Microsoft.Widgets", "widgets/w1", "parts", "p1", "2023-01-01", armresources.GenericResource{Location: to.Ptr("eastus")}, nil)).PollUntilDone(ctx, &azruntimePoll))
	must(must(c.BeginCreateOrUpdate(ctx, "g", "Microsoft.Widgets", "", "widgets", "w2", "2023-01-01", armresources.GenericResource{Location: to.Ptr("eastus")}, nil)).PollUntilDone(ctx, &azruntimePoll))

	// lists: by group (with the child), by subscription
	var ids []string
	for p := c.NewListByResourceGroupPager("g", nil); p.More(); {
		for _, r := range must(p.NextPage(ctx)).Value {
			ids = append(ids, *r.Name)
		}
	}
	if strings.Join(ids, ",") != "w1,p1,w2" {
		t.Fatal(ids)
	}
	ids = nil
	for p := c.NewListPager(nil); p.More(); {
		for _, r := range must(p.NextPage(ctx)).Value {
			ids = append(ids, *r.Name)
		}
	}
	if len(ids) != 3 {
		t.Fatal(ids)
	}
	// the collection path lists one type under its parent
	st, out, _ := e.do(t, "GET", "/subscriptions/"+sub+"/resourceGroups/g/providers/Microsoft.Widgets/widgets/w1/parts?api-version=2023-01-01", "")
	if v, _ := out["value"].([]any); st != 200 || len(v) != 1 {
		t.Fatal(st, out)
	}
	// PATCH merges into the stored resource
	st, out, _ = e.do(t, "PATCH", "/subscriptions/"+sub+"/resourceGroups/g/providers/Microsoft.Widgets/widgets/w1?api-version=2023-01-01", `{"properties":{"color":"red"},"tags":{"z":"1"}}`)
	if p := out["properties"].(map[string]any); st != 200 || p["color"] != "red" || p["size"] != float64(3) || len(out["tags"].(map[string]any)) != 1 {
		t.Fatal(st, out)
	}

	// deleting the parent deletes the children; the group goes with everything in it
	must(must(c.BeginDeleteByID(ctx, *res.ID, "2023-01-01", nil)).PollUntilDone(ctx, &azruntimePoll))
	_, err = c.Get(ctx, "g", "Microsoft.Widgets", "widgets/w1", "parts", "p1", "2023-01-01", nil)
	wantErr(t, err, 404, "ResourceNotFound")
	must(must(f.NewResourceGroupsClient().BeginDelete(ctx, "g", nil)).PollUntilDone(ctx, &azruntimePoll))
	if st, _, _ := e.do(t, "GET", "/subscriptions/"+sub+"/resources?api-version=2021-04-01", ""); st != 200 {
		t.Fatal(st)
	}
	if _, err := c.Get(ctx, "g", "Microsoft.Widgets", "", "widgets", "w2", "2023-01-01", nil); err == nil {
		t.Fatal("w2 survived its group")
	}
	wantErr(t, must0(c.Get(ctx, "nogroup", "Microsoft.Widgets", "", "widgets", "w2", "2023-01-01", nil)), 404, "ResourceGroupNotFound")
}

func must0[T any](_ T, err error) error { return err }

var azruntimePoll = runtime.PollUntilDoneOptions{Frequency: 10 * time.Millisecond}

// testProvider is a service's management plane: Microsoft.Test/things, with a
// child type, an action and long-running operations.
func testProvider(t *testing.T, e *env) {
	var mu sync.Mutex
	things := map[string]map[string]any{}
	arm.Register(e.srv.Hub, arm.Provider{Type: "Microsoft.Test/things", Async: true,
		Put: func(c *core.Ctx, id arm.ID, body map[string]any) (map[string]any, error) {
			mu.Lock()
			defer mu.Unlock()
			if id.Name() == "bad" {
				return nil, core.Errorf(400, "InvalidName", "No bad things.")
			}
			r := arm.Resource(id, str(body["location"]), body["tags"], map[string]any{"provisioningState": "Succeeded", "from": "provider"})
			things[id.String()] = r
			return r, nil
		},
		Get: func(c *core.Ctx, id arm.ID) (map[string]any, error) {
			mu.Lock()
			defer mu.Unlock()
			if r, ok := things[id.String()]; ok {
				return r, nil
			}
			return nil, arm.NotFound(id)
		},
		Delete: func(c *core.Ctx, id arm.ID) error {
			mu.Lock()
			delete(things, id.String())
			mu.Unlock()
			return nil
		},
		List: func(c *core.Ctx, parent arm.ID) ([]map[string]any, error) {
			mu.Lock()
			defer mu.Unlock()
			var out []map[string]any
			for _, k := range core.SortedKeys(things) {
				if strings.HasPrefix(k, parent.String()+"/") {
					out = append(out, things[k])
				}
			}
			return out, nil
		},
		Actions: map[string]func(*core.Ctx, arm.ID, map[string]any) (any, error){
			"listKeys": func(c *core.Ctx, id arm.ID, body map[string]any) (any, error) {
				return map[string]any{"keys": []string{"k1", "k2"}, "for": id.Name()}, nil
			}}})
}

func str(v any) string { s, _ := v.(string); return s }

func TestProvider(t *testing.T) {
	e := start(t)
	testProvider(t, e)
	g := "/subscriptions/" + sub + "/resourceGroups/pogi"
	base := g + "/providers/Microsoft.Test/things"

	// a long-running PUT: 201 with operation headers that already say Succeeded
	st, out, h := e.do(t, "PUT", base+"/t1?api-version=2023-01-01", `{"location":"eastus","tags":{"a":"b"}}`)
	if st != 201 || out["name"] != "t1" || h.Get("Azure-AsyncOperation") == "" || h.Get("Location") == "" {
		t.Fatal(st, out, h)
	}
	if st, op, _ := e.do(t, "GET", strings.TrimPrefix(h.Get("Azure-AsyncOperation"), e.base), ""); st != 200 || op["status"] != "Succeeded" {
		t.Fatal(st, op)
	}
	st, out, _ = e.do(t, "PUT", base+"/t1?api-version=2023-01-01", `{"location":"eastus"}`)
	if st != 201 { // async providers answer 201 for updates too
		t.Fatal(st, out)
	}
	if st, out, _ = e.do(t, "GET", base+"/t1?api-version=2023-01-01", ""); st != 200 || out["properties"].(map[string]any)["from"] != "provider" {
		t.Fatal(st, out)
	}
	// provider errors keep their code and status
	if st, out, _ = e.do(t, "PUT", base+"/bad?api-version=2023-01-01", `{}`); st != 400 || out["error"].(map[string]any)["code"] != "InvalidName" {
		t.Fatal(st, out)
	}
	if st, out, _ = e.do(t, "GET", base+"/nope?api-version=2023-01-01", ""); st != 404 || out["error"].(map[string]any)["code"] != "ResourceNotFound" {
		t.Fatal(st, out)
	}
	// list wrapping, in the group and at subscription scope
	e.do(t, "PUT", base+"/t2?api-version=2023-01-01", `{}`)
	for _, path := range []string{base, "/subscriptions/" + sub + "/providers/Microsoft.Test/things"} {
		if st, out, _ = e.do(t, "GET", path+"?api-version=2023-01-01", ""); len(out["value"].([]any)) != 2 {
			t.Fatal(st, path, out)
		}
	}
	// actions
	if st, out, _ = e.do(t, "POST", base+"/t1/listKeys?api-version=2023-01-01", `{}`); st != 200 || out["for"] != "t1" {
		t.Fatal(st, out)
	}
	if st, _, _ = e.do(t, "POST", base+"/t1/nothing?api-version=2023-01-01", `{}`); st != 404 {
		t.Fatal(st)
	}
	if st, _, _ = e.do(t, "POST", base+"/missing/listKeys?api-version=2023-01-01", `{}`); st != 404 {
		t.Fatal(st)
	}
	// the SDK follows the operation headers
	c := e.factory(sub).NewClient()
	must(must(c.BeginCreateOrUpdate(ctx, "pogi", "Microsoft.Test", "", "things", "t3", "2023-01-01", armresources.GenericResource{Location: to.Ptr("eastus")}, nil)).PollUntilDone(ctx, &azruntimePoll))
	must(must(c.BeginDelete(ctx, "pogi", "Microsoft.Test", "", "things", "t3", "2023-01-01", nil)).PollUntilDone(ctx, &azruntimePoll))
	if st, _, _ = e.do(t, "GET", base+"/t3?api-version=2023-01-01", ""); st != 404 {
		t.Fatal(st)
	}
	// delete answers 202 for what exists, 204 for what doesn't
	if st, _, h = e.do(t, "DELETE", base+"/t2?api-version=2023-01-01", ""); st != 202 || h.Get("Azure-AsyncOperation") == "" {
		t.Fatal(st, h)
	}
	if st, _, _ = e.do(t, "DELETE", base+"/t2?api-version=2023-01-01", ""); st != 204 {
		t.Fatal(st)
	}
	// resources of a Provider show up in group listings, and die with their group
	must(e.factory(sub).NewResourceGroupsClient().CreateOrUpdate(ctx, "tg", armresources.ResourceGroup{Location: to.Ptr("eastus")}, nil))
	e.do(t, "PUT", "/subscriptions/"+sub+"/resourceGroups/tg/providers/Microsoft.Test/things/in-tg?api-version=2023-01-01", `{}`)
	var names []string
	for p := c.NewListByResourceGroupPager("tg", nil); p.More(); {
		for _, r := range must(p.NextPage(ctx)).Value {
			names = append(names, *r.Name)
		}
	}
	if strings.Join(names, ",") != "in-tg" {
		t.Fatal(names)
	}
	must(must(e.factory(sub).NewResourceGroupsClient().BeginDelete(ctx, "tg", nil)).PollUntilDone(ctx, &azruntimePoll))
	if st, _, _ = e.do(t, "GET", "/subscriptions/"+sub+"/resourceGroups/tg/providers/Microsoft.Test/things/in-tg?api-version=2023-01-01", ""); st != 404 {
		t.Fatal(st)
	}
}

func TestDeploymentsAndProviders(t *testing.T) {
	e := start(t)
	f := e.factory(sub)
	d := f.NewDeploymentsClient()
	poller := must(d.BeginCreateOrUpdate(ctx, "pogi", "dep1", armresources.Deployment{Properties: &armresources.DeploymentProperties{
		Mode: to.Ptr(armresources.DeploymentModeIncremental), Template: map[string]any{"resources": []any{}}}}, nil))
	got := must(poller.PollUntilDone(ctx, &azruntimePoll))
	if *got.Properties.ProvisioningState != "Succeeded" || *got.Name != "dep1" {
		t.Fatalf("%+v", got.Properties)
	}
	if got := must(d.Get(ctx, "pogi", "dep1", nil)); *got.Properties.ProvisioningState != "Succeeded" {
		t.Fatal("not stored")
	}
	must(d.CheckExistence(ctx, "pogi", "dep1", nil))

	// providers read as Registered, listed, got and registered
	ps := f.NewProvidersClient()
	if p := must(ps.Get(ctx, "Microsoft.Storage", nil)); *p.RegistrationState != "Registered" {
		t.Fatal(*p.RegistrationState)
	}
	if p := must(ps.Register(ctx, "Microsoft.Anything", nil)); *p.Namespace != "Microsoft.Anything" || *p.RegistrationState != "Registered" {
		t.Fatalf("%+v", p)
	}
	var found bool
	for pg := ps.NewListPager(nil); pg.More(); {
		for _, p := range must(pg.NextPage(ctx)).Value {
			found = found || *p.Namespace == "Microsoft.ManagedIdentity" && *p.RegistrationState == "Registered"
		}
	}
	if !found {
		t.Fatal("namespaces missing")
	}
}

func TestSubscriptionsAndMetadata(t *testing.T) {
	e := start(t)
	// subscriptions: the default one, then any that was used
	st, out, _ := e.do(t, "GET", "/subscriptions?api-version=2020-01-01", "")
	v := out["value"].([]any)
	if st != 200 || len(v) != 1 || v[0].(map[string]any)["subscriptionId"] != sub || v[0].(map[string]any)["tenantId"] != arm.TenantID {
		t.Fatal(st, out)
	}
	st, out, _ = e.do(t, "GET", "/subscriptions/22222222-2222-2222-2222-222222222222?api-version=2020-01-01", "")
	if st != 200 || out["state"] != "Enabled" {
		t.Fatal(st, out)
	}
	if _, out, _ = e.do(t, "GET", "/subscriptions?api-version=2020-01-01", ""); len(out["value"].([]any)) != 2 {
		t.Fatal(out)
	}
	if _, out, _ = e.do(t, "GET", "/tenants?api-version=2020-01-01", ""); out["value"].([]any)[0].(map[string]any)["tenantId"] != arm.TenantID {
		t.Fatal(out)
	}
	if _, out, _ = e.do(t, "GET", "/subscriptions/"+sub+"/locations?api-version=2020-01-01", ""); len(out["value"].([]any)) < 10 {
		t.Fatal(out)
	}
	// the metadata endpoint names pogi, in both api-versions
	for _, v := range []string{"2022-09-01", "2019-05-01"} {
		st, out, _ = e.do(t, "GET", "/metadata/endpoints?api-version="+v, "")
		auth := out["authentication"].(map[string]any)
		if st != 200 || out["resourceManager"] != e.base+"/" || auth["loginEndpoint"] != e.base+"/" || out["microsoftGraphResourceId"] != e.base+"/" || out["graph"] != e.base+"/" {
			t.Fatal(v, st, out)
		}
		kv := out["suffixes"].(map[string]any)["keyVaultDns"].(string)
		if !strings.HasSuffix(kv, "vault.localhost:"+e.base[strings.LastIndexByte(e.base, ':')+1:]) || (v == "2019-05-01") != strings.HasPrefix(kv, ".") {
			t.Fatal(v, kv)
		}
	}
	// a bearer-less metadata request still works; an unknown path is not ours
	req, _ := http.NewRequest("GET", e.base+"/metadata/endpoints?api-version=2022-09-01", nil)
	if res, err := e.http.Do(req); err != nil || res.StatusCode != 200 {
		t.Fatal(err)
	}
	if st, out, _ = e.do(t, "GET", "/subscriptions/"+sub+"/nonsense?api-version=2020-01-01", ""); st != 404 || out["error"].(map[string]any)["code"] != "InvalidRequestUri" {
		t.Fatal(st, out)
	}
	// built-in roles resolve by name, and role assignments are stored
	st, out, _ = e.do(t, "GET", "/subscriptions/"+sub+"/providers/Microsoft.Authorization/roleDefinitions?api-version=2022-04-01&$filter=roleName%20eq%20'Contributor'", "")
	rd := out["value"].([]any)
	if st != 200 || len(rd) != 1 || rd[0].(map[string]any)["name"] != "b24988ac-6180-42a0-ab88-20f7382dd24c" {
		t.Fatal(st, out)
	}
	ra := "/subscriptions/" + sub + "/resourceGroups/pogi/providers/Microsoft.Authorization/roleAssignments/" + core.UUID()
	st, out, _ = e.do(t, "PUT", ra+"?api-version=2022-04-01", `{"properties":{"principalId":"p","roleDefinitionId":"r"}}`)
	if st != 201 || out["properties"].(map[string]any)["principalId"] != "p" {
		t.Fatal(st, out)
	}
	// a role assignment on a resource's own scope is an extension resource
	ext := "/subscriptions/" + sub + "/resourceGroups/pogi/providers/Microsoft.Storage/storageAccounts/sa/providers/Microsoft.Authorization/roleAssignments/x"
	if st, out, _ = e.do(t, "PUT", ext+"?api-version=2022-04-01", `{"properties":{"principalId":"q"}}`); st != 201 || out["type"] != "Microsoft.Authorization/roleAssignments" {
		t.Fatal(st, out)
	}
	if st, out, _ = e.do(t, "GET", ext+"?api-version=2022-04-01", ""); st != 200 || out["properties"].(map[string]any)["principalId"] != "q" {
		t.Fatal(st, out)
	}
	if st, _, _ = e.do(t, "DELETE", ext+"?api-version=2022-04-01", ""); st != 200 {
		t.Fatal(st)
	}
	// name checks and validations succeed without a provider
	if _, out, _ = e.do(t, "POST", "/subscriptions/"+sub+"/providers/Microsoft.Storage/checkNameAvailability?api-version=2023-01-01", `{"name":"x"}`); out["nameAvailable"] != true {
		t.Fatal(out)
	}
}

func TestPersistence(t *testing.T) {
	e := start(t)
	f := e.factory(sub)
	must(f.NewResourceGroupsClient().CreateOrUpdate(ctx, "keep", armresources.ResourceGroup{Location: to.Ptr("eastus")}, nil))
	must(must(f.NewClient().BeginCreateOrUpdate(ctx, "keep", "Microsoft.Widgets", "", "widgets", "w", "2023-01-01", armresources.GenericResource{Location: to.Ptr("eastus")}, nil)).PollUntilDone(ctx, &azruntimePoll))
	snap := must(e.srv.Hub.Snapshot())
	e.srv.Reset("arm")
	if _, err := f.NewResourceGroupsClient().Get(ctx, "keep", nil); err == nil {
		t.Fatal("reset left the group")
	}
	if err := e.srv.Hub.Restore(snap); err != nil {
		t.Fatal(err)
	}
	must(f.NewClient().Get(ctx, "keep", "Microsoft.Widgets", "", "widgets", "w", "2023-01-01", nil))
	if got := must(f.NewResourceGroupsClient().Get(ctx, "keep", nil)); *got.Location != "eastus" {
		t.Fatal(got)
	}
	tables := e.srv.Hub.Resources("arm")["arm"]
	if len(tables) != 3 || len(tables[1].Rows) != 2 || len(tables[2].Rows) != 1 {
		t.Fatalf("%+v", tables)
	}
}

// Bad requests get ARM's own errors and store nothing.
func TestBadRequests(t *testing.T) {
	e := start(t)
	g := "/subscriptions/" + sub + "/resourceGroups/pogi/providers/"
	for name, c := range map[string]struct {
		method, path, body string
		status             int
		code               string
	}{
		"null body":         {"PUT", g + "Microsoft.Widgets/widgets/a?api-version=1", `null`, 400, "InvalidRequestContent"},
		"array body":        {"PUT", g + "Microsoft.Widgets/widgets/a?api-version=1", `[]`, 400, "InvalidRequestContent"},
		"malformed body":    {"PUT", g + "Microsoft.Widgets/widgets/a?api-version=1", `{`, 400, "InvalidRequestContent"},
		"sku is a string":   {"PUT", g + "Microsoft.Widgets/widgets/a?api-version=1", `{"sku":"S1"}`, 400, "InvalidRequestContent"},
		"location a number": {"PUT", g + "Microsoft.Widgets/widgets/a?api-version=1", `{"location":5}`, 400, "InvalidRequestContent"},
		"unknown namespace": {"PUT", g + "Microsoft.Made/ups/a?api-version=1", `{}`, 400, "NoRegisteredProviderFound"},
		"unknown type":      {"PUT", g + "Microsoft.Storage/madeUpThings/a?api-version=1", `{}`, 400, "InvalidResourceType"},
		"retired MariaDB":   {"PUT", g + "Microsoft.DBforMariaDB/servers/a?api-version=2018-06-01", `{}`, 400, "NoRegisteredProviderFound"},
		"no api-version":    {"GET", "/subscriptions/" + sub + "/resourceGroups/pogi", ``, 400, "MissingApiVersionParameter"},
		"empty segment":     {"PUT", g + "Microsoft.Storage//a?api-version=1", `null`, 400, "InvalidRequestUri"},
	} {
		if st, out, _ := e.do(t, c.method, c.path, c.body); st != c.status || fmt.Sprint(out["error"]) == "" || out["error"].(map[string]any)["code"] != c.code {
			t.Errorf("%s: %d %v, want %d %s", name, st, out, c.status, c.code)
		}
	}
	// nothing was stored, so the list still works
	if st, out, _ := e.do(t, "GET", g+"Microsoft.Widgets/widgets?api-version=1", ""); st != 200 || len(out["value"].([]any)) != 0 {
		t.Errorf("list after bad PUTs: %d %v", st, out)
	}
	// a deployment that holds an unknown type fails, a known one succeeds, and one with no apiVersion is refused
	if st, out, _ := e.do(t, "PUT", g+"Microsoft.Resources/deployments/d?api-version=1", `{"properties":{"mode":"Incremental","template":{"resources":[{"type":"Microsoft.Widgets/widgets","name":"x"}]}}}`); st != 400 || out["error"].(map[string]any)["code"] != "InvalidTemplate" {
		t.Errorf("no apiVersion: %d %v", st, out)
	}
	dep := func(typ string) string {
		e.do(t, "PUT", g+"Microsoft.Resources/deployments/d?api-version=1", `{"properties":{"mode":"Incremental","template":{"resources":[{"type":"`+typ+`","apiVersion":"2023-01-01","name":"x"}]}}}`)
		_, out, _ := e.do(t, "GET", g+"Microsoft.Resources/deployments/d?api-version=1", "")
		props, _ := out["properties"].(map[string]any)
		return fmt.Sprint(props["provisioningState"])
	}
	if got := dep("Microsoft.Widgets/widgets"); got != "Succeeded" {
		t.Errorf("known type: %s", got)
	}
	if got := dep("Microsoft.Made/ups"); got != "Failed" {
		t.Errorf("unknown type: %s", got)
	}
}

// az resource list -g, az resource list --tag and az group list --tag send a $filter; a filter pogi cannot read is a 400.
func TestListFilters(t *testing.T) {
	e := start(t)
	f := e.factory(sub)
	for g, tags := range map[string]string{"g1": "dev", "g2": "prod"} {
		must(f.NewResourceGroupsClient().CreateOrUpdate(ctx, g, armresources.ResourceGroup{Location: to.Ptr("eastus"), Tags: map[string]*string{"env": to.Ptr(tags)}}, nil))
		must(must(f.NewClient().BeginCreateOrUpdate(ctx, g, "Microsoft.Widgets", "", "widgets", "w-"+g, "2023-01-01",
			armresources.GenericResource{Location: to.Ptr("eastus"), Tags: map[string]*string{"env": to.Ptr(tags)}}, nil)).PollUntilDone(ctx, &azruntimePoll))
	}
	names := func(path string) string {
		t.Helper()
		st, out, _ := e.do(t, "GET", path, "")
		if st != 200 {
			t.Fatalf("%s: %d %v", path, st, out)
		}
		var n []string
		for _, it := range out["value"].([]any) {
			n = append(n, it.(map[string]any)["name"].(string))
		}
		return strings.Join(n, ",")
	}
	base := "/subscriptions/" + sub
	for path, want := range map[string]string{
		base + "/resources?api-version=2021-04-01&$filter=" + url.QueryEscape("resourceGroup eq 'G1'"):                                                 "w-g1",
		base + "/resources?api-version=2021-04-01&$filter=" + url.QueryEscape("resourceGroup eq 'g1' and resourceType eq 'Microsoft.Widgets/widgets'"): "w-g1",
		base + "/resources?api-version=2021-04-01&$filter=" + url.QueryEscape("tagName eq 'env' and tagValue eq 'prod'"):                               "w-g2",
		base + "/resourcegroups?api-version=2021-04-01&$filter=" + url.QueryEscape("tagname eq 'env' and tagvalue eq 'dev'"):                           "g1",
		base + "/resourcegroups?api-version=2021-04-01&$filter=" + url.QueryEscape("tagname eq 'nope'"):                                                "",
	} {
		if got := names(path); got != want {
			t.Errorf("%s: got %q, want %q", path, got, want)
		}
	}
	for _, p := range []string{"/resources", "/resourcegroups", "/resourceGroups/g1/resources"} {
		st, out, _ := e.do(t, "GET", base+p+"?api-version=2021-04-01&$filter="+url.QueryEscape("name like 'a'"), "")
		if st != 400 || out["error"].(map[string]any)["code"] != "InvalidFilter" {
			t.Errorf("%s: bad filter gave %d %v", p, st, out)
		}
	}
}
