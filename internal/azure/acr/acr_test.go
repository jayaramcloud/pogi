package acr_test

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/containerregistry/armcontainerregistry"
	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/azure/armtest"
	"github.com/nrndev/pogi/internal/core"
)

var (
	ctx = armtest.Ctx
	sub = core.AzureSubscription
	rg  = core.AzureResourceGroup
)

func digest(b []byte) string { return fmt.Sprintf("sha256:%x", sha256.Sum256(b)) }

// call sends a request to pogi's port, with the Host header host when it is set.
func call(t *testing.T, method, base, host, path string, body []byte, hdr ...string) (*http.Response, string) {
	t.Helper()
	req, _ := http.NewRequest(method, base+path, bytes.NewReader(body))
	if host != "" {
		req.Host = host
	}
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp, string(b)
}

// push stores an image of one layer under name:tag, at the repository path of the registry host.
func push(t *testing.T, base, host, name, tag string) {
	t.Helper()
	config, layer := []byte(`{"architecture":"amd64","os":"linux"}`), []byte("a layer of "+name)
	for _, blob := range [][]byte{config, layer} {
		if resp, body := call(t, "POST", base, host, "/v2/"+name+"/blobs/uploads/?digest="+url.QueryEscape(digest(blob)), blob); resp.StatusCode != 201 {
			t.Fatalf("blob upload to %s = %s %s", name, resp.Status, body)
		}
	}
	m := fmt.Appendf(nil, `{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{"mediaType":"application/vnd.oci.image.config.v1+json","digest":%q,"size":%d},`+
		`"layers":[{"mediaType":"application/vnd.oci.image.layer.v1.tar","digest":%q,"size":%d}]}`, digest(config), len(config), digest(layer), len(layer))
	if resp, body := call(t, "PUT", base, host, "/v2/"+name+"/manifests/"+tag, m, "Content-Type", "application/vnd.oci.image.manifest.v1+json"); resp.StatusCode != 201 {
		t.Fatalf("manifest put to %s = %s %s", name, resp.Status, body)
	}
}

func TestRegistries(t *testing.T) {
	e := armtest.Start(t, pogi.Config{}, "ecr", "azacr")
	regs := armtest.Must(armcontainerregistry.NewRegistriesClient(sub, e.Cred, e.Opts))

	// 1. create, get, list
	poller := armtest.Must(regs.BeginCreate(ctx, rg, "pogiregistry", armcontainerregistry.Registry{Location: to.Ptr("eastus"),
		SKU:        &armcontainerregistry.SKU{Name: to.Ptr(armcontainerregistry.SKUNameBasic)},
		Properties: &armcontainerregistry.RegistryProperties{AdminUserEnabled: to.Ptr(true)}, Tags: map[string]*string{"env": to.Ptr("test")}}, nil))
	created := armtest.Must(poller.PollUntilDone(ctx, armtest.Fast))
	p := created.Properties
	if *created.Name != "pogiregistry" || *p.ProvisioningState != armcontainerregistry.ProvisioningStateSucceeded || !*p.AdminUserEnabled || *created.SKU.Tier != armcontainerregistry.SKUTierBasic {
		t.Fatalf("%+v %+v", created, p)
	}
	login := *p.LoginServer
	if !strings.HasPrefix(login, "pogiregistry.azurecr.localhost:") {
		t.Fatalf("login server %q", login)
	}
	if got := armtest.Must(regs.Get(ctx, rg, "pogiregistry", nil)); *got.Tags["env"] != "test" || *got.Properties.LoginServer != login {
		t.Fatalf("%+v", got)
	}
	n := 0
	for lp := regs.NewListByResourceGroupPager(rg, nil); lp.More(); {
		n += len(armtest.Must(lp.NextPage(ctx)).Value)
	}
	for sp := regs.NewListPager(nil); sp.More(); {
		n += len(armtest.Must(sp.NextPage(ctx)).Value)
	}
	if n != 2 { // once per listing
		t.Fatalf("registries listed: %d", n)
	}

	// 2. update: tags and a setting survive, the SKU stays
	up := armtest.Must(armtest.Must(regs.BeginUpdate(ctx, rg, "pogiregistry", armcontainerregistry.RegistryUpdateParameters{
		Tags: map[string]*string{"env": to.Ptr("prod"), "team": to.Ptr("x")}}, nil)).PollUntilDone(ctx, armtest.Fast))
	if len(up.Tags) != 2 || *up.Tags["env"] != "prod" || *up.SKU.Name != armcontainerregistry.SKUNameBasic || !*up.Properties.AdminUserEnabled {
		t.Fatalf("%+v", up)
	}

	// 3. credentials: the admin user and two passwords; regenerating one changes only it
	cr := armtest.Must(regs.ListCredentials(ctx, rg, "pogiregistry", nil))
	if *cr.Username != "pogiregistry" || len(cr.Passwords) != 2 || *cr.Passwords[0].Value == "" || *cr.Passwords[0].Value == *cr.Passwords[1].Value {
		t.Fatalf("%+v", cr)
	}
	re := armtest.Must(regs.RegenerateCredential(ctx, rg, "pogiregistry", armcontainerregistry.RegenerateCredentialParameters{Name: to.Ptr(armcontainerregistry.PasswordNamePassword2)}, nil))
	if *re.Passwords[0].Value != *cr.Passwords[0].Value || *re.Passwords[1].Value == *cr.Passwords[1].Value {
		t.Fatalf("regenerated %+v from %+v", re, cr)
	}

	// 4. names: taken, free, malformed; and a malformed one cannot be created
	for name, want := range map[string]bool{"pogiregistry": false, "freeregistry": true, "no": false} {
		if got := armtest.Must(regs.CheckNameAvailability(ctx, armcontainerregistry.RegistryNameCheckRequest{Name: to.Ptr(name), Type: to.Ptr("Microsoft.ContainerRegistry/registries")}, nil)); *got.NameAvailable != want {
			t.Errorf("name %q available = %v", name, *got.NameAvailable)
		}
	}
	_, err := regs.BeginCreate(ctx, rg, "bad-name", armcontainerregistry.Registry{Location: to.Ptr("eastus"), SKU: &armcontainerregistry.SKU{Name: to.Ptr(armcontainerregistry.SKUNameBasic)}}, nil)
	armtest.WantErr(t, err, 400, "InvalidParameter")

	// 5. docker's endpoint: the registry's own host and the path form reach the same images
	push(t, e.Srv.URL, login, "team/app", "v1")
	push(t, e.Srv.URL, "", "pogiregistry/team/web", "v2")
	_, body := call(t, "GET", e.Srv.URL, login, "/v2/_catalog", nil)
	if !strings.Contains(body, `"team/app"`) || !strings.Contains(body, `"team/web"`) {
		t.Fatalf("catalog = %s", body)
	}
	if _, body = call(t, "GET", e.Srv.URL, "", "/v2/pogiregistry/team/web/tags/list", nil); !strings.Contains(body, `"v2"`) {
		t.Fatalf("tags = %s", body)
	}
	if resp, body := call(t, "GET", e.Srv.URL, login, "/v2/team/app/manifests/v1", nil); resp.StatusCode != 200 || !strings.Contains(body, "layers") {
		t.Fatalf("pull = %s %s", resp.Status, body)
	}
	// a registry that does not exist, and a name outside every registry, have no repositories
	if resp, _ := call(t, "POST", e.Srv.URL, "ghostregistry.azurecr.localhost:1", "/v2/app/blobs/uploads/", nil); resp.StatusCode != 404 {
		t.Fatalf("push to a registry that does not exist = %s", resp.Status)
	}
	usages := armtest.Must(regs.ListUsages(ctx, rg, "pogiregistry", nil))
	if *usages.Value[0].CurrentValue != 2 {
		t.Fatalf("usages %+v", usages.Value[0])
	}

	// 6. az acr login: an Entra token for a refresh token, that for an access token
	_, out := call(t, "POST", e.Srv.URL, login, "/oauth2/exchange", []byte("grant_type=access_token&service="+login+"&access_token=x"), "Content-Type", "application/x-www-form-urlencoded")
	if !strings.Contains(out, `"refresh_token"`) {
		t.Fatalf("exchange = %s", out)
	}
	if _, out = call(t, "POST", e.Srv.URL, login, "/oauth2/token", []byte("grant_type=refresh_token&service="+login+"&scope=repository:team/app:pull"), "Content-Type", "application/x-www-form-urlencoded"); !strings.Contains(out, `"access_token"`) {
		t.Fatalf("token = %s", out)
	}

	// 7. delete takes the images with it; the registry is gone
	armtest.Must(regs.BeginDelete(ctx, rg, "pogiregistry", nil)).PollUntilDone(ctx, armtest.Fast)
	_, err = regs.Get(ctx, rg, "pogiregistry", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	if resp, _ := call(t, "GET", e.Srv.URL, "", "/v2/pogiregistry/team/web/tags/list", nil); resp.StatusCode != 404 {
		t.Fatalf("images of a deleted registry: %s", resp.Status)
	}
}

// A registry and its images survive a save and a load, and a reset forgets them: the registry's claim on its names is
// ECR's to keep, and the registry's to make again.
func TestPersistence(t *testing.T) {
	e := armtest.Start(t, pogi.Config{}, "ecr", "azacr")
	regs := armtest.Must(armcontainerregistry.NewRegistriesClient(sub, e.Cred, e.Opts))
	armtest.Must(regs.BeginCreate(ctx, rg, "keptregistry", armcontainerregistry.Registry{Location: to.Ptr("eastus"),
		SKU: &armcontainerregistry.SKU{Name: to.Ptr(armcontainerregistry.SKUNameStandard)}}, nil)).PollUntilDone(ctx, armtest.Fast)
	creds := armtest.Must(regs.ListCredentials(ctx, rg, "keptregistry", nil))
	push(t, e.Srv.URL, "", "keptregistry/app", "v1")

	saved := map[string][]byte{}
	for _, n := range []string{"ecr", "azacr"} {
		saved[n] = armtest.Must(e.Srv.Hub.Service(n).(core.Stateful).Save())
	}
	e.Srv.Reset("ecr", "azacr")
	if _, err := regs.Get(ctx, rg, "keptregistry", nil); err == nil {
		t.Fatal("a reset keeps the registry")
	}
	if resp, _ := call(t, "POST", e.Srv.URL, "", "/v2/keptregistry/app/blobs/uploads/", nil); resp.StatusCode != 404 {
		t.Fatal("a reset keeps the registry's names")
	}
	for _, n := range []string{"ecr", "azacr"} {
		if err := e.Srv.Hub.Service(n).(core.Stateful).Load(saved[n]); err != nil {
			t.Fatal(err)
		}
	}
	got := armtest.Must(regs.Get(ctx, rg, "keptregistry", nil))
	if *got.SKU.Name != armcontainerregistry.SKUNameStandard || *armtest.Must(regs.ListCredentials(ctx, rg, "keptregistry", nil)).Passwords[0].Value != *creds.Passwords[0].Value {
		t.Fatalf("%+v", got)
	}
	if _, body := call(t, "GET", e.Srv.URL, "", "/v2/keptregistry/app/tags/list", nil); !strings.Contains(body, `"v1"`) {
		t.Fatalf("tags after a load: %s", body)
	}
	if resp, body := call(t, "POST", e.Srv.URL, "", "/v2/keptregistry/app/blobs/uploads/", nil); resp.StatusCode != 202 {
		t.Fatalf("push after a load: %s %s", resp.Status, body)
	}
}
