package web_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"testing"

	"github.com/nrndev/pogi/internal/core"
)

// azDo is one Azure request to pogi over plain HTTP, with an optional bearer token and Host.
func (c *console) azDo(method, path, host, token string, body any) (int, map[string]any) {
	c.t.Helper()
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = strings.NewReader(string(b))
	}
	req, _ := http.NewRequest(method, c.base+path, rd)
	if host != "" {
		req.Host = host
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	req.Header.Set("Metadata", "true")
	req.Header.Set("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		c.t.Fatal(err)
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	out := map[string]any{}
	json.Unmarshal(b, &out)
	return res.StatusCode, out
}

// The Azure view of the IAM page: a managed identity's recorded calls, the generated role, attach
// (a custom role definition plus an assignment, through ARM) and undo, and the badge.
func TestIAMAzurePage(t *testing.T) {
	const rg = "/subscriptions/" + core.AzureSubscription + "/resourceGroups/" + core.AzureResourceGroup
	c := start(t, "arm", "entra", "graph", "azblob")
	want(t, c.get("/_pogi/iam?c=azure"), "No calls recorded yet", "Google Cloud", "Azure")

	st, mi := c.azDo("PUT", rg+"/providers/Microsoft.ManagedIdentity/userAssignedIdentities/ci?api-version=2023-01-31", "", "", map[string]any{"location": "eastus"})
	if st/100 != 2 {
		t.Fatalf("identity: %d %v", st, mi)
	}
	props := mi["properties"].(map[string]any)
	oid := props["principalId"].(string)
	st, tok := c.azDo("GET", "/metadata/identity/oauth2/token?api-version=2018-02-01&resource="+url.QueryEscape(c.base)+"&client_id="+props["clientId"].(string), "", "", nil)
	if st != 200 {
		t.Fatalf("token: %d %v", st, tok)
	}
	token := tok["access_token"].(string)
	blobs := func() int {
		st, _ := c.azDo("GET", "/box?restype=container&comp=list", "store.blob.localhost:"+c.base[strings.LastIndexByte(c.base, ':')+1:], token, nil)
		return st
	}

	c.post("/_pogi/iam/mode", url.Values{"mode": {"strict"}, "c": {"azure"}})
	if st := blobs(); st != 403 {
		t.Fatalf("strict mode let a role-less identity read blobs: %d", st)
	}
	page := c.get("/_pogi/iam?c=azure")
	want(t, page, "ci · managedIdentity", "containers/blobs/read", "would deny", ">data<", "Create and assign pogi-ci",
		"Strict: calls without an allowing role assignment", "1 denied")
	want(t, c.get("/_pogi/iam/azure/policy?principal="+oid), `"roleName": "pogi-ci"`, `"CustomRole"`, `"dataActions"`)
	want(t, c.get("/_pogi/iam?c=azure&fmt=cli"), "az role definition create", "az role assignment create --assignee-object-id &#39;"+oid+"&#39;") // shell-quoted, then HTML-escaped

	// the attach drawer shows what it will create
	want(t, c.get("/_pogi/iam/azure/attach?principal="+oid), "+1 permission", "pogi-ci", rg, "custom role")

	body, final := c.post("/_pogi/iam/azure/attach", url.Values{"principal": {oid}})
	if final != "/_pogi/iam" || strings.Contains(body, `class="flash"`) {
		t.Fatalf("attach ended on %s: %s", final, flash(body))
	}
	want(t, body, "Assigned pogi-ci at", "Undo", "Nothing to attach", "Switch to strict and watch")
	if st := blobs(); st == 403 {
		t.Fatal("the generated role did not open the data plane")
	}
	form := url.Values{}
	for _, f := range []string{"principal", "role", "roleid", "assignment", "scope", "existed"} {
		m := regexp.MustCompile(`name="` + f + `" value="([^"]*)"`).FindStringSubmatch(body)
		if m == nil {
			t.Fatalf("undo form lacks %s:\n%s", f, body)
		}
		form.Set(f, m[1])
	}
	if st, _ := c.azDo("GET", form.Get("assignment")+"?api-version=2022-04-01", "", "", nil); st != 200 {
		t.Fatalf("the assignment is not in ARM: %d", st)
	}

	c.post("/_pogi/iam/azure/undo", form)
	if st := blobs(); st != 403 {
		t.Fatalf("after undo: %d", st)
	}
	want(t, c.get("/_pogi/iam?c=azure"), "would deny")
	if st, _ := c.azDo("GET", form.Get("assignment")+"?api-version=2022-04-01", "", "", nil); st != 404 {
		t.Fatalf("assignment after undo: %d", st)
	}
	c.post("/_pogi/iam/azure/restart", url.Values{})
	want(t, c.get("/_pogi/iam?c=azure"), "No calls recorded yet")
}
