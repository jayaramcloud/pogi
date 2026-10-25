package gapi

import (
	"net/http/httptest"
	"testing"
	"time"

	"cloud.google.com/go/iam/apiv1/iampb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
)

// A null body is an empty message, an unknown enum name and a negative page_size are refused,
// and a huge page is capped.
func TestDecode(t *testing.T) {
	in, err := ParseBody([]byte("null"), "policy")
	if err != nil {
		t.Fatal(err)
	}
	req := &iampb.SetIamPolicyRequest{}
	if err := Decode(req, in); err != nil || req.Policy == nil {
		t.Fatalf("null body: %v %v", err, req.Policy)
	}
	if err := Decode(&iampb.BindingDelta{}, map[string]any{"action": "ADD"}); err != nil {
		t.Fatalf("a known enum name: %v", err)
	}
	if err := Decode(&iampb.BindingDelta{}, map[string]any{"action": "FROBNICATE"}); err == nil {
		t.Fatal("an unknown enum name was accepted")
	}
	if err := Decode(&longrunningpb.ListOperationsRequest{}, map[string]any{"pageSize": "-1"}); err == nil {
		t.Fatal("a negative page size was accepted")
	}
	if page, next := Page(make([]int, 2500), 1<<30, ""); len(page) != MaxPageSize || next == "" {
		t.Fatalf("a huge page size: %d items, next %q", len(page), next)
	}
}

func TestMuxPatterns(t *testing.T) {
	m := NewMux(nil,
		Rule{HTTP: "GET /v1/{name=projects/*/serviceAccounts/*}", Method: "Get"},
		Rule{HTTP: "POST /v1/{name=projects/*/serviceAccounts/*}:disable", Method: "Disable"},
		Rule{HTTP: "GET /v1/{parent=projects/*}/serviceAccounts", Method: "List"},
		Rule{HTTP: "GET /v1/projects/{id}", Method: "Project"},
		Rule{HTTP: "POST /v1/projects/{id}:undelete", Method: "Undelete"},
		Rule{HTTP: "GET /v1/roles", Method: "Roles"},
		Rule{HTTP: "GET /v1/{name=roles/*}", Method: "Role"},
		Rule{HTTP: "GET /v1/{name=**}", Method: "Any"})
	for _, c := range []struct{ method, path, want, name string }{
		{"GET", "/v1/projects/p/serviceAccounts/a@p.iam.gserviceaccount.com", "Get", "projects/p/serviceAccounts/a@p.iam.gserviceaccount.com"},
		{"POST", "/v1/projects/-/serviceAccounts/x:disable", "Disable", "projects/-/serviceAccounts/x"},
		{"GET", "/v1/projects/p/serviceAccounts", "List", ""},
		{"GET", "/v1/projects/p", "Project", ""},
		{"POST", "/v1/projects/p:undelete", "Undelete", ""},
		{"GET", "/v1/roles", "Roles", ""},
		{"GET", "/v1/roles/viewer", "Role", "roles/viewer"},
		{"GET", "/v1/anything/at/all", "Any", "anything/at/all"},
		{"DELETE", "/v1/projects/p", "", ""},
		{"POST", "/v1/projects/p/serviceAccounts/x:enable", "", ""},
	} {
		rl, vars := m.find(httptest.NewRequest(c.method, c.path, nil))
		switch {
		case c.want == "" && rl != nil:
			t.Errorf("%s %s matched %s", c.method, c.path, rl.Method)
		case c.want != "" && (rl == nil || rl.Method != c.want):
			t.Errorf("%s %s: got %v, want %s", c.method, c.path, rl, c.want)
		case c.name != "" && vars["name"] != c.name:
			t.Errorf("%s %s: name = %q, want %q", c.method, c.path, vars["name"], c.name)
		}
	}
}

func TestTokens(t *testing.T) {
	tok := AccessToken("a@b", "s1 s2", time.Hour)
	got, ok := ParseAccessToken(tok)
	if !ok || got.Email != "a@b" || got.Scope != "s1 s2" || time.Until(got.Expires) < 59*time.Minute {
		t.Fatalf("%+v %v", got, ok)
	}
	for _, bad := range []string{"", "ya29.x", tok[:len(tok)-2] + "AA", AccessToken("a", "", -time.Hour), "other" + tok[len(tokenPrefix):]} {
		if _, ok := ParseAccessToken(bad); ok {
			t.Errorf("accepted %q", bad)
		}
	}
	j, err := ParseJWT(OIDCToken("a@b", "aud"))
	if err != nil || !j.Verify(PublicKey()) || j.Str("aud") != "aud" || j.Header["kid"] != KeyID {
		t.Fatalf("%v %+v", err, j)
	}
}

// Page takes an int32, so a page size above MaxInt32 is refused instead of wrapping negative and skipping paging.
func TestPageSizeOK(t *testing.T) {
	for v, want := range map[string]bool{"": true, "0": true, "50": true, "2147483647": true, "2147483648": false, "3000000000": false, "-1": false, "x": false} {
		if got := PageSizeOK(v); got != want {
			t.Errorf("PageSizeOK(%q) = %v, want %v", v, got, want)
		}
	}
}
