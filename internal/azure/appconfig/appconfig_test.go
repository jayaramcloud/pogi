package appconfig_test

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azappconfig"
	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/azure/appconfig"
	"github.com/nrndev/pogi/internal/core"
)

var ctx = context.Background()

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func wantStatus(t *testing.T, err error, status int) {
	t.Helper()
	var re *azcore.ResponseError
	if !errors.As(err, &re) || re.StatusCode != status {
		t.Fatalf("got %v, want HTTP %d", err, status)
	}
}

func start(t *testing.T, cfg pogi.Config) (*pogi.Running, *appconfig.Service) {
	t.Helper()
	cfg.Services = []string{"azappconfig", "keyvault"}
	s := pogi.Start(t, cfg)
	svc := s.Hub.Service("azappconfig").(*appconfig.Service)
	if err := svc.CreateStore("conf"); err != nil {
		t.Fatal(err)
	}
	return s, svc
}

func client(t *testing.T, svc *appconfig.Service, opts *azappconfig.ClientOptions) *azappconfig.Client {
	t.Helper()
	return must(azappconfig.NewClientFromConnectionString(must(svc.ConnectionString("conf")), opts))
}

func all[T any](t *testing.T, pager *runtime.Pager[T], each func(T)) {
	t.Helper()
	for pager.More() {
		each(must(pager.NextPage(ctx)))
	}
}

func keys(t *testing.T, c *azappconfig.Client, sel azappconfig.SettingSelector) []string {
	t.Helper()
	var out []string
	all(t, c.NewListSettingsPager(sel, nil), func(p azappconfig.ListSettingsPageResponse) {
		for _, s := range p.Settings {
			label := ""
			if s.Label != nil {
				label = "/" + *s.Label
			}
			out = append(out, *s.Key+label)
		}
	})
	return out
}

func TestSettings(t *testing.T) {
	_, svc := start(t, pogi.Config{})
	c := client(t, svc, nil)

	set := must(c.SetSetting(ctx, "app/color", to.Ptr("blue"), &azappconfig.SetSettingOptions{
		ContentType: to.Ptr("text/plain"), Label: to.Ptr("prod")}))
	if *set.Value != "blue" || *set.Label != "prod" || *set.ContentType != "text/plain" || set.ETag == nil || set.LastModified == nil || *set.IsReadOnly {
		t.Fatalf("set: %+v", set.Setting)
	}
	got := must(c.GetSetting(ctx, "app/color", &azappconfig.GetSettingOptions{Label: to.Ptr("prod")}))
	if *got.Value != "blue" || *got.ETag != *set.ETag || got.SyncToken == "" {
		t.Fatalf("get: %+v", got)
	}
	_, err := c.GetSetting(ctx, "app/color", nil) // the null label has nothing
	wantStatus(t, err, 404)
	_, err = c.GetSetting(ctx, "nope", nil)
	wantStatus(t, err, 404)

	// null label, tags, a value-less setting
	must(c.SetSetting(ctx, "app/color", to.Ptr("red"), nil))
	must(c.SetSetting(ctx, "app/size", to.Ptr("L"), &azappconfig.SetSettingOptions{Label: to.Ptr("dev")}))
	must(c.SetSetting(ctx, "db/url", nil, nil))
	if g := must(c.GetSetting(ctx, "db/url", nil)); g.Value != nil {
		t.Fatalf("a setting without a value has %q", *g.Value)
	}

	for _, tc := range []struct {
		key, label string
		want       string
	}{
		{"", "", "app/color app/color/prod app/size/dev db/url"},
		{"app/*", "", "app/color app/color/prod app/size/dev"},
		{"app/color", "*", "app/color app/color/prod"},
		{"*color", "", "app/color app/color/prod"},
		{"*/s*", "", "app/size/dev"},
		{"app/color", `\0`, "app/color"},
		{"app/color", "prod", "app/color/prod"},
		{"app/size,db/url", "", "app/size/dev db/url"},
		{"app/color", `\0,prod`, "app/color app/color/prod"},
		{"app/*", "d*", "app/size/dev"},
		{`app/co\*`, "", ""},
	} {
		sel := azappconfig.SettingSelector{}
		if tc.key != "" {
			sel.KeyFilter = to.Ptr(tc.key)
		}
		if tc.label != "" {
			sel.LabelFilter = to.Ptr(tc.label)
		}
		if got := strings.Join(keys(t, c, sel), " "); got != tc.want {
			t.Errorf("key %q label %q: %q, want %q", tc.key, tc.label, got, tc.want)
		}
	}

	// $select
	all(t, c.NewListSettingsPager(azappconfig.SettingSelector{KeyFilter: to.Ptr("app/size"),
		Fields: []azappconfig.SettingFields{azappconfig.SettingFieldsKey, azappconfig.SettingFieldsValue}}, nil), func(p azappconfig.ListSettingsPageResponse) {
		if s := p.Settings[0]; *s.Value != "L" || s.Label != nil || s.ETag != nil {
			t.Fatalf("selected: %+v", s)
		}
	})

	// ETag conditions
	_, err = c.AddSetting(ctx, "app/color", to.Ptr("green"), &azappconfig.AddSettingOptions{Label: to.Ptr("prod")})
	wantStatus(t, err, 412)
	fresh := must(c.AddSetting(ctx, "app/new", to.Ptr("1"), nil))
	stale := *set.ETag
	must(c.SetSetting(ctx, "app/color", to.Ptr("navy"), &azappconfig.SetSettingOptions{Label: to.Ptr("prod")}))
	_, err = c.SetSetting(ctx, "app/color", to.Ptr("teal"), &azappconfig.SetSettingOptions{Label: to.Ptr("prod"), OnlyIfUnchanged: &stale})
	wantStatus(t, err, 412)
	if g := must(c.GetSetting(ctx, "app/color", &azappconfig.GetSettingOptions{Label: to.Ptr("prod")})); *g.Value != "navy" {
		t.Fatalf("a failed conditional write changed the value: %q", *g.Value)
	}
	cur := must(c.GetSetting(ctx, "app/color", &azappconfig.GetSettingOptions{Label: to.Ptr("prod")}))
	must(c.SetSetting(ctx, "app/color", to.Ptr("teal"), &azappconfig.SetSettingOptions{Label: to.Ptr("prod"), OnlyIfUnchanged: cur.ETag}))
	_, err = c.GetSetting(ctx, "app/new", &azappconfig.GetSettingOptions{OnlyIfChanged: fresh.ETag})
	wantStatus(t, err, 304)
	_, err = c.DeleteSetting(ctx, "app/new", &azappconfig.DeleteSettingOptions{OnlyIfUnchanged: to.Ptr(azcore.ETag("stale"))})
	wantStatus(t, err, 412)

	// delete
	del := must(c.DeleteSetting(ctx, "app/new", nil))
	if *del.Value != "1" {
		t.Fatalf("delete returned %+v", del.Setting)
	}
	_, err = c.GetSetting(ctx, "app/new", nil)
	wantStatus(t, err, 404)
	if _, err := c.DeleteSetting(ctx, "app/new", nil); err != nil { // deleting again is fine
		t.Fatal(err)
	}

	// tags round-trip through the pager
	all(t, c.NewListSettingsPager(azappconfig.SettingSelector{KeyFilter: to.Ptr("app/size")}, nil), func(p azappconfig.ListSettingsPageResponse) {
		if len(p.Settings) != 1 || p.ETag == nil {
			t.Fatalf("page: %+v", p)
		}
	})
}

func TestTagsAndPaging(t *testing.T) {
	_, svc := start(t, pogi.Config{})
	c := client(t, svc, nil)
	for i := range 250 {
		must(c.SetSetting(ctx, fmt.Sprintf("item/%03d", i), to.Ptr("v"), nil))
	}
	pages, n := 0, 0
	all(t, c.NewListSettingsPager(azappconfig.SettingSelector{KeyFilter: to.Ptr("item/*")}, nil), func(p azappconfig.ListSettingsPageResponse) {
		pages++
		n += len(p.Settings)
	})
	if pages != 3 || n != 250 {
		t.Fatalf("%d settings on %d pages", n, pages)
	}

	// tags are not in the SDK's setters: PUT them with plain HTTP, read them back with the SDK
	req, _ := http.NewRequest("PUT", svc.Endpoint("conf")+"/kv/tagged?api-version=1.0", strings.NewReader(`{"value":"x","tags":{"team":"core","tier":"1"}}`))
	req.Header.Set("Authorization", "Bearer anything")
	if resp, err := http.DefaultClient.Do(req); err != nil || resp.StatusCode != 200 {
		t.Fatalf("put with tags: %v %v", resp, err)
	}
	if g := must(c.GetSetting(ctx, "tagged", nil)); g.Tags["team"] != "core" || g.Tags["tier"] != "1" {
		t.Fatalf("tags: %v", g.Tags)
	}

	// keys and labels
	must(c.SetSetting(ctx, "item/000", to.Ptr("v"), &azappconfig.SetSettingOptions{Label: to.Ptr("blue")}))
	body := func(path string) map[string]any {
		t.Helper()
		req, _ := http.NewRequest("GET", svc.Endpoint("conf")+path, nil)
		req.Header.Set("Authorization", "Bearer anything")
		resp, err := http.DefaultClient.Do(req)
		if err != nil || resp.StatusCode != 200 {
			t.Fatalf("GET %s: %v %v", path, resp, err)
		}
		defer resp.Body.Close()
		var m map[string]any
		json.NewDecoder(resp.Body).Decode(&m)
		return m
	}
	if m := body("/keys?name=tag*"); len(m["items"].([]any)) != 1 {
		t.Fatalf("keys: %v", m)
	}
	if m := body("/labels?api-version=1.0"); len(m["items"].([]any)) != 1 || m["items"].([]any)[0].(map[string]any)["name"] != "blue" {
		t.Fatalf("labels: %v", m)
	}
}

func TestLocksAndRevisions(t *testing.T) {
	_, svc := start(t, pogi.Config{})
	c := client(t, svc, nil)
	first := must(c.SetSetting(ctx, "flag", to.Ptr("1"), nil))
	time.Sleep(1100 * time.Millisecond) // Accept-Datetime has a resolution of a second
	between := time.Now()
	time.Sleep(10 * time.Millisecond)
	must(c.SetSetting(ctx, "flag", to.Ptr("2"), nil))

	locked := must(c.SetReadOnly(ctx, "flag", true, nil))
	if !*locked.IsReadOnly || *locked.ETag == *first.ETag {
		t.Fatalf("locked: %+v", locked.Setting)
	}
	if g := must(c.GetSetting(ctx, "flag", nil)); !*g.IsReadOnly {
		t.Fatal("a locked setting is not read-only")
	}
	_, err := c.SetSetting(ctx, "flag", to.Ptr("3"), nil)
	wantStatus(t, err, 409)
	_, err = c.DeleteSetting(ctx, "flag", nil)
	wantStatus(t, err, 409)
	_, err = c.SetReadOnly(ctx, "ghost", true, nil)
	wantStatus(t, err, 404)
	_, err = c.SetReadOnly(ctx, "flag", false, &azappconfig.SetReadOnlyOptions{OnlyIfUnchanged: to.Ptr(azcore.ETag("stale"))})
	wantStatus(t, err, 412)
	if un := must(c.SetReadOnly(ctx, "flag", false, nil)); *un.IsReadOnly {
		t.Fatal("still locked")
	}
	must(c.SetSetting(ctx, "flag", to.Ptr("3"), nil))
	must(c.SetSetting(ctx, "other", to.Ptr("x"), nil))

	var values []string
	all(t, c.NewListRevisionsPager(azappconfig.SettingSelector{KeyFilter: to.Ptr("flag")}, nil), func(p azappconfig.ListRevisionsPageResponse) {
		for _, s := range p.Settings {
			values = append(values, *s.Value)
		}
	})
	if strings.Join(values, ",") != "1,2,2,2,3" { // two writes, lock, unlock, a write
		t.Fatalf("revisions: %v", values)
	}

	// Accept-Datetime sees the setting as it was
	old := must(c.GetSetting(ctx, "flag", &azappconfig.GetSettingOptions{AcceptDateTime: &between}))
	if *old.Value != "1" {
		t.Fatalf("flag at %s: %q", between, *old.Value)
	}
	before := first.LastModified.Add(-time.Second)
	_, err = c.GetSetting(ctx, "flag", &azappconfig.GetSettingOptions{AcceptDateTime: &before})
	wantStatus(t, err, 404)
	var atKeys []string
	all(t, c.NewListSettingsPager(azappconfig.SettingSelector{AcceptDateTime: &between}, nil), func(p azappconfig.ListSettingsPageResponse) {
		for _, s := range p.Settings {
			atKeys = append(atKeys, *s.Key+"="+*s.Value)
		}
	})
	if strings.Join(atKeys, ",") != "flag=1" {
		t.Fatalf("settings at %s: %v", between, atKeys)
	}

	// a deleted setting leaves its history
	must(c.DeleteSetting(ctx, "other", nil))
	n := 0
	all(t, c.NewListRevisionsPager(azappconfig.SettingSelector{KeyFilter: to.Ptr("other")}, nil), func(p azappconfig.ListRevisionsPageResponse) { n += len(p.Settings) })
	if n != 1 {
		t.Fatalf("revisions of a deleted setting: %d", n)
	}
}

func TestSnapshots(t *testing.T) {
	_, svc := start(t, pogi.Config{})
	c := client(t, svc, nil)
	must(c.SetSetting(ctx, "app/a", to.Ptr("1"), nil))
	must(c.SetSetting(ctx, "app/b", to.Ptr("2"), &azappconfig.SetSettingOptions{Label: to.Ptr("v1")}))
	must(c.SetSetting(ctx, "other", to.Ptr("3"), nil))

	poller := must(c.BeginCreateSnapshot(ctx, "release-1", []azappconfig.SettingFilter{{KeyFilter: to.Ptr("app/*")}}, &azappconfig.BeginCreateSnapshotOptions{
		CompositionType: to.Ptr(azappconfig.CompositionTypeKeyLabel), Tags: map[string]*string{"by": to.Ptr("ci")}}))
	snap := must(poller.PollUntilDone(ctx, &runtime.PollUntilDoneOptions{Frequency: time.Second}))
	if *snap.Name != "release-1" || *snap.Status != azappconfig.SnapshotStatusReady || *snap.ItemsCount != 2 || *snap.Size == 0 {
		t.Fatalf("snapshot: %+v", snap.Snapshot)
	}

	// later changes do not reach it
	must(c.SetSetting(ctx, "app/a", to.Ptr("changed"), nil))
	must(c.DeleteSetting(ctx, "app/b", &azappconfig.DeleteSettingOptions{Label: to.Ptr("v1")}))
	var got []string
	all(t, c.NewListSettingsForSnapshotPager("release-1", nil), func(p azappconfig.ListSettingsForSnapshotResponse) {
		for _, s := range p.Settings {
			got = append(got, *s.Key+"="+*s.Value)
		}
	})
	if strings.Join(got, ",") != "app/a=1,app/b=2" {
		t.Fatalf("snapshot settings: %v", got)
	}
	g := must(c.GetSnapshot(ctx, "release-1", nil))
	if g.Tags["by"] == nil || *g.Tags["by"] != "ci" || len(g.Filters) != 1 || *g.Filters[0].KeyFilter != "app/*" || *g.CompositionType != azappconfig.CompositionTypeKeyLabel {
		t.Fatalf("get: %+v", g.Snapshot)
	}
	_, err := c.GetSnapshot(ctx, "nope", nil)
	wantStatus(t, err, 404)
	_, err = c.BeginCreateSnapshot(ctx, "release-1", []azappconfig.SettingFilter{{KeyFilter: to.Ptr("*")}}, nil)
	wantStatus(t, err, 409)

	// key composition refuses a key under two labels
	must(c.SetSetting(ctx, "dup", to.Ptr("1"), &azappconfig.SetSettingOptions{Label: to.Ptr("a")}))
	must(c.SetSetting(ctx, "dup", to.Ptr("2"), &azappconfig.SetSettingOptions{Label: to.Ptr("b")}))
	p2 := must(c.BeginCreateSnapshot(ctx, "dups", []azappconfig.SettingFilter{{KeyFilter: to.Ptr("dup")}}, nil))
	if _, err := p2.PollUntilDone(ctx, &runtime.PollUntilDoneOptions{Frequency: time.Second}); err == nil {
		t.Fatal("a key composition snapshot took a key twice")
	}

	// list, archive, recover
	var names []string
	all(t, c.NewListSnapshotsPager(nil), func(p azappconfig.ListSnapshotsResponse) {
		for _, s := range p.Snapshots {
			names = append(names, *s.Name+":"+string(*s.Status))
		}
	})
	if strings.Join(names, ",") != "dups:failed,release-1:ready" {
		t.Fatalf("snapshots: %v", names)
	}
	ready := 0
	all(t, c.NewListSnapshotsPager(&azappconfig.ListSnapshotsOptions{Status: []azappconfig.SnapshotStatus{azappconfig.SnapshotStatusReady}}), func(p azappconfig.ListSnapshotsResponse) { ready += len(p.Snapshots) })
	if ready != 1 {
		t.Fatalf("ready snapshots: %d", ready)
	}
	arch := must(c.ArchiveSnapshot(ctx, "release-1", nil))
	if *arch.Status != azappconfig.SnapshotStatusArchived || arch.Expires == nil {
		t.Fatalf("archived: %+v", arch)
	}
	_, err = c.ArchiveSnapshot(ctx, "release-1", nil)
	wantStatus(t, err, 409)
	rec := must(c.RecoverSnapshot(ctx, "release-1", nil))
	if *rec.Status != azappconfig.SnapshotStatusReady || rec.Expires != nil {
		t.Fatalf("recovered: %+v", rec)
	}
}

func TestFeatureFlag(t *testing.T) {
	_, svc := start(t, pogi.Config{})
	c := client(t, svc, nil)
	const ct = "application/vnd.microsoft.appconfig.ff+json;charset=utf-8"
	flag := `{"id":"Beta","description":"","enabled":true,"conditions":{"client_filters":[{"name":"Microsoft.Percentage","parameters":{"Value":50}}]}}`
	key := ".appconfig.featureflag/Beta" // the SDK escapes the "/" in the path
	must(c.SetSetting(ctx, key, &flag, &azappconfig.SetSettingOptions{ContentType: to.Ptr(ct), Label: to.Ptr("prod")}))
	g := must(c.GetSetting(ctx, key, &azappconfig.GetSettingOptions{Label: to.Ptr("prod")}))
	var back struct {
		ID         string
		Enabled    bool
		Conditions struct{ Client_filters []struct{ Name string } }
	}
	if err := json.Unmarshal([]byte(*g.Value), &back); err != nil || *g.ContentType != ct || back.ID != "Beta" || !back.Enabled || back.Conditions.Client_filters[0].Name != "Microsoft.Percentage" {
		t.Fatalf("flag: %v %+v", err, g.Setting)
	}
	n := 0
	all(t, c.NewListSettingsPager(azappconfig.SettingSelector{KeyFilter: to.Ptr(".appconfig.featureflag/*")}, nil), func(p azappconfig.ListSettingsPageResponse) {
		for _, s := range p.Settings {
			if *s.ContentType == ct {
				n++
			}
		}
	})
	if n != 1 {
		t.Fatalf("feature flags listed: %d", n)
	}
}

// TestHMAC runs the SDK against a server that checks signatures, with the
// store's own keys, a wrong secret, a read-only key and bearer tokens.
func TestHMAC(t *testing.T) {
	_, svc := start(t, pogi.Config{ValidateSigs: true})
	c := client(t, svc, nil)
	must(c.SetSetting(ctx, "k", to.Ptr("v"), nil))
	if g := must(c.GetSetting(ctx, "k", nil)); *g.Value != "v" {
		t.Fatalf("get: %+v", g)
	}

	keys := must(svc.AccessKeys("conf"))
	cs := func(k appconfig.AccessKey, secret string) string {
		return "Endpoint=" + svc.Endpoint("conf") + ";Id=" + k.ID + ";Secret=" + secret
	}
	bad := must(azappconfig.NewClientFromConnectionString(cs(keys[0], base64.StdEncoding.EncodeToString([]byte("not the secret"))), nil))
	_, err := bad.GetSetting(ctx, "k", nil)
	wantStatus(t, err, 401)
	unknown := must(azappconfig.NewClientFromConnectionString(cs(appconfig.AccessKey{ID: "nope"}, keys[0].Secret), nil))
	_, err = unknown.GetSetting(ctx, "k", nil)
	wantStatus(t, err, 401)

	ro := must(azappconfig.NewClientFromConnectionString(cs(keys[2], keys[2].Secret), nil))
	if !keys[2].ReadOnly {
		t.Fatal("key 2 is not read-only")
	}
	if g := must(ro.GetSetting(ctx, "k", nil)); *g.Value != "v" {
		t.Fatalf("read-only get: %+v", g)
	}
	_, err = ro.SetSetting(ctx, "k", to.Ptr("w"), nil)
	wantStatus(t, err, 403)

	// a body the signature did not cover, a replayed old date and no credentials
	req := func(mod func(*http.Request)) int {
		r, _ := http.NewRequest("PUT", svc.Endpoint("conf")+"/kv/x?api-version=1.0", strings.NewReader(`{"value":"1"}`))
		sign(r, keys[0], time.Now())
		mod(r)
		resp, err := http.DefaultClient.Do(r)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	if code := req(func(*http.Request) {}); code != 200 {
		t.Fatalf("hand-signed request: %d", code)
	}
	if code := req(func(r *http.Request) { r.Body = io.NopCloser(strings.NewReader(`{"value":"2"}`)); r.ContentLength = 13 }); code != 401 {
		t.Fatalf("tampered body: %d", code)
	}
	if code := req(func(r *http.Request) { sign(r, keys[0], time.Now().Add(-time.Hour)) }); code != 401 {
		t.Fatalf("old date: %d", code)
	}
	if code := req(func(r *http.Request) { r.Header.Del("Authorization") }); code != 401 {
		t.Fatalf("no credentials: %d", code)
	}
	if code := req(func(r *http.Request) { r.Header.Set("Authorization", "Bearer eyJ.any.token") }); code != 200 {
		t.Fatalf("bearer token: %d", code)
	}
}

// sign adds the HMAC-SHA256 headers of the REST authentication scheme
// (x-ms-date flavour, "&"-separated) to r.
func sign(r *http.Request, k appconfig.AccessKey, now time.Time) {
	body := []byte{}
	if r.Body != nil {
		body, _ = io.ReadAll(r.Body)
		r.Body = io.NopCloser(strings.NewReader(string(body)))
	}
	sum := sha256.Sum256(body)
	hash := base64.StdEncoding.EncodeToString(sum[:])
	date := now.UTC().Format(http.TimeFormat)
	secret, _ := base64.StdEncoding.DecodeString(k.Secret)
	mac := hmac.New(sha256.New, secret)
	fmt.Fprintf(mac, "%s\n%s\n%s;%s;%s", r.Method, r.URL.RequestURI(), date, r.URL.Host, hash)
	r.Header.Set("x-ms-date", date)
	r.Header.Set("x-ms-content-sha256", hash)
	r.Header.Set("Authorization", "HMAC-SHA256 Credential="+k.ID+"&SignedHeaders=x-ms-date;host;x-ms-content-sha256&Signature="+base64.StdEncoding.EncodeToString(mac.Sum(nil)))
}

// TestRouting reaches a store by host, by path and by its HMAC credential
// alone, and checks that neither Key Vault nor the store trade requests.
func TestRouting(t *testing.T) {
	s, svc := start(t, pogi.Config{})
	keys := must(svc.AccessKeys("conf"))
	addr := strings.TrimPrefix(s.URL, "http://")

	// <store>.azconfig.localhost: every name dials pogi, as *.localhost would resolve to it
	dial := &http.Transport{DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, addr)
	}}
	hostCS := "Endpoint=http://conf.azconfig.localhost:" + addr[strings.LastIndexByte(addr, ':')+1:] + ";Id=" + keys[0].ID + ";Secret=" + keys[0].Secret
	byHost := must(azappconfig.NewClientFromConnectionString(hostCS, &azappconfig.ClientOptions{ClientOptions: azcore.ClientOptions{Transport: &http.Client{Transport: dial}}}))
	must(byHost.SetSetting(ctx, "via", to.Ptr("host"), nil))

	// the path form is what ConnectionString gives
	if g := must(client(t, svc, nil).GetSetting(ctx, "via", nil)); *g.Value != "host" {
		t.Fatalf("path form: %+v", g)
	}

	// the main address with HMAC credentials and no store name: the credential finds the store
	bare := must(azappconfig.NewClientFromConnectionString("Endpoint="+s.URL+";Id="+keys[0].ID+";Secret="+keys[0].Secret, nil))
	if g := must(bare.GetSetting(ctx, "via", nil)); *g.Value != "host" {
		t.Fatalf("by credential: %+v", g)
	}

	// Key Vault's /keys with a bearer token still goes to Key Vault, App Configuration's to App Configuration
	get := func(host, path, auth string) (int, string) {
		req, _ := http.NewRequest("GET", s.URL+path, nil)
		if host != "" {
			req.Host = host
		}
		req.Header.Set("Authorization", auth)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}
	if code, body := get("", "/keys?api-version=7.4", "Bearer token"); code != 200 || !strings.Contains(body, `"value"`) {
		t.Fatalf("key vault keys: %d %s", code, body)
	}
	if code, body := get("conf.azconfig.localhost", "/keys?api-version=1.0", "Bearer token"); code != 200 || !strings.Contains(body, `"name":"via"`) {
		t.Fatalf("app configuration keys: %d %s", code, body)
	}
	// without credentials pogi's lenient mode answers for a store it does not know yet
	if code, _ := get("fresh.azconfig.localhost", "/kv?api-version=1.0", "Bearer x"); code != 200 || !contains(svc.Stores(), "fresh") {
		t.Fatalf("unknown store: %d %v", code, svc.Stores())
	}
}

func contains(l []string, s string) bool {
	for _, x := range l {
		if x == s {
			return true
		}
	}
	return false
}

// TestStoresAndState covers the Go side the resource manager uses, the
// console and persistence.
func TestStoresAndState(t *testing.T) {
	s, svc := start(t, pogi.Config{})
	if err := svc.CreateStore("bad name"); err == nil {
		t.Fatal("a store name with a space")
	}
	c := client(t, svc, nil)
	must(c.SetSetting(ctx, "a", to.Ptr("1"), nil))
	cs := must(svc.ConnectionString("conf"))
	if !strings.HasPrefix(cs, "Endpoint="+s.URL+"/_azure/appconfig/conf;Id=") || !strings.Contains(cs, ";Secret=") {
		t.Fatalf("connection string %q", cs)
	}
	if _, err := svc.ConnectionString("ghost"); err == nil {
		t.Fatal("connection string of a missing store")
	}
	svc.CreateStore("other")
	if got := strings.Join(svc.Stores(), ","); got != "conf,other" {
		t.Fatalf("stores %s", got)
	}
	if keys := must(svc.AccessKeys("conf")); len(keys) != 4 || keys[0].ReadOnly || !keys[3].ReadOnly {
		t.Fatalf("keys %+v", keys)
	}
	// the same key again is left alone; seeding does not add revisions
	one, two := "1", "2"
	svc.SetKeyValue("conf", "a", "", &two, nil, nil)
	svc.SetKeyValue("other", "seeded", "x", &one, nil, map[string]string{"t": "1"})
	if g := must(c.GetSetting(ctx, "a", nil)); *g.Value != "1" {
		t.Fatalf("a seed overwrote a setting: %q", *g.Value)
	}

	// console
	res := svc.Resources()
	if len(res[0].Rows) != 2 || len(res[1].Rows) != 2 {
		t.Fatalf("resources: %v", res)
	}
	if in, _ := json.Marshal(svc.Inspect("Stores", "conf")); !strings.Contains(string(in), "connectionString") {
		t.Fatalf("inspect store: %s", in)
	}
	if in, _ := json.Marshal(svc.Inspect("Key-values", "other/seeded?label=x")); !strings.Contains(string(in), `"seeded"`) {
		t.Fatalf("inspect key-value: %s", in)
	}

	// save, reset, load: settings, history, stores and keys survive
	keysBefore := must(svc.AccessKeys("conf"))
	b := must(svc.Save())
	svc.Reset()
	if len(svc.Stores()) != 0 {
		t.Fatal("reset left stores")
	}
	if err := svc.Load(b); err != nil {
		t.Fatal(err)
	}
	if g := must(c.GetSetting(ctx, "a", nil)); *g.Value != "1" {
		t.Fatalf("after load: %+v", g)
	}
	if k := must(svc.AccessKeys("conf")); k[0] != keysBefore[0] {
		t.Fatal("access keys changed over a save and load")
	}
	if !svc.DeleteStore("other") || svc.DeleteStore("other") {
		t.Fatal("DeleteStore")
	}
	var _ core.Service = svc
}
