package web_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/core"
)

// console drives the console's HTTP handlers like a browser: cookies, redirects, forms.
type console struct {
	t    *testing.T
	base string
	c    *http.Client
	srv  *pogi.Running
}

func start(t *testing.T, services ...string) *console {
	t.Helper()
	srv := pogi.Start(t, pogi.Config{Services: services})
	jar, _ := cookiejar.New(nil)
	return &console{t, srv.URL, &http.Client{Jar: jar}, srv}
}

func (c *console) get(path string) string {
	c.t.Helper()
	res, err := c.c.Get(c.base + path)
	if err != nil {
		c.t.Fatal(err)
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	if res.StatusCode != 200 {
		c.t.Fatalf("GET %s = %d: %s", path, res.StatusCode, b)
	}
	return string(b)
}

// post submits a form and returns the page it ends on (redirects followed) and its URL path.
func (c *console) post(path string, v url.Values) (body, final string) {
	c.t.Helper()
	res, err := c.c.PostForm(c.base+path, v)
	if err != nil {
		c.t.Fatal(err)
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	if res.StatusCode != 200 {
		c.t.Fatalf("POST %s = %d: %s", path, res.StatusCode, b)
	}
	return string(b), res.Request.URL.Path
}

// ok posts a form that must succeed: it lands on the resources page with no error flash.
func (c *console) ok(path string, v url.Values) string {
	c.t.Helper()
	body, final := c.post(path, v)
	if strings.Contains(body, `class="flash"`) {
		c.t.Fatalf("POST %s: %s", path, flash(body))
	}
	if !strings.HasPrefix(final, "/_pogi/resources/") {
		c.t.Fatalf("POST %s ended on %s, not the resources page: %s", path, final, flash(body))
	}
	return body
}

func flash(body string) string {
	_, rest, _ := strings.Cut(body, `class="flash"`)
	msg, _, _ := strings.Cut(rest, "</div>")
	return msg
}

func want(t *testing.T, page string, subs ...string) {
	t.Helper()
	for _, s := range subs {
		if !strings.Contains(page, s) {
			t.Fatalf("page lacks %q:\n%s", s, page)
		}
	}
}

func wantNot(t *testing.T, page string, subs ...string) {
	t.Helper()
	for _, s := range subs {
		if strings.Contains(page, s) {
			t.Fatalf("page has %q:\n%s", s, page)
		}
	}
}

// rowOf finds the encoded checkbox of the row that has name in it.
func rowOf(t *testing.T, page, name string) string {
	t.Helper()
	for _, part := range strings.Split(page, "<tr") {
		if strings.Contains(part, ">"+name+"<") || strings.Contains(part, name+"</a>") {
			_, rest, ok := strings.Cut(part, `name="row" value="`)
			if ok {
				v, _, _ := strings.Cut(rest, `"`)
				return v
			}
		}
	}
	t.Fatalf("no row with %q in\n%s", name, page)
	return ""
}

func TestGenericServiceForm(t *testing.T) {
	c := start(t, "codeartifact")
	// the service's page offers the model's create and update calls
	page := c.get("/_pogi/resources/codeartifact")
	want(t, page, "/_pogi/new/codeartifact/CreateDomain", "/_pogi/new/codeartifact/CreateRepository", "/_pogi/new/codeartifact/UpdateRepository")

	// a form: required members marked, nested lists repeatable, enums as selects, labels from member names
	form := c.get("/_pogi/new/codeartifact/CreateRepository")
	want(t, form, `name="domain"`, `name="repository"`, "required", "Domain", `class="req"`, `hx-get="/_pogi/new/codeartifact/CreateRepository/item?path=upstreams&amp;n=0"`)
	// the add button answers with one more fieldset and the button for the next
	item := c.get("/_pogi/new/codeartifact/CreateRepository/item?path=upstreams&n=0")
	want(t, item, `name="upstreams.0.repositoryName"`, `item?path=upstreams&amp;n=1`)
	item = c.get("/_pogi/new/codeartifact/CreateRepository/item?path=tags&n=2")
	want(t, item, `name="tags.2.key"`, `name="tags.2.value"`)

	// submit it: the domain is in the table, with its tag
	c.ok("/_pogi/new/codeartifact/CreateDomain", url.Values{"domain": {"acme"}, "tags.0.key": {"env"}, "tags.0.value": {"dev"}})
	page = c.get("/_pogi/resources/codeartifact")
	want(t, page, "acme", "Domain")
	g, _ := c.srv.Hub.Service("codeartifact").(core.Inspectable)
	if v, _ := json.Marshal(g.Inspect("Domain", rowArn(t, page, "acme"))); !strings.Contains(string(v), `"env":"dev"`) {
		t.Fatalf("domain = %s", v)
	}
	c.ok("/_pogi/new/codeartifact/CreateRepository", url.Values{"domain": {"acme"}, "repository": {"npm-store"}, "upstreams.0.repositoryName": {"npm-mirror"}})
	want(t, c.get("/_pogi/resources/codeartifact?tab=Repository"), "npm-store")

	// API errors come back inline, the form keeps what was typed
	body, final := c.post("/_pogi/new/codeartifact/CreateDomain", url.Values{"domain": {"acme"}})
	if final != "/_pogi/new/codeartifact/CreateDomain" {
		t.Fatalf("ended on %s", final)
	}
	want(t, body, `id="apierr"`, "ConflictException", `value="acme"`)
	// so do values that are not what the field takes
	body, _ = c.post("/_pogi/new/codeartifact/CreateRepository", url.Values{"domain": {"acme"}, "repository": {"x"}, "upstreams.0.repositoryName": {"u"}, "tags.0.key": {"k"}})
	want(t, body, `id="apierr"`)
}

// rowArn is the first cell of the table row that mentions name.
func rowArn(t *testing.T, page, name string) string {
	t.Helper()
	for _, part := range strings.Split(page, "<tr") {
		if strings.Contains(part, name) {
			_, rest, ok := strings.Cut(part, `data-drawer="/_pogi/resources/codeartifact/Domain/`)
			if ok {
				v, _, _ := strings.Cut(rest, `"`)
				u, _ := url.PathUnescape(v)
				return u
			}
		}
	}
	t.Fatalf("no row for %q", name)
	return ""
}

func TestSQSForm(t *testing.T) {
	c := start(t, "sqs")
	page := c.get("/_pogi/resources/sqs")
	want(t, page, "/_pogi/new/sqs/CreateQueue")
	form := c.get("/_pogi/new/sqs/CreateQueue")
	want(t, form, `name="QueueName"`, "required", "Queue Name", `path=Attributes&amp;n=0`)
	// map entries are a key and a value
	want(t, c.get("/_pogi/new/sqs/CreateQueue/item?path=Attributes&n=0"), `name="Attributes.0.k"`, `name="Attributes.0.v"`)

	c.ok("/_pogi/new/sqs/CreateQueue", url.Values{"QueueName": {"orders"}, "Attributes.0.k": {"VisibilityTimeout"}, "Attributes.0.v": {"45"}})
	page = c.get("/_pogi/resources/sqs")
	want(t, page, "orders")
	q, _ := c.srv.Hub.Service("sqs").(core.Inspectable)
	if v, _ := json.Marshal(q.Inspect("Queues", "orders")); !strings.Contains(string(v), `"VisibilityTimeout":"45"`) {
		t.Fatalf("queue = %s", v)
	}

	// the queue's API error shows inline
	body, _ := c.post("/_pogi/new/sqs/CreateQueue", url.Values{"QueueName": {"not a valid name!"}})
	want(t, body, `id="apierr"`, "InvalidParameterValue")

	// bulk tag, then bulk delete, through SQS's own TagQueue and DeleteQueue
	c.ok("/_pogi/new/sqs/CreateQueue", url.Values{"QueueName": {"audit"}})
	page = c.get("/_pogi/resources/sqs")
	want(t, page, `name="row"`, "Delete the selected queues", "hx-get=\"/_pogi/bulk/sqs/Queues\"")
	c.ok("/_pogi/bulk/sqs/Queues", url.Values{"action": {"tag"}, "tags": {"team=core, env=dev"}, "row": {rowOf(t, page, "orders"), rowOf(t, page, "audit")}})
	if v, _ := json.Marshal(q.Inspect("Queues", "audit")); !strings.Contains(string(v), `"team":"core"`) {
		t.Fatalf("queue = %s", v)
	}
	c.ok("/_pogi/bulk/sqs/Queues", url.Values{"action": {"delete"}, "row": {rowOf(t, page, "orders"), rowOf(t, page, "audit")}})
	page = c.get("/_pogi/resources/sqs")
	wantNot(t, page, ">orders<", ">audit<")
}

func TestBulkGeneric(t *testing.T) {
	c := start(t, "codeartifact")
	for _, d := range []string{"alpha", "beta", "gamma"} {
		c.ok("/_pogi/new/codeartifact/CreateDomain", url.Values{"domain": {d}})
	}
	page := c.get("/_pogi/resources/codeartifact")
	// tag two, delete two: through the service's TagResource and DeleteDomain
	c.ok("/_pogi/bulk/codeartifact/Domain", url.Values{"action": {"tag"}, "tags": {"owner=ops"}, "row": {rowOf(t, page, "alpha"), rowOf(t, page, "beta")}})
	g, _ := c.srv.Hub.Service("codeartifact").(core.Inspectable)
	for _, d := range []string{"alpha", "beta"} {
		if v, _ := json.Marshal(g.Inspect("Domain", rowArn(t, page, d))); !strings.Contains(string(v), `"owner":"ops"`) {
			t.Fatalf("%s = %s", d, v)
		}
	}
	c.ok("/_pogi/bulk/codeartifact/Domain", url.Values{"action": {"delete"}, "row": {rowOf(t, page, "alpha"), rowOf(t, page, "gamma")}})
	page = c.get("/_pogi/resources/codeartifact")
	want(t, page, "beta")
	wantNot(t, page, "alpha", "gamma")

	// nothing ticked is not an error; a bad action is
	c.ok("/_pogi/bulk/codeartifact/Domain", url.Values{"action": {"delete"}})
	body, _ := c.post("/_pogi/bulk/codeartifact/Domain", url.Values{"action": {"tag"}, "row": {rowOf(t, page, "beta")}})
	want(t, body, `class="flash"`, "key=value")
}

func TestPubSubTopicForm(t *testing.T) {
	c := start(t, "pubsub")
	page := c.get("/_pogi/resources/pubsub")
	want(t, page, "/_pogi/new/pubsub/Publisher.CreateTopic", "/_pogi/new/pubsub/Subscriber.CreateSubscription")
	// the form is the CreateTopic request message; the name is prefilled from the pattern in its REST binding
	form := c.get("/_pogi/new/pubsub/Publisher.CreateTopic")
	want(t, form, `name="name"`, `value="projects/pogi/topics/"`, "required", `name="kmsKeyName"`, "Message Storage Policy", `name="messageRetentionDuration"`)

	c.ok("/_pogi/new/pubsub/Publisher.CreateTopic", url.Values{"name": {"projects/pogi/topics/events"}, "labels.0.k": {"env"}, "labels.0.v": {"dev"}})
	page = c.get("/_pogi/resources/pubsub")
	want(t, page, "projects/pogi/topics/events")
	p, _ := c.srv.Hub.Service("pubsub").(core.Inspectable)
	if v, _ := json.Marshal(p.Inspect("Topics", "projects/pogi/topics/events")); !strings.Contains(string(v), `"env":"dev"`) {
		t.Fatalf("topic = %s", v)
	}
	// Google's error, inline
	body, _ := c.post("/_pogi/new/pubsub/Publisher.CreateTopic", url.Values{"name": {"projects/pogi/topics/events"}})
	want(t, body, `id="apierr"`, "ALREADY_EXISTS")
	body, _ = c.post("/_pogi/new/pubsub/Publisher.CreateTopic", url.Values{"labels.0.k": {"a"}})
	want(t, body, `id="apierr"`, "name is required")

	// a subscription refers to the topic
	c.ok("/_pogi/new/pubsub/Subscriber.CreateSubscription", url.Values{"name": {"projects/pogi/subscriptions/worker"}, "topic": {"projects/pogi/topics/events"}, "ackDeadlineSeconds": {"20"}})
	subs := c.get("/_pogi/resources/pubsub?tab=Subscriptions")
	want(t, subs, "projects/pogi/subscriptions/worker")

	// bulk delete: Google's DeleteSubscription and DeleteTopic
	c.ok("/_pogi/bulk/pubsub/Subscriptions", url.Values{"action": {"delete"}, "row": {rowOf(t, subs, "projects/pogi/subscriptions/worker")}})
	c.ok("/_pogi/bulk/pubsub/Topics", url.Values{"action": {"delete"}, "row": {rowOf(t, page, "projects/pogi/topics/events")}})
	wantNot(t, c.get("/_pogi/resources/pubsub"), "projects/pogi/topics/events")
	wantNot(t, c.get("/_pogi/resources/pubsub?tab=Subscriptions"), "projects/pogi/subscriptions/worker")
}

func TestAzureResourceGroupEditor(t *testing.T) {
	c := start(t, "arm", "keyvault")
	sub := core.AzureSubscription
	page := c.get("/_pogi/resources/arm")
	want(t, page, "/_pogi/arm/new")
	// the editor starts from a template for the type
	editor := c.get("/_pogi/arm/new?type=Microsoft.Resources/resourceGroups")
	want(t, editor, "/subscriptions/"+sub+"/resourceGroups/&lt;name&gt;", "eastus", "Microsoft.KeyVault/vaults")
	want(t, c.get("/_pogi/arm/new?type=Microsoft.KeyVault/vaults"), "accessPolicies", "/providers/Microsoft.KeyVault/vaults/&lt;name&gt;")

	// validated: JSON, then the ID, then the round trip
	post := func(path, body string) string {
		b, _ := c.post("/_pogi/arm/new", url.Values{"type": {"Microsoft.Resources/resourceGroups"}, "path": {path}, "body": {body}})
		return b
	}
	want(t, post("/subscriptions/"+sub+"/resourceGroups/web", `{"location": `), `id="apierr"`, "not a JSON object")
	want(t, post("/subscriptions/"+sub+"/resourceGroups/<name>", `{"location": "eastus"}`), `id="apierr"`, "fill in")
	want(t, post("/subscriptions/"+sub+"/resourceGroups/web", `{"tags": {}}`), `id="apierr"`, "LocationRequired") // ARM's own error
	res := post("/subscriptions/"+sub+"/resourceGroups/web", `{"location": "eastus", "tags": {"env": "dev"}}`)
	want(t, res, `id="armresult"`, "&#34;name&#34;: &#34;web&#34;", "Succeeded")
	page = c.get("/_pogi/resources/arm?tab=Resource+groups")
	want(t, page, ">web<", "eastus")

	// any registered type: the ID must be of the chosen type, and the resource must read back
	vault := `{"location": "eastus", "properties": {"tenantId": "` + core.AzureSubscription + `", "sku": {"family": "A", "name": "standard"}, "accessPolicies": []}}`
	vpath := "/subscriptions/" + sub + "/resourceGroups/web/providers/Microsoft.KeyVault/vaults/kv1"
	b, _ := c.post("/_pogi/arm/new", url.Values{"type": {"Microsoft.KeyVault/vaults"}, "path": {"/subscriptions/" + sub + "/resourceGroups/web"}, "body": {vault}})
	want(t, b, `id="apierr"`, "is not a Microsoft.KeyVault/vaults")
	b, _ = c.post("/_pogi/arm/new", url.Values{"type": {"Microsoft.KeyVault/vaults"}, "path": {vpath}, "body": {vault}})
	want(t, b, `id="armresult"`, "kv1")

	// bulk: tag the group (a read, merge and PATCH), then delete it
	c.ok("/_pogi/bulk/arm/Resource groups", url.Values{"action": {"tag"}, "tags": {"team=core"}, "row": {rowOf(t, page, "web")}})
	g, _ := c.srv.Hub.Service("arm").(core.Inspectable)
	if v, _ := json.Marshal(g.Inspect("Resource groups", "web")); !strings.Contains(string(v), `"team":"core"`) || !strings.Contains(string(v), `"env":"dev"`) {
		t.Fatalf("group = %s", v)
	}
	c.ok("/_pogi/bulk/arm/Resource groups", url.Values{"action": {"delete"}, "row": {rowOf(t, page, "web")}})
	wantNot(t, c.get("/_pogi/resources/arm?tab=Resource+groups"), ">web<")
}

func TestSwitcher(t *testing.T) {
	c := start(t, "sqs", "pubsub", "arm")
	// defaults are pogi's own
	page := c.get("/_pogi/resources/sqs")
	want(t, page, `name="account" value="000000000000"`, `name="project" value="pogi"`, `name="subscription" value="`+core.AzureSubscription+`"`, `<option value="us-east-1" selected>us-east-1</option>`)

	c.ok("/_pogi/new/sqs/CreateQueue", url.Values{"QueueName": {"east"}})
	// switching region is kept in a cookie, and returns to the page it was used on
	body, final := c.post("/_pogi/scope", url.Values{"account": {"000000000000"}, "region": {"eu-west-1"}, "project": {"pogi"}, "subscription": {core.AzureSubscription}, "back": {"/_pogi/resources/sqs"}})
	if final != "/_pogi/resources/sqs" {
		t.Fatalf("ended on %s", final)
	}
	want(t, body, `<option value="eu-west-1" selected>eu-west-1</option>`)
	wantNot(t, body, ">east<") // the view is scoped
	// forms submit into the scope
	c.ok("/_pogi/new/sqs/CreateQueue", url.Values{"QueueName": {"west"}})
	page = c.get("/_pogi/resources/sqs")
	want(t, page, ">west<", "eu-west-1")
	wantNot(t, page, ">east<")
	// "*" shows everything; a switch back shows the first again
	body, _ = c.post("/_pogi/scope", url.Values{"region": {"*"}, "back": {"/_pogi/resources/sqs"}})
	want(t, body, ">west<", ">east<")
	body, _ = c.post("/_pogi/scope", url.Values{"region": {"us-east-1"}, "back": {"/_pogi/resources/sqs"}})
	want(t, body, ">east<")
	wantNot(t, body, ">west<")

	// the account scopes too: queues of another account are not in the default view
	c.post("/_pogi/scope", url.Values{"account": {"123456789012"}, "back": {"/_pogi/"}})
	c.ok("/_pogi/new/sqs/CreateQueue", url.Values{"QueueName": {"other"}})
	page = c.get("/_pogi/resources/sqs")
	want(t, page, ">other<", "123456789012")
	wantNot(t, page, ">east<")
	c.post("/_pogi/scope", url.Values{"account": {"000000000000"}, "back": {"/_pogi/"}})
	wantNot(t, c.get("/_pogi/resources/sqs"), ">other<")

	// the project scopes Google Cloud resources, the subscription Azure's
	c.ok("/_pogi/new/pubsub/Publisher.CreateTopic", url.Values{"name": {"projects/pogi/topics/mine"}})
	c.ok("/_pogi/new/pubsub/Publisher.CreateTopic", url.Values{"name": {"projects/elsewhere/topics/theirs"}})
	page = c.get("/_pogi/resources/pubsub")
	want(t, page, "topics/mine")
	wantNot(t, page, "topics/theirs")
	c.post("/_pogi/scope", url.Values{"project": {"elsewhere"}, "back": {"/_pogi/"}})
	page = c.get("/_pogi/resources/pubsub")
	want(t, page, "topics/theirs")
	wantNot(t, page, "topics/mine")
	// forms prefill the project
	want(t, c.get("/_pogi/new/pubsub/Publisher.CreateTopic"), `value="projects/elsewhere/topics/"`)

	// a value that is not an identifier is ignored, not stored
	c.post("/_pogi/scope", url.Values{"region": {`"><script>`}, "back": {"//evil.example"}})
	want(t, c.get("/_pogi/resources/sqs"), `name="project" value="elsewhere"`)
	wantNot(t, c.get("/_pogi/resources/sqs"), "<script>")
}

// TestConsoleHeaders: console pages refuse sniffing and foreign framing; a stored file opened inline is sandboxed.
func TestConsoleHeaders(t *testing.T) {
	c := start(t, "s3")
	for _, req := range []struct{ method, path, body, ct string }{
		{"PUT", "/bkt", "", ""},
		{"PUT", "/bkt/page.html", "<script>alert(1)</script>", "text/html"},
	} {
		r, _ := http.NewRequest(req.method, c.base+req.path, strings.NewReader(req.body))
		r.Header.Set("Content-Type", req.ct)
		res, err := c.c.Do(r)
		if err != nil || res.StatusCode != 200 {
			t.Fatalf("%s %s = %v, %v", req.method, req.path, res, err)
		}
		res.Body.Close()
	}
	head := func(path string) http.Header {
		res, err := c.c.Get(c.base + path)
		if err != nil || res.StatusCode != 200 {
			t.Fatalf("GET %s = %v, %v", path, res, err)
		}
		res.Body.Close()
		return res.Header
	}
	h := head("/_pogi/")
	if h.Get("X-Content-Type-Options") != "nosniff" || h.Get("X-Frame-Options") != "SAMEORIGIN" ||
		h.Get("Content-Security-Policy") == "" || h.Get("Referrer-Policy") != "same-origin" ||
		h.Get("Cross-Origin-Opener-Policy") != "same-origin" || h.Get("Cross-Origin-Resource-Policy") != "same-origin" ||
		h.Get("Permissions-Policy") == "" {
		t.Errorf("console page headers = %v", h)
	}
	if csp := h.Get("Content-Security-Policy"); strings.Contains(csp, "*") || strings.Contains(csp, "unsafe-eval") || !strings.Contains(csp, "script-src 'self';") {
		t.Errorf("CSP is too loose: %s", csp)
	}
	if h := head("/_pogi/objects/s3/bkt?download=page.html&inline=1"); h.Get("Content-Security-Policy") != "sandbox" || h.Get("X-Content-Type-Options") != "nosniff" || h.Get("Cross-Origin-Embedder-Policy") != "" {
		t.Errorf("inline file headers = %v", h)
	}
}

// The CSP forbids inline scripts, inline event handlers and eval, so no template may use them.
func TestTemplatesNeedNoInlineScript(t *testing.T) {
	bad := regexp.MustCompile(`(?i)<script(\s[^>]*)?>|\son[a-z]+\s*=\s*["']|javascript:|hx-trigger="[^"]*\[`)
	names, _ := filepath.Glob("templates/*.html")
	for _, name := range names {
		b, _ := os.ReadFile(name)
		for _, m := range bad.FindAllString(string(b), -1) {
			if !strings.Contains(m, " src=") {
				t.Errorf("%s: %q needs inline script or eval, which the console CSP blocks", name, m)
			}
		}
	}
}
