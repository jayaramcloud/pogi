package web_test

import (
	"archive/zip"
	"bytes"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"strings"
	"testing"
)

// hx is a GET the way htmx makes one: with the target it swaps into.
func (c *console) hx(path, target string) string {
	c.t.Helper()
	req, _ := http.NewRequest("GET", c.base+path, nil)
	req.Header.Set("HX-Request", "true")
	req.Header.Set("HX-Target", target)
	res, err := c.c.Do(req)
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

// landed is the path a GET ends on after redirects.
func (c *console) landed(path string) string {
	res, err := c.c.Get(c.base + path)
	if err != nil {
		c.t.Fatal(err)
	}
	res.Body.Close()
	return res.Request.URL.Path
}

func TestCatalog(t *testing.T) {
	c := start(t, "s3", "sqs", "pubsub", "arm")
	page := c.get("/_pogi/services")
	// grouped by the coverage categories, with a pin star and a link per tile
	want(t, page, `<h2>Storage`, `<h2>Messaging &amp; events`, `data-pin="s3"`, `href="/_pogi/resources/sqs"`, `class="dot gcp"`, `class="tag deep"`)
	if strings.Index(page, "<h2>Storage") > strings.Index(page, "<h2>Messaging") {
		t.Fatal("categories are out of coverage order")
	}
	// the cloud filter
	c.cookie("pogi-cloud", "gcp")
	gcp := c.get("/_pogi/services")
	want(t, gcp, `href="/_pogi/resources/pubsub"`)
	wantNot(t, gcp, `href="/_pogi/resources/s3"`)
	c.cookie("pogi-cloud", "")
	// in use only: nothing has traffic yet
	want(t, c.get("/_pogi/services?use=1"), "No services match")
	// a pinned service is starred
	c.c.Jar.SetCookies(mustURL(c.base), []*http.Cookie{{Name: "pogi-pins", Value: "s3", Path: "/"}})
	want(t, c.get("/_pogi/services"), `class="pin on"`)
}

func mustURL(s string) *url.URL { u, _ := url.Parse(s); return u }

func TestServicePageTabs(t *testing.T) {
	c := start(t, "sqs", "dynamodb", "kms")
	// empty: the empty state, the create button and the CLI hint
	page := c.get("/_pogi/resources/sqs")
	want(t, page, "No queues in", "data-create", `href="/_pogi/new/sqs/CreateQueue"`, "Create queue", "pogi aws sqs help", `data-js="scope"`, "Show all regions", "Nothing here yet")
	c.ok("/_pogi/new/sqs/CreateQueue", url.Values{"QueueName": {"jobs"}})
	page = c.get("/_pogi/resources/sqs")
	// rows open the detail in the drawer; checkboxes and the bulk bar; a filter
	want(t, page, `<h1><span class="dot aws">`, `<em>1</em>`, `data-drawer="/_pogi/sqs/000000000000/us-east-1/jobs"`, `name="row"`, `id="bulk"`, `class="filter"`, "1 resource")
	// activity and about
	want(t, c.get("/_pogi/resources/sqs?tab=activity"), "No calls yet")
	about := c.get("/_pogi/resources/sqs?tab=about")
	want(t, about, "What works", "Not yet", "Reset SQS", `hx-post="/_pogi/reset"`)
	// explorers are tabs that load their content
	ddb := c.get("/_pogi/resources/dynamodb")
	want(t, ddb, `href="/_pogi/resources/dynamodb?tab=query"`, ">Query<")
	want(t, c.get("/_pogi/resources/dynamodb?tab=query"), `id="xtab"`, `hx-get="/_pogi/query/dynamodb"`)
	frag := c.hx("/_pogi/query/dynamodb", "xtab")
	want(t, frag, `id="xtab"`, `hx-target="#xtab"`)
	wantNot(t, frag, "<html")
	want(t, c.get("/_pogi/resources/kms?tab=crypto"), `hx-get="/_pogi/crypto"`, "Encrypt &amp; decrypt")
	want(t, c.hx("/_pogi/crypto", "xtab"), `hx-post="/_pogi/crypto"`)
	// an unknown service goes to the catalog
	if p := c.landed("/_pogi/resources/nope"); p != "/_pogi/services" {
		t.Fatalf("landed on %s", p)
	}
}

func TestExplorerRoutesRedirectAndRender(t *testing.T) {
	c := start(t, "s3")
	for _, p := range []string{"/_pogi/resources", "/_pogi/explore"} {
		if got := c.landed(p); got != "/_pogi/services" {
			t.Fatalf("%s landed on %s", p, got)
		}
	}
	want(t, c.get("/_pogi/query/dynamodb"), `id="xtab"`, "<html") // still a page of its own
}

func TestDrawers(t *testing.T) {
	c := start(t, "sqs", "s3", "codeartifact")
	drawer := func(path string) string { return c.hx(path, "drawer") }

	// generated form: header, optional section, CLI band, footer wired to the form
	form := drawer("/_pogi/new/sqs/CreateQueue")
	want(t, form, `<header>`, `id="dform"`, `Optional (`, `/_pogi/cli/sqs/CreateQueue`, `form="dform"`, `data-close`)
	wantNot(t, form, "<html")

	// SQS queue: counts, send, peek, purge
	c.ok("/_pogi/new/sqs/CreateQueue", url.Values{"QueueName": {"jobs"}})
	q := "/_pogi/sqs/000000000000/us-east-1/jobs"
	body, _ := c.post(q+"/send", url.Values{"body": {`{"job":1}`}})
	want(t, body, "jobs")
	d := drawer(q)
	want(t, d, "visible", "in flight", "delayed", "Purge queue", `{&#34;job&#34;:1}`, `hx-post="`+q+`/send"`)
	wantNot(t, d, "<html")
	c.post(q+"/purge", url.Values{})
	want(t, drawer(q), "Queue is empty")

	// inspect: JSON, Activity, Delete
	ins := drawer("/_pogi/resources/sqs/Queues/jobs")
	want(t, ins, `class="tabs"`, ">JSON<", ">Activity<", "Delete", `&#34;name&#34;: &#34;jobs&#34;`)
	// SDK traffic naming the queue (the JSON protocol keeps names in the body) shows under Activity
	req, _ := http.NewRequest("POST", c.base+"/", strings.NewReader(`{"QueueName":"jobs"}`))
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", "AmazonSQS.CreateQueue")
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20260101/us-east-1/sqs/aws4_request, SignedHeaders=host, Signature=x")
	if res, err := http.DefaultClient.Do(req); err != nil {
		t.Fatal(err)
	} else {
		res.Body.Close()
	}
	want(t, drawer("/_pogi/resources/sqs/Queues/jobs?tab=activity"), "CreateQueue")
	want(t, c.get("/_pogi/resources/sqs?tab=activity"), "CreateQueue")

	// bucket: folders, delete, from the drawer
	c.ok("/_pogi/new/s3/CreateBucket", url.Values{"Bucket": {"assets"}})
	b := "/_pogi/objects/s3/assets"
	c.post(b, url.Values{"action": {"mkdir"}, "folder": {"img"}, "prefix": {""}})
	od := drawer(b)
	want(t, od, "img", `hx-encoding="multipart/form-data"`, "Delete bucket", `name="folder"`)
	wantNot(t, od, "<html")
	want(t, drawer(b+"?prefix=img%2F"), "Empty. Upload a file above.")
	// a missing resource answers with an error, not a page
	if !strings.Contains(c.get("/_pogi/resources/sqs/Queues/ghost"), "not found") {
		t.Fatal("no error for a missing resource")
	}

	// bulk tag drawer carries the ticked rows
	page := c.get("/_pogi/resources/sqs")
	tag := drawer("/_pogi/bulk/sqs/Queues?row=" + rowOf(t, page, "jobs"))
	want(t, tag, "Add tags", `name="tags"`, `name="action" value="tag"`, `name="row"`)
}

func TestSpecialForms(t *testing.T) {
	c := start(t, "s3", "sns", "events", "lambda")
	// the create bars are real form ops now
	want(t, c.get("/_pogi/resources/s3"), `href="/_pogi/new/s3/CreateBucket"`)
	c.ok("/_pogi/new/s3/CreateBucket", url.Values{"Bucket": {"assets"}})
	want(t, c.get("/_pogi/resources/s3"), ">assets<")

	c.ok("/_pogi/new/sns/CreateTopic", url.Values{"Name": {"alerts"}})
	want(t, c.get("/_pogi/new/sns/Publish"), `arn:aws:sns:`, `name="Message"`)
	c.ok("/_pogi/new/sns/Publish", url.Values{"TopicArn": {"arn:aws:sns:us-east-1:000000000000:alerts"}, "Message": {"hello"}})
	c.ok("/_pogi/new/events/PutEvent", url.Values{"EventBusName": {"default"}, "Source": {"my.app"}, "DetailType": {"thing"}, "Detail": {`{"id":1}`}})
	body, _ := c.post("/_pogi/new/events/PutEvent", url.Values{"Source": {"my.app"}, "DetailType": {"thing"}, "Detail": {`{`}})
	want(t, body, `id="apierr"`, "not valid JSON")

	// Lambda takes a zip
	var zb bytes.Buffer
	zw := zip.NewWriter(&zb)
	f, _ := zw.Create("app.py")
	f.Write([]byte("def handler(e, c):\n    return {'ok': True}\n"))
	zw.Close()
	var mb bytes.Buffer
	mw := multipart.NewWriter(&mb)
	for k, v := range map[string]string{"FunctionName": "hello", "Runtime": "python3.12", "Handler": "app.handler", "Role": "arn:aws:iam::000000000000:role/lambda"} {
		mw.WriteField(k, v)
	}
	fw, _ := mw.CreateFormFile("ZipFile", "app.zip")
	fw.Write(zb.Bytes())
	mw.Close()
	res, err := c.c.Post(c.base+"/_pogi/new/lambda/CreateFunction", mw.FormDataContentType(), &mb)
	if err != nil {
		t.Fatal(err)
	}
	out, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if strings.Contains(string(out), `id="apierr"`) || !strings.HasPrefix(res.Request.URL.Path, "/_pogi/resources/lambda") {
		t.Fatalf("create function: %s %s", res.Request.URL.Path, flash(string(out)))
	}
	want(t, c.get("/_pogi/resources/lambda"), ">hello<", `data-drawer="/_pogi/lambda/`)
	want(t, c.hx("/_pogi/lambda/000000000000/us-east-1/hello", "drawer"), "Invoke", `id="result"`)
}

func TestCLIMirror(t *testing.T) {
	c := start(t, "sqs", "s3", "pubsub", "events", "lambda")
	cli := func(svc, op string, v url.Values) string {
		body, _ := c.post("/_pogi/cli/"+svc+"/"+op, v)
		return body
	}
	// simple values inline, structures as JSON in single quotes
	got := cli("sqs", "CreateQueue", url.Values{"QueueName": {"orders"}, "Attributes.0.k": {"VisibilityTimeout"}, "Attributes.0.v": {"45"}})
	want(t, got, "pogi aws sqs create-queue --queue-name orders", `--attributes &#39;{&#34;VisibilityTimeout&#34;:&#34;45&#34;}&#39;`)
	// nothing filled in yet: just the command
	want(t, cli("sqs", "CreateQueue", url.Values{}), "pogi aws sqs create-queue")
	want(t, cli("s3", "CreateBucket", url.Values{"Bucket": {"assets"}}), "pogi aws s3api create-bucket --bucket assets")
	// Google Cloud: the REST call the form submits
	g := cli("pubsub", "Publisher.CreateTopic", url.Values{"name": {"projects/pogi/topics/events"}})
	want(t, g, "curl -X PUT", "/v1/projects/pogi/topics/events", "&#39;")
	want(t, cli("lambda", "CreateFunction", url.Values{"FunctionName": {"hello"}, "Runtime": {"python3.12"}}), "pogi aws lambda create-function --function-name hello", "--zip-file fileb://function.zip")
	want(t, cli("arm", "new", url.Values{"path": {"/subscriptions/x/resourceGroups/web"}, "body": {`{ "location": "eastus" }`}}), "curl -X PUT", "resourceGroups/web?api-version=", `{&#34;location&#34;:&#34;eastus&#34;}`)
	// a form that does not exist
	want(t, cli("sqs", "Nope", url.Values{}), "no such form")
}

// An upload over the limit is refused with the caution message; one under it goes through.
func TestConsoleUploadLimit(t *testing.T) {
	c := start(t, "s3")
	c.ok("/_pogi/new/s3/CreateBucket", url.Values{"Bucket": {"assets"}})
	upload := func(size int) string {
		var mb bytes.Buffer
		mw := multipart.NewWriter(&mb)
		mw.WriteField("action", "upload")
		mw.WriteField("prefix", "")
		fw, _ := mw.CreateFormFile("file", "big.bin")
		fw.Write(make([]byte, size))
		mw.Close()
		res, err := c.c.Post(c.base+"/_pogi/objects/s3/assets", mw.FormDataContentType(), &mb)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		b, _ := io.ReadAll(res.Body)
		return string(b)
	}
	want(t, upload(11<<20), "limited to 10 MB (this one is 1")
	if body := upload(1 << 20); strings.Contains(body, "limited to") {
		t.Fatalf("a 1 MB upload was refused: %s", flash(body))
	}
	want(t, c.get("/_pogi/objects/s3/assets"), "big.bin", `<meta name="pogi-upload-mb" content="10">`, "data-upload")
}

// The console's Lambda form takes a zip up to the code limit (50 MB), not the plain 10 MB.
func TestConsoleLambdaCodeLimit(t *testing.T) {
	c := start(t, "lambda")
	create := func(size int) string {
		var mb bytes.Buffer
		mw := multipart.NewWriter(&mb)
		for k, v := range map[string]string{"FunctionName": "big", "Runtime": "python3.12", "Handler": "app.handler", "Role": "arn:aws:iam::000000000000:role/lambda"} {
			mw.WriteField(k, v)
		}
		fw, _ := mw.CreateFormFile("ZipFile", "app.zip")
		fw.Write(make([]byte, size))
		mw.Close()
		res, err := c.c.Post(c.base+"/_pogi/new/lambda/CreateFunction", mw.FormDataContentType(), &mb)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		b, _ := io.ReadAll(res.Body)
		return string(b)
	}
	if body := create(12 << 20); strings.Contains(body, "limited to") { // not a zip, but past the upload stage
		t.Fatalf("a 12 MB zip hit a limit: %s", body)
	}
	want(t, create(60<<20), "Lambda code is limited to 50 MB zipped (this one is 6")
	want(t, c.get("/_pogi/new/lambda/CreateFunction"), `name="ZipFile"`, "data-code")
}
