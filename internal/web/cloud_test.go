package web_test

import (
	"net/http"
	"net/url"
	"strings"
	"testing"
)

func TestCloudSelectorCookie(t *testing.T) {
	c := start(t, "s3")
	nr := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	post := func(cloud string) *http.Response {
		res, err := nr.PostForm(c.base+"/_pogi/cloud", url.Values{"cloud": {cloud}, "back": {"/_pogi/services"}})
		if err != nil {
			t.Fatal(err)
		}
		return res
	}
	res := post("gcp")
	if res.StatusCode != 303 || res.Header.Get("Location") != "/_pogi/services" {
		t.Fatalf("redirect = %d %s", res.StatusCode, res.Header.Get("Location"))
	}
	if cs := res.Cookies(); len(cs) != 1 || cs[0].Name != "pogi-cloud" || cs[0].Value != "gcp" {
		t.Fatalf("cookies = %v", cs)
	}
	for _, v := range []string{"", "nope"} {
		if cs := post(v).Cookies(); len(cs) != 1 || cs[0].MaxAge >= 0 {
			t.Fatalf("cloud %q should clear the cookie: %v", v, cs)
		}
	}
}

// With a cloud selected every filtered page, the sidebar and ⌘K leave the others out.
func TestCloudSelectorFilters(t *testing.T) {
	c := start(t, "s3", "sqs", "pubsub", "iam")
	c.traffic() // s3: one good call and one failing call
	req, _ := http.NewRequest("PUT", c.base+"/v1/projects/pogi/topics/events", strings.NewReader("{}"))
	if res, err := http.DefaultClient.Do(req); err != nil || res.StatusCode != 200 {
		t.Fatalf("create topic: %v %v", res, err)
	}

	all := c.get("/_pogi/services")
	want(t, all, `href="/_pogi/resources/s3"`, `href="/_pogi/resources/pubsub"`)
	wantNot(t, all, `class="banner cloudnote"`)

	c.cookie("pogi-cloud", "gcp")
	svcs := c.get("/_pogi/services")
	want(t, svcs, `href="/_pogi/resources/pubsub"`, "Showing <b>Google Cloud</b> only", `<button name="cloud" value="">Show all clouds</button>`, `value="gcp" class="on"`, "Google Cloud · pogi")
	wantNot(t, svcs, `id="cloudsel"`)
	wantNot(t, svcs, `href="/_pogi/resources/s3"`)
	// the scope popover still posts every group's inputs
	want(t, svcs, `name="account"`, `name="region"`, `name="project"`, `name="subscription"`)

	wantNot(t, c.get("/_pogi/cost"), "/_pogi/resources/s3")
	wantNot(t, c.get("/_pogi/requests"), "GetObject", "CreateBucket")
	want(t, c.get("/_pogi/requests"), "CreateTopic")
	home := c.get("/_pogi/")
	wantNot(t, home, "NoSuchKey", `href="/_pogi/resources/s3"`, "Create an S3 bucket", "000000000000 / ")
	want(t, home, "Create a Pub/Sub topic")
	wantNot(t, c.get("/_pogi/cost"), `action="/_pogi/cost"`) // GCP is priced for one region
	wantNot(t, c.get("/_pogi/search?q=queue"), "SQS")
	want(t, c.get("/_pogi/iam"), "Enforcement") // the GCP view: filled in by iam_gcp.go
	// a service page of another cloud says so
	want(t, c.get("/_pogi/resources/s3"), "This service is on AWS; the console is showing <b>Google Cloud</b> only")
	// the bell is not filtered
	want(t, c.get("/_pogi/notes"), "GetObject failed")

	c.cookie("pogi-cloud", "aws")
	wantNot(t, c.get("/_pogi/services"), `href="/_pogi/resources/pubsub"`)
	want(t, c.get("/_pogi/search?q=queue"), "SQS")
	wantNot(t, c.get("/_pogi/iam"), `class="seg iamtabs"`) // a picked cloud needs no tabs
	wantNot(t, c.get("/_pogi/resources/s3"), `class="banner cloudnote"`)
}
