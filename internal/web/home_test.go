package web_test

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

// getH is get with request headers (htmx's).
func (c *console) getH(path string, h map[string]string) string {
	c.t.Helper()
	req, _ := http.NewRequest("GET", c.base+path, nil)
	for k, v := range h {
		req.Header.Set(k, v)
	}
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

// traffic makes one good call and one failing call against s3.
func (c *console) traffic() {
	c.t.Helper()
	for _, p := range []struct{ m, path string }{{"PUT", "/demo"}, {"GET", "/demo/missing.txt"}} {
		req, _ := http.NewRequest(p.m, c.base+p.path, nil)
		req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20260101/us-east-1/s3/aws4_request")
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			c.t.Fatal(err)
		}
		res.Body.Close()
	}
}

func TestFirstRunHome(t *testing.T) {
	c := start(t, "s3", "sqs")
	want(t, c.get("/_pogi/"), "Welcome to pogi", "Waiting for the first request", "Create an SQS queue", "Popular services", `data-pane="tf"`)
	if res, err := c.c.Get(c.base + "/_pogi/?wait=1"); err != nil || res.StatusCode != 204 {
		t.Fatalf("first-run poll with no traffic: %v %v", res, err)
	}
}

func TestHome(t *testing.T) {
	c := start(t, "s3", "sqs")
	c.traffic()
	body := c.get("/_pogi/")
	want(t, body, "Quick actions", "Live activity", "Recent errors", "/_pogi/requests?errors=1", "/_pogi/resources/s3", "NoSuchKey", `<svg class="spark"`, " / min now")
	wantNot(t, body, "Welcome to pogi")
	if res, _ := c.c.Get(c.base + "/_pogi/?wait=1"); res.Header.Get("HX-Refresh") != "true" {
		t.Fatal("the first-run poll should ask for a reload once traffic exists")
	}
}

func TestActivityFilters(t *testing.T) {
	c := start(t, "s3", "sqs")
	c.traffic()
	all := c.get("/_pogi/requests")
	want(t, all, "Activity", "IAM denied", "GetObject", "CreateBucket")
	errs := c.get("/_pogi/requests?show=errors")
	want(t, errs, "GetObject")
	wantNot(t, errs, "CreateBucket")
	wantNot(t, c.get("/_pogi/requests?errors=1"), "CreateBucket")
	c.cookie("pogi-cloud", "gcp")
	wantNot(t, c.get("/_pogi/requests"), "GetObject")
	c.cookie("pogi-cloud", "aws")
	want(t, c.get("/_pogi/requests"), "GetObject")
	wantNot(t, c.get("/_pogi/requests?show=chaos"), "GetObject")
	// the polling fragment is rows only
	frag := c.getH("/_pogi/requests", map[string]string{"HX-Request": "true", "HX-Target": "reqs"})
	want(t, frag, "GetObject")
	wantNot(t, frag, "<html")
}

func TestRequestDrawer(t *testing.T) {
	c := start(t, "s3")
	c.traffic()
	d := c.getH("/_pogi/requests/2", map[string]string{"HX-Request": "true", "HX-Target": "drawer"})
	want(t, d, "<header>", `class="dbody"`, "Request #2", "GetObject", "Copy as curl", "curl -X GET", `data-pane="response"`, "NoSuchKey")
	wantNot(t, d, "<html")
	want(t, c.get("/_pogi/requests/2"), "<html", "Copy as curl") // the same URL is a full page
}

func TestNotes(t *testing.T) {
	c := start(t, "s3")
	c.traffic()
	// no cookie yet: the old error is listed, not unread; the first render sets the baseline
	want(t, c.get("/_pogi/notes"), "GetObject failed")
	wantNot(t, c.get("/_pogi/notes"), "note-it unread")
	c.cookie("pogi-notes-seen", "1")
	want(t, c.get("/_pogi/notes"), "note-it unread", "GetObject failed", "/_pogi/requests/2", "SDK errors")
	var poll struct {
		Unread int
		New    []struct{ Msg, Kind, Href string }
	}
	if err := json.Unmarshal([]byte(c.get("/_pogi/notes/poll?since=0")), &poll); err != nil || poll.Unread != 1 || len(poll.New) != 1 || poll.New[0].Kind != "bad" || !strings.Contains(poll.New[0].Msg, "GetObject") {
		t.Fatalf("poll = %+v, %v", poll, err)
	}
	// mark all read: the cookie hides the unread state
	if _, final := c.post("/_pogi/notes", map[string][]string{"read": {"1"}}); final != "/_pogi/notes" {
		t.Fatal(final)
	}
	wantNot(t, c.get("/_pogi/notes"), "note-it unread")
	// muting a kind drops it
	c.post("/_pogi/notes", map[string][]string{"toggle": {"sdk"}})
	wantNot(t, c.get("/_pogi/notes"), "GetObject failed")
}
