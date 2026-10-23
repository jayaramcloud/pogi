package web_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"testing"
)

func total(t *testing.T, c *console) string {
	t.Helper()
	m := regexp.MustCompile(`of (\d+)</span>`).FindStringSubmatch(c.get("/_pogi/requests"))
	if m == nil {
		t.Fatal("no total on the Activity page")
	}
	return m[1]
}

// A write made in the console shows in Activity, marked "console". The reads the console makes to draw its own pages do not.
func TestConsoleWritesAreLoggedReadsAreNot(t *testing.T) {
	c := start(t, "sqs")
	before := total(t, c)
	c.get("/_pogi/resources/sqs")
	c.get("/_pogi/resources/sqs?tab=activity")
	if after := total(t, c); after != before {
		t.Fatalf("console reads were logged: %s requests before, %s after", before, after)
	}
	c.ok("/_pogi/new/sqs/CreateQueue", url.Values{"QueueName": {"jobs"}})
	act := c.get("/_pogi/requests")
	want(t, act, "CreateQueue", `>console</span>`)
	c.get("/_pogi/sqs/" + "000000000000/us-east-1/jobs") // a page that reads tags and attributes
	if n := strings.Count(c.get("/_pogi/requests"), ">console</span>"); n != 1 {
		t.Fatalf("want one console row, got %d", n)
	}
	// the service's own Activity tab carries the label too
	want(t, c.get("/_pogi/resources/sqs?tab=activity"), ">console</span>")
}

// A post that ends in a redirect says what it did, in a success flash that is not an error.
func TestRedirectPostShowsSuccessFlash(t *testing.T) {
	c := start(t, "sqs")
	body, _ := c.post("/_pogi/new/sqs/CreateQueue", url.Values{"QueueName": {"jobs"}})
	want(t, body, `class="flash ok" role="status">Queue jobs created`)
	wantNot(t, body, `role="alert"`)
}

func TestDoneText(t *testing.T) {
	c := start(t, "s3")
	body, _ := c.post("/_pogi/new/s3/CreateBucket", url.Values{"Bucket": {"photos"}})
	want(t, body, "Bucket photos created")
}

// The shell has the confirm dialog, the activity indicator and the script that drives them; tables carry what live rows need.
func TestFeedbackMarkup(t *testing.T) {
	c := start(t, "sqs")
	c.ok("/_pogi/new/sqs/CreateQueue", url.Values{"QueueName": {"jobs"}})
	page := c.get("/_pogi/resources/sqs")
	want(t, page, `<dialog id="confirm" aria-labelledby="confirm-title" aria-describedby="confirm-msg"`, `id="confirm-ok"`, `<button class="btn" value="cancel" autofocus>`,
		`id="taskBtn" hidden`, `aria-controls="taskpop"`, `id="taskpop" role="dialog"`, `/_pogi/static/feedback.js`,
		`id="rowsbody" data-live-rows data-svc="sqs"`)
	js := c.get("/_pogi/static/feedback.js")
	want(t, js, "htmx:confirm", "aria-busy", "/_pogi/tasks")
	want(t, c.get("/_pogi/static/pogi.css"), "prefers-reduced-motion: no-preference) { #taskBtn .tdot")
	// removing one chaos rule asks first, like removing them all
	c.post("/_pogi/chaos", url.Values{"service": {"sqs"}, "status": {"503"}, "code": {"X"}})
	want(t, c.get("/_pogi/chaos"), `hx-confirm="Remove the chaos rule for`)
}

func TestFlashDoesNotBreakOtherPages(t *testing.T) {
	c := start(t, "sqs")
	res, err := c.c.Get(c.base + "/_pogi/resources/sqs?ok=" + url.QueryEscape("<b>x</b>"))
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode != http.StatusOK || strings.Contains(string(b), "<b>x</b>") {
		t.Fatalf("a flash must be escaped: %d", res.StatusCode)
	}
}

// The first thing a new user does: create a bucket. It shows in Activity as a console write; looking at it does not.
func TestConsoleBucketShowsInActivity(t *testing.T) {
	c := start(t, "s3")
	c.ok("/_pogi/new/s3/CreateBucket", url.Values{"Bucket": {"photos"}})
	act := c.get("/_pogi/requests")
	want(t, act, "CreateBucket", ">console</span>")
	before := total(t, c)
	c.get("/_pogi/objects/s3/photos")
	c.get("/_pogi/resources/s3")
	if after := total(t, c); after != before {
		t.Fatalf("console reads were logged: %s before, %s after", before, after)
	}
}

// A failed console write is listed in the bell, but the bell's poll does not toast it again: the console already did.
func TestConsoleFailureIsNotToastedTwice(t *testing.T) {
	c := start(t, "sqs")
	c.post("/_pogi/sqs/000000000000/us-east-1/nope/send", url.Values{"body": {"hi"}})
	want(t, c.get("/_pogi/requests?show=errors"), "SendMessage", ">console</span>")
	want(t, c.get("/_pogi/notes"), "SendMessage failed")
	poll := c.get("/_pogi/notes/poll?since=0")
	want(t, poll, `"new":[]`)
}

// GET /_pogi/rev changes when a service gains a resource, so a service page can show resources made elsewhere.
func TestServiceRevChanges(t *testing.T) {
	c := start(t, "s3")
	rev := func() string {
		var j struct{ Rev string }
		if err := json.Unmarshal([]byte(c.get("/_pogi/rev?svc=s3")), &j); err != nil {
			t.Fatal(err)
		}
		return j.Rev
	}
	before := rev()
	want(t, c.get("/_pogi/resources/s3"), `data-rev="`+before+`"`) // the page starts from the same rev
	c.ok("/_pogi/new/s3/CreateBucket", url.Values{"Bucket": {"made-elsewhere"}})
	if after := rev(); after == before || after == "" {
		t.Fatalf("rev did not change: %q then %q", before, after)
	}
	var j struct{ Rev string }
	if err := json.Unmarshal([]byte(c.get("/_pogi/rev?svc=nope")), &j); err != nil || j.Rev != "" {
		t.Fatalf("unknown service: %q", j.Rev)
	}
}

// Deleting a bucket says which one, after the redirect back to the bucket list.
func TestDeleteBucketNamesIt(t *testing.T) {
	c := start(t, "s3")
	c.ok("/_pogi/new/s3/CreateBucket", url.Values{"Bucket": {"gone-soon"}})
	body, final := c.post("/_pogi/objects/s3/gone-soon", url.Values{"action": {"drop"}})
	if final != "/_pogi/resources/s3" {
		t.Fatalf("ended on %s", final)
	}
	want(t, body, "Bucket gone-soon deleted")
}

// CloudTrail's event history is a log, not resources: other services' calls must not put CloudTrail "In use"
// (found 2026-10-21, after "In use" began counting resources as well as requests).
func TestLoggedEventsDoNotMakeCloudTrailInUse(t *testing.T) {
	c := start(t, "s3", "cloudtrail")
	c.ok("/_pogi/new/s3/CreateBucket", url.Values{"Bucket": {"traced"}})
	want(t, c.get("/_pogi/resources/cloudtrail"), "Events") // the call was recorded
	if home := c.get("/_pogi/"); strings.Contains(home, `href="/_pogi/resources/cloudtrail" title=`) {
		t.Fatal("CloudTrail is listed under In use, though nobody used it")
	}
}
