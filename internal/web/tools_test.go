package web_test

import (
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
)

// drawer GETs a page the way the drawer does (HX-Target: drawer).
func (c *console) drawer(path string) string {
	c.t.Helper()
	req, _ := http.NewRequest("GET", c.base+path, nil)
	req.Header.Set("HX-Request", "true")
	req.Header.Set("HX-Target", "drawer")
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

// The chaos add-rule drawer: presets fill the fields and the CLI band, and the rule it posts shows in the table.
func TestChaosDrawer(t *testing.T) {
	c := start(t, "s3", "sqs")
	d := c.drawer("/_pogi/chaos/new")
	want(t, d, "<header>", "Add rule", `value="ThrottlingException"`, `value="20"`, "pogi chaos add --rate 0.20 --status 400 --code ThrottlingException", "<footer>")
	wantNot(t, d, "<html")
	d = c.drawer("/_pogi/chaos/new?preset=slow")
	want(t, d, `value="800"`, "pogi chaos add --rate 1.00 --latency 800ms")
	want(t, c.drawer("/_pogi/chaos/new?preset=outage"), "--status 503 --code ServiceUnavailable")
	want(t, c.get("/_pogi/chaos/new"), "<html", "Add rule") // without the drawer it is a page

	body, final := c.post("/_pogi/chaos", url.Values{"service": {"sqs"}, "op": {"Send*"}, "pct": {"35"}, "status": {"503"}, "code": {"ServiceUnavailable"}})
	if final != "/_pogi/chaos" || strings.Contains(body, `class="flash"`) {
		t.Fatalf("add ended on %s: %s", final, flash(body))
	}
	want(t, body, "sqs", "Send*", "503 ServiceUnavailable", "35%")
	body, _ = c.post("/_pogi/chaos", url.Values{"service": {"s3"}})
	want(t, body, "a rule needs an error status, latency or both")
}

func TestSnapshotsAndConnectPages(t *testing.T) {
	c := start(t, "s3")
	want(t, c.get("/_pogi/snapshots"), "Save snapshot", "Import…", "State lives in memory", "No snapshots yet")
	body, _ := c.post("/_pogi/snapshots", url.Values{"name": {"before"}})
	want(t, body, "before", "Load", "/_pogi/api/snapshots/before")
	want(t, c.get("/_pogi/connect"), `eval "$(pogi env)"`, "Health", "Docker", "CA certificate")
	want(t, c.get("/_pogi/connect?tab=sdk"), "boto3", "aws-sdk-go-v2")
	want(t, c.get("/_pogi/inbox"), "Nothing sent yet")
}
