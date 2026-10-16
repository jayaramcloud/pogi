package web_test

import (
	"html"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/nrndev/pogi/internal/gcp/gapi"
)

func (c *console) gcp(method, path, token, body string) (int, string) {
	c.t.Helper()
	req, _ := http.NewRequest(method, c.srv.URL+path, strings.NewReader(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		c.t.Fatal(err)
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	return res.StatusCode, string(b)
}

// The Google Cloud view of the IAM page: recorded permissions with the strict verdict, the bindings
// that follow in gcloud and Terraform, attach through the service's own setIamPolicy, undo, and
// the custom-role alternative.
func TestIAMGoogleCloud(t *testing.T) {
	c := start(t, "gcpiam", "resourcemanager", "pubsub")
	page := c.get("/_pogi/iam?c=gcp")
	want(t, page, "No calls recorded yet", "Enforcement")

	if code, b := c.gcp("POST", "/v1/projects/pogi/serviceAccounts", "", `{"accountId":"orders"}`); code != 200 {
		t.Fatalf("create service account: %d %s", code, b)
	}
	const email, member = "orders@pogi.iam.gserviceaccount.com", "serviceAccount:orders@pogi.iam.gserviceaccount.com"
	tok := gapi.AccessToken(email, "", time.Hour)
	c.gcp("PUT", "/v1/projects/pogi/topics/orders", "", "")
	publish := func() int {
		code, _ := c.gcp("POST", "/v1/projects/pogi/topics/orders:publish", tok, `{"messages":[{"data":"aGk="}]}`)
		return code
	}
	if publish() != 200 {
		t.Fatal("off mode refused a publish")
	}

	page = c.get("/_pogi/iam?c=gcp")
	want(t, page, member, "pubsub.topics.publish", "would deny", "topics/orders", "roles/pubsub.publisher",
		"gcloud pubsub topics add-iam-policy-binding orders --project=pogi", "Attach bindings", `href="/_pogi/iam/gcp/attach?principal=serviceAccount%3aorders`)
	wantNot(t, page, "roles/pubsub.editor")
	want(t, c.get("/_pogi/iam?c=gcp&fmt=tf"), `google_pubsub_topic_iam_member`, `topic = &#34;projects/pogi/topics/orders&#34;`)
	want(t, c.get("/_pogi/iam/gcp/snippet?principal="+url.QueryEscape(member)), "gcloud pubsub topics add-iam-policy-binding")

	// the attach drawer shows the diff
	want(t, c.get("/_pogi/iam/gcp/attach?principal="+url.QueryEscape(member)), "roles/pubsub.publisher on topics/orders", "setIamPolicy")

	body, final := c.post("/_pogi/iam/gcp/attach", url.Values{"principal": {member}})
	if final != "/_pogi/iam" || strings.Contains(body, `class="flash"`) {
		t.Fatalf("attach ended on %s: %s", final, flash(body))
	}
	want(t, body, "Attached: 1 policy change", "Undo", "Switch to strict and watch", "Nothing to attach")
	if _, b := c.gcp("GET", "/v1/projects/pogi/topics/orders:getIamPolicy", "", ""); !strings.Contains(b, "roles/pubsub.publisher") || !strings.Contains(b, email) {
		t.Fatalf("the topic's policy: %s", b)
	}
	c.post("/_pogi/iam/mode", url.Values{"mode": {"strict"}})
	if publish() != 200 {
		t.Fatal("strict mode refused a publish after attach")
	}

	// undo puts the old policy back
	u := html.UnescapeString(regexp.MustCompile(`name="u" value="([^"]+)"`).FindStringSubmatch(body)[1])
	body, _ = c.post("/_pogi/iam/gcp/undo", url.Values{"principal": {member}, "u": {u}})
	want(t, body, "would deny")
	if _, b := c.gcp("GET", "/v1/projects/pogi/topics/orders:getIamPolicy", "", ""); strings.Contains(b, "roles/pubsub.publisher") {
		t.Fatalf("the binding survived the undo: %s", b)
	}
	if publish() != 403 {
		t.Fatal("strict mode allowed a publish after undo")
	}

	// one custom role with exactly the permissions
	want(t, c.get("/_pogi/iam?c=gcp&role=custom"), "projects/pogi/roles/pogi_orders", "gcloud iam roles create pogi_orders")
	body, final = c.post("/_pogi/iam/gcp/attach", url.Values{"principal": {member}, "role": {"custom"}})
	if final != "/_pogi/iam" || strings.Contains(body, `class="flash"`) {
		t.Fatalf("custom attach ended on %s: %s", final, flash(body))
	}
	want(t, body, "and the role pogi_orders")
	if code, b := c.gcp("GET", "/v1/projects/pogi/roles/pogi_orders", "", ""); code != 200 || !strings.Contains(b, "pubsub.topics.publish") {
		t.Fatalf("the custom role: %d %s", code, b)
	}
	if publish() != 200 {
		t.Fatal("strict mode refused a publish with the custom role")
	}
	u = html.UnescapeString(regexp.MustCompile(`name="u" value="([^"]+)"`).FindStringSubmatch(body)[1])
	c.post("/_pogi/iam/gcp/undo", url.Values{"principal": {member}, "u": {u}})
	if publish() != 403 {
		t.Fatal("strict mode allowed a publish after the custom role was undone")
	}

	c.post("/_pogi/iam/gcp/restart", url.Values{})
	want(t, c.get("/_pogi/iam?c=gcp"), "No calls recorded yet", "Strict: calls without an allowing policy fail")
}
