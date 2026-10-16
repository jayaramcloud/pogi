package iam_test

import (
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/gcp/gapi"
	giam "github.com/nrndev/pogi/internal/gcp/iam"
	"google.golang.org/api/iam/v1"
)

func call(t *testing.T, s *pogi.Running, method, path, token, body string) (int, string) {
	t.Helper()
	req, _ := http.NewRequest(method, s.URL+path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	return res.StatusCode, string(b)
}

// Strict mode denies a service account without a binding, allows it once roles/pubsub.publisher is
// bound (on the project, or on the topic alone), and never checks the default credentials.
func TestStrictPubSub(t *testing.T) {
	s := pogi.Start(t, pogi.Config{Services: []string{"gcpiam", "resourcemanager", "pubsub"}, IAM: "strict"})
	sa := must(rest(t, s).Projects.ServiceAccounts.Create("projects/pogi", &iam.CreateServiceAccountRequest{AccountId: "orders", ServiceAccount: &iam.ServiceAccount{}}).Do())
	tok := gapi.AccessToken(sa.Email, "", time.Hour)
	const publish = `{"messages":[{"data":"aGk="}]}`
	member := "serviceAccount:" + sa.Email

	// default credentials: no token, pogi's own token, and a token for no one
	for _, def := range []string{"", "pogi-token", gapi.OAuthToken(), gapi.AccessToken("user@example.com", "", time.Hour)} {
		if code, b := call(t, s, "PUT", "/v1/projects/pogi/topics/orders", def, ""); code != 200 && code != 409 {
			t.Fatalf("default credentials %q were refused: %d %s", def, code, b)
		}
	}
	if code, b := call(t, s, "POST", "/v1/projects/pogi/topics/orders:publish", tok, publish); code != 403 || !strings.Contains(b, "pubsub.topics.publish") {
		t.Fatalf("publish without a binding: %d %s", code, b)
	}
	if code, _ := call(t, s, "GET", "/v1/projects/pogi/topics/orders", tok, ""); code != 403 {
		t.Fatalf("get without a binding: %d", code)
	}

	// the denied call is recorded, and the generator asks for publisher and nothing bigger
	ps := giam.Recorded(s.Hub)
	if len(ps) != 1 || ps[0].Member != member || len(ps[0].WouldDeny()) != 2 {
		t.Fatalf("recorded: %+v", ps)
	}
	pl, diff, _ := giam.Preview(s.Hub, member, false)
	roles := map[string]string{}
	for _, b := range pl.Bindings {
		roles[b.Role] = b.Resource
	}
	if len(roles) != 2 || roles["roles/pubsub.publisher"] != "projects/pogi/topics/orders" || roles["roles/pubsub.viewer"] != "projects/pogi/topics/orders" {
		t.Fatalf("plan: %v", roles)
	}
	if len(diff) != 2 {
		t.Fatalf("diff: %v", diff)
	}

	// a binding on the topic allows publish there (and nothing else)
	if code, b := call(t, s, "POST", "/v1/projects/pogi/topics/orders:setIamPolicy", "", `{"policy":{"bindings":[{"role":"roles/pubsub.publisher","members":["`+member+`"]}]}}`); code != 200 {
		t.Fatalf("setIamPolicy on the topic: %d %s", code, b)
	}
	if code, b := call(t, s, "POST", "/v1/projects/pogi/topics/orders:publish", tok, publish); code != 200 {
		t.Fatalf("publish with a topic binding: %d %s", code, b)
	}
	call(t, s, "PUT", "/v1/projects/pogi/topics/audit", "", "")
	if code, _ := call(t, s, "POST", "/v1/projects/pogi/topics/audit:publish", tok, publish); code != 403 {
		t.Fatalf("publish to another topic: %d", code)
	}
	// ... and one on the project allows it everywhere
	if code, b := call(t, s, "POST", "/v1/projects/pogi:setIamPolicy", "", `{"policy":{"bindings":[{"role":"roles/pubsub.publisher","members":["`+member+`"]}]}}`); code != 200 {
		t.Fatalf("setIamPolicy on the project: %d %s", code, b)
	}
	if code, b := call(t, s, "POST", "/v1/projects/pogi/topics/audit:publish", tok, publish); code != 200 {
		t.Fatalf("publish with a project binding: %d %s", code, b)
	}
	// publisher does not grant reading
	if code, _ := call(t, s, "GET", "/v1/projects/pogi/topics/audit", tok, ""); code != 403 {
		t.Fatalf("get with publisher only: %d", code)
	}
	// allAuthenticatedUsers on a topic lets any checked principal read it
	call(t, s, "POST", "/v1/projects/pogi/topics/audit:setIamPolicy", "", `{"policy":{"bindings":[{"role":"roles/pubsub.viewer","members":["allAuthenticatedUsers"]}]}}`)
	if code, b := call(t, s, "GET", "/v1/projects/pogi/topics/audit", tok, ""); code != 200 {
		t.Fatalf("get with allAuthenticatedUsers: %d %s", code, b)
	}
	// a service the table does not know is allowed and recorded as not modelled
	if code, _ := call(t, s, "GET", "/v1/projects/pogi/serviceAccounts", tok, ""); code == 403 {
		t.Fatal("an unmodelled call was denied")
	}
}

// Off and soft record and flag but never deny.
func TestSoftAndOff(t *testing.T) {
	s := pogi.Start(t, pogi.Config{Services: []string{"gcpiam", "resourcemanager", "pubsub"}})
	sa := must(rest(t, s).Projects.ServiceAccounts.Create("projects/pogi", &iam.CreateServiceAccountRequest{AccountId: "orders", ServiceAccount: &iam.ServiceAccount{}}).Do())
	call(t, s, "PUT", "/v1/projects/pogi/topics/orders", "", "")
	tok := gapi.AccessToken(sa.Email, "", time.Hour)
	if code, b := call(t, s, "POST", "/v1/projects/pogi/topics/orders:publish", tok, `{"messages":[{"data":"aGk="}]}`); code != 200 {
		t.Fatalf("off mode denied a call: %d %s", code, b)
	}
	if ps := giam.Recorded(s.Hub); len(ps) != 1 || len(ps[0].WouldDeny()) != 1 {
		t.Fatalf("recorded: %+v", ps)
	}
}
