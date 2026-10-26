package web_test

import (
	"net/url"
	"strings"
	"testing"
)

func TestScopeSetCookies(t *testing.T) {
	c := start(t, "pubsub")
	c.post("/_pogi/scope", url.Values{"location": {"europe-west1"}, "group": {"demo"}, "azlocation": {"westus"}, "back": {"/_pogi/"}})
	if !strings.Contains(c.get("/_pogi/"), "Google Cloud") {
		t.Fatal("home does not name Google Cloud")
	}
	c.post("/_pogi/scope", url.Values{"location": {`"><x>`}, "back": {"/_pogi/"}}) // bad value keeps the old one
	c.post("/_pogi/cloud", url.Values{"cloud": {"gcp"}, "back": {"/_pogi/"}})
	want(t, c.get("/_pogi/"), "Google Cloud · pogi · europe-west1")
	c.post("/_pogi/scope", url.Values{"reset": {"1"}, "back": {"/_pogi/"}})
	want(t, c.get("/_pogi/"), "Google Cloud · pogi · all locations") // the filter starts at all; creates still go to us-central1
	want(t, c.get("/_pogi/scope/values"), `"location":[{"Value":"*"`, `"Value":"us-central1"`)
}

func TestFormDrawerSaysItsCloudScope(t *testing.T) {
	c := start(t, "container", "sqs")
	want(t, c.get("/_pogi/new/container/ClusterManager.CreateCluster"), "Kubernetes Engine · pogi · us-central1")
	want(t, c.get("/_pogi/new/sqs/CreateQueue"), "000000000000 / us-east-1")
}
