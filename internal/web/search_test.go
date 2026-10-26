package web_test

import (
	"net/url"
	"strings"
	"testing"
)

// ⌘K: services in use rank first, plain words find services, resources come before create actions,
// and a create action says which cloud it creates in.
func TestSearchRanking(t *testing.T) {
	c := start(t, "s3", "sqs")
	c.aws("sqs", "", url.Values{"Action": {"CreateQueue"}, "QueueName": {"queue1"}, "Version": {"2012-11-05"}})
	before := func(page, a, b string) {
		t.Helper()
		if i, j := strings.Index(page, a), strings.Index(page, b); i < 0 || j < 0 || i > j {
			t.Fatalf("%q should come before %q (%d, %d):\n%s", a, b, i, j, page)
		}
	}
	// S3 sorts before SQS by name; only SQS is in use
	before(c.get("/_pogi/search?q=s"), `/_pogi/resources/sqs"`, `/_pogi/resources/s3"`)
	want(t, c.get("/_pogi/search?q=bucket"), `/_pogi/resources/s3"`)
	q := c.get("/_pogi/search?q=queue")
	want(t, q, `/_pogi/resources/sqs"`, "Create queue", "SQS · AWS", "queue1")
	before(q, `>Resources<`, `>Actions<`)
}

func TestHomeErrorsInOtherClouds(t *testing.T) {
	c := start(t, "s3", "pubsub")
	c.traffic()
	c.loud() // one failing S3 call that counts
	c.cookie("pogi-cloud", "gcp")
	home := c.get("/_pogi/")
	want(t, home, "No errors in Google Cloud", "1 in other clouds", `name="cloud" value=""`)
	c.cookie("pogi-cloud", "aws")
	wantNot(t, c.get("/_pogi/"), "in other clouds")
}

func TestBucketNameHelp(t *testing.T) {
	c := start(t, "s3")
	want(t, c.get("/_pogi/new/s3/CreateBucket"), `pattern="[a-z0-9][a-z0-9.\-]{1,61}[a-z0-9]"`, "3–63 characters")
	body, _ := c.post("/_pogi/new/s3/CreateBucket", url.Values{"Bucket": {"Bad_Name"}})
	want(t, body, "Bucket name is invalid", "Bad_Name", "lowercase letters")
}
