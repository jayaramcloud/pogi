package test

// Snapshots from a newer format, empty ones and bad ones are refused, and a failed load changes nothing.

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/awstest"
)

func snapCall(t *testing.T, method, url, body string) (int, string) {
	t.Helper()
	req, _ := http.NewRequest(method, url, strings.NewReader(body))
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	return res.StatusCode, string(b)
}

// snapLoad imports body as snapshot "bad" and loads it.
func snapLoad(t *testing.T, base, body string) (importCode int, loadCode int, loadBody string) {
	t.Helper()
	importCode, _ = snapCall(t, "PUT", base+"/_pogi/api/snapshots/bad", body)
	if importCode != 200 {
		return importCode, 0, ""
	}
	loadCode, loadBody = snapCall(t, "POST", base+"/_pogi/api/snapshots/bad/load", "")
	return
}

func TestSnapshotRefusals(t *testing.T) {
	ctx := context.Background()
	srv := pogi.Start(t)
	cfg := awstest.Config(t, srv.URL)
	s3c := s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true })
	sqc := sqs.NewFromConfig(cfg)
	mk := func() {
		s3c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("mine")})
		sqc.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("mine")})
	}
	have := func() (bucket, queue bool) {
		_, err := s3c.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String("mine")})
		q, _ := sqc.ListQueues(ctx, &sqs.ListQueuesInput{})
		return err == nil, len(q.QueueUrls) > 0
	}
	good, err := srv.Hub.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	var f map[string]any
	json.Unmarshal(good, &f)

	t.Run("newer format version", func(t *testing.T) {
		mk()
		f["version"] = 99
		f["futureField"] = true
		f["services"].(map[string]any)["futuresvc"] = map[string]any{"x": 1}
		b, _ := json.Marshal(f)
		ic, lc, body := snapLoad(t, srv.URL, string(b))
		t.Logf("import %d, load %d %s", ic, lc, body)
		if ic == 200 && lc == 200 {
			t.Fatal("a snapshot saying version 99 loaded as if it were version 1: no version check, unknown services dropped silently")
		}
	})
	t.Run("no version, no services", func(t *testing.T) {
		mk()
		ic, lc, body := snapLoad(t, srv.URL, `{}`)
		t.Logf("import %d, load %d %s", ic, lc, body)
		b, q := have()
		if lc == 200 && !b && !q {
			t.Fatal("loading {} succeeded and wiped every service: any JSON object is accepted as a snapshot")
		}
	})
	t.Run("not a snapshot", func(t *testing.T) {
		for _, body := range []string{`[]`, `nope`, `{"services":{"s3":"x"}}`, `{"services":{"sqs":{"Queues":5}}}`} {
			mk()
			ic, lc, lb := snapLoad(t, srv.URL, body)
			t.Logf("%s -> import %d, load %d %s", body, ic, lc, lb)
			if ic == 200 && lc == 200 {
				t.Fatalf("%s accepted", body)
			}
		}
	})
	t.Run("one bad service blob leaves a half-loaded state", func(t *testing.T) {
		// corrupt the blob of whichever of s3 and sqs loads second; the other may load first and wipe its state
		order := []string{}
		for _, s := range srv.Hub.Services() {
			if n := s.Info().Name; n == "s3" || n == "sqs" {
				order = append(order, n)
			}
		}
		svcs := f["services"].(map[string]any)
		delete(svcs, "futuresvc")
		f["version"] = 1
		svcs[order[1]] = "garbage"
		b, _ := json.Marshal(f)
		mk()
		ic, lc, body := snapLoad(t, srv.URL, string(b))
		t.Logf("import %d, load %d %s (load order %v)", ic, lc, body, order)
		bk, q := have()
		if lc == 200 {
			t.Fatal("bad blob accepted")
		}
		if !bk || !q {
			t.Fatalf("load failed with %d, yet state changed: bucket kept=%v queue kept=%v (service %s was already replaced before %s failed)", lc, bk, q, order[0], order[1])
		}
	})
}

func TestSnapshotErrorsArePlain(t *testing.T) {
	srv := pogi.Start(t)
	_, _, body := snapLoad(t, srv.URL, `{"version":99,"services":{"s3":{}}}`)
	if code, b := snapCall(t, "PUT", srv.URL+"/_pogi/api/snapshots/n", `{"version":99,"services":{"s3":{}}}`); code == 200 || !strings.Contains(b, "newer pogi") {
		t.Fatalf("newer format: %d %s", code, b)
	}
	if code, b := snapCall(t, "PUT", srv.URL+"/_pogi/api/snapshots/e", `{}`); code == 200 || !strings.Contains(b, "no services") {
		t.Fatalf("empty: %d %s", code, b)
	}
	snapCall(t, "PUT", srv.URL+"/_pogi/api/snapshots/w", `{"version":1,"services":{"sqs":{"Queues":5}}}`)
	if code, b := snapCall(t, "POST", srv.URL+"/_pogi/api/snapshots/w/load", ""); code == 200 || !strings.Contains(b, "snapshot w is not valid: sqs") {
		t.Fatalf("wrong type: %d %s %s", code, b, body)
	}
}
