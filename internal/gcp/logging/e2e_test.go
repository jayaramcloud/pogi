package logging_test

import (
	"encoding/json"
	"io"
	"net/http"
	"os/exec"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/logging"
	"cloud.google.com/go/logging/logadmin"
	ps "cloud.google.com/go/pubsub/v2"
	pb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	runv2 "cloud.google.com/go/run/apiv2"
	"cloud.google.com/go/run/apiv2/runpb"
	"github.com/nrndev/pogi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/protobuf/types/known/durationpb"
)

// start runs pogi with the services logging delivers to and collects from.
func start(t *testing.T, services ...string) (*pogi.Running, *logging.Client, *logadmin.Client) {
	t.Helper()
	srv := pogi.Start(t, pogi.Config{Services: append([]string{"logging", "operations"}, services...)})
	lc := must(logging.NewClient(ctx, "projects/proj", opts(srv.URL)...))
	ac := must(logadmin.NewClient(ctx, "proj", opts(srv.URL)...))
	t.Cleanup(func() { lc.Close(); ac.Close() })
	return srv, lc, ac
}

func TestSinkToPubSubStorageAndBigQuery(t *testing.T) {
	srv, lc, ac := start(t, "pubsub", "gcs", "bigquery")
	t.Setenv("PUBSUB_EMULATOR_HOST", strings.TrimPrefix(srv.URL, "http://"))
	psc := must(ps.NewClient(ctx, "proj"))
	defer psc.Close()
	topic := must(psc.TopicAdminClient.CreateTopic(ctx, &pb.Topic{Name: "projects/proj/topics/errs"})).Name
	sub := must(psc.SubscriptionAdminClient.CreateSubscription(ctx, &pb.Subscription{Name: "projects/proj/subscriptions/errs-sub", Topic: topic})).Name
	resp := must(http.Post(srv.URL+"/storage/v1/b?project=proj", "application/json", strings.NewReader(`{"name":"archive"}`)))
	resp.Body.Close()
	bq := must(bigquery.NewClient(ctx, "proj", option.WithEndpoint(srv.URL+"/bigquery/v2/"), option.WithoutAuthentication()))
	defer bq.Close()
	if err := bq.Dataset("logs").Create(ctx, nil); err != nil {
		t.Fatal(err)
	}
	for _, s := range []logadmin.Sink{
		{ID: "to-pubsub", Destination: "pubsub.googleapis.com/projects/proj/topics/errs", Filter: "severity>=ERROR"},
		{ID: "to-gcs", Destination: "storage.googleapis.com/archive", Filter: `logName:"audit"`},
		{ID: "to-bq", Destination: "bigquery.googleapis.com/projects/proj/datasets/logs", Filter: `logName:"app"`},
	} {
		must(ac.CreateSink(ctx, &s))
	}

	app, audit := lc.Logger("app"), lc.Logger("audit")
	app.Log(logging.Entry{Severity: logging.Info, Payload: "all good"})
	app.Log(logging.Entry{Severity: logging.Error, Payload: "disk full", Labels: map[string]string{"host": "h1"}})
	audit.Log(logging.Entry{Severity: logging.Notice, Payload: map[string]any{"who": "ada", "did": "login"}})
	if err := lc.Close(); err != nil {
		t.Fatal(err)
	}

	// Pub/Sub got the error, as the LogEntry JSON.
	msgs := must(psc.SubscriptionAdminClient.Pull(ctx, &pb.PullRequest{Subscription: sub, MaxMessages: 10, ReturnImmediately: true})).ReceivedMessages
	if len(msgs) != 1 {
		t.Fatalf("%d messages on the topic, want the one error", len(msgs))
	}
	var en struct {
		LogName, Severity, TextPayload string
		Labels                         map[string]string
	}
	json.Unmarshal(msgs[0].Message.Data, &en)
	if en.LogName != "projects/proj/logs/app" || en.Severity != "ERROR" || en.TextPayload != "disk full" || en.Labels["host"] != "h1" {
		t.Fatalf("message = %s", msgs[0].Message.Data)
	}
	// Cloud Storage got the audit entry as an object under the log's name.
	var list struct{ Items []struct{ Name string } }
	resp = must(http.Get(srv.URL + "/storage/v1/b/archive/o"))
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	json.Unmarshal(b, &list)
	if len(list.Items) != 1 || !strings.HasPrefix(list.Items[0].Name, "audit/") || !strings.HasSuffix(list.Items[0].Name, ".json") {
		t.Fatalf("objects = %s", b)
	}
	resp = must(http.Get(srv.URL + "/storage/v1/b/archive/o/" + strings.ReplaceAll(list.Items[0].Name, "/", "%2F") + "?alt=media"))
	b, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(b), `"who":"ada"`) || !strings.Contains(string(b), `"severity":"NOTICE"`) {
		t.Fatalf("object = %s", b)
	}
	// BigQuery got the app entries (and the client's own diagnostic one) in a table named after the log.
	it := bq.Dataset("logs").Table("app").Read(ctx)
	n, found := 0, false
	for {
		var r map[string]bigquery.Value
		err := it.Next(&r)
		if err == iterator.Done {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		n++
		found = found || r["severity"] == "ERROR" && r["textPayload"] == "disk full"
	}
	if n < 2 || !found {
		t.Fatalf("%d rows in the table, error found: %v", n, found)
	}
}

// A Cloud Run job's output lines are entries of its resource.
func TestCloudRunJobLogs(t *testing.T) {
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("no docker CLI")
	}
	srv, _, ac := start(t, "run")
	if !srv.Hub.Docker().Available() {
		t.Skip("no container engine")
	}
	const parent = "projects/proj/locations/us-central1"
	jobs := must(runv2.NewJobsClient(ctx, opts(srv.URL)...))
	defer jobs.Close()
	job := must(must(jobs.CreateJob(ctx, &runpb.CreateJobRequest{Parent: parent, JobId: "chatty", Job: &runpb.Job{Template: &runpb.ExecutionTemplate{TaskCount: 1,
		Template: &runpb.TaskTemplate{Timeout: durationpb.New(time.Minute), Containers: []*runpb.Container{{Image: "python:3.12-slim", Command: []string{"sh", "-c"},
			Args: []string{`echo "hello from the job"; echo '{"severity":"WARNING","message":"structured"}'`}}}}}}})).Wait(ctx))
	must(must(jobs.RunJob(ctx, &runpb.RunJobRequest{Name: job.Name})).Wait(ctx))
	var got []string
	for end := time.Now().Add(10 * time.Second); time.Now().Before(end) && len(got) < 2; time.Sleep(100 * time.Millisecond) {
		got = nil
		it := ac.Entries(ctx, logadmin.Filter(`resource.type="cloud_run_job" AND resource.labels.job_name="chatty"`))
		for {
			e, err := it.Next()
			if err != nil {
				break
			}
			b, _ := json.Marshal(e.Payload)
			got = append(got, e.Severity.String()+" "+strings.Trim(string(b), `"`))
		}
	}
	if len(got) != 2 || !strings.Contains(strings.Join(got, "|"), "Default hello from the job") || !strings.Contains(strings.Join(got, "|"), `Warning {"message":"structured","severity":"WARNING"}`) {
		t.Fatalf("job logs = %q", got)
	}
}
