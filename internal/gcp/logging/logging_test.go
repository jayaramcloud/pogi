package logging_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/logging"
	gapic "cloud.google.com/go/logging/apiv2"
	lpb "cloud.google.com/go/logging/apiv2/loggingpb"
	"cloud.google.com/go/logging/logadmin"
	"github.com/nrndev/pogi/internal/core"
	svc "github.com/nrndev/pogi/internal/gcp/logging"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	mrpb "google.golang.org/genproto/googleapis/api/monitoredres"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

var ctx = context.Background()

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func opts(url string) []option.ClientOption {
	return []option.ClientOption{option.WithEndpoint(strings.TrimPrefix(url, "http://")), option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials()))}
}

type env struct {
	svc   *svc.Service
	hub   *core.Hub
	url   string
	log   *logging.Client
	admin *logadmin.Client
}

func setup(t *testing.T, extra ...func(*core.Hub) core.Service) *env {
	t.Helper()
	e := &env{}
	e.hub, e.url = core.ServeTest(t, append(extra, func(h *core.Hub) core.Service { e.svc = svc.New(h); return e.svc })...)
	e.log = must(logging.NewClient(ctx, "projects/proj", opts(e.url)...))
	e.admin = must(logadmin.NewClient(ctx, "proj", opts(e.url)...))
	t.Cleanup(func() { e.log.Close(); e.admin.Close() })
	return e
}

func wantCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if status.Code(err) != want {
		t.Fatalf("got %v, want %v", err, want)
	}
}

// entries lists what a filter matches, as "<severity> <text>" lines (oldest first unless newest).
func (e *env) entries(t *testing.T, filter string, o ...logadmin.EntriesOption) []string {
	t.Helper()
	it := e.admin.Entries(ctx, append(o, logadmin.Filter(filter))...)
	var out []string
	for {
		en, err := it.Next()
		if err == iterator.Done {
			return out
		}
		if err != nil {
			t.Fatal(err)
		}
		text, _ := en.Payload.(string)
		if text == "" {
			b, _ := json.Marshal(en.Payload)
			if strings.Contains(string(b), "logging.googleapis.com/diagnostic") {
				continue // the client's own note about itself
			}
			text = string(b)
		}
		out = append(out, en.Severity.String()+" "+text)
	}
}

func eq(t *testing.T, got []string, want ...string) {
	t.Helper()
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestWriteAndList(t *testing.T) {
	e := setup(t)
	res := logging.CommonResource(&mrpb.MonitoredResource{Type: "gce_instance", Labels: map[string]string{"instance_id": "i-1", "zone": "z1", "project_id": "proj"}})
	app := e.log.Logger("app", res, logging.CommonLabels(map[string]string{"env": "dev"}))
	base := time.Now().Add(-time.Minute)
	app.Log(logging.Entry{Severity: logging.Debug, Payload: "starting up", Timestamp: base})
	app.Log(logging.Entry{Severity: logging.Info, Payload: "listening on :8080", Timestamp: base.Add(time.Second), Labels: map[string]string{"component": "http"}})
	app.Log(logging.Entry{Severity: logging.Warning, Payload: map[string]any{"message": "slow query", "ms": 1500}, Timestamp: base.Add(2 * time.Second)})
	app.Log(logging.Entry{Severity: logging.Error, Payload: "Disk FULL", Timestamp: base.Add(3 * time.Second), Trace: "projects/proj/traces/abc"})
	other := e.log.Logger("batch", logging.CommonResource(&mrpb.MonitoredResource{Type: "global", Labels: map[string]string{"project_id": "proj"}}))
	other.Log(logging.Entry{Severity: logging.Info, Payload: "job done", Timestamp: base.Add(4 * time.Second)})
	if err := e.log.Close(); err != nil { // flushes both loggers
		t.Fatal(err)
	}

	eq(t, e.entries(t, `logName="projects/proj/logs/app"`), "Debug starting up", "Info listening on :8080", `Warning {"message":"slow query","ms":1500}`, "Error Disk FULL")
	eq(t, e.entries(t, `logName="projects/proj/logs/app" severity>=WARNING`), `Warning {"message":"slow query","ms":1500}`, "Error Disk FULL")
	eq(t, e.entries(t, `severity=INFO`), "Info listening on :8080", "Info job done")
	eq(t, e.entries(t, `resource.type="gce_instance" AND resource.labels.zone="z1" AND severity<ERROR AND severity>DEBUG`), "Info listening on :8080", `Warning {"message":"slow query","ms":1500}`)
	eq(t, e.entries(t, `labels.component="http"`), "Info listening on :8080")
	eq(t, e.entries(t, `labels.env=dev AND NOT labels.component:*`), "Debug starting up", `Warning {"message":"slow query","ms":1500}`, "Error Disk FULL")
	eq(t, e.entries(t, `"disk full"`), "Error Disk FULL")                                                               // global text search ignores case
	eq(t, e.entries(t, `jsonPayload.message:"slow" jsonPayload.ms>1000`), `Warning {"message":"slow query","ms":1500}`) // payload fields
	eq(t, e.entries(t, `textPayload=~"^listening on :\\d+$"`), "Info listening on :8080")
	eq(t, e.entries(t, `severity=DEBUG OR severity=ERROR`), "Debug starting up", "Error Disk FULL") // OR
	eq(t, e.entries(t, `(severity=DEBUG OR severity=ERROR) AND logName:"app"`), "Debug starting up", "Error Disk FULL")
	eq(t, e.entries(t, `log_id("batch")`), "Info job done")
	eq(t, e.entries(t, `trace:"abc"`), "Error Disk FULL")
	eq(t, e.entries(t, `timestamp>="`+base.Add(1500*time.Millisecond).Format(time.RFC3339Nano)+`" timestamp<"`+base.Add(3500*time.Millisecond).Format(time.RFC3339Nano)+`"`),
		`Warning {"message":"slow query","ms":1500}`, "Error Disk FULL")
	eq(t, e.entries(t, `logName="projects/proj/logs/app"`, logadmin.NewestFirst())[:2], "Error Disk FULL", `Warning {"message":"slow query","ms":1500}`)
	eq(t, e.entries(t, `severity>=ERROR AND resource.type="nothing"`))

	// A bad filter is INVALID_ARGUMENT.
	_, err := e.admin.Entries(ctx, logadmin.Filter(`severity=((`)).Next()
	wantCode(t, err, codes.InvalidArgument)

	// Logs: listing and deleting.
	var logs []string
	for it := e.admin.Logs(ctx); ; {
		n, err := it.Next()
		if err != nil {
			break
		}
		logs = append(logs, n)
	}
	eq(t, slices.DeleteFunc(logs, func(n string) bool { return n == "diagnostic-log" }), "app", "batch") // the client reports itself in that one, once per process
	if err := e.admin.DeleteLog(ctx, "batch"); err != nil {
		t.Fatal(err)
	}
	wantCode(t, e.admin.DeleteLog(ctx, "batch"), codes.NotFound)
	eq(t, e.entries(t, `log_id("batch")`))

	// Everything survives Save and Load.
	b := must(e.svc.Save())
	e.svc.Reset()
	eq(t, e.entries(t, `severity>=WARNING`))
	if err := e.svc.Load(b); err != nil {
		t.Fatal(err)
	}
	eq(t, e.entries(t, `severity>=WARNING`), `Warning {"message":"slow query","ms":1500}`, "Error Disk FULL")
	rows := e.svc.Resources()
	if len(rows) != 6 || len(slices.DeleteFunc(rows[1].Rows, func(r []string) bool { return strings.HasSuffix(r[2], "diagnostic-log") })) != 4 {
		t.Fatalf("console = %v", rows)
	}
}

func TestTail(t *testing.T) {
	e := setup(t)
	lc := must(gapic.NewClient(ctx, opts(e.url)...))
	defer lc.Close()
	stream := must(lc.TailLogEntries(ctx))
	if err := stream.Send(&lpb.TailLogEntriesRequest{ResourceNames: []string{"projects/proj"}, Filter: `severity>=ERROR`}); err != nil {
		t.Fatal(err)
	}
	got := make(chan *lpb.LogEntry, 4)
	go func() {
		for {
			r, err := stream.Recv()
			if err != nil {
				return
			}
			for _, en := range r.Entries {
				got <- en
			}
		}
	}()
	time.Sleep(200 * time.Millisecond) // the stream is registered when its first request arrives
	app := e.log.Logger("tail")
	app.Log(logging.Entry{Severity: logging.Info, Payload: "not wanted"})
	app.Log(logging.Entry{Severity: logging.Critical, Payload: "wanted"})
	if err := app.Flush(); err != nil {
		t.Fatal(err)
	}
	select {
	case en := <-got:
		if en.GetTextPayload() != "wanted" || en.LogName != "projects/proj/logs/tail" {
			t.Fatalf("tailed %v", en)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the new entry never arrived")
	}
	select {
	case en := <-got:
		t.Fatalf("an entry the filter excludes: %v", en)
	case <-time.After(300 * time.Millisecond):
	}
}

func TestSinks(t *testing.T) {
	e := setup(t)
	var delivered atomic.Value // the last message to the topic
	var count atomic.Int32
	e.hub.OnDeliver("pubsub", func(topic string, m *core.Message) error {
		delivered.Store(topic + "\n" + m.Body)
		count.Add(1)
		return nil
	})
	sink := must(e.admin.CreateSink(ctx, &logadmin.Sink{ID: "errors", Destination: "pubsub.googleapis.com/projects/proj/topics/errs", Filter: `severity>=ERROR`}))
	if sink.WriterIdentity != "serviceAccount:cloud-logs@system.gserviceaccount.com" {
		t.Fatalf("sink = %+v", sink)
	}
	uniq := must(e.admin.CreateSinkOpt(ctx, &logadmin.Sink{ID: "all-to-bucket", Destination: "storage.googleapis.com/archive"}, logadmin.SinkOptions{UniqueWriterIdentity: true}))
	if !strings.HasPrefix(uniq.WriterIdentity, "serviceAccount:service-") || !strings.HasSuffix(uniq.WriterIdentity, "@gcp-sa-logging.iam.gserviceaccount.com") {
		t.Fatalf("unique writer = %q", uniq.WriterIdentity)
	}
	_, err := e.admin.CreateSink(ctx, &logadmin.Sink{ID: "errors", Destination: "pubsub.googleapis.com/projects/proj/topics/x"})
	wantCode(t, err, codes.AlreadyExists)
	_, err = e.admin.CreateSink(ctx, &logadmin.Sink{ID: "bad", Destination: "ftp://nowhere"})
	wantCode(t, err, codes.InvalidArgument)
	_, err = e.admin.CreateSink(ctx, &logadmin.Sink{ID: "bad2", Destination: "pubsub.googleapis.com/projects/proj/topics/x", Filter: "severity="})
	wantCode(t, err, codes.InvalidArgument)

	app := e.log.Logger("svc")
	app.Log(logging.Entry{Severity: logging.Info, Payload: "fine"})
	app.Log(logging.Entry{Severity: logging.Error, Payload: "broken"})
	if err := app.Flush(); err != nil {
		t.Fatal(err)
	}
	if count.Load() != 1 {
		t.Fatalf("%d deliveries, want 1", count.Load())
	}
	topic, body, _ := strings.Cut(delivered.Load().(string), "\n")
	var en struct{ LogName, Severity, TextPayload string }
	json.Unmarshal([]byte(body), &en)
	if topic != "projects/proj/topics/errs" || en.TextPayload != "broken" || en.Severity != "ERROR" || en.LogName != "projects/proj/logs/svc" {
		t.Fatalf("delivered %s %s", topic, body)
	}

	// Update, a disabled sink, and a sink's own exclusions.
	upd := must(e.admin.UpdateSink(ctx, &logadmin.Sink{ID: "errors", Destination: sink.Destination, Filter: `severity>=INFO`}))
	if upd.Filter != "severity>=INFO" {
		t.Fatalf("updated = %+v", upd)
	}
	app.Log(logging.Entry{Severity: logging.Info, Payload: "now delivered"})
	app.Flush()
	if count.Load() != 2 {
		t.Fatalf("%d deliveries, want 2", count.Load())
	}
	cfg := must(gapic.NewConfigClient(ctx, opts(e.url)...))
	defer cfg.Close()
	must(cfg.UpdateSink(ctx, &lpb.UpdateSinkRequest{SinkName: "projects/proj/sinks/errors", Sink: &lpb.LogSink{Disabled: true}, UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"disabled"}}}))
	app.Log(logging.Entry{Severity: logging.Error, Payload: "ignored"})
	app.Flush()
	if count.Load() != 2 {
		t.Fatalf("a disabled sink delivered: %d", count.Load())
	}
	if err := e.admin.DeleteSink(ctx, "errors"); err != nil {
		t.Fatal(err)
	}
	_, err = e.admin.Sink(ctx, "errors")
	wantCode(t, err, codes.NotFound)
	var ids []string
	for it := e.admin.Sinks(ctx); ; {
		s, err := it.Next()
		if err != nil {
			break
		}
		ids = append(ids, s.ID)
	}
	eq(t, ids, "all-to-bucket")
}

func TestExclusionsMetricsAndBuckets(t *testing.T) {
	e := setup(t)
	cfg := must(gapic.NewConfigClient(ctx, opts(e.url)...))
	defer cfg.Close()
	x := must(cfg.CreateExclusion(ctx, &lpb.CreateExclusionRequest{Parent: "projects/proj", Exclusion: &lpb.LogExclusion{Name: "noise", Filter: `severity<WARNING`}}))
	if x.Name != "noise" {
		t.Fatalf("exclusion = %v", x)
	}
	app := e.log.Logger("x")
	app.Log(logging.Entry{Severity: logging.Info, Payload: "dropped"})
	app.Log(logging.Entry{Severity: logging.Error, Payload: "kept"})
	app.Flush()
	eq(t, e.entries(t, `logName:"x"`), "Error kept")
	must(cfg.UpdateExclusion(ctx, &lpb.UpdateExclusionRequest{Name: "projects/proj/exclusions/noise", Exclusion: &lpb.LogExclusion{Disabled: true}, UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"disabled"}}}))
	app.Log(logging.Entry{Severity: logging.Info, Payload: "now stored"})
	app.Flush()
	eq(t, e.entries(t, `logName:"x"`), "Error kept", "Info now stored")
	if err := cfg.DeleteExclusion(ctx, &lpb.DeleteExclusionRequest{Name: "projects/proj/exclusions/noise"}); err != nil {
		t.Fatal(err)
	}
	_, err := cfg.GetExclusion(ctx, &lpb.GetExclusionRequest{Name: "projects/proj/exclusions/noise"})
	wantCode(t, err, codes.NotFound)

	// Log-based metrics.
	if err := e.admin.CreateMetric(ctx, &logadmin.Metric{ID: "errors", Description: "errors seen", Filter: `severity>=ERROR`}); err != nil {
		t.Fatal(err)
	}
	wantCode(t, e.admin.CreateMetric(ctx, &logadmin.Metric{ID: "errors", Filter: "severity>=ERROR"}), codes.AlreadyExists)
	wantCode(t, e.admin.CreateMetric(ctx, &logadmin.Metric{ID: "nofilter"}), codes.InvalidArgument)
	if m := must(e.admin.Metric(ctx, "errors")); m.Description != "errors seen" || m.Filter != `severity>=ERROR` {
		t.Fatalf("metric = %+v", m)
	}
	if err := e.admin.UpdateMetric(ctx, &logadmin.Metric{ID: "errors", Description: "v2", Filter: `severity>=WARNING`}); err != nil {
		t.Fatal(err)
	}
	if m := must(e.admin.Metric(ctx, "errors")); m.Description != "v2" || m.Filter != `severity>=WARNING` {
		t.Fatalf("updated metric = %+v", m)
	}
	if err := e.admin.DeleteMetric(ctx, "errors"); err != nil {
		t.Fatal(err)
	}
	_, err = e.admin.Metric(ctx, "errors")
	wantCode(t, err, codes.NotFound)

	// Buckets: the two every project has, and a custom one.
	var names []string
	for it := cfg.ListBuckets(ctx, &lpb.ListBucketsRequest{Parent: "projects/proj/locations/global"}); ; {
		b, err := it.Next()
		if err != nil {
			break
		}
		names = append(names, b.Name)
	}
	eq(t, names, "projects/proj/locations/global/buckets/_Default", "projects/proj/locations/global/buckets/_Required")
	bk := must(cfg.CreateBucket(ctx, &lpb.CreateBucketRequest{Parent: "projects/proj/locations/global", BucketId: "audit", Bucket: &lpb.LogBucket{Description: "mine", RetentionDays: 90}}))
	if bk.RetentionDays != 90 || bk.LifecycleState != lpb.LifecycleState_ACTIVE {
		t.Fatalf("bucket = %v", bk)
	}
	_, err = cfg.CreateBucket(ctx, &lpb.CreateBucketRequest{Parent: "projects/proj/locations/global", BucketId: "audit", Bucket: &lpb.LogBucket{}})
	wantCode(t, err, codes.AlreadyExists)
	bk = must(cfg.UpdateBucket(ctx, &lpb.UpdateBucketRequest{Name: bk.Name, Bucket: &lpb.LogBucket{RetentionDays: 7}, UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"retention_days"}}}))
	if bk.RetentionDays != 7 {
		t.Fatalf("updated bucket = %v", bk)
	}
	wantCode(t, cfg.DeleteBucket(ctx, &lpb.DeleteBucketRequest{Name: "projects/proj/locations/global/buckets/_Default"}), codes.InvalidArgument)
	if err := cfg.DeleteBucket(ctx, &lpb.DeleteBucketRequest{Name: bk.Name}); err != nil {
		t.Fatal(err)
	}
	if got := must(cfg.GetBucket(ctx, &lpb.GetBucketRequest{Name: bk.Name})); got.LifecycleState != lpb.LifecycleState_DELETE_REQUESTED {
		t.Fatalf("deleted bucket = %v", got)
	}
}

func rest(t *testing.T, method, url, body string, out any) {
	t.Helper()
	req, _ := http.NewRequest(method, url, strings.NewReader(body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("%s %s = %d %s", method, url, resp.StatusCode, b)
	}
	if out != nil {
		if err := json.Unmarshal(b, out); err != nil {
			t.Fatal(err)
		}
	}
}

func TestREST(t *testing.T) {
	e := setup(t)
	rest(t, "POST", e.url+"/v2/entries:write", `{"logName":"projects/proj/logs/rest","resource":{"type":"global"},"entries":[
		{"severity":"ERROR","textPayload":"over rest"},{"severity":"INFO","jsonPayload":{"k":"v"}}]}`, nil)
	var list struct {
		Entries []struct{ TextPayload, Severity string }
	}
	rest(t, "POST", e.url+"/v2/entries:list", `{"resourceNames":["projects/proj"],"filter":"severity>=ERROR","orderBy":"timestamp desc"}`, &list)
	if len(list.Entries) != 1 || list.Entries[0].TextPayload != "over rest" {
		t.Fatalf("list = %+v", list)
	}
	var sink struct{ Name, WriterIdentity string }
	rest(t, "POST", e.url+"/v2/projects/proj/sinks?uniqueWriterIdentity=true", `{"name":"s","destination":"storage.googleapis.com/b"}`, &sink)
	if sink.Name != "s" || !strings.Contains(sink.WriterIdentity, "gcp-sa-logging") {
		t.Fatalf("sink = %+v", sink)
	}
	rest(t, "PATCH", e.url+"/v2/projects/proj/sinks/s?updateMask=filter", `{"filter":"severity>=ERROR"}`, &sink)
	var sinks struct{ Sinks []struct{ Filter string } }
	rest(t, "GET", e.url+"/v2/projects/proj/sinks", "", &sinks)
	if len(sinks.Sinks) != 1 || sinks.Sinks[0].Filter != "severity>=ERROR" {
		t.Fatalf("sinks = %+v", sinks)
	}
	var logs struct{ LogNames []string }
	rest(t, "GET", e.url+"/v2/projects/proj/logs", "", &logs)
	if len(logs.LogNames) != 1 || logs.LogNames[0] != "projects/proj/logs/rest" {
		t.Fatalf("logs = %+v", logs)
	}
	rest(t, "DELETE", e.url+"/v2/projects/proj/logs/rest", "", nil)
	var buckets struct{ Buckets []struct{ Name string } }
	rest(t, "GET", e.url+"/v2/projects/proj/locations/global/buckets", "", &buckets)
	if len(buckets.Buckets) != 2 {
		t.Fatalf("buckets = %+v", buckets)
	}
}

// Cloud Run, Cloud Functions and Run jobs write their output lines through Hub.Deliver.
func TestDeliveredLogs(t *testing.T) {
	e := setup(t)
	err := e.hub.Deliver("//logging.googleapis.com/projects/proj/logs/run.googleapis.com%2Fstdout", &core.Message{Body: "GET /health 200", Attributes: map[string]core.MessageAttribute{
		"resource.type": {StringValue: "cloud_run_revision"}, "resource.label.service_name": {StringValue: "api"}, "resource.label.revision_name": {StringValue: "api-00001"}}})
	if err != nil {
		t.Fatal(err)
	}
	e.hub.Deliver("//logging.googleapis.com/projects/proj/logs/run.googleapis.com%2Fstdout", &core.Message{Body: `{"severity":"ERROR","message":"boom"}`, Attributes: map[string]core.MessageAttribute{
		"resource.type": {StringValue: "cloud_run_revision"}, "resource.label.service_name": {StringValue: "api"}}})
	eq(t, e.entries(t, `resource.type="cloud_run_revision" resource.labels.service_name="api" logName:"stdout"`), "Default GET /health 200", `Error {"message":"boom","severity":"ERROR"}`)
	eq(t, e.entries(t, `severity=ERROR AND jsonPayload.message="boom"`), `Error {"message":"boom","severity":"ERROR"}`)
	if err := e.hub.Deliver("//logging.googleapis.com/projects/proj/logs", &core.Message{Body: "x"}); err == nil {
		t.Fatal("a target without a log id was accepted")
	}
}
