package queue_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azqueue"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azqueue/queueerror"
	"github.com/nrndev/pogi/internal/azure/queue"
	"github.com/nrndev/pogi/internal/core"
)

var ctx = context.Background()

// Azurite's well-known development account key.
const key = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func wantCode(t *testing.T, err error, code queueerror.Code) {
	t.Helper()
	if !queueerror.HasCode(err, code) {
		t.Fatalf("want %s, got %v", code, err)
	}
}

func TestOfficialSDK(t *testing.T) {
	var impl *queue.Service
	_, url := core.ServePort(t, func(h *core.Hub) core.Service { impl = queue.New(h); return impl })
	conn := "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=" + key + ";QueueEndpoint=" + url + "/devstoreaccount1;"
	svc := must(azqueue.NewServiceClientFromConnectionString(conn, nil))

	// Queues: re-creating with the same metadata is fine, with other metadata a conflict.
	md := map[string]*string{"env": to.Ptr("dev")}
	must(svc.CreateQueue(ctx, "jobs", &azqueue.CreateOptions{Metadata: md}))
	must(svc.CreateQueue(ctx, "jobs", &azqueue.CreateOptions{Metadata: md}))
	_, err := svc.CreateQueue(ctx, "jobs", nil)
	wantCode(t, err, queueerror.QueueAlreadyExists)
	must(svc.CreateQueue(ctx, "logs", nil))
	var names []string
	pager := svc.NewListQueuesPager(&azqueue.ListQueuesOptions{Include: azqueue.ListQueuesInclude{Metadata: true}, MaxResults: to.Ptr(int32(1))})
	for pager.More() {
		for _, q := range must(pager.NextPage(ctx)).Queues {
			names = append(names, *q.Name)
			if *q.Name == "jobs" && (q.Metadata["env"] == nil || *q.Metadata["env"] != "dev") {
				t.Fatalf("listed metadata = %v", q.Metadata)
			}
		}
	}
	if strings.Join(names, ",") != "jobs,logs" {
		t.Fatalf("queues = %v", names)
	}

	q := svc.NewQueueClient("jobs")
	must(q.SetMetadata(ctx, &azqueue.SetMetadataOptions{Metadata: map[string]*string{"team": to.Ptr("core")}}))
	for _, text := range []string{"one", "two", "<three & four>"} {
		must(q.EnqueueMessage(ctx, text, nil))
	}
	props := must(q.GetProperties(ctx, nil))
	if *props.ApproximateMessagesCount != 3 || len(props.Metadata) != 1 {
		t.Fatalf("props = %d %v", *props.ApproximateMessagesCount, props.Metadata)
	}
	peek := must(q.PeekMessages(ctx, &azqueue.PeekMessagesOptions{NumberOfMessages: to.Ptr(int32(5))}))
	if len(peek.Messages) != 3 || *peek.Messages[2].MessageText != "<three & four>" || *peek.Messages[0].DequeueCount != 0 {
		t.Fatalf("peek = %+v", peek.Messages)
	}

	// A dequeued message is invisible until its visibility timeout passes.
	m := must(q.DequeueMessage(ctx, &azqueue.DequeueMessageOptions{VisibilityTimeout: to.Ptr(int32(1))})).Messages[0]
	if *m.MessageText != "one" || *m.DequeueCount != 1 || *m.PopReceipt == "" {
		t.Fatalf("dequeued = %+v", m)
	}
	if p := must(q.PeekMessage(ctx, nil)); *p.Messages[0].MessageText != "two" {
		t.Fatalf("peek while invisible = %s", *p.Messages[0].MessageText)
	}
	logs := svc.NewQueueClient("logs")
	must(logs.EnqueueMessage(ctx, "short-lived", &azqueue.EnqueueMessageOptions{TimeToLive: to.Ptr(int32(1))}))
	time.Sleep(1100 * time.Millisecond)
	if p := must(q.PeekMessage(ctx, nil)); *p.Messages[0].MessageText != "one" {
		t.Fatalf("peek after timeout = %s", *p.Messages[0].MessageText)
	}
	if p := must(logs.PeekMessages(ctx, nil)); len(p.Messages) != 0 {
		t.Fatalf("expired message still there: %+v", p.Messages)
	}

	// Update swaps the pop receipt; the old one no longer deletes.
	upd := must(q.UpdateMessage(ctx, *m.MessageID, *m.PopReceipt, "uno", &azqueue.UpdateMessageOptions{VisibilityTimeout: to.Ptr(int32(0))}))
	if *upd.PopReceipt == *m.PopReceipt || upd.TimeNextVisible == nil {
		t.Fatalf("update = %+v", upd)
	}
	_, err = q.DeleteMessage(ctx, *m.MessageID, *m.PopReceipt, nil)
	wantCode(t, err, queueerror.PopReceiptMismatch)
	if p := must(q.PeekMessage(ctx, nil)); *p.Messages[0].MessageText != "uno" {
		t.Fatalf("updated text = %s", *p.Messages[0].MessageText)
	}
	must(q.DeleteMessage(ctx, *m.MessageID, *upd.PopReceipt, nil))
	_, err = q.DeleteMessage(ctx, *m.MessageID, *upd.PopReceipt, nil)
	wantCode(t, err, queueerror.MessageNotFound)
	got := must(q.DequeueMessages(ctx, &azqueue.DequeueMessagesOptions{NumberOfMessages: to.Ptr(int32(5))}))
	if len(got.Messages) != 2 || *got.Messages[0].MessageText != "two" {
		t.Fatalf("dequeue 5 = %+v", got.Messages)
	}
	must(q.ClearMessages(ctx, nil))
	if n := *must(q.GetProperties(ctx, nil)).ApproximateMessagesCount; n != 0 {
		t.Fatalf("after clear: %d messages", n)
	}

	// Access policies.
	start, expiry := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)
	acl := []*azqueue.SignedIdentifier{{ID: to.Ptr("read"), AccessPolicy: &azqueue.AccessPolicy{Start: &start, Expiry: &expiry, Permission: to.Ptr("r")}}}
	must(q.SetAccessPolicy(ctx, &azqueue.SetAccessPolicyOptions{QueueACL: acl}))
	if p := must(q.GetAccessPolicy(ctx, nil)); len(p.SignedIdentifiers) != 1 || *p.SignedIdentifiers[0].ID != "read" || !p.SignedIdentifiers[0].AccessPolicy.Expiry.Equal(expiry) {
		t.Fatalf("acl = %+v", p.SignedIdentifiers)
	}
	_, err = q.SetAccessPolicy(ctx, &azqueue.SetAccessPolicyOptions{QueueACL: append(acl, acl[0], acl[0], acl[0], acl[0], acl[0])})
	wantCode(t, err, queueerror.InvalidXMLDocument)

	// Service properties and stats.
	must(svc.SetProperties(ctx, &azqueue.SetPropertiesOptions{Logging: &azqueue.Logging{Read: to.Ptr(true), Write: to.Ptr(false), Delete: to.Ptr(false),
		RetentionPolicy: &azqueue.RetentionPolicy{Enabled: to.Ptr(false)}}}))
	if p := must(svc.GetServiceProperties(ctx, nil)); p.Logging == nil || !*p.Logging.Read {
		t.Fatalf("service props = %+v", p.Logging)
	}
	if st := must(svc.GetStatistics(ctx, nil)); st.GeoReplication == nil || st.GeoReplication.LastSyncTime == nil {
		t.Fatalf("stats = %+v", st.GeoReplication)
	}

	// Errors SDKs branch on.
	_, err = svc.NewQueueClient("missing").EnqueueMessage(ctx, "x", nil)
	wantCode(t, err, queueerror.QueueNotFound)
	_, err = q.DequeueMessages(ctx, &azqueue.DequeueMessagesOptions{NumberOfMessages: to.Ptr(int32(33))})
	wantCode(t, err, queueerror.OutOfRangeQueryParameterValue)
	_, err = q.EnqueueMessage(ctx, "x", &azqueue.EnqueueMessageOptions{TimeToLive: to.Ptr(int32(1)), VisibilityTimeout: to.Ptr(int32(2))})
	wantCode(t, err, queueerror.InvalidQueryParameterValue)
	_, err = q.EnqueueMessage(ctx, strings.Repeat("x", 64<<10+1), nil)
	wantCode(t, err, queueerror.MessageTooLarge)
	_, err = svc.CreateQueue(ctx, "Bad_Name", nil)
	wantCode(t, err, queueerror.InvalidResourceName)

	// Console helpers and persistence.
	if err := impl.Send("devstoreaccount1", "jobs", "from console"); err != nil {
		t.Fatal(err)
	}
	state := must(impl.Save())
	impl.Reset()
	if err := impl.Load(state); err != nil {
		t.Fatal(err)
	}
	if p := impl.Peek("devstoreaccount1", "jobs", 10); len(p) != 1 || p[0].Text != "from console" {
		t.Fatalf("console peek = %+v", p)
	}
	if rows := impl.Resources()[0].Rows; len(rows) != 2 || rows[0][0] != "jobs" || rows[0][2] != "1" {
		t.Fatalf("resources = %v", rows)
	}

	must(svc.DeleteQueue(ctx, "logs", nil))
	_, err = logs.GetProperties(ctx, nil)
	wantCode(t, err, queueerror.QueueNotFound)
}
