package storage_test

import (
	"testing"

	gcs "cloud.google.com/go/storage"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/gcp/storage"
)

func TestLinks(t *testing.T) {
	var svc *storage.Service
	_, url := core.ServeTest(t, func(h *core.Hub) core.Service { svc = storage.New(h); return svc })
	t.Setenv("STORAGE_EMULATOR_HOST", url)
	client := must(gcs.NewClient(ctx))
	defer client.Close()
	bkt := client.Bucket("uploads")
	if err := bkt.Create(ctx, "proj", nil); err != nil {
		t.Fatal(err)
	}
	must(bkt.AddNotification(ctx, &gcs.Notification{TopicProjectID: "proj", TopicID: "events", PayloadFormat: gcs.JSONPayload}))
	want := core.Link{From: "//storage.googleapis.com/projects/_/buckets/uploads", To: "//pubsub.googleapis.com/projects/proj/topics/events", Kind: "configured", Label: "notification"}
	if l := svc.Links(); len(l) != 1 || l[0] != want {
		t.Fatalf("links = %+v, want %+v", l, want)
	}
}
