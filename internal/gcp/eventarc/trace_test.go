package eventarc_test

import (
	"testing"

	pb "cloud.google.com/go/eventarc/apiv1/eventarcpb"
	"github.com/nrndev/pogi/internal/core"
)

// An event delivered on a trace reaches the destination on the same trace, from the trigger.
func TestTracePropagates(t *testing.T) {
	e := setup(t)
	run := e.capture("run")
	tr := create(t, e, "traced", &pb.Trigger{EventFilters: filters("type", "custom.type"), Destination: toRun("api", "/")})
	root := "1-5f84c7a1-31d9a2b4c5e6f7a8b9c0d1e2"
	if err := e.hub.Deliver("//eventarc.googleapis.com/projects/proj/events", &core.Message{DetailType: "custom.type", Body: "{}", Trace: root}); err != nil {
		t.Fatal(err)
	}
	m := run.wait(t, 1).msg
	if m.Trace != root || m.From != "//eventarc.googleapis.com/"+tr.Name {
		t.Fatalf("delivery lost the trace: trace %q from %q", m.Trace, m.From)
	}
}

func TestLinks(t *testing.T) {
	e := setup(t)
	tr := create(t, e, "on-upload", &pb.Trigger{EventFilters: filters("type", "google.cloud.storage.object.v1.finalized", "bucket", "uploads"), Destination: toRun("thumbnailer", "/")})
	id := "//eventarc.googleapis.com/" + tr.Name
	want := map[core.Link]bool{
		{From: "//storage.googleapis.com/projects/_/buckets/uploads", To: id, Kind: "configured", Label: "event source"}:                     true,
		{From: id, To: "//run.googleapis.com/projects/proj/locations/us-central1/services/thumbnailer", Kind: "configured", Label: "target"}: true,
	}
	for _, l := range e.svc.Links() {
		delete(want, l)
	}
	if len(want) > 0 {
		t.Fatalf("missing links: %v", want)
	}
}
