package serviceusage_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	su "cloud.google.com/go/serviceusage/apiv1"
	pb "cloud.google.com/go/serviceusage/apiv1/serviceusagepb"
	"github.com/nrndev/pogi"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	suapi "google.golang.org/api/serviceusage/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

var ctx = context.Background()

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func start(t *testing.T) *pogi.Running {
	t.Helper()
	return pogi.Start(t, pogi.Config{Services: []string{"serviceusage", "operations"}})
}

func TestEnableAndListGRPC(t *testing.T) {
	s := start(t)
	c := must(su.NewClient(ctx, option.WithEndpoint(strings.TrimPrefix(s.URL, "http://")), option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials()))))
	defer c.Close()
	enabled := func(parent string) (out []string) {
		it := c.ListServices(ctx, &pb.ListServicesRequest{Parent: parent, Filter: "state:ENABLED"})
		for {
			svc, err := it.Next()
			if err == iterator.Done {
				return
			}
			out = append(out, must(svc, err).Config.Name)
		}
	}
	if got := enabled("projects/p1"); len(got) != 0 {
		t.Fatalf("fresh project: %v", got)
	}

	op := must(c.EnableService(ctx, &pb.EnableServiceRequest{Name: "projects/p1/services/pubsub.googleapis.com"}))
	resp := must(op.Wait(ctx))
	if resp.Service.State != pb.State_ENABLED || resp.Service.Name != "projects/p1/services/pubsub.googleapis.com" ||
		resp.Service.Parent != "projects/p1" || resp.Service.Config.Title != "Cloud Pub/Sub API" {
		t.Fatalf("enable response %v", resp)
	}
	if got := enabled("projects/p1"); len(got) != 1 || got[0] != "pubsub.googleapis.com" {
		t.Fatalf("enabled %v", got)
	}
	if got := enabled("projects/p2"); len(got) != 0 {
		t.Fatalf("other project: %v", got)
	}
	if svc := must(c.GetService(ctx, &pb.GetServiceRequest{Name: "projects/p1/services/pubsub.googleapis.com"})); svc.State != pb.State_ENABLED {
		t.Fatalf("get %v", svc)
	}
	if svc := must(c.GetService(ctx, &pb.GetServiceRequest{Name: "projects/p1/services/run.googleapis.com"})); svc.State != pb.State_DISABLED {
		t.Fatalf("get disabled %v", svc)
	}

	batch := must(must(c.BatchEnableServices(ctx, &pb.BatchEnableServicesRequest{Parent: "projects/p1",
		ServiceIds: []string{"run.googleapis.com", "storage.googleapis.com"}})).Wait(ctx))
	if len(batch.Services) != 2 {
		t.Fatalf("batch %v", batch)
	}
	if got := enabled("projects/p1"); len(got) != 3 {
		t.Fatalf("after batch %v", got)
	}
	if got := must(c.BatchGetServices(ctx, &pb.BatchGetServicesRequest{Parent: "projects/p1", Names: []string{
		"projects/p1/services/run.googleapis.com", "projects/p1/services/bigquery.googleapis.com"}})); got.Services[0].State != pb.State_ENABLED || got.Services[1].State != pb.State_DISABLED {
		t.Fatalf("batch get %v", got)
	}

	must(must(c.DisableService(ctx, &pb.DisableServiceRequest{Name: "projects/p1/services/pubsub.googleapis.com"})).Wait(ctx))
	if got := enabled("projects/p1"); len(got) != 2 {
		t.Fatalf("after disable %v", got)
	}
	_, err := c.EnableService(ctx, &pb.EnableServiceRequest{Name: "projects/p1/services/NOT_A_SERVICE"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("bad service: %v", err)
	}
}

// TestProviderCalls makes the REST calls Terraform's google_project_service makes, with the same client.
func TestProviderCalls(t *testing.T) {
	s := start(t)
	svc := must(suapi.NewService(ctx, option.WithEndpoint(s.URL), option.WithoutAuthentication()))

	op := must(svc.Services.BatchEnable("projects/proj-1", &suapi.BatchEnableServicesRequest{ServiceIds: []string{"compute.googleapis.com"}}).Do())
	if !strings.HasPrefix(op.Name, "operations/") {
		t.Fatalf("operation name %q", op.Name)
	}
	if polled := must(svc.Operations.Get(op.Name).Do()); !polled.Done || polled.Error != nil {
		t.Fatalf("polled %+v", polled)
	}
	got := must(svc.Services.Get("projects/proj-1/services/compute.googleapis.com").Do())
	if got.State != "ENABLED" || got.Config.Title != "Compute Engine API" {
		t.Fatalf("get %+v", got)
	}
	must(svc.Services.Enable("projects/proj-1/services/iam.googleapis.com", &suapi.EnableServiceRequest{}).Do())

	list := must(svc.Services.List("projects/proj-1").Filter("state:ENABLED").PageSize(200).Do())
	if len(list.Services) != 2 || list.Services[0].Config.Name != "compute.googleapis.com" || list.Services[1].Config.Name != "iam.googleapis.com" {
		t.Fatalf("list %+v", list.Services)
	}
	if page := must(svc.Services.List("projects/proj-1").Filter("state:ENABLED").PageSize(1).Do()); len(page.Services) != 1 || page.NextPageToken == "" {
		t.Fatalf("page %+v", page)
	}

	must(svc.Services.Disable("projects/proj-1/services/iam.googleapis.com", &suapi.DisableServiceRequest{}).Do())
	if list := must(svc.Services.List("projects/proj-1").Filter("state:ENABLED").Do()); len(list.Services) != 1 {
		t.Fatalf("after disable %+v", list.Services)
	}
	var ge *googleapi.Error
	if _, err := svc.Services.Enable("projects/proj-1/services/nonsense", &suapi.EnableServiceRequest{}).Do(); !errors.As(err, &ge) || ge.Code != 404 {
		t.Fatalf("bad service: %v", err)
	}
	if _, err := svc.Operations.Get("operations/nope").Do(); !errors.As(err, &ge) || ge.Code != 404 {
		t.Fatalf("unknown operation: %v", err)
	}
}
