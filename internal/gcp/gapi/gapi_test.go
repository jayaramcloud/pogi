package gapi_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/gcp/gapi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestOperations(t *testing.T) {
	s := pogi.Start(t, pogi.Config{Services: []string{"gcp"}})
	ops := gapi.Operations(s.Hub)
	parent := "projects/p/locations/us-central1"
	done := ops.Done(parent, nil, structpb.NewStringValue("hi"))
	release := make(chan struct{})
	running := ops.Start(parent, nil, func() (proto.Message, error) { <-release; return nil, errors.New("boom") })

	// REST: get, then list.
	get := func(path string) map[string]any {
		t.Helper()
		resp, err := http.Get(s.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var m map[string]any
		json.NewDecoder(resp.Body).Decode(&m)
		if resp.StatusCode != 200 {
			t.Fatalf("GET %s: %d %v", path, resp.StatusCode, m)
		}
		return m
	}
	if m := get("/v2/" + done.Name); m["done"] != true || m["response"] == nil {
		t.Fatalf("done op: %v", m)
	}
	if m := get("/v1/" + parent + "/operations"); len(m["operations"].([]any)) != 2 {
		t.Fatalf("list: %v", m)
	}
	if resp, _ := http.Get(s.URL + "/v2/" + parent + "/operations/nope"); resp.StatusCode != 404 {
		t.Fatalf("missing op: %d", resp.StatusCode)
	}

	// gRPC: the running op is not done until run returns, then carries its error.
	conn, err := grpc.NewClient(strings.TrimPrefix(s.URL, "http://"), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	c := longrunningpb.NewOperationsClient(conn)
	ctx := context.Background()
	if p, err := c.GetOperation(ctx, &longrunningpb.GetOperationRequest{Name: running.Name}); err != nil || p.Done {
		t.Fatalf("running op: %v %v", p, err)
	}
	close(release)
	p, err := c.WaitOperation(ctx, &longrunningpb.WaitOperationRequest{Name: running.Name})
	if err != nil || !p.Done || p.GetError().GetMessage() != "boom" {
		t.Fatalf("waited op: %v %v", p, err)
	}
}

// Enabling one GCP service by name still brings the operations service, so
// its long-running operations can be polled.
func TestOperationsWithOneService(t *testing.T) {
	if pogi.Start(t, pogi.Config{Services: []string{"pubsub"}}).Hub.Service("operations") == nil {
		t.Fatal("operations not enabled alongside pubsub")
	}
	if pogi.Start(t, pogi.Config{Services: []string{"-operations"}}).Hub.Service("operations") != nil {
		t.Fatal("-operations did not exclude it")
	}
}
