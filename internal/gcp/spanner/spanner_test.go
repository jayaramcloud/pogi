package spanner_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/docker"
	"google.golang.org/api/iterator"
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

func wantCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if status.Code(err) != want {
		t.Fatalf("got %v, want %v", err, want)
	}
}

func start(t *testing.T, cfg pogi.Config) *pogi.Running {
	t.Helper()
	cfg.Services = []string{"spanner", "operations"}
	s := pogi.Start(t, cfg)
	t.Setenv("SPANNER_EMULATOR_HOST", strings.TrimPrefix(s.URL, "http://"))
	return s
}

func rest(t *testing.T, s *pogi.Running, method, path, body string) (int, map[string]any) {
	t.Helper()
	req := must(http.NewRequest(method, s.URL+"/spanner/v1/projects/p/"+path, strings.NewReader(body)))
	resp := must(http.DefaultClient.Do(req))
	defer resp.Body.Close()
	var out map[string]any
	json.Unmarshal(must(io.ReadAll(resp.Body)), &out)
	return resp.StatusCode, out
}

// TestNoDocker: without a container engine every call fails at once and says what is missing.
func TestNoDocker(t *testing.T) {
	s := start(t, pogi.Config{DockerHost: "/nonexistent/docker.sock"})
	ia := must(instance.NewInstanceAdminClient(ctx))
	defer ia.Close()
	_, err := ia.CreateInstance(ctx, &instancepb.CreateInstanceRequest{Parent: "projects/p", InstanceId: "i1",
		Instance: &instancepb.Instance{Config: "projects/p/instanceConfigs/regional-us-central1", NodeCount: 1}})
	wantCode(t, err, codes.FailedPrecondition)
	if !strings.Contains(err.Error(), "Docker") {
		t.Fatalf("the error does not say that Docker is needed: %v", err)
	}
	code, out := rest(t, s, "GET", "instances", "")
	if code != 400 || !strings.Contains(out["error"].(map[string]any)["message"].(string), "Docker") {
		t.Fatalf("REST: %d %v", code, out)
	}
	// instance configs are answered by pogi, with or without Docker
	if code, out := rest(t, s, "GET", "instanceConfigs/regional-us-central1", ""); code != 200 || out["name"] != "projects/p/instanceConfigs/regional-us-central1" {
		t.Fatalf("config: %d %v", code, out)
	}
}

// TestEmulator drives the real emulator through pogi with the official clients.
func TestEmulator(t *testing.T) {
	if testing.Short() || !docker.New(docker.Options{}).Available() {
		t.Skip("needs Docker")
	}
	s := start(t, pogi.Config{})
	ia := must(instance.NewInstanceAdminClient(ctx))
	defer ia.Close()
	da := must(database.NewDatabaseAdminClient(ctx))
	defer da.Close()

	create := func(id string) error {
		op, err := ia.CreateInstance(ctx, &instancepb.CreateInstanceRequest{Parent: "projects/p", InstanceId: id,
			Instance: &instancepb.Instance{Config: "projects/p/instanceConfigs/regional-us-central1", DisplayName: id, NodeCount: 1}})
		if err != nil {
			return err
		}
		_, err = op.Wait(ctx)
		return err
	}
	if err := create("test-inst"); err != nil {
		t.Fatal(err)
	}
	wantCode(t, create("test-inst"), codes.AlreadyExists)
	wantCode(t, create("X"), codes.InvalidArgument) // a bad instance id
	_, err := ia.GetInstance(ctx, &instancepb.GetInstanceRequest{Name: "projects/p/instances/nope"})
	wantCode(t, err, codes.NotFound)
	_, err = ia.CreateInstance(ctx, &instancepb.CreateInstanceRequest{Parent: "projects/p", Instance: &instancepb.Instance{NodeCount: 1}}) // no id
	wantCode(t, err, codes.InvalidArgument)

	const inst = "projects/p/instances/test-inst"
	op := must(da.CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{Parent: inst, CreateStatement: "CREATE DATABASE `app`",
		ExtraStatements: []string{"CREATE TABLE users (id INT64 NOT NULL, name STRING(100)) PRIMARY KEY (id)"}}))
	must(op.Wait(ctx))
	_, err = da.CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{Parent: "projects/p/instances/nope", CreateStatement: "CREATE DATABASE `app`"})
	wantCode(t, err, codes.NotFound)

	// an operation is found by its name through pogi's operations service
	conn := must(grpc.NewClient(strings.TrimPrefix(s.URL, "http://"), grpc.WithTransportCredentials(insecure.NewCredentials())))
	defer conn.Close()
	ops := longrunningpb.NewOperationsClient(conn)
	got := must(ops.GetOperation(ctx, &longrunningpb.GetOperationRequest{Name: op.Name()}))
	if got.Name != op.Name() || !got.Done {
		t.Fatalf("operation: %v", got)
	}
	_, err = ops.GetOperation(ctx, &longrunningpb.GetOperationRequest{Name: inst + "/operations/nope"})
	wantCode(t, err, codes.NotFound)

	// data: write with a mutation, read back with SQL
	db := inst + "/databases/app"
	c := must(spanner.NewClient(ctx, db))
	defer c.Close()
	must(c.Apply(ctx, []*spanner.Mutation{
		spanner.Insert("users", []string{"id", "name"}, []any{1, "ada"}),
		spanner.Insert("users", []string{"id", "name"}, []any{2, "grace"}),
	}))
	it := c.Single().Query(ctx, spanner.Statement{SQL: "SELECT id, name FROM users WHERE id >= @min ORDER BY id", Params: map[string]any{"min": 1}})
	defer it.Stop()
	var names []string
	for {
		row, err := it.Next()
		if err == iterator.Done {
			break
		}
		var id int64
		var name string
		if err := must(row, err).Columns(&id, &name); err != nil {
			t.Fatal(err)
		}
		names = append(names, name)
	}
	if strings.Join(names, ",") != "ada,grace" {
		t.Fatalf("rows: %v", names)
	}
	_, err = c.Apply(ctx, []*spanner.Mutation{spanner.Insert("users", []string{"id", "name"}, []any{1, "dup"})})
	if spanner.ErrCode(err) != codes.AlreadyExists {
		t.Fatalf("duplicate key: %v", err)
	}

	// REST, as Terraform calls it
	if code, out := rest(t, s, "GET", "instances/test-inst", ""); code != 200 || out["name"] != inst || out["state"] != "READY" {
		t.Fatalf("get instance: %d %v", code, out)
	}
	code, out := rest(t, s, "GET", "instances/nope", "")
	if e, _ := out["error"].(map[string]any); code != 404 || e["status"] != "NOT_FOUND" {
		t.Fatalf("missing instance: %d %v", code, out)
	}
	if code, _ := rest(t, s, "POST", "instances", `{"instanceId":"test-inst","instance":{"config":"projects/p/instanceConfigs/regional-us-central1","nodeCount":1}}`); code != 409 {
		t.Fatalf("duplicate instance over REST: %d", code)
	}
	if code, out := rest(t, s, "GET", "instanceConfigs/regional-us-central1", ""); code != 200 || out["displayName"] != "regional-us-central1" {
		t.Fatalf("config: %d %v", code, out)
	}
	if code, _ := rest(t, s, "GET", "instanceConfigs/bogus", ""); code != 404 {
		t.Fatalf("unknown config: %d", code)
	}
	if err := da.DropDatabase(ctx, &databasepb.DropDatabaseRequest{Database: db}); err != nil {
		t.Fatal(err)
	}
}
