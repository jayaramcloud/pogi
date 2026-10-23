package memorystore_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	redis "cloud.google.com/go/redis/apiv1"
	"cloud.google.com/go/redis/apiv1/redispb"
	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/docker"
	"github.com/nrndev/pogi/internal/docker/dockertest"
	"github.com/nrndev/pogi/internal/gcp/memorystore"
	"google.golang.org/api/option"
	redisapi "google.golang.org/api/redis/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

var ctx = context.Background()

const loc = "projects/p/locations/us-central1"

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

func start(t *testing.T, cfg pogi.Config) (*pogi.Running, *redis.CloudRedisClient) {
	t.Helper()
	cfg.Services = []string{"redis", "operations"}
	s := pogi.Start(t, cfg)
	c := must(redis.NewCloudRedisClient(ctx, option.WithEndpoint(strings.TrimPrefix(s.URL, "http://")), option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials()))))
	t.Cleanup(func() { c.Close() })
	return s, c
}

// fakeEngine runs pogi against a fake container engine whose containers all
// publish a listener that answers like Redis.
func fakeEngine(t *testing.T) (*dockertest.Engine, *pogi.Running, *redis.CloudRedisClient) {
	ln := must(net.Listen("tcp", "127.0.0.1:0"))
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				c.Write([]byte("+PONG\r\n"))
				io.Copy(io.Discard, c)
			}()
		}
	}()
	e := dockertest.Start(t, ln.Addr().(*net.TCPAddr).Port, false)
	s, c := start(t, pogi.Config{})
	return e, s, c
}

func create(id string, in *redispb.Instance) *redispb.CreateInstanceRequest {
	return &redispb.CreateInstanceRequest{Parent: loc, InstanceId: id, Instance: in}
}

func TestInstanceLifecycle(t *testing.T) {
	e, _, c := fakeEngine(t)
	name := loc + "/instances/cache"
	got := must(must(c.CreateInstance(ctx, create("cache", &redispb.Instance{
		Tier: redispb.Instance_BASIC, MemorySizeGb: 2, AuthEnabled: true, DisplayName: "main",
		Labels:       map[string]string{"env": "test"},
		RedisConfigs: map[string]string{"maxmemory-policy": "allkeys-lru"},
	}))).Wait(ctx))
	if got.Name != name || got.State != redispb.Instance_READY || got.Host != "127.0.0.1" || got.Port == 0 || got.RedisVersion != "REDIS_7_0" ||
		got.Tier != redispb.Instance_BASIC || got.MemorySizeGb != 2 || !got.AuthEnabled || got.CurrentLocationId != "us-central1-a" ||
		got.Labels["env"] != "test" || got.CreateTime == nil {
		t.Fatalf("created instance: %v", got)
	}
	if len(got.Labels) != 1 {
		t.Fatalf("labels carry pogi's own entries: %v", got.Labels)
	}
	cmds := e.Called("create --name pogi-memorystore-")
	if len(cmds) != 1 || !strings.Contains(cmds[0], "--maxmemory 2gb --maxmemory-policy allkeys-lru --requirepass") {
		t.Fatalf("container: %v", cmds)
	}

	auth := must(c.GetInstanceAuthString(ctx, &redispb.GetInstanceAuthStringRequest{Name: name})).AuthString
	if auth == "" || !strings.Contains(cmds[0], auth) {
		t.Fatalf("auth string %q does not match the container: %v", auth, cmds)
	}
	again := must(c.GetInstance(ctx, &redispb.GetInstanceRequest{Name: name}))
	if again.Port != got.Port || again.State != redispb.Instance_READY {
		t.Fatalf("get: %v", again)
	}

	op := must(c.UpdateInstance(ctx, &redispb.UpdateInstanceRequest{
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"display_name", "labels", "memory_size_gb", "redis_configs"}},
		Instance: &redispb.Instance{Name: name, DisplayName: "renamed", Labels: map[string]string{"a": "b"}, MemorySizeGb: 3,
			RedisConfigs: map[string]string{"maxmemory-policy": "noeviction"}}}))
	up := must(op.Wait(ctx))
	if up.DisplayName != "renamed" || up.MemorySizeGb != 3 || up.Labels["a"] != "b" || up.Labels["env"] != "" || up.RedisConfigs["maxmemory-policy"] != "noeviction" || !up.AuthEnabled {
		t.Fatalf("updated instance: %v", up)
	}
	waitFor(t, func() bool {
		return len(e.Called("exec pogi-memorystore-")) == 2
	})
	for _, want := range []string{"config set maxmemory 3gb", "config set maxmemory-policy noeviction"} {
		if x := e.Called("exec pogi-memorystore-"); !strings.Contains(strings.Join(x, "\n"), want) {
			t.Fatalf("missing %q in %v", want, x)
		}
	}

	list := c.ListInstances(ctx, &redispb.ListInstancesRequest{Parent: "projects/p/locations/-"})
	n := 0
	for i, err := list.Next(); err == nil; i, err = list.Next() {
		n++
		if i.Name != name {
			t.Fatalf("listed %s", i.Name)
		}
	}
	if n != 1 {
		t.Fatalf("listed %d instances", n)
	}
	other := c.ListInstances(ctx, &redispb.ListInstancesRequest{Parent: "projects/p/locations/europe-west1"})
	if _, err := other.Next(); err == nil {
		t.Fatal("an instance of another location was listed")
	}

	if err := must(c.DeleteInstance(ctx, &redispb.DeleteInstanceRequest{Name: name})).Wait(ctx); err != nil {
		t.Fatal(err)
	}
	_, err := c.GetInstance(ctx, &redispb.GetInstanceRequest{Name: name})
	wantCode(t, err, codes.NotFound)
	waitFor(t, func() bool { return len(e.Called("rm -f -v pogi-memorystore-")) >= 1 })
}

func waitFor(t *testing.T, ok func() bool) {
	t.Helper()
	for i := 0; i < 200; i++ {
		if ok() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("timed out")
}

func TestFailures(t *testing.T) {
	_, _, c := fakeEngine(t)
	ok := func(id string) *redispb.CreateInstanceRequest { return create(id, &redispb.Instance{MemorySizeGb: 1}) }
	cases := []struct {
		what string
		req  *redispb.CreateInstanceRequest
	}{
		{"bad id", ok("Bad_ID")},
		{"id ending in a hyphen", ok("cache-")},
		{"no instance", create("a", nil)},
		{"no size", create("a", &redispb.Instance{})},
		{"size too big", create("a", &redispb.Instance{MemorySizeGb: 301})},
		{"bad version", create("a", &redispb.Instance{MemorySizeGb: 1, RedisVersion: "7.0"})},
		{"unsupported config", create("a", &redispb.Instance{MemorySizeGb: 1, RedisConfigs: map[string]string{"dir": "/etc"}})},
		{"config value as a flag", create("a", &redispb.Instance{MemorySizeGb: 1, RedisConfigs: map[string]string{"maxmemory-policy": "--dir"}})},
		{"location of another region", create("a", &redispb.Instance{MemorySizeGb: 1, LocationId: "europe-west1-b"})},
		{"bad parent", &redispb.CreateInstanceRequest{Parent: "projects/p", InstanceId: "a", Instance: &redispb.Instance{MemorySizeGb: 1}}},
	}
	for _, tc := range cases {
		_, err := c.CreateInstance(ctx, tc.req)
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("%s: got %v, want InvalidArgument", tc.what, err)
		}
	}

	must(must(c.CreateInstance(ctx, ok("dup"))).Wait(ctx))
	_, err := c.CreateInstance(ctx, ok("dup"))
	wantCode(t, err, codes.AlreadyExists)

	missing := loc + "/instances/nope"
	_, err = c.GetInstance(ctx, &redispb.GetInstanceRequest{Name: missing})
	wantCode(t, err, codes.NotFound)
	_, err = c.DeleteInstance(ctx, &redispb.DeleteInstanceRequest{Name: missing})
	wantCode(t, err, codes.NotFound)
	_, err = c.GetInstanceAuthString(ctx, &redispb.GetInstanceAuthStringRequest{Name: missing})
	wantCode(t, err, codes.NotFound)
	_, err = c.GetInstance(ctx, &redispb.GetInstanceRequest{Name: "projects/p/instances/x"})
	wantCode(t, err, codes.InvalidArgument)
	_, err = c.UpdateInstance(ctx, &redispb.UpdateInstanceRequest{
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"display_name"}}, Instance: &redispb.Instance{Name: missing}})
	wantCode(t, err, codes.NotFound)

	dup := loc + "/instances/dup"
	_, err = c.GetInstanceAuthString(ctx, &redispb.GetInstanceAuthStringRequest{Name: dup})
	wantCode(t, err, codes.FailedPrecondition) // AUTH is off
	for what, mask := range map[string][]string{
		"no mask":            nil,
		"unknown path":       {"nonsense"},
		"output-only field":  {"host"},
		"tier":               {"tier"},
		"inside a map":       {"labels.env"},
		"identity":           {"name"},
		"size out of range":  {"memory_size_gb"}, // the update sets 0
		"unsupported config": {"redis_configs"},
	} {
		in := &redispb.Instance{Name: dup, RedisConfigs: map[string]string{"dir": "/x"}}
		_, err := c.UpdateInstance(ctx, &redispb.UpdateInstanceRequest{UpdateMask: &fieldmaskpb.FieldMask{Paths: mask}, Instance: in})
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("%s: got %v, want InvalidArgument", what, err)
		}
	}
	got := must(c.GetInstance(ctx, &redispb.GetInstanceRequest{Name: dup}))
	if got.MemorySizeGb != 1 || len(got.RedisConfigs) != 0 {
		t.Fatalf("a refused update changed the instance: %v", got)
	}
}

// TestNoDocker: without a container engine the API answers from metadata.
func TestNoDocker(t *testing.T) {
	s, c := start(t, pogi.Config{DockerHost: "/nonexistent/docker.sock"})
	got := must(must(c.CreateInstance(ctx, create("meta", &redispb.Instance{MemorySizeGb: 1}))).Wait(ctx))
	if got.State != redispb.Instance_READY || got.Host != "127.0.0.1" || got.Port != 6379 {
		t.Fatalf("instance without Docker: %v", got)
	}
	svc := s.Hub.Service("redis").(*memorystore.Service)
	b := must(svc.Save())
	svc.Reset()
	if _, err := c.GetInstance(ctx, &redispb.GetInstanceRequest{Name: got.Name}); status.Code(err) != codes.NotFound {
		t.Fatalf("after reset: %v", err)
	}
	if err := svc.Load(b); err != nil {
		t.Fatal(err)
	}
	if back := must(c.GetInstance(ctx, &redispb.GetInstanceRequest{Name: got.Name})); back.State != redispb.Instance_READY || back.MemorySizeGb != 1 {
		t.Fatalf("after load: %v", back)
	}
}

// TestREST: the REST client of google.golang.org/api, through the /redis/ prefix.
func TestREST(t *testing.T) {
	_, s, _ := fakeEngine(t)
	api := must(redisapi.NewService(ctx, option.WithEndpoint(s.URL+"/redis/"), option.WithoutAuthentication()))
	inst := api.Projects.Locations.Instances
	op := must(inst.Create(loc, &redisapi.Instance{MemorySizeGb: 1, Tier: "BASIC", Labels: map[string]string{"k": "v"}}).InstanceId("rest").Do())
	for i := 0; !op.Done; i++ {
		if i > 200 {
			t.Fatal("operation never finished")
		}
		time.Sleep(20 * time.Millisecond)
		op = must(api.Projects.Locations.Operations.Get(op.Name).Do())
	}
	if op.Error != nil {
		t.Fatalf("operation: %v", op.Error)
	}
	got := must(inst.Get(loc + "/instances/rest").Do())
	if got.State != "READY" || got.Host != "127.0.0.1" || got.Port == 0 || got.Tier != "BASIC" || got.Labels["k"] != "v" {
		t.Fatalf("rest get: %+v", got)
	}
	up := must(inst.Patch(got.Name, &redisapi.Instance{DisplayName: "x"}).UpdateMask("displayName").Do())
	if !up.Done {
		t.Fatalf("patch: %+v", up)
	}
	if got = must(inst.Get(got.Name).Do()); got.DisplayName != "x" {
		t.Fatalf("patched: %+v", got)
	}
	if _, err := inst.Patch(got.Name, &redisapi.Instance{}).UpdateMask("nope").Do(); err == nil {
		t.Fatal("bad updateMask path accepted")
	}
	if _, err := inst.Get(loc + "/instances/none").Do(); err == nil || !strings.Contains(err.Error(), "404") {
		t.Fatalf("missing instance: %v", err)
	}
	l := must(inst.List("projects/p/locations/-").Do())
	if len(l.Instances) != 1 {
		t.Fatalf("list: %+v", l)
	}
	d := must(inst.Delete(got.Name).Do())
	if d.Name == "" {
		t.Fatal("no delete operation")
	}
}

// TestRealRedis: with a container engine, the instance's host and port are a
// working Redis, and AUTH is enforced.
func TestRealRedis(t *testing.T) {
	if testing.Short() || !docker.New(docker.Options{}).Available() {
		t.Skip("needs a running container engine")
	}
	_, c := start(t, pogi.Config{})
	name := loc + "/instances/real"
	got := must(must(c.CreateInstance(ctx, create("real", &redispb.Instance{MemorySizeGb: 1, AuthEnabled: true}))).Wait(ctx))
	t.Cleanup(func() { must(c.DeleteInstance(ctx, &redispb.DeleteInstanceRequest{Name: name})) })
	if got.State != redispb.Instance_READY {
		t.Fatalf("state %v", got.State)
	}
	addr := fmt.Sprintf("%s:%d", got.Host, got.Port)
	pw := must(c.GetInstanceAuthString(ctx, &redispb.GetInstanceAuthStringRequest{Name: name})).AuthString
	if r := resp(t, addr, "", "SET", "k", "v"); !strings.HasPrefix(r, "-NOAUTH") {
		t.Fatalf("a command without AUTH got %q", r)
	}
	if r := resp(t, addr, pw, "SET", "k", "v"); r != "+OK" {
		t.Fatalf("SET: %q", r)
	}
	if r := resp(t, addr, pw, "GET", "k"); r != "$1\r\nv" {
		t.Fatalf("GET: %q", r)
	}
	// the update reaches the running server
	must(must(c.UpdateInstance(ctx, &redispb.UpdateInstanceRequest{UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"redis_configs"}},
		Instance: &redispb.Instance{Name: name, RedisConfigs: map[string]string{"maxmemory-policy": "allkeys-lfu"}}})).Wait(ctx))
	waitFor(t, func() bool {
		return strings.Contains(resp(t, addr, pw, "CONFIG", "GET", "maxmemory-policy"), "allkeys-lfu")
	})
}

// resp sends one command (after AUTH when pw is set) and returns the raw reply.
func resp(t *testing.T, addr, pw string, args ...string) string {
	t.Helper()
	conn := must(net.DialTimeout("tcp", addr, 5*time.Second))
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	send := func(a ...string) string {
		fmt.Fprintf(conn, "*%d\r\n", len(a))
		for _, x := range a {
			fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(x), x)
		}
		buf := make([]byte, 4096)
		n := must(conn.Read(buf))
		return strings.TrimSpace(string(buf[:n]))
	}
	if pw != "" {
		send("AUTH", pw)
	}
	return send(args...)
}
