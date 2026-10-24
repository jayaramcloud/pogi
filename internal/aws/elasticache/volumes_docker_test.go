package elasticache_test

// Data volumes with a real engine: data survives a forced recreate of the container; deleting the resource, a
// reset and the exit of a memory-only pogi leave no volume. Everything here has a name made from this test's own
// process id; nothing else on the engine is touched.

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nrndev/pogi/internal/aws/elasticache"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/docker"
)

// volRedis sends one inline command to the cluster and returns the first line of the reply.
func volRedis(t *testing.T, c cacheCluster, cmd string) string {
	t.Helper()
	conn, err := net.DialTimeout("tcp", net.JoinHostPort(c.Nodes[0].Endpoint.Address, strconv.Itoa(c.Nodes[0].Endpoint.Port)), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(10 * time.Second))
	fmt.Fprintf(conn, "%s\r\n", cmd)
	r := bufio.NewReader(conn)
	line, _ := r.ReadString('\n')
	if strings.HasPrefix(line, "$") && line != "$-1\r\n" { // a bulk string: the value is on the next line
		v, _ := r.ReadString('\n')
		line += v
	}
	return strings.TrimSpace(line)
}

func volHub(t *testing.T, dir string) (*core.Hub, string) {
	storage := "persistent"
	if dir == "" {
		storage = "memory"
	}
	h := core.NewHub(core.Config{Storage: storage, DataDir: dir}, nil)
	h.Register(elasticache.New(h))
	if err := h.OpenStorage(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(h.Close)
	return h, core.Listen(t, h)
}

func volAvailable(t *testing.T, base, id string) cacheCluster {
	t.Helper()
	var c cacheCluster
	eventually(t, id+" to be available (the first run pulls the image)", 5*time.Minute, func() bool {
		if c = describe(t, base, id); c.CacheClusterStatus == "create-failed" {
			t.Fatalf("container failed: %+v", c)
		}
		return c.CacheClusterStatus == "available"
	})
	return c
}

func volGone(t *testing.T, d *docker.Client, name string) {
	t.Helper()
	eventually(t, name+" to be gone", 30*time.Second, func() bool {
		_, err := d.Inspect(context.Background(), name)
		return err != nil && !d.HasVolume(context.Background(), name+"-data")
	})
}

func TestDataSurvivesRecreateAndGoesWithResource(t *testing.T) {
	d := docker.New(docker.Options{})
	if testing.Short() || !d.Available() {
		t.Skip("needs a running container engine")
	}
	ctx := context.Background()
	id := fmt.Sprintf("vol-%d", os.Getpid())
	name := "pogi-elasticache-" + id
	create := []string{"Action", "CreateCacheCluster", "CacheClusterId", id, "Engine", "redis", "CacheNodeType", "cache.t3.micro"}
	t.Cleanup(func() { d.Remove(ctx, name); d.RemoveVolume(ctx, name+"-data") })

	dir := t.TempDir()
	h, base := volHub(t, dir)
	ok(t, base, nil, create...)
	c := volAvailable(t, base, id)
	if r := volRedis(t, c, "SET kept yes"); r != "+OK" {
		t.Fatalf("SET: %q", r)
	}
	if r := volRedis(t, c, "SAVE"); r != "+OK" {
		t.Fatalf("SAVE: %q", r)
	}
	h.Close()                                   // keeps the container for the next run
	if err := d.Remove(ctx, name); err != nil { // the container is removed while pogi is down
		t.Fatal(err)
	}
	if !d.HasVolume(ctx, name+"-data") {
		t.Fatal("the data volume went with the container")
	}

	h, base = volHub(t, dir)
	// Wait for Redis itself to answer. An open port is not enough: Docker Desktop's port proxy accepts connections
	// before Redis listens, and a GET sent then gets no reply ("" below, not "$-1"), which looked like lost data.
	eventually(t, "the recreated container to answer", 2*time.Minute, func() bool { // the saved status says available at once
		c = describe(t, base, id)
		conn, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(c.Nodes[0].Endpoint.Port)), time.Second)
		if err != nil || c.CacheClusterStatus != "available" {
			return false
		}
		conn.Close()
		return volRedis(t, c, "PING") == "+PONG"
	})
	if r := volRedis(t, c, "GET kept"); r != "$3\r\nyes" {
		t.Fatalf("the data did not survive the recreate: GET kept = %q", r)
	}

	// deleting the resource removes its volume too
	ok(t, base, nil, "Action", "DeleteCacheCluster", "CacheClusterId", id)
	volGone(t, d, name)

	// a reset removes the volumes of what it deletes
	ok(t, base, nil, create...)
	volAvailable(t, base, id)
	if !d.HasVolume(ctx, name+"-data") {
		t.Fatal("no data volume for the new cluster")
	}
	resp, err := http.Post(base+"/_pogi/api/reset", "application/json", nil)
	if err != nil || resp.StatusCode/100 != 2 {
		t.Fatalf("reset: %v %v", resp, err)
	}
	resp.Body.Close()
	volGone(t, d, name)
	h.Close()

	// a memory-only pogi removes its volumes when it exits
	h, base = volHub(t, "")
	ok(t, base, nil, create...)
	volAvailable(t, base, id)
	h.Close()
	volGone(t, d, name)
}
