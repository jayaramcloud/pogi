package rds_test

// A dead, silent or killed container, as RDS shows it. Dead and silent engines are fakes started here. The real
// engine is used only for one container this file starts, and that one is removed at the end.

import (
	"crypto/rand"
	"io"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nrndev/pogi/internal/aws/rds"
	"github.com/nrndev/pogi/internal/aws/secretsmanager"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/docker"
)

func serve(t *testing.T) (*rds.Service, string) {
	var svc *rds.Service
	_, base := core.ServeTest(t, func(h *core.Hub) core.Service { svc = rds.New(h); return svc },
		func(h *core.Hub) core.Service { return secretsmanager.New(h) })
	return svc, base
}

// A database created while Docker is not running stays "available" with its note (the API works), and the console
// table shows the note next to the status. A Docker that starts later is used for new databases.
func TestDatabaseCreatedWithoutDockerShowsNoteInTable(t *testing.T) {
	t.Setenv("DOCKER_HOST", "tcp://127.0.0.1:"+strconv.Itoa(freePort(t))) // nothing listens
	svc, base := serve(t)
	create(t, base, "nodocker")
	if st := describe(t, base, "nodocker").DBInstanceStatus; st != "available" {
		t.Errorf("status %q, want available", st)
	}
	row := svc.Resources()[0].Rows[0]
	if want := "available · no database: Docker isn't running"; row[2] != want {
		t.Errorf("status cell %q, want %q", row[2], want)
	}
	f := fake(t, false)                 // Docker starts now
	time.Sleep(2200 * time.Millisecond) // pogi looks again every 2s
	create(t, base, "withdocker")
	eventually(t, "container of the new database", func() bool { return len(f.Called("create")) > 0 })
}

// hungEngine answers pings, inspects and network calls, and never answers container create. No container exists.
func hungEngine(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path
		switch {
		case strings.HasSuffix(p, "/_ping"):
			io.WriteString(w, "OK")
		case strings.HasSuffix(p, "/version"):
			io.WriteString(w, `{"Version":"29.0.0","ApiVersion":"1.54"}`)
		case strings.Contains(p, "/containers/create"):
			io.Copy(io.Discard, r.Body) // reading the body is how the server notices the client left
			select {
			case <-release:
			case <-r.Context().Done():
			}
		case strings.Contains(p, "/containers/pogi-") && strings.HasSuffix(p, "/json"):
			http.Error(w, `{"message":"No such container"}`, http.StatusNotFound)
		default:
			io.WriteString(w, "{}")
		}
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(release) }) // runs first: lets the hung handler go, so Close can finish
	t.Setenv("DOCKER_HOST", "tcp://"+srv.Listener.Addr().String())
}

// D2: the engine hangs in the middle of a create. The create API call must not hang (it does
// not: the container starts in the background). The database then sits in "creating" and
// nothing ends it (no timeout on the engine call), so the console shows a spinner for ever.
func TestDatabaseOnSilentEngineFailsWithNote(t *testing.T) {
	docker.CallLimit = 2 * time.Second
	t.Cleanup(func() { docker.CallLimit = 2 * time.Minute })
	hungEngine(t)
	svc, base := serve(t)
	start := time.Now()
	create(t, base, "hung")
	if d := time.Since(start); d > 3*time.Second {
		t.Fatalf("the create API call itself hung for %v", d)
	}
	var st string
	eventually(t, "the hung create to end", func() bool { st = describe(t, base, "hung").DBInstanceStatus; return st != "creating" })
	if note := svc.Inspect("DB instances", "hung").(*rds.DBInstance).Pogi.Note; st != "failed" || note != "Docker stopped answering" {
		t.Errorf("status %q, note %q: want failed, Docker stopped answering", st, note)
	}
	// can the user remove it?
	ok(t, base, nil, "Action", "DeleteDBInstance", "DBInstanceIdentifier", "hung", "SkipFinalSnapshot", "true")
	eventually(t, "the hung database to be deleted", func() bool {
		st, _ := call(t, base, "Action", "DescribeDBInstances", "DBInstanceIdentifier", "hung")
		return st == 404
	})
}

// A database container killed for lack of memory (real engine, a postgres container pogi starts here; only that
// container is capped, with docker update) shows as failed, with a note that says why.
func TestKilledContainerFailsTheDatabase(t *testing.T) {
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("no docker")
	}
	if out, err := exec.Command("docker", "image", "inspect", "postgres:17-alpine").CombinedOutput(); err != nil {
		t.Skipf("postgres:17-alpine not local: %s", out)
	}
	svc, base := serve(t)
	id := "audit-oom-" + strings.ToLower(rand.Text()[:6])
	name := "pogi-rds-" + id
	t.Cleanup(func() { exec.Command("docker", "rm", "-f", name).Run() })
	create(t, base, id, "EngineVersion", "17.2")
	await(t, base, id, "available")
	if out, err := exec.Command("docker", "update", "--memory", "6m", "--memory-swap", "6m", name).CombinedOutput(); err != nil {
		t.Skipf("could not cap the memory of %s: %s", name, out)
	}
	var state string
	for end := time.Now().Add(60 * time.Second); time.Now().Before(end); time.Sleep(500 * time.Millisecond) {
		out, _ := exec.Command("docker", "inspect", "-f", "{{.State.Status}} oom={{.State.OOMKilled}} exit={{.State.ExitCode}}", name).Output()
		if state = strings.TrimSpace(string(out)); strings.HasPrefix(state, "exited") {
			break
		}
	}
	t.Logf("engine says: %s", state)
	if !strings.HasPrefix(state, "exited") {
		t.Skip("the container was not killed by the cap: " + state)
	}
	var i instance
	eventually(t, "the watcher to notice", func() bool { i = describe(t, base, id); return i.DBInstanceStatus != "available" })
	v := svc.Inspect("DB instances", id).(*rds.DBInstance)
	t.Logf("after the OOM kill: status %q, pogi state %q, note %q", i.DBInstanceStatus, v.Pogi.State, v.Pogi.Note)
	if i.DBInstanceStatus == "available" {
		t.Errorf("RDS still says available 15s after the engine killed the container (%s)", state)
	}
	if !strings.Contains(strings.ToLower(v.Pogi.Note), "memory") {
		t.Errorf("the console note does not say why (note %q)", v.Pogi.Note)
	}
}

// A container that exits as it starts fails the database at once, with its exit code and last log line, and
// pogi does not poll for five minutes.
func TestContainerThatExitsAtStartFailsAtOnce(t *testing.T) {
	var made atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path
		switch {
		case strings.HasSuffix(p, "/_ping"):
			io.WriteString(w, "OK")
		case strings.HasSuffix(p, "/version"):
			io.WriteString(w, `{"Version":"29.0.0","ApiVersion":"1.54"}`)
		case strings.Contains(p, "/containers/create"):
			made.Store(true)
			w.WriteHeader(http.StatusCreated)
			io.WriteString(w, `{"Id":"x"}`)
		case strings.HasSuffix(p, "/start"):
			w.WriteHeader(http.StatusNoContent)
		case strings.HasSuffix(p, "/wait"):
			io.WriteString(w, `{"StatusCode":1}`)
		case strings.HasSuffix(p, "/logs"):
			w.Write(append([]byte{2, 0, 0, 0, 0, 0, 0, 5}, "boom\n"...))
		case strings.Contains(p, "/containers/") && !made.Load():
			http.Error(w, `{"message":"No such container"}`, http.StatusNotFound)
		case strings.Contains(p, "/containers/"):
			io.WriteString(w, `{"State":{"Status":"exited","ExitCode":1},"NetworkSettings":{"Ports":{"5432/tcp":[{"HostPort":"54999"}]}}}`)
		default:
			io.WriteString(w, "{}")
		}
	}))
	t.Cleanup(srv.Close)
	t.Setenv("DOCKER_HOST", "tcp://"+srv.Listener.Addr().String())
	svc, base := serve(t)
	create(t, base, "dies")
	var st string
	eventually(t, "the database to fail", func() bool { st = describe(t, base, "dies").DBInstanceStatus; return st != "creating" })
	if note := svc.Inspect("DB instances", "dies").(*rds.DBInstance).Pogi.Note; st != "failed" || note != "stopped: exit 1 — boom" {
		t.Errorf("status %q, note %q", st, note)
	}
}
