package rds_test

// Failure-mode audit C5: a restart with saved RDS state, when the container
// is still there, was removed while pogi was down, or Docker is off. The fake
// engine is wrapped so that a "removed" container answers 404 until it is
// created again, like the real engine.

import (
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/nrndev/pogi/internal/aws/rds"
	"github.com/nrndev/pogi/internal/core"
)

// persistentHub makes a hub that keeps state in dir.
func persistentHub(t *testing.T, dir string) (*core.Hub, *rds.Service) {
	h := core.NewHub(core.Config{Storage: "persistent", DataDir: dir}, nil)
	svc := rds.New(h)
	h.Register(svc)
	if err := h.OpenStorage(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(h.Close) // a restart stops the pogi before: it holds the data folder's lock
	return h, svc
}

func TestRestartWithContainers(t *testing.T) {
	f := fake(t, false)
	var gone atomic.Bool // the container was removed behind pogi's back
	real := os.Getenv("DOCKER_HOST")
	engine := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if gone.Load() && strings.Contains(r.URL.Path, "/containers/pogi-rds-keep") {
			if r.Method == http.MethodDelete {
				gone.Store(false) // "removed" already; the real engine says 404 too, but the run goes on
			}
			http.Error(w, `{"message":"No such container: pogi-rds-keep"}`, http.StatusNotFound)
			return
		}
		if strings.HasSuffix(r.URL.Path, "/containers/create") {
			gone.Store(false) // a new container with that name
		}
		req, _ := http.NewRequest(r.Method, "http://"+strings.TrimPrefix(real, "tcp://")+r.URL.RequestURI(), r.Body)
		req.Header = r.Header
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		defer res.Body.Close()
		w.WriteHeader(res.StatusCode)
		buf := make([]byte, 32<<10)
		for {
			n, err := res.Body.Read(buf)
			w.Write(buf[:n])
			if err != nil {
				return
			}
		}
	}))
	t.Cleanup(engine.Close)
	t.Setenv("DOCKER_HOST", "tcp://"+engine.Listener.Addr().String())

	dir := t.TempDir()
	h, _ := persistentHub(t, dir)
	base := core.Listen(t, h)
	create(t, base, "keep")
	await(t, base, "keep", "available")
	h.Close() // writes state, keeps the container

	t.Run("container still there", func(t *testing.T) {
		_, again := persistentHub(t, dir)
		eventually(t, "reconnect", func() bool {
			v, _ := again.Inspect("DB instances", "keep").(*rds.DBInstance)
			return v != nil && v.DBInstanceStatus == "available" && len(f.Called("start pogi-rds-keep")) >= 2
		})
	})
	t.Run("container removed while pogi was down keeps its data volume", func(t *testing.T) {
		creates, rms := len(f.Called("create --name pogi-rds-keep")), len(f.Called("volume rm pogi-rds-keep-data"))
		gone.Store(true)
		_, again := persistentHub(t, dir)
		eventually(t, "a new container", func() bool { return len(f.Called("create --name pogi-rds-keep")) > creates })
		eventually(t, "state", func() bool {
			v, _ := again.Inspect("DB instances", "keep").(*rds.DBInstance)
			return v != nil && v.DBInstanceStatus != "creating"
		})
		v := again.Inspect("DB instances", "keep").(*rds.DBInstance)
		// The data lives in the named volume, so nothing is lost: available, no "empty" note, same volume mounted again.
		if v.DBInstanceStatus != "available" || v.Pogi.Note != "" {
			t.Fatalf("the data volume was still there: want available with no note, got %q %q", v.DBInstanceStatus, v.Pogi.Note)
		}
		if n := len(f.Called("bind pogi-rds-keep pogi-rds-keep-data:/var/lib/postgresql/data")); n < 2 {
			t.Fatalf("the new container did not mount the data volume again: %v", f.Called("bind"))
		}
		if got := f.Called("volume rm pogi-rds-keep-data"); len(got) != rms {
			t.Fatalf("the volume was removed on a recreate: %v", got)
		}
	})
	t.Run("container and volume removed while pogi was down", func(t *testing.T) {
		creates := len(f.Called("create --name pogi-rds-keep"))
		gone.Store(true)
		f.DropVolume("pogi-rds-keep-data")
		_, again := persistentHub(t, dir)
		eventually(t, "a new container", func() bool { return len(f.Called("create --name pogi-rds-keep")) > creates })
		// a resumed instance shows its saved status ("available") until the new container answers, so wait for the
		// note itself: reading at once raced the recreate and saw no note yet (a run of 2026-10-22)
		eventually(t, "the note that the data is gone", func() bool {
			v, _ := again.Inspect("DB instances", "keep").(*rds.DBInstance)
			return v != nil && v.Pogi.Note == rds.Removed
		})
	})
	t.Run("docker off at restart", func(t *testing.T) {
		t.Setenv("DOCKER_HOST", "tcp://127.0.0.1:1") // nothing listens
		_, again := persistentHub(t, dir)
		v := again.Inspect("DB instances", "keep").(*rds.DBInstance)
		t.Logf("status %q, note %q, port %d", v.DBInstanceStatus, v.Pogi.Note, v.Endpoint.Port)
		if v.DBInstanceStatus == "available" && v.Pogi.Note == "" {
			t.Fatalf("Docker is off but the instance shows available with no note: its endpoint %d answers nothing", v.Endpoint.Port)
		}
	})
}
