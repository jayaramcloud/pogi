// Package duckdb runs DuckDB in a container for the services that execute SQL
// on it (Athena, BigQuery): a Python image with DuckDB, started on the first
// query, serving the Script the service gives it on port 8080.
package duckdb

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/docker"
)

const (
	version   = "1.5.6"
	image     = "pogi/athena-duckdb" // named after the first service to use it; images already built are kept
	baseImage = "python:3.12-slim"
)

// ErrNoDocker is why a query cannot run without a container engine: Start's
// error for that matches it with errors.Is.
var ErrNoDocker = errors.New("pogi: Docker is not available, so queries cannot run")

type noDocker string

func (e noDocker) Error() string {
	return "pogi: Docker is not available, so " + string(e) + " cannot run queries"
}
func (noDocker) Is(target error) bool { return target == ErrNoDocker }

// Engine is the DuckDB container: started on the first query, kept for the
// life of the service and removed when it closes.
type Engine struct {
	Hub     *core.Hub
	Name    string // what the user knows the service as ("Athena"), for errors
	Service string // the label of the container and of its data directory
	Script  string // the Python program the container runs: a server on port 8080 that answers GET / with 200

	mu  sync.Mutex // held while it starts
	id  string
	url string
	dir string // the host directory mounted at /data
}

// Start runs the engine and returns its URL and the host directory it
// mounts at /data. The image is built on first use (pip needs the network
// then) and committed, so later starts are offline.
func (e *Engine) Start(ctx context.Context) (string, string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.url != "" {
		return e.url, e.dir, nil
	}
	d := e.Hub.Docker()
	if !d.Available() {
		return "", "", noDocker(e.Name)
	}
	img := image + ":" + version
	ok, err := d.HasImage(ctx, img)
	if err == nil && !ok {
		err = e.build(ctx, d)
	}
	if err == nil && e.dir == "" {
		var root string // absolute: Docker binds no relative paths
		if root, err = filepath.Abs(filepath.Join(e.Hub.Cfg.DataDir, e.Service)); err == nil {
			if err = os.MkdirAll(root, 0o755); err == nil {
				e.dir, err = os.MkdirTemp(root, "data-")
			}
		}
	}
	var id string
	if err == nil {
		id, err = d.Run(ctx, &docker.Container{Name: docker.Name(e.Service, "duckdb-"+core.RandHex(4)), Image: img, Service: e.Service,
			Resource: "duckdb", Cmd: []string{"python", "-c", e.Script}, Ports: map[int]int{8080: 0}, Binds: []string{e.dir + ":/data"}})
	}
	if err == nil {
		var info *docker.Info
		if info, err = d.Inspect(ctx, id); err == nil && info.HostPort(8080) == 0 {
			err = errors.New("its container publishes no port")
		} else if err == nil {
			e.url = "http://" + d.Addr(info.HostPort(8080))
			err = ready(ctx, e.url)
		}
		if err != nil {
			e.url = ""
			d.Remove(context.WithoutCancel(ctx), id)
		}
	}
	if err != nil {
		return "", "", fmt.Errorf("pogi: cannot start the DuckDB engine: %w", err)
	}
	e.id = id
	return e.url, e.dir, nil
}

// build installs DuckDB into the Python image and commits the result.
func (e *Engine) build(ctx context.Context, d *docker.Client) error {
	e.Hub.Log.Info(e.Service+": building the DuckDB engine image once", "image", image+":"+version)
	id, err := d.Run(ctx, &docker.Container{Name: docker.Name(e.Service, "build-"+core.RandHex(4)), Image: baseImage, Service: e.Service, Resource: "build",
		Cmd: []string{"pip", "install", "--no-cache-dir", "--disable-pip-version-check", "--root-user-action=ignore", "duckdb==" + version, "pytz==2025.2"}})
	if err != nil {
		return err
	}
	defer d.Remove(context.WithoutCancel(ctx), id)
	exit, err := d.Wait(ctx, id)
	if err == nil && exit != 0 {
		var out []string
		d.Logs(ctx, id, false, func(l string) { out = append(out, l) })
		err = fmt.Errorf("pip install duckdb==%s failed: %s", version, strings.Join(out[max(0, len(out)-3):], " "))
	}
	if err == nil {
		err = d.Commit(ctx, id, image, version)
	}
	return err
}

// ready waits until the server answers.
func ready(ctx context.Context, url string) error {
	client := &http.Client{Timeout: 2 * time.Second}
	for end := time.Now().Add(time.Minute); time.Now().Before(end); {
		if resp, err := client.Get(url); err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
	return errors.New("the DuckDB server did not answer within a minute")
}

// Forget drops a container that stopped answering; the next query starts another.
func (e *Engine) Forget() {
	e.mu.Lock()
	id := e.id
	e.id, e.url = "", ""
	e.mu.Unlock()
	if id != "" {
		e.Hub.Docker().Remove(context.Background(), id)
	}
}

// Close removes the container and its data directory.
func (e *Engine) Close() {
	e.Forget()
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.dir != "" {
		os.RemoveAll(e.dir)
		e.dir = ""
	}
}
