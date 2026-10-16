// Package dockertest fakes the Docker Engine API, so tests of services that
// run containers pass without an engine.
package dockertest

import (
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// Engine records what services ask of it, docker CLI style: "create --name n
// -p 127.0.0.1:h:p -e K=V image args…", "start n", "stop n", "restart n",
// "exec n cmd…", "rm -f -v n". Every published port is Port on inspect.
type Engine struct {
	Port int  // the host port inspect reports; point it at a listener of the test
	Down bool // answer like an engine that is not running

	mu    sync.Mutex
	calls []string
	ports map[string][]string // container → its published ports ("5432/tcp")
}

// Start serves a fake engine until the test ends and points DOCKER_HOST at it.
func Start(t testing.TB, port int, down bool) *Engine {
	e := &Engine{Port: port, Down: down, ports: map[string][]string{}}
	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)
	t.Setenv("DOCKER_HOST", "tcp://"+srv.Listener.Addr().String())
	return e
}

// Called lists the recorded calls that start with prefix.
func (e *Engine) Called(prefix string) []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	var out []string
	for _, c := range e.calls {
		if strings.HasPrefix(c, prefix) {
			out = append(out, c)
		}
	}
	return out
}

func (e *Engine) add(call string) {
	e.mu.Lock()
	e.calls = append(e.calls, call)
	e.mu.Unlock()
}

func (e *Engine) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if e.Down {
		http.Error(w, `{"message":"the engine is not running"}`, http.StatusServiceUnavailable)
		return
	}
	path := r.URL.Path
	if strings.HasPrefix(path, "/v1.") {
		path = path[strings.Index(path[1:], "/")+1:]
	}
	p := strings.Split(strings.Trim(path, "/"), "/")
	switch {
	case path == "/_ping":
		io.WriteString(w, "OK")
	case path == "/version":
		io.WriteString(w, `{"Version":"fake","ApiVersion":"1.41"}`)
	case path == "/containers/create":
		e.create(w, r)
	case path == "/containers/json":
		io.WriteString(w, "[]")
	case p[0] == "containers" && r.Method == http.MethodDelete:
		e.add("rm -f -v " + p[1])
		w.WriteHeader(http.StatusNoContent)
	case path == "/commit":
		e.add("commit " + r.URL.Query().Get("container") + " " + r.URL.Query().Get("repo") + ":" + r.URL.Query().Get("tag"))
		io.WriteString(w, `{"Id":"sha256:x"}`)
	case p[0] == "images" && r.Method == http.MethodDelete:
		e.add("rmi " + strings.Join(p[1:], "/"))
		io.WriteString(w, "[]")
	case p[0] == "containers" && len(p) == 3 && p[2] == "json":
		e.mu.Lock()
		ports := map[string][]map[string]string{}
		for _, k := range e.ports[p[1]] {
			ports[k] = []map[string]string{{"HostIp": "127.0.0.1", "HostPort": strconv.Itoa(e.Port)}}
		}
		e.mu.Unlock()
		json.NewEncoder(w).Encode(map[string]any{"Id": p[1], "State": map[string]any{"Status": "running", "Running": true},
			"NetworkSettings": map[string]any{"Ports": ports}})
	case p[0] == "containers" && len(p) == 3 && p[2] == "exec":
		var x struct{ Cmd []string }
		json.NewDecoder(r.Body).Decode(&x)
		e.add("exec " + p[1] + " " + strings.Join(x.Cmd, " "))
		w.WriteHeader(http.StatusCreated)
		io.WriteString(w, `{"Id":"x"}`)
	case p[0] == "containers" && len(p) == 3: // start, stop, restart
		e.add(p[2] + " " + p[1])
		w.WriteHeader(http.StatusNoContent)
	case p[0] == "exec" && p[len(p)-1] == "start": // no output
	case p[0] == "exec":
		io.WriteString(w, `{"Running":false,"ExitCode":0}`)
	default: // images and networks exist
		io.WriteString(w, "{}")
	}
}

func (e *Engine) create(w http.ResponseWriter, r *http.Request) {
	var c struct {
		Image      string
		Env, Cmd   []string
		HostConfig struct {
			PortBindings map[string][]struct{ HostIp, HostPort string }
		}
	}
	json.NewDecoder(r.Body).Decode(&c)
	name := r.URL.Query().Get("name")
	call := "create --name " + name
	keys := slices.Sorted(maps.Keys(c.HostConfig.PortBindings))
	for _, k := range keys {
		for _, b := range c.HostConfig.PortBindings[k] {
			call += " -p " + b.HostIp + ":" + b.HostPort + ":" + strings.TrimSuffix(k, "/tcp")
		}
	}
	for _, v := range c.Env {
		call += " -e " + v
	}
	call += " " + strings.Join(append([]string{c.Image}, c.Cmd...), " ")
	e.mu.Lock()
	e.calls, e.ports[name] = append(e.calls, call), keys
	e.mu.Unlock()
	w.WriteHeader(http.StatusCreated)
	fmt.Fprintf(w, `{"Id":%q}`, name)
}
