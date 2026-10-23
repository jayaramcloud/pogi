package core

import (
	"encoding/json"
	"errors"
	"io/fs"
	"net/http"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"
)

// ServiceStatus is one row of /_pogi/api/info.
type ServiceStatus struct {
	Name      string `json:"name"`
	Cloud     string `json:"cloud"`
	Title     string `json:"title"`
	Resources int    `json:"resources"`
	Requests  int    `json:"requests"`
}

// Status is the payload of /_pogi/api/info.
type Status struct {
	Version  string          `json:"version"`
	Started  time.Time       `json:"started"`
	Uptime   int64           `json:"uptime"`
	Region   string          `json:"region"`
	Persist  bool            `json:"persist"`
	DataDir  string          `json:"dataDir"`
	IAM      string          `json:"iam"`
	Lambda   string          `json:"lambda"`
	Ready    bool            `json:"ready"`
	Services []ServiceStatus `json:"services"`
	Problems []Problem       `json:"problems,omitempty"` // saved state that could not be read or written, ports that could not be opened
}

// Status reports versions, uptime and per-service counts.
func (h *Hub) Status() Status {
	totals := h.Requests.Totals()
	st := Status{Version: h.Cfg.Version, Started: h.Started, Uptime: int64(time.Since(h.Started).Seconds()),
		Region: h.Cfg.Region, Persist: len(h.stores) > 0, DataDir: h.Cfg.DataDir, IAM: h.IAMMode(), Lambda: h.Cfg.Lambda, Ready: h.Ready.Load(), Problems: h.Problems()}
	for _, s := range h.services {
		info := s.Info()
		n := 0
		if b, ok := s.(Browsable); ok {
			for _, t := range b.Resources() {
				if !t.Log {
					n += len(t.Rows)
				}
			}
		}
		st.Services = append(st.Services, ServiceStatus{info.Name, info.Cloud, info.Title, n, totals[info.Name]})
	}
	return st
}

// Resources returns the resource tables of one service or of all.
func (h *Hub) Resources(service string) map[string][]Table {
	out := map[string][]Table{}
	for _, s := range h.services {
		if b, ok := s.(Browsable); ok && (service == "" || s.Info().Name == service) {
			out[s.Info().Name] = b.Resources()
		}
	}
	return out
}

func (h *Hub) adminMux() *http.ServeMux {
	m := http.NewServeMux()
	m.HandleFunc("GET /_pogi/api/info", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, h.Status()) })
	m.HandleFunc("GET /_pogi/api/requests", h.apiRequests)
	m.HandleFunc("GET /_pogi/api/spans", h.apiSpans)
	m.HandleFunc("GET /_pogi/api/requests/{id}", func(w http.ResponseWriter, r *http.Request) {
		id, _ := strconv.ParseInt(r.PathValue("id"), 10, 64)
		if e, ok := h.Requests.Get(id); ok {
			writeJSON(w, 200, e)
			return
		}
		writeJSON(w, 404, map[string]string{"error": "request not found (the log keeps the last 1000)"})
	})
	m.HandleFunc("GET /_pogi/api/resources", func(w http.ResponseWriter, r *http.Request) {
		if name := r.URL.Query().Get("service"); h.knownService(w, name) {
			writeJSON(w, 200, h.Resources(name))
		}
	})
	m.HandleFunc("GET /_pogi/api/resources/{service}/{table}/{id...}", func(w http.ResponseWriter, r *http.Request) {
		if s, ok := h.Service(r.PathValue("service")).(Inspectable); ok {
			if v := s.Inspect(r.PathValue("table"), r.PathValue("id")); v != nil {
				writeJSON(w, 200, v)
				return
			}
		}
		writeJSON(w, 404, map[string]string{"error": "resource not found"})
	})
	m.HandleFunc("POST /_pogi/api/reset", func(w http.ResponseWriter, r *http.Request) {
		// No service parameter resets everything. A given one must name only enabled services, so an
		// empty value (an unset variable in a script) or a typo cannot wipe everything or claim success.
		var names []string
		if q := r.URL.Query(); q.Has("service") {
			names = strings.Split(q.Get("service"), ",")
			for _, n := range names {
				if h.Service(n) == nil {
					writeJSON(w, 400, map[string]string{"error": "no enabled service " + strconv.Quote(n) + " (pogi services lists them)"})
					return
				}
			}
		}
		h.Reset(names...)
		writeJSON(w, 200, map[string]any{"reset": names})
	})
	m.HandleFunc("GET /_pogi/api/snapshots", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, h.Snapshots()) })
	m.HandleFunc("POST /_pogi/api/snapshots/{name}", h.snapshotAction(func(p string, r *http.Request) error { return h.SaveFile(p) }))
	m.HandleFunc("POST /_pogi/api/snapshots/{name}/load", h.snapshotAction(func(p string, r *http.Request) error { return h.LoadFile(p) }))
	m.HandleFunc("DELETE /_pogi/api/snapshots/{name}", h.snapshotAction(func(p string, r *http.Request) error { return h.DeleteSnapshot(p) }))
	m.HandleFunc("PUT /_pogi/api/snapshots/{name}", h.snapshotAction(func(p string, r *http.Request) error { return h.ImportSnapshot(p, r.Body) }))
	m.HandleFunc("GET /_pogi/api/snapshots/{name}", func(w http.ResponseWriter, r *http.Request) {
		p, err := h.SnapshotPath(r.PathValue("name"))
		if err == nil {
			_, err = os.Stat(p) // http.ServeFile would answer a plain "404 page not found"
		}
		if err != nil {
			snapshotError(w, r.PathValue("name"), err)
			return
		}
		w.Header().Set("Content-Disposition", `attachment; filename="`+r.PathValue("name")+`.json"`)
		http.ServeFile(w, r, p)
	})
	m.HandleFunc("GET /_pogi/api/chaos", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, h.Chaos.List()) })
	m.HandleFunc("POST /_pogi/api/chaos", func(w http.ResponseWriter, r *http.Request) {
		var rule Rule
		err := json.NewDecoder(r.Body).Decode(&rule)
		if err == nil {
			err = rule.Check()
		}
		if err != nil {
			writeJSON(w, 400, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, 200, h.Chaos.Add(rule))
	})
	m.HandleFunc("DELETE /_pogi/api/chaos", func(w http.ResponseWriter, r *http.Request) { h.Chaos.Remove(""); w.WriteHeader(204) })
	m.HandleFunc("DELETE /_pogi/api/chaos/{id}", func(w http.ResponseWriter, r *http.Request) {
		if !h.Chaos.Remove(r.PathValue("id")) {
			writeJSON(w, 404, map[string]string{"error": "rule not found"})
			return
		}
		w.WriteHeader(204)
	})

	// LocalStack and moto compatibility, so existing tooling keeps working.
	m.HandleFunc("GET /_localstack/health", func(w http.ResponseWriter, r *http.Request) {
		svcs := map[string]string{}
		for _, s := range h.services {
			svcs[s.Info().Name] = "running"
		}
		writeJSON(w, 200, map[string]any{"services": svcs, "edition": "community", "version": "pogi-" + h.Cfg.Version})
	})
	m.HandleFunc("GET /_localstack/info", func(w http.ResponseWriter, r *http.Request) {
		_, docker := os.Stat("/.dockerenv")
		writeJSON(w, 200, map[string]any{
			"version": "pogi-" + h.Cfg.Version, "edition": "community", "is_license_activated": false,
			"session_id": "pogi", "machine_id": "pogi", "system": runtime.GOOS, "is_docker": docker == nil,
			"server_time_utc": time.Now().UTC().Format(time.RFC3339), "uptime": int64(time.Since(h.Started).Seconds()),
		})
	})
	m.HandleFunc("GET /_localstack/init", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, h.Init.Status()) })
	m.HandleFunc("GET /_localstack/init/{stage}", func(w http.ResponseWriter, r *http.Request) {
		st := h.Init.Status()
		stage := strings.ToUpper(r.PathValue("stage"))
		var scripts []InitScript
		for _, s := range st["scripts"].([]InitScript) {
			if s.Stage == stage {
				scripts = append(scripts, s)
			}
		}
		writeJSON(w, 200, map[string]any{"completed": st["completed"].(map[string]bool)[stage], "scripts": scripts})
	})
	m.HandleFunc("POST /moto-api/reset", func(w http.ResponseWriter, r *http.Request) {
		h.Reset()
		writeJSON(w, 200, map[string]string{"status": "ok"})
	})
	return m
}

// knownService answers 404 and returns false when a ?service= filter names no running service, so a
// typo or a service that is off is not read as "nothing there". An empty name means every service.
func (h *Hub) knownService(w http.ResponseWriter, name string) bool {
	if name == "" || h.Service(name) != nil {
		return true
	}
	writeJSON(w, 404, map[string]string{"error": "no enabled service " + strconv.Quote(name) + " (pogi services lists them)"})
	return false
}

func (h *Hub) apiRequests(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	if !h.knownService(w, q.Get("service")) {
		return
	}
	after, _ := strconv.ParseInt(q.Get("after"), 10, 64)
	limit, _ := strconv.Atoi(q.Get("limit"))
	if limit <= 0 {
		limit = 200
	}
	if wait, _ := strconv.Atoi(q.Get("wait")); wait > 0 {
		h.Requests.Wait(r.Context(), after, time.Duration(min(wait, 60))*time.Second)
	}
	out := h.Requests.Query(after, limit, q.Get("service"), q.Get("q"), q.Get("errors") != "")
	if out == nil {
		out = []Entry{}
	}
	writeJSON(w, 200, out)
}

// apiSpans returns the newest ?limit= flow steps (default 500, at most 5000), oldest first.
func (h *Hub) apiSpans(w http.ResponseWriter, r *http.Request) {
	limit := 500
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 5000 {
			writeJSON(w, 400, map[string]string{"error": "limit must be a whole number from 1 to 5000"})
			return
		}
		limit = n
	}
	out := h.Spans.All()
	if len(out) > limit {
		out = out[len(out)-limit:]
	}
	if out == nil {
		out = []Span{}
	}
	writeJSON(w, 200, out)
}

func (h *Hub) snapshotAction(f func(path string, r *http.Request) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		p, err := h.SnapshotPath(r.PathValue("name"))
		if err == nil {
			err = f(p, r)
		}
		if err != nil {
			snapshotError(w, r.PathValue("name"), err)
			return
		}
		writeJSON(w, 200, map[string]string{"snapshot": r.PathValue("name")})
	}
}

// snapshotError answers a failed snapshot call: 404 for a name with no file, worded without the
// server's file path, and 400 for anything else.
func snapshotError(w http.ResponseWriter, name string, err error) {
	if errors.Is(err, fs.ErrNotExist) {
		writeJSON(w, 404, map[string]string{"error": "no snapshot named " + strconv.Quote(name) + " (pogi snapshot ls lists them)"})
		return
	}
	writeJSON(w, 400, map[string]string{"error": err.Error()})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

// SortedKeys returns the keys of a string-keyed map in order.
func SortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
