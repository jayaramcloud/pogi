package core

import (
	"encoding/json"
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
}

// Status reports versions, uptime and per-service counts.
func (h *Hub) Status() Status {
	totals := h.Requests.Totals()
	st := Status{Version: h.Cfg.Version, Started: h.Started, Uptime: int64(time.Since(h.Started).Seconds()),
		Region: h.Cfg.Region, Persist: len(h.stores) > 0, DataDir: h.Cfg.DataDir, IAM: h.IAMMode(), Lambda: h.Cfg.Lambda, Ready: h.Ready.Load()}
	for _, s := range h.services {
		info := s.Info()
		n := 0
		if b, ok := s.(Browsable); ok {
			for _, t := range b.Resources() {
				n += len(t.Rows)
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
	m.HandleFunc("GET /_pogi/api/requests/{id}", func(w http.ResponseWriter, r *http.Request) {
		id, _ := strconv.ParseInt(r.PathValue("id"), 10, 64)
		if e, ok := h.Requests.Get(id); ok {
			writeJSON(w, 200, e)
			return
		}
		writeJSON(w, 404, map[string]string{"error": "request not found (the log keeps the last 1000)"})
	})
	m.HandleFunc("GET /_pogi/api/resources", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, h.Resources(r.URL.Query().Get("service")))
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
		var names []string
		if s := r.URL.Query().Get("service"); s != "" {
			names = strings.Split(s, ",")
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
		if err != nil {
			writeJSON(w, 400, map[string]string{"error": err.Error()})
			return
		}
		w.Header().Set("Content-Disposition", `attachment; filename="`+r.PathValue("name")+`.json"`)
		http.ServeFile(w, r, p)
	})
	m.HandleFunc("GET /_pogi/api/chaos", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, h.Chaos.List()) })
	m.HandleFunc("POST /_pogi/api/chaos", func(w http.ResponseWriter, r *http.Request) {
		var rule Rule
		if err := json.NewDecoder(r.Body).Decode(&rule); err != nil {
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

func (h *Hub) apiRequests(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
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

func (h *Hub) snapshotAction(f func(path string, r *http.Request) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		p, err := h.SnapshotPath(r.PathValue("name"))
		if err == nil {
			err = f(p, r)
		}
		if err != nil {
			writeJSON(w, 400, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, 200, map[string]string{"snapshot": r.PathValue("name")})
	}
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
