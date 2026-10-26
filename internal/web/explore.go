package web

// Explorers: pages that query, decrypt, reveal or draw what the services hold.
// Each one is a new route under /_pogi/ in its own file; this one has the
// shared parts: registration, the in-process gateway call and result tables.

import (
	"bytes"
	"cmp"
	"encoding/json"
	"html/template"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"

	"github.com/nrndev/pogi/internal/core"
)

func (ui *UI) exploreRoutes(m *http.ServeMux) {
	layout := shell()
	for _, p := range []string{"query", "sql", "sfn", "logq", "crypto", "secrets", "mail", "network"} {
		ui.pages["x-"+p] = template.Must(template.Must(layout.Clone()).ParseFS(files, "templates/x-common.html", "templates/x-"+p+".html"))
	}
	m.HandleFunc("GET /_pogi/explore", ui.explore)
	m.HandleFunc("GET /_pogi/query/{kind}", ui.queryPage)
	m.HandleFunc("GET /_pogi/sql", ui.sqlPage)
	m.HandleFunc("POST /_pogi/sql", ui.sqlRun)
	m.HandleFunc("GET /_pogi/sfn", ui.sfnPage)
	m.HandleFunc("GET /_pogi/logs", ui.logPage)
	m.HandleFunc("GET /_pogi/crypto", ui.cryptoPage)
	m.HandleFunc("POST /_pogi/crypto", ui.cryptoRun)
	m.HandleFunc("GET /_pogi/secrets", ui.secretsPage)
	m.HandleFunc("POST /_pogi/secrets/reveal", ui.reveal)
	m.HandleFunc("GET /_pogi/inbox/{n}", ui.mailPage)
	m.HandleFunc("GET /_pogi/network", ui.networkPage)
	ui.dataRoutes(m)
}

// explore is the old index of explorers: they live on their services' pages now.
func (ui *UI) explore(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, "/_pogi/services", http.StatusSeeOther)
}

// xrender renders an explorer. The service page loads one into its tab with an htmx request aimed at
// #xtab, and that gets the content alone; a plain request gets the whole page.
func (ui *UI) xrender(w http.ResponseWriter, r *http.Request, name, title, svc string, data any) {
	if r.Header.Get("HX-Target") != "xtab" {
		ui.render(w, r, name, title, svc, data)
		return
	}
	var buf bytes.Buffer
	if err := ui.pages[name].ExecuteTemplate(&buf, "content", page{Title: title, Active: svc, Endpoint: "http://" + r.Host, Scope: ui.scope(r), Back: r.URL.RequestURI(), Data: data}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	buf.WriteTo(w)
}

// gw sends an in-process request through the gateway as the console (no
// signature, chaos or IAM) and returns the status and body. host is the Host
// header; "" is the main address.
func (ui *UI) gw(method, host, path string, header map[string]string, body []byte) (int, []byte) {
	req, err := http.NewRequest(method, path, bytes.NewReader(body))
	if err != nil {
		return http.StatusBadRequest, []byte(err.Error())
	}
	req.Host = cmp.Or(host, "localhost:"+strconv.Itoa(ui.hub.Cfg.Port))
	req.Header.Set("X-Pogi-Internal", "1") // honoured only for in-process calls: RemoteAddr stays empty
	for k, v := range header {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	ui.hub.ServeHTTP(rec, req)
	return rec.Code, rec.Body.Bytes()
}

// gwJSON is gw for JSON in and out; a failure status becomes an error that
// carries the service's message.
func (ui *UI) gwJSON(method, host, path string, header map[string]string, in, out any) error {
	var body []byte
	if in != nil {
		body, _ = json.Marshal(in)
	}
	h := map[string]string{"Content-Type": "application/json"}
	for k, v := range header {
		h[k] = v
	}
	st, b := ui.gw(method, host, path, h, body)
	if st >= 300 {
		return &httpError{st, apiMessage(b)}
	}
	if out != nil && len(b) > 0 {
		return json.Unmarshal(b, out)
	}
	return nil
}

type httpError struct {
	Status int
	Msg    string
}

func (e *httpError) Error() string { return strconv.Itoa(e.Status) + ": " + e.Msg }

// apiMessage digs the message out of an error body (Google, Azure and AWS shapes).
func apiMessage(b []byte) string {
	var m map[string]any
	if json.Unmarshal(b, &m) != nil {
		return strings.TrimSpace(string(b))
	}
	if e, ok := m["error"].(map[string]any); ok {
		m = e
	}
	for _, k := range []string{"message", "Message", "msg"} {
		if s, ok := m[k].(string); ok && s != "" {
			return s
		}
	}
	return strings.TrimSpace(string(b))
}

// rows returns the rows of one of a service's resource tables.
// Only the rows in the header switcher's scope (account, region, project, subscription) are returned.
func (ui *UI) rows(r *http.Request, svc, table string) [][]string {
	b, ok := ui.hub.Service(svc).(core.Browsable)
	if !ok {
		return nil
	}
	sc := ui.scope(r).forCloud(ui.hub.Service(svc).Info().Cloud)
	for _, t := range b.Resources() {
		if t.Name == table {
			var out [][]string
			for _, row := range t.Rows {
				if sc.keeps(t, row) {
					out = append(out, row)
				}
			}
			return out
		}
	}
	return nil
}

// inspectMap returns a service's Inspect output as a generic JSON tree.
func (ui *UI) inspectMap(svc, table, id string) map[string]any {
	s, ok := ui.hub.Service(svc).(core.Inspectable)
	if !ok {
		return nil
	}
	b, _ := json.Marshal(s.Inspect(table, id))
	var m map[string]any
	json.Unmarshal(b, &m)
	return m
}

// dig reads m[k1][k2]... as a string ("" when absent).
func dig(m any, keys ...string) string {
	for _, k := range keys {
		mm, _ := m.(map[string]any)
		m = mm[k]
	}
	switch v := m.(type) {
	case string:
		return v
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	}
	return ""
}

// choice is an option of a select.
type choice struct{ Value, Label string }

// result is a table an explorer shows.
type result struct {
	Cols []string
	Rows [][]string
	Note string
	Raw  string // the response as JSON, for the details pane
}

// cell renders a value for a table cell: strings as they are, the rest as compact JSON.
func cell(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	b, _ := json.Marshal(v)
	return string(b)
}

// fromDocs makes a table of JSON documents: a column per key (system keys
// starting with "_" last), or one "value" column for scalars.
func fromDocs(docs []any) *result {
	keys := map[string]bool{}
	scalar := false
	for _, d := range docs {
		if m, ok := d.(map[string]any); ok {
			for k := range m {
				keys[k] = true
			}
		} else {
			scalar = true
		}
	}
	cols := core.SortedKeys(keys)
	slices.SortStableFunc(cols, func(a, b string) int { return under(a) - under(b) })
	if scalar {
		cols = append([]string{"value"}, cols...)
	}
	res := &result{Cols: cols}
	for _, d := range docs {
		m, isMap := d.(map[string]any)
		row := make([]string, len(cols))
		for i, c := range cols {
			switch v, has := m[c]; {
			case has:
				row[i] = cell(v)
			case !isMap && c == "value":
				row[i] = cell(d)
			}
		}
		res.Rows = append(res.Rows, row)
	}
	b, _ := json.MarshalIndent(docs, "", "  ")
	res.Raw = string(b)
	return res
}

func under(s string) int {
	if strings.HasPrefix(s, "_") {
		return 1
	}
	return 0
}
