package web

// Log queries: CloudWatch Logs Insights (or a filter pattern), KQL on Azure
// Monitor workspaces, and Cloud Logging filters.

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

type logData struct {
	Kind    string
	Targets []choice
	V       url.Values
	Res     *result
	Err     string
}

func (ui *UI) logPage(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	d := &logData{Kind: q.Get("kind"), V: q}
	var run func(url.Values) (*result, error)
	switch d.Kind {
	case "kql":
		for _, ws := range ui.rows(r, "azmonitor", "Workspaces") {
			d.Targets = append(d.Targets, choice{ws[1], ws[0]})
		}
		run = ui.kqlRun
	case "gcp":
		run = func(q url.Values) (*result, error) {
			if q.Get("project") == "" { // the switcher's project is the default
				q = url.Values{"project": {ui.scope(r).project()}, "filter": q["filter"], "limit": q["limit"]}
			}
			return ui.gcpLogRun(q)
		}
	default:
		d.Kind = "cwl"
		for _, g := range ui.rows(r, "logs", "Log groups") {
			d.Targets = append(d.Targets, choice{g[2] + "/" + g[1] + "/" + g[0], g[0]})
		}
		run = ui.cwlRun
	}
	if q.Get("run") != "" {
		res, err := run(q)
		d.Res = res
		if err != nil {
			d.Err = err.Error()
		}
	}
	ui.render(w, r, "x-logq", "Log queries", "explore", d)
}

// ---- CloudWatch Logs ----

func (ui *UI) cwlRun(q url.Values) (*result, error) {
	t := strings.SplitN(q.Get("target"), "/", 3) // account/region/group
	if len(t) != 3 {
		return nil, errors.New("choose a log group")
	}
	account, region, group := t[0], t[1], t[2]
	mins, _ := strconv.Atoi(q.Get("minutes"))
	if mins <= 0 {
		mins = 1440
	}
	end := time.Now().Add(time.Hour)
	start := time.Now().Add(-time.Duration(mins) * time.Minute)
	if q.Get("mode") == "filter" {
		var out struct {
			Events []struct {
				Timestamp     int64
				LogStreamName string
				Message       string
			}
		}
		err := ui.hub.Call(account, region, "Logs_20140328", "FilterLogEvents", map[string]any{"logGroupName": group,
			"filterPattern": q.Get("query"), "startTime": start.UnixMilli(), "endTime": end.UnixMilli(), "limit": 200}, &out)
		if err != nil {
			return nil, err
		}
		res := &result{Cols: []string{"timestamp", "stream", "message"}, Note: fmt.Sprintf("%d events", len(out.Events))}
		for _, e := range out.Events {
			res.Rows = append(res.Rows, []string{time.UnixMilli(e.Timestamp).UTC().Format("2006-01-02 15:04:05.000"), e.LogStreamName, strings.TrimRight(e.Message, "\n")})
		}
		return res, nil
	}
	query := strings.TrimSpace(q.Get("query"))
	if query == "" {
		query = "fields @timestamp, @message | sort @timestamp desc | limit 50"
	}
	var started struct{ QueryId string }
	if err := ui.hub.Call(account, region, "Logs_20140328", "StartQuery", map[string]any{"logGroupNames": []string{group},
		"startTime": start.Unix(), "endTime": end.Unix(), "queryString": query}, &started); err != nil {
		return nil, err
	}
	var out struct {
		Status  string
		Results [][]struct{ Field, Value string }
	}
	for i := 0; i < 50; i++ { // pogi finishes a query at once; the loop is for slow ones
		if err := ui.hub.Call(account, region, "Logs_20140328", "GetQueryResults", map[string]any{"queryId": started.QueryId}, &out); err != nil {
			return nil, err
		}
		if out.Status != "Running" && out.Status != "Scheduled" {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	res := &result{Note: fmt.Sprintf("%s · %d rows", out.Status, len(out.Results))}
	idx := map[string]int{}
	for _, row := range out.Results {
		for _, f := range row {
			if _, ok := idx[f.Field]; !ok && f.Field != "@ptr" {
				idx[f.Field] = len(res.Cols)
				res.Cols = append(res.Cols, f.Field)
			}
		}
	}
	for _, row := range out.Results {
		cells := make([]string, len(res.Cols))
		for _, f := range row {
			if i, ok := idx[f.Field]; ok {
				cells[i] = f.Value
			}
		}
		res.Rows = append(res.Rows, cells)
	}
	return res, nil
}

// ---- Azure Monitor (KQL) ----

func (ui *UI) kqlRun(q url.Values) (*result, error) {
	ws := q.Get("target")
	if ws == "" {
		return nil, errors.New("choose a workspace")
	}
	var out struct {
		Tables []struct {
			Name    string
			Columns []struct{ Name string }
			Rows    [][]any
		}
	}
	// the query API of Azure Monitor, as the Log Analytics SDKs call it
	if err := ui.gwJSON("POST", "", "/_azure/monitor/query/v1/workspaces/"+url.PathEscape(ws)+"/query", nil, map[string]string{"query": q.Get("query")}, &out); err != nil {
		return nil, err
	}
	if len(out.Tables) == 0 {
		return &result{Note: "no result table"}, nil
	}
	t := out.Tables[0]
	res := &result{Note: fmt.Sprintf("%d rows", len(t.Rows))}
	for _, c := range t.Columns {
		res.Cols = append(res.Cols, c.Name)
	}
	for _, row := range t.Rows {
		cells := make([]string, len(row))
		for i, v := range row {
			cells[i] = cell(v)
		}
		res.Rows = append(res.Rows, cells)
	}
	return res, nil
}

// ---- Cloud Logging ----

func (ui *UI) gcpLogRun(q url.Values) (*result, error) {
	project := strings.TrimSpace(q.Get("project"))
	if project == "" {
		project = core.DefaultProject
	}
	n, _ := strconv.Atoi(q.Get("limit"))
	if n <= 0 {
		n = 100
	}
	var out struct {
		Entries []map[string]any
	}
	err := ui.gwJSON("POST", "", "/v2/entries:list", nil, map[string]any{"resourceNames": []string{"projects/" + project},
		"filter": q.Get("filter"), "orderBy": "timestamp desc", "pageSize": n}, &out)
	if err != nil {
		return nil, err
	}
	res := &result{Cols: []string{"timestamp", "severity", "log", "resource", "message"}, Note: fmt.Sprintf("%d entries", len(out.Entries))}
	for _, e := range out.Entries {
		msg := dig(e, "textPayload")
		for _, k := range []string{"jsonPayload", "protoPayload"} {
			if p, ok := e[k]; ok && msg == "" {
				msg = cell(p)
			}
		}
		log := dig(e, "logName")
		res.Rows = append(res.Rows, []string{dig(e, "timestamp"), dig(e, "severity"), log[strings.LastIndex(log, "/")+1:], dig(e, "resource", "type"), msg})
	}
	return res, nil
}
