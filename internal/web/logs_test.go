package web_test

import (
	"bytes"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/core"
)

func TestCloudWatchLogsQuery(t *testing.T) {
	srv := pogi.Start(t, pogi.Config{Services: []string{"logs"}})
	call(t, srv, "Logs_20140328", "CreateLogGroup", map[string]any{"logGroupName": "/app/api"})
	call(t, srv, "Logs_20140328", "CreateLogStream", map[string]any{"logGroupName": "/app/api", "logStreamName": "s1"})
	now := time.Now().UnixMilli()
	call(t, srv, "Logs_20140328", "PutLogEvents", map[string]any{"logGroupName": "/app/api", "logStreamName": "s1", "logEvents": []any{
		map[string]any{"timestamp": now - 3000, "message": "GET /health 200"},
		map[string]any{"timestamp": now - 2000, "message": "ERROR payment failed: card declined"},
		map[string]any{"timestamp": now - 1000, "message": "GET /orders 200"},
	}})
	target := core.DefaultAccount + "/" + core.DefaultRegion + "//app/api"
	has(t, page(t, srv, "/_pogi/logs"), `value="`+target+`"`)
	run := func(mode, q string) string {
		return page(t, srv, "/_pogi/logs?"+url.Values{"kind": {"cwl"}, "run": {"1"}, "target": {target}, "mode": {mode}, "query": {q}}.Encode())
	}

	p := run("insights", "fields @timestamp, @message | filter @message like /ERROR/ | sort @timestamp desc | limit 10")
	shows(t, p, "Complete", "1 rows", "card declined")
	hides(t, p, "/health")
	p = run("insights", "")
	shows(t, p, "3 rows", "/health", "/orders") // the default query
	shows(t, run("insights", "this is not a query"), `class="flash"`, "MalformedQueryException")

	p = run("filter", "ERROR")
	shows(t, p, "1 events", "card declined")
	hides(t, p, "/orders")
}

func TestCloudLoggingFilter(t *testing.T) {
	srv := pogi.Start(t, pogi.Config{Services: []string{"logging"}})
	entries := `{"entries":[
	  {"logName":"projects/` + core.DefaultProject + `/logs/app","resource":{"type":"global"},"severity":"INFO","textPayload":"started"},
	  {"logName":"projects/` + core.DefaultProject + `/logs/app","resource":{"type":"global"},"severity":"ERROR","textPayload":"disk full"},
	  {"logName":"projects/` + core.DefaultProject + `/logs/app","resource":{"type":"global"},"severity":"ERROR","jsonPayload":{"event":"oom","pid":7}}]}`
	res, err := http.Post(srv.URL+"/v2/entries:write", "application/json", strings.NewReader(entries))
	if err != nil || res.StatusCode != 200 {
		t.Fatalf("write entries: %v %v", res, err)
	}
	run := func(filter string) string {
		return page(t, srv, "/_pogi/logs?"+url.Values{"kind": {"gcp"}, "run": {"1"}, "filter": {filter}}.Encode())
	}
	p := run("severity>=ERROR")
	shows(t, p, "2 entries", "disk full", "oom")
	hides(t, p, "started")
	p = run(`textPayload:"started"`)
	shows(t, p, "1 entries", "started")
	hides(t, p, "disk full")
}

func TestKQLQuery(t *testing.T) {
	srv := pogi.Start(t, pogi.Config{Services: []string{"azmonitor"}})
	h := srv.Hub
	put := func(typ, id string, body map[string]any) map[string]any {
		t.Helper()
		rid, _ := arm.ParseID(id)
		res, err := arm.Lookup(h, typ).Put(&core.Ctx{}, rid, body)
		if err != nil {
			t.Fatalf("put %s: %v", id, err)
		}
		return res
	}
	rg := "/subscriptions/" + core.AzureSubscription + "/resourceGroups/" + core.AzureResourceGroup
	ws := rg + "/providers/Microsoft.OperationalInsights/workspaces/logs"
	wsRes := put("Microsoft.OperationalInsights/workspaces", ws, map[string]any{"location": "eastus"})
	customer := wsRes["properties"].(map[string]any)["customerId"].(string)
	col := func(n, ty string) any { return map[string]any{"name": n, "type": ty} }
	put("Microsoft.OperationalInsights/workspaces/tables", ws+"/tables/App_CL", map[string]any{"properties": map[string]any{
		"schema": map[string]any{"name": "App_CL", "columns": []any{col("TimeGenerated", "datetime"), col("Level", "string"), col("Count", "long")}}}})
	put("Microsoft.Insights/dataCollectionEndpoints", rg+"/providers/Microsoft.Insights/dataCollectionEndpoints/dce", map[string]any{})
	dcr := put("Microsoft.Insights/dataCollectionRules", rg+"/providers/Microsoft.Insights/dataCollectionRules/dcr", map[string]any{"properties": map[string]any{
		"dataCollectionEndpointId": rg + "/providers/Microsoft.Insights/dataCollectionEndpoints/dce",
		"streamDeclarations":       map[string]any{"Custom-App": map[string]any{"columns": []any{col("Time", "string"), col("Level", "string"), col("Count", "long")}}},
		"destinations":             map[string]any{"logAnalytics": []any{map[string]any{"name": "la", "workspaceResourceId": ws}}},
		"dataFlows": []any{map[string]any{"streams": []any{"Custom-App"}, "destinations": []any{"la"}, "outputStream": "Custom-App_CL",
			"transformKql": "source | extend TimeGenerated = todatetime(Time) | project-away Time"}}}})
	rule := dcr["properties"].(map[string]any)["immutableId"].(string)
	rows := `[{"Time":"` + time.Now().UTC().Format(time.RFC3339) + `","Level":"error","Count":3},{"Time":"` + time.Now().UTC().Format(time.RFC3339) + `","Level":"info","Count":9}]`
	res, err := http.Post(srv.URL+"/_azure/monitor/ingest/dataCollectionRules/"+rule+"/streams/Custom-App", "application/json", bytes.NewReader([]byte(rows)))
	if err != nil || res.StatusCode >= 300 {
		t.Fatalf("ingest: %v %v", res, err)
	}

	has(t, page(t, srv, "/_pogi/logs?kind=kql"), `value="`+customer+`"`)
	run := func(q string) string {
		return page(t, srv, "/_pogi/logs?"+url.Values{"kind": {"kql"}, "run": {"1"}, "target": {customer}, "query": {q}}.Encode())
	}
	p := run("App_CL | where Level == 'error' | project Level, Count")
	shows(t, p, "1 rows", ">error<", ">3<")
	hides(t, p, ">info<")
	shows(t, run("App_CL | summarize total = sum(Count)"), ">12<")
	shows(t, run("App_CL | nonsense"), `class="flash"`)
}
