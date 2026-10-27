package monitor_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/azure/armtest"
	"github.com/nrndev/pogi/internal/core"
)

// TestResourceGraph runs the queries az graph query sends: POST /providers/Microsoft.ResourceGraph/resources.
func TestResourceGraph(t *testing.T) {
	e := armtest.Start(t, pogi.Config{}, "azmonitor")
	hc := e.Opts.Transport.(*http.Client)
	call := func(method, path, body string) (int, map[string]any) {
		t.Helper()
		req, _ := http.NewRequest(method, e.Base+path, strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer x")
		req.Header.Set("Content-Type", "application/json")
		res, err := hc.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		b, _ := io.ReadAll(res.Body)
		var out map[string]any
		json.Unmarshal(b, &out)
		return res.StatusCode, out
	}
	sub := "/subscriptions/" + core.AzureSubscription
	for _, p := range []struct{ path, body string }{
		{sub + "/resourceGroups/Demo?api-version=2022-09-01", `{"location":"westeurope","tags":{"env":"test"}}`},
		{sub + "/resourceGroups/Demo/providers/Microsoft.OperationalInsights/workspaces/logs?api-version=2022-10-01", `{"location":"westeurope","tags":{"env":"test"}}`},
		{sub + "/resourceGroups/pogi/providers/Microsoft.OperationalInsights/workspaces/other?api-version=2022-10-01", `{"location":"eastus"}`},
		{sub + "/resourceGroups/pogi/providers/Microsoft.Insights/actionGroups/oncall?api-version=2023-01-01", `{"location":"global","properties":{"groupShortName":"oncall"}}`},
	} {
		if st, out := call("PUT", p.path, p.body); st >= 300 {
			t.Fatalf("PUT %s: %d %v", p.path, st, out)
		}
	}
	query := func(q string, opts string) (int, map[string]any) {
		t.Helper()
		b, _ := json.Marshal(q)
		if opts == "" {
			opts = `{"$top":100,"$skip":0,"resultFormat":"objectArray"}`
		}
		return call("POST", "/providers/Microsoft.ResourceGraph/resources?api-version=2021-03-01",
			`{"subscriptions":["`+core.AzureSubscription+`"],"query":`+string(b)+`,"options":`+opts+`}`)
	}
	rows := func(out map[string]any) string {
		var l []string
		for _, r := range out["data"].([]any) {
			b, _ := json.Marshal(r)
			l = append(l, string(b))
		}
		return strings.Join(l, "\n")
	}

	// a filter on the type as written, with the type and resource group lower-cased in the answer, and a tag
	st, out := query("Resources | where type =~ 'Microsoft.OperationalInsights/workspaces' and resourceGroup == 'demo' | extend env = tostring(tags.env) | project name, type, resourceGroup, location, env", "")
	if want := `{"env":"test","location":"westeurope","name":"logs","resourceGroup":"demo","type":"microsoft.operationalinsights/workspaces"}`; st != 200 || rows(out) != want || out["totalRecords"] != 1.0 || out["count"] != 1.0 {
		t.Fatalf("%d %v", st, out)
	}
	// a query that starts with an operator reads Resources; summarize and order
	st, out = query("summarize n = count() by type | order by type asc", "")
	if want := "{\"n\":1,\"type\":\"microsoft.insights/actiongroups\"}\n{\"n\":2,\"type\":\"microsoft.operationalinsights/workspaces\"}"; st != 200 || rows(out) != want {
		t.Fatalf("%d %v", st, out)
	}
	// the containers: the subscription and its groups
	st, out = query("ResourceContainers | order by type asc, name asc | project name, type", "")
	if want := "{\"name\":\"pogi\",\"type\":\"microsoft.resources/subscriptions\"}\n" +
		"{\"name\":\"Demo\",\"type\":\"microsoft.resources/subscriptions/resourcegroups\"}\n" +
		"{\"name\":\"pogi\",\"type\":\"microsoft.resources/subscriptions/resourcegroups\"}"; st != 200 || rows(out) != want {
		t.Fatalf("%d %v", st, out)
	}
	// pages: $top rows and a $skipToken for the rest
	st, out = query("Resources | order by name asc | project name", `{"$top":2}`)
	if st != 200 || rows(out) != "{\"name\":\"logs\"}\n{\"name\":\"oncall\"}" || out["$skipToken"] != "2" || out["totalRecords"] != 3.0 || out["count"] != 2.0 {
		t.Fatalf("page 1: %d %v", st, out)
	}
	st, out = query("Resources | order by name asc | project name", `{"$top":2,"$skipToken":"2"}`)
	if st != 200 || rows(out) != `{"name":"other"}` || out["$skipToken"] != nil {
		t.Fatalf("page 2: %d %v", st, out)
	}
	// the table format
	st, out = query("Resources | where name == 'logs' | project name, tags", `{"resultFormat":"table"}`)
	if b, _ := json.Marshal(out["data"]); st != 200 || string(b) != `{"columns":[{"name":"name","type":"string"},{"name":"tags","type":"object"}],"rows":[["logs",{"env":"test"}]]}` {
		t.Fatalf("table: %d %s", st, b)
	}
	// another subscription has nothing
	st, out = call("POST", "/providers/Microsoft.ResourceGraph/resources?api-version=2021-03-01", `{"subscriptions":["11111111-1111-1111-1111-111111111111"],"query":"Resources"}`)
	if st != 200 || out["totalRecords"] != 0.0 {
		t.Fatalf("other subscription: %d %v", st, out)
	}
	// bad requests are refused with what was wrong
	for name, c := range map[string]struct{ q, opts string }{
		"syntax":        {"Resources | where", ""},
		"unknown table": {"AdvisorResources", ""},
		"top too big":   {"Resources", `{"$top":5000}`},
		"bad token":     {"Resources", `{"$skipToken":"x"}`},
		"bad format":    {"Resources", `{"resultFormat":"csv"}`},
		"no query":      {" ", ""},
	} {
		if st, out := query(c.q, c.opts); st != 400 || fmt.Sprint(out["error"].(map[string]any)["code"]) != "BadRequest" {
			t.Errorf("%s: %d %v", name, st, out)
		}
	}
}
