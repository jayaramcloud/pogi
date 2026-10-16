package monitor_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/cloud"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	ingest "github.com/Azure/azure-sdk-for-go/sdk/monitor/ingestion/azlogs"
	query "github.com/Azure/azure-sdk-for-go/sdk/monitor/query/azlogs"
	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/azure/monitor"
	"github.com/nrndev/pogi/internal/core"
)

var ctx = context.Background()

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

type token struct{}

func (token) GetToken(context.Context, policy.TokenRequestOptions) (azcore.AccessToken, error) {
	return azcore.AccessToken{Token: "pogi", ExpiresOn: time.Now().Add(time.Hour)}, nil
}

const rg = "/subscriptions/" + core.AzureSubscription + "/resourceGroups/" + core.AzureResourceGroup

func put(t *testing.T, h *core.Hub, typ, id string, body map[string]any) map[string]any {
	t.Helper()
	p := arm.Lookup(h, typ)
	if p == nil {
		t.Fatalf("no provider for %s", typ)
	}
	rid, ok := arm.ParseID(id)
	if !ok {
		t.Fatal(id)
	}
	res, err := p.Put(&core.Ctx{}, rid, body)
	if err != nil {
		t.Fatalf("put %s: %v", id, err)
	}
	return res
}

func pid(s string) arm.ID {
	id, ok := arm.ParseID(s)
	if !ok {
		panic(s)
	}
	return id
}

func props(res map[string]any) map[string]any { return res["properties"].(map[string]any) }

func wantErr(t *testing.T, err error, status int, code string) {
	t.Helper()
	var re *azcore.ResponseError
	if !errors.As(err, &re) || re.StatusCode != status || (code != "" && re.ErrorCode != code) {
		t.Fatalf("got %v, want HTTP %d %s", err, status, code)
	}
}

type fixture struct {
	s         *pogi.Running
	svc       *monitor.Service
	workspace string // customerId
	dcr       string // immutable id of the "Custom-App" rule
	dce       string // logs ingestion endpoint
}

func setup(t *testing.T) *fixture {
	s := pogi.Start(t, pogi.Config{Services: []string{"azmonitor"}})
	h := s.Hub
	f := &fixture{s: s, svc: h.Service("azmonitor").(*monitor.Service)}

	wsID := rg + "/providers/Microsoft.OperationalInsights/workspaces/logs"
	ws := put(t, h, "Microsoft.OperationalInsights/workspaces", wsID, map[string]any{"location": "westeurope", "tags": map[string]any{"env": "test"}})
	f.workspace = props(ws)["customerId"].(string)

	put(t, h, "Microsoft.OperationalInsights/workspaces/tables", wsID+"/tables/Custom_CL", map[string]any{"properties": map[string]any{
		"schema": map[string]any{"name": "Custom_CL", "columns": []any{
			map[string]any{"name": "TimeGenerated", "type": "datetime"}, map[string]any{"name": "Level", "type": "string"},
			map[string]any{"name": "DurationMs", "type": "real"}, map[string]any{"name": "Count", "type": "long"},
			map[string]any{"name": "Ok", "type": "boolean"}, map[string]any{"name": "Extra", "type": "dynamic"}}}}})

	dce := put(t, h, "Microsoft.Insights/dataCollectionEndpoints", rg+"/providers/Microsoft.Insights/dataCollectionEndpoints/dce", map[string]any{})
	f.dce = props(dce)["logsIngestion"].(map[string]any)["endpoint"].(string)

	dcr := put(t, h, "Microsoft.Insights/dataCollectionRules", rg+"/providers/Microsoft.Insights/dataCollectionRules/dcr", map[string]any{"properties": map[string]any{
		"dataCollectionEndpointId": rg + "/providers/Microsoft.Insights/dataCollectionEndpoints/dce",
		"streamDeclarations": map[string]any{"Custom-App": map[string]any{"columns": []any{
			map[string]any{"name": "Time", "type": "string"}, map[string]any{"name": "Level", "type": "string"},
			map[string]any{"name": "DurationMs", "type": "real"}, map[string]any{"name": "Count", "type": "long"},
			map[string]any{"name": "Ok", "type": "boolean"}, map[string]any{"name": "Extra", "type": "dynamic"}}}},
		"destinations": map[string]any{"logAnalytics": []any{map[string]any{"name": "la", "workspaceResourceId": wsID}}},
		"dataFlows": []any{
			map[string]any{"streams": []any{"Custom-App"}, "destinations": []any{"la"}, "outputStream": "Custom-Custom_CL",
				"transformKql": "source | extend TimeGenerated = todatetime(Time) | where Level != 'noise' | project-away Time"},
			map[string]any{"streams": []any{"Custom-Raw"}, "destinations": []any{"la"}, "outputStream": "Custom-Raw_CL"}, // no transform: source
		}}})
	p := props(dcr)
	f.dcr = p["immutableId"].(string)
	if !strings.HasPrefix(f.dcr, "dcr-") || p["destinations"].(map[string]any)["logAnalytics"].([]any)[0].(map[string]any)["workspaceId"] != f.workspace {
		t.Fatalf("dcr: %v", p)
	}
	return f
}

// transport trusts pogi's CA and reaches pogi for any host name, so *.localhost needs no DNS.
// Bearer tokens only go over TLS, which pogi serves on its one port.
func (f *fixture) transport(t *testing.T) azcore.ClientOptions {
	ca, _, _ := f.s.Hub.CertPaths()
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(must(os.ReadFile(ca)))
	addr := "127.0.0.1:" + f.port()
	tr := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, ServerName: "localhost"},
		DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, addr)
		}}
	t.Cleanup(tr.CloseIdleConnections)
	return azcore.ClientOptions{Transport: &http.Client{Transport: tr}}
}

func (f *fixture) port() string { return f.s.URL[strings.LastIndexByte(f.s.URL, ':')+1:] }

func (f *fixture) ingestClient(t *testing.T, hostForm bool) *ingest.Client {
	endpoint := "https://localhost:" + f.port() + "/_azure/monitor/ingest"
	if hostForm {
		endpoint = f.dce
	}
	return must(ingest.NewClient(endpoint, token{}, &ingest.ClientOptions{ClientOptions: f.transport(t)}))
}

func (f *fixture) queryClient(t *testing.T, hostForm bool) *query.Client {
	endpoint := "https://localhost:" + f.port() + "/_azure/monitor/query"
	opts := &query.ClientOptions{ClientOptions: f.transport(t)}
	if hostForm {
		endpoint = "https://api.loganalytics.localhost:" + f.port()
	}
	opts.Cloud = cloud.Configuration{Services: map[cloud.ServiceName]cloud.ServiceConfiguration{
		query.ServiceName: {Endpoint: endpoint, Audience: "https://api.loganalytics.io"}}}
	return must(query.NewClient(token{}, opts))
}

func (f *fixture) run(t *testing.T, c *query.Client, q string) query.Table {
	t.Helper()
	res, err := c.QueryWorkspace(ctx, f.workspace, query.QueryBody{Query: &q, Timespan: to.Ptr(query.NewTimeInterval(time.Now().Add(-3*time.Hour), time.Now().Add(time.Hour)))}, nil)
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	if len(res.Tables) != 1 || *res.Tables[0].Name != "PrimaryResult" {
		t.Fatalf("%s: %+v", q, res.Tables)
	}
	return res.Tables[0]
}

func colTypes(tb query.Table) string {
	var out []string
	for _, c := range tb.Columns {
		out = append(out, *c.Name+":"+string(*c.Type))
	}
	return strings.Join(out, ",")
}

func TestIngestAndQuery(t *testing.T) {
	f := setup(t)
	now := time.Now().UTC()
	at := func(d time.Duration) string { return now.Add(d).Format(time.RFC3339Nano) }
	rows := []map[string]any{
		{"Time": at(-70 * time.Minute), "Level": "error", "DurationMs": 100.5, "Count": 3, "Ok": false, "Extra": map[string]any{"host": "a"}},
		{"Time": at(-65 * time.Minute), "Level": "error", "DurationMs": 300.5, "Count": 4, "Ok": false},
		{"Time": at(-20 * time.Minute), "Level": "info", "DurationMs": 5, "Count": 1, "Ok": true},
		{"Time": at(-10 * time.Minute), "Level": "error", "DurationMs": 40, "Count": 2, "Ok": false, "Extra": []any{1, 2}},
		{"Time": at(-5 * time.Minute), "Level": "noise", "DurationMs": 1, "Count": 0, "Ok": true}, // dropped by the transform
	}
	raw := must(json.Marshal(rows))

	// the official ingestion client, by path and by the DCE host; plain and gzip
	if _, err := f.ingestClient(t, false).Upload(ctx, f.dcr, "Custom-App", raw[:0:0], nil); err == nil {
		t.Fatal("an empty body is not a JSON array")
	}
	var zipped bytes.Buffer
	zw := gzip.NewWriter(&zipped)
	zw.Write(raw)
	zw.Close()
	must(f.ingestClient(t, true).Upload(ctx, f.dcr, "Custom-App", zipped.Bytes(), &ingest.UploadOptions{ContentEncoding: to.Ptr("gzip")}))

	qc := f.queryClient(t, false)
	tb := f.run(t, qc, `Custom_CL | order by TimeGenerated asc | project Level, DurationMs, Count, Ok, Extra`)
	if got := colTypes(tb); got != "Level:string,DurationMs:real,Count:long,Ok:bool,Extra:dynamic" {
		t.Fatalf("columns: %s", got)
	}
	if len(tb.Rows) != 4 {
		t.Fatalf("rows: %v", tb.Rows)
	}
	first := tb.Rows[0]
	if first[0] != "error" || first[1] != 100.5 || first[2] != 3.0 || first[3] != false || first[4].(map[string]any)["host"] != "a" {
		t.Fatalf("first row: %#v", first)
	}

	// where | summarize count() by bin(...): what the brief names
	tb = f.run(t, qc, `Custom_CL | where Level == "error" | summarize n = count(), slow = max(DurationMs), total = sum(Count) by bin(TimeGenerated, 1h) | order by TimeGenerated asc`)
	if got := colTypes(tb); got != "TimeGenerated:datetime,n:long,slow:real,total:long" {
		t.Fatalf("columns: %s", got)
	}
	var n float64
	for _, r := range tb.Rows {
		if _, err := time.Parse(time.RFC3339Nano, r[0].(string)); err != nil {
			t.Fatalf("datetime cell %v: %v", r[0], err)
		}
		n += r[1].(float64)
	}
	if n != 3 {
		t.Fatalf("errors counted %v in %v", n, tb.Rows)
	}

	// the same through api.loganalytics.localhost
	tb = f.run(t, f.queryClient(t, true), `Custom_CL | summarize count() by Level | order by Level asc`)
	if len(tb.Rows) != 2 || tb.Rows[0][0] != "error" || tb.Rows[0][1] != 3.0 || tb.Rows[1][0] != "info" {
		t.Fatalf("by host: %v", tb.Rows)
	}

	// a timespan filters on TimeGenerated: only the last 30 minutes
	q := `Custom_CL | count`
	res := must(qc.QueryWorkspace(ctx, f.workspace, query.QueryBody{Query: &q, Timespan: to.Ptr(query.TimeInterval("PT30M"))}, nil))
	if res.Tables[0].Rows[0][0] != 2.0 {
		t.Fatalf("PT30M: %v", res.Tables[0].Rows)
	}

	// a stream with no declaration and no transform: columns come from the rows
	must(f.ingestClient(t, false).Upload(ctx, f.dcr, "Custom-Raw", must(json.Marshal([]map[string]any{{"Msg": "hello", "N": 7, "R": 1.5}, {"Msg": "bye", "N": 8, "R": 2.5}})), nil))
	tb = f.run(t, qc, `Raw_CL | where N > 7 | project Msg, N, R, TimeGenerated`)
	if got := colTypes(tb); got != "Msg:string,N:long,R:real,TimeGenerated:datetime" || len(tb.Rows) != 1 || tb.Rows[0][0] != "bye" || tb.Rows[0][3] == "" {
		t.Fatalf("raw: %s %v", got, tb.Rows)
	}

	// errors: SDKs branch on these
	_, err := f.ingestClient(t, false).Upload(ctx, "dcr-00000000000000000000000000000000", "Custom-App", raw, nil)
	wantErr(t, err, http.StatusNotFound, "NotFound")
	_, err = f.ingestClient(t, false).Upload(ctx, f.dcr, "Custom-Nope", raw, nil)
	wantErr(t, err, http.StatusBadRequest, "InvalidStream")
	for q, inner := range map[string]string{`Custom_CL | where Nope == 1`: "SemanticError", `Custom_CL | where (`: "SyntaxError", `NoTable | count`: "SemanticError"} {
		_, err = qc.QueryWorkspace(ctx, f.workspace, query.QueryBody{Query: &q}, nil)
		wantErr(t, err, http.StatusBadRequest, "BadArgumentError")
		if !strings.Contains(err.Error(), inner) {
			t.Errorf("%s: %v", q, err)
		}
	}
	q = `Custom_CL | count`
	_, err = qc.QueryWorkspace(ctx, "00000000-0000-0000-0000-00000000dead", query.QueryBody{Query: &q}, nil)
	wantErr(t, err, http.StatusNotFound, "PathNotFoundError")

	// batch
	b := must(qc.QueryBatch(ctx, query.BatchRequest{Requests: []query.BatchQueryRequest{
		{ID: to.Ptr("1"), WorkspaceID: &f.workspace, Body: &query.QueryBody{Query: to.Ptr("Raw_CL | count")}},
		{ID: to.Ptr("2"), WorkspaceID: &f.workspace, Body: &query.QueryBody{Query: to.Ptr("Raw_CL | where")}}}}, nil))
	if *b.Responses[0].Status != 200 || b.Responses[0].Body.Tables[0].Rows[0][0] != 2.0 || *b.Responses[1].Status != 400 {
		t.Fatalf("batch: %+v", b.Responses)
	}

	// persistence: saved rows come back typed
	blob := must(f.svc.Save())
	f.svc.Reset()
	if _, err := qc.QueryWorkspace(ctx, f.workspace, query.QueryBody{Query: &q}, nil); err == nil {
		t.Fatal("reset keeps the workspace")
	}
	if err := f.svc.Load(blob); err != nil {
		t.Fatal(err)
	}
	tb = f.run(t, qc, `Custom_CL | where Level == "error" | summarize sum(DurationMs), max(TimeGenerated), dcount(Level)`)
	if tb.Rows[0][0] != 441.0 || colTypes(tb) != "sum_DurationMs:real,max_TimeGenerated:datetime,dcount_Level:long" {
		t.Fatalf("after load: %s %v", colTypes(tb), tb.Rows)
	}
	if rs := f.svc.Resources(); len(rs) != 3 || len(rs[0].Rows) != 1 || f.svc.Inspect("Workspaces", "logs") == nil {
		t.Fatalf("console: %+v", rs)
	}
}

func TestProviders(t *testing.T) {
	f := setup(t)
	h := f.s.Hub
	wsType := "Microsoft.OperationalInsights/workspaces"
	wsID := rg + "/providers/" + wsType + "/logs"
	ws := arm.Lookup(h, wsType)
	id := pid(wsID)

	got := must(ws.Get(&core.Ctx{}, id))
	if got["location"] != "westeurope" || got["type"] != wsType || got["id"] != wsID || got["tags"].(map[string]any)["env"] != "test" ||
		props(got)["customerId"] != f.workspace || props(got)["provisioningState"] != "Succeeded" || props(got)["sku"].(map[string]any)["name"] != "PerGB2018" {
		t.Fatalf("workspace: %v", got)
	}
	// a second PUT keeps the customer ID
	again := put(t, h, wsType, wsID, map[string]any{"location": "westeurope", "properties": map[string]any{"retentionInDays": 90}})
	if props(again)["customerId"] != f.workspace || props(again)["retentionInDays"] != 90.0 {
		t.Fatalf("update: %v", again)
	}
	for _, a := range []string{"listKeys", "sharedKeys"} {
		keys := must(ws.Actions[a](&core.Ctx{}, id, nil)).(map[string]any)
		if keys["primarySharedKey"] == "" || keys["primarySharedKey"] == keys["secondarySharedKey"] {
			t.Fatalf("%s: %v", a, keys)
		}
	}
	list := must(ws.List(&core.Ctx{}, pid(rg)))
	if len(list) != 1 || list[0]["name"] != "logs" {
		t.Fatalf("list: %v", list)
	}
	tables := must(arm.Lookup(h, wsType+"/tables").List(&core.Ctx{}, id))
	if len(tables) != 1 || tables[0]["name"] != "Custom_CL" || props(tables[0])["schema"].(map[string]any)["columns"] == nil {
		t.Fatalf("tables: %v", tables)
	}

	// DCE and DCR: endpoints, immutable id kept across updates, KQL validated
	dce := must(arm.Lookup(h, "Microsoft.Insights/dataCollectionEndpoints").Get(&core.Ctx{}, pid(rg+"/providers/Microsoft.Insights/dataCollectionEndpoints/dce")))
	if ep := props(dce)["logsIngestion"].(map[string]any)["endpoint"]; ep != f.dce || !strings.Contains(f.dce, "dce.ingest.monitor.localhost:") {
		t.Fatalf("dce endpoint: %v", ep)
	}
	dcrID := rg + "/providers/Microsoft.Insights/dataCollectionRules/dcr"
	dcr := arm.Lookup(h, "Microsoft.Insights/dataCollectionRules")
	cur := must(dcr.Get(&core.Ctx{}, pid(dcrID)))
	cur["properties"].(map[string]any)["dataFlows"].([]any)[0].(map[string]any)["transformKql"] = "source | where ((("
	if _, err := dcr.Put(&core.Ctx{}, pid(dcrID), cur); core.AsError(err).Status != http.StatusBadRequest {
		t.Fatalf("a bad transform is accepted: %v", err)
	}
	cur["properties"].(map[string]any)["dataFlows"].([]any)[0].(map[string]any)["transformKql"] = "source"
	if upd := must(dcr.Put(&core.Ctx{}, pid(dcrID), cur)); props(upd)["immutableId"] != f.dcr {
		t.Fatalf("immutable id changed: %v", props(upd)["immutableId"])
	}

	// Application Insights, action groups and alerts are stored
	ai := put(t, h, "Microsoft.Insights/components", rg+"/providers/Microsoft.Insights/components/ai", map[string]any{"kind": "web", "properties": map[string]any{"Application_Type": "web"}})
	cs := props(ai)["ConnectionString"].(string)
	if !strings.Contains(cs, "InstrumentationKey="+props(ai)["InstrumentationKey"].(string)) || !strings.Contains(cs, "IngestionEndpoint=") || props(ai)["AppId"] == "" || ai["kind"] != "web" {
		t.Fatalf("component: %v", ai)
	}
	ag := put(t, h, "Microsoft.Insights/actionGroups", rg+"/providers/Microsoft.Insights/actionGroups/ag", map[string]any{"properties": map[string]any{"groupShortName": "ag", "enabled": true,
		"emailReceivers": []any{map[string]any{"name": "me", "emailAddress": "me@example.com"}}}})
	if ag["location"] != "global" || props(ag)["groupShortName"] != "ag" {
		t.Fatalf("action group: %v", ag)
	}
	put(t, h, "Microsoft.Insights/metricAlerts", rg+"/providers/Microsoft.Insights/metricAlerts/high-cpu", map[string]any{"properties": map[string]any{"severity": 2, "enabled": true}})
	if l := must(arm.Lookup(h, "Microsoft.Insights/metricAlerts").List(&core.Ctx{}, pid(rg))); len(l) != 1 {
		t.Fatalf("alerts: %v", l)
	}

	// deleting a workspace drops its tables and rows; a missing one is a 404 on Get only
	must(f.ingestClient(t, false).Upload(ctx, f.dcr, "Custom-Raw", []byte(`[{"A":1}]`), nil))
	if err := ws.Delete(&core.Ctx{}, id); err != nil {
		t.Fatal(err)
	}
	if err := ws.Delete(&core.Ctx{}, id); err != nil {
		t.Fatalf("second delete: %v", err)
	}
	if _, err := ws.Get(&core.Ctx{}, id); core.AsError(err).Status != http.StatusNotFound || core.AsError(err).Code != "ResourceNotFound" {
		t.Fatalf("get after delete: %v", err)
	}
	if l := must(arm.Lookup(h, wsType+"/tables").List(&core.Ctx{}, id)); len(l) != 0 {
		t.Fatalf("tables survive: %v", l)
	}
	q := "Custom_CL | count"
	_, err := f.queryClient(t, false).QueryWorkspace(ctx, f.workspace, query.QueryBody{Query: &q}, nil)
	wantErr(t, err, http.StatusNotFound, "")
}

func TestAppInsightsAndMetrics(t *testing.T) {
	f := setup(t)
	h := f.s.Hub
	wsID := rg + "/providers/Microsoft.OperationalInsights/workspaces/logs"
	linked := put(t, h, "Microsoft.Insights/components", rg+"/providers/Microsoft.Insights/components/linked", map[string]any{"properties": map[string]any{"WorkspaceResourceId": wsID}})
	alone := put(t, h, "Microsoft.Insights/components", rg+"/providers/Microsoft.Insights/components/alone", map[string]any{})

	track := func(path, ikey string, gz bool) *http.Response {
		env := func(typ, name string, data map[string]any) map[string]any {
			return map[string]any{"name": "Microsoft.ApplicationInsights." + name, "time": time.Now().UTC().Format(time.RFC3339Nano), "iKey": ikey,
				"tags": map[string]any{"ai.operation.id": "op1", "ai.cloud.role": "api"}, "data": map[string]any{"baseType": typ, "baseData": data}}
		}
		items := []map[string]any{
			env("RequestData", "Request", map[string]any{"id": "r1", "name": "GET /x", "url": "http://x/x", "duration": "00:00:01.5000000", "responseCode": "200", "success": true, "properties": map[string]any{"k": "v"}}),
			env("RequestData", "Request", map[string]any{"id": "r2", "name": "GET /y", "duration": "00:00:00.2500000", "responseCode": "500", "success": false}),
			env("MessageData", "Message", map[string]any{"message": "hello", "severityLevel": 1}),
			env("EventData", "Event", map[string]any{"name": "dropped"}),
		}
		var b bytes.Buffer
		var w io.Writer = &b
		var zw *gzip.Writer
		if gz {
			zw = gzip.NewWriter(&b)
			w = zw
		}
		for _, it := range items { // newline-delimited, as the SDKs send it
			json.NewEncoder(w).Encode(it)
		}
		if zw != nil {
			zw.Close()
		}
		req, _ := http.NewRequest(http.MethodPost, f.s.URL+path, &b)
		req.Header.Set("Content-Type", "application/x-json-stream")
		if gz {
			req.Header.Set("Content-Encoding", "gzip")
		}
		return must(http.DefaultClient.Do(req))
	}
	resp := track("/_azure/monitor/appinsights/v2/track", props(linked)["InstrumentationKey"].(string), false)
	var ack struct {
		Received int `json:"itemsReceived"`
		Accepted int `json:"itemsAccepted"`
	}
	json.NewDecoder(resp.Body).Decode(&ack)
	resp.Body.Close()
	if resp.StatusCode != 200 || ack.Received != 4 || ack.Accepted != 4 {
		t.Fatalf("track: %d %+v", resp.StatusCode, ack)
	}
	if r := track("/v2/track", props(alone)["InstrumentationKey"].(string), true); r.StatusCode != 200 {
		t.Fatalf("track gzip on /v2/track: %d", r.StatusCode)
	}
	if r := track("/v2.1/track", "no-such-key", false); r.StatusCode != http.StatusPartialContent {
		t.Fatalf("unknown ikey: %d", r.StatusCode)
	}

	qc := f.queryClient(t, false)
	tb := f.run(t, qc, `requests | summarize n = count(), slow = max(duration), failed = countif(success == false) by cloud_RoleName`)
	if got := colTypes(tb); got != "cloud_RoleName:string,n:long,slow:real,failed:long" || tb.Rows[0][0] != "api" || tb.Rows[0][1] != 2.0 || tb.Rows[0][2] != 1500.0 || tb.Rows[0][3] != 1.0 {
		t.Fatalf("requests: %s %v", got, tb.Rows)
	}
	tb = f.run(t, qc, `traces | where message == "hello" | project message, severityLevel, operation_Id, tostring(customDimensions.none)`)
	if len(tb.Rows) != 1 || tb.Rows[0][2] != "op1" {
		t.Fatalf("traces: %v", tb.Rows)
	}
	// an unlinked component is queried as an app
	appQuery := `{"query": "requests | summarize count()"}`
	resp = must(http.Post(f.s.URL+"/_azure/monitor/query/v1/apps/"+props(alone)["AppId"].(string)+"/query", "application/json", strings.NewReader(appQuery)))
	var out struct {
		Tables []struct{ Rows [][]any }
	}
	json.NewDecoder(resp.Body).Decode(&out)
	resp.Body.Close()
	if resp.StatusCode != 200 || len(out.Tables) != 1 || out.Tables[0].Rows[0][0] != 2.0 {
		t.Fatalf("app query: %d %+v", resp.StatusCode, out)
	}

	// metrics: the right shape, zero series
	get := func(path string) map[string]any {
		resp := must(http.Get(f.s.URL + path))
		defer resp.Body.Close()
		var m map[string]any
		json.NewDecoder(resp.Body).Decode(&m)
		if resp.StatusCode != 200 {
			t.Fatalf("%s: %d %v", path, resp.StatusCode, m)
		}
		return m
	}
	res := rg + "/providers/Microsoft.Storage/storageAccounts/acct"
	m := get(res + "/providers/Microsoft.Insights/metrics?api-version=2018-01-01&metricnames=Transactions,UsedCapacity&timespan=PT10M&interval=PT1M&aggregation=Total,Average")
	vals := m["value"].([]any)
	series := vals[0].(map[string]any)["timeseries"].([]any)[0].(map[string]any)["data"].([]any)
	pt := series[0].(map[string]any)
	if len(vals) != 2 || len(series) != 10 || m["namespace"] != "Microsoft.Storage/storageAccounts" || m["interval"] != "PT1M" ||
		vals[1].(map[string]any)["name"].(map[string]any)["value"] != "UsedCapacity" || pt["total"] != 0.0 || pt["average"] != 0.0 || pt["timeStamp"] == nil {
		t.Fatalf("metrics: %v", m)
	}
	if defs := get(res + "/providers/Microsoft.Insights/metricDefinitions?api-version=2018-01-01"); defs["value"] == nil {
		t.Fatalf("definitions: %v", defs)
	}
}
