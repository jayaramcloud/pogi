package generic_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/amp"
	"github.com/aws/aws-sdk-go-v2/service/amp/types"
	ampdata "github.com/nrndev/pogi/internal/aws/amp"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestAMP(t *testing.T) {
	_, base := start(t, "amp")
	ctx := context.Background()
	c := amp.NewFromConfig(awstest.Config(t, base))

	w, err := c.CreateWorkspace(ctx, &amp.CreateWorkspaceInput{Alias: aws.String("prod"), Tags: map[string]string{"env": "dev"}})
	if err != nil {
		t.Fatal(err)
	}
	id := aws.ToString(w.WorkspaceId)
	if !strings.HasPrefix(id, "ws-") || w.Status.StatusCode != types.WorkspaceStatusCodeCreating {
		t.Fatalf("workspace: %q %s %q", id, aws.ToString(w.Arn), w.Status.StatusCode)
	}
	d, err := c.DescribeWorkspace(ctx, &amp.DescribeWorkspaceInput{WorkspaceId: w.WorkspaceId})
	if err != nil || aws.ToString(d.Workspace.Alias) != "prod" || d.Workspace.Status.StatusCode != types.WorkspaceStatusCodeActive || !strings.HasSuffix(aws.ToString(d.Workspace.PrometheusEndpoint), "/workspaces/"+id+"/") {
		t.Fatalf("describe: %v alias=%v status=%v ep=%v", err, aws.ToString(d.Workspace.Alias), d.Workspace.Status, aws.ToString(d.Workspace.PrometheusEndpoint))
	}
	if _, err := c.UpdateWorkspaceAlias(ctx, &amp.UpdateWorkspaceAliasInput{WorkspaceId: w.WorkspaceId, Alias: aws.String("production")}); err != nil {
		t.Fatal(err)
	}
	l, err := c.ListWorkspaces(ctx, &amp.ListWorkspacesInput{})
	if err != nil || len(l.Workspaces) != 1 || aws.ToString(l.Workspaces[0].Alias) != "production" {
		t.Fatalf("list: %v %+v", err, l)
	}
	if _, err := c.TagResource(ctx, &amp.TagResourceInput{ResourceArn: w.Arn, Tags: map[string]string{"a": "b"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.UntagResource(ctx, &amp.UntagResourceInput{ResourceArn: w.Arn, TagKeys: []string{"env"}}); err != nil {
		t.Fatal(err)
	}
	if tg, err := c.ListTagsForResource(ctx, &amp.ListTagsForResourceInput{ResourceArn: w.Arn}); err != nil || len(tg.Tags) != 1 || tg.Tags["a"] != "b" {
		t.Fatalf("tags: %v %+v", err, tg)
	}
	rules := "groups:\n- name: example\n  rules:\n  - alert: HighLatency\n    expr: up == 0\n"
	if _, err := c.CreateRuleGroupsNamespace(ctx, &amp.CreateRuleGroupsNamespaceInput{WorkspaceId: w.WorkspaceId, Name: aws.String("rules"), Data: []byte(rules)}); err != nil {
		t.Fatal(err)
	}
	rg, err := c.DescribeRuleGroupsNamespace(ctx, &amp.DescribeRuleGroupsNamespaceInput{WorkspaceId: w.WorkspaceId, Name: aws.String("rules")})
	if err != nil || string(rg.RuleGroupsNamespace.Data) != rules {
		t.Fatalf("rule groups: %v %+v", err, rg)
	}
	if _, err := c.PutRuleGroupsNamespace(ctx, &amp.PutRuleGroupsNamespaceInput{WorkspaceId: w.WorkspaceId, Name: aws.String("rules"), Data: []byte("groups: []")}); err != nil {
		t.Fatal(err)
	}
	if rg, _ = c.DescribeRuleGroupsNamespace(ctx, &amp.DescribeRuleGroupsNamespaceInput{WorkspaceId: w.WorkspaceId, Name: aws.String("rules")}); string(rg.RuleGroupsNamespace.Data) != "groups: []" {
		t.Fatalf("rule groups after put: %+v", rg)
	}
	if lr, err := c.ListRuleGroupsNamespaces(ctx, &amp.ListRuleGroupsNamespacesInput{WorkspaceId: w.WorkspaceId}); err != nil || len(lr.RuleGroupsNamespaces) != 1 {
		t.Fatalf("list rule groups: %v %+v", err, lr)
	}
	am := "alertmanager_config: |\n  route:\n    receiver: default\n"
	if _, err := c.CreateAlertManagerDefinition(ctx, &amp.CreateAlertManagerDefinitionInput{WorkspaceId: w.WorkspaceId, Data: []byte(am)}); err != nil {
		t.Fatal(err)
	}
	if ad, err := c.DescribeAlertManagerDefinition(ctx, &amp.DescribeAlertManagerDefinitionInput{WorkspaceId: w.WorkspaceId}); err != nil || string(ad.AlertManagerDefinition.Data) != am {
		t.Fatalf("alert manager: %v %+v", err, ad)
	}
	if _, err := c.CreateRuleGroupsNamespace(ctx, &amp.CreateRuleGroupsNamespaceInput{WorkspaceId: aws.String("ws-nope"), Name: aws.String("x"), Data: []byte("groups: []")}); err == nil {
		t.Fatal("rule groups in a workspace that does not exist")
	}

	// remote write: samples of a counter and of a gauge, then queries over them
	t0 := int64(1_700_000_000_000) // ms
	counter := &ampdata.Series{Labels: map[string]string{"__name__": "http_requests_total", "job": "api", "instance": "a"}}
	counter2 := &ampdata.Series{Labels: map[string]string{"__name__": "http_requests_total", "job": "api", "instance": "b"}}
	for i := range 6 { // a: 1 per second; b: 2 per second, with a reset after the third sample
		counter.Samples = append(counter.Samples, ampdata.Sample{T: t0 + int64(i)*10_000, V: float64(i * 10)})
		v := float64(i * 20)
		if i > 2 {
			v = float64((i - 3) * 20)
		}
		counter2.Samples = append(counter2.Samples, ampdata.Sample{T: t0 + int64(i)*10_000, V: v})
	}
	tx := &ampdata.Series{Labels: map[string]string{"__name__": "temp", "room": "x"}, Samples: []ampdata.Sample{{T: t0, V: 20}, {T: t0 + 50_000, V: 21}}}
	ty := &ampdata.Series{Labels: map[string]string{"__name__": "temp", "room": "y"}, Samples: []ampdata.Sample{{T: t0, V: 30}}}
	post := func(path string, body []byte, ws string) (int, string) {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, base+"/workspaces/"+ws+"/api/v1/"+path, bytes.NewReader(body))
		req.Header.Set("Content-Encoding", "snappy")
		req.Header.Set("Content-Type", "application/x-protobuf")
		awstest.Sign(t, req, "aps")
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		b, _ := io.ReadAll(res.Body)
		return res.StatusCode, string(b)
	}
	if st, body := post("remote_write", ampdata.EncodeWrite(counter, counter2, tx, ty), id); st != 200 {
		t.Fatalf("remote write: %d %s", st, body)
	}
	if st, _ := post("remote_write", ampdata.EncodeWrite(tx), "ws-nope"); st != 404 {
		t.Fatalf("remote write to a missing workspace: %d", st)
	}
	if st, _ := post("remote_write", []byte("not snappy"), id); st != 400 {
		t.Fatalf("garbled remote write: %d", st)
	}
	type result struct {
		Metric map[string]string `json:"metric"`
		Value  []any             `json:"value"`
		Values [][]any           `json:"values"`
	}
	query := func(path string, q url.Values) (string, []result) {
		t.Helper()
		req, _ := http.NewRequest(http.MethodGet, base+"/workspaces/"+id+"/api/v1/"+path+"?"+q.Encode(), nil)
		awstest.Sign(t, req, "aps")
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		var out struct {
			Status    string `json:"status"`
			Error     string `json:"error"`
			ErrorType string `json:"errorType"`
			Data      struct {
				ResultType string   `json:"resultType"`
				Result     []result `json:"result"`
			} `json:"data"`
		}
		json.NewDecoder(res.Body).Decode(&out)
		if out.Status != "success" {
			return out.ErrorType + ": " + out.Error, nil
		}
		return out.Data.ResultType, out.Data.Result
	}
	at := func(sec int64) string { return strconv.FormatInt(sec, 10) }
	val := func(r result) string { return r.Value[1].(string) }

	if typ, rs := query("query", url.Values{"query": {"temp"}, "time": {at(t0/1000 + 60)}}); typ != "vector" || len(rs) != 2 || rs[0].Metric["room"] != "x" || val(rs[0]) != "21" || val(rs[1]) != "30" {
		t.Fatalf("instant: %s %+v", typ, rs)
	}
	if _, rs := query("query", url.Values{"query": {`temp{room="y"}`}, "time": {at(t0/1000 + 60)}}); len(rs) != 1 || val(rs[0]) != "30" {
		t.Fatalf("matcher: %+v", rs)
	}
	if _, rs := query("query", url.Values{"query": {`temp{room!="y"}`}, "time": {at(t0/1000 + 60)}}); len(rs) != 1 || rs[0].Metric["room"] != "x" {
		t.Fatalf("not-equal matcher: %+v", rs)
	}
	if _, rs := query("query", url.Values{"query": {`{__name__=~"te.*", room=~"x|z"}`}, "time": {at(t0/1000 + 60)}}); len(rs) != 1 || rs[0].Metric["__name__"] != "temp" {
		t.Fatalf("regex matcher: %+v", rs)
	}
	if _, rs := query("query", url.Values{"query": {`temp`}, "time": {at(t0/1000 + 3600)}}); len(rs) != 0 {
		t.Fatalf("a stale sample was returned: %+v", rs)
	}
	if _, rs := query("query", url.Values{"query": {"sum(temp)"}, "time": {at(t0/1000 + 60)}}); len(rs) != 1 || val(rs[0]) != "51" || len(rs[0].Metric) != 0 {
		t.Fatalf("sum: %+v", rs)
	}
	if _, rs := query("query", url.Values{"query": {"sum by (room) (temp)"}, "time": {at(t0/1000 + 60)}}); len(rs) != 2 || val(rs[0]) != "21" || rs[1].Metric["room"] != "y" {
		t.Fatalf("sum by: %+v", rs)
	}
	if _, rs := query("query", url.Values{"query": {"count(temp) by (job)"}, "time": {at(t0/1000 + 60)}}); len(rs) != 1 || val(rs[0]) != "2" {
		t.Fatalf("count: %+v", rs)
	}
	if _, rs := query("query", url.Values{"query": {"max(temp)"}, "time": {at(t0/1000 + 60)}}); len(rs) != 1 || val(rs[0]) != "30" {
		t.Fatalf("max: %+v", rs)
	}
	// a counter: 1/s on instance a; 2/s on b, whose reset is not counted as a drop
	if _, rs := query("query", url.Values{"query": {"rate(http_requests_total[5m])"}, "time": {at(t0/1000 + 50)}}); len(rs) != 2 || rs[0].Metric["instance"] != "a" || val(rs[0]) != "1" ||
		rs[0].Metric["__name__"] != "" {
		t.Fatalf("rate: %+v", rs)
	} else if f, _ := strconv.ParseFloat(val(rs[1]), 64); f < 1.5 || f > 2.5 {
		t.Fatalf("rate over a reset: %v", rs[1])
	}
	if _, rs := query("query", url.Values{"query": {"sum by (job) (rate(http_requests_total[5m]))"}, "time": {at(t0/1000 + 50)}}); len(rs) != 1 || rs[0].Metric["job"] != "api" {
		t.Fatalf("sum of rate: %+v", rs)
	} else if f, _ := strconv.ParseFloat(val(rs[0]), 64); f < 2.5 || f > 3.5 {
		t.Fatalf("sum of rates: %v", f)
	}
	if _, rs := query("query", url.Values{"query": {`increase(http_requests_total{instance="a"}[5m])`}, "time": {at(t0/1000 + 50)}}); len(rs) != 1 || val(rs[0]) != "50" {
		t.Fatalf("increase: %+v", rs)
	}
	if typ, rs := query("query", url.Values{"query": {`http_requests_total{instance="a"}[25s]`}, "time": {at(t0/1000 + 50)}}); typ != "matrix" || len(rs) != 1 || len(rs[0].Values) != 3 {
		t.Fatalf("range selector: %s %+v", typ, rs)
	}
	if typ, rs := query("query_range", url.Values{"query": {`temp{room="x"}`}, "start": {at(t0 / 1000)}, "end": {at(t0/1000 + 60)}, "step": {"30"}}); typ != "matrix" || len(rs) != 1 || len(rs[0].Values) != 3 ||
		rs[0].Values[0][1] != "20" || rs[0].Values[2][1] != "21" {
		t.Fatalf("query_range: %s %+v", typ, rs)
	}
	if msg, _ := query("query", url.Values{"query": {"temp + 1"}}); !strings.HasPrefix(msg, "bad_data") {
		t.Fatalf("an unsupported expression: %q", msg)
	}
	if msg, _ := query("query", url.Values{"query": {"temp{"}}); !strings.HasPrefix(msg, "bad_data") {
		t.Fatalf("a broken expression: %q", msg)
	}
	var labels struct{ Data []string }
	req, _ := http.NewRequest(http.MethodGet, base+"/workspaces/"+id+"/api/v1/label/room/values", nil)
	awstest.Sign(t, req, "aps")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	json.NewDecoder(res.Body).Decode(&labels)
	res.Body.Close()
	if len(labels.Data) != 2 || labels.Data[0] != "x" {
		t.Fatalf("label values: %+v", labels)
	}

	// a deleted workspace takes its children and its samples with it
	if _, err := c.DeleteWorkspace(ctx, &amp.DeleteWorkspaceInput{WorkspaceId: w.WorkspaceId}); err != nil {
		t.Fatal(err)
	}
	var nf *types.ResourceNotFoundException
	if _, err := c.DescribeWorkspace(ctx, &amp.DescribeWorkspaceInput{WorkspaceId: w.WorkspaceId}); !errors.As(err, &nf) {
		t.Fatalf("workspace not found: %T %v", err, err)
	}
	if _, err := c.DescribeAlertManagerDefinition(ctx, &amp.DescribeAlertManagerDefinitionInput{WorkspaceId: w.WorkspaceId}); !errors.As(err, &nf) {
		t.Fatalf("alert manager of a deleted workspace: %T %v", err, err)
	}
	if st, _ := post("remote_write", ampdata.EncodeWrite(tx), id); st != 404 {
		t.Fatalf("remote write to a deleted workspace: %d", st)
	}
}
