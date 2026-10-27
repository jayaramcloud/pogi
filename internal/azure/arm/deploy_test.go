package arm_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/resources/armresources"
)

const depAPI = "?api-version=2022-09-01"

// deployBody is a deployment request for a template and parameter values (JSON objects).
func deployBody(tmpl, params string) string {
	if params == "" {
		params = "{}"
	}
	return `{"properties":{"mode":"Incremental","template":` + tmpl + `,"parameters":` + params + `}}`
}

// run PUTs a deployment and GETs it once it has run, as a client that polls does.
func (e *env) run(t *testing.T, path, body string) (int, map[string]any) {
	t.Helper()
	st, out, _ := e.do(t, "PUT", path+depAPI, body)
	if st >= 300 {
		return st, out
	}
	if p := out["properties"].(map[string]any); p["provisioningState"] != "Accepted" {
		t.Errorf("PUT %s answered %v, not Accepted", path, p["provisioningState"])
	}
	_, out, _ = e.do(t, "GET", path+depAPI, "")
	return st, out
}

func errCode(out map[string]any) string {
	e, _ := out["error"].(map[string]any)
	return fmt.Sprint(e["code"], ": ", e["message"])
}

// TestDeploymentRuns deploys a template with the Go SDK, as az and Terraform do, and checks what it made.
func TestDeploymentRuns(t *testing.T) {
	e := start(t, "azmonitor")
	g := "/subscriptions/" + sub + "/resourceGroups/demo"
	if st, out, _ := e.do(t, "PUT", g+depAPI, `{"location":"westeurope"}`); st != 201 {
		t.Fatal(st, out)
	}
	tmpl := `{
	  "$schema": "https://schema.management.azure.com/schemas/2019-04-01/deploymentTemplate.json#", "contentVersion": "1.0.0.0",
	  "parameters": {
	    "prefix": {"type": "string", "minLength": 2},
	    "count": {"type": "int", "defaultValue": 2, "maxValue": 5},
	    "location": {"type": "string", "defaultValue": "[resourceGroup().location]"},
	    "make": {"type": "bool", "defaultValue": false}
	  },
	  "variables": {
	    "wsName": "[concat(parameters('prefix'), '-logs')]",
	    "copy": [{"name": "sizes", "count": "[parameters('count')]", "input": "[mul(copyIndex('sizes'), 10)]"}]
	  },
	  "resources": [
	    {"type": "Microsoft.Widgets/widgets", "apiVersion": "2023-01-01", "name": "[format('{0}-w{1}', parameters('prefix'), copyIndex())]",
	     "location": "[parameters('location')]", "copy": {"name": "widgets", "count": "[parameters('count')]"},
	     "dependsOn": ["[resourceId('Microsoft.OperationalInsights/workspaces', variables('wsName'))]"],
	     "properties": {"size": "[variables('sizes')[copyIndex()]]", "literal": "[[not evaluated]",
	       "workspace": "[reference(resourceId('Microsoft.OperationalInsights/workspaces', variables('wsName')), '2022-10-01').customerId]",
	       "copy": [{"name": "slots", "count": 2, "input": {"n": "[copyIndex('slots', 1)]"}}]},
	     "resources": [{"type": "parts", "apiVersion": "2023-01-01", "name": "p1", "properties": {"of": "[copyIndex('widgets')]"}}]},
	    {"type": "Microsoft.OperationalInsights/workspaces", "apiVersion": "2022-10-01", "name": "[variables('wsName')]",
	     "location": "[parameters('location')]", "properties": {"retentionInDays": 30}},
	    {"condition": "[parameters('make')]", "type": "Microsoft.Widgets/widgets", "apiVersion": "2023-01-01", "name": "skipped"},
	    {"type": "Microsoft.Authorization/locks", "apiVersion": "2020-05-01", "name": "nodelete", "properties": {"level": "CanNotDelete"},
	     "scope": "[format('Microsoft.Widgets/widgets/{0}-w0', parameters('prefix'))]", "dependsOn": ["widgets"]}
	  ],
	  "outputs": {
	    "wsId": {"type": "string", "value": "[resourceId('Microsoft.OperationalInsights/workspaces', variables('wsName'))]"},
	    "hasKey": {"type": "bool", "value": "[not(empty(listKeys(resourceId('Microsoft.OperationalInsights/workspaces', variables('wsName')), '2022-10-01').primarySharedKey))]"},
	    "names": {"type": "array", "copy": {"count": "[parameters('count')]", "input": "[format('{0}-w{1}', parameters('prefix'), copyIndex())]"}},
	    "unique": {"type": "string", "value": "[uniqueString(resourceGroup().id)]"},
	    "never": {"type": "string", "condition": "[parameters('make')]", "value": "x"}
	  }
	}`
	var tm map[string]any
	if err := json.Unmarshal([]byte(tmpl), &tm); err != nil {
		t.Fatal(err)
	}
	d := e.factory(sub).NewDeploymentsClient()
	poller := must(d.BeginCreateOrUpdate(ctx, "demo", "main", armresources.Deployment{Properties: &armresources.DeploymentProperties{
		Mode: to.Ptr(armresources.DeploymentModeIncremental), Template: tm, Parameters: map[string]any{"prefix": map[string]any{"value": "demo"}}}}, nil))
	dep := must(poller.PollUntilDone(ctx, &azruntimePoll))
	if *dep.Properties.ProvisioningState != armresources.ProvisioningStateSucceeded {
		t.Fatalf("%+v", dep.Properties.Error)
	}
	outs, _ := json.Marshal(dep.Properties.Outputs)
	wsID := g + "/providers/Microsoft.OperationalInsights/workspaces/demo-logs"
	if want := `"hasKey":{"type":"Bool","value":true},"names":{"type":"Array","value":["demo-w0","demo-w1"]}`; !strings.Contains(string(outs), want) ||
		!strings.Contains(string(outs), `"wsId":{"type":"String","value":"`+wsID+`"}`) || strings.Contains(string(outs), "never") {
		t.Errorf("outputs: %s", outs)
	}
	if u := dep.Properties.Outputs.(map[string]any)["unique"].(map[string]any)["value"].(string); len(u) != 13 {
		t.Errorf("uniqueString: %q", u)
	}
	if p := dep.Properties.Parameters.(map[string]any); fmt.Sprint(p["count"]) != "map[type:Int value:2]" || fmt.Sprint(p["location"]) != "map[type:String value:westeurope]" {
		t.Errorf("parameters: %v", p)
	}

	// the resources are there, built from the template
	_, ws, _ := e.do(t, "GET", wsID+depAPI, "")
	for i, size := range []float64{0, 10} {
		_, w, _ := e.do(t, "GET", fmt.Sprintf("%s/providers/Microsoft.Widgets/widgets/demo-w%d%s", g, i, depAPI), "")
		p, _ := w["properties"].(map[string]any)
		if p["size"] != size || p["workspace"] != ws["properties"].(map[string]any)["customerId"] || w["location"] != "westeurope" || p["literal"] != "[not evaluated]" ||
			fmt.Sprint(p["slots"]) != "[map[n:1] map[n:2]]" {
			t.Errorf("widget %d: %v", i, w)
		}
		if st, part, _ := e.do(t, "GET", fmt.Sprintf("%s/providers/Microsoft.Widgets/widgets/demo-w%d/parts/p1%s", g, i, depAPI), ""); st != 200 || part["properties"].(map[string]any)["of"] != float64(i) {
			t.Errorf("part %d: %d %v", i, st, part)
		}
	}
	if st, _, _ := e.do(t, "GET", g+"/providers/Microsoft.Widgets/widgets/skipped"+depAPI, ""); st != 404 {
		t.Errorf("a resource whose condition is false was made: %d", st)
	}
	if st, lock, _ := e.do(t, "GET", g+"/providers/Microsoft.Widgets/widgets/demo-w0/providers/Microsoft.Authorization/locks/nodelete"+depAPI, ""); st != 200 {
		t.Errorf("lock: %d %v", st, lock)
	}

	// operations, in the order the resources were made: the workspace first, as the widgets depend on it
	var got []string
	for pg := must(armresources.NewDeploymentOperationsClient(sub, e.cred, e.opts)).NewListPager("demo", "main", nil); pg.More(); {
		for _, op := range must(pg.NextPage(ctx)).Value {
			got = append(got, *op.Properties.TargetResource.ResourceName+" "+*op.Properties.ProvisioningState)
		}
	}
	if want := "demo-logs Succeeded,demo-w0 Succeeded,demo-w0/p1 Succeeded,demo-w1 Succeeded,demo-w1/p1 Succeeded,nodelete Succeeded"; strings.Join(got, ",") != want {
		t.Errorf("operations: %s", strings.Join(got, ","))
	}
	// deployments and their operations are not resources of the group (the workspace, the widgets and their parts are)
	_, list, _ := e.do(t, "GET", g+"/resources"+depAPI, "")
	if n := len(list["value"].([]any)); n != 5 {
		t.Errorf("resources of the group: %d %v", n, list)
	}

	// a run that fails: the first resource is made, the second is refused, the third never runs
	bad := `{"resources": [
	  {"type": "Microsoft.Widgets/widgets", "apiVersion": "2023-01-01", "name": "first"},
	  {"type": "Microsoft.Made/ups", "apiVersion": "2023-01-01", "name": "second", "dependsOn": ["first"]},
	  {"type": "Microsoft.Widgets/widgets", "apiVersion": "2023-01-01", "name": "third", "dependsOn": ["second"]}]}`
	tm = nil
	json.Unmarshal([]byte(bad), &tm)
	poller = must(d.BeginCreateOrUpdate(ctx, "demo", "broken", armresources.Deployment{Properties: &armresources.DeploymentProperties{
		Mode: to.Ptr(armresources.DeploymentModeIncremental), Template: tm}}, nil))
	_, err := poller.PollUntilDone(ctx, &azruntimePoll)
	var re *azcore.ResponseError
	if !errors.As(err, &re) || re.ErrorCode != "DeploymentFailed" || !strings.Contains(err.Error(), "NoRegisteredProviderFound") {
		t.Fatalf("a failed deployment: %v", err)
	}
	if got := must(d.Get(ctx, "demo", "broken", nil)); *got.Properties.ProvisioningState != armresources.ProvisioningStateFailed {
		t.Errorf("state: %s", *got.Properties.ProvisioningState)
	}
	if st, _, _ := e.do(t, "GET", g+"/providers/Microsoft.Widgets/widgets/first"+depAPI, ""); st != 200 {
		t.Errorf("the resource made before the failure is gone: %d", st)
	}
	if st, _, _ := e.do(t, "GET", g+"/providers/Microsoft.Widgets/widgets/third"+depAPI, ""); st != 404 {
		t.Errorf("a resource after the failure was made: %d", st)
	}
	_, ops, _ := e.do(t, "GET", g+"/providers/Microsoft.Resources/deployments/broken/operations"+depAPI, "")
	if v := ops["value"].([]any); len(v) != 2 || v[1].(map[string]any)["properties"].(map[string]any)["provisioningState"] != "Failed" ||
		v[1].(map[string]any)["properties"].(map[string]any)["statusCode"] != "BadRequest" {
		t.Errorf("operations of the failed run: %v", ops)
	}
	// running it again replaces the run and its operations
	if st, out := e.run(t, g+"/providers/Microsoft.Resources/deployments/broken", deployBody(`{"resources":[]}`, "")); st != 200 || out["properties"].(map[string]any)["provisioningState"] != "Succeeded" {
		t.Errorf("re-run: %d %v", st, out)
	}
	if _, ops, _ = e.do(t, "GET", g+"/providers/Microsoft.Resources/deployments/broken/operations"+depAPI, ""); len(ops["value"].([]any)) != 0 {
		t.Errorf("old operations kept: %v", ops)
	}
	// deleting a deployment is long-running, as az expects, and leaves its resources
	del := must(d.BeginDelete(ctx, "demo", "broken", nil))
	must(del.PollUntilDone(ctx, &azruntimePoll))
	if st, _, _ := e.do(t, "GET", g+"/providers/Microsoft.Resources/deployments/broken/operations"+depAPI, ""); st != 404 {
		t.Errorf("operations of a deleted deployment: %d", st)
	}
	if st, _, _ := e.do(t, "GET", g+"/providers/Microsoft.Widgets/widgets/first"+depAPI, ""); st != 200 {
		t.Errorf("deleting the deployment deleted its resource: %d", st)
	}
}

// TestDeploymentScopes runs nested deployments (Bicep modules), a deployment at subscription scope and a template
// with symbolic names.
func TestDeploymentScopes(t *testing.T) {
	e := start(t)
	s := "/subscriptions/" + sub
	for _, rg := range []string{"demo", "other"} {
		e.do(t, "PUT", s+"/resourceGroups/"+rg+depAPI, `{"location":"eastus"}`)
	}
	tmpl := `{"parameters": {"prefix": {"type": "string"}}, "variables": {"tag": "[[literal]"}, "resources": [
	  {"type": "Microsoft.Resources/deployments", "apiVersion": "2022-09-01", "name": "inner", "resourceGroup": "other",
	   "properties": {"mode": "Incremental", "expressionEvaluationOptions": {"scope": "inner"}, "parameters": {"n": {"value": "[parameters('prefix')]"}},
	     "template": {"parameters": {"n": {"type": "string"}}, "resources": [
	       {"type": "Microsoft.Widgets/widgets", "apiVersion": "2023-01-01", "name": "[concat(parameters('n'), '-inner')]"}],
	       "outputs": {"rg": {"type": "string", "value": "[resourceGroup().name]"}}}}},
	  {"type": "Microsoft.Resources/deployments", "apiVersion": "2022-09-01", "name": "outer",
	   "properties": {"mode": "Incremental", "template": {"resources": [
	     {"type": "Microsoft.Widgets/widgets", "apiVersion": "2023-01-01", "name": "[concat(parameters('prefix'), '-outer')]", "tags": {"t": "[variables('tag')]"}}]}}}],
	  "outputs": {"fromModule": {"type": "string", "value": "[reference(resourceId('other', 'Microsoft.Resources/deployments', 'inner'), '2022-09-01').outputs.rg.value]"}}}`
	st, out := e.run(t, s+"/resourceGroups/demo/providers/Microsoft.Resources/deployments/modules", deployBody(tmpl, `{"prefix":{"value":"m"}}`))
	if p := out["properties"].(map[string]any); st != 201 || p["provisioningState"] != "Succeeded" || fmt.Sprint(p["outputs"]) != "map[fromModule:map[type:String value:other]]" {
		t.Fatalf("modules: %d %v", st, out)
	}
	if st, _, _ := e.do(t, "GET", s+"/resourceGroups/other/providers/Microsoft.Widgets/widgets/m-inner"+depAPI, ""); st != 200 {
		t.Errorf("inner module's widget: %d", st)
	}
	if st, w, _ := e.do(t, "GET", s+"/resourceGroups/demo/providers/Microsoft.Widgets/widgets/m-outer"+depAPI, ""); st != 200 || fmt.Sprint(w["tags"]) != "map[t:[literal]]" {
		t.Errorf("outer-scope widget: %d %v", st, w)
	}
	if st, _, _ := e.do(t, "GET", s+"/resourceGroups/other/providers/Microsoft.Resources/deployments/inner"+depAPI, ""); st != 200 {
		t.Errorf("the nested deployment is not kept: %d", st)
	}

	// subscription scope: a group, then a module into it
	tmpl = `{"resources": [
	  {"type": "Microsoft.Resources/resourceGroups", "apiVersion": "2022-09-01", "name": "fromsub", "location": "westus", "tags": {"by": "[deployment().name]"}},
	  {"type": "Microsoft.Resources/deployments", "apiVersion": "2022-09-01", "name": "into", "resourceGroup": "fromsub",
	   "dependsOn": ["[resourceId('Microsoft.Resources/resourceGroups', 'fromsub')]"],
	   "properties": {"mode": "Incremental", "expressionEvaluationOptions": {"scope": "inner"}, "template": {"resources": [
	     {"type": "Microsoft.Widgets/widgets", "apiVersion": "2023-01-01", "name": "w", "location": "[resourceGroup().location]"}]}}}]}`
	st, out = e.run(t, s+"/providers/Microsoft.Resources/deployments/subdep", `{"location":"westus",`+deployBody(tmpl, "")[1:])
	if st != 201 || out["properties"].(map[string]any)["provisioningState"] != "Succeeded" {
		t.Fatalf("subscription scope: %d %v", st, out)
	}
	if st, g, _ := e.do(t, "GET", s+"/resourceGroups/fromsub"+depAPI, ""); st != 200 || fmt.Sprint(g["tags"]) != "map[by:subdep]" {
		t.Errorf("group: %d %v", st, g)
	}
	if st, w, _ := e.do(t, "GET", s+"/resourceGroups/fromsub/providers/Microsoft.Widgets/widgets/w"+depAPI, ""); st != 200 || w["location"] != "westus" {
		t.Errorf("widget in the new group: %d %v", st, w)
	}
	if _, l, _ := e.do(t, "GET", s+"/providers/Microsoft.Resources/deployments"+depAPI, ""); len(l["value"].([]any)) != 1 {
		t.Errorf("the subscription's deployments: %v", l)
	}

	// languageVersion 2.0: symbolic names in dependsOn and reference(), and an existing resource
	tmpl = `{"languageVersion": "2.0", "resources": {
	  "part": {"type": "Microsoft.Widgets/widgets/parts", "apiVersion": "2023-01-01", "name": "sym/p", "dependsOn": ["w"]},
	  "w": {"type": "Microsoft.Widgets/widgets", "apiVersion": "2023-01-01", "name": "sym", "properties": {"size": 7}},
	  "old": {"existing": true, "type": "Microsoft.Widgets/widgets", "apiVersion": "2023-01-01", "name": "m-outer"}},
	  "outputs": {"size": {"type": "int", "value": "[reference('w').size]"}, "oldTags": {"type": "object", "value": "[reference('old', '2023-01-01', 'Full').tags]"}}}`
	st, out = e.run(t, s+"/resourceGroups/demo/providers/Microsoft.Resources/deployments/sym", deployBody(tmpl, ""))
	if p := out["properties"].(map[string]any); st != 201 || p["provisioningState"] != "Succeeded" || fmt.Sprint(p["outputs"]) != "map[oldTags:map[type:Object value:map[t:[literal]]] size:map[type:Int value:7]]" {
		t.Errorf("symbolic names: %d %v", st, out)
	}
}

// TestDeploymentChecks refuses templates Azure refuses, before anything is made, on PUT and on validate.
func TestDeploymentChecks(t *testing.T) {
	e := start(t)
	g := "/subscriptions/" + sub + "/resourceGroups/pogi/providers/Microsoft.Resources/deployments/"
	w := func(extra string) string {
		return `{"type": "Microsoft.Widgets/widgets", "apiVersion": "2023-01-01", "name": "a"` + extra + `}`
	}
	params := `{"p": {"type": "string", "allowedValues": ["x", "y"]}, "n": {"type": "int", "defaultValue": 1}}`
	for name, c := range map[string]struct{ body, code, msg string }{
		"missing parameter": {deployBody(`{"parameters":`+params+`}`, ""), "InvalidTemplate", "'p' is not provided"},
		"not allowed":       {deployBody(`{"parameters":`+params+`}`, `{"p":{"value":"z"}}`), "InvalidTemplate", "allowed values"},
		"wrong type":        {deployBody(`{"parameters":`+params+`}`, `{"p":{"value":"x"},"n":{"value":"1"}}`), "InvalidTemplate", "type 'int'"},
		"undeclared":        {deployBody(`{}`, `{"q":{"value":1}}`), "InvalidTemplate", "does not declare"},
		"unknown dependsOn": {deployBody(`{"resources":[`+w(`,"dependsOn":["nope"]`)+`]}`, ""), "InvalidTemplate", "'nope' is not defined"},
		"circular":          {deployBody(`{"resources":[`+w(`,"dependsOn":["b"]`)+`,{"type":"Microsoft.Widgets/widgets","apiVersion":"1","name":"b","dependsOn":["a"]}]}`, ""), "InvalidTemplate", "circular"},
		"bad expression":    {deployBody(`{"resources":[{"type":"Microsoft.Widgets/widgets","apiVersion":"1","name":"[concat('a']"}]}`, ""), "InvalidTemplate", "not valid"},
		"unknown function":  {deployBody(`{"resources":[{"type":"Microsoft.Widgets/widgets","apiVersion":"1","name":"a","properties":{"x":"[madeUp()]"}}]}`, ""), "InvalidTemplate", "madeup"},
		"name segments":     {deployBody(`{"resources":[{"type":"Microsoft.Widgets/widgets/parts","apiVersion":"1","name":"onlyone"}]}`, ""), "InvalidTemplate", "2 name segments"},
		"too many copies":   {deployBody(`{"resources":[`+w(`,"copy":{"name":"c","count":900}`)+`]}`, ""), "InvalidTemplate", "from 0 to 800"},
		"variable cycle":    {deployBody(`{"variables":{"a":"[variables('b')]","b":"[variables('a')]"},"resources":[{"type":"Microsoft.Widgets/widgets","apiVersion":"1","name":"[variables('a')]"}]}`, ""), "InvalidTemplate", "refers to itself"},
		"complete mode":     {`{"properties":{"mode":"Complete","template":{}}}`, "InvalidDeploymentMode", "Incremental"},
		"linked template":   {`{"properties":{"mode":"Incremental","templateLink":{"uri":"https://example.com/t.json"}}}`, "InvalidTemplateDeployment", "linked templates"},
		"no template":       {`{"properties":{"mode":"Incremental"}}`, "InvalidTemplate", "no template"},
		"key vault param":   {deployBody(`{"parameters":`+params+`}`, `{"p":{"reference":{"keyVault":{"id":"x"},"secretName":"s"}}}`), "InvalidTemplate", "Key Vault"},
		"user functions":    {deployBody(`{"functions":[{"namespace":"x"}]}`, ""), "InvalidTemplate", "user-defined functions"},
		"copyIndex outside": {deployBody(`{"resources":[{"type":"Microsoft.Widgets/widgets","apiVersion":"1","name":"[string(copyIndex())]"}]}`, ""), "InvalidTemplate", "outside a copy loop"},
	} {
		for _, path := range []string{g + "d", g + "d/validate"} {
			method := "PUT"
			if strings.HasSuffix(path, "validate") {
				method = "POST"
			}
			st, out, _ := e.do(t, method, path+depAPI, c.body)
			if got := errCode(out); st != 400 || !strings.HasPrefix(got, c.code+":") || !strings.Contains(got, c.msg) {
				t.Errorf("%s (%s): %d %s", name, method, st, got)
			}
		}
	}
	if st, _, _ := e.do(t, "GET", g+"d"+depAPI, ""); st != 404 {
		t.Errorf("a refused deployment was stored: %d", st)
	}
	if st, _, _ := e.do(t, "GET", "/subscriptions/"+sub+"/resourceGroups/pogi/providers/Microsoft.Widgets/widgets/a"+depAPI, ""); st != 404 {
		t.Errorf("a refused deployment made a resource: %d", st)
	}
	st, out, _ := e.do(t, "PUT", "/subscriptions/"+sub+"/providers/Microsoft.Resources/deployments/s"+depAPI,
		deployBody(`{"resources":[{"type":"Microsoft.Widgets/widgets","apiVersion":"1","name":"[resourceGroup().name]"}]}`, ""))
	if st != 400 || !strings.Contains(errCode(out), "subscription scope") {
		t.Errorf("resourceGroup() at subscription scope: %d %v", st, out)
	}
	// validate answers what would be made
	st, out, _ = e.do(t, "POST", g+"v/validate"+depAPI, deployBody(`{"resources":[`+w(`,"copy":{"name":"c","count":2}`)+`]}`, ""))
	if v := out["properties"].(map[string]any)["validatedResources"].([]any); st != 200 || len(v) != 2 {
		t.Errorf("validate: %d %v", st, out)
	}
	// nested deployments stop at a depth
	nest := `{"resources":[]}`
	for range 12 {
		nest = `{"resources":[{"type":"Microsoft.Resources/deployments","apiVersion":"2022-09-01","name":"n","properties":{"mode":"Incremental","expressionEvaluationOptions":{"scope":"inner"},"template":` + nest + `}}]}`
	}
	if st, out := e.run(t, g+"deep", deployBody(nest, "")); st != 201 || !strings.Contains(fmt.Sprint(out), "nested more than 10 deep") {
		t.Errorf("deep nesting: %d %v", st, out)
	}
}
