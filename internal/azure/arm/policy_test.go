package arm_test

import (
	"fmt"
	"net/url"
	"strings"
	"testing"
)

// TestPolicyEvaluation creates policies as az policy does and checks what they deny and what policy states report.
func TestPolicyEvaluation(t *testing.T) {
	e := start(t, "azpolicy")
	s := "/subscriptions/" + sub
	demo := s + "/resourceGroups/demo"
	const v = "?api-version=2023-04-01"
	put := func(path, body string, status int, code string) map[string]any {
		t.Helper()
		st, out, _ := e.do(t, "PUT", path+v, body)
		if got := fmt.Sprint(out["error"]); st != status || code != "" && !strings.Contains(got, "code:"+code+" ") {
			t.Fatalf("PUT %s: %d %v, want %d %s", path, st, out, status, code)
		}
		return out
	}
	put(demo, `{"location":"westeurope","tags":{"env":"test"}}`, 201, "")

	// a deny definition with a parameter, as az policy definition create sends it
	put(s+"/providers/Microsoft.Authorization/policyDefinitions/allowed-locations", `{"properties":{"displayName":"Allowed locations","mode":"Indexed",
		"parameters":{"allowed":{"type":"Array"}},
		"policyRule":{"if":{"not":{"field":"location","in":"[parameters('allowed')]"}},"then":{"effect":"deny"}}}}`, 201, "")
	// a rule with an unknown operator, or no effect, is refused
	put(s+"/providers/Microsoft.Authorization/policyDefinitions/bad", `{"properties":{"policyRule":{"if":{"field":"location","equalz":"x"},"then":{"effect":"deny"}}}}`, 400, "InvalidPolicyRule")
	put(s+"/providers/Microsoft.Authorization/policyDefinitions/bad", `{"properties":{"policyRule":{"if":{"field":"location","equals":"x"},"then":{}}}}`, 400, "InvalidPolicyRule")

	// an assignment must give the parameters its definition needs, and no others
	asg := demo + "/providers/Microsoft.Authorization/policyAssignments/only-europe"
	def := s + "/providers/Microsoft.Authorization/policyDefinitions/allowed-locations"
	put(asg, `{"properties":{"policyDefinitionId":"`+def+`"}}`, 400, "MissingPolicyParameter")
	put(asg, `{"properties":{"policyDefinitionId":"`+def+`","parameters":{"allowed":{"value":["westeurope"]},"other":{"value":1}}}}`, 400, "UndefinedPolicyParameter")
	put(asg, `{"properties":{"displayName":"Only Europe","policyDefinitionId":"`+def+`","parameters":{"allowed":{"value":["westeurope","northeurope"]}}}}`, 201, "")

	// deny: a resource outside the allowed locations is refused, with the policy's name; others are created
	w := demo + "/providers/Microsoft.Widgets/widgets/"
	out := put(w+"us", `{"location":"eastus"}`, 403, "RequestDisallowedByPolicy")
	if msg := fmt.Sprint(out["error"]); !strings.Contains(msg, "Only Europe") || !strings.Contains(msg, "Allowed locations") || !strings.Contains(msg, "Resource 'us' was disallowed") {
		t.Errorf("deny message: %s", msg)
	}
	if st, _, _ := e.do(t, "GET", w+"us"+v, ""); st != 404 {
		t.Errorf("a denied resource was stored: %d", st)
	}
	put(w+"eu", `{"location":"westeurope","properties":{"color":"red-ish","parts":[{"size":1},{"size":5}]}}`, 201, "")
	put(w+"ne", `{"location":"northeurope","tags":{"env":"prod"},"properties":{"color":"blue","parts":[{"size":1}]}}`, 201, "")
	put(s+"/resourceGroups/pogi/providers/Microsoft.Widgets/widgets/elsewhere", `{"location":"eastus"}`, 201, "") // outside the scope
	// an update is evaluated too: PATCH merges, then the result is checked
	if st, out, _ := e.do(t, "PATCH", w+"eu"+v, `{"location":"eastus"}`); st != 403 {
		t.Errorf("PATCH to a denied location: %d %v", st, out)
	}

	// audit, at the subscription: a tag named by a parameter, the field built with concat, and the effect a parameter
	put(s+"/providers/Microsoft.Authorization/policyDefinitions/need-tag", `{"properties":{"mode":"All",
		"parameters":{"tagName":{"type":"String"},"effect":{"type":"String","defaultValue":"Audit"}},
		"policyRule":{"if":{"field":"[concat('tags[', parameters('tagName'), ']')]","exists":"false"},"then":{"effect":"[parameters('effect')]"}}}}`, 201, "")
	put(s+"/providers/Microsoft.Authorization/policyAssignments/need-env", `{"properties":{"policyDefinitionId":"`+s+`/providers/Microsoft.Authorization/policyDefinitions/need-tag",
		"parameters":{"tagName":{"value":"env"}},"notScopes":["`+s+`/resourceGroups/pogi"]}}`, 201, "")

	// policy states of the group: each resource against each deny and audit assignment
	states := func(scope, filter string) []any {
		t.Helper()
		st, out, _ := e.do(t, "POST", scope+"/providers/Microsoft.PolicyInsights/policyStates/latest/queryResults?api-version=2019-10-01&$filter="+url.QueryEscape(filter), "")
		if st != 200 {
			t.Fatalf("states: %d %v", st, out)
		}
		return out["value"].([]any)
	}
	brief := func(l []any) string {
		var b []string
		for _, it := range l {
			m := it.(map[string]any)
			b = append(b, fmt.Sprintf("%s %s %s", m["policyAssignmentName"], m["resourceId"].(string)[len(s):], m["complianceState"]))
		}
		return strings.Join(b, "\n")
	}
	want := "need-env /resourceGroups/demo/providers/Microsoft.Widgets/widgets/eu NonCompliant\n" +
		"need-env /resourceGroups/demo/providers/Microsoft.Widgets/widgets/ne Compliant\n" +
		"need-env /resourceGroups/demo Compliant\n" +
		"only-europe /resourceGroups/demo/providers/Microsoft.Widgets/widgets/eu Compliant\n" +
		"only-europe /resourceGroups/demo/providers/Microsoft.Widgets/widgets/ne Compliant"
	if got := brief(states(demo, "")); got != want {
		t.Errorf("states:\n%s\nwant:\n%s", got, want)
	}
	if got := brief(states(s, "complianceState eq 'NonCompliant'")); got != "need-env /resourceGroups/demo/providers/Microsoft.Widgets/widgets/eu NonCompliant" {
		t.Errorf("non-compliant states of the subscription:\n%s", got)
	}
	if got := len(states(s+"/resourceGroups/demo/providers/Microsoft.Authorization/policyAssignments/only-europe", "")); got != 2 {
		t.Errorf("states of one assignment: %d", got)
	}
	st, sum, _ := e.do(t, "POST", demo+"/providers/Microsoft.PolicyInsights/policyStates/latest/summarize?api-version=2019-10-01", "")
	if res := sum["value"].([]any)[0].(map[string]any)["results"].(map[string]any); st != 200 || res["nonCompliantResources"] != 1.0 || res["nonCompliantPolicies"] != 1.0 {
		t.Errorf("summarize: %d %v", st, sum)
	}

	// DoNotEnforce: nothing is denied, but the resource is reported
	put(asg, `{"properties":{"enforcementMode":"DoNotEnforce","policyDefinitionId":"`+def+`","parameters":{"allowed":{"value":["westeurope"]}}}}`, 200, "")
	put(w+"us", `{"location":"eastus","tags":{"env":"dev"}}`, 201, "")
	if got := brief(states(demo, "complianceState eq 'NonCompliant'")); !strings.Contains(got, "only-europe /resourceGroups/demo/providers/Microsoft.Widgets/widgets/us NonCompliant") {
		t.Errorf("DoNotEnforce states:\n%s", got)
	}

	// the other operators and logic: like on an alias, [*] over an array, anyOf, and a resource group rule in mode All
	put(s+"/providers/Microsoft.Authorization/policyDefinitions/no-red", `{"properties":{"mode":"All","policyRule":{"if":{"allOf":[
		{"field":"type","equals":"Microsoft.Widgets/widgets"},
		{"anyOf":[{"field":"Microsoft.Widgets/widgets/color","like":"RED*"},{"field":"Microsoft.Widgets/widgets/parts[*].size","greater":3}]}]},
		"then":{"effect":"deny"}}}}`, 201, "")
	put(s+"/providers/Microsoft.Authorization/policyDefinitions/rg-us", `{"properties":{"mode":"All","policyRule":{"if":{"allOf":[
		{"field":"type","equals":"Microsoft.Resources/subscriptions/resourceGroups"},{"field":"location","notEquals":"eastus"}]},
		"then":{"effect":"deny"}}}}`, 201, "")
	for _, name := range []string{"no-red", "rg-us"} {
		put(s+"/providers/Microsoft.Authorization/policyAssignments/"+name, `{"properties":{"policyDefinitionId":"`+s+`/providers/Microsoft.Authorization/policyDefinitions/`+name+`"}}`, 201, "")
	}
	put(w+"red", `{"location":"westeurope","tags":{"env":"x"},"properties":{"color":"red"}}`, 403, "RequestDisallowedByPolicy")
	put(w+"big", `{"location":"westeurope","tags":{"env":"x"},"properties":{"parts":[{"size":4},{"size":5}]}}`, 403, "RequestDisallowedByPolicy")
	put(w+"mixed", `{"location":"westeurope","tags":{"env":"x"},"properties":{"color":"green","parts":[{"size":4},{"size":1}]}}`, 201, "") // not every part is big
	put(s+"/resourceGroups/west", `{"location":"westus"}`, 403, "RequestDisallowedByPolicy")
	put(s+"/resourceGroups/east", `{"location":"eastus"}`, 201, "")
}
