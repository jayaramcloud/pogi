package arm

// Azure Policy evaluation. The policy assignments of a resource's subscription and resource group are evaluated
// against every create and update that comes through ARM: one whose rule matches with the effect "deny" refuses
// the request with RequestDisallowedByPolicy. Microsoft.PolicyInsights policy states report each resource in
// scope against each deny and audit assignment: they are worked out when asked, so a new assignment covers the
// resources that already exist at once.
//
// ponytail: an alias is read from its name, not from Azure's alias table: <type>/<path> is properties.<path>, or
// the top-level member for sku, kind, identity, plan, zones and extendedLocation; [*] means every element. No
// count conditions, policy sets, exemptions, management group or resource scopes, and only the deny and audit
// effects. A rule pogi can't evaluate (a count, a definition it does not have) denies nothing and reports nothing.

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

const (
	typAssignment = "Microsoft.Authorization/policyAssignments"
	typDefinition = "Microsoft.Authorization/policyDefinitions"
	typGroup      = "Microsoft.Resources/subscriptions/resourceGroups" // a resource group's type in policy rules
)

// policyOps are the operators of a policy condition, lower-cased.
var policyOps = []string{"equals", "notequals", "like", "notlike", "match", "notmatch", "matchinsensitively",
	"notmatchinsensitively", "contains", "notcontains", "in", "notin", "containskey", "notcontainskey", "less",
	"lessorequals", "greater", "greaterorequals", "exists"}

// get reads a member of a policy rule object: the rule language's names ignore case.
func get(m map[string]any, name string) (any, bool) {
	if v, ok := m[name]; ok {
		return v, true
	}
	for k, v := range m {
		if strings.EqualFold(k, name) {
			return v, true
		}
	}
	return nil, false
}

// CheckPolicyRule refuses a policy definition's rule that Azure would refuse: no "if" or "then", no effect, or a
// condition with an unknown operator.
func CheckPolicyRule(rule any) error {
	bad := func(f string, a ...any) error {
		return core.Errorf(http.StatusBadRequest, "InvalidPolicyRule", "The policy rule is not valid: "+f, a...)
	}
	m, ok := rule.(map[string]any)
	if !ok {
		return bad("policyRule must be an object with 'if' and 'then'.")
	}
	then, _ := get(m, "then")
	tm, _ := then.(map[string]any)
	if eff, _ := get(tm, "effect"); fmt.Sprint(eff) == "" || eff == nil {
		return bad("'then' must name an effect.")
	}
	cond, ok := get(m, "if")
	if !ok {
		return bad("'if' is missing.")
	}
	var check func(c any, depth int) error
	check = func(c any, depth int) error {
		cm, ok := c.(map[string]any)
		if !ok || depth > 64 {
			return bad("a condition must be an object, nested at most 64 deep.")
		}
		for _, logical := range []string{"allOf", "anyOf"} {
			if v, ok := get(cm, logical); ok {
				l, ok := v.([]any)
				if !ok || len(l) == 0 {
					return bad("'%s' must be a list of conditions.", logical)
				}
				for _, sub := range l {
					if err := check(sub, depth+1); err != nil {
						return err
					}
				}
				return nil
			}
		}
		if v, ok := get(cm, "not"); ok {
			return check(v, depth+1)
		}
		subjects := 0
		for _, k := range []string{"field", "value", "count"} {
			if _, ok := get(cm, k); ok {
				subjects++
			}
		}
		if subjects != 1 {
			return bad("a condition needs one of 'field', 'value' or 'count': %s.", compact(cm))
		}
		ops := 0
		for k := range cm {
			switch lk := strings.ToLower(k); {
			case lk == "field", lk == "value", lk == "count":
			case slices.Contains(policyOps, lk):
				ops++
			default:
				return bad("'%s' is not an operator.", k)
			}
		}
		if ops != 1 {
			return bad("a condition needs one operator: %s.", compact(cm))
		}
		return nil
	}
	return check(cond, 0)
}

func compact(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

// assignment is a policy assignment with its definition.
type assignment struct {
	doc, def map[string]any
}

func props(r map[string]any) map[string]any {
	p, _ := r["properties"].(map[string]any)
	if p == nil {
		p = map[string]any{}
	}
	return p
}

func (a assignment) id() string    { return str(a.doc["id"]) }
func (a assignment) defID() string { return str(props(a.doc)["policyDefinitionId"]) }

// scope is where the assignment applies: its ID without the assignment part.
func (a assignment) scope() string {
	id := a.id()
	if i := strings.LastIndex(strings.ToLower(id), "/providers/microsoft.authorization/policyassignments/"); i >= 0 {
		return id[:i]
	}
	return id
}

func within(id, scope string) bool {
	id, scope = strings.ToLower(id), strings.ToLower(scope)
	return id == scope || strings.HasPrefix(id, scope+"/")
}

// covers reports whether a resource is in the assignment's scope and not in one of its notScopes.
func (a assignment) covers(rid string) bool {
	if !within(rid, a.scope()) {
		return false
	}
	ns, _ := props(a.doc)["notScopes"].([]any)
	return !slices.ContainsFunc(ns, func(n any) bool { return within(rid, str(n)) })
}

// display is the name Azure shows: the display name, else the name.
func display(r map[string]any) string {
	if d := str(props(r)["displayName"]); d != "" {
		return d
	}
	return str(r["name"])
}

// policies lists the assignments of a subscription and its resource groups whose definitions pogi has.
func (s *Service) policies(c *core.Ctx, sub string) []assignment {
	ap, dp := Lookup(s.h, typAssignment), Lookup(s.h, typDefinition)
	if ap == nil || ap.List == nil || dp == nil || dp.Get == nil {
		return nil
	}
	items, _ := ap.List(c, ID{Subscription: sub})
	var out []assignment
	for _, it := range items {
		a := assignment{doc: it}
		did, ok := ParseID(a.defID())
		if !ok || !strings.EqualFold(did.Type(), typDefinition) {
			continue // a policy set, or a definition of a management group
		}
		if def, err := dp.Get(c, did); err == nil {
			a.def = def
			out = append(out, a)
		}
	}
	return out
}

// errNotEvaluated is a rule pogi can't evaluate: it denies nothing and reports nothing.
var errNotEvaluated = errors.New("not evaluated")

// wild is the value of an alias with [*]: a condition on it holds when it holds for every element.
type wild []any

// eval evaluates the assignment's rule against a resource: whether "if" matches, and the effect, lower-cased.
func (a assignment) eval(s *Service, c *core.Ctx, rid ID, res map[string]any) (bool, string, error) {
	dprops := props(a.def)
	mode := strings.ToLower(str(dprops["mode"]))
	switch {
	case mode != "" && mode != "all" && mode != "indexed": // a resource provider mode, for data such as Kubernetes objects
		return false, "", errNotEvaluated
	case mode == "indexed" && (str(res["location"]) == "" || strings.EqualFold(str(res["type"]), typGroup)):
		return false, "", errNotEvaluated
	}
	rule, _ := dprops["policyRule"].(map[string]any)
	e := &policyEval{s: s, c: c, a: a, rid: rid, res: res}
	e.env = &exprEnv{fn: e.fn}
	then, _ := get(rule, "then")
	tm, _ := then.(map[string]any)
	ev, _ := get(tm, "effect")
	effect, err := e.env.value(ev)
	if err != nil {
		return false, "", err
	}
	cond, _ := get(rule, "if")
	match, err := e.cond(cond)
	return match, strings.ToLower(fmt.Sprint(effect)), err
}

type policyEval struct {
	s   *Service
	c   *core.Ctx
	a   assignment
	rid ID
	res map[string]any
	env *exprEnv
}

// fn answers the policy functions of the template language.
func (e *policyEval) fn(name string, args []any) (any, bool, error) {
	switch name {
	case "parameters":
		if len(args) != 1 {
			return nil, true, fmt.Errorf("parameters() takes a name")
		}
		v, err := e.param(fmt.Sprint(args[0]))
		return v, true, err
	case "field":
		if len(args) != 1 {
			return nil, true, fmt.Errorf("field() takes a name")
		}
		v := e.field(fmt.Sprint(args[0]))
		if w, ok := v.(wild); ok {
			return []any(w), true, nil
		}
		return v, true, nil
	case "resourcegroup":
		g := map[string]any{"name": e.rid.ResourceGroup, "id": "/subscriptions/" + e.rid.Subscription + "/resourceGroups/" + e.rid.ResourceGroup}
		e.s.mu.Lock()
		if doc := e.s.st.Groups[groupKey(e.rid.Subscription, e.rid.ResourceGroup)]; doc != nil {
			g["location"], g["tags"] = doc["location"], doc["tags"]
		}
		e.s.mu.Unlock()
		return g, true, nil
	case "subscription":
		return map[string]any{"id": "/subscriptions/" + e.rid.Subscription, "subscriptionId": e.rid.Subscription,
			"tenantId": TenantID, "displayName": e.s.subName(e.rid.Subscription)}, true, nil
	case "policy":
		return map[string]any{"assignmentId": e.a.id(), "definitionId": e.a.defID(), "setDefinitionId": "", "definitionReferenceId": ""}, true, nil
	}
	return nil, false, nil
}

// param is a parameter's value: the assignment's, else the definition's default.
func (e *policyEval) param(name string) (any, error) {
	ap, _ := props(e.a.doc)["parameters"].(map[string]any)
	if v, ok := get(ap, name); ok {
		if m, ok := v.(map[string]any); ok {
			if val, ok := get(m, "value"); ok {
				return val, nil
			}
		}
	}
	dp, _ := props(e.a.def)["parameters"].(map[string]any)
	if v, ok := get(dp, name); ok {
		if m, ok := v.(map[string]any); ok {
			if def, ok := get(m, "defaultValue"); ok {
				return def, nil
			}
		}
	}
	return nil, fmt.Errorf("the parameter '%s' has no value", name)
}

// field reads a field of the resource: name, type, location, tags and the like, or an alias.
func (e *policyEval) field(name string) any {
	r := e.res
	switch low := strings.ToLower(name); low {
	case "name":
		return r["name"]
	case "fullname":
		if len(e.rid.Names) == 0 {
			return r["name"]
		}
		return strings.Join(e.rid.Names, "/")
	case "type", "kind", "tags", "id":
		v, _ := get(r, low)
		return v
	case "location":
		if l, ok := r["location"].(string); ok {
			return strings.ToLower(strings.ReplaceAll(l, " ", ""))
		}
		return nil
	case "identity.type":
		return walk(r, []string{"identity", "type"})
	}
	if tag, ok := tagField(name); ok {
		tags, _ := r["tags"].(map[string]any)
		v, _ := get(tags, tag)
		return v
	}
	typ := str(r["type"])
	if len(name) <= len(typ)+1 || !strings.EqualFold(name[:len(typ)+1], typ+"/") {
		return nil // an alias of another type, or a field pogi does not know: it doesn't exist here
	}
	rest := name[len(typ)+1:]
	if strings.Contains(rest, "/") {
		return nil // an alias of a child type
	}
	segs := strings.Split(rest, ".")
	switch strings.ToLower(strings.TrimSuffix(segs[0], "[*]")) {
	case "sku", "kind", "identity", "plan", "zones", "extendedlocation":
		return walk(r, segs)
	}
	return walk(r["properties"], segs)
}

// tagField reads tags['x'], tags[x] and tags.x.
func tagField(name string) (string, bool) {
	if len(name) < 6 || !strings.EqualFold(name[:4], "tags") {
		return "", false
	}
	rest := name[4:]
	switch {
	case rest[0] == '.':
		return rest[1:], true
	case rest[0] == '[' && rest[len(rest)-1] == ']':
		return strings.Trim(rest[1:len(rest)-1], "'"), true
	}
	return "", false
}

// walk follows the path of an alias. A segment that ends in [*] maps the rest over each element of its array.
func walk(v any, segs []string) any {
	if len(segs) == 0 {
		return v
	}
	name, all := strings.CutSuffix(segs[0], "[*]")
	m, _ := v.(map[string]any)
	x, _ := get(m, name)
	if !all {
		return walk(x, segs[1:])
	}
	l, ok := x.([]any)
	if !ok {
		return nil
	}
	out := wild{}
	for _, el := range l {
		if w, ok := walk(el, segs[1:]).(wild); ok {
			out = append(out, w...)
		} else {
			out = append(out, walk(el, segs[1:]))
		}
	}
	return out
}

// cond evaluates a condition of the rule.
func (e *policyEval) cond(c any) (bool, error) {
	m, ok := c.(map[string]any)
	if !ok {
		return false, fmt.Errorf("a condition must be an object")
	}
	if v, ok := get(m, "allOf"); ok {
		l, _ := v.([]any)
		for _, sub := range l {
			if ok, err := e.cond(sub); err != nil || !ok {
				return false, err
			}
		}
		return true, nil
	}
	if v, ok := get(m, "anyOf"); ok {
		l, _ := v.([]any)
		for _, sub := range l {
			if ok, err := e.cond(sub); err != nil || ok {
				return ok, err
			}
		}
		return false, nil
	}
	if v, ok := get(m, "not"); ok {
		ok, err := e.cond(v)
		return !ok, err
	}
	var subject any
	if f, ok := get(m, "field"); ok {
		name, err := e.env.value(f)
		if err != nil {
			return false, err
		}
		subject = e.field(fmt.Sprint(name))
	} else if v, ok := get(m, "value"); ok {
		var err error
		if subject, err = e.env.value(v); err != nil {
			return false, err
		}
	} else {
		return false, errNotEvaluated // count
	}
	for k, want := range m {
		if op := strings.ToLower(k); slices.Contains(policyOps, op) {
			w, err := e.env.value(want)
			if err != nil {
				return false, err
			}
			return policyTest(subject, op, w)
		}
	}
	return false, fmt.Errorf("the condition %s has no operator", compact(m))
}

// policyTest applies an operator. Strings compare without regard to case, as Azure Policy does.
func policyTest(v any, op string, want any) (bool, error) {
	if w, ok := v.(wild); ok {
		for _, el := range w {
			if ok, err := policyTest(el, op, want); err != nil || !ok {
				return false, err
			}
		}
		return true, nil
	}
	if base, ok := negated[op]; ok {
		ok, err := policyTest(v, base, want)
		return !ok && err == nil, err
	}
	s, isStr := v.(string)
	ws := fmt.Sprint(want)
	switch op {
	case "equals":
		return looseEqual(v, want), nil
	case "like":
		re := "(?is)^" + strings.ReplaceAll(regexp.QuoteMeta(ws), `\*`, ".*") + "$"
		return isStr && regexp.MustCompile(re).MatchString(s), nil
	case "match", "matchinsensitively":
		var b strings.Builder
		for _, r := range ws {
			switch r {
			case '#':
				b.WriteString(`\d`)
			case '?':
				b.WriteString(`\pL`)
			case '.':
				b.WriteString(`.`)
			default:
				b.WriteString(regexp.QuoteMeta(string(r)))
			}
		}
		flags := "(?s)"
		if op == "matchinsensitively" {
			flags = "(?is)"
		}
		return isStr && regexp.MustCompile(flags+"^"+b.String()+"$").MatchString(s), nil
	case "contains":
		if l, ok := v.([]any); ok {
			return slices.ContainsFunc(l, func(el any) bool { return looseEqual(el, want) }), nil
		}
		return isStr && strings.Contains(strings.ToLower(s), strings.ToLower(ws)), nil
	case "in":
		l, ok := want.([]any)
		if !ok {
			return false, fmt.Errorf("'in' needs a list, not %s", kind(want))
		}
		return slices.ContainsFunc(l, func(el any) bool { return looseEqual(v, el) }), nil
	case "containskey":
		m, _ := v.(map[string]any)
		_, ok := get(m, ws)
		return ok, nil
	case "less", "lessorequals", "greater", "greaterorequals":
		var c int
		if x, ok := toInt(v); ok {
			y, ok := toInt(want)
			if !ok {
				return false, nil
			}
			c = map[bool]int{true: -1, false: 1}[x < y]
			if x == y {
				c = 0
			}
		} else if isStr {
			c = strings.Compare(strings.ToLower(s), strings.ToLower(ws))
		} else {
			return false, nil
		}
		return map[string]bool{"less": c < 0, "lessorequals": c <= 0, "greater": c > 0, "greaterorequals": c >= 0}[op], nil
	case "exists":
		b, err := strconv.ParseBool(strings.ToLower(ws))
		if err != nil {
			return false, fmt.Errorf("'exists' takes true or false, not %v", want)
		}
		return (v != nil) == b, nil
	}
	return false, fmt.Errorf("'%s' is not an operator", op)
}

var negated = map[string]string{"notequals": "equals", "notlike": "like", "notmatch": "match", "notmatchinsensitively": "matchinsensitively",
	"notcontains": "contains", "notin": "in", "notcontainskey": "containskey"}

// looseEqual compares a field with a value: strings without regard to case, numbers by value, the rest as JSON.
func looseEqual(a, b any) bool {
	if x, ok := a.(string); ok {
		y, ok := b.(string)
		return ok && strings.EqualFold(x, y)
	}
	return jsonEqual(a, b)
}

// policyDoc is a resource as policy rules see it: the request body with its id, name and type.
func policyDoc(id ID, body map[string]any) map[string]any {
	doc := clone(body)
	doc["id"], doc["name"], doc["type"] = id.String(), id.Name(), id.Type()
	if id.Namespace == "" {
		doc["name"], doc["type"] = id.ResourceGroup, typGroup
	}
	return doc
}

// checkPolicy refuses a create or update that a deny assignment in scope matches.
func (s *Service) checkPolicy(c *core.Ctx, id ID, body map[string]any) error {
	if id.Subscription == "" || !isResource(id.Type()) {
		return nil
	}
	doc := policyDoc(id, body)
	var hits []any
	for _, a := range s.policies(c, id.Subscription) {
		if !a.covers(id.String()) || strings.EqualFold(str(props(a.doc)["enforcementMode"]), "DoNotEnforce") {
			continue
		}
		if match, effect, err := a.eval(s, c, id, doc); err == nil && match && effect == "deny" {
			hits = append(hits, map[string]any{
				"policyAssignment": map[string]any{"name": display(a.doc), "id": a.id()},
				"policyDefinition": map[string]any{"name": display(a.def), "id": a.defID()}})
		}
	}
	if hits == nil {
		return nil
	}
	return core.Errorf(http.StatusForbidden, "RequestDisallowedByPolicy", "Resource '%s' was disallowed by policy. Policy identifiers: '%s'.", str(doc["name"]), compact(hits))
}

// policyStates serves Microsoft.PolicyInsights policy states at a scope: queryResults, summarize and triggerEvaluation
// of .../providers/Microsoft.PolicyInsights/policyStates/{latest|default}/<action>.
func (s *Service) policyStates(k *call, scope, rest string) (int, any, error) {
	which, action, _ := strings.Cut(rest, "/")
	if k.r.Method != http.MethodPost {
		return 0, nil, methodNotAllowed(k.r.Method, "Microsoft.PolicyInsights/policyStates")
	}
	if !strings.EqualFold(which, "latest") && !strings.EqualFold(which, "default") {
		return 0, nil, core.Errorf(http.StatusBadRequest, "InvalidPolicyStatesResource", "The policy states resource '%s' is not valid: use latest or default.", which)
	}
	sid, _ := ParseID(scope)
	sub := sid.Subscription
	if sub == "" {
		if _, after, ok := strings.Cut(strings.ToLower(scope), "/subscriptions/"); ok {
			sub, _, _ = strings.Cut(after, "/")
		}
	}
	ctx := k.base + "/subscriptions/" + sub + "/providers/Microsoft.PolicyInsights/policyStates/$metadata"
	switch strings.ToLower(action) {
	case "triggerevaluation": // states are worked out when asked: there is nothing to scan
		k.async(sub)
		return http.StatusAccepted, nil, nil
	case "queryresults":
		q, _ := url.ParseQuery(k.c.RawQuery)
		states, err := filter(s.states(k.c, sub, scope), q.Get("$filter"))
		if err != nil {
			return 0, nil, err
		}
		if t := q.Get("$top"); t != "" {
			n, err := strconv.Atoi(t)
			if err != nil || n < 0 {
				return 0, nil, core.Errorf(http.StatusBadRequest, "InvalidQueryParameterValue", "$top must be a whole number, not '%s'.", t)
			}
			states = states[:min(n, len(states))]
		}
		for _, st := range states {
			st["@odata.context"] = ctx + "#" + which + "/$entity"
		}
		return http.StatusOK, map[string]any{"@odata.context": ctx + "#" + which, "@odata.count": len(states), "value": nonNil(states)}, nil
	case "summarize":
		states := s.states(k.c, sub, scope)
		var byAssignment []any
		var ids []string
		for _, st := range states {
			if id := str(st["policyAssignmentId"]); !slices.Contains(ids, id) {
				ids = append(ids, id)
			}
		}
		for _, id := range ids {
			mine := slices.DeleteFunc(slices.Clone(states), func(st map[string]any) bool { return st["policyAssignmentId"] != id })
			byAssignment = append(byAssignment, map[string]any{"policyAssignmentId": id, "policySetDefinitionId": "",
				"results": summary(mine), "policyGroups": []any{}, "policyDefinitions": []any{map[string]any{
					"policyDefinitionReferenceId": "", "policyDefinitionId": mine[0]["policyDefinitionId"],
					"effect": mine[0]["policyDefinitionAction"], "results": summary(mine)}}})
		}
		return http.StatusOK, map[string]any{"@odata.context": ctx + "#summary", "@odata.count": 1, "value": []any{map[string]any{
			"@odata.id": nil, "@odata.context": ctx + "#summary/$entity", "results": summary(states),
			"policyAssignments": nonNil(byAssignment)}}}, nil
	}
	return 0, nil, core.Errorf(http.StatusNotFound, "InvalidRequestUri", "No route registered for '%s'.", k.path())
}

func nonNil[T any](l []T) []T {
	if l == nil {
		return []T{}
	}
	return l
}

// summary counts states as policy states' summarize does.
func summary(states []map[string]any) map[string]any {
	res, pol := map[string]string{}, map[string]string{}
	for _, st := range states {
		r, p := str(st["resourceId"]), str(st["policyAssignmentId"])
		if st["complianceState"] == "NonCompliant" {
			res[r], pol[p] = "noncompliant", "noncompliant"
		} else {
			res[r] = cmpOr(res[r], "compliant")
			pol[p] = cmpOr(pol[p], "compliant")
		}
	}
	count := func(m map[string]string) []any {
		var out []any
		for _, state := range []string{"compliant", "noncompliant"} {
			n := 0
			for _, v := range m {
				if v == state {
					n++
				}
			}
			out = append(out, map[string]any{"complianceState": state, "count": n})
		}
		return out
	}
	nonComp := func(m map[string]string) int {
		n := 0
		for _, v := range m {
			if v == "noncompliant" {
				n++
			}
		}
		return n
	}
	return map[string]any{"queryResultsUri": "", "nonCompliantResources": nonComp(res), "nonCompliantPolicies": nonComp(pol),
		"resourceDetails": count(res), "policyDetails": count(pol), "policyGroupDetails": []any{}}
}

func cmpOr(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

// states evaluates the deny and audit assignments of a subscription against its resources and groups, keeping
// those within scope: the subscription, a group, a resource, or one assignment.
func (s *Service) states(c *core.Ctx, sub, scope string) []map[string]any {
	if sub == "" {
		return nil
	}
	only := ""
	if strings.Contains(strings.ToLower(scope), "/providers/microsoft.authorization/policyassignments/") {
		only, scope = scope, "/subscriptions/"+sub
	}
	resources := s.allResources(c, ID{Subscription: sub})
	for _, g := range s.listGroups(sub) {
		resources = append(resources, policyDoc(ID{Subscription: sub, ResourceGroup: str(g["name"])}, g))
	}
	now := time.Now().UTC().Format(time.RFC3339)
	var out []map[string]any
	for _, a := range s.policies(c, sub) {
		if only != "" && !strings.EqualFold(a.id(), only) {
			continue
		}
		for _, r := range resources {
			rid, ok := ParseID(str(r["id"]))
			if !ok || !within(rid.String(), scope) || !a.covers(rid.String()) {
				continue
			}
			match, effect, err := a.eval(s, c, rid, r)
			if err != nil || effect != "deny" && effect != "audit" {
				continue
			}
			state := "Compliant"
			if match {
				state = "NonCompliant"
			}
			params, _ := props(a.doc)["parameters"].(map[string]any)
			out = append(out, map[string]any{"@odata.id": nil, "timestamp": now, "resourceId": rid.String(),
				"policyAssignmentId": a.id(), "policyDefinitionId": a.defID(), "isCompliant": !match,
				"complianceState": state, "subscriptionId": sub, "resourceType": r["type"], "resourceLocation": r["location"],
				"resourceGroup": rid.ResourceGroup, "resourceTags": compact(r["tags"]), "policyAssignmentName": a.doc["name"],
				"policyAssignmentOwner": "tbd", "policyAssignmentParameters": compact(params), "policyAssignmentScope": a.scope(),
				"policyDefinitionName": a.def["name"], "policyDefinitionAction": effect, "policyDefinitionCategory": "tbd",
				"policySetDefinitionId": "", "policySetDefinitionName": "", "policyDefinitionReferenceId": "",
				"managementGroupIds": "", "effectiveParameters": "", "policyDefinitionGroupNames": []any{""}})
		}
	}
	return out
}
