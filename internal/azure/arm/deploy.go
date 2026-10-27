package arm

// ARM template deployments run. A PUT of Microsoft.Resources/deployments at resource group or subscription scope
// checks the template (parameters, variables, names, dependsOn), then creates its resources one at a time in
// dependsOn order, each with an in-process PUT through the Resource Manager, so every Provider, parent check and
// policy applies as it does to a client. The deployment ends Succeeded, or Failed at the first resource that
// fails, with an operation per resource as Azure lists them; what was created stays (Azure does not roll back
// either). POST .../validate runs the checks alone; nothing is created. Nested deployments (Bicep modules) are
// deployments too, run the same way.
//
// ponytail: one resource at a time, inside the PUT, which answers when the deployment has finished. No linked
// templates or template specs, no Complete mode, no user-defined functions, no Key Vault parameter references,
// no management group or tenant scope.

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
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
	typDeployment = "Microsoft.Resources/deployments"
	depthHeader   = "X-Pogi-Deployment-Depth" // how deep in nested deployments an in-process PUT is
	maxResources  = 800                       // Azure's limit on the resources of a template, copies included
	maxDepth      = 10                        // nested deployments, deepest
)

// templateKeys are the members of a template resource that describe the template, not the resource.
var templateKeys = []string{"type", "apiVersion", "name", "dependsOn", "copy", "condition", "resources", "comments", "scope",
	"existing", "resourceGroup", "subscriptionId"}

var deploymentName = regexp.MustCompile(`^[-\w.()]{1,64}$`)

func isDeployment(id ID) bool { return strings.EqualFold(id.Type(), typDeployment) }

// invalid is the error Azure answers for a template that fails its checks.
func invalid(f string, a ...any) *core.Error {
	return core.Errorf(http.StatusBadRequest, "InvalidTemplate", "Deployment template validation failed: '"+f+"'.", a...)
}

// tmplRes is one resource of the template, one per copy.
type tmplRes struct {
	sym       string // symbolic name (languageVersion 2.0)
	loop      string // the copy loop it is part of
	raw       map[string]any
	parent    *tmplRes // the resource a nested one sits in
	typ, name string   // full type and name
	api, id   string
	ctx       *evalCtx
	deps      []*tmplRes
	existing  bool // languageVersion 2.0: a resource that is only referred to
	skipped   bool // its condition was false
}

// evalCtx is what copyIndex() reads: the copy loops being worked on.
type evalCtx struct {
	loops map[string]int // lower-cased loop name -> index
	cur   string         // the innermost loop
}

func (c *evalCtx) with(loop string, i int) *evalCtx {
	n := &evalCtx{loops: map[string]int{}, cur: strings.ToLower(loop)}
	for k, v := range c.loops {
		n.loops[k] = v
	}
	n.loops[n.cur] = i
	return n
}

type deployment struct {
	s      *Service
	k      *call
	id     ID // the deployment
	scope  ID // where its resources go: a group, or the subscription
	group  map[string]any
	props  map[string]any // the deployment's properties as sent
	tmpl   map[string]any
	in     map[string]any // parameter values sent, by name
	pdecl  map[string]any
	vdecl  map[string]any
	params map[string]any // resolved, by lower-cased name
	vars   map[string]any
	busy   map[string]bool
	res    []*tmplRes // in the order they run
	depth  int
}

// newDeployment reads a deployment request: refused with a 400 when it asks for what pogi does not do.
func (s *Service) newDeployment(k *call, id ID, body map[string]any) (*deployment, error) {
	if !deploymentName.MatchString(id.Name()) {
		return nil, core.Errorf(http.StatusBadRequest, "InvalidDeploymentName", "The deployment name '%s' is not valid: use 1 to 64 letters, digits and - _ . ( ).", id.Name())
	}
	if id.Subscription == "" || len(id.Types) != 1 {
		return nil, core.Errorf(http.StatusBadRequest, "InvalidDeploymentScope", "pogi runs deployments at resource group and subscription scope only.")
	}
	d := &deployment{s: s, k: k, id: id, scope: ID{Subscription: id.Subscription, ResourceGroup: id.ResourceGroup},
		params: map[string]any{}, vars: map[string]any{}, busy: map[string]bool{}}
	d.depth, _ = strconv.Atoi(k.r.Header.Get(depthHeader))
	d.depth = max(d.depth, 0)
	if d.depth >= maxDepth {
		return nil, invalid("deployments are nested more than %d deep", maxDepth)
	}
	d.props, _ = body["properties"].(map[string]any)
	if d.props == nil {
		return nil, core.Errorf(http.StatusBadRequest, "InvalidRequestContent", "The deployment has no properties.")
	}
	if m := str(d.props["mode"]); m != "" && !strings.EqualFold(m, "Incremental") {
		return nil, core.Errorf(http.StatusBadRequest, "InvalidDeploymentMode", "pogi runs Incremental deployments only, not '%s' (Complete mode deletes what the template leaves out).", m)
	}
	if d.props["templateLink"] != nil || d.props["parametersLink"] != nil {
		return nil, core.Errorf(http.StatusBadRequest, "InvalidTemplateDeployment", "pogi does not fetch linked templates or template specs: send the template in the request (az deployment group create --template-file does).")
	}
	if d.tmpl, _ = d.props["template"].(map[string]any); d.tmpl == nil {
		return nil, invalid("the deployment has no template")
	}
	if f, _ := d.tmpl["functions"].([]any); len(f) > 0 {
		return nil, invalid("pogi does not support user-defined functions")
	}
	d.pdecl, _ = d.tmpl["parameters"].(map[string]any)
	d.vdecl, _ = d.tmpl["variables"].(map[string]any)
	d.in = map[string]any{}
	sent, _ := d.props["parameters"].(map[string]any)
	for name, v := range sent {
		m, ok := v.(map[string]any)
		switch {
		case !ok:
			return nil, invalid("the parameter '%s' must be an object with a value", name)
		case m["reference"] != nil:
			return nil, invalid("the parameter '%s' is a Key Vault reference, which pogi does not resolve: send its value", name)
		}
		if _, ok := get(d.pdecl, name); !ok {
			return nil, invalid("the template parameter '%s' was sent, but the template does not declare it", name)
		}
		d.in[strings.ToLower(name)] = m["value"]
	}
	if d.scope.ResourceGroup != "" {
		s.mu.Lock()
		d.group = s.st.Groups[groupKey(d.scope.Subscription, d.scope.ResourceGroup)]
		s.mu.Unlock()
	}
	return d, nil
}

// deployFuncs are the template functions a deployment answers itself, besides exprFuncs and list*.
var deployFuncs = []string{"parameters", "variables", "copyindex", "resourceid", "subscriptionresourceid", "tenantresourceid",
	"extensionresourceid", "resourcegroup", "subscription", "tenant", "deployment", "environment", "managementgroup",
	"pickzones", "reference", "if"}

// checkExprs refuses an expression anywhere in the template that does not parse or calls a function pogi lacks,
// before anything is made.
func checkExprs(v any) error {
	var walk func(n any) error
	walk = func(n any) error {
		switch n := n.(type) {
		case *member:
			if err := walk(n.x); err != nil {
				return err
			}
			return walk(n.key)
		case *fcall:
			if exprFuncs[n.name] == nil && !slices.Contains(deployFuncs, n.name) && (!strings.HasPrefix(n.name, "list") || n.name == "list") {
				return fmt.Errorf("the template function '%s' is not valid, or pogi does not support it", n.name)
			}
			for _, a := range n.args {
				if err := walk(a); err != nil {
					return err
				}
			}
		}
		return nil
	}
	switch x := v.(type) {
	case string:
		if isExpr(x) {
			n, err := parseExpr(x)
			if err != nil {
				return err
			}
			return walk(n)
		}
	case []any:
		for _, e := range x {
			if err := checkExprs(e); err != nil {
				return err
			}
		}
	case map[string]any:
		for _, k := range core.SortedKeys(x) {
			if err := checkExprs(x[k]); err != nil {
				return err
			}
		}
	}
	return nil
}

// check resolves the parameters and lays out the resources, their IDs and their order.
func (d *deployment) check() error {
	if err := checkExprs(d.tmpl); err != nil {
		return invalid("%v", err)
	}
	for _, name := range core.SortedKeys(d.pdecl) {
		if _, err := d.param(name); err != nil {
			return invalid("%v", err)
		}
	}
	if err := d.plan(); err != nil {
		return invalid("%v", err)
	}
	return nil
}

// ---- values ----

func (d *deployment) env(c *evalCtx) *exprEnv {
	return &exprEnv{fn: func(name string, args []any) (any, bool, error) { return d.fn(c, name, args) }}
}

// value evaluates a template value: expressions, in arrays and objects too, and the property copy loops of an
// object ("copy": [{"name", "count", "input"}]).
func (d *deployment) value(v any, c *evalCtx) (any, error) {
	switch x := v.(type) {
	case string:
		if strings.HasPrefix(x, "[[") {
			return x[1:], nil
		}
		if !isExpr(x) {
			return x, nil
		}
		n, err := parseExpr(x)
		if err != nil {
			return nil, err
		}
		return d.env(c).eval(n)
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			var err error
			if out[i], err = d.value(e, c); err != nil {
				return nil, err
			}
		}
		return out, nil
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, e := range x {
			if loops, ok := copyLoops(k, e); ok {
				for _, l := range loops {
					arr, err := d.loop(l, c)
					if err != nil {
						return nil, err
					}
					out[str(l["name"])] = arr
				}
				continue
			}
			var err error
			if out[k], err = d.value(e, c); err != nil {
				return nil, err
			}
		}
		return out, nil
	}
	return v, nil
}

// copyLoops reads "copy": [{"name", "count", "input"}], the property loops of an object.
func copyLoops(k string, v any) ([]map[string]any, bool) {
	l, ok := v.([]any)
	if k != "copy" || !ok || len(l) == 0 {
		return nil, false
	}
	var out []map[string]any
	for _, e := range l {
		m, ok := e.(map[string]any)
		if !ok || str(m["name"]) == "" || m["count"] == nil || m["input"] == nil {
			return nil, false
		}
		out = append(out, m)
	}
	return out, true
}

// loop evaluates a property or variable copy loop to its array.
func (d *deployment) loop(l map[string]any, c *evalCtx) ([]any, error) {
	n, err := d.count(l["count"], c)
	if err != nil {
		return nil, err
	}
	out := make([]any, n)
	for i := range out {
		if out[i], err = d.value(l["input"], c.with(str(l["name"]), i)); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (d *deployment) count(v any, c *evalCtx) (int, error) {
	cv, err := d.value(v, c)
	if err != nil {
		return 0, err
	}
	n, ok := toInt(cv)
	if !ok || n < 0 || n > maxResources {
		return 0, fmt.Errorf("a copy count must be a whole number from 0 to %d, not %v", maxResources, cv)
	}
	return int(n), nil
}

// param resolves a parameter: the value sent, else the default (an expression, perhaps of other parameters).
func (d *deployment) param(name string) (any, error) {
	key := strings.ToLower(name)
	if v, ok := d.params[key]; ok {
		return v, nil
	}
	decl, ok := get(d.pdecl, name)
	dm, _ := decl.(map[string]any)
	if !ok || dm == nil {
		return nil, fmt.Errorf("the template parameter '%s' is not found", name)
	}
	if d.busy["p:"+key] {
		return nil, fmt.Errorf("the parameter '%s' refers to itself", name)
	}
	d.busy["p:"+key] = true
	defer delete(d.busy, "p:"+key)
	v, sent := d.in[key]
	if !sent {
		def, ok := get(dm, "defaultValue")
		if !ok {
			return nil, fmt.Errorf("the value for the template parameter '%s' is not provided", name)
		}
		var err error
		if v, err = d.value(def, &evalCtx{}); err != nil {
			return nil, fmt.Errorf("the default of parameter '%s': %v", name, err)
		}
	}
	if err := checkParam(name, dm, v); err != nil {
		return nil, err
	}
	d.params[key] = v
	return v, nil
}

// checkParam checks a parameter's value against its declaration.
func checkParam(name string, decl map[string]any, v any) error {
	typ := strings.ToLower(str(decl["type"]))
	if !typeOK(typ, v) {
		if _, known := outputTypes[typ]; !known {
			return fmt.Errorf("the parameter '%s' has the type '%s', which is not a template type", name, decl["type"])
		}
		return fmt.Errorf("the template parameter '%s' needs a value of type '%s', not %s", name, decl["type"], kind(v))
	}
	if allowed, ok := decl["allowedValues"].([]any); ok {
		vals := []any{v}
		if l, ok := v.([]any); ok && typ == "array" {
			vals = l
		}
		for _, x := range vals {
			if !slices.ContainsFunc(allowed, func(a any) bool { return jsonEqual(a, x) }) {
				return fmt.Errorf("the value '%v' of parameter '%s' is not one of its allowed values %s", x, name, compact(allowed))
			}
		}
	}
	n, isNum := toInt(v)
	if s, ok := v.(string); ok {
		n, isNum = int64(len([]rune(s))), false
		if lo, ok := toInt(decl["minLength"]); ok && n < lo {
			return fmt.Errorf("the parameter '%s' must be at least %d characters long", name, lo)
		}
		if hi, ok := toInt(decl["maxLength"]); ok && n > hi {
			return fmt.Errorf("the parameter '%s' must be at most %d characters long", name, hi)
		}
	}
	if l, ok := v.([]any); ok {
		if lo, ok := toInt(decl["minLength"]); ok && int64(len(l)) < lo {
			return fmt.Errorf("the parameter '%s' must have at least %d items", name, lo)
		}
		if hi, ok := toInt(decl["maxLength"]); ok && int64(len(l)) > hi {
			return fmt.Errorf("the parameter '%s' must have at most %d items", name, hi)
		}
	}
	if isNum && typ == "int" {
		if lo, ok := toInt(decl["minValue"]); ok && n < lo {
			return fmt.Errorf("the parameter '%s' must be at least %d", name, lo)
		}
		if hi, ok := toInt(decl["maxValue"]); ok && n > hi {
			return fmt.Errorf("the parameter '%s' must be at most %d", name, hi)
		}
	}
	return nil
}

// outputTypes are the template types, with the names Azure reports them by.
var outputTypes = map[string]string{"string": "String", "securestring": "SecureString", "int": "Int", "bool": "Bool",
	"object": "Object", "secureobject": "SecureObject", "array": "Array"}

func typeOK(typ string, v any) bool {
	switch typ {
	case "string", "securestring":
		_, ok := v.(string)
		return ok
	case "int":
		_, ok := toInt(v)
		return ok
	case "bool":
		_, ok := v.(bool)
		return ok
	case "object", "secureobject":
		_, ok := v.(map[string]any)
		return ok
	case "array":
		_, ok := v.([]any)
		return ok
	}
	return false
}

// variable resolves a variable, or a variable copy loop.
func (d *deployment) variable(name string) (any, error) {
	key := strings.ToLower(name)
	if v, ok := d.vars[key]; ok {
		return v, nil
	}
	if d.busy["v:"+key] {
		return nil, fmt.Errorf("the variable '%s' refers to itself", name)
	}
	d.busy["v:"+key] = true
	defer delete(d.busy, "v:"+key)
	var v any
	var err error
	if decl, ok := get(d.vdecl, name); ok && key != "copy" {
		v, err = d.value(decl, &evalCtx{})
	} else if loops, ok := copyLoops("copy", d.vdecl["copy"]); ok && slices.ContainsFunc(loops, func(l map[string]any) bool { return strings.EqualFold(str(l["name"]), name) }) {
		i := slices.IndexFunc(loops, func(l map[string]any) bool { return strings.EqualFold(str(l["name"]), name) })
		v, err = d.loop(loops[i], &evalCtx{})
	} else {
		return nil, fmt.Errorf("the template variable '%s' is not found", name)
	}
	if err != nil {
		return nil, fmt.Errorf("the variable '%s': %v", name, err)
	}
	d.vars[key] = v
	return v, nil
}

// fn answers the template functions that need the deployment.
func (d *deployment) fn(c *evalCtx, name string, args []any) (any, bool, error) {
	arg := func(i int) (string, error) {
		if i >= len(args) {
			return "", fmt.Errorf("%s(): argument %d is missing", name, i+1)
		}
		s, ok := args[i].(string)
		if !ok {
			return "", fmt.Errorf("%s(): argument %d must be a string, not %s", name, i+1, kind(args[i]))
		}
		return s, nil
	}
	strs := func(from int) ([]string, error) {
		var out []string
		for i := from; i < len(args); i++ {
			s, err := arg(i)
			if err != nil {
				return nil, err
			}
			out = append(out, s)
		}
		return out, nil
	}
	switch name {
	case "parameters", "variables":
		n, err := arg(0)
		if err != nil {
			return nil, true, err
		}
		if name == "parameters" {
			v, err := d.param(n)
			return v, true, err
		}
		v, err := d.variable(n)
		return v, true, err
	case "copyindex":
		loop, off := c.cur, int64(0)
		for _, a := range args {
			if s, ok := a.(string); ok {
				loop = strings.ToLower(s)
			} else if n, ok := toInt(a); ok {
				off = n
			}
		}
		i, ok := c.loops[loop]
		if !ok {
			return nil, true, fmt.Errorf("copyIndex() is used outside a copy loop, or names a loop '%s' that is not running", loop)
		}
		return int64(i) + off, true, nil
	case "resourceid", "subscriptionresourceid", "tenantresourceid":
		all, err := strs(0)
		if err != nil {
			return nil, true, err
		}
		t := slices.IndexFunc(all, func(s string) bool { return strings.Contains(s, "/") })
		if t < 0 {
			return nil, true, fmt.Errorf("%s(): no resource type is given", name)
		}
		sub, rg := d.scope.Subscription, d.scope.ResourceGroup
		switch {
		case name == "tenantresourceid":
			sub, rg = "", ""
		case name == "subscriptionresourceid" && t == 1:
			sub, rg = all[0], ""
		case name == "subscriptionresourceid":
			rg = ""
		case t == 1:
			rg = all[0]
		case t == 2:
			sub, rg = all[0], all[1]
		}
		if t > 2 || name == "subscriptionresourceid" && t > 1 || name == "tenantresourceid" && t > 0 {
			return nil, true, fmt.Errorf("%s(): too many arguments before the resource type", name)
		}
		id, err := resID(sub, rg, all[t], all[t+1:])
		return id, true, err
	case "extensionresourceid":
		all, err := strs(0)
		if err != nil || len(all) < 3 {
			return nil, true, fmt.Errorf("extensionResourceId() takes a resource ID, a type and names")
		}
		id, err := extID(all[0], all[1], all[2:])
		return id, true, err
	case "resourcegroup":
		if d.scope.ResourceGroup == "" {
			return nil, true, fmt.Errorf("resourceGroup() can't be used in a deployment at subscription scope")
		}
		g := map[string]any{"id": d.scope.String(), "name": d.scope.ResourceGroup, "type": "Microsoft.Resources/resourceGroups",
			"properties": map[string]any{"provisioningState": "Succeeded"}}
		if d.group != nil {
			g["name"], g["location"], g["tags"], g["managedBy"] = d.group["name"], d.group["location"], d.group["tags"], d.group["managedBy"]
		}
		return g, true, nil
	case "subscription":
		return map[string]any{"id": "/subscriptions/" + d.scope.Subscription, "subscriptionId": d.scope.Subscription,
			"tenantId": TenantID, "displayName": d.s.subName(d.scope.Subscription)}, true, nil
	case "tenant":
		return tenant(), true, nil
	case "deployment":
		ver := str(d.tmpl["contentVersion"])
		return map[string]any{"name": d.id.Name(), "properties": map[string]any{"template": map[string]any{"contentVersion": ver},
			"templateHash": d.hash(), "parameters": d.props["parameters"], "mode": "Incremental", "provisioningState": "Running"}}, true, nil
	case "environment":
		return metadata(d.k), true, nil
	case "managementgroup":
		return nil, true, fmt.Errorf("managementGroup() needs a management group deployment, which pogi does not run")
	case "pickzones":
		return []any{}, true, nil
	case "reference":
		target, err := arg(0)
		if err != nil {
			return nil, true, err
		}
		id, api := d.resolve(target), "2022-09-01"
		if len(args) > 1 {
			if api, err = arg(1); err != nil {
				return nil, true, err
			}
		}
		st, out := d.send(http.MethodGet, id, api, nil)
		if st != http.StatusOK {
			return nil, true, fmt.Errorf("reference(): the resource '%s' was not found (%d %s)", id, st, errText(out))
		}
		if len(args) > 2 && strings.EqualFold(fmt.Sprint(args[2]), "Full") {
			return out, true, nil
		}
		p, _ := out["properties"].(map[string]any)
		if p == nil {
			p = map[string]any{}
		}
		return p, true, nil
	}
	if action, ok := strings.CutPrefix(name, "list"); ok && action != "" {
		id, err := arg(0)
		if err != nil {
			return nil, true, err
		}
		api, err := arg(1)
		if err != nil {
			return nil, true, err
		}
		var body any
		if len(args) > 2 {
			body = args[2]
		}
		st, out := d.send(http.MethodPost, d.resolve(id)+"/"+name, api, body)
		if st >= 300 {
			return nil, true, fmt.Errorf("%s(): %d %s", name, st, errText(out))
		}
		return out, true, nil
	}
	return nil, false, nil
}

// resolve turns the name of a resource of the template (its symbolic name, or its name) into its ID; anything
// else is taken as an ID.
func (d *deployment) resolve(target string) string {
	if !strings.HasPrefix(target, "/") {
		for _, r := range d.res {
			if r.sym != "" && strings.EqualFold(r.sym, target) || strings.EqualFold(r.name, target) {
				return r.id
			}
		}
	}
	return target
}

// resID builds a resource ID at a group, a subscription (rg empty) or the tenant (both empty).
func resID(sub, rg, typ string, names []string) (string, error) {
	if strings.EqualFold(typ, "Microsoft.Resources/resourceGroups") && len(names) == 1 {
		return "/subscriptions/" + sub + "/resourceGroups/" + names[0], nil
	}
	base := ""
	if sub != "" {
		base = "/subscriptions/" + sub
		if rg != "" {
			base += "/resourceGroups/" + rg
		}
	}
	return extID(base, typ, names)
}

// extID builds <base>/providers/<namespace>/<type>/<name>[/<type>/<name>...].
func extID(base, typ string, names []string) (string, error) {
	ns, rest, ok := strings.Cut(typ, "/")
	if !ok || ns == "" || rest == "" {
		return "", fmt.Errorf("'%s' is not a resource type", typ)
	}
	types := strings.Split(rest, "/")
	if len(names) == 1 && len(types) > 1 {
		names = strings.Split(names[0], "/")
	}
	if len(names) != len(types) {
		return "", fmt.Errorf("the resource type '%s' needs %d name segments, not %d (%s)", typ, len(types), len(names), strings.Join(names, "/"))
	}
	var b strings.Builder
	b.WriteString(base + "/providers/" + ns)
	for i, t := range types {
		if names[i] == "" {
			return "", fmt.Errorf("the resource of type '%s' has an empty name segment", typ)
		}
		b.WriteString("/" + t + "/" + names[i])
	}
	return b.String(), nil
}

// ---- the plan ----

// plan expands the resources (nested ones and copies), works out their IDs and puts them in dependsOn order.
func (d *deployment) plan() error {
	type entry struct {
		sym string
		r   map[string]any
	}
	var entries []entry
	switch x := d.tmpl["resources"].(type) {
	case nil:
	case []any:
		for i, e := range x {
			r, ok := e.(map[string]any)
			if !ok {
				return fmt.Errorf("resource %d is not an object", i+1)
			}
			entries = append(entries, entry{"", r})
		}
	case map[string]any: // languageVersion 2.0: symbolic name -> resource
		for _, sym := range core.SortedKeys(x) {
			r, ok := x[sym].(map[string]any)
			if !ok {
				return fmt.Errorf("the resource '%s' is not an object", sym)
			}
			entries = append(entries, entry{sym, r})
		}
	default:
		return fmt.Errorf("'resources' must be a list")
	}
	var all []*tmplRes
	for _, e := range entries {
		if err := d.expand(&all, e.sym, e.r, nil); err != nil {
			return err
		}
	}
	d.res = all // resolve() reads them from here on
	for _, t := range all {
		if t.parent != nil {
			t.deps = append(t.deps, t.parent)
		}
		deps, ok := t.raw["dependsOn"].([]any)
		if t.raw["dependsOn"] != nil && !ok {
			return fmt.Errorf("dependsOn of '%s' must be a list", t.name)
		}
		for _, dep := range deps {
			v, err := d.value(dep, t.ctx)
			if err != nil {
				return fmt.Errorf("dependsOn of '%s': %v", t.name, err)
			}
			ref, _ := v.(string)
			found := false
			for _, u := range all {
				if u != t && (strings.EqualFold(u.id, ref) || u.loop != "" && strings.EqualFold(u.loop, ref) || u.sym != "" && strings.EqualFold(u.sym, ref) ||
					strings.EqualFold(u.name, ref) || strings.EqualFold(u.typ+"/"+u.name, ref)) {
					t.deps, found = append(t.deps, u), true
				}
			}
			if !found {
				return fmt.Errorf("the resource '%v' is not defined in the template", v)
			}
		}
	}
	// dependsOn order, the template's order where it leaves a choice
	done := map[*tmplRes]bool{}
	d.res = nil
	for len(d.res) < len(all) {
		progressed := false
		for _, t := range all {
			if !done[t] && !slices.ContainsFunc(t.deps, func(u *tmplRes) bool { return !done[u] }) {
				d.res, done[t], progressed = append(d.res, t), true, true
			}
		}
		if !progressed {
			var names []string
			for _, t := range all {
				if !done[t] {
					names = append(names, t.name)
				}
			}
			return fmt.Errorf("circular dependency detected on resources: %s", strings.Join(names, ", "))
		}
	}
	return nil
}

// expand adds a template resource: one per copy, and its nested resources after it.
func (d *deployment) expand(all *[]*tmplRes, sym string, r map[string]any, parent *tmplRes) error {
	base := &evalCtx{loops: map[string]int{}}
	if parent != nil {
		base = parent.ctx
	}
	typ, err := d.value(r["type"], base)
	if err != nil {
		return err
	}
	ts, _ := typ.(string)
	if ts == "" {
		return fmt.Errorf("a resource has no type")
	}
	if parent != nil && !strings.Contains(strings.SplitN(ts, "/", 2)[0], ".") { // a nested type is relative to its parent
		ts = parent.typ + "/" + ts
	}
	count, loop := 1, ""
	if cp, ok := r["copy"].(map[string]any); ok {
		if loop = str(cp["name"]); loop == "" {
			return fmt.Errorf("the copy loop of a resource of type '%s' has no name", ts)
		}
		if count, err = d.count(cp["count"], base); err != nil {
			return fmt.Errorf("the copy loop '%s': %v", loop, err)
		}
	}
	for i := range count {
		c := base
		if loop != "" {
			c = base.with(loop, i)
		}
		t := &tmplRes{sym: sym, loop: loop, raw: r, parent: parent, typ: ts, ctx: c}
		t.existing, _ = r["existing"].(bool)
		name, err := d.value(r["name"], c)
		if err != nil {
			return fmt.Errorf("the name of a resource of type '%s': %v", ts, err)
		}
		if t.name, _ = name.(string); t.name == "" {
			return fmt.Errorf("a resource of type '%s' has no name", ts)
		}
		if parent != nil && !strings.HasPrefix(strings.ToLower(t.name), strings.ToLower(parent.name)+"/") {
			t.name = parent.name + "/" + t.name
		}
		api, err := d.value(r["apiVersion"], c)
		if err != nil {
			return err
		}
		if t.api, _ = api.(string); t.api == "" {
			return fmt.Errorf("the resource '%s' has no apiVersion", t.name)
		}
		if t.id, err = d.resourceID(t); err != nil {
			return fmt.Errorf("the resource '%s': %v", t.name, err)
		}
		if *all = append(*all, t); len(*all) > maxResources {
			return fmt.Errorf("the template has more than %d resources", maxResources)
		}
		kids, _ := r["resources"].([]any)
		for _, k := range kids {
			km, ok := k.(map[string]any)
			if !ok {
				return fmt.Errorf("a nested resource of '%s' is not an object", t.name)
			}
			if err := d.expand(all, "", km, t); err != nil {
				return err
			}
		}
	}
	return nil
}

// resourceID is where a template resource goes: its scope's, or a nested deployment's group or subscription.
func (d *deployment) resourceID(t *tmplRes) (string, error) {
	names := strings.Split(t.name, "/")
	if strings.EqualFold(t.typ, typDeployment) {
		sub, rg := d.scope.Subscription, d.scope.ResourceGroup
		if v, ok := t.raw["subscriptionId"]; ok {
			s, err := d.value(v, t.ctx)
			if err != nil {
				return "", err
			}
			if s := fmt.Sprint(s); !strings.EqualFold(s, sub) {
				sub, rg = s, ""
			}
		}
		if v, ok := t.raw["resourceGroup"]; ok {
			g, err := d.value(v, t.ctx)
			if err != nil {
				return "", err
			}
			rg = fmt.Sprint(g)
		}
		return resID(sub, rg, t.typ, names)
	}
	if v, ok := t.raw["scope"]; ok {
		sc, err := d.value(v, t.ctx)
		if err != nil {
			return "", err
		}
		base := fmt.Sprint(sc)
		if !strings.HasPrefix(base, "/") { // Bicep's form: <namespace>/<type>/<name>, in the deployment's scope
			base = d.scope.String() + "/providers/" + base
		}
		return extID(strings.TrimSuffix(base, "/"), t.typ, names)
	}
	return resID(d.scope.Subscription, d.scope.ResourceGroup, t.typ, names)
}

// ---- running ----

// send makes an in-process ARM request as the caller, one level deeper in nested deployments.
func (d *deployment) send(method, path, api string, body any) (int, map[string]any) {
	var b []byte
	if body != nil {
		b, _ = json.Marshal(body)
	}
	req, _ := http.NewRequest(method, "http://localhost/", bytes.NewReader(b))
	req.URL.Path, req.URL.RawQuery = path, url.Values{"api-version": {api}}.Encode()
	req.Host = d.k.r.Host
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", d.k.r.Header.Get("Authorization"))
	req.Header.Set(depthHeader, strconv.Itoa(d.depth+1))
	st, out := d.s.h.DoService("arm", req)
	var m map[string]any
	json.Unmarshal(out, &m)
	return st, m
}

func errText(out map[string]any) string {
	if e, ok := out["error"].(map[string]any); ok {
		return fmt.Sprintf("%v: %v", e["code"], e["message"])
	}
	return compact(out)
}

// body is what a template resource sends: the resource without the template's own members, evaluated. A nested
// deployment's template is left for that deployment to evaluate; with the outer scope it gets this one's
// parameters and variables as its own.
func (d *deployment) body(t *tmplRes) (map[string]any, error) {
	raw := clone(t.raw)
	for _, k := range templateKeys {
		delete(raw, k)
	}
	if !strings.EqualFold(t.typ, typDeployment) {
		v, err := d.value(raw, t.ctx)
		m, _ := v.(map[string]any)
		return m, err
	}
	p, _ := raw["properties"].(map[string]any)
	nested, _ := p["template"].(map[string]any)
	delete(p, "template")
	v, err := d.value(raw, t.ctx)
	if err != nil {
		return nil, err
	}
	out := v.(map[string]any)
	op, _ := out["properties"].(map[string]any)
	if op == nil || nested == nil {
		return out, nil
	}
	eo, _ := op["expressionEvaluationOptions"].(map[string]any)
	if !strings.EqualFold(str(eo["scope"]), "inner") {
		nested = clone(nested)
		params := map[string]any{}
		for _, name := range core.SortedKeys(d.pdecl) {
			v, err := d.param(name)
			if err != nil {
				return nil, err
			}
			decl, _ := d.pdecl[name].(map[string]any)
			params[name] = map[string]any{"type": decl["type"], "defaultValue": escape(v)}
		}
		vars := map[string]any{}
		for _, name := range core.SortedKeys(d.vdecl) {
			if name == "copy" {
				continue
			}
			v, err := d.variable(name)
			if err != nil {
				return nil, err
			}
			vars[name] = escape(v)
		}
		if loops, ok := copyLoops("copy", d.vdecl["copy"]); ok {
			for _, l := range loops {
				v, err := d.variable(str(l["name"]))
				if err != nil {
					return nil, err
				}
				vars[str(l["name"])] = escape(v)
			}
		}
		nested["parameters"], nested["variables"] = params, vars
		op["expressionEvaluationOptions"] = map[string]any{"scope": "inner"}
	}
	op["template"] = nested
	return out, nil
}

// escape keeps a value literal inside another template: a string that starts with "[" gets another.
func escape(v any) any {
	switch x := v.(type) {
	case string:
		if strings.HasPrefix(x, "[") {
			return "[" + x
		}
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			out[i] = escape(e)
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, e := range x {
			out[k] = escape(e)
		}
		return out
	}
	return v
}

// run creates the resources in order, stopping at the first that fails, then works out the outputs. It fills the
// deployment's properties and returns its operations.
func (d *deployment) run(dp map[string]any) []map[string]any {
	start := time.Now()
	var ops, outRes []map[string]any
	var failed *core.Error
	skipped := map[*tmplRes]bool{}
	for _, t := range d.res {
		if t.existing {
			continue
		}
		if t.parent != nil && skipped[t.parent] {
			skipped[t] = true
			continue
		}
		if c, ok := t.raw["condition"]; ok {
			v, err := d.value(c, t.ctx)
			if err != nil {
				failed = invalid("the condition of '%s': %v", t.name, err)
				break
			}
			if b, ok := v.(bool); ok && !b {
				skipped[t] = true
				continue
			} else if !ok {
				failed = invalid("the condition of '%s' must be a boolean, not %s", t.name, kind(v))
				break
			}
		}
		opStart := time.Now()
		body, err := d.body(t)
		st, out := 0, map[string]any(nil)
		if err != nil {
			st, out = http.StatusBadRequest, map[string]any{"error": map[string]any{"code": "InvalidTemplate", "message": fmt.Sprintf("The resource '%s': %v", t.name, err)}}
		} else {
			st, out = d.send(http.MethodPut, t.id, t.api, body)
		}
		state := "Succeeded"
		if st < 300 && strings.EqualFold(t.typ, typDeployment) { // the PUT answered Accepted: read how the nested run ended
			if _, out = d.send(http.MethodGet, t.id, t.api, nil); str(props(out)["provisioningState"]) != "Succeeded" {
				e, _ := props(out)["error"].(map[string]any)
				if l, _ := e["details"].([]any); len(l) > 0 { // the cause, not "a deployment failed"
					e, _ = l[0].(map[string]any)
				}
				st, out = http.StatusBadRequest, map[string]any{"error": e}
			}
		}
		if st >= 300 {
			state = "Failed"
			e, _ := out["error"].(map[string]any)
			failed = core.Errorf(st, cmpOr(str(e["code"]), "DeploymentFailed"), "%s", cmpOr(str(e["message"]), compact(out)))
		} else if strings.EqualFold(t.typ, typDeployment) {
			if l, ok := props(out)["outputResources"].([]any); ok {
				for _, r := range l {
					if m, ok := r.(map[string]any); ok {
						outRes = append(outRes, m)
					}
				}
			}
		} else {
			outRes = append(outRes, map[string]any{"id": t.id})
		}
		opID := fmt.Sprintf("%04X%s", len(ops), strings.ToUpper(core.RandHex(6))) // listed by ID: in the order they ran
		msg := any(out)
		if st < 300 {
			msg = nil
		}
		ops = append(ops, map[string]any{"id": d.id.String() + "/operations/" + opID, "operationId": opID,
			"type": typDeployment + "/operations", "properties": map[string]any{"provisioningOperation": "Create",
				"provisioningState": state, "timestamp": time.Now().UTC().Format(time.RFC3339Nano), "duration": duration(time.Since(opStart)),
				"trackingId": core.UUID(), "serviceRequestId": core.UUID(), "statusCode": strings.ReplaceAll(http.StatusText(st), " ", ""),
				"statusMessage": msg, "targetResource": map[string]any{"id": t.id, "resourceType": t.typ, "resourceName": t.name, "apiVersion": t.api}}})
		if failed != nil {
			break
		}
	}
	outputs := map[string]any{}
	if failed == nil {
		if out, err := d.outputs(); err != nil {
			failed = invalid("%v", err)
		} else {
			outputs = out
		}
	}
	dp["outputs"], dp["outputResources"] = outputs, nonNil(outRes)
	dp["parameters"], dp["templateHash"] = d.paramView(), d.hash()
	dp["dependencies"], dp["providers"] = d.dependencies(), d.providers()
	dp["timestamp"], dp["duration"] = time.Now().UTC().Format(time.RFC3339Nano), duration(time.Since(start))
	dp["mode"], dp["provisioningState"] = "Incremental", "Succeeded"
	delete(dp, "error")
	if failed != nil {
		dp["provisioningState"] = "Failed"
		dp["error"] = map[string]any{"code": "DeploymentFailed", "target": d.id.String(),
			"message": "At least one resource deployment operation failed. Please list deployment operations for details. Please see https://aka.ms/arm-deployment-operations for usage details.",
			"details": []any{map[string]any{"code": failed.Code, "message": failed.Msg}}}
	}
	return ops
}

func duration(d time.Duration) string { return fmt.Sprintf("PT%.3fS", d.Seconds()) }

// outputs evaluates the template's outputs, with their types as Azure names them.
func (d *deployment) outputs() (map[string]any, error) {
	decl, _ := d.tmpl["outputs"].(map[string]any)
	out := map[string]any{}
	for _, name := range core.SortedKeys(decl) {
		o, _ := decl[name].(map[string]any)
		base := &evalCtx{loops: map[string]int{}}
		if c, ok := o["condition"]; ok {
			v, err := d.value(c, base)
			if err != nil {
				return nil, fmt.Errorf("the condition of output '%s': %v", name, err)
			}
			if b, _ := v.(bool); !b {
				continue
			}
		}
		var v any
		var err error
		if cp, ok := o["copy"].(map[string]any); ok {
			v, err = d.loop(map[string]any{"name": name, "count": cp["count"], "input": cp["input"]}, base)
		} else {
			v, err = d.value(o["value"], base)
		}
		if err != nil {
			return nil, fmt.Errorf("the output '%s': %v", name, err)
		}
		typ := strings.ToLower(str(o["type"]))
		if !typeOK(typ, v) {
			return nil, fmt.Errorf("the output '%s' is %s, not of its type '%s'", name, kind(v), o["type"])
		}
		entry := map[string]any{"type": outputTypes[typ], "value": v}
		if strings.HasPrefix(typ, "secure") {
			delete(entry, "value")
		}
		out[name] = entry
	}
	return out, nil
}

// paramView is the parameters as a deployment reports them: type and value, no value for secure ones.
func (d *deployment) paramView() map[string]any {
	out := map[string]any{}
	for _, name := range core.SortedKeys(d.pdecl) {
		decl, _ := d.pdecl[name].(map[string]any)
		typ := strings.ToLower(str(decl["type"]))
		e := map[string]any{"type": outputTypes[typ]}
		if v, ok := d.params[strings.ToLower(name)]; ok && !strings.HasPrefix(typ, "secure") {
			e["value"] = v
		}
		out[name] = e
	}
	return out
}

func (d *deployment) hash() string {
	b, _ := json.Marshal(d.tmpl)
	sum := sha256.Sum256(b)
	return strconv.FormatUint(binary.BigEndian.Uint64(sum[:8]), 10)
}

func (d *deployment) dependencies() []any {
	out := []any{}
	for _, t := range d.res {
		if len(t.deps) == 0 {
			continue
		}
		var on []any
		for _, u := range t.deps {
			on = append(on, map[string]any{"id": u.id, "resourceType": u.typ, "resourceName": u.name})
		}
		out = append(out, map[string]any{"id": t.id, "resourceType": t.typ, "resourceName": t.name, "dependsOn": on})
	}
	return out
}

func (d *deployment) providers() []any {
	types := map[string][]string{}
	var order []string
	for _, t := range d.res {
		ns, rt, _ := strings.Cut(t.typ, "/")
		if _, ok := types[ns]; !ok {
			order = append(order, ns)
		}
		if !slices.Contains(types[ns], rt) {
			types[ns] = append(types[ns], rt)
		}
	}
	out := []any{}
	for _, ns := range order {
		var rts []any
		for _, rt := range types[ns] {
			rts = append(rts, map[string]any{"resourceType": rt, "locations": []any{}})
		}
		out = append(out, map[string]any{"namespace": ns, "resourceTypes": rts})
	}
	return out
}

// ---- the routes ----

// putDeployment checks and runs a deployment, then stores it with its operations, replacing an earlier run.
// It answers like a long-running operation whose status is the deployment's own.
func (s *Service) putDeployment(k *call, id ID, body map[string]any, exists bool) (int, any, error) {
	d, err := s.newDeployment(k, id, body)
	if err != nil {
		return 0, nil, err
	}
	if err := d.check(); err != nil {
		return 0, nil, err
	}
	doc := genericDoc(gid{path: id.String(), typ: typDeployment, name: id.Name()}, body)
	ops := d.run(doc["properties"].(map[string]any))
	key := strings.ToLower(id.String())
	s.deleteGeneric(key)
	s.mu.Lock()
	s.st.Generic[key] = doc
	for _, op := range ops {
		s.st.Generic[strings.ToLower(str(op["id"]))] = op
	}
	s.mu.Unlock()
	// Azure answers Accepted and reports how it ended through the operation; an SDK that read a final state here
	// would not poll, and so would not see the error of a failed run.
	k.hdr.Set("Azure-AsyncOperation", k.base+id.String()+"/operationStatuses/"+core.RandHex(8)+"?api-version=2022-09-01")
	k.hdr.Set("Retry-After", "1")
	accepted := clone(doc)
	props(accepted)["provisioningState"] = "Accepted"
	delete(props(accepted), "error")
	if exists {
		return http.StatusOK, accepted, nil
	}
	return http.StatusCreated, accepted, nil
}

// deploymentAction serves the POSTs on a deployment: validate and whatIf need none to exist.
func (s *Service) deploymentAction(k *call, id ID, name string) (int, any, error) {
	switch strings.ToLower(name) {
	case "validate":
		d, err := s.newDeployment(k, id, k.payload())
		if err != nil {
			return 0, nil, err
		}
		if err := d.check(); err != nil {
			return 0, nil, err
		}
		var validated []any
		for _, t := range d.res {
			if !t.existing {
				validated = append(validated, map[string]any{"id": t.id})
			}
		}
		return http.StatusOK, map[string]any{"id": id.String(), "name": id.Name(), "type": typDeployment, "properties": map[string]any{
			"provisioningState": "Succeeded", "mode": "Incremental", "templateHash": d.hash(), "parameters": d.paramView(),
			"timestamp": time.Now().UTC().Format(time.RFC3339Nano), "duration": "PT0S", "correlationId": core.UUID(),
			"providers": d.providers(), "dependencies": d.dependencies(), "validatedResources": nonNil(validated)}}, nil
	case "whatif":
		return http.StatusOK, genericAction(typDeployment, name), nil
	}
	return s.action(k, id, name)
}

// deploymentStatus is the long-running operation of a deployment PUT: the deployment's own state.
func (s *Service) deploymentStatus(c *core.Ctx, id ID) (map[string]any, error) {
	dep, err := s.get(c, id.Parent(), nil)
	if err != nil {
		return nil, err
	}
	p := props(dep)
	out := map[string]any{"id": id.String(), "name": id.Name(), "status": p["provisioningState"], "startTime": p["timestamp"], "endTime": p["timestamp"]}
	if p["error"] != nil {
		out["error"] = p["error"]
	}
	return out, nil
}
