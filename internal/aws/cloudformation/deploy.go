package cloudformation

import (
	"cmp"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

func strconvItoa(n int) string { return strconv.Itoa(n) }

// params validates parameter inputs against the template and resolves
// defaults, previous values and SSM-typed parameters.
func (s *Service) params(c *core.Ctx, tmpl map[string]any, inputs []paramInput, previous []Param) (map[string]any, []Param, error) {
	defs, _ := tmpl["Parameters"].(map[string]any)
	given := map[string]string{}
	for _, in := range inputs {
		if _, ok := defs[in.ParameterKey]; !ok {
			return nil, nil, validation("Parameters: [%s] do not exist in the template", in.ParameterKey)
		}
		v := in.ParameterValue
		if in.UsePreviousValue {
			for _, p := range previous {
				if p.Key == in.ParameterKey {
					v = p.Value
				}
			}
		}
		given[in.ParameterKey] = v
	}
	values := map[string]any{}
	var out []Param
	var missing []string
	for _, name := range core.SortedKeys(defs) {
		d, _ := defs[name].(map[string]any)
		typ := str(d["Type"])
		v, ok := given[name]
		if !ok {
			if def, has := d["Default"]; has {
				v = str(def)
			} else {
				missing = append(missing, name)
				continue
			}
		}
		if err := checkParam(name, v, typ, d); err != nil {
			return nil, nil, err
		}
		p := Param{Key: name, Value: v, NoEcho: str(d["NoEcho"]) == "true"}
		resolved := v
		if strings.HasPrefix(typ, "AWS::SSM::Parameter::Value<") {
			var res struct{ Parameter struct{ Value string } }
			if err := s.hub.Call(c.Account, c.Region, "AmazonSSM", "GetParameter", map[string]any{"Name": v, "WithDecryption": true}, &res); err != nil {
				return nil, nil, validation("Unable to fetch parameters [%s] from parameter store for this account.", v)
			}
			resolved, p.Resolved = res.Parameter.Value, res.Parameter.Value
		}
		if strings.Contains(typ, "List<") || typ == "CommaDelimitedList" {
			var list []any
			for _, x := range strings.Split(resolved, ",") {
				list = append(list, strings.TrimSpace(x))
			}
			values[name] = list
		} else {
			values[name] = resolved
		}
		out = append(out, p)
	}
	if len(missing) > 0 {
		return nil, nil, validation("Parameters: [%s] must have values", strings.Join(missing, ", "))
	}
	return values, out, nil
}

func checkParam(name, v, typ string, d map[string]any) error {
	if allowed, ok := d["AllowedValues"].([]any); ok && !containsAny(allowed, v) {
		return validation("Parameter '%s' must be one of AllowedValues", name)
	}
	if p := str(d["AllowedPattern"]); p != "" {
		if re, err := regexp.Compile("^(?:" + p + ")$"); err == nil && !re.MatchString(v) {
			return validation("Parameter '%s' must match pattern %s", name, p)
		}
	}
	if n, err := strconv.Atoi(str(d["MinLength"])); err == nil && len(v) < n {
		return validation("Parameter '%s' must contain at least %d characters", name, n)
	}
	if n, err := strconv.Atoi(str(d["MaxLength"])); err == nil && len(v) > n {
		return validation("Parameter '%s' must contain at most %d characters", name, n)
	}
	if typ == "Number" {
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return validation("Parameter '%s' must be a number not %s", name, v)
		}
		if m, err := strconv.ParseFloat(str(d["MinValue"]), 64); err == nil && f < m {
			return validation("Parameter '%s' must be a number not less than %s", name, str(d["MinValue"]))
		}
		if m, err := strconv.ParseFloat(str(d["MaxValue"]), 64); err == nil && f > m {
			return validation("Parameter '%s' must be a number not greater than %s", name, str(d["MaxValue"]))
		}
	}
	return nil
}

// evaluator builds an intrinsic-function evaluator for a stack.
func (s *Service) evaluator(st *Stack, tmpl map[string]any, params map[string]any) *evaluator {
	conds, _ := tmpl["Conditions"].(map[string]any)
	maps, _ := tmpl["Mappings"].(map[string]any)
	return &evaluator{account: st.Account, region: st.Region, stack: st.Name, stackID: st.ID, params: params,
		condDefs: conds, conds: map[string]bool{}, mappings: maps, resources: map[string]*Resource{},
		imports: func(name string) (string, bool) {
			s.mu.Lock()
			defer s.mu.Unlock()
			v, ok := s.exports(st.Account, st.Region)[name]
			return v, ok
		}}
}

// rules checks template Rules before anything deploys, as CloudFormation does.
func (s *Service) rules(c *core.Ctx, stack string, tmpl, params map[string]any) error {
	return checkRules(s.evaluator(&Stack{Name: stack, Account: c.Account, Region: c.Region}, tmpl, params), tmpl)
}

// checkRules evaluates template Rules (CDK's CheckBootstrapVersion lives here).
func checkRules(e *evaluator, tmpl map[string]any) error {
	rules, _ := tmpl["Rules"].(map[string]any)
	for _, name := range core.SortedKeys(rules) {
		rule, _ := rules[name].(map[string]any)
		if cond, ok := rule["RuleCondition"]; ok {
			v, err := e.eval(cond)
			if err != nil {
				return err
			}
			if b, _ := v.(bool); !b {
				continue
			}
		}
		asserts, _ := rule["Assertions"].([]any)
		for _, a := range asserts {
			am, _ := a.(map[string]any)
			v, err := e.eval(am["Assert"])
			if err != nil {
				return err
			}
			if b, _ := v.(bool); !b {
				msg := str(am["AssertDescription"])
				if msg == "" {
					msg = "Template validation failed for rule " + name
				}
				return validation("%s", msg)
			}
		}
	}
	return nil
}

// ---- events and status ----

func (s *Service) event(st *Stack, logical, physical, typ, status, reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st.Events = append(st.Events, Event{ID: core.UUID(), LogicalID: logical, PhysicalID: physical, Type: typ, Status: status, Reason: reason, Time: time.Now().UTC()})
	if len(st.Events) > 2000 {
		st.Events = st.Events[len(st.Events)-2000:]
	}
	if logical == st.Name && typ == "AWS::CloudFormation::Stack" {
		st.Status, st.Reason = status, reason
		if strings.HasPrefix(status, "UPDATE") {
			st.Updated = time.Now().UTC()
		}
	} else if r := st.Resources[logical]; r != nil {
		r.Status, r.Reason, r.Updated = status, reason, time.Now().UTC()
		if physical != "" {
			r.PhysicalID = physical
		}
	}
	s.hub.MarkDirty()
}

func (s *Service) stackEvent(st *Stack, status, reason string) {
	s.event(st, st.Name, st.ID, "AWS::CloudFormation::Stack", status, reason)
}

// ---- operations (run in the background) ----

func (s *Service) runCreate(st *Stack, tmpl map[string]any, params map[string]any) {
	s.stackEvent(st, "CREATE_IN_PROGRESS", "User Initiated")
	u, err := s.apply(st, tmpl, params, "CREATE", "")
	if err == nil {
		s.stackEvent(st, "CREATE_COMPLETE", "")
		return
	}
	switch {
	case st.DisableRollback || st.OnFailure == "DO_NOTHING":
		s.stackEvent(st, "CREATE_FAILED", err.Error())
	default:
		s.stackEvent(st, "ROLLBACK_IN_PROGRESS", "The following resource(s) failed to create: "+err.Error()+". Rollback requested by user.")
		s.rollback(st, u, nil)
		s.stackEvent(st, "ROLLBACK_COMPLETE", "")
		if st.OnFailure == "DELETE" {
			s.finishDelete(st)
		}
	}
}

func (s *Service) runUpdate(st *Stack, body string, resolved []Param, tmpl map[string]any, params map[string]any) {
	s.mu.Lock()
	before := map[string]Resource{}
	for k, r := range st.Resources {
		before[k] = *r
	}
	prevBody, prevParams, prevDesc := st.Body, st.Params, st.Description
	st.Body, st.Params, st.Description = body, resolved, str(tmpl["Description"])
	s.mu.Unlock()
	s.stackEvent(st, "UPDATE_IN_PROGRESS", "User Initiated")
	u, err := s.apply(st, tmpl, params, "UPDATE", prevBody)
	if err == nil {
		// like CloudFormation, old physical resources go only now, after the update worked
		s.stackEvent(st, "UPDATE_COMPLETE_CLEANUP_IN_PROGRESS", "")
		s.cleanup(st, u)
		s.stackEvent(st, "UPDATE_COMPLETE", "")
		return
	}
	s.stackEvent(st, "UPDATE_ROLLBACK_IN_PROGRESS", err.Error())
	s.rollback(st, u, before)
	s.mu.Lock()
	st.Resources = map[string]*Resource{}
	for k, r := range before {
		st.Resources[k] = &r
	}
	st.Body, st.Params, st.Description = prevBody, prevParams, prevDesc
	s.mu.Unlock()
	s.stackEvent(st, "UPDATE_ROLLBACK_COMPLETE", "")
}

// undoLog records what apply did, so a failed run can be undone and a
// successful one can finish its cleanup.
type undoLog struct {
	steps  []undoStep
	old    []Resource      // replaced resources: deleted at cleanup, kept until then
	gone   []string        // logical ids dropped from the template: deleted at cleanup
	retain map[string]bool // of gone, the ones to keep
}

// undoStep is one change to undo: a resource created (or replaced: the
// new one goes away), or one updated in place (reverted with its old properties).
type undoStep struct {
	id      string
	inPlace bool
}

// rollback undoes the steps in reverse. before holds the resources as they
// were before the update (nil for a create: nothing to revert).
func (s *Service) rollback(st *Stack, u *undoLog, before map[string]Resource) {
	for i := len(u.steps) - 1; i >= 0; i-- {
		step := u.steps[i]
		if !step.inPlace {
			s.destroy(st, []string{step.id}, nil)
			continue
		}
		s.mu.Lock()
		var cur Resource
		if r := st.Resources[step.id]; r != nil {
			cur = *r
		}
		s.mu.Unlock()
		p := s.provider(cur.Type)
		s.event(st, step.id, cur.PhysicalID, cur.Type, "UPDATE_IN_PROGRESS", "")
		// update(deployed props, wanted props): the old properties become the wanted ones
		_, _, err := p.update(s.ctx(st, step.id, cur.Type), cur.PhysicalID, cur.Props, before[step.id].Props)
		if err != nil {
			s.event(st, step.id, cur.PhysicalID, cur.Type, "UPDATE_FAILED", err.Error())
			continue
		}
		s.event(st, step.id, cur.PhysicalID, cur.Type, "UPDATE_COMPLETE", "")
	}
}

// cleanup deletes what an update replaced or dropped. A failure here shows
// as DELETE_FAILED on the resource and does not fail the update.
func (s *Service) cleanup(st *Stack, u *undoLog) {
	for _, r := range u.old {
		s.provider(r.Type).del(s.ctx(st, r.LogicalID, r.Type), r.PhysicalID, r.Props)
	}
	s.destroy(st, u.gone, u.retain)
}

func (s *Service) runDelete(st *Stack, retain []string) {
	s.stackEvent(st, "DELETE_IN_PROGRESS", "User Initiated")
	tmpl, _ := s.template(st.Body)
	var ids []string
	if tmpl != nil {
		ids, _ = order(tmpl["Resources"].(map[string]any))
	}
	s.mu.Lock()
	for id := range st.Resources {
		if !slices.Contains(ids, id) {
			ids = append(ids, id)
		}
	}
	s.mu.Unlock()
	if err := s.destroy(st, ids, retainSet(tmpl, retain)); err != nil {
		s.stackEvent(st, "DELETE_FAILED", err.Error())
		return
	}
	s.finishDelete(st)
}

func (s *Service) finishDelete(st *Stack) {
	s.stackEvent(st, "DELETE_COMPLETE", "")
	s.mu.Lock()
	st.Deleted = time.Now().UTC()
	st.Outputs = nil
	st.ChangeSets = nil
	// keep a bounded history of deleted stacks
	n := 0
	for _, x := range s.st.Stacks {
		if !x.Deleted.IsZero() {
			n++
		}
	}
	for n > 100 {
		for i, x := range s.st.Stacks {
			if !x.Deleted.IsZero() {
				s.st.Stacks = append(s.st.Stacks[:i], s.st.Stacks[i+1:]...)
				break
			}
		}
		n--
	}
	s.mu.Unlock()
}

// retainSet lists resources with DeletionPolicy Retain (or explicitly retained).
func retainSet(tmpl map[string]any, extra []string) map[string]bool {
	out := map[string]bool{}
	for _, r := range extra {
		out[r] = true
	}
	if tmpl == nil {
		return out
	}
	for id, def := range tmpl["Resources"].(map[string]any) {
		d, _ := def.(map[string]any)
		if p := str(d["DeletionPolicy"]); p == "Retain" || p == "RetainExceptOnCreate" {
			out[id] = true
		}
	}
	return out
}

// destroy deletes resources in reverse of the given dependency order.
func (s *Service) destroy(st *Stack, ids []string, retain map[string]bool) error {
	var firstErr error
	for i := len(ids) - 1; i >= 0; i-- {
		id := ids[i]
		s.mu.Lock()
		r := st.Resources[id]
		var cp Resource
		if r != nil {
			cp = *r
		}
		s.mu.Unlock()
		if r == nil {
			continue
		}
		if cp.PhysicalID == "" || retain[id] || cp.Status == "DELETE_COMPLETE" {
			if retain[id] {
				s.event(st, id, cp.PhysicalID, cp.Type, "DELETE_SKIPPED", "")
			}
			s.mu.Lock()
			delete(st.Resources, id)
			s.mu.Unlock()
			continue
		}
		s.event(st, id, cp.PhysicalID, cp.Type, "DELETE_IN_PROGRESS", "")
		err := s.provider(cp.Type).del(s.ctx(st, id, cp.Type), cp.PhysicalID, cp.Props)
		if err != nil && !notFound(err) {
			s.event(st, id, cp.PhysicalID, cp.Type, "DELETE_FAILED", err.Error())
			if firstErr == nil {
				firstErr = fmt.Errorf("%s: %v", id, err)
			}
			continue
		}
		s.event(st, id, cp.PhysicalID, cp.Type, "DELETE_COMPLETE", "")
		s.mu.Lock()
		delete(st.Resources, id)
		s.mu.Unlock()
	}
	return firstErr
}

// notFound treats "already gone" as a successful delete.
func notFound(err error) bool {
	e := core.AsError(err)
	return e.Status == 404 || strings.Contains(e.Code, "NotFound") || strings.Contains(e.Code, "NoSuch") || strings.Contains(e.Code, "NonExistent") || strings.Contains(e.Code, "DoesNotExist")
}

// apply creates, updates and removes resources so the stack matches tmpl
// (prevBody orders the removal of what it no longer has). Nothing is deleted
// here: the undo log says what to remove on rollback or in cleanup.
func (s *Service) apply(st *Stack, tmpl map[string]any, params map[string]any, kind, prevBody string) (*undoLog, error) {
	u := &undoLog{}
	e := s.evaluator(st, tmpl, params)
	if err := checkRules(e, tmpl); err != nil {
		return u, err
	}
	resources, _ := tmpl["Resources"].(map[string]any)
	ids, err := order(resources)
	if err != nil {
		return u, err
	}
	wanted := map[string]bool{}
	for _, id := range ids {
		d, _ := resources[id].(map[string]any)
		typ := str(d["Type"])
		if cond := str(d["Condition"]); cond != "" {
			ok, err := e.condition(cond)
			if err != nil {
				return u, err
			}
			if !ok {
				continue
			}
		}
		wanted[id] = true
		s.mu.Lock()
		e.resources = map[string]*Resource{}
		for k, r := range st.Resources {
			cp := *r
			e.resources[k] = &cp
		}
		cur := e.resources[id]
		s.mu.Unlock()
		raw, err := e.eval(d["Properties"])
		if err != nil {
			s.event(st, id, "", typ, kind+"_FAILED", err.Error())
			return u, fmt.Errorf("[%s] %v", id, err)
		}
		props, _ := raw.(map[string]any)
		if props == nil {
			props = map[string]any{}
		}
		if err := s.resolveDynamic(st, props); err != nil {
			s.event(st, id, "", typ, kind+"_FAILED", err.Error())
			return u, fmt.Errorf("[%s] %v", id, err)
		}
		p := s.provider(typ)
		c := s.ctx(st, id, typ)
		switch {
		case cur == nil || cur.PhysicalID == "":
			s.mu.Lock()
			st.Resources[id] = &Resource{LogicalID: id, Type: typ, Status: "CREATE_IN_PROGRESS", Updated: time.Now().UTC()}
			s.mu.Unlock()
			s.event(st, id, "", typ, "CREATE_IN_PROGRESS", "")
			phys, attrs, err := p.create(c, props)
			if err != nil {
				s.event(st, id, "", typ, "CREATE_FAILED", err.Error())
				u.steps = append(u.steps, undoStep{id: id})
				return u, fmt.Errorf("[%s] %v", id, err)
			}
			s.mu.Lock()
			r := st.Resources[id]
			r.PhysicalID, r.Props, r.Attrs = phys, props, attrs
			s.mu.Unlock()
			u.steps = append(u.steps, undoStep{id: id})
			if p.note != "" {
				s.event(st, id, phys, typ, "CREATE_COMPLETE", p.note)
			} else {
				s.event(st, id, phys, typ, "CREATE_COMPLETE", "")
			}
		case equal(props, cur.Props) && cur.Type == typ:
			// unchanged
		default:
			s.event(st, id, cur.PhysicalID, typ, "UPDATE_IN_PROGRESS", "")
			var attrs map[string]any
			replace := cur.Type != typ || p.update == nil
			if !replace {
				attrs, replace, err = p.update(c, cur.PhysicalID, cur.Props, props)
				if err != nil {
					s.event(st, id, cur.PhysicalID, typ, "UPDATE_FAILED", err.Error())
					return u, fmt.Errorf("[%s] %v", id, err)
				}
				if !replace {
					u.steps = append(u.steps, undoStep{id: id, inPlace: true})
				}
			}
			phys := cur.PhysicalID
			if replace { // create the replacement; the old one stays until cleanup
				var err error
				phys, attrs, err = p.create(c, props)
				if err != nil {
					s.event(st, id, cur.PhysicalID, typ, "UPDATE_FAILED", err.Error())
					return u, fmt.Errorf("[%s] %v", id, err)
				}
				if phys != cur.PhysicalID {
					u.steps = append(u.steps, undoStep{id: id})
					u.old = append(u.old, *cur)
				}
			}
			s.mu.Lock()
			r := st.Resources[id]
			r.PhysicalID, r.Props, r.Type = phys, props, typ
			if attrs != nil {
				r.Attrs = attrs
			}
			s.mu.Unlock()
			s.event(st, id, phys, typ, "UPDATE_COMPLETE", "")
		}
	}
	// list resources no longer in the template, dependents first
	var gone []string
	s.mu.Lock()
	for id := range st.Resources {
		if !wanted[id] {
			gone = append(gone, id)
		}
	}
	s.mu.Unlock()
	pos := map[string]int{}
	if old, err := s.template(prevBody); err == nil {
		ids, _ := order(old["Resources"].(map[string]any))
		for i, id := range ids {
			pos[id] = i
		}
	}
	slices.SortFunc(gone, func(a, b string) int { return pos[a] - pos[b] })
	u.gone, u.retain = gone, retainSet(tmpl, nil)
	return u, s.outputs(st, tmpl, e)
}

// outputs evaluates Outputs (and Export names) once resources exist.
func (s *Service) outputs(st *Stack, tmpl map[string]any, e *evaluator) error {
	s.mu.Lock()
	e.resources = map[string]*Resource{}
	for k, r := range st.Resources {
		cp := *r
		e.resources[k] = &cp
	}
	s.mu.Unlock()
	defs, _ := tmpl["Outputs"].(map[string]any)
	var out []Output
	for _, k := range core.SortedKeys(defs) {
		d, _ := defs[k].(map[string]any)
		if cond := str(d["Condition"]); cond != "" {
			if ok, _ := e.condition(cond); !ok {
				continue
			}
		}
		v, err := e.eval(d["Value"])
		if err != nil {
			return fmt.Errorf("output %s: %v", k, err)
		}
		o := Output{Key: k, Value: str(v), Description: str(d["Description"])}
		if l, ok := v.([]any); ok {
			var parts []string
			for _, x := range l {
				parts = append(parts, str(x))
			}
			o.Value = strings.Join(parts, ",")
		}
		if exp, ok := d["Export"].(map[string]any); ok {
			name, err := e.eval(exp["Name"])
			if err != nil {
				return err
			}
			o.Export = str(name)
		}
		out = append(out, o)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	taken := s.exports(st.Account, st.Region)
	for _, o := range st.Outputs { // our own previous exports are fine
		delete(taken, o.Export)
	}
	for _, o := range out {
		if _, dup := taken[o.Export]; o.Export != "" && dup {
			return validation("Export with name %s is already exported by another stack", o.Export)
		}
	}
	st.Outputs = out
	return nil
}

var dynamicRef = regexp.MustCompile(`\{\{resolve:(ssm|ssm-secure|secretsmanager):([^}]+)\}\}`)

// resolveDynamic replaces {{resolve:…}} references in string properties.
func (s *Service) resolveDynamic(st *Stack, props map[string]any) error {
	var firstErr error
	var walk func(v any) any
	walk = func(v any) any {
		switch x := v.(type) {
		case string:
			return dynamicRef.ReplaceAllStringFunc(x, func(m string) string {
				parts := dynamicRef.FindStringSubmatch(m)
				val, err := s.dynamic(st, parts[1], parts[2])
				if err != nil && firstErr == nil {
					firstErr = err
				}
				return val
			})
		case map[string]any:
			for k, val := range x {
				x[k] = walk(val)
			}
		case []any:
			for i, val := range x {
				x[i] = walk(val)
			}
		}
		return v
	}
	walk(props)
	return firstErr
}

func (s *Service) dynamic(st *Stack, kind, ref string) (string, error) {
	if kind == "secretsmanager" {
		// secret-id:SecretString:json-key:version-stage:version-id
		parts := strings.Split(ref, ":")
		id := parts[0]
		if strings.HasPrefix(ref, "arn:") && len(parts) >= 7 {
			id, parts = strings.Join(parts[:7], ":"), append([]string{strings.Join(parts[:7], ":")}, parts[7:]...)
		}
		in := map[string]any{"SecretId": id}
		if len(parts) > 3 && parts[3] != "" {
			in["VersionStage"] = parts[3]
		}
		if len(parts) > 4 && parts[4] != "" {
			in["VersionId"] = parts[4]
		}
		var out struct{ SecretString string }
		if err := s.hub.Call(st.Account, st.Region, "secretsmanager", "GetSecretValue", in, &out); err != nil {
			return "", fmt.Errorf("could not resolve secret %s: %v", id, err)
		}
		if len(parts) > 2 && parts[2] != "" {
			var m map[string]any
			if json.Unmarshal([]byte(out.SecretString), &m) != nil {
				return "", fmt.Errorf("secret %s is not JSON", id)
			}
			return str(m[parts[2]]), nil
		}
		return out.SecretString, nil
	}
	name, version, _ := strings.Cut(ref, ":")
	if version != "" {
		name += ":" + version
	}
	var out struct{ Parameter struct{ Value string } }
	if err := s.hub.Call(st.Account, st.Region, "AmazonSSM", "GetParameter", map[string]any{"Name": name, "WithDecryption": true}, &out); err != nil {
		return "", fmt.Errorf("could not resolve SSM parameter %s: %v", name, err)
	}
	return out.Parameter.Value, nil
}

// equal compares property maps as JSON (numbers and nesting normalized).
func equal(a, b any) bool {
	ja, _ := json.Marshal(a)
	jb, _ := json.Marshal(b)
	var na, nb any
	json.Unmarshal(ja, &na)
	json.Unmarshal(jb, &nb)
	return reflect.DeepEqual(na, nb)
}

// ---- custom resources ----

type customResponse struct {
	Status             string
	Reason             string
	PhysicalResourceId string
	Data               map[string]any
	NoEcho             bool
}

// customCallback receives the PUT a custom resource handler sends to ResponseURL.
func (s *Service) customCallback(w http.ResponseWriter, r *http.Request) {
	token := strings.TrimPrefix(r.URL.Path, "/_pogi/cfn-response/")
	var res customResponse
	body, _ := io.ReadAll(r.Body)
	if err := json.Unmarshal(body, &res); err != nil {
		http.Error(w, "bad response body", http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	ch := s.waiting[token]
	s.mu.Unlock()
	if ch == nil {
		http.Error(w, "unknown or expired response URL", http.StatusNotFound)
		return
	}
	select {
	case ch <- res:
	default:
	}
	w.WriteHeader(http.StatusOK)
}

// CustomTimeout bounds how long a custom resource may take to answer.
var CustomTimeout = 5 * time.Minute

// custom runs a custom resource request through its Lambda (or SNS) service token.
func (s *Service) custom(c *pctx, requestType, physical string, props, old map[string]any) (string, map[string]any, error) {
	token := core.NewID()
	ch := make(chan customResponse, 1)
	s.mu.Lock()
	s.waiting[token] = ch
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.waiting, token)
		s.mu.Unlock()
	}()
	// http on pogi's port; the port also speaks TLS, and the native Node
	// bootstrap routes port-less https requests (CDK's provider framework) to it
	host := "localhost:7644"
	if u, err := url.Parse(s.hub.URL); err == nil && u.Host != "" {
		host = u.Host
	}
	event := map[string]any{"RequestType": requestType, "ResponseURL": "http://" + host + "/_pogi/cfn-response/" + token, "StackId": c.stackID,
		"RequestId": core.UUID(), "ResourceType": c.typ, "LogicalResourceId": c.logical, "ResourceProperties": props,
		"ServiceToken": props["ServiceToken"]}
	if physical != "" {
		event["PhysicalResourceId"] = physical
	}
	if old != nil {
		event["OldResourceProperties"] = old
	}
	payload, _ := json.Marshal(event)
	target := str(props["ServiceToken"])
	answer := func(res customResponse) (string, map[string]any, error) {
		if res.Status != "SUCCESS" {
			return "", nil, fmt.Errorf("Received response status [FAILED] from custom resource. Message returned: %s", res.Reason)
		}
		phys := cmp.Or(res.PhysicalResourceId, physical)
		if phys == "" {
			phys = c.name(64, false)
		}
		return phys, res.Data, nil
	}
	if svc, _, _, _, _ := core.ParseARN(target); svc == "lambda" {
		if _, err := s.hub.Invoke(target, payload); err != nil {
			select {
			case res := <-ch: // it answered before failing
				return answer(res)
			default:
				return "", nil, fmt.Errorf("custom resource handler %s failed: %v", target, err)
			}
		}
	} else if err := s.hub.Deliver(target, &core.Message{Body: string(payload)}); err != nil {
		return "", nil, fmt.Errorf("could not invoke custom resource handler %s: %v", target, err)
	}
	select {
	case res := <-ch:
		return answer(res)
	case <-time.After(CustomTimeout):
		return "", nil, fmt.Errorf("custom resource %s did not respond within %s", c.logical, CustomTimeout)
	}
}
