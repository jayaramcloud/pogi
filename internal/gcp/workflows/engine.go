package workflows

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"
)

// The caps on one execution. Google's own limits are 100,000 steps and 20 nested calls.
const (
	maxSteps = 100000 // steps run in one execution, loop bodies included
	maxLoop  = 100000 // iterations of one `for` loop
	maxDepth = 20     // nested subworkflow calls
)

type param struct {
	name string
	def  any
	has  bool
}

type routine struct {
	params []param
	steps  []any
}

// program is a parsed workflow source: its main and subworkflows.
type program map[string]*routine

// parseSource reads a workflow's YAML (or JSON) and checks its shape, as the API does at create time.
// A step type pogi cannot run is not refused here: it fails the execution, naming it.
func parseSource(src string) (program, error) {
	var doc any
	if err := yaml.Unmarshal([]byte(src), &doc); err != nil {
		return nil, fmt.Errorf("could not parse the source: %v", err)
	}
	doc = wholeNumbers(doc)
	p := program{}
	switch d := doc.(type) {
	case []any:
		p["main"] = &routine{steps: d}
	case map[string]any:
		for name, v := range d {
			m, ok := v.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("%s must be a map with steps", name)
			}
			steps, _ := m["steps"].([]any)
			r := &routine{steps: steps}
			if ps, ok := m["params"]; ok {
				list, ok := ps.([]any)
				if !ok {
					return nil, fmt.Errorf("%s: params must be a list", name)
				}
				for _, e := range list {
					switch x := e.(type) {
					case string:
						r.params = append(r.params, param{name: x})
					case map[string]any:
						if len(x) != 1 {
							return nil, fmt.Errorf("%s: a param with a default has one key", name)
						}
						for k, d := range x {
							r.params = append(r.params, param{name: k, def: d, has: true})
						}
					default:
						return nil, fmt.Errorf("%s: bad param", name)
					}
				}
			}
			p[name] = r
		}
	default:
		return nil, fmt.Errorf("the source must be a list of steps or a map of subworkflows")
	}
	if p["main"] == nil {
		return nil, fmt.Errorf("the source needs a main workflow")
	}
	for name, r := range p {
		if len(r.steps) == 0 {
			return nil, fmt.Errorf("%s needs at least one step", name)
		}
		if err := checkSteps(name, r.steps); err != nil {
			return nil, err
		}
	}
	return p, nil
}

// wholeNumbers makes the ints YAML gives into int64, the language's integer type.
func wholeNumbers(v any) any {
	switch x := v.(type) {
	case int:
		return int64(x)
	case map[string]any:
		for k, e := range x {
			x[k] = wholeNumbers(e)
		}
	case []any:
		for i, e := range x {
			x[i] = wholeNumbers(e)
		}
	}
	return v
}

// checkSteps checks that a list holds steps (one name each, with a map body) and goes into the nested lists.
func checkSteps(where string, list any) error {
	l, ok := list.([]any)
	if !ok {
		return fmt.Errorf("%s: steps must be a list", where)
	}
	for _, it := range l {
		m, ok := it.(map[string]any)
		if !ok || len(m) != 1 {
			return fmt.Errorf("%s: each step is a name with a body, got %v", where, it)
		}
		for name, b := range m {
			body, ok := b.(map[string]any)
			if !ok {
				return fmt.Errorf("%s: the body of step %s must be a map", where, name)
			}
			if err := checkBody(where+"."+name, body); err != nil {
				return err
			}
		}
	}
	return nil
}

func checkBody(where string, body map[string]any) error {
	for _, k := range []string{"steps"} {
		if v, ok := body[k]; ok {
			if err := checkSteps(where, v); err != nil {
				return err
			}
		}
	}
	if f, ok := body["for"].(map[string]any); ok {
		if err := checkSteps(where, f["steps"]); err != nil {
			return err
		}
	}
	if t, ok := body["try"].(map[string]any); ok {
		if _, has := t["steps"]; has {
			if err := checkSteps(where, t["steps"]); err != nil {
				return err
			}
		}
	}
	if x, ok := body["except"].(map[string]any); ok {
		if _, has := x["steps"]; has {
			if err := checkSteps(where, x["steps"]); err != nil {
				return err
			}
		}
	}
	if sw, ok := body["switch"].([]any); ok {
		for _, c := range sw {
			cm, ok := c.(map[string]any)
			if !ok {
				return fmt.Errorf("%s: a switch branch must be a map with a condition", where)
			}
			if _, ok := cm["condition"]; !ok {
				return fmt.Errorf("%s: a switch branch needs a condition", where)
			}
			if err := checkBody(where, cm); err != nil {
				return err
			}
		}
	}
	return nil
}

// runEnv is one execution's state.
type runEnv struct {
	ctx   context.Context
	svc   *Service
	prog  program
	steps int
	meta  map[string]string // GOOGLE_CLOUD_* values
	user  map[string]string // the workflow's user_env_vars
}

func (e *runEnv) getEnv(name string) (any, error) {
	if v, ok := e.meta[name]; ok {
		return v, nil
	}
	if v, ok := e.user[name]; ok {
		return v, nil
	}
	return nil, newErr(fmt.Sprintf("sys.get_env: %s is not set", name), "KeyError")
}

// ctl says what a step asks the loop around it to do.
type ctl struct {
	kind  int
	label string
	val   any
}

const (
	cNone = iota
	cNext
	cReturn
	cEnd
	cBreak
	cContinue
)

// runMain runs main with the execution's argument and returns its result.
func (e *runEnv) runMain(arg any) (any, error) {
	m := e.prog["main"]
	args := map[string]any{}
	if len(m.params) > 0 {
		if arg != nil {
			args[m.params[0].name] = arg
		} else {
			args[m.params[0].name] = map[string]any{}
		}
	}
	return e.callRoutine("main", m, args, 0)
}

func (e *runEnv) callRoutine(name string, r *routine, args map[string]any, depth int) (any, error) {
	if depth >= maxDepth {
		return nil, newErr(fmt.Sprintf("calls are nested more than %d deep", maxDepth), "ResourceLimitError")
	}
	vars := map[string]any{}
	for _, p := range r.params {
		if v, ok := args[p.name]; ok {
			vars[p.name] = v
		} else if p.has {
			vars[p.name] = copyOf(p.def)
		} else {
			return nil, newErr(fmt.Sprintf("%s needs the argument %s", name, p.name), "ValueError")
		}
	}
	for k := range args {
		if _, ok := vars[k]; !ok {
			return nil, newErr(fmt.Sprintf("%s has no parameter %s", name, k), "ValueError")
		}
	}
	sc := &scope{vars: vars, env: e, routine: name, depth: depth}
	c, err := sc.runSteps(r.steps)
	if err != nil {
		return nil, err
	}
	switch c.kind {
	case cReturn:
		return c.val, nil
	case cNone, cEnd:
		return nil, nil
	case cNext:
		return nil, unsupported("a jump to the unknown step %q", c.label)
	}
	return nil, unsupported("break or continue outside a loop")
}

func (s *scope) runSteps(list []any) (ctl, error) {
	index := map[string]int{}
	for i, it := range list {
		for name := range it.(map[string]any) {
			index[name] = i
		}
	}
	for i := 0; i < len(list); {
		var name string
		var body map[string]any
		for n, b := range list[i].(map[string]any) {
			name, body = n, b.(map[string]any)
		}
		c, err := s.runStep(name, body)
		if err != nil {
			return ctl{}, err
		}
		switch c.kind {
		case cNone:
			i++
		case cNext:
			j, ok := index[c.label]
			if !ok {
				return c, nil // a step of an enclosing list
			}
			i = j
		default:
			return c, nil
		}
	}
	return ctl{}, nil
}

// runStep runs one step and records where an error came from.
func (s *scope) runStep(name string, body map[string]any) (ctl, error) {
	if err := s.env.ctx.Err(); err != nil {
		return ctl{}, err
	}
	if s.env.steps++; s.env.steps > maxSteps {
		e := newErr(fmt.Sprintf("the execution ran more than the limit of %d steps", maxSteps), "ResourceLimitError")
		e.fatal = true
		return ctl{}, e
	}
	c, err := s.runBody(name, body)
	if we, ok := err.(*wfErr); ok && (len(we.stack) == 0 || we.stack[len(we.stack)-1].routine != s.routine) {
		we.stack = append(we.stack, frame{name, s.routine})
	}
	if err == nil && c.kind == cNone {
		if n, ok := body["next"].(string); ok {
			c = jump(n)
		}
	}
	return c, err
}

func jump(label string) ctl {
	switch label {
	case "end":
		return ctl{kind: cEnd}
	case "break":
		return ctl{kind: cBreak}
	case "continue":
		return ctl{kind: cContinue}
	}
	return ctl{kind: cNext, label: label}
}

func has(m map[string]any, k string) bool { _, ok := m[k]; return ok }

func (s *scope) runBody(name string, b map[string]any) (ctl, error) {
	switch {
	case has(b, "parallel"):
		return ctl{}, unsupported("the step type parallel (step %s)", name)
	case has(b, "assign"):
		return ctl{}, s.assign(b["assign"])
	case has(b, "call"):
		return ctl{}, s.callStep(b)
	case has(b, "switch"):
		list, _ := b["switch"].([]any)
		for _, br := range list {
			bm := br.(map[string]any)
			v, err := s.evalValue(bm["condition"])
			if err != nil {
				return ctl{}, err
			}
			ok, isBool := v.(bool)
			if !isBool {
				return ctl{}, newErr(fmt.Sprintf("a switch condition must be true or false, got %s", typeName(v)), "TypeError")
			}
			if ok {
				rest := map[string]any{}
				for k, e := range bm {
					if k != "condition" {
						rest[k] = e
					}
				}
				c, err := s.runBody(name, rest)
				if n, ok := rest["next"].(string); ok && err == nil && c.kind == cNone {
					c = jump(n)
				}
				return c, err
			}
		}
		return ctl{}, nil
	case has(b, "for"):
		return s.forLoop(name, b["for"].(map[string]any))
	case has(b, "try"):
		return s.try(name, b)
	case has(b, "return"):
		v, err := s.evalValue(b["return"])
		return ctl{kind: cReturn, val: copyOf(v)}, err
	case has(b, "raise"):
		v, err := s.evalValue(b["raise"])
		if err != nil {
			return ctl{}, err
		}
		return ctl{}, &wfErr{val: copyOf(v)}
	case has(b, "steps"):
		return s.runSteps(b["steps"].([]any))
	case has(b, "next") && len(b) == 1:
		return ctl{}, nil // the next is applied by runStep
	}
	return ctl{}, unsupported("the step %s: it has no step type pogi knows (%s)", name, strings.Join(keysOf(b), ", "))
}

func keysOf(m map[string]any) []string {
	var ks []string
	for k := range m {
		ks = append(ks, k)
	}
	return ks
}

func (s *scope) assign(v any) error {
	list, ok := v.([]any)
	if !ok {
		return newErr("assign must be a list", "ValueError")
	}
	for _, it := range list {
		m, ok := it.(map[string]any)
		if !ok {
			return newErr("each assign item is a name and a value", "ValueError")
		}
		for target, val := range m {
			x, err := s.evalValue(val)
			if err != nil {
				return err
			}
			if err := s.set(target, copyOf(x)); err != nil {
				return err
			}
		}
	}
	return nil
}

// set stores v in a variable, a map key (a.b, a["b"]) or an existing list element (a[0]).
func (s *scope) set(target string, v any) error {
	n, err := parseExpr(target)
	r, ok := n.(*ref)
	if err != nil || !ok {
		return newErr(fmt.Sprintf("cannot assign to %q", target), "SyntaxError")
	}
	if len(r.sels) == 0 {
		s.vars[r.name] = v
		return nil
	}
	cur, ok := s.vars[r.name]
	if !ok {
		return newErr(fmt.Sprintf("variable not defined: %s", r.name), "KeyError")
	}
	if cur, err = s.walk(cur, r.sels[:len(r.sels)-1]); err != nil {
		return err
	}
	k, err := s.eval(r.sels[len(r.sels)-1])
	if err != nil {
		return err
	}
	switch c := cur.(type) {
	case map[string]any:
		ks, ok := k.(string)
		if !ok {
			return newErr("map keys must be strings", "TypeError")
		}
		c[ks] = v
	case []any:
		i, ok := k.(int64)
		if !ok || i < 0 || i >= int64(len(c)) {
			return newErr(fmt.Sprintf("list index out of range: %v", k), "IndexError")
		}
		c[i] = v
	default:
		return newErr(fmt.Sprintf("cannot assign into %s", typeName(cur)), "TypeError")
	}
	return nil
}

func (s *scope) forLoop(name string, f map[string]any) (ctl, error) {
	valName, _ := f["value"].(string)
	idxName, _ := f["index"].(string)
	if valName == "" && idxName == "" {
		valName = "value"
	}
	var items []any
	switch {
	case has(f, "in"):
		v, err := s.evalValue(f["in"])
		if err != nil {
			return ctl{}, err
		}
		l, ok := v.([]any)
		if !ok {
			return ctl{}, newErr(fmt.Sprintf("for ... in wants a list, got %s", typeName(v)), "TypeError")
		}
		items = l
	case has(f, "range"):
		v, err := s.evalValue(f["range"])
		r, ok := v.([]any)
		if err != nil {
			return ctl{}, err
		}
		if !ok || len(r) != 2 {
			return ctl{}, newErr("for ... range wants [from, to]", "ValueError")
		}
		from, ok1 := r[0].(int64)
		to, ok2 := r[1].(int64)
		if !ok1 || !ok2 {
			return ctl{}, newErr("for ... range wants whole numbers", "TypeError")
		}
		if to-from+1 > maxLoop {
			return ctl{}, loopLimit()
		}
		for i := from; i <= to; i++ {
			items = append(items, i)
		}
	default:
		return ctl{}, newErr("for needs in or range", "ValueError")
	}
	if len(items) > maxLoop {
		return ctl{}, loopLimit()
	}
	steps := f["steps"].([]any)
	defer delete(s.vars, valName)
	defer delete(s.vars, idxName)
	for i, it := range items {
		if valName != "" {
			s.vars[valName] = copyOf(it)
		}
		if idxName != "" {
			s.vars[idxName] = int64(i)
		}
		c, err := s.runSteps(steps)
		if err != nil {
			return ctl{}, err
		}
		switch c.kind {
		case cBreak:
			return ctl{}, nil
		case cNone, cContinue:
		default:
			return c, nil
		}
	}
	return ctl{}, nil
}

func loopLimit() error {
	e := newErr(fmt.Sprintf("a loop ran more than the limit of %d iterations", maxLoop), "ResourceLimitError")
	e.fatal = true
	return e
}

// try runs the try body; on an error it retries as the retry policy says, then runs except.
func (s *scope) try(name string, b map[string]any) (ctl, error) {
	pol, err := s.retryPolicy(b["retry"])
	if err != nil {
		return ctl{}, err
	}
	for attempt := 0; ; attempt++ {
		var c ctl
		var err error
		if tb, ok := b["try"].(map[string]any); ok && has(tb, "steps") {
			c, err = s.runSteps(tb["steps"].([]any))
		} else if ok {
			c, err = s.runBody(name, tb)
		} else {
			return ctl{}, newErr("try must be a map", "ValueError")
		}
		we, isWf := err.(*wfErr)
		if err == nil || !isWf || we.fatal {
			return c, err
		}
		if pol != nil && attempt < pol.max {
			retry, perr := pol.retry(s, we.val)
			if perr != nil {
				return ctl{}, perr
			}
			if retry {
				if err := sleep(s.env.ctx, pol.delay(attempt)); err != nil {
					return ctl{}, err
				}
				continue
			}
		}
		ex, ok := b["except"].(map[string]any)
		if !ok {
			return ctl{}, err
		}
		if as, ok := ex["as"].(string); ok {
			s.vars[as] = copyOf(we.val)
			defer delete(s.vars, as)
		}
		if has(ex, "steps") {
			return s.runSteps(ex["steps"].([]any))
		}
		return ctl{}, nil
	}
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type policy struct {
	predicate    string // the name inside ${...}
	max          int
	initial, top float64 // seconds
	mult         float64
}

func (p *policy) delay(attempt int) time.Duration {
	d := math.Min(p.initial*math.Pow(p.mult, float64(attempt)), p.top)
	return time.Duration(d * float64(time.Second))
}

func (s *scope) retryPolicy(v any) (*policy, error) {
	if v == nil {
		return nil, nil
	}
	if str, ok := v.(string); ok {
		switch strings.TrimSpace(strings.Trim(str, "${}")) {
		case "http.default_retry":
			return &policy{predicate: "http.default_retry_predicate", max: 5, initial: 1, top: 60, mult: 1.25}, nil
		case "http.default_retry_non_idempotent":
			return &policy{predicate: "http.default_retry_predicate_non_idempotent", max: 5, initial: 1, top: 60, mult: 1.25}, nil
		}
		return nil, unsupported("the retry policy %s", str)
	}
	m, ok := v.(map[string]any)
	if !ok {
		return nil, newErr("retry must be a map or a policy name", "ValueError")
	}
	p := &policy{initial: 1, top: 60, mult: 1.25}
	if pr, ok := m["predicate"].(string); ok {
		p.predicate = pr
	}
	// the predicate is a name, not a value: only the numbers are evaluated
	max, err := s.evalValue(m["max_retries"])
	if err != nil {
		return nil, err
	}
	if n, ok := max.(int64); ok {
		p.max = int(n)
	}
	bov, err := s.evalValue(m["backoff"])
	if err != nil {
		return nil, err
	}
	if bo, ok := bov.(map[string]any); ok {
		for k, dst := range map[string]*float64{"initial_delay": &p.initial, "max_delay": &p.top, "multiplier": &p.mult} {
			if f, ok := num(bo[k]); ok {
				*dst = f
			}
		}
	}
	return p, nil
}

// retry asks the policy's predicate whether this error is worth another try.
func (p *policy) retry(s *scope, errVal any) (bool, error) {
	pred := strings.TrimSpace(strings.Trim(p.predicate, "${}"))
	switch pred {
	case "":
		return true, nil
	case "http.default_retry_predicate", "http.default_retry_predicate_non_idempotent":
		m, _ := errVal.(map[string]any)
		code, _ := m["code"].(int64)
		tags, _ := m["tags"].([]any)
		for _, t := range tags {
			if t == "ConnectionError" || t == "TimeoutError" {
				return pred == "http.default_retry_predicate", nil
			}
		}
		if pred == "http.default_retry_predicate" {
			return code == 429 || code == 502 || code == 503 || code == 504, nil
		}
		return code == 429 || code == 503, nil
	}
	r, ok := s.env.prog[pred]
	if !ok || len(r.params) != 1 {
		return false, unsupported("the retry predicate %s (use http.default_retry_predicate or a subworkflow with one parameter)", p.predicate)
	}
	v, err := s.env.callRoutine(pred, r, map[string]any{r.params[0].name: copyOf(errVal)}, s.depth+1)
	if err != nil {
		return false, err
	}
	b, _ := v.(bool)
	return b, nil
}
