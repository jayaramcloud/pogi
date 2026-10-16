package stepfunctions

// JSONata in the interpreter: {% %} expressions in Arguments, Output, Assign,
// Items, Condition and the other fields, the $states variable, state
// variables (Assign), and the create-time checks for both query languages.

import (
	"fmt"
	"strings"
	"unicode"

	"github.com/nrndev/pogi/internal/core"
)

const queryErr = "States.QueryEvaluationError"

// bind is what $states holds while an expression runs.
type bind struct {
	input     any // $states.input, and the context value of the expression
	result    any // $states.result (Output and Assign of Task, Parallel and Map)
	hasResult bool
	errOut    any // $states.errorOutput (Catch)
	hasErr    bool
}

// jsonataExpr returns the expression inside {% %}.
func jsonataExpr(s string) (string, bool) {
	if len(s) >= 4 && strings.HasPrefix(s, "{%") && strings.HasSuffix(s, "%}") {
		return s[2 : len(s)-2], true
	}
	return "", false
}

// jeval evaluates one expression; an error or an undefined result is a States.QueryEvaluationError.
func (t *thread) jeval(src, field string, b bind) (any, error) {
	states := map[string]any{"input": b.input, "context": t.ctxObj()}
	if b.hasResult {
		states["result"] = b.result
	}
	if b.hasErr {
		states["errorOutput"] = b.errOut
	}
	vars := make(map[string]any, len(t.vars)+1)
	for k, v := range t.vars {
		vars[k] = v
	}
	vars["states"] = states
	v, ok, err := jsonataRun(src, b.input, vars)
	switch {
	case err != nil:
		return nil, t.jfail(src, field, "threw an error during evaluation: "+err.Error())
	case !ok:
		return nil, t.jfail(src, field, "returned nothing (undefined).")
	}
	return v, nil
}

// jfail records an EvaluationFailed event and returns the state's error. reason finishes the
// sentence "The JSONata expression ... specified for the field ... <reason>".
func (t *thread) jfail(src, field, reason string) error {
	cause := fmt.Sprintf("An error occurred while executing the state '%s' (entered at the event id #%d). The JSONata expression '%s' specified for the field '%s' %s",
		t.state, t.enterID, src, field, reason)
	t.event("EvaluationFailed", map[string]any{"error": queryErr, "cause": cause, "location": field, "state": t.state})
	return &failure{queryErr, cause}
}

// jtemplate evaluates every {% %} string inside a value (an object, an array or a string).
func (t *thread) jtemplate(v any, field string, b bind) (any, error) {
	switch x := v.(type) {
	case string:
		if src, ok := jsonataExpr(x); ok {
			return t.jeval(src, field, b)
		}
	case map[string]any:
		out := make(map[string]any, len(x))
		for _, k := range core.SortedKeys(x) {
			r, err := t.jtemplate(x[k], field, b)
			if err != nil {
				return nil, err
			}
			out[k] = r
		}
		return out, nil
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			r, err := t.jtemplate(e, field, b)
			if err != nil {
				return nil, err
			}
			out[i] = r
		}
		return out, nil
	}
	return v, nil
}

// jfinish ends a JSONata state: it evaluates Assign and Output against the variables as they
// were when the state began, then sets the variables. def is the output when there is no Output.
func (t *thread) jfinish(assign, output any, b bind, def any) (any, error) {
	var vars any
	var err error
	if assign != nil {
		if vars, err = t.jtemplate(assign, "Assign", b); err != nil {
			return nil, err
		}
	}
	out := def
	if output != nil {
		if out, err = t.jtemplate(output, "Output", b); err != nil {
			return nil, err
		}
	}
	t.setVars(vars)
	return out, nil
}

// jchoice picks the first rule whose Condition is true.
func (t *thread) jchoice(st *State, raw any) (any, string, error) {
	b := bind{input: raw}
	for _, r := range st.Choices {
		var ok bool
		switch c := r["Condition"].(type) {
		case bool:
			ok = c
		case string:
			src, isExpr := jsonataExpr(c)
			if !isExpr {
				return nil, "", runtimeErr("The Condition of a Choice rule must be true, false or a JSONata expression: %q", c)
			}
			v, err := t.jeval(src, "Condition", b)
			if err != nil {
				return nil, "", err
			}
			var isBool bool
			if ok, isBool = v.(bool); !isBool {
				return nil, "", t.jfail(src, "Condition", "returned a value that is not a boolean: "+js(v))
			}
		}
		if ok {
			next, _ := r["Next"].(string)
			out, err := t.jfinish(r["Assign"], r["Output"], b, raw)
			return out, next, err
		}
	}
	if st.Default == "" {
		return nil, "", &failure{"States.NoChoiceMatched", "No Choice Rules matched and no Default was specified"}
	}
	out, err := t.jfinish(st.Assign, st.Output, b, raw)
	return out, st.Default, err
}

// numField reads a number field that may be an expression in a JSONata state. It is 0 when not set.
func (t *thread) numField(st *State, n num, field string, raw any) (float64, error) {
	if n.expr == "" {
		return n.f, nil
	}
	src, ok := jsonataExpr(n.expr)
	if !ok || st.lang != "JSONata" {
		return 0, runtimeErr("The field '%s' must be a number", field)
	}
	v, err := t.jeval(src, field, bind{input: raw})
	if err != nil {
		return 0, err
	}
	f, isNum := v.(float64)
	if !isNum {
		return 0, t.jfail(src, field, "returned a value that is not a number: "+js(v))
	}
	return f, nil
}

// assignPath handles Assign in a JSONPath state: $ in its paths is the state's result.
func (t *thread) assignPath(assign, in any) error {
	if assign == nil {
		return nil
	}
	vars, err := t.template(assign, in)
	if err == nil {
		t.setVars(vars)
	}
	return err
}

// setVars sets the variables an Assign produced (an object of name → value).
func (t *thread) setVars(v any) {
	m, ok := v.(map[string]any)
	if !ok || len(m) == 0 {
		return
	}
	if t.vars == nil {
		t.vars = map[string]any{}
	}
	for k, x := range m {
		t.vars[k] = x
	}
	t.assigned = m
}

// variable reads $name or $name.path in a JSONPath state.
func (t *thread) variable(p string) (any, bool, error) {
	end := strings.IndexAny(p, ".[")
	if end < 0 {
		end = len(p)
	}
	v, ok := t.vars[p[1:end]]
	if !ok {
		return nil, false, nil
	}
	segs, err := parsePath("$" + p[end:])
	if err != nil {
		return nil, false, runtimeErr("%v", err)
	}
	r, found := get(v, segs, v)
	return r, found, nil
}

// isVariable tells $name (a variable) from $, $$ and $.path.
func isVariable(p string) bool {
	if len(p) < 2 || p[0] != '$' {
		return false
	}
	r := []rune(p[1:])[0]
	return r == '_' || unicode.IsLetter(r)
}

// ---- create-time checks ----

func isLang(s string) bool { return s == "" || s == "JSONPath" || s == "JSONata" }

// validateLang checks the fields a state may use in its query language (top is the state machine's), and the JSONata expressions.
func (st *State) validateLang(loc, top string) (out []diag) {
	bad := func(code, field, format string, a ...any) {
		out = append(out, diag{"ERROR", code, fmt.Sprintf(format, a...), loc + field})
	}
	if !isLang(st.QueryLanguage) {
		bad("SCHEMA_VALIDATION_FAILED", "/QueryLanguage", "The field 'QueryLanguage' should be JSONPath or JSONata")
		return
	}
	if top == "JSONata" && st.QueryLanguage == "JSONPath" { // AWS accepts JSONata inside a JSONPath machine, not the other way round
		bad("SCHEMA_VALIDATION_FAILED", "/QueryLanguage", "The field 'QueryLanguage' of a state cannot be JSONPath when the state machine uses JSONata")
	}
	jsonata := st.lang == "JSONata"
	only := func(field string, present bool, want string) { // a field that belongs to one language
		if present && jsonata != (want == "JSONata") {
			bad("SCHEMA_VALIDATION_FAILED", "/"+field, "The field '%s' is only allowed when the query language is %s", field, want)
		}
	}
	for _, f := range []struct {
		name    string
		present bool
	}{{"InputPath", st.InputPath.isSet()}, {"OutputPath", st.OutputPath.isSet()}, {"ResultPath", st.ResultPath.isSet()}, {"ItemsPath", st.ItemsPath.isSet()},
		{"Parameters", st.Parameters != nil}, {"ResultSelector", st.ResultSelector != nil}, {"Result", st.Result != nil},
		{"SecondsPath", st.SecondsPath != ""}, {"TimestampPath", st.TimestampPath != ""}, {"TimeoutSecondsPath", st.TimeoutSecondsPath != ""},
		{"ErrorPath", st.ErrorPath != ""}, {"CausePath", st.CausePath != ""}, {"MaxConcurrencyPath", st.MaxConcurrencyPath != ""},
		{"ToleratedFailureCountPath", st.ToleratedFailureCountPath != ""}, {"ToleratedFailurePercentagePath", st.ToleratedFailurePercentagePath != ""}} {
		only(f.name, f.present, "JSONPath")
	}
	for _, f := range []struct {
		name    string
		present bool
	}{{"Arguments", st.Arguments != nil}, {"Output", st.Output != nil}, {"Items", st.Items != nil}} {
		only(f.name, f.present, "JSONata")
	}
	for i, c := range st.Catch {
		cl := fmt.Sprintf("/Catch/%d/", i)
		only(cl[1:]+"ResultPath", c.ResultPath.isSet(), "JSONPath")
		only(cl[1:]+"Output", c.Output != nil, "JSONata")
	}
	for i, r := range st.Choices {
		for k := range r {
			switch k {
			case "Next", "Assign", "Comment":
			case "Output", "Condition":
				only(fmt.Sprintf("Choices/%d/%s", i, k), true, "JSONata")
			default:
				only(fmt.Sprintf("Choices/%d/%s", i, k), true, "JSONPath")
			}
		}
	}
	if !jsonata {
		for _, n := range []num{st.Seconds, st.TimeoutSeconds, st.HeartbeatSeconds, st.MaxConcurrency, st.ToleratedFailureCount, st.ToleratedFailurePercentage} {
			if n.expr != "" {
				bad("SCHEMA_VALIDATION_FAILED", "", "A number field is a string: expressions need the JSONata query language")
			}
		}
		return
	}
	// JSONata: a field name that ends in .$ is a JSONPath form, and every expression must parse.
	check := func(field string, v any) {
		var walk func(v any)
		walk = func(v any) {
			switch x := v.(type) {
			case string:
				if src, ok := jsonataExpr(x); ok {
					if _, err := jsonataParse(src); err != nil {
						bad("INVALID_JSONATA_EXPRESSION", field, "Invalid JSONata expression '%s': %v", strings.TrimSpace(src), err)
					}
				}
			case map[string]any:
				for _, k := range core.SortedKeys(x) {
					if strings.HasSuffix(k, ".$") {
						bad("SCHEMA_VALIDATION_FAILED", field, "The field '%s' ends with .$, which is only allowed when the query language is JSONPath", k)
					}
					walk(x[k])
				}
			case []any:
				for _, e := range x {
					walk(e)
				}
			}
		}
		walk(v)
	}
	for _, f := range []struct {
		name string
		v    any
	}{{"Arguments", st.Arguments}, {"Output", st.Output}, {"Assign", st.Assign}, {"Items", st.Items}, {"ItemSelector", st.ItemSelector},
		{"Seconds", st.Seconds.expr}, {"TimeoutSeconds", st.TimeoutSeconds.expr}, {"HeartbeatSeconds", st.HeartbeatSeconds.expr},
		{"MaxConcurrency", st.MaxConcurrency.expr}, {"Timestamp", st.Timestamp}, {"Error", st.Error}, {"Cause", st.Cause}} {
		check(loc2(loc, f.name), f.v)
	}
	for i, c := range st.Catch {
		check(fmt.Sprintf("%s/Catch/%d/Output", loc, i), c.Output)
		check(fmt.Sprintf("%s/Catch/%d/Assign", loc, i), c.Assign)
	}
	for i, r := range st.Choices {
		for _, k := range []string{"Condition", "Output", "Assign"} {
			check(fmt.Sprintf("%s/Choices/%d/%s", loc, i, k), r[k])
		}
	}
	return
}

func loc2(loc, field string) string { return loc + "/" + field }
