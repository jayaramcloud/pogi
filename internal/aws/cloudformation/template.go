package cloudformation

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"net"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/nrndev/pogi/internal/core"
	yaml "go.yaml.in/yaml/v3"
)

func validation(format string, a ...any) error {
	return core.Errorf(400, "ValidationError", format, a...)
}

// parseTemplate reads a JSON or YAML template into plain Go values, turning
// YAML short-form tags (!Ref, !Sub, !GetAtt …) into their long form.
func parseTemplate(body string) (map[string]any, error) {
	v, err := parseDoc(body)
	if err != nil {
		return nil, err
	}
	out, ok := v.(map[string]any)
	if !ok {
		return nil, validation("Template format error: the template must be a map")
	}
	if res, ok := out["Resources"].(map[string]any); !ok || len(res) == 0 {
		return nil, validation("Template format error: At least one Resources member must be defined.")
	}
	return out, nil
}

// parseDoc reads any JSON or YAML document (a template or an include snippet).
func parseDoc(body string) (any, error) {
	t := strings.TrimSpace(body)
	if t == "" {
		return nil, validation("Template body cannot be empty")
	}
	if strings.HasPrefix(t, "{") {
		var out map[string]any
		if err := json.Unmarshal([]byte(t), &out); err != nil {
			return nil, validation("Template format error: JSON not well-formed. (%v)", err)
		}
		return out, nil
	}
	var n yaml.Node
	if err := yaml.Unmarshal([]byte(t), &n); err != nil {
		return nil, validation("Template format error: YAML not well-formed. (%v)", err)
	}
	return fromYAML(&n)
}

// template parses a template, applies its Fn::Transform AWS::Include nodes
// (they pull a snippet from S3 into the place of the node) and the SAM transform.
func (s *Service) template(body string) (map[string]any, error) {
	tmpl, err := parseTemplate(body)
	if err != nil {
		return nil, err
	}
	inc, err := s.include(tmpl)
	if err != nil {
		return nil, err
	}
	out, _ := inc.(map[string]any)
	if err := s.transform(out); err != nil { // the SAM transform
		return nil, err
	}
	return out, nil
}

// include walks v and replaces each Fn::Transform AWS::Include node by its snippet:
// a map snippet is merged into the node's parent map (keys already there win),
// anything else replaces the parent when the node is all it holds.
func (s *Service) include(v any) (any, error) {
	switch x := v.(type) {
	case []any:
		for i := range x {
			r, err := s.include(x[i])
			if err != nil {
				return nil, err
			}
			x[i] = r
		}
	case map[string]any:
		if tr, ok := x["Fn::Transform"].(map[string]any); ok && str(tr["Name"]) == "AWS::Include" {
			params, _ := tr["Parameters"].(map[string]any)
			snippet, err := s.snippet(str(params["Location"]))
			if err != nil {
				return nil, err
			}
			delete(x, "Fn::Transform")
			if m, ok := snippet.(map[string]any); ok {
				for k, sv := range m {
					if _, has := x[k]; !has {
						x[k] = sv
					}
				}
			} else if len(x) == 0 {
				return s.include(snippet)
			}
		}
		for k := range x {
			r, err := s.include(x[k])
			if err != nil {
				return nil, err
			}
			x[k] = r
		}
	}
	return v, nil
}

func (s *Service) snippet(location string) (any, error) {
	if location == "" {
		return nil, validation("Template format error: AWS::Include needs a Location")
	}
	body, err := s.templateBody("", location)
	if err != nil {
		return nil, err
	}
	return parseDoc(body)
}

func fromYAML(n *yaml.Node) (any, error) {
	switch n.Kind {
	case yaml.DocumentNode:
		if len(n.Content) == 0 {
			return nil, nil
		}
		return fromYAML(n.Content[0])
	case yaml.AliasNode:
		return fromYAML(n.Alias)
	}
	custom := strings.HasPrefix(n.Tag, "!") && !strings.HasPrefix(n.Tag, "!!")
	var v any
	switch n.Kind {
	case yaml.MappingNode:
		m := map[string]any{}
		for i := 0; i+1 < len(n.Content); i += 2 {
			val, err := fromYAML(n.Content[i+1])
			if err != nil {
				return nil, err
			}
			m[n.Content[i].Value] = val
		}
		v = m
	case yaml.SequenceNode:
		l := []any{}
		for _, c := range n.Content {
			x, err := fromYAML(c)
			if err != nil {
				return nil, err
			}
			l = append(l, x)
		}
		v = l
	case yaml.ScalarNode:
		if custom {
			v = n.Value
			break
		}
		switch n.ShortTag() {
		case "!!int":
			i, err := strconv.ParseInt(n.Value, 0, 64)
			if err != nil {
				v = n.Value
			} else {
				v = float64(i)
			}
		case "!!float":
			f, _ := strconv.ParseFloat(n.Value, 64)
			v = f
		case "!!bool":
			v = n.Value == "true" || n.Value == "True" || n.Value == "TRUE"
		case "!!null":
			v = nil
		default:
			v = n.Value
		}
	}
	if !custom {
		return v, nil
	}
	switch name := n.Tag[1:]; name {
	case "Ref", "Condition":
		return map[string]any{name: v}, nil
	case "GetAtt":
		if s, ok := v.(string); ok {
			if i := strings.Index(s, "."); i > 0 {
				v = []any{s[:i], s[i+1:]}
			}
		}
		return map[string]any{"Fn::GetAtt": v}, nil
	default:
		return map[string]any{"Fn::" + name: v}, nil
	}
}

// noValue is AWS::NoValue: the property disappears.
type noValue struct{}

// evaluator resolves intrinsic functions against a stack's state.
type evaluator struct {
	account, region, stack, stackID string
	params                          map[string]any // string or []any for list types
	condDefs                        map[string]any
	conds                           map[string]bool
	mappings                        map[string]any
	resources                       map[string]*Resource
	imports                         func(name string) (string, bool)
}

func (e *evaluator) eval(v any) (any, error) {
	switch x := v.(type) {
	case map[string]any:
		if len(x) == 1 {
			for k, arg := range x {
				if k == "Ref" || k == "Condition" || strings.HasPrefix(k, "Fn::") {
					return e.intrinsic(k, arg)
				}
			}
		}
		out := make(map[string]any, len(x))
		for k, val := range x {
			r, err := e.eval(val)
			if err != nil {
				return nil, err
			}
			if _, skip := r.(noValue); !skip {
				out[k] = r
			}
		}
		return out, nil
	case []any:
		out := make([]any, 0, len(x))
		for _, val := range x {
			r, err := e.eval(val)
			if err != nil {
				return nil, err
			}
			if _, skip := r.(noValue); !skip {
				out = append(out, r)
			}
		}
		return out, nil
	}
	return v, nil
}

func args(fn string, arg any, n int) ([]any, error) {
	l, ok := arg.([]any)
	if !ok || n > 0 && len(l) != n {
		return nil, validation("Template error: %s requires a list of %d arguments", fn, n)
	}
	return l, nil
}

func str(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(x)
	case nil:
		return ""
	}
	b, _ := json.Marshal(v)
	return string(b)
}

func (e *evaluator) intrinsic(fn string, arg any) (any, error) {
	switch fn {
	case "Ref":
		name, _ := arg.(string)
		return e.ref(name)
	case "Condition":
		name, _ := arg.(string)
		return e.condition(name)
	case "Fn::GetAtt":
		var parts []string
		switch g := arg.(type) {
		case string:
			a, b, _ := strings.Cut(g, ".")
			parts = []string{a, b}
		case []any:
			for _, p := range g {
				ev, err := e.eval(p)
				if err != nil {
					return nil, err
				}
				parts = append(parts, str(ev))
			}
		}
		if len(parts) < 2 {
			return nil, validation("Template error: every Fn::GetAtt object requires two non-empty parameters")
		}
		return e.getAtt(parts[0], strings.Join(parts[1:], "."))
	case "Fn::Sub":
		return e.sub(arg)
	case "Fn::Join":
		l, err := args(fn, arg, 2)
		if err != nil {
			return nil, err
		}
		items, err := e.eval(l[1])
		if err != nil {
			return nil, err
		}
		list, ok := items.([]any)
		if !ok {
			return nil, validation("Template error: Fn::Join requires a list as its second argument")
		}
		var parts []string
		for _, it := range list {
			parts = append(parts, str(it))
		}
		return strings.Join(parts, str(l[0])), nil
	case "Fn::Select":
		l, err := args(fn, arg, 2)
		if err != nil {
			return nil, err
		}
		idx, err := e.eval(l[0])
		if err != nil {
			return nil, err
		}
		items, err := e.eval(l[1])
		if err != nil {
			return nil, err
		}
		list, _ := items.([]any)
		i, err := strconv.Atoi(str(idx))
		if err != nil || i < 0 || i >= len(list) {
			return nil, validation("Template error: Fn::Select cannot select nonexistent value at index %s", str(idx))
		}
		return list[i], nil
	case "Fn::Split":
		l, err := args(fn, arg, 2)
		if err != nil {
			return nil, err
		}
		s, err := e.eval(l[1])
		if err != nil {
			return nil, err
		}
		var out []any
		for _, p := range strings.Split(str(s), str(l[0])) {
			out = append(out, p)
		}
		return out, nil
	case "Fn::If":
		l, err := args(fn, arg, 3)
		if err != nil {
			return nil, err
		}
		ok, err := e.condition(str(l[0]))
		if err != nil {
			return nil, err
		}
		if ok {
			return e.eval(l[1])
		}
		return e.eval(l[2])
	case "Fn::Equals":
		l, err := args(fn, arg, 2)
		if err != nil {
			return nil, err
		}
		a, err := e.eval(l[0])
		if err != nil {
			return nil, err
		}
		b, err := e.eval(l[1])
		if err != nil {
			return nil, err
		}
		return str(a) == str(b), nil
	case "Fn::And", "Fn::Or":
		l, err := args(fn, arg, 0)
		if err != nil {
			return nil, err
		}
		result := fn == "Fn::And"
		for _, c := range l {
			v, err := e.eval(c)
			if err != nil {
				return nil, err
			}
			b, _ := v.(bool)
			if fn == "Fn::And" {
				result = result && b
			} else {
				result = result || b
			}
		}
		return result, nil
	case "Fn::Not":
		l, err := args(fn, arg, 1)
		if err != nil {
			return nil, err
		}
		v, err := e.eval(l[0])
		if err != nil {
			return nil, err
		}
		b, _ := v.(bool)
		return !b, nil
	case "Fn::FindInMap":
		l, err := args(fn, arg, 0)
		if err != nil || len(l) < 3 {
			return nil, validation("Template error: Fn::FindInMap requires 3 arguments")
		}
		var keys []string
		for _, k := range l[:3] {
			v, err := e.eval(k)
			if err != nil {
				return nil, err
			}
			keys = append(keys, str(v))
		}
		m, _ := e.mappings[keys[0]].(map[string]any)
		top, _ := m[keys[1]].(map[string]any)
		if v, ok := top[keys[2]]; ok {
			return v, nil
		}
		if len(l) == 4 { // AWS::LanguageExtensions DefaultValue
			if d, ok := l[3].(map[string]any); ok {
				return e.eval(d["DefaultValue"])
			}
		}
		return nil, validation("Template error: Unable to get mapping for %s::%s::%s", keys[0], keys[1], keys[2])
	case "Fn::Base64":
		v, err := e.eval(arg)
		if err != nil {
			return nil, err
		}
		return base64.StdEncoding.EncodeToString([]byte(str(v))), nil
	case "Fn::GetAZs":
		r, err := e.eval(arg)
		if err != nil {
			return nil, err
		}
		region := str(r)
		if region == "" {
			region = e.region
		}
		return []any{region + "a", region + "b", region + "c"}, nil
	case "Fn::ImportValue":
		v, err := e.eval(arg)
		if err != nil {
			return nil, err
		}
		if val, ok := e.imports(str(v)); ok {
			return val, nil
		}
		return nil, validation("No export named %s found.", str(v))
	case "Fn::Cidr":
		l, err := args(fn, arg, 3)
		if err != nil {
			return nil, err
		}
		ev, err := e.eval(l)
		if err != nil {
			return nil, err
		}
		el := ev.([]any)
		return cidr(str(el[0]), str(el[1]), str(el[2]))
	case "Fn::ToJsonString":
		v, err := e.eval(arg)
		if err != nil {
			return nil, err
		}
		b, _ := json.Marshal(v)
		return string(b), nil
	case "Fn::Length":
		v, err := e.eval(arg)
		if err != nil {
			return nil, err
		}
		l, _ := v.([]any)
		return float64(len(l)), nil
	case "Fn::Contains": // rules
		l, err := args(fn, arg, 2)
		if err != nil {
			return nil, err
		}
		ev, err := e.eval(l)
		if err != nil {
			return nil, err
		}
		el := ev.([]any)
		list, _ := el[0].([]any)
		for _, x := range list {
			if str(x) == str(el[1]) {
				return true, nil
			}
		}
		return false, nil
	case "Fn::EachMemberEquals", "Fn::EachMemberIn": // rules
		l, err := args(fn, arg, 2)
		if err != nil {
			return nil, err
		}
		ev, err := e.eval(l)
		if err != nil {
			return nil, err
		}
		el := ev.([]any)
		list, _ := el[0].([]any)
		for _, x := range list {
			if fn == "Fn::EachMemberEquals" && str(x) != str(el[1]) {
				return false, nil
			}
			if fn == "Fn::EachMemberIn" && !containsAny(el[1], x) {
				return false, nil
			}
		}
		return true, nil
	case "Fn::RefAll", "Fn::ValueOf", "Fn::ValueOfAll":
		return []any{}, nil
	}
	return nil, validation("Template format error: Unsupported intrinsic function %s", fn)
}

func containsAny(list any, x any) bool {
	l, _ := list.([]any)
	for _, v := range l {
		if str(v) == str(x) {
			return true
		}
	}
	return false
}

func (e *evaluator) ref(name string) (any, error) {
	switch name {
	case "AWS::AccountId":
		return e.account, nil
	case "AWS::Region":
		return e.region, nil
	case "AWS::StackName":
		return e.stack, nil
	case "AWS::StackId":
		return e.stackID, nil
	case "AWS::Partition":
		return core.Partition(e.region), nil
	case "AWS::URLSuffix":
		return core.URLSuffix(e.region), nil
	case "AWS::NotificationARNs":
		return []any{}, nil
	case "AWS::NoValue":
		return noValue{}, nil
	}
	if v, ok := e.params[name]; ok {
		return v, nil
	}
	if r := e.resources[name]; r != nil && r.PhysicalID != "" {
		return r.PhysicalID, nil
	}
	return nil, validation("Template format error: Unresolved resource dependencies [%s] in the Resources block of the template", name)
}

func (e *evaluator) getAtt(logical, attr string) (any, error) {
	r := e.resources[logical]
	if r == nil || r.PhysicalID == "" {
		return nil, validation("Template format error: Unresolved resource dependencies [%s] in the Resources block of the template", logical)
	}
	if v, ok := r.Attrs[attr]; ok {
		return v, nil
	}
	return nil, validation("Template error: resource %s does not support attribute type %s in Fn::GetAtt", logical, attr)
}

var subVar = regexp.MustCompile(`\$\{([^}]*)\}`)

func (e *evaluator) sub(arg any) (any, error) {
	tmpl, vars := "", map[string]any{}
	switch x := arg.(type) {
	case string:
		tmpl = x
	case []any:
		if len(x) != 2 {
			return nil, validation("Template error: Fn::Sub requires a string and a map")
		}
		tmpl = str(x[0])
		if m, ok := x[1].(map[string]any); ok {
			for k, v := range m {
				ev, err := e.eval(v)
				if err != nil {
					return nil, err
				}
				vars[k] = ev
			}
		}
	}
	var firstErr error
	out := subVar.ReplaceAllStringFunc(tmpl, func(m string) string {
		name := m[2 : len(m)-1]
		if strings.HasPrefix(name, "!") {
			return "${" + name[1:] + "}"
		}
		if v, ok := vars[name]; ok {
			return str(v)
		}
		var v any
		var err error
		if logical, attr, ok := strings.Cut(name, "."); ok && !strings.Contains(name, "::") {
			v, err = e.getAtt(logical, attr)
		} else {
			v, err = e.ref(name)
		}
		if err != nil && firstErr == nil {
			firstErr = err
		}
		if l, ok := v.([]any); ok {
			var parts []string
			for _, p := range l {
				parts = append(parts, str(p))
			}
			return strings.Join(parts, ",")
		}
		return str(v)
	})
	return out, firstErr
}

func (e *evaluator) condition(name string) (bool, error) {
	if v, ok := e.conds[name]; ok {
		return v, nil
	}
	def, ok := e.condDefs[name]
	if !ok {
		return false, validation("Template format error: Unresolved condition dependency %s in Fn::If", name)
	}
	e.conds[name] = false // guards against cycles
	v, err := e.eval(def)
	if err != nil {
		return false, err
	}
	b, _ := v.(bool)
	e.conds[name] = b
	return b, nil
}

// deps lists the logical ids a template fragment refers to.
func deps(v any, out map[string]bool) {
	switch x := v.(type) {
	case map[string]any:
		if len(x) == 1 {
			if r, ok := x["Ref"].(string); ok {
				out[r] = true
				return
			}
			if ga, ok := x["Fn::GetAtt"]; ok {
				switch g := ga.(type) {
				case []any:
					if len(g) > 0 {
						if s, ok := g[0].(string); ok {
							out[s] = true
						}
					}
				case string:
					a, _, _ := strings.Cut(g, ".")
					out[a] = true
				}
				return
			}
			if sub, ok := x["Fn::Sub"]; ok {
				tmpl := ""
				switch s := sub.(type) {
				case string:
					tmpl = s
				case []any:
					if len(s) > 0 {
						tmpl = str(s[0])
					}
					if len(s) > 1 {
						deps(s[1], out)
					}
				}
				for _, m := range subVar.FindAllStringSubmatch(tmpl, -1) {
					if !strings.HasPrefix(m[1], "!") && !strings.Contains(m[1], "::") {
						a, _, _ := strings.Cut(m[1], ".")
						out[a] = true
					}
				}
				return
			}
		}
		for _, val := range x {
			deps(val, out)
		}
	case []any:
		for _, val := range x {
			deps(val, out)
		}
	}
}

// order sorts resources so dependencies come first.
func order(resources map[string]any) ([]string, error) {
	edges := map[string][]string{}
	for id, def := range resources {
		d, _ := def.(map[string]any)
		found := map[string]bool{}
		deps(d["Properties"], found)
		switch dep := d["DependsOn"].(type) {
		case string:
			found[dep] = true
		case []any:
			for _, x := range dep {
				found[str(x)] = true
			}
		}
		for k := range found {
			if _, ok := resources[k]; ok && k != id {
				edges[id] = append(edges[id], k)
			}
		}
	}
	var out []string
	state := map[string]int{} // 1 visiting, 2 done
	var visit func(string) error
	visit = func(id string) error {
		switch state[id] {
		case 1:
			return validation("Circular dependency between resources: [%s]", id)
		case 2:
			return nil
		}
		state[id] = 1
		sort.Strings(edges[id])
		for _, d := range edges[id] {
			if err := visit(d); err != nil {
				return err
			}
		}
		state[id] = 2
		out = append(out, id)
		return nil
	}
	for _, id := range core.SortedKeys(resources) {
		if err := visit(id); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// cidr implements Fn::Cidr for IPv4.
func cidr(block, count, bits string) ([]any, error) {
	_, n, err := net.ParseCIDR(block)
	if err != nil || n.IP.To4() == nil {
		return nil, validation("Template error: Fn::Cidr needs an IPv4 CIDR, got %q", block)
	}
	c, _ := strconv.Atoi(count)
	b, _ := strconv.Atoi(bits)
	prefix := 32 - b
	base := new(big.Int).SetBytes(n.IP.To4())
	var out []any
	for i := 0; i < c; i++ {
		ip := new(big.Int).Add(base, new(big.Int).Lsh(big.NewInt(int64(i)), uint(b)))
		raw := ip.FillBytes(make([]byte, 4))
		out = append(out, fmt.Sprintf("%s/%d", net.IP(raw).String(), prefix))
	}
	return out, nil
}
