package terraform

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/nrndev/pogi/internal/core"
)

const planHint = "make one with: terraform plan -out plan.out && terraform show -json plan.out > plan.json"

// The parts of "terraform show -json" that pogi reads. Nothing here is logged or kept.
type planFile struct {
	FormatVersion    string `json:"format_version"`
	TerraformVersion string `json:"terraform_version"`
	Variables        map[string]struct {
		Value any `json:"value"`
	} `json:"variables"`
	PlannedValues struct {
		RootModule planModule `json:"root_module"`
	} `json:"planned_values"`
	ResourceChanges []planChange `json:"resource_changes"`
	PriorState      struct {
		Values struct {
			RootModule planModule `json:"root_module"`
		} `json:"values"`
	} `json:"prior_state"`
	Configuration struct {
		ProviderConfig map[string]struct {
			Expressions map[string]any `json:"expressions"`
		} `json:"provider_config"`
		RootModule confModule `json:"root_module"`
	} `json:"configuration"`
}

type planModule struct {
	Resources    []planRes    `json:"resources"`
	ChildModules []planModule `json:"child_modules"`
}

type planRes struct {
	Address   string         `json:"address"`
	Values    map[string]any `json:"values"`
	DependsOn []string       `json:"depends_on"` // in prior_state: what Terraform recorded at the last apply
}

type planChange struct {
	Address       string `json:"address"`
	ModuleAddress string `json:"module_address"`
	Mode          string `json:"mode"`
	Type          string `json:"type"`
	Name          string `json:"name"`
	Change        struct {
		Actions         []string       `json:"actions"`
		Before          map[string]any `json:"before"`
		After           map[string]any `json:"after"`
		AfterUnknown    any            `json:"after_unknown"`
		BeforeSensitive any            `json:"before_sensitive"`
		AfterSensitive  any            `json:"after_sensitive"`
	} `json:"change"`
}

type confModule struct {
	Resources   []confRes             `json:"resources"`
	ModuleCalls map[string]confCall   `json:"module_calls"`
	Outputs     map[string]confOutput `json:"outputs"`
}

type confRes struct {
	Mode              string         `json:"mode"`
	Type              string         `json:"type"`
	Name              string         `json:"name"`
	ProviderConfigKey string         `json:"provider_config_key"`
	Expressions       map[string]any `json:"expressions"`
	CountExpression   any            `json:"count_expression"`
	ForEachExpression any            `json:"for_each_expression"`
	DependsOn         []string       `json:"depends_on"`
}

type confCall struct {
	Expressions map[string]any `json:"expressions"`
	Module      confModule     `json:"module"`
}

type confOutput struct {
	Expression any      `json:"expression"`
	DependsOn  []string `json:"depends_on"`
}

// IsPlan reports whether data looks like a plan JSON file.
func IsPlan(data []byte) bool {
	var f map[string]json.RawMessage
	if json.Unmarshal(data, &f) != nil {
		return false
	}
	_, v := f["format_version"]
	_, pv := f["planned_values"]
	_, rc := f["resource_changes"]
	_, c := f["configuration"]
	return v && (pv || rc || c)
}

// ReadPlan reads the JSON that "terraform show -json plan.out" writes.
func ReadPlan(data []byte) (*Config, error) {
	if !json.Valid(data) && len(bytes.TrimSpace(data)) > 0 {
		var v any
		return nil, fmt.Errorf("this is not a Terraform plan: %s: %s", core.PlainJSONOf("the file", json.Unmarshal(data, &v)), planHint)
	}
	if !IsPlan(data) {
		return nil, fmt.Errorf("this is not a Terraform plan in JSON: %s", planHint)
	}
	var f planFile
	if err := json.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("the plan JSON could not be read: %s", core.PlainJSONOf("the plan", err))
	}
	if !strings.HasPrefix(f.FormatVersion, "1.") {
		return nil, fmt.Errorf("plan format_version %q is not supported (pogi reads 1.x): %s", f.FormatVersion, planHint)
	}
	cfg := &Config{Source: "plan", Version: f.TerraformVersion}
	planned := map[string]map[string]any{}
	var order []string
	var walk func(m planModule)
	walk = func(m planModule) {
		for _, r := range m.Resources {
			planned[r.Address] = r.Values
			order = append(order, r.Address)
		}
		for _, c := range m.ChildModules {
			walk(c)
		}
	}
	walk(f.PlannedValues.RootModule)

	mods := map[string]*confMod{}
	indexModules(&f.Configuration.RootModule, "", nil, nil, mods)
	known := map[string]bool{}
	for _, m := range mods {
		for _, r := range m.mod.Resources {
			known[m.blockOf(r)] = true
		}
	}
	for _, c := range f.ResourceChanges { // a deleted resource is no longer in the configuration
		known[planBlock(c.ModuleAddress, c.Mode, c.Type, c.Name)] = true
	}
	byBlock := map[string]*confRes{}
	for _, m := range mods {
		for i := range m.mod.Resources {
			r := &m.mod.Resources[i]
			byBlock[m.blockOf(*r)] = r
		}
	}
	// A deleted resource is no longer in the configuration, but the state Terraform recorded at the
	// last apply still lists what it depends on (references included).
	prior := map[string][]string{}
	var walkPrior func(m planModule)
	walkPrior = func(m planModule) {
		for _, r := range m.Resources {
			prior[r.Address] = r.DependsOn
		}
		for _, c := range m.ChildModules {
			walkPrior(c)
		}
	}
	walkPrior(f.PriorState.Values.RootModule)
	deps := func(address, block string) []string {
		r := byBlock[block]
		if r == nil {
			var out []string
			for _, d := range prior[address] {
				if b := stripKeys(d); known[b] && b != block && !slices.Contains(out, b) {
					out = append(out, b)
				}
			}
			sort.Strings(out)
			return out
		}
		return resolveDeps(mods, known, modPathOf(block, r), r)
	}
	seen := map[string]bool{}
	add := func(address, moduleAddr, mode, typ, name string, actions []string, before, after map[string]any, unk, bsens, asens any) {
		seen[address] = true
		module := stripKeys(moduleAddr)
		block := planBlock(moduleAddr, mode, typ, name)
		action := actionOf(actions)
		r := &Resource{Address: address, Block: block, Module: module, Type: typ, Name: name, Data: mode == "data",
			Cloud: cloudOf(typ), Action: action}
		if action == "delete" {
			r.Attrs = maskMap(before, bsens, false)
		} else {
			if v, ok := planned[address]; ok && v != nil {
				after = v
			}
			r.Attrs = maskMap(maskMap(after, unk, true), asens, false)
			if before != nil {
				r.Before = maskMap(before, bsens, false)
			}
		}
		r.Deps = deps(address, block)
		key := cloudProvider(typ)
		if c := byBlock[block]; c != nil {
			key = providerKey(c)
		}
		r.Region = regionOf(r.Cloud, r.Attrs, providerValues(&f, key))
		cfg.Resources = append(cfg.Resources, r)
	}
	for _, c := range f.ResourceChanges {
		add(c.Address, c.ModuleAddress, c.Mode, c.Type, c.Name, c.Change.Actions, c.Change.Before, c.Change.After,
			c.Change.AfterUnknown, c.Change.BeforeSensitive, c.Change.AfterSensitive)
	}
	for _, a := range order { // resources the plan lists but has no change for
		if !seen[a] {
			typ, name, mode, module := splitAddress(a)
			add(a, module, mode, typ, name, []string{"no-op"}, nil, nil, nil, nil, nil)
		}
	}
	// Data sources Terraform read during the plan are only in the configuration.
	have := map[string]bool{}
	for _, r := range cfg.Resources {
		have[r.Block] = true
	}
	blocks := make([]string, 0, len(byBlock))
	for b := range byBlock {
		blocks = append(blocks, b)
	}
	sort.Strings(blocks)
	for _, b := range blocks {
		c := byBlock[b]
		if c.Mode != "data" || have[b] {
			continue
		}
		cfg.Resources = append(cfg.Resources, &Resource{Address: b, Block: b, Module: modPathOf(b, c), Type: c.Type, Name: c.Name,
			Data: true, Cloud: cloudOf(c.Type), Attrs: map[string]any{}, Deps: deps(b, b), Action: "read",
			Region: regionOf(cloudOf(c.Type), nil, providerValues(&f, providerKey(c)))})
	}
	inheritRegions(cfg.Resources)
	return cfg, nil
}

func providerKey(c *confRes) string {
	if c.ProviderConfigKey != "" {
		return c.ProviderConfigKey
	}
	return cloudProvider(c.Type)
}

// planBlock is a resource's Block: its address without module or instance keys.
func planBlock(moduleAddr, mode, typ, name string) string {
	b := typ + "." + name
	if mode == "data" {
		b = "data." + b
	}
	if m := stripKeys(moduleAddr); m != "" {
		b = m + "." + b
	}
	return b
}

func actionOf(a []string) string {
	switch strings.Join(a, ",") {
	case "create":
		return "create"
	case "update":
		return "update"
	case "delete":
		return "delete"
	case "delete,create", "create,delete":
		return "replace"
	case "read":
		return "read"
	}
	return "no-op"
}

// splitAddress takes a resource address apart: module.a["k"].aws_x.y[0] → aws_x, y, managed, module.a["k"].
func splitAddress(a string) (typ, name, mode, module string) {
	rest := a
	for strings.HasPrefix(rest, "module.") {
		i := nameEnd(rest, len("module."))
		module = a[:len(a)-len(rest)+i]
		rest = strings.TrimPrefix(rest[i:], ".")
	}
	mode = "managed"
	if strings.HasPrefix(rest, "data.") {
		mode, rest = "data", rest[len("data."):]
	}
	typ, rest, _ = strings.Cut(rest, ".")
	name = stripKeys(rest)
	return
}

// nameEnd returns the end of the module name that starts at i, including its [key].
func nameEnd(s string, i int) int {
	for i < len(s) && s[i] != '.' && s[i] != '[' {
		i++
	}
	if i < len(s) && s[i] == '[' {
		q := false
		for ; i < len(s); i++ {
			if s[i] == '\\' && q {
				i++
			} else if s[i] == '"' {
				q = !q
			} else if s[i] == ']' && !q {
				return i + 1
			}
		}
	}
	return i
}

// stripKeys removes every [key] from an address.
func stripKeys(a string) string {
	var b strings.Builder
	in, q := false, false
	for i := 0; i < len(a); i++ {
		c := a[i]
		switch {
		case !in && c == '[':
			in = true
		case in && q && c == '\\':
			i++
		case in && c == '"':
			q = !q
		case in && !q && c == ']':
			in = false
		case !in:
			b.WriteByte(c)
		}
	}
	return b.String()
}

// maskMap applies a plan's after_unknown (unknown=true: marked values become Unknown) or
// *_sensitive (the marked values are dropped) mask to attrs.
func maskMap(attrs map[string]any, mask any, unknown bool) map[string]any {
	if attrs == nil && !unknown {
		return map[string]any{}
	}
	if attrs == nil {
		attrs = map[string]any{}
	}
	if m, ok := applyMask(attrs, mask, unknown).(map[string]any); ok {
		return m
	}
	return attrs
}

func applyMask(v, mask any, unknown bool) any {
	switch m := mask.(type) {
	case map[string]any:
		vm, _ := v.(map[string]any)
		for k, mv := range m {
			if b, ok := mv.(bool); ok {
				if b && unknown {
					if vm == nil {
						vm = map[string]any{}
					}
					vm[k] = Unknown{}
				} else if b {
					delete(vm, k)
				}
				continue
			}
			if sub := applyMask(vm[k], mv, unknown); sub != nil {
				if vm == nil {
					vm = map[string]any{}
				}
				vm[k] = sub
			}
		}
		if vm == nil {
			return nil
		}
		return vm
	case []any:
		vl, _ := v.([]any)
		for i, mv := range m {
			if b, ok := mv.(bool); ok {
				if b && unknown && i < len(vl) {
					vl[i] = Unknown{}
				}
				continue
			}
			var cur any
			if i < len(vl) {
				cur = vl[i]
			}
			if sub := applyMask(cur, mv, unknown); sub != nil {
				for len(vl) <= i {
					vl = append(vl, map[string]any{})
				}
				vl[i] = sub
			}
		}
		if vl == nil {
			return nil
		}
		return vl
	}
	return v
}

// providerValues reads region and zone from a provider's configuration in the plan: a constant,
// or a reference to a root variable.
func providerValues(f *planFile, key string) map[string]any {
	out := map[string]any{}
	pc, ok := f.Configuration.ProviderConfig[key]
	if !ok {
		return out
	}
	for _, k := range []string{"region", "zone"} {
		e, _ := pc.Expressions[k].(map[string]any)
		if s, ok := e["constant_value"].(string); ok {
			out[k] = s
		}
		if refs, ok := e["references"].([]any); ok {
			for _, r := range refs {
				if name, ok := strings.CutPrefix(fmt.Sprint(r), "var."); ok {
					if s, ok := f.Variables[name].Value.(string); ok {
						out[k] = s
					}
				}
			}
		}
	}
	return out
}

// confMod is a module of the plan's configuration, with the call that made it.
type confMod struct {
	mod    *confModule
	path   string    // module.a.module.b ("" for the root)
	parent *confMod  // the module that holds the call
	call   *confCall // nil for the root
}

func (m *confMod) prefix() string {
	if m.path == "" {
		return ""
	}
	return m.path + "."
}

func (m *confMod) blockOf(r confRes) string {
	b := r.Type + "." + r.Name
	if r.Mode == "data" {
		b = "data." + b
	}
	return m.prefix() + b
}

func indexModules(m *confModule, path string, parent *confMod, call *confCall, out map[string]*confMod) {
	cm := &confMod{mod: m, path: path, parent: parent, call: call}
	out[path] = cm
	for name, c := range m.ModuleCalls {
		c := c
		p := path
		if p != "" {
			p += "."
		}
		indexModules(&c.Module, p+"module."+name, cm, &c, out)
	}
}

func modPathOf(block string, r *confRes) string {
	typ := r.Type + "." + r.Name
	if r.Mode == "data" {
		typ = "data." + typ
	}
	return strings.TrimSuffix(strings.TrimSuffix(block, typ), ".")
}

// refsOf collects every "references" list inside an expression structure, nested ones too.
func refsOf(x any, out *[]string) {
	switch v := x.(type) {
	case map[string]any:
		if r, ok := v["references"].([]any); ok {
			for _, s := range r {
				*out = append(*out, fmt.Sprint(s))
			}
		}
		for k, e := range v {
			if k != "references" {
				refsOf(e, out)
			}
		}
	case []any:
		for _, e := range v {
			refsOf(e, out)
		}
	}
}

// resolveDeps finds the resource Blocks a configured resource refers to, following module inputs
// and outputs. Locals are not in a plan's configuration, so a reference through one is lost.
func resolveDeps(mods map[string]*confMod, known map[string]bool, path string, r *confRes) []string {
	var refs []string
	refsOf(r.Expressions, &refs)
	refsOf(r.CountExpression, &refs)
	refsOf(r.ForEachExpression, &refs)
	refs = append(refs, r.DependsOn...)
	set := depset{}
	seen := map[string]bool{}
	var resolve func(path, ref string)
	resolve = func(path, ref string) {
		if seen[path+"|"+ref] {
			return
		}
		seen[path+"|"+ref] = true
		m := mods[path]
		if m == nil {
			return
		}
		parts := strings.Split(ref, ".")
		if len(parts) < 2 {
			return
		}
		switch parts[0] {
		case "data":
			if len(parts) >= 3 && known[m.prefix()+"data."+parts[1]+"."+parts[2]] {
				set[m.prefix()+"data."+parts[1]+"."+parts[2]] = true
			}
		case "module":
			child := mods[m.prefix()+"module."+parts[1]]
			if child == nil {
				return
			}
			for name, o := range child.mod.Outputs {
				if len(parts) >= 3 && parts[2] != name {
					continue
				}
				var rs []string
				refsOf(o.Expression, &rs)
				rs = append(rs, o.DependsOn...)
				for _, x := range rs {
					resolve(child.path, x)
				}
			}
		case "var":
			if m.call != nil && m.parent != nil {
				var rs []string
				refsOf(m.call.Expressions[parts[1]], &rs)
				for _, x := range rs {
					resolve(m.parent.path, x)
				}
			}
		case "local", "each", "count", "path", "terraform", "self":
		default:
			if known[m.prefix()+parts[0]+"."+parts[1]] {
				set[m.prefix()+parts[0]+"."+parts[1]] = true
			}
		}
	}
	for _, ref := range refs {
		resolve(path, ref)
	}
	self := mods[path].prefix() + r.Type + "." + r.Name
	if r.Mode == "data" {
		self = mods[path].prefix() + "data." + r.Type + "." + r.Name
	}
	delete(set, self)
	return sortedKeys(set)
}
