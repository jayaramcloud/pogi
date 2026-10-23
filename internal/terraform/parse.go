package terraform

import (
	"encoding/json"
	"fmt"
	"path"
	"sort"
	"strings"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/ext/typeexpr"
	"github.com/hashicorp/hcl/v2/hclsyntax"
	hcljson "github.com/hashicorp/hcl/v2/json"
	"github.com/zclconf/go-cty/cty"
	"github.com/zclconf/go-cty/cty/convert"
)

// maxInstances caps the resource instances (count, for_each and modules together) one Parse builds,
// so a count of a billion can't exhaust memory.
const maxInstances = 20000

// maxDepth caps module nesting, so a module that calls itself ends with an error.
const maxDepth = 16

// Parse reads a folder's files. files maps a slash path relative to the folder ("main.tf",
// "modules/net/main.tf", ".terraform/modules/modules.json") to its content. varFiles name keys
// of files to read as variable files, in order, after terraform.tfvars and *.auto.tfvars.
func Parse(files map[string][]byte, varFiles []string) (*Config, error) {
	if len(files) > MaxFiles {
		return nil, fmt.Errorf("too many files: %d (the limit is %d)", len(files), MaxFiles)
	}
	total := 0
	for _, b := range files {
		total += len(b)
	}
	if total > MaxBytes {
		return nil, fmt.Errorf("the files are too big: %d MB (the limit is %d MB)", total>>20, MaxBytes>>20)
	}
	p := newParser(files)
	root, err := p.loadModule("")
	if err != nil {
		return nil, err
	}
	if root == nil {
		return nil, fmt.Errorf("no .tf files found")
	}
	if err := p.readVarFiles(varFiles); err != nil {
		return nil, err
	}
	s := p.newScope(nil, nil, root, "", nil, 0)
	s.expand()
	if p.err != nil {
		return nil, p.err
	}
	cfg := &Config{Source: "folder", Warnings: p.warns}
	s.collect(&cfg.Resources)
	inheritRegions(cfg.Resources)
	return cfg, nil
}

// module is one parsed folder of .tf files.
type module struct {
	dir       string
	vars      map[string]*varDef
	locals    map[string]hcl.Expression
	outputs   map[string]hcl.Expression
	resources []*resDef
	calls     []*callDef
	provs     []*provDef
}

type varDef struct{ def, typ hcl.Expression }

type resDef struct {
	data      bool
	typ, name string
	file      string
	line      int
	body      hcl.Body
}

type callDef struct {
	name string
	body hcl.Body
}

type provDef struct {
	name, alias string
	body        hcl.Body
}

var topSchema = &hcl.BodySchema{Blocks: []hcl.BlockHeaderSchema{
	{Type: "resource", LabelNames: []string{"type", "name"}},
	{Type: "data", LabelNames: []string{"type", "name"}},
	{Type: "variable", LabelNames: []string{"name"}},
	{Type: "output", LabelNames: []string{"name"}},
	{Type: "module", LabelNames: []string{"name"}},
	{Type: "provider", LabelNames: []string{"name"}},
	{Type: "locals"},
}}

func syntaxErr(diags hcl.Diagnostics) error {
	for _, d := range diags {
		if d.Severity != hcl.DiagError {
			continue
		}
		msg := d.Summary
		if d.Detail != "" {
			msg += ": " + d.Detail
		}
		if d.Subject != nil {
			return fmt.Errorf("%s:%d: %s", d.Subject.Filename, d.Subject.Start.Line, msg)
		}
		return fmt.Errorf("%s", msg)
	}
	return nil
}

// parseFile parses one .tf, .tf.json, .tfvars or .tfvars.json file.
func (p *parser) parseFile(name string) (hcl.Body, error) {
	if err := checkDepth(name, p.files[name]); err != nil {
		return nil, err
	}
	var f *hcl.File
	var diags hcl.Diagnostics
	if strings.HasSuffix(name, ".json") {
		f, diags = hcljson.Parse(p.files[name], name)
	} else {
		f, diags = hclsyntax.ParseConfig(p.files[name], name, hcl.InitialPos)
	}
	if err := syntaxErr(diags); err != nil {
		return nil, err
	}
	return f.Body, nil
}

func isTF(name string) bool {
	return strings.HasSuffix(name, ".tf") || strings.HasSuffix(name, ".tf.json")
}

// loadModule parses the .tf files directly inside dir ("" is the root). It returns nil when there are none.
func (p *parser) loadModule(dir string) (*module, error) {
	if m, ok := p.mods[dir]; ok {
		return m, nil
	}
	var names []string
	for name := range p.files {
		if isTF(name) && path.Dir(name) == pathDir(dir) {
			names = append(names, name)
		}
	}
	if len(names) == 0 {
		p.mods[dir] = nil
		return nil, nil
	}
	sort.Strings(names)
	m := &module{dir: dir, vars: map[string]*varDef{}, locals: map[string]hcl.Expression{}, outputs: map[string]hcl.Expression{}}
	for _, name := range names {
		body, err := p.parseFile(name)
		if err != nil {
			return nil, err
		}
		content, _, diags := body.PartialContent(topSchema)
		if err := syntaxErr(diags); err != nil {
			return nil, err
		}
		for _, b := range content.Blocks {
			if err := p.addBlock(m, name, b); err != nil {
				return nil, err
			}
		}
	}
	p.mods[dir] = m
	return m, nil
}

func pathDir(dir string) string {
	if dir == "" {
		return "."
	}
	return dir
}

func (p *parser) addBlock(m *module, file string, b *hcl.Block) error {
	switch b.Type {
	case "resource", "data":
		m.resources = append(m.resources, &resDef{data: b.Type == "data", typ: b.Labels[0], name: b.Labels[1], file: file, line: b.DefRange.Start.Line, body: b.Body})
	case "module":
		m.calls = append(m.calls, &callDef{name: b.Labels[0], body: b.Body})
	case "variable":
		c, _, diags := b.Body.PartialContent(&hcl.BodySchema{Attributes: []hcl.AttributeSchema{{Name: "default"}, {Name: "type"}}})
		if err := syntaxErr(diags); err != nil {
			return err
		}
		v := &varDef{}
		if a := c.Attributes["default"]; a != nil {
			v.def = a.Expr
		}
		if a := c.Attributes["type"]; a != nil {
			v.typ = a.Expr
		}
		m.vars[b.Labels[0]] = v
	case "output":
		c, _, diags := b.Body.PartialContent(&hcl.BodySchema{Attributes: []hcl.AttributeSchema{{Name: "value"}}})
		if err := syntaxErr(diags); err != nil {
			return err
		}
		if a := c.Attributes["value"]; a != nil {
			m.outputs[b.Labels[0]] = a.Expr
		}
	case "provider":
		c, _, diags := b.Body.PartialContent(&hcl.BodySchema{Attributes: []hcl.AttributeSchema{{Name: "alias"}}})
		if err := syntaxErr(diags); err != nil {
			return err
		}
		d := &provDef{name: b.Labels[0], body: b.Body}
		if a := c.Attributes["alias"]; a != nil {
			if v, diags := a.Expr.Value(nil); !diags.HasErrors() && v.Type() == cty.String && v.IsKnown() && !v.IsNull() {
				d.alias = v.AsString()
			}
		}
		m.provs = append(m.provs, d)
	case "locals":
		attrs, diags := b.Body.JustAttributes()
		if err := syntaxErr(diags); err != nil {
			return err
		}
		for k, a := range attrs {
			m.locals[k] = a.Expr
		}
	}
	return nil
}

// readVarFiles fills p.varVals: terraform.tfvars, terraform.tfvars.json, *.auto.tfvars(.json) in
// alphabetical order, then the --var-file files in order. Later files win.
func (p *parser) readVarFiles(varFiles []string) error {
	var auto []string
	for name := range p.files {
		if !strings.Contains(name, "/") && (strings.HasSuffix(name, ".auto.tfvars") || strings.HasSuffix(name, ".auto.tfvars.json")) {
			auto = append(auto, name)
		}
	}
	sort.Strings(auto)
	order := []string{"terraform.tfvars", "terraform.tfvars.json"}
	order = append(order, auto...)
	for _, name := range order {
		if _, ok := p.files[name]; ok {
			if err := p.readVarFile(name); err != nil {
				return err
			}
		}
	}
	for _, name := range varFiles {
		if _, ok := p.files[name]; !ok {
			return fmt.Errorf("var file %q was not found", name)
		}
		if err := p.readVarFile(name); err != nil {
			return err
		}
	}
	return nil
}

func (p *parser) readVarFile(name string) error {
	body, err := p.parseFile(name)
	if err != nil {
		return err
	}
	attrs, diags := body.JustAttributes()
	if err := syntaxErr(diags); err != nil {
		return err
	}
	for k, a := range attrs {
		v, diags := a.Expr.Value(p.base)
		if err := syntaxErr(diags); err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		p.varVals[k] = v
	}
	return nil
}

// convertVar applies a variable's type constraint to v. A value that does not fit is kept as it
// is, with a warning.
func (p *parser) convertVar(name string, d *varDef, v cty.Value) cty.Value {
	if d.typ == nil || !v.IsKnown() {
		return v
	}
	ty, diags := typeexpr.TypeConstraint(d.typ)
	if diags.HasErrors() {
		return v
	}
	out, err := convert.Convert(v, ty)
	if err != nil {
		p.warn(fmt.Sprintf("variable %q: the value does not match its type: %s", name, err))
		return v
	}
	return out
}

// modulesJSON reads .terraform/modules/modules.json into p.modDirs (module key → folder).
func (p *parser) modulesJSON() {
	data, ok := p.files[".terraform/modules/modules.json"]
	if !ok {
		return
	}
	var f struct {
		Modules []struct{ Key, Dir string }
	}
	if err := json.Unmarshal(data, &f); err != nil {
		p.warn(".terraform/modules/modules.json could not be read: " + err.Error())
		return
	}
	for _, m := range f.Modules {
		p.modDirs[m.Key] = path.Clean(strings.ReplaceAll(m.Dir, "\\", "/"))
	}
}

// pathJoin joins a module folder and a relative source. It reports false when the result leaves the root.
func pathJoin(dir, rel string) (string, bool) {
	j := path.Join(dir, rel)
	if j == ".." || strings.HasPrefix(j, "../") {
		return "", false
	}
	if j == "." {
		j = ""
	}
	return j, true
}

// checkDepth refuses a file that nests deeper than MaxDepth, before the parser sees it. It counts brackets and
// "${" or "%{" inside strings, and skips comments. A heredoc is read as plain text, so unbalanced brackets in
// one only count when there are hundreds of them.
func checkDepth(name string, src []byte) error {
	var st []byte // '"' inside a string, '{' inside a bracket or interpolation
	line := 1
	for i := 0; i < len(src); i++ {
		c := src[i]
		if c == '\n' {
			line++
		}
		if len(st) > 0 && st[len(st)-1] == '"' {
			switch {
			case c == '\\':
				i++
			case c == '"':
				st = st[:len(st)-1]
			case (c == '$' || c == '%') && i+1 < len(src) && src[i+1] == '{':
				st = append(st, '{')
				i++
			}
		} else {
			switch {
			case c == '"':
				st = append(st, '"')
			case c == '(' || c == '[' || c == '{':
				st = append(st, '{')
			case (c == ')' || c == ']' || c == '}') && len(st) > 0:
				st = st[:len(st)-1]
			case c == '#' || c == '/' && i+1 < len(src) && src[i+1] == '/':
				for i+1 < len(src) && src[i+1] != '\n' {
					i++
				}
			case c == '/' && i+1 < len(src) && src[i+1] == '*':
				for i += 2; i+1 < len(src) && !(src[i] == '*' && src[i+1] == '/'); i++ {
					if src[i] == '\n' {
						line++
					}
				}
				i++
			}
		}
		if len(st) > MaxDepth {
			return fmt.Errorf("%s:%d: nested too deep (over %d levels)", name, line, MaxDepth)
		}
	}
	return nil
}
