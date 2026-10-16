package cloudformation

import "strings"

// Most resources are one create call and one delete call on a service that takes
// the template's property names as they are. spec says how to make such a provider.

type spec struct {
	target string // JSON target of the service
	sign   string // or, for a Query-protocol service, its signing name ("autoscaling")
	create string // create operation
	update string // operation for in-place changes (same input as create); empty: any change replaces
	del    string
	name   string // property that names it, made up when the template has none; empty: no name
	max    int    // longest generated name
	lower  bool   // generated names are lower case
	key    string // input member for the name (default: the property); "-": none
	updKey string // input member for the physical id in update (default: key)
	drop   []string
	rename map[string]string // property -> input member
	// inPlace are the properties update may change; a change to any other one replaces the resource
	inPlace []string
	build   func(c *pctx, p map[string]any, name string) map[string]any // the create input, when the properties are not it
	id      func(name string, out map[string]any) string                // the physical id (default: the name)
	delIn   func(phys string, p map[string]any) map[string]any          // the delete input (default: the name)
	attrs   func(c *pctx, phys string, p, out map[string]any) map[string]any
}

// dig reads a nested member of a JSON answer as text.
func dig(m map[string]any, path ...string) string {
	var v any = m
	for _, k := range path {
		mm, _ := v.(map[string]any)
		if mm == nil {
			return ""
		}
		v = mm[k]
	}
	return str(v)
}

// call runs one operation and returns its answer as a map (for a Query service, the members of its result).
func (sp spec) call(c *pctx, op string, in map[string]any) (map[string]any, error) {
	if sp.sign == "" {
		out := map[string]any{}
		return out, c.call(sp.target, op, in, &out)
	}
	n, err := c.query(sp.sign, op, in)
	if err != nil {
		return nil, err
	}
	if r := n.get(op + "Result"); r != nil {
		n = r
	}
	m, _ := xmap(n).(map[string]any)
	return m, nil
}

// xmap turns an XML answer into maps (elements with children), lists (<member> and <item> children) and text.
func xmap(n *xnode) any {
	if len(n.Kids) == 0 {
		return strings.TrimSpace(n.Text)
	}
	if k := n.Kids[0]; k.Name == "member" || k.Name == "item" {
		out := []any{}
		for _, e := range n.Kids {
			out = append(out, xmap(e))
		}
		return out
	}
	out := map[string]any{}
	for _, k := range n.Kids {
		out[k.Name] = xmap(k)
	}
	return out
}

func simple(sp spec) *provider {
	key := or(sp.key, sp.name)
	input := func(c *pctx, p map[string]any, name string) map[string]any {
		if sp.build != nil {
			return sp.build(c, p, name)
		}
		in := map[string]any{}
		for k, v := range p {
			if to, ok := sp.rename[k]; ok {
				k = to
			}
			in[k] = v
		}
		for _, k := range sp.drop {
			delete(in, k)
		}
		return in
	}
	pr := &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			name := ""
			if sp.name != "" {
				if name = str(p[sp.name]); name == "" {
					name = c.name(sp.max, sp.lower)
				}
			}
			in := input(c, p, name)
			if key != "" && key != "-" {
				in[key] = name
			}
			out, err := sp.call(c, sp.create, in)
			if err != nil {
				return "", nil, err
			}
			phys := name
			if sp.id != nil {
				phys = sp.id(name, out)
			}
			var attrs map[string]any
			if sp.attrs != nil {
				attrs = sp.attrs(c, phys, p, out)
			}
			return phys, attrs, nil
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			in := map[string]any{key: phys}
			if sp.delIn != nil {
				in = sp.delIn(phys, p)
			}
			_, err := sp.call(c, sp.del, in)
			return err
		},
	}
	if sp.update != "" {
		pr.update = func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if differs(old, p, sp.inPlace...) {
				return nil, true, nil
			}
			in := input(c, p, "")
			if k := or(sp.updKey, key); k != "" && k != "-" {
				in[k] = phys
			}
			out, err := sp.call(c, sp.update, in)
			if err != nil {
				return nil, false, err
			}
			var attrs map[string]any
			if sp.attrs != nil {
				attrs = sp.attrs(c, phys, p, out)
			}
			return attrs, false, nil
		}
	}
	return pr
}

// jsonText turns an object property (a policy, an event pattern) into the JSON text an API takes.
func jsonText(v any) any {
	if v == nil {
		return nil
	}
	return jsonString(v)
}
