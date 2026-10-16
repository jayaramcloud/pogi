package generic

import (
	"slices"
	"strings"

	"github.com/nrndev/pogi/internal/core"
)

// Console support: forms from the model's create and update inputs, and the
// delete and tag calls behind the resources table's bulk actions.

// formDepth is how deep a form nests structures before it settles for a JSON box.
const formDepth = 3

// FormOps lists the create and update operations of the service, sorted.
func (s *Service) FormOps() []string {
	m, err := s.model()
	if err != nil {
		return nil
	}
	var out []string
	for n, op := range m.ops {
		if (op.kind == kCreate || op.kind == kUpdate) && op.in != nil {
			out = append(out, n)
		}
	}
	slices.Sort(out)
	return out
}

// Form builds the form of a create or update operation from its input shape.
func (s *Service) Form(op string) *core.Form {
	m, err := s.model()
	if err != nil || m.ops[op] == nil || m.ops[op].in == nil {
		return nil
	}
	return &core.Form{Op: op, Title: core.Label(op), Fields: members(m.ops[op].in, formDepth)}
}

// members are a structure's fields, required ones first.
func members(sh *shape, depth int) []core.Field {
	var out []core.Field
	for _, req := range []bool{true, false} {
		for _, n := range sh.names {
			if sh.req[n] == req {
				out = append(out, field(n, sh.Members[n], req, depth))
			}
		}
	}
	return out
}

func field(name string, r *ref, req bool, depth int) core.Field {
	f := core.Field{Name: name, Label: core.Label(name), Required: req}
	sh := r.sh
	switch {
	case sh == nil || sh.Document:
		f.Kind = "json"
	case len(sh.Enum) > 0:
		f.Kind, f.Enum = "enum", sh.Enum
	case sh.Type == "structure":
		if depth == 0 {
			f.Kind = "json" // ponytail: deeper structures are a JSON box; a recursive model would make an endless form
			break
		}
		f.Kind, f.Fields = "struct", members(sh, depth-1)
	case sh.Type == "list" && sh.Member != nil && depth > 0:
		e := field("", sh.Member, false, depth-1)
		f.Kind, f.Elem = "list", &e
	case sh.Type == "map" && sh.Value != nil && depth > 0:
		e := field("", sh.Value, false, depth-1)
		f.Kind, f.Elem = "map", &e
	case sh.Type == "list" || sh.Type == "map":
		f.Kind = "json"
	case sh.Type == "integer" || sh.Type == "long":
		f.Kind = "integer"
	case sh.Type == "float" || sh.Type == "double":
		f.Kind = "number"
	case sh.Type == "boolean":
		f.Kind = "bool"
	default: // string, blob, timestamp
		f.Kind = "string"
	}
	return f
}

// row returns the stored resource a console row shows (its first cell).
func (s *Service) row(table, id string) *Resource {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.st.Res {
		if r.Type == table && r.identity() == id {
			return r.clone()
		}
	}
	return nil
}

// Remove deletes the resource a console row shows through its type's delete operation.
func (s *Service) Remove(table, id string) error {
	r, op, err := s.target(table, id, kDelete)
	if err != nil {
		return err
	}
	in := map[string]any{}
	for k, v := range r.IDs {
		in[k] = v
	}
	_, err = s.Do(r.Account, r.Region, op.Name, in)
	return err
}

// Tag adds tags to the resource a console row shows through the service's tag operation.
func (s *Service) Tag(table, id string, tags map[string]string) error {
	r, op, err := s.target(table, id, kTag)
	if err != nil {
		return err
	}
	in := map[string]any{}
	for _, n := range op.in.names {
		mr, nn := op.in.Members[n], norm(n)
		switch {
		case isTagMember(n, mr.sh):
			in[n] = tagValue(mr.sh, tags)
		case strings.Contains(nn, "resource") || strings.Contains(nn, "arn"):
			v := r.ARN
			if v == "" {
				v = r.Name
			}
			if mr.sh.Type == "list" {
				in[n] = []any{v}
			} else if in[n] == nil || strings.Contains(nn, "resource") {
				in[n] = v
			}
		}
	}
	_, err = s.Do(r.Account, r.Region, op.Name, in)
	return err
}

// target finds the row's resource and the operation of a kind that applies to it: the type's own
// delete, or the service's tag call.
func (s *Service) target(table, id string, k kind) (*Resource, *operation, error) {
	m, err := s.model()
	if err != nil {
		return nil, nil, err
	}
	r := s.row(table, id)
	if r == nil {
		return nil, nil, core.Errorf(404, "ResourceNotFoundException", "%s %s not found", table, id)
	}
	var ops []*operation
	if k == kDelete {
		if t := m.types[norm(table)]; t != nil {
			ops = t.ops[kDelete]
		}
	} else {
		for _, op := range m.ops {
			if op.kind == k && op.in != nil {
				ops = append(ops, op)
			}
		}
	}
	if len(ops) == 0 {
		return nil, nil, core.Errorf(400, "UnsupportedOperation", "%s has no such operation for %s", s.meta.Name, table)
	}
	slices.SortFunc(ops, func(a, b *operation) int { return len(a.Name) - len(b.Name) })
	return r, ops[0], nil
}

// tagValue puts tags where the operation takes them: a map, or a list of key and value structures.
func tagValue(sh *shape, tags map[string]string) any {
	if sh.Type == "map" {
		out := map[string]any{}
		for k, v := range tags {
			out[k] = v
		}
		return out
	}
	kn, vn := "Key", "Value"
	if sh.Member != nil && sh.Member.sh != nil && sh.Member.sh.Type == "structure" {
		kn, vn = "", ""
		for _, n := range sh.Member.sh.names {
			switch nn := norm(n); {
			case strings.Contains(nn, "key") && kn == "":
				kn = n
			case strings.Contains(nn, "value") && vn == "":
				vn = n
			}
		}
	}
	var out []any
	for _, k := range core.SortedKeys(tags) {
		out = append(out, map[string]any{kn: k, vn: tags[k]})
	}
	return out
}
