package generic

import "testing"

func lim(f float64) *float64 { return &f }

// The messages and member paths are the ones AWS prints.
func TestValidateLimits(t *testing.T) {
	str := &shape{Type: "string", Min: lim(1), Max: lim(5)}
	enum := &shape{Type: "string", Enum: []string{"A", "B"}}
	num := &shape{Type: "integer", Min: lim(1), Max: lim(10)}
	item := &shape{Type: "structure", Members: map[string]*ref{"Name": {sh: str}}, names: []string{"Name"}}
	list := &shape{Type: "list", Min: lim(1), Max: lim(2), Member: &ref{sh: item}}
	ids := &shape{Type: "list", Member: &ref{sh: num}}
	in := &shape{Type: "structure", Required: []string{"Must"}, names: []string{"Ids", "Items", "Kind", "MaxResults", "Name"},
		Members: map[string]*ref{"Name": {sh: str}, "Kind": {sh: enum}, "MaxResults": {sh: num}, "Items": {sh: list}, "Ids": {sh: ids}}}
	for _, c := range []struct {
		name     string
		in       map[string]any
		required bool
		want     string
	}{
		{"fine", map[string]any{"Name": "abc", "Kind": "A", "MaxResults": 5.0}, false, ""},
		{"int min", map[string]any{"MaxResults": -5.0}, false,
			"1 validation error detected: Value '-5' at 'maxResults' failed to satisfy constraint: Member must have value greater than or equal to 1"},
		{"int max", map[string]any{"MaxResults": 11.0}, false,
			"1 validation error detected: Value '11' at 'maxResults' failed to satisfy constraint: Member must have value less than or equal to 10"},
		{"string too long", map[string]any{"Name": "abcdef"}, false,
			"1 validation error detected: Value 'abcdef' at 'name' failed to satisfy constraint: Member must have length less than or equal to 5"},
		{"string too short", map[string]any{"Name": ""}, false,
			"1 validation error detected: Value '' at 'name' failed to satisfy constraint: Member must have length greater than or equal to 1"},
		{"enum", map[string]any{"Kind": "C"}, false,
			"1 validation error detected: Value 'C' at 'kind' failed to satisfy constraint: Member must satisfy enum value set: [A, B]"},
		{"list size", map[string]any{"Items": []any{}}, false,
			"1 validation error detected: Value '[]' at 'items' failed to satisfy constraint: Member must have length greater than or equal to 1"},
		{"member in a list", map[string]any{"Items": []any{map[string]any{"Name": "ok"}, map[string]any{"Name": "toolong"}}}, false,
			"1 validation error detected: Value 'toolong' at 'items.2.member.name' failed to satisfy constraint: Member must have length less than or equal to 5"},
		{"scalar in a list", map[string]any{"Ids": []any{1.0, 0.0}}, false,
			"1 validation error detected: Value '0' at 'ids.2.member' failed to satisfy constraint: Member must have value greater than or equal to 1"},
		{"two errors", map[string]any{"MaxResults": 0.0, "Name": "abcdef"}, false,
			"2 validation errors detected: Value '0' at 'maxResults' failed to satisfy constraint: Member must have value greater than or equal to 1; " +
				"Value 'abcdef' at 'name' failed to satisfy constraint: Member must have length less than or equal to 5"},
		{"required", map[string]any{}, true,
			"1 validation error detected: Value null at 'must' failed to satisfy constraint: Member must not be null"},
		{"required is not checked for hand-written services", map[string]any{}, false, ""},
		{"a value of the wrong type is left to the service", map[string]any{"MaxResults": "many"}, false, ""},
	} {
		if got := validate(in, c.in, c.required, nil); got != c.want {
			t.Errorf("%s:\n got %q\nwant %q", c.name, got, c.want)
		}
	}
}
