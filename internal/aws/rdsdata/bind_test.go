package rdsdata

import (
	"reflect"
	"testing"
)

// TestBind checks the :name translation without a database.
func TestBind(t *testing.T) {
	one, day := int64(1), "2024-01-02"
	params := []param{{Name: "1", Value: value{LongValue: &one}}, {Name: "a", Value: value{LongValue: &one}}, {Name: "d", TypeHint: "DATE", Value: value{StringValue: &day}}}
	for _, c := range []struct {
		in, out string
		my      bool
		args    int
	}{
		{`SELECT :a, :a::text, ':a', "x:a", /* :a */ $$ :a $$, $t$ :a $t$, :missing, $1 -- :a`,
			`SELECT $1, $1::text, ':a', "x:a", /* :a */ $$ :a $$, $t$ :a $t$, :missing, $1 -- :a`, false, 1},
		{`SELECT :d, :a`, `SELECT $1::date, $2`, false, 2},
		{"SELECT :a, :a, 'it\\'s :a', `:a` # :a", "SELECT ?, ?, 'it\\'s :a', `:a` # :a", true, 2},
		{`SELECT :1, :1::text, a[1:2]`, `SELECT $1, $1::text, a[1:2]`, false, 1}, // floci #5112: numeric names
	} {
		out, args := bind(c.in, params, c.my)
		if out != c.out || len(args) != c.args {
			t.Errorf("bind(%q) = %q with %d args, want %q with %d", c.in, out, len(args), c.out, c.args)
		}
	}
}

// TestArrayField checks Postgres array columns become arrayValue (floci #5117).
func TestArrayField(t *testing.T) {
	for _, c := range []struct {
		in, typ string
		want    any
	}{
		{"{1,2,3}", "_int4", map[string]any{"arrayValue": map[string]any{"longValues": []int64{1, 2, 3}}}},
		{`{a,"b \"c\", d",e}`, "_text", map[string]any{"arrayValue": map[string]any{"stringValues": []string{"a", `b "c", d`, "e"}}}},
		{"{{1,2},{3,4}}", "_int8", map[string]any{"arrayValue": map[string]any{"arrayValues": []any{
			map[string]any{"longValues": []int64{1, 2}}, map[string]any{"longValues": []int64{3, 4}}}}}},
		{"{t,f}", "_bool", map[string]any{"arrayValue": map[string]any{"booleanValues": []bool{true, false}}}},
		{"{1.5}", "_float8", map[string]any{"arrayValue": map[string]any{"doubleValues": []float64{1.5}}}},
		{"{}", "_int4", map[string]any{"arrayValue": map[string]any{"longValues": []int64{}}}},
		{"{1,NULL}", "_int4", map[string]any{"stringValue": "{1,NULL}"}}, // not shown as an array
	} {
		if got := field(c.in, c.typ); !reflect.DeepEqual(got, c.want) {
			t.Errorf("field(%q, %s) = %v, want %v", c.in, c.typ, got, c.want)
		}
	}
}
