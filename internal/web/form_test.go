package web

import (
	"encoding/json"
	"net/url"
	"strings"
	"testing"

	"github.com/nrndev/pogi/internal/core"
)

var testFields = []core.Field{
	{Name: "Name", Kind: "string", Required: true},
	{Name: "Count", Kind: "integer"},
	{Name: "On", Kind: "bool"},
	{Name: "Doc", Kind: "json"},
	{Name: "Cfg", Kind: "struct", Fields: []core.Field{{Name: "Mode", Kind: "enum", Enum: []string{"a", "b"}}, {Name: "Size", Kind: "number"}}},
	{Name: "Rules", Kind: "list", Elem: &core.Field{Kind: "struct", Fields: []core.Field{
		{Name: "Id", Kind: "string"},
		{Name: "Tags", Kind: "map", Elem: &core.Field{Kind: "string"}},
	}}},
	{Name: "Names", Kind: "list", Elem: &core.Field{Kind: "string"}},
	{Name: "Opts", Kind: "map", Elem: &core.Field{Kind: "struct", Fields: []core.Field{{Name: "N", Kind: "integer"}}}},
}

func TestInput(t *testing.T) {
	v := url.Values{
		"Name": {"x"}, "Count": {"3"}, "On": {"false"}, "Doc": {`{"a": [1]}`}, "Cfg.Mode": {"b"}, "Cfg.Size": {"1.5"},
		"Rules.0.Id": {"r0"}, "Rules.0.Tags.0.k": {"env"}, "Rules.0.Tags.0.v": {"dev"}, "Rules.0.Tags.2.k": {"blank"},
		"Rules.2.Id": {"r2"}, // the item between was removed: numbers have gaps, order is kept
		"Names.0":    {"a"}, "Names.1": {""}, "Names.3": {"d"},
		"Opts.0.k": {"first"}, "Opts.0.v.N": {"7"}, "Opts.1.k": {""}, "Opts.1.v.N": {"9"}, // no key, no entry
	}
	in, err := input(testFields, v)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := json.Marshal(in)
	const want = `{"Cfg":{"Mode":"b","Size":1.5},"Count":3,"Doc":{"a":[1]},"Name":"x","Names":["a","d"],"On":false,"Opts":{"first":{"N":7}},` +
		`"Rules":[{"Id":"r0","Tags":{"blank":"","env":"dev"}},{"Id":"r2"}]}`
	if string(got) != want {
		t.Fatalf("input =\n%s\nwant\n%s", got, want)
	}

	// nothing entered is nothing sent
	in, err = input(testFields, url.Values{"Name": {"x"}, "Cfg.Mode": {""}, "Rules.0.Id": {""}})
	if got, _ := json.Marshal(in); err != nil || string(got) != `{"Name":"x"}` {
		t.Fatalf("input = %s, %v", got, err)
	}
	for field, bad := range map[string]string{"Count": "three", "Cfg.Size": "big", "On": "maybe", "Doc": "{", "Opts.0.v.N": "x"} {
		v := url.Values{field: {bad}}
		if field == "Opts.0.v.N" {
			v.Set("Opts.0.k", "k")
		}
		if _, err := input(testFields, v); err == nil || !strings.Contains(err.Error(), bad) && field != "Doc" {
			t.Errorf("%s=%q: err = %v", field, bad, err)
		}
	}
}

func TestLookup(t *testing.T) {
	for path, kind := range map[string]string{"Rules": "list", "Rules.4.Tags": "map", "Cfg.Mode": "enum", "Opts.1.v.N": "integer", "Opts.0.k": "string", "Names.2": "string"} {
		f, ok := lookup(testFields, path)
		if !ok || f.Kind != kind {
			t.Errorf("lookup(%s) = %s, %v; want %s", path, f.Kind, ok, kind)
		}
	}
	for _, path := range []string{"Nope", "Rules.x", "Cfg.Mode.deeper", "Count.0"} {
		if _, ok := lookup(testFields, path); ok {
			t.Errorf("lookup(%s) found something", path)
		}
	}
}

func TestScopeAllows(t *testing.T) {
	sc := scope{Account: "000000000000", Region: "us-east-1", Project: "pogi", Subscription: core.AzureSubscription, Location: all, Group: all, AzLocation: all}
	for _, c := range []struct {
		cols, row []string
		want      bool
	}{
		{[]string{"Name", "Region", "Account"}, []string{"q", "us-east-1", "000000000000"}, true},
		{[]string{"Name", "Region", "Account"}, []string{"q", "eu-west-1", "000000000000"}, false},
		{[]string{"Name", "Region", "Account"}, []string{"q", "us-east-1", "111111111111"}, false},
		{[]string{"Name", "Region"}, []string{"q", ""}, true}, // says nothing: stays
		{[]string{"Id"}, []string{"arn:aws:sns:us-west-2:000000000000:t"}, false},
		{[]string{"Id"}, []string{"arn:aws:sns:us-east-1:000000000000:t"}, true},
		{[]string{"Id", "Name", "Region"}, []string{"000000000000/us-west-2/x", "x", "us-west-2"}, false},
		{[]string{"Id"}, []string{"000000000000/us-east-1/x"}, true},
		{[]string{"Name"}, []string{"projects/other/topics/t"}, false},
		{[]string{"Name"}, []string{"projects/pogi/topics/t"}, true},
		{[]string{"ID"}, []string{"/subscriptions/11111111-1111-1111-1111-111111111111/resourceGroups/g"}, false},
	} {
		if got := sc.allows(c.cols, c.row); got != c.want {
			t.Errorf("allows(%v, %v) = %v", c.cols, c.row, got)
		}
	}
	if !(scope{all, all, all, all, all, all, all}).allows([]string{"Region"}, []string{"mars-1"}) {
		t.Error("* did not allow everything")
	}
	// an Azure storage account or a GCP region is not the switcher's AWS account or region
	if aws := (scope{"000000000000", "us-east-1", all, all, all, all, all}); !aws.forCloud(core.Azure).allows([]string{"Account", "Region"}, []string{"mystore", "eastus"}) {
		t.Error("an Azure row was filtered by the AWS scope")
	}
}
