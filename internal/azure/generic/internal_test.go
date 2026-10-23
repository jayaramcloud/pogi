package generic

import (
	"strings"
	"testing"

	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/core"
)

// The checks that no listed type exercises today: required properties and the name rules of a spec.
func TestBuildChecks(t *testing.T) {
	ts := &typeSpec{typ: "Microsoft.Test/things", Tracked: true, ReadOnly: []string{"ro"}, Required: []string{"sku", "properties.size"}}
	ts.Name.Max = 5
	id := arm.ID{Subscription: "s", ResourceGroup: "g", Namespace: "Microsoft.Test", Types: []string{"things"}, Names: []string{"t1"}}
	body := func(m map[string]any) map[string]any { m["location"] = "eastus"; return m }
	for name, in := range map[string]map[string]any{
		"no sku":        body(map[string]any{"properties": map[string]any{"size": 1}}),
		"no properties": body(map[string]any{"sku": map[string]any{}}),
		"no size":       body(map[string]any{"sku": map[string]any{}, "properties": map[string]any{}}),
		"null size":     body(map[string]any{"sku": map[string]any{}, "properties": map[string]any{"size": nil}}),
	} {
		_, err := ts.build(id, in)
		if e := core.AsError(err); err == nil || e.Code != "InvalidRequestContent" || e.Status != 400 || !strings.Contains(e.Msg, "Required property") {
			t.Errorf("%s: %v", name, err)
		}
	}
	ok := body(map[string]any{"sku": map[string]any{}, "properties": map[string]any{"size": 1, "ro": "mine"}})
	d, err := ts.build(id, ok)
	if err != nil {
		t.Fatal(err)
	}
	p := d["properties"].(map[string]any)
	if _, has := p["ro"]; has || p["provisioningState"] != "Succeeded" || d["id"] != id.String() || d["tags"] == nil {
		t.Fatalf("built: %v", d)
	}
	if _, err := ts.build(id, map[string]any{"sku": map[string]any{}, "properties": map[string]any{"size": 1}}); core.AsError(err).Code != "LocationRequired" {
		t.Fatalf("no location: %v", err)
	}
	long := id
	long.Names = []string{"toolong"}
	if _, err := ts.build(long, ok); core.AsError(err).Code != "InvalidResourceName" {
		t.Fatalf("a name over the spec's maximum: %v", err)
	}
	ctl := id
	ctl.Names = []string{"a\x01"}
	if _, err := ts.build(ctl, ok); core.AsError(err).Code != "InvalidResourceName" {
		t.Fatalf("a control character: %v", err)
	}
}

// Every service of services.txt has its types, and every type can be put and read back.
func TestSpecs(t *testing.T) {
	names := Names()
	if len(names) < 3 {
		t.Fatalf("services: %v", names)
	}
	for _, n := range names {
		sp := specs()[n]
		if sp.Title == "" || len(sp.Types) == 0 {
			t.Errorf("%s: %+v", n, sp)
		}
		for typ, ts := range sp.Types {
			if len(ts.Versions) == 0 || !contains(ts.Ops, "put") { // a type may have no GET in its spec (a static site's config: its settings are read by POST)
				t.Errorf("%s: %+v", typ, ts)
			}
		}
	}
	if z := specs()["azdns"].Types["Microsoft.Network/dnsZones"]; !z.Tracked || !contains(z.LRO, "delete") || contains(z.LRO, "put") {
		t.Errorf("a DNS zone is tracked, and only its delete is long-running: %+v", z)
	}
}

func contains(l []string, s string) bool {
	for _, x := range l {
		if x == s {
			return true
		}
	}
	return false
}

// A hook's action runs only on a resource that exists.
func TestActions(t *testing.T) {
	ts := &typeSpec{typ: "Microsoft.Test/keyed"}
	Override(ts.typ, Hook{Actions: map[string]func(*Service, *core.Ctx, arm.ID, map[string]any) (any, error){
		"listKeys": func(_ *Service, _ *core.Ctx, id arm.ID, _ map[string]any) (any, error) {
			return map[string]any{"primaryKey": "k-" + id.Name()}, nil
		},
	}})
	defer delete(hooks, "microsoft.test/keyed")
	s := &Service{}
	p := s.provider(ts)
	id := arm.ID{Subscription: "s", ResourceGroup: "g", Namespace: "Microsoft.Test", Types: []string{"keyed"}, Names: []string{"k1"}}
	if _, err := p.Actions["listKeys"](nil, id, nil); core.AsError(err).Status != 404 {
		t.Fatalf("a missing resource: %v", err)
	}
	s.st.ARM.Put(id, map[string]any{"id": id.String()})
	out, err := p.Actions["listKeys"](nil, id, nil)
	if err != nil || out.(map[string]any)["primaryKey"] != "k-k1" {
		t.Fatalf("listKeys = %v, %v", out, err)
	}
}
