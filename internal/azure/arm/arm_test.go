package arm

import "testing"

func TestID(t *testing.T) {
	s := "/subscriptions/s1/resourceGroups/rg/providers/Microsoft.ServiceBus/namespaces/ns/queues/q"
	id, ok := ParseID(s)
	if !ok || id.String() != s || id.Type() != "Microsoft.ServiceBus/namespaces/queues" || id.Name() != "q" {
		t.Fatalf("%+v %v", id, ok)
	}
	if p := id.Parent(); p.String() != "/subscriptions/s1/resourceGroups/rg/providers/Microsoft.ServiceBus/namespaces/ns" {
		t.Fatal(p)
	}
	if p := id.Parent().Parent(); p.String() != "/subscriptions/s1/resourceGroups/rg" {
		t.Fatal(p)
	}
	if c := id.Parent().Child("topics", "t"); c.Type() != "Microsoft.ServiceBus/namespaces/topics" || c.Name() != "t" {
		t.Fatal(c)
	}
	coll, ok := ParseID("/subscriptions/s1/resourceGroups/rg/providers/Microsoft.Storage/storageAccounts")
	if !ok || coll.Name() != "" || coll.Type() != "Microsoft.Storage/storageAccounts" {
		t.Fatalf("%+v", coll)
	}
	for _, bad := range []string{"", "/foo/bar", "/subscriptions/s/resourceGroups/rg/nope/x/y"} {
		if _, ok := ParseID(bad); ok {
			t.Errorf("ParseID(%q) ok", bad)
		}
	}
}
