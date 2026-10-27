package arm

import (
	"slices"
	"testing"
)

// Every $filter that az, azurerm and the Azure SDKs send to a listing pogi serves, and what each keeps.
func TestFilter(t *testing.T) {
	item := func(name, rg, typ string, tags map[string]any) map[string]any {
		return map[string]any{"id": "/subscriptions/s/resourceGroups/" + rg + "/providers/" + typ + "/" + name, "name": name,
			"type": typ, "location": "eastus", "tags": tags, "properties": map[string]any{"roleName": name, "principalId": "p-" + name}}
	}
	items := []map[string]any{
		item("a", "Rg1", "Microsoft.KeyVault/vaults", map[string]any{"env": "dev", "team": "x"}),
		item("b", "rg1", "Microsoft.Storage/storageAccounts", map[string]any{"env": "prod"}),
		item("c", "rg2", "Microsoft.KeyVault/vaults", nil),
	}
	for _, c := range []struct{ f, want string }{
		{"", "abc"},
		{"resourceGroup eq 'rg1'", "ab"},
		{"resourceGroup eq 'rg2' and resourceType eq 'Microsoft.KeyVault/vaults'", "c"},
		{"resourceGroup eq 'rg1' and name eq 'b' and location eq 'eastus'", "b"},
		{"resourceType eq 'Microsoft.KeyVault/vaults'", "ac"},
		{"tagName eq 'env'", "ab"},
		{"tagname eq 'env' and tagvalue eq 'dev'", "a"},
		{"tagName eq 'env' and tagValue eq 'DEV'", "a"}, // values compare without case, as other filters do
		{"tagname eq 'env' and tagvalue eq 'qa'", ""},
		{"startswith(tagname, 'te')", "a"},
		{"roleName eq 'Contributor'", ""},
		{"roleName eq 'c'", "c"},
		{"atScope()", "abc"},
		{"atScope() and assignedTo('p-b')", "b"},
		{"assignedTo('p-a')", "a"},
		{"principalId eq 'p-c'", "c"},
		{"name eq 'it''s'", ""},
		// a filter that cannot be read
		{"resourceGroup eq", "!"},
		{"resourceGroup", "!"},
		{"name like 'a'", "!"},
		{"name eq 'a' and (", "!"},
		{"substringof('a', name)", "!"},
	} {
		got, err := filter(items, c.f)
		if c.want == "!" {
			if err == nil {
				t.Errorf("%q: want an error, kept %d", c.f, len(got))
			}
			continue
		}
		var names string
		for _, it := range got {
			names += it["name"].(string)
		}
		if err != nil || !slices.Equal([]byte(names), []byte(c.want)) {
			t.Errorf("%q: kept %q, want %q (%v)", c.f, names, c.want, err)
		}
	}
}
