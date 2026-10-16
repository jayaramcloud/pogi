package keyvault

import (
	"encoding/json"
	"net/http"
	"net/url"
	"slices"
	"strings"

	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/azure/armres"
	"github.com/nrndev/pogi/internal/core"
)

// Resource-manager side: vaults (with their access policies) and their
// secrets. Vault data is the data plane's own, reached in-process.

const vaults = "Microsoft.KeyVault/vaults"

func (s *Service) registerARM(h *core.Hub) {
	s.hub = h
	arm.Register(h, arm.Provider{Type: vaults, Async: true, Deleted: 200, Put: s.putVault, Get: s.getVault, Delete: s.deleteVault, List: s.listVaults})
	arm.Register(h, arm.Provider{Type: vaults + "/accessPolicies", Put: s.putPolicies,
		Get: func(_ *core.Ctx, id arm.ID) (map[string]any, error) { return nil, arm.NotFound(id) }})
	arm.Register(h, armres.TombstoneProvider("Microsoft.KeyVault", "deletedVaults", &s.mu, &s.st.ARM))
	arm.Register(h, arm.Provider{Type: vaults + "/secrets", Put: s.putSecret, Get: s.getSecret, Delete: s.deleteSecret, List: s.listSecrets})
}

func (s *Service) vaultView(c *core.Ctx, r map[string]any) map[string]any {
	p := armres.Props(r)
	armres.Defaults(p, map[string]any{
		"tenantId": core.AzureSubscription, "sku": map[string]any{"family": "A", "name": "standard"}, "accessPolicies": []any{},
		"enabledForDeployment": false, "enabledForDiskEncryption": false, "enabledForTemplateDeployment": false,
		"enableSoftDelete": true, "softDeleteRetentionInDays": 90, "enableRbacAuthorization": false, "publicNetworkAccess": "Enabled",
		"networkAcls": map[string]any{"bypass": "AzureServices", "defaultAction": "Allow", "ipRules": []any{}, "virtualNetworkRules": []any{}},
	})
	p["provisioningState"] = "Succeeded"
	p["vaultUri"] = armres.Labeled(c, "https", r["name"].(string)+".vault") + "/"
	return r
}

func (s *Service) putVault(c *core.Ctx, id arm.ID, body map[string]any) (map[string]any, error) {
	r := armres.Envelope(id, body)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.st.Vaults[id.Name()] == nil {
		s.st.Vaults[id.Name()] = Vault{}
	}
	s.st.ARM.Put(id, r)
	return s.vaultView(c, armres.Clone(r)), nil
}

func (s *Service) getVault(c *core.Ctx, id arm.ID) (map[string]any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.st.ARM.Get(id)
	if !ok {
		return nil, arm.NotFound(id)
	}
	return s.vaultView(c, r), nil
}

func (s *Service) deleteVault(c *core.Ctx, id arm.ID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.st.Vaults, id.Name()) // ponytail: the secrets go with the vault; a purged or recovered one does not bring them back
	if r, ok := s.st.ARM.Get(id); ok {
		armres.Tombstone(&s.st.ARM, id, "deletedVaults", armres.Str(r, "location"), map[string]any{"vaultId": id.String(), "tags": r["tags"]})
	}
	s.st.ARM.Delete(id)
	return nil
}

func (s *Service) listVaults(c *core.Ctx, parent arm.ID) ([]map[string]any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []map[string]any
	for _, r := range s.st.ARM.List(parent, vaults) {
		out = append(out, s.vaultView(c, r))
	}
	return out, nil
}

// putPolicies is PUT vaults/<v>/accessPolicies/add|replace|remove.
func (s *Service) putPolicies(c *core.Ctx, id arm.ID, body map[string]any) (map[string]any, error) {
	vid := id.Parent()
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.st.ARM.Get(vid)
	if !ok {
		return nil, arm.NotFound(vid)
	}
	same := func(a, b any) bool { // a policy is identified by tenant, object and application
		x, _ := a.(map[string]any)
		y, _ := b.(map[string]any)
		return x["tenantId"] == y["tenantId"] && x["objectId"] == y["objectId"] && x["applicationId"] == y["applicationId"]
	}
	cur, _ := armres.Props(r)["accessPolicies"].([]any)
	in, _ := armres.Props(body)["accessPolicies"].([]any)
	switch strings.ToLower(id.Name()) {
	case "add", "replace":
		for _, n := range in {
			if i := slices.IndexFunc(cur, func(o any) bool { return same(o, n) }); i >= 0 {
				if id.Name() == "replace" {
					cur[i] = n
				} else { // add merges the permissions
					cur[i] = mergePermissions(cur[i], n)
				}
			} else {
				cur = append(cur, n)
			}
		}
	case "remove":
		cur = slices.DeleteFunc(cur, func(o any) bool { return slices.ContainsFunc(in, func(n any) bool { return same(o, n) }) })
	default:
		return nil, core.Errorf(http.StatusBadRequest, "InvalidOperationName", "The access policy operation must be add, replace or remove.")
	}
	if cur == nil {
		cur = []any{}
	}
	armres.Props(r)["accessPolicies"] = cur
	s.st.ARM.Put(vid, r)
	return map[string]any{"id": id.String(), "name": id.Name(), "type": id.Type(), "properties": map[string]any{"accessPolicies": cur}}, nil
}

func mergePermissions(old, add any) any {
	o, _ := old.(map[string]any)
	a, _ := add.(map[string]any)
	op, _ := o["permissions"].(map[string]any)
	ap, _ := a["permissions"].(map[string]any)
	for k, v := range ap {
		have, _ := op[k].([]any)
		for _, x := range v.([]any) {
			if !slices.Contains(have, x) {
				have = append(have, x)
			}
		}
		if op == nil {
			op = map[string]any{}
		}
		op[k] = have
	}
	o["permissions"] = op
	return o
}

// ---- secrets, through the data plane ----

func (s *Service) vault(c *core.Ctx, vault, method, path string, body any) (int, map[string]any) {
	status, resp := armres.Send(c.Hub, "keyvault", method, vault+".vault.localhost", path+"?api-version=7.4", body)
	var out map[string]any
	json.Unmarshal(resp, &out)
	return status, out
}

func secretView(id arm.ID, b map[string]any) map[string]any {
	p := map[string]any{"attributes": b["attributes"], "secretUri": "", "secretUriWithVersion": ""}
	if sid, _ := b["id"].(string); sid != "" {
		if u, err := url.Parse(sid); err == nil {
			p["secretUri"], p["secretUriWithVersion"] = u.Scheme+"://"+u.Host+"/secrets/"+id.Name(), sid
		}
	}
	if ct, ok := b["contentType"]; ok {
		p["contentType"] = ct
	}
	r := map[string]any{"id": id.String(), "name": id.Name(), "type": id.Type(), "location": "eastus", "tags": map[string]any{}, "properties": p}
	if t, ok := b["tags"]; ok {
		r["tags"] = t
	}
	return r
}

func (s *Service) putSecret(c *core.Ctx, id arm.ID, body map[string]any) (map[string]any, error) {
	p := armres.Props(body)
	in := map[string]any{"value": p["value"], "tags": body["tags"]}
	if ct, ok := p["contentType"]; ok {
		in["contentType"] = ct
	}
	if a, ok := p["attributes"]; ok {
		in["attributes"] = a
	}
	status, out := s.vault(c, id.Parent().Name(), http.MethodPut, "/secrets/"+id.Name(), in)
	if status >= 300 {
		return nil, keyvaultError(status, out)
	}
	return secretView(id, out), nil
}

func (s *Service) getSecret(c *core.Ctx, id arm.ID) (map[string]any, error) {
	status, out := s.vault(c, id.Parent().Name(), http.MethodGet, "/secrets/"+id.Name(), nil)
	if status == http.StatusNotFound {
		return nil, arm.NotFound(id)
	}
	if status >= 300 {
		return nil, keyvaultError(status, out)
	}
	return secretView(id, out), nil
}

func (s *Service) deleteSecret(c *core.Ctx, id arm.ID) error {
	if status, _ := s.vault(c, id.Parent().Name(), http.MethodDelete, "/secrets/"+id.Name(), nil); status < 300 {
		s.vault(c, id.Parent().Name(), http.MethodDelete, "/deletedsecrets/"+id.Name(), nil) // purge
	}
	return nil
}

func (s *Service) listSecrets(c *core.Ctx, parent arm.ID) ([]map[string]any, error) {
	status, out := s.vault(c, parent.Name(), http.MethodGet, "/secrets", nil)
	if status >= 300 {
		return nil, keyvaultError(status, out)
	}
	var res []map[string]any
	items, _ := out["value"].([]any)
	for _, it := range items {
		b, _ := it.(map[string]any)
		sid, _ := b["id"].(string)
		res = append(res, secretView(parent.Child("secrets", sid[strings.LastIndex(sid, "/")+1:]), b))
	}
	return res, nil
}

// keyvaultError turns a data-plane error body into an ARM error.
func keyvaultError(status int, out map[string]any) error {
	e, _ := out["error"].(map[string]any)
	return core.Errorf(status, armres.Str(e, "code"), "%s", armres.Str(e, "message"))
}
