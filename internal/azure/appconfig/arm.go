package appconfig

import (
	"net/http"
	"time"

	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/azure/armres"
	"github.com/nrndev/pogi/internal/core"
)

// Resource-manager side: configuration stores and their access keys.

func (s *Service) registerARM() {
	arm.Register(s.hub, armres.Stored{Type: "Microsoft.AppConfiguration/configurationStores", Async: true, Mu: &s.mu, St: &s.st.ARM,
		Put: func(c *core.Ctx, id arm.ID, r map[string]any) error {
			if err := s.CreateStore(id.Name()); err != nil {
				return core.Errorf(http.StatusBadRequest, "InvalidResourceName", "%v", err)
			}
			s.mu.Lock()
			armres.Props(r)["creationDate"] = s.st.Stores[id.Name()].Created.UTC().Format(time.RFC3339)
			s.mu.Unlock()
			return nil
		},
		Delete:    func(c *core.Ctx, id arm.ID) { s.DeleteStore(id.Name()) },
		Tombstone: "deletedConfigurationStores",
		View: func(c *core.Ctx, r map[string]any) map[string]any {
			armres.Defaults(r, map[string]any{"sku": map[string]any{"name": "standard"}})
			p := armres.Props(r)
			armres.Defaults(p, map[string]any{"disableLocalAuth": false, "softDeleteRetentionInDays": 7,
				"enablePurgeProtection": false, "privateEndpointConnections": []any{}, "createMode": "Default"})
			enc, _ := p["encryption"].(map[string]any)
			if kv, _ := enc["keyVaultProperties"].(map[string]any); kv["keyIdentifier"] == nil {
				delete(p, "encryption") // the provider sends it empty, and an empty block would read as one that was set
			}
			p["provisioningState"], p["endpoint"] = "Succeeded", s.Endpoint(armres.Str(r, "name"))
			return r
		},
		Actions: map[string]func(*core.Ctx, arm.ID, map[string]any) (any, error){"listKeys": s.listKeys},
	}.Provider())
	arm.Register(s.hub, armres.TombstoneProvider("Microsoft.AppConfiguration", "deletedConfigurationStores", &s.mu, &s.st.ARM))
}

func (s *Service) listKeys(c *core.Ctx, id arm.ID, _ map[string]any) (any, error) {
	keys, err := s.AccessKeys(id.Name())
	if err != nil {
		return nil, arm.NotFound(id)
	}
	out := []any{}
	for _, k := range keys {
		out = append(out, map[string]any{"id": k.ID, "name": k.Name, "value": k.Secret, "readOnly": k.ReadOnly,
			"lastModified":     "2020-01-01T00:00:00+00:00",
			"connectionString": "Endpoint=" + s.Endpoint(id.Name()) + ";Id=" + k.ID + ";Secret=" + k.Secret})
	}
	return map[string]any{"value": out, "nextLink": nil}, nil
}
