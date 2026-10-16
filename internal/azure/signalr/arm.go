package signalr

import (
	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/azure/armres"
	"github.com/nrndev/pogi/internal/core"
)

// Resource-manager side: SignalR resources and their keys. A resource's
// access key is the one its negotiation tokens are signed with.

func (s *Service) registerARM(h *core.Hub) {
	endpoint := func(c *core.Ctx, name string) string {
		return armres.URL(c, "https", name+".service.signalr", "/_azure/signalr/"+name)
	}
	arm.Register(h, armres.Stored{Type: "Microsoft.SignalRService/signalR", Async: true, Mu: &s.mu, St: &s.st.ARM,
		Put: func(c *core.Ctx, id arm.ID, r map[string]any) error {
			s.mu.Lock()
			if s.st.Keys[id.Name()] == "" || s.st.Keys[id.Name()] == DefaultKey {
				s.st.Keys[id.Name()] = armres.Key("signalr/1/" + id.Name())[:44]
			}
			s.mu.Unlock()
			return nil
		},
		Delete: func(c *core.Ctx, id arm.ID) {
			s.mu.Lock()
			delete(s.st.Keys, id.Name())
			s.mu.Unlock()
		},
		View: func(c *core.Ctx, r map[string]any) map[string]any {
			name := armres.Str(r, "name")
			armres.Defaults(r, map[string]any{"kind": "SignalR", "sku": map[string]any{"name": "Standard_S1", "tier": "Standard", "capacity": 1}})
			p := armres.Props(r)
			armres.Defaults(p, map[string]any{"features": []any{}, "cors": map[string]any{"allowedOrigins": []any{"*"}},
				"upstream": map[string]any{"templates": []any{}}, "publicNetworkAccess": "Enabled", "disableLocalAuth": false, "disableAadAuth": false,
				"tls": map[string]any{"clientCertEnabled": false}, "networkACLs": map[string]any{"defaultAction": "Deny"}})
			p["provisioningState"], p["version"], p["publicPort"], p["serverPort"] = "Succeeded", "1.0", 443, 443
			p["hostName"], p["externalIP"], p["hostNamePrefix"] = armres.HostOf(endpoint(c, name)), "127.0.0.1", name
			return r
		},
		Actions: map[string]func(*core.Ctx, arm.ID, map[string]any) (any, error){
			"listKeys": func(c *core.Ctx, id arm.ID, _ map[string]any) (any, error) {
				s.mu.Lock()
				_, ok := s.st.ARM.Get(id)
				key := s.st.Keys[id.Name()]
				s.mu.Unlock()
				if !ok {
					return nil, arm.NotFound(id)
				}
				cs := "Endpoint=" + endpoint(c, id.Name()) + ";AccessKey=" + key + ";Version=1.0;"
				return map[string]any{"primaryKey": key, "secondaryKey": key, "primaryConnectionString": cs, "secondaryConnectionString": cs}, nil
			},
		},
	}.Provider())
}
