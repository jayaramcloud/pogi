package email

import (
	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/azure/armres"
	"github.com/nrndev/pogi/internal/core"
)

// Resource-manager side: Communication Services resources and their keys.
// ponytail: only the Email part of Communication Services exists, and the
// email and domain resources (Microsoft.Communication/emailServices) are not
// served.

func (s *Service) registerARM() {
	h := s.hub
	endpoint := func(c *core.Ctx, name string) string {
		return armres.URL(c, "https", name+".communication", "/_azure/communication/"+name)
	}
	arm.Register(h, armres.Stored{Type: "Microsoft.Communication/communicationServices", Async: true, Mu: &s.mu, St: &s.st.ARM,
		Put: func(c *core.Ctx, id arm.ID, r map[string]any) error {
			s.mu.Lock()
			if s.st.Keys[id.Name()] == "" {
				s.st.Keys[id.Name()] = armres.Key("comm/1/" + id.Name())
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
			p := armres.Props(r)
			armres.Defaults(p, map[string]any{"dataLocation": "United States", "linkedDomains": []any{}})
			p["provisioningState"], p["hostName"], p["version"] = "Succeeded", armres.HostOf(endpoint(c, name)), "0.2.0"
			p["immutableResourceId"] = armres.Key("comm/id/" + name)[:36]
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
				cs := "endpoint=" + endpoint(c, id.Name()) + "/;accesskey=" + key
				return map[string]any{"primaryKey": key, "secondaryKey": key, "primaryConnectionString": cs, "secondaryConnectionString": cs}, nil
			},
		},
	}.Provider())
}
