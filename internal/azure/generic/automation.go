package generic

import (
	"net/http"
	"net/url"

	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/azure/armres"
	"github.com/nrndev/pogi/internal/core"
)

// Automation: accounts, and runbooks whose script is kept as text (PUT .../draft/content, POST publish, GET
// .../content). A runbook never runs: no job starts, and there are no schedules or modules.

const runbooks = "Microsoft.Automation/automationAccounts/runbooks"

func init() {
	Override("Microsoft.Automation/automationAccounts", Hook{Put: func(s *Service, c *core.Ctx, id arm.ID, r map[string]any) error {
		armres.Defaults(r, map[string]any{"sku": map[string]any{"name": "Basic"}})
		p := armres.Props(r)
		armres.Defaults(p, map[string]any{"state": "Ok", "publicNetworkAccess": true, "disableLocalAuth": false,
			"creationTime": now(), "automationHybridServiceUrl": "https://" + hash8(id.String()) + ".agentsvc.azure-automation.net/accounts/" + id.Name()})
		p["lastModifiedTime"] = now()
		return nil
	}})
	Override(runbooks, Hook{
		LongActions: []string{"publish"},
		Put: func(s *Service, c *core.Ctx, id arm.ID, r map[string]any) error {
			p := armres.Props(r)
			state := "New"
			if p["publishContentLink"] != nil {
				state = "Published"
			}
			created := now()
			if old, ok := s.Raw(id); ok { // an update keeps the state and the creation time
				state, created = armres.Str(armres.Props(old), "state"), armres.Str(armres.Props(old), "creationTime")
			}
			armres.Defaults(p, map[string]any{"state": state, "creationTime": created, "logProgress": false, "logVerbose": false,
				"description": "", "jobCount": 0})
			p["lastModifiedTime"] = now()
			return nil
		},
		Actions: map[string]func(*Service, *core.Ctx, arm.ID, map[string]any) (any, error){
			// the draft becomes the published script
			"publish": func(s *Service, c *core.Ctx, id arm.ID, _ map[string]any) (any, error) {
				doc, _ := s.Raw(id)
				armres.Props(doc)["state"] = "Published"
				return nil, s.Create(c, id, doc)
			},
		},
		Extra: func(s *Service) []arm.Provider {
			return []arm.Provider{{Type: runbooks + "/content", Doc: func(c *core.Ctx, parent arm.ID, _ url.Values) (any, error) {
				draft, _ := s.Raw(parent.Child("draft", "content"))
				return arm.Text(armres.Str(draft, "content")), nil
			}}}
		},
	})
	// the script is sent as text; the client accepts only 202
	Override(runbooks+"/draft", Hook{Created: http.StatusAccepted})
}
