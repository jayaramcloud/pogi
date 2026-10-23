package generic

import (
	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/azure/armres"
	"github.com/nrndev/pogi/internal/core"
)

// Azure Maps: an account has a client id (uniqueId) and two keys. No map, search or route API answers.

func init() {
	Override("Microsoft.Maps/accounts", Hook{
		Put: func(s *Service, c *core.Ctx, id arm.ID, r map[string]any) error {
			armres.Defaults(r, map[string]any{"kind": "Gen2", "sku": map[string]any{"name": "G2"}})
			armres.Defaults(armres.Props(r), map[string]any{"disableLocalAuth": false})
			return nil
		},
		View: func(s *Service, id arm.ID, r map[string]any) {
			p := armres.Props(r)
			h := hash8(id.String())
			p["uniqueId"] = h + "-" + h[:4] + "-4" + h[5:8] + "-8" + h[1:4] + "-" + h + h[:4]
			r["properties"] = p
		},
		Actions: map[string]func(*Service, *core.Ctx, arm.ID, map[string]any) (any, error){
			"listKeys": func(s *Service, c *core.Ctx, id arm.ID, _ map[string]any) (any, error) {
				p, q := accessKeys(id)
				return map[string]any{"primaryKey": p[:44], "secondaryKey": q[:44], "primaryKeyLastUpdated": now(), "secondaryKeyLastUpdated": now()}, nil
			},
		},
	})
}
