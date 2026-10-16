package armres

import (
	"sync"

	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/core"
)

// RuleProvider serves the shared access policies ("authorizationRules") of a Service Bus or Event Hubs
// namespace or entity of type typ, and their listKeys, with connection strings that reach the AMQP
// broker at endpoint (the hub's key for it, def its usual port). Every namespace has the rule
// RootManageSharedAccessKey. The rules are kept in st, guarded by mu.
func RuleProvider(typ, endpoint string, def int, mu sync.Locker, st *Store) arm.Provider {
	const root = "RootManageSharedAccessKey"
	view := func(id arm.ID, stored map[string]any) map[string]any {
		r := Envelope(id, stored)
		delete(r, "location")
		delete(r, "tags")
		Defaults(Props(r), map[string]any{"rights": []any{"Listen", "Manage", "Send"}})
		return r
	}
	get := func(c *core.Ctx, id arm.ID) (map[string]any, error) {
		mu.Lock()
		r, ok := st.Get(id)
		mu.Unlock()
		if !ok && !(id.Name() == root && len(id.Types) == 2) { // every namespace has the root rule
			return nil, arm.NotFound(id)
		}
		return view(id, r), nil
	}
	keys := func(c *core.Ctx, id arm.ID, _ map[string]any) (any, error) {
		if _, err := get(c, id); err != nil {
			return nil, err
		}
		addr := Hostport(c.Hub, endpoint, id.Names[0], def)
		conn := func(key string) string {
			cs := "Endpoint=sb://" + addr + ";SharedAccessKeyName=" + id.Name() + ";SharedAccessKey=" + key + ";UseDevelopmentEmulator=true"
			if len(id.Types) > 2 { // the rule of a queue, topic or event hub names its entity
				cs += ";EntityPath=" + id.Names[1]
			}
			return cs
		}
		k1, k2 := Key("sas/1/" + id.String())[:44], Key("sas/2/" + id.String())[:44]
		return map[string]any{"keyName": id.Name(), "primaryKey": k1, "secondaryKey": k2,
			"primaryConnectionString": conn(k1), "secondaryConnectionString": conn(k2)}, nil
	}
	return arm.Provider{Type: typ, Get: get,
		Put: func(c *core.Ctx, id arm.ID, body map[string]any) (map[string]any, error) {
			r := Envelope(id, body)
			mu.Lock()
			st.Put(id, r)
			mu.Unlock()
			return view(id, r), nil
		},
		Delete: func(c *core.Ctx, id arm.ID) error {
			mu.Lock()
			st.Delete(id)
			mu.Unlock()
			return nil
		},
		List: func(c *core.Ctx, parent arm.ID) ([]map[string]any, error) {
			mu.Lock()
			stored := st.List(parent, typ)
			mu.Unlock()
			var out []map[string]any
			hasRoot := len(parent.Types) != 1
			for _, r := range stored {
				id, _ := arm.ParseID(Str(r, "id"))
				hasRoot = hasRoot || id.Name() == root
				out = append(out, view(id, r))
			}
			if !hasRoot {
				out = append([]map[string]any{view(parent.Child("authorizationRules", root), nil)}, out...)
			}
			return out, nil
		},
		Actions: map[string]func(*core.Ctx, arm.ID, map[string]any) (any, error){"listKeys": keys, "regenerateKeys": keys},
	}
}
