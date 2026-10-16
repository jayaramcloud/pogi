package signalr

import (
	"strings"

	"github.com/nrndev/pogi/internal/core"
)

// Resources lists the resources, and the connections and groups that exist now.
func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	res := core.Table{Name: "Resources", Columns: []string{"Name", "Access key"}}
	for _, k := range core.SortedKeys(s.st.Keys) {
		res.Rows = append(res.Rows, []string{k, s.st.Keys[k]})
	}
	conns := core.Table{Name: "Connections", Columns: []string{"Id", "Resource", "Hub", "User"}}
	groups := core.Table{Name: "Groups", Columns: []string{"Group", "Resource", "Hub", "Connections", "Users"}}
	for _, k := range core.SortedKeys(s.hubs) {
		resource, hub, _ := strings.Cut(k, "/")
		h := s.hubs[k]
		for _, id := range core.SortedKeys(h.conns) {
			conns.Rows = append(conns.Rows, []string{id, resource, hub, h.conns[id].user})
		}
		for _, g := range core.SortedKeys(h.groups) {
			groups.Rows = append(groups.Rows, []string{g, resource, hub, strings.Join(core.SortedKeys(h.groups[g].conns), ", "), strings.Join(core.SortedKeys(h.groups[g].users), ", ")})
		}
	}
	return []core.Table{res, conns, groups}
}

// Inspect returns a resource with its endpoints, or a connection with its groups.
func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch table {
	case "Resources":
		if key, ok := s.st.Keys[id]; ok {
			return map[string]any{"name": id, "accessKey": key,
				"endpoint":   "https://" + id + ".service.signalr.localhost:7644",
				"pathForm":   "/_azure/signalr/" + id,
				"negotiate":  "POST /client/negotiate?hub=<hub>",
				"serverApis": []string{"/api/v1/hubs/<hub>", "/api/hubs/<hub>/:send?api-version=2022-11-01"}}
		}
	case "Connections":
		for _, k := range core.SortedKeys(s.hubs) {
			if cn := s.hubs[k].conns[id]; cn != nil {
				var in []string
				for g, m := range s.hubs[k].groups {
					if m.conns[id] || cn.user != "" && m.users[cn.user] {
						in = append(in, g)
					}
				}
				return map[string]any{"id": id, "resource": strings.SplitN(k, "/", 2)[0], "hub": cn.hub, "user": cn.user, "groups": in}
			}
		}
	}
	return nil
}
