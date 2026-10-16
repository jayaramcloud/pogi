package signalr

import (
	"encoding/json"
	"net/http"
	"slices"
	"strings"

	"github.com/nrndev/pogi/internal/core"
)

// rest is the server API. The v1 form (/api/v1/hubs/<hub>/...) answers 202 to everything that changes
// something; the 2022-11-01 form (/api/hubs/<hub>/..., sends as .../:send) answers 202 to sends, 200 to
// adding and 204 to removing. Any bearer token is accepted.
func (s *Service) rest(w http.ResponseWriter, r *http.Request, c *core.Ctx, name, path string) {
	v1 := strings.HasPrefix(path, "/api/v1/")
	if path == "/api/health" || path == "/api/v1/health" {
		c.Op = "GetHealthStatus"
		w.WriteHeader(http.StatusOK)
		return
	}
	rest, ok := strings.CutPrefix(path, "/api/v1/hubs/")
	if !ok {
		rest, ok = strings.CutPrefix(path, "/api/hubs/")
	}
	hub, rest, _ := strings.Cut(rest, "/")
	if !ok || hub == "" {
		writeError(w, c, http.StatusNotFound, "NotFound", "pogi does not implement this SignalR server API.")
		return
	}
	// the 2022 API marks sends with a trailing :send, as in /users/bob/:send
	p := strings.Split(strings.Trim(rest, "/"), "/")
	if last := len(p) - 1; last >= 0 && (p[last] == ":send" || strings.HasSuffix(p[last], ":send")) {
		p[last] = strings.TrimSuffix(p[last], ":send")
	}
	p = slices.DeleteFunc(p, func(x string) bool { return x == "" })
	accepted, added, removed := http.StatusAccepted, http.StatusAccepted, http.StatusAccepted
	if !v1 {
		added, removed = http.StatusOK, http.StatusNoContent
	}
	status, op := 0, ""
	m := r.Method
	switch {
	case m == http.MethodPost && len(p) == 0:
		op, status = "SendToAll", s.broadcast(r, name, hub, func(*conn) bool { return true }, accepted)
	case m == http.MethodPost && len(p) == 2 && p[0] == "users":
		op, status = "SendToUser", s.broadcast(r, name, hub, func(cn *conn) bool { return cn.user == p[1] }, accepted)
	case m == http.MethodPost && len(p) == 2 && p[0] == "connections":
		op, status = "SendToConnection", s.broadcast(r, name, hub, func(cn *conn) bool { return cn.id == p[1] }, accepted)
	case m == http.MethodPost && len(p) == 2 && p[0] == "groups":
		op, status = "SendToGroup", s.toGroup(r, name, hub, p[1], accepted)

	case m == http.MethodHead && len(p) == 2 && p[0] == "connections":
		op, status = "CheckConnection", s.check(name, hub, func(h *hubState) bool { return h.conns[p[1]] != nil })
	case m == http.MethodHead && len(p) == 2 && p[0] == "users":
		op, status = "CheckUser", s.check(name, hub, func(h *hubState) bool { return userConns(h, p[1]) })
	case m == http.MethodHead && len(p) == 2 && p[0] == "groups":
		op, status = "CheckGroup", s.check(name, hub, func(h *hubState) bool { g := h.groups[p[1]]; return g != nil && len(g.conns)+len(g.users) > 0 })
	case m == http.MethodHead && len(p) == 4 && p[0] == "groups" && p[2] == "connections":
		op, status = "CheckConnectionInGroup", s.check(name, hub, func(h *hubState) bool { g := h.groups[p[1]]; return g != nil && g.conns[p[3]] })
	case m == http.MethodHead && isUserGroup(p):
		g, u := userGroup(p)
		op, status = "CheckUserInGroup", s.check(name, hub, func(h *hubState) bool { x := h.groups[g]; return x != nil && x.users[u] })

	case m == http.MethodPut && len(p) == 4 && p[0] == "groups" && p[2] == "connections":
		op, status = "AddConnectionToGroup", s.member(name, hub, p[1], p[3], "", true, added)
	case m == http.MethodDelete && len(p) == 4 && p[0] == "groups" && p[2] == "connections":
		op, status = "RemoveConnectionFromGroup", s.member(name, hub, p[1], p[3], "", false, removed)
	case m == http.MethodPut && isUserGroup(p):
		g, u := userGroup(p)
		op, status = "AddUserToGroup", s.member(name, hub, g, "", u, true, added)
	case m == http.MethodDelete && isUserGroup(p):
		g, u := userGroup(p)
		op, status = "RemoveUserFromGroup", s.member(name, hub, g, "", u, false, removed)
	case m == http.MethodDelete && len(p) == 3 && p[0] == "users" && p[2] == "groups":
		op, status = "RemoveUserFromAllGroups", s.dropUser(name, hub, p[1], removed)
	case m == http.MethodDelete && len(p) == 2 && p[0] == "connections":
		op, status = "CloseConnection", s.kick(name, hub, p[1], removed)
	default:
		writeError(w, c, http.StatusNotFound, "NotFound", "pogi does not implement this SignalR server API.")
		return
	}
	c.Op = op
	switch {
	case status == http.StatusNotFound && m != http.MethodHead:
		writeError(w, c, status, "NotFound", "The connection does not exist.")
	case status == http.StatusBadRequest:
		writeError(w, c, status, "BadRequest", `The body must be {"target": "<method>", "arguments": [...]}.`)
	default:
		w.WriteHeader(status)
	}
}

// isUserGroup matches users/<u>/groups/<g> and groups/<g>/users/<u>.
func isUserGroup(p []string) bool {
	return len(p) == 4 && (p[0] == "users" && p[2] == "groups" || p[0] == "groups" && p[2] == "users")
}

func userGroup(p []string) (grp, user string) {
	if p[0] == "users" {
		return p[3], p[1]
	}
	return p[1], p[3]
}

// remove forgets a connection and its group memberships.
func (h *hubState) remove(id string) {
	delete(h.conns, id)
	for _, g := range h.groups {
		delete(g.conns, id)
	}
}

func userConns(h *hubState, user string) bool {
	for _, cn := range h.conns {
		if cn.user == user {
			return true
		}
	}
	return false
}

func (s *Service) check(name, hub string, f func(*hubState) bool) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if f(s.hub(name, hub)) {
		return http.StatusOK
	}
	return http.StatusNotFound
}

// frame builds the invocation a request body describes.
func frame(r *http.Request) ([]byte, bool) {
	var in struct {
		Target    string `json:"target"`
		Arguments []any  `json:"arguments"`
	}
	if json.Unmarshal(core.Body(r), &in) != nil || in.Target == "" {
		return nil, false
	}
	if in.Arguments == nil {
		in.Arguments = []any{}
	}
	b, _ := json.Marshal(struct { // the fields in the order SignalR servers write them
		Type      int    `json:"type"`
		Target    string `json:"target"`
		Arguments []any  `json:"arguments"`
	}{1, in.Target, in.Arguments})
	return append(b, rs...), true
}

// broadcast sends the body's invocation to the connections of a hub that pass want, minus ?excluded=.
func (s *Service) broadcast(r *http.Request, name, hub string, want func(*conn) bool, ok int) int {
	f, valid := frame(r)
	if !valid {
		return http.StatusBadRequest
	}
	excluded := r.URL.Query()["excluded"]
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, cn := range s.hub(name, hub).conns {
		if want(cn) && !slices.Contains(excluded, cn.id) {
			cn.send(f)
		}
	}
	return ok
}

// toGroup sends to the connections in a group, and to those of the users in it.
func (s *Service) toGroup(r *http.Request, name, hub, grp string, ok int) int {
	var members func(*conn) bool
	s.mu.Lock()
	if g := s.hub(name, hub).groups[grp]; g != nil {
		members = func(cn *conn) bool { return g.conns[cn.id] || cn.user != "" && g.users[cn.user] }
	}
	s.mu.Unlock()
	if members == nil {
		if _, valid := frame(r); !valid {
			return http.StatusBadRequest
		}
		return ok // an empty group hears nothing
	}
	return s.broadcast(r, name, hub, members, ok)
}

// member adds or removes a connection or a user to or from a group.
func (s *Service) member(name, hub, grp, connID, user string, add bool, ok int) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	h := s.hub(name, hub)
	g := h.groups[grp]
	if add {
		if connID != "" && h.conns[connID] == nil {
			return http.StatusNotFound
		}
		if g == nil {
			g = &group{conns: map[string]bool{}, users: map[string]bool{}}
			h.groups[grp] = g
		}
		if connID != "" {
			g.conns[connID] = true
		} else {
			g.users[user] = true
		}
		return ok
	}
	if g != nil {
		delete(g.conns, connID)
		delete(g.users, user)
		if len(g.conns)+len(g.users) == 0 {
			delete(h.groups, grp)
		}
	}
	return ok
}

func (s *Service) dropUser(name, hub, user string, ok int) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	h := s.hub(name, hub)
	for k, g := range h.groups {
		delete(g.users, user)
		for id, cn := range h.conns {
			if cn.user == user {
				delete(g.conns, id)
			}
		}
		if len(g.conns)+len(g.users) == 0 {
			delete(h.groups, k)
		}
	}
	return ok
}

func (s *Service) kick(name, hub, id string, ok int) int {
	s.mu.Lock()
	h := s.hub(name, hub)
	cn := h.conns[id]
	h.remove(id) // at once, not when the socket is done closing
	s.mu.Unlock()
	if cn == nil {
		return http.StatusNotFound
	}
	cn.close()
	return ok
}
