package elasticache

// Role-based access control: users and user groups. A replication group or
// serverless cache with user groups gets their users in its server (ACL
// SETUSER), and a proxy in front when IAM users are among them (proxy.go).

import (
	"cmp"
	"fmt"
	"slices"
	"strings"

	"github.com/nrndev/pogi/internal/aws/rds"
	"github.com/nrndev/pogi/internal/core"
)

// User is an ElastiCache user.
type User struct {
	UserId, UserName, Status, Engine, MinimumEngineVersion, AccessString string
	UserGroupIds                                                         []string
	Authentication                                                       Authentication
	ARN                                                                  string
	Tags                                                                 []rds.Tag `json:",omitempty" query:"-"`
	Passwords                                                            []string  `json:"passwords,omitempty" query:"-"` // in the server; an IAM user's is pogi's secret
}

// Authentication is how a user signs in: password, no-password or iam.
type Authentication struct {
	Type          string
	PasswordCount int `json:",omitempty"`
}

// UserGroup is a user group.
type UserGroup struct {
	UserGroupId, Status, Engine         string
	UserIds                             []string
	MinimumEngineVersion                string
	ReplicationGroups, ServerlessCaches []string
	ARN                                 string
	Tags                                []rds.Tag `json:",omitempty" query:"-"`
}

// defaultUser is the user every account has, open to all.
func defaultUser(c *core.Ctx) *User {
	return &User{UserId: "default", UserName: "default", Status: "active", Engine: "redis", MinimumEngineVersion: "6.0",
		AccessString: "on ~* +@all", Authentication: Authentication{Type: "no-password"}, ARN: c.ARN("elasticache", "user:default")}
}

func (s *Service) user(c *core.Ctx, id string) (*User, error) {
	if u := s.st.Users[key(c, id)]; u != nil {
		return u, nil
	}
	if id == "default" {
		return defaultUser(c), nil
	}
	return nil, core.Errorf(404, "UserNotFound", "User %s not found.", id)
}

func (s *Service) userGroup(c *core.Ctx, id string) (*UserGroup, error) {
	if g := s.st.UserGroups[key(c, strings.ToLower(id))]; g != nil {
		return g, nil
	}
	return nil, core.Errorf(404, "UserGroupNotFound", "User group %s not found.", id)
}

// userView adds the groups of a user; lock held.
func (s *Service) userView(c *core.Ctx, u *User) *User {
	v := rds.Clone(u)
	for _, id := range core.SortedKeys(s.st.UserGroups) {
		if g := s.st.UserGroups[id]; mine(c, id) && slices.Contains(g.UserIds, u.UserId) {
			v.UserGroupIds = append(v.UserGroupIds, g.UserGroupId)
		}
	}
	return v
}

// userGroupView adds the caches of a user group; lock held.
// ponytail: rds.Lists names list items by list name, so ReplicationGroups
// items read <ReplicationGroup>, not <member>; SDKs see none.
func (s *Service) userGroupView(c *core.Ctx, g *UserGroup) *UserGroup {
	v := rds.Clone(g)
	for _, k := range core.SortedKeys(s.st.Groups) {
		if rg := s.st.Groups[k]; mine(c, k) && slices.Contains(rg.UserGroupIds, g.UserGroupId) {
			v.ReplicationGroups = append(v.ReplicationGroups, rg.ReplicationGroupId)
		}
	}
	for _, k := range core.SortedKeys(s.st.Serverless) {
		if sc := s.st.Serverless[k]; mine(c, k) && sc.UserGroupId == g.UserGroupId {
			v.ServerlessCaches = append(v.ServerlessCaches, sc.ServerlessCacheName)
		}
	}
	return v
}

// ---- users ----

type userInput struct {
	UserId, UserName, Engine, AccessString, AppendAccessString, Marker string
	Passwords                                                          []string
	NoPasswordRequired                                                 *bool
	AuthenticationMode                                                 *struct {
		Type      string
		Passwords []string
	}
	Tags       []rds.Tag
	MaxRecords int
}

// auth resolves the authentication of a create or modify: the server
// passwords of the user (IAM: a new secret) and the API's view of them. ok is
// false when the input says nothing about it.
func (in *userInput) auth(id, name string) (pws []string, a Authentication, ok bool, err error) {
	mode, pws := "", in.Passwords
	switch {
	case in.AuthenticationMode != nil:
		if mode = in.AuthenticationMode.Type; len(in.AuthenticationMode.Passwords) > 0 {
			pws = in.AuthenticationMode.Passwords
		}
	case in.NoPasswordRequired != nil && *in.NoPasswordRequired:
		mode = "no-password-required"
	case len(pws) > 0:
		mode = "password"
	default:
		return nil, a, false, nil
	}
	for _, p := range pws {
		if len(p) < 16 || len(p) > 128 {
			return nil, a, false, invalid("Passwords must be 16 to 128 characters long.")
		}
	}
	switch {
	case mode == "password" && len(pws) > 0 && len(pws) <= 2:
		return pws, Authentication{"password", len(pws)}, true, nil
	case mode == "no-password-required" && len(pws) == 0:
		return nil, Authentication{Type: "no-password"}, true, nil
	case mode == "iam" && len(pws) == 0 && id != name:
		return nil, a, false, invalid("An IAM-enabled user must have the same user id and user name.")
	case mode == "iam" && len(pws) == 0:
		return []string{core.RandString(32, core.AlphaNum)}, Authentication{Type: "iam"}, true, nil
	}
	return nil, a, false, combo("Specify one or two Passwords for the password authentication mode, and none otherwise (got %s).", mode)
}

func engineOf(e string) (string, error) {
	switch e = strings.ToLower(e); e {
	case "redis", "valkey":
		return e, nil
	}
	return "", invalid("Invalid engine: %s. Users and user groups support redis and valkey.", e)
}

func (s *Service) createUser(c *core.Ctx, in *userInput) (any, error) {
	engine, err := engineOf(in.Engine)
	if err != nil {
		return nil, err
	}
	if err := rds.ValidID("UserId", in.UserId, 40); err != nil {
		return nil, err
	}
	switch {
	case in.UserName == "":
		return nil, invalid("The parameter UserName must be provided and must not be blank.")
	case in.AccessString == "":
		return nil, invalid("The parameter AccessString must be provided and must not be blank.")
	}
	pws, a, ok, err := in.auth(in.UserId, in.UserName)
	if err == nil && !ok {
		err = combo("No password was given: specify Passwords, NoPasswordRequired or AuthenticationMode.")
	}
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.user(c, in.UserId); err == nil {
		return nil, core.Errorf(400, "UserAlreadyExists", "User %s already exists.", in.UserId)
	}
	u := &User{UserId: in.UserId, UserName: in.UserName, Status: "active", Engine: engine, MinimumEngineVersion: "6.0",
		AccessString: in.AccessString, Authentication: a, ARN: c.ARN("elasticache", "user:"+in.UserId), Tags: rds.AddTags(nil, in.Tags), Passwords: pws}
	if a.Type == "iam" {
		u.MinimumEngineVersion = "7.0"
	}
	s.st.Users[key(c, in.UserId)] = u
	return s.userView(c, u), nil
}

func (s *Service) describeUsers(c *core.Ctx, in *userInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	list := []*User{defaultUser(c)}
	for k, u := range s.st.Users {
		if mine(c, k) {
			list = append(list, u)
		}
	}
	list = slices.DeleteFunc(list, func(u *User) bool {
		return in.UserId != "" && u.UserId != in.UserId || in.Engine != "" && !strings.EqualFold(u.Engine, in.Engine)
	})
	if in.UserId != "" && len(list) == 0 {
		return nil, core.Errorf(404, "UserNotFound", "User %s not found.", in.UserId)
	}
	list, marker := rds.Page(list, func(u *User) string { return u.UserId }, in.Marker, in.MaxRecords)
	out := make([]*User, len(list))
	for n, u := range list {
		out[n] = s.userView(c, u)
	}
	return map[string]any{"Users": out, "Marker": marker}, nil
}

func (s *Service) modifyUser(c *core.Ctx, in *userInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	u, err := s.user(c, in.UserId)
	if err != nil {
		return nil, err
	}
	if s.st.Users[key(c, in.UserId)] == nil {
		return nil, invalid("The default user cannot be modified; make another user named default.")
	}
	pws, a, ok, err := in.auth(u.UserId, u.UserName)
	if err != nil {
		return nil, err
	}
	if ok {
		u.Passwords, u.Authentication = pws, a
	}
	u.AccessString = cmp.Or(in.AccessString, u.AccessString)
	if in.AppendAccessString != "" {
		u.AccessString += " " + in.AppendAccessString
	}
	s.sync()
	return s.userView(c, u), nil
}

func (s *Service) deleteUser(c *core.Ctx, in *userInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	u, err := s.user(c, in.UserId)
	if err != nil {
		return nil, err
	}
	v := s.userView(c, u)
	switch {
	case s.st.Users[key(c, in.UserId)] == nil:
		return nil, invalid("The default user cannot be deleted.")
	case u.UserName == "default" && len(v.UserGroupIds) > 0:
		return nil, core.Errorf(400, "DefaultUserAssociatedToUserGroup", "User %s is the default user of user group %s.", u.UserId, v.UserGroupIds[0])
	}
	for k, g := range s.st.UserGroups { // it leaves its groups, and so their caches
		if mine(c, k) {
			g.UserIds = slices.DeleteFunc(g.UserIds, func(id string) bool { return id == u.UserId })
		}
	}
	delete(s.st.Users, key(c, u.UserId))
	s.sync()
	v.Status = "deleting"
	return v, nil
}

// ---- user groups ----

type userGroupInput struct {
	UserGroupId, Engine, Marker            string
	UserIds, UserIdsToAdd, UserIdsToRemove []string
	Tags                                   []rds.Tag
	MaxRecords                             int
}

// checkUsers validates the users of a user group: they exist, and exactly
// one of them, by name, is default. Lock held.
func (s *Service) checkUsers(c *core.Ctx, ids []string) error {
	names := map[string]bool{}
	for _, id := range ids {
		u, err := s.user(c, id)
		if err != nil {
			return err
		}
		if names[u.UserName] {
			return core.Errorf(400, "DuplicateUserName", "A user group cannot have two users named %s.", u.UserName)
		}
		names[u.UserName] = true
	}
	if !names["default"] {
		return core.Errorf(400, "DefaultUserRequired", "A user group must contain a user named default.")
	}
	return nil
}

func (s *Service) createUserGroup(c *core.Ctx, in *userGroupInput) (any, error) {
	id := strings.ToLower(in.UserGroupId)
	engine, err := engineOf(in.Engine)
	if err != nil {
		return nil, err
	}
	if err := rds.ValidID("UserGroupId", id, 40); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.st.UserGroups[key(c, id)] != nil {
		return nil, core.Errorf(400, "UserGroupAlreadyExists", "User group %s already exists.", id)
	}
	ids := slices.Compact(slices.Sorted(slices.Values(in.UserIds)))
	if err := s.checkUsers(c, ids); err != nil {
		return nil, err
	}
	g := &UserGroup{UserGroupId: id, Status: "active", Engine: engine, UserIds: ids, MinimumEngineVersion: "6.0",
		ARN: c.ARN("elasticache", "usergroup:"+id), Tags: rds.AddTags(nil, in.Tags)}
	s.st.UserGroups[key(c, id)] = g
	return s.userGroupView(c, g), nil
}

func (s *Service) describeUserGroups(c *core.Ctx, in *userGroupInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var list []*UserGroup
	if in.UserGroupId != "" {
		g, err := s.userGroup(c, in.UserGroupId)
		if err != nil {
			return nil, err
		}
		list = append(list, g)
	} else {
		for k, g := range s.st.UserGroups {
			if mine(c, k) {
				list = append(list, g)
			}
		}
	}
	list, marker := rds.Page(list, func(g *UserGroup) string { return g.UserGroupId }, in.Marker, in.MaxRecords)
	out := make([]*UserGroup, len(list))
	for n, g := range list {
		out[n] = s.userGroupView(c, g)
	}
	return map[string]any{"UserGroups": out, "Marker": marker}, nil
}

func (s *Service) modifyUserGroup(c *core.Ctx, in *userGroupInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, err := s.userGroup(c, in.UserGroupId)
	if err != nil {
		return nil, err
	}
	ids := slices.DeleteFunc(append(slices.Clone(g.UserIds), in.UserIdsToAdd...), func(id string) bool { return slices.Contains(in.UserIdsToRemove, id) })
	ids = slices.Compact(slices.Sorted(slices.Values(ids)))
	if err := s.checkUsers(c, ids); err != nil {
		return nil, err
	}
	g.UserIds = ids
	s.sync()
	return s.userGroupView(c, g), nil
}

func (s *Service) deleteUserGroup(c *core.Ctx, in *userGroupInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, err := s.userGroup(c, in.UserGroupId)
	if err != nil {
		return nil, err
	}
	v := s.userGroupView(c, g)
	if len(v.ReplicationGroups)+len(v.ServerlessCaches) > 0 {
		return nil, core.Errorf(400, "InvalidUserGroupState", "User group %s is in use by %s.", g.UserGroupId, slices.Concat(v.ReplicationGroups, v.ServerlessCaches)[0])
	}
	delete(s.st.UserGroups, key(c, g.UserGroupId))
	v.Status = "deleting"
	return v, nil
}

// checkGroups validates the user groups given to a cache; lock held.
func (s *Service) checkGroups(c *core.Ctx, ids []string) error {
	for _, id := range ids {
		if _, err := s.userGroup(c, id); err != nil {
			return err
		}
	}
	return nil
}

// checkIAM refuses user groups with an IAM user on a Redis older than 7,
// which cannot take IAM sign-in (Valkey always can). AWS also wants in-transit
// encryption; pogi does not check that, as it serves plain connections. Lock held.
func (s *Service) checkIAM(c *core.Ctx, ids []string, engine, ver string) error {
	if engine != "redis" || ver == "" || ver[0] >= '7' {
		return nil
	}
	for _, id := range ids {
		g, err := s.userGroup(c, id)
		if err != nil {
			return err
		}
		for _, uid := range g.UserIds {
			if u, err := s.user(c, uid); err == nil && u.Authentication.Type == "iam" {
				return combo("User %s uses IAM authentication, which needs Redis 7 or newer (or Valkey).", u.UserId)
			}
		}
	}
	return nil
}

// ---- users in the servers ----

// ACLUser is a user of a Redis or Valkey server.
type ACLUser struct {
	Name, Access string   // access string, e.g. "on ~* &* +@all"
	Passwords    []string // none: nopass
}

// Admin is the user pogi works as in a server whose users it manages, as
// those may turn the default user off.
const Admin = "pogi-admin"

// ACLScript is a shell script, for the container of a Redis or Valkey
// server, that gives the server exactly users plus pogi's Admin (with
// secret). It waits for the server, so it may run while that starts; the
// default user of a new or restarted server is open and makes the admin. A
// cluster-mode server gets its slots there too.
func ACLScript(engine, secret string, cluster bool, users []ACLUser) string {
	q := func(s string) string { return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'" }
	cli, as := engine+"-cli", engine+"-cli --no-auth-warning --user "+Admin+" --pass "+q(secret)
	var b strings.Builder
	fmt.Fprintf(&b, `for i in $(seq 150); do %s ACL SETUSER %s on %s '~*' '&*' '+@all' >/dev/null 2>&1; [ "$(%s ping 2>/dev/null)" = PONG ] && break; sleep 0.2; done`,
		cli, Admin, q(">"+secret), as)
	if cluster {
		b.WriteString("; " + as + " cluster addslotsrange 0 16383 >/dev/null 2>&1")
	}
	fmt.Fprintf(&b, `; for u in $(%s ACL USERS); do [ "$u" = default ] || [ "$u" = %s ] || %s ACL DELUSER "$u"; done`, as, Admin, as)
	for _, u := range users {
		b.WriteString("; " + as + " ACL SETUSER " + q(u.Name) + " reset")
		for _, r := range strings.Fields(u.Access) {
			b.WriteString(" " + q(r))
		}
		for _, p := range u.Passwords {
			b.WriteString(" " + q(">"+p))
		}
		if len(u.Passwords) == 0 {
			b.WriteString(" nopass")
		}
	}
	return b.String()
}

// open is the user of a server without user groups.
var open = []ACLUser{{Name: "default", Access: "on ~* &* +@all"}}

// rbac is a server that may have user groups: a replication group's or a serverless cache's.
type rbac struct {
	c       *core.Ctx
	name    string // what IAM auth tokens name
	srv     *rds.Server
	engine  string
	cluster bool
	groups  []string
}

// rbacs lists the servers that may have user groups; lock held.
func (s *Service) rbacs() []rbac {
	var out []rbac
	for k, g := range s.st.Groups {
		out = append(out, rbac{ctxOf(k), g.ReplicationGroupId, &g.Pogi, g.Engine, g.ClusterEnabled, g.UserGroupIds})
	}
	for k, sc := range s.st.Serverless {
		var groups []string
		if sc.UserGroupId != "" {
			groups = []string{sc.UserGroupId}
		}
		out = append(out, rbac{ctxOf(k), sc.ServerlessCacheName, &sc.Pogi, sc.Engine, false, groups})
	}
	return out
}

// users are the users of a server's user groups, by name; lock held.
func (s *Service) users(r rbac) map[string]*User {
	out := map[string]*User{}
	for _, gid := range r.groups {
		if g := s.st.UserGroups[key(r.c, gid)]; g != nil {
			for _, id := range g.UserIds {
				if u, err := s.user(r.c, id); err == nil && out[u.UserName] == nil {
					out[u.UserName] = u
				}
			}
		}
	}
	return out
}

// sync puts the users of their user groups in the servers that run, and a
// proxy in front of those with IAM users. Lock held.
func (s *Service) sync() {
	live := map[string]bool{}
	for _, r := range s.rbacs() {
		if r.srv.Container == "" || r.engine == "memcached" {
			continue
		}
		name, users := r.srv.Container, s.users(r)
		live[name] = true
		var list []ACLUser
		iam := false
		for _, n := range core.SortedKeys(users) {
			u := users[n]
			list = append(list, ACLUser{u.UserName, u.AccessString, u.Passwords})
			iam = iam || u.Authentication.Type == "iam"
		}
		want := ""
		if len(r.groups) > 0 {
			r.srv.Password = cmp.Or(r.srv.Password, core.RandString(32, core.AlphaNum)) // the admin's
			want = ACLScript(r.engine, r.srv.Password, r.cluster, list)
		}
		if want != s.applied[name] { // no groups any more: back to open
			s.applied[name] = want
			s.box.Exec(r.srv, "sh", "-c", cmp.Or(want, ACLScript(r.engine, r.srv.Password, r.cluster, open)))
		}
		switch p := s.proxies[name]; {
		case iam && p == nil:
			if p = s.listen(name); p != nil {
				s.proxies[name] = p
			}
		case !iam && p != nil:
			p.ln.Close()
			delete(s.proxies, name)
		}
	}
	for name, p := range s.proxies {
		if !live[name] {
			p.ln.Close()
			delete(s.proxies, name)
		}
	}
}
