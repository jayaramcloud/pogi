// Package graph emulates the part of Microsoft Graph that the azuread
// Terraform provider and `az ad` use: applications, service principals, users
// and groups (create, read, update, delete, filter), client secrets
// (addPassword, removePassword), owners and members, /me, /organization and
// /domains, on /v1.0 and /beta of pogi's main port. Requests need a Bearer
// token and any one will do.
//
// The directory is also what the Entra token endpoint authenticates against:
// a client that is an application or service principal with secrets must
// present one of them.
package graph

import (
	"encoding/json"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/core"
)

// Service is Microsoft Graph.
type Service struct {
	mu sync.Mutex
	st state
}

type state struct {
	// Objects are the directory's objects by collection, then by object ID.
	// Keys that start with "_" are internal: "_owners" and "_members" (object IDs), "_password" (users).
	Objects map[string]map[string]map[string]any `json:"objects"`
	// Secrets are the client secrets handed out by addPassword, "<object ID>/<key ID>": the
	// objects themselves keep only the hint, as Graph does.
	Secrets map[string]string `json:"secrets"`
}

var collections = []string{"applications", "servicePrincipals", "users", "groups"}

// odata types by collection.
var odataType = map[string]string{"applications": "#microsoft.graph.application", "servicePrincipals": "#microsoft.graph.servicePrincipal",
	"users": "#microsoft.graph.user", "groups": "#microsoft.graph.group"}

func New(*core.Hub) *Service {
	s := &Service{}
	s.Reset()
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "graph", Cloud: core.Azure, Title: "Microsoft Graph", Protocol: "azure-json", Match: match}
}

func match(r *http.Request) bool {
	first, _, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/"), "/")
	return first == "v1.0" || first == "beta"
}

// ---- the directory, for Entra ----

// App finds an application or service principal by client ID. oid is the
// service principal's object ID when there is one; secrets are the secrets
// of both that have not expired.
func (s *Service) App(appID string) (oid string, secrets []string, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, coll := range []string{"applications", "servicePrincipals"} {
		for id, o := range s.st.Objects[coll] {
			if !strings.EqualFold(str(o["appId"]), appID) {
				continue
			}
			if oid == "" || coll == "servicePrincipals" {
				oid = id
			}
			ok = true
			for _, pc := range list(o["passwordCredentials"]) {
				if end, err := time.Parse(time.RFC3339, str(pc["endDateTime"])); err == nil && end.Before(time.Now()) {
					continue
				}
				secrets = append(secrets, s.st.Secrets[id+"/"+str(pc["keyId"])])
			}
		}
	}
	return oid, secrets, ok
}

// User finds a user by principal name or object ID.
func (s *Service) User(name string) (oid, upn, password string, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if id, u := s.findUser(name); u != nil {
		return id, str(u["userPrincipalName"]), str(u["_password"]), true
	}
	return "", "", "", false
}

func (s *Service) findUser(name string) (string, map[string]any) {
	for id, u := range s.st.Objects["users"] {
		if id == name || strings.EqualFold(str(u["userPrincipalName"]), name) {
			return id, u
		}
	}
	return "", nil
}

// ---- requests ----

var keyed = regexp.MustCompile(`^(\w+)\((\w+)='([^']*)'\)$`) // applications(appId='...')

type call struct {
	r      *http.Request
	c      *core.Ctx
	base   string // scheme://host/v1.0
	origin string // scheme://host
	q      url.Values
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c := core.C(r)
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	seg := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	k := &call{r: r, c: c, base: scheme + "://" + r.Host + "/" + seg[0], origin: scheme + "://" + r.Host, q: r.URL.Query()}
	c.Op = r.Method + " " + strings.Join(seg[1:min(len(seg), 3)], "/")
	status, out, err := 0, any(nil), c.Intercept()
	if err == nil {
		if scheme, _, _ := strings.Cut(r.Header.Get("Authorization"), " "); !strings.EqualFold(scheme, "Bearer") {
			err = apiError(http.StatusUnauthorized, "InvalidAuthenticationToken", "Access token is empty.")
		} else {
			func() {
				s.mu.Lock()
				defer s.mu.Unlock()
				status, out, err = s.route(k, seg[1:])
			}()
		}
	}
	if err != nil {
		e := core.AsError(err)
		c.ErrCode = e.Code
		status, out = e.Status, map[string]any{"error": map[string]any{"code": e.Code, "message": e.Msg,
			"innerError": map[string]any{"date": time.Now().UTC().Format(time.RFC3339), "request-id": c.RequestID, "client-request-id": c.RequestID}}}
	}
	h := w.Header()
	h.Set("request-id", c.RequestID)
	h.Set("client-request-id", c.RequestID)
	if out == nil {
		w.WriteHeader(status)
		return
	}
	h.Set("Content-Type", "application/json;odata.metadata=minimal;odata.streaming=true;IEEE754Compatible=false;charset=utf-8")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(out)
}

func apiError(status int, code, format string, a ...any) error {
	return core.Errorf(status, code, format, a...)
}

func notFound(id string) error {
	return apiError(http.StatusNotFound, "Request_ResourceNotFound", "Resource '%s' does not exist or one of its queried reference-property objects are not present.", id)
}

func badRequest(format string, a ...any) error {
	return apiError(http.StatusBadRequest, "Request_BadRequest", format, a...)
}

func str(v any) string { s, _ := v.(string); return s }

// list reads a JSON array of objects.
func list(v any) []map[string]any {
	var out []map[string]any
	items, _ := v.([]any)
	for _, it := range items {
		if m, ok := it.(map[string]any); ok {
			out = append(out, m)
		}
	}
	return out
}

func (s *Service) route(k *call, seg []string) (int, any, error) {
	m := k.r.Method
	if len(seg) == 0 {
		return 0, nil, apiError(http.StatusNotFound, "Request_ResourceNotFound", "Resource not found for the segment ''.")
	}
	name, key, keyVal := seg[0], "", ""
	if g := keyed.FindStringSubmatch(seg[0]); g != nil {
		name, key, keyVal = g[1], g[2], g[3]
	}
	switch name {
	case "me":
		return s.me(k)
	case "organization":
		org := map[string]any{"id": arm.TenantID, "displayName": "pogi", "tenantType": "AAD", "verifiedDomains": []any{map[string]any{
			"name": "pogi.onmicrosoft.com", "isDefault": true, "isInitial": true, "type": "Managed"}}}
		if len(seg) > 1 {
			return http.StatusOK, org, nil
		}
		return http.StatusOK, k.envelope("organization", []map[string]any{org}), nil
	case "domains":
		return http.StatusOK, k.envelope("domains", []map[string]any{{"id": "pogi.onmicrosoft.com", "isDefault": true, "isInitial": true,
			"isVerified": true, "authenticationType": "Managed"}}), nil
	case "directoryObjects":
		if len(seg) == 2 {
			if coll, id := s.find(seg[1]); coll != "" {
				return http.StatusOK, s.render(k, coll, id, nil), nil
			}
			return 0, nil, notFound(seg[1])
		}
	case "applications", "servicePrincipals", "users", "groups":
		return s.collection(k, name, key, keyVal, seg[1:], m)
	}
	return 0, nil, apiError(http.StatusNotFound, "Request_ResourceNotFound", "Resource not found for the segment '%s'.", seg[0])
}

func (s *Service) me(k *call) (int, any, error) {
	claims := jwtClaims(strings.TrimPrefix(k.r.Header.Get("Authorization"), "Bearer "))
	if id, u := s.findUser(str(claims["oid"])); u != nil {
		return http.StatusOK, s.render(k, "users", id, nil), nil
	}
	upn := str(claims["upn"])
	if upn == "" {
		return 0, nil, badRequest("/me request is only valid with delegated authentication flow.")
	}
	return http.StatusOK, map[string]any{"@odata.context": k.base + "/$metadata#users/$entity", "id": claims["oid"], "userPrincipalName": upn,
		"displayName": str(claims["name"]), "mail": nil, "accountEnabled": true, "userType": "Member"}, nil
}

// jwtClaims reads a JWT's claims without checking it.
func jwtClaims(tok string) map[string]any {
	_, rest, _ := strings.Cut(tok, ".")
	body, _, _ := strings.Cut(rest, ".")
	b, _ := decodeB64(body)
	var claims map[string]any
	json.Unmarshal(b, &claims)
	return claims
}

// find looks an object up by ID in every collection.
func (s *Service) find(id string) (coll, oid string) {
	for _, c := range collections {
		if _, ok := s.st.Objects[c][id]; ok {
			return c, id
		}
	}
	return "", ""
}

// resolve finds the object a path names: by object ID, by user principal
// name, or by an alternate key such as applications(appId='...').
func (s *Service) resolve(coll, key, val, id string) (string, map[string]any) {
	if key == "" && id != "" {
		if o, ok := s.st.Objects[coll][id]; ok {
			return id, o
		}
		if coll == "users" {
			return s.findUser(id)
		}
		return "", nil
	}
	for oid, o := range s.st.Objects[coll] {
		if strings.EqualFold(str(o[key]), val) {
			return oid, o
		}
	}
	return "", nil
}

func (s *Service) collection(k *call, coll, key, keyVal string, rest []string, m string) (int, any, error) {
	if len(rest) == 0 && key == "" { // the collection
		switch m {
		case http.MethodGet:
			return s.listColl(k, coll)
		case http.MethodPost:
			return s.create(k, coll)
		}
		return 0, nil, apiError(http.StatusMethodNotAllowed, "Request_BadRequest", "Method %s is not allowed on %s.", m, coll)
	}
	id := ""
	if key == "" {
		id, rest = rest[0], rest[1:]
	}
	oid, obj := s.resolve(coll, key, keyVal, id)
	if obj == nil {
		return 0, nil, notFound(id + keyVal)
	}
	if len(rest) == 0 {
		switch m {
		case http.MethodGet:
			return http.StatusOK, s.render(k, coll, oid, selectFields(k.q)), nil
		case http.MethodPatch:
			return s.update(k, coll, oid, obj)
		case http.MethodDelete:
			s.remove(coll, oid)
			return http.StatusNoContent, nil, nil
		}
		return 0, nil, apiError(http.StatusMethodNotAllowed, "Request_BadRequest", "Method %s is not allowed on %s.", m, coll)
	}
	switch rest[0] {
	case "addPassword":
		if m == http.MethodPost && (coll == "applications" || coll == "servicePrincipals") {
			return s.addPassword(k, oid, obj)
		}
	case "removePassword":
		if m == http.MethodPost && (coll == "applications" || coll == "servicePrincipals") {
			return s.removePassword(k, oid, obj)
		}
	case "owners", "members":
		if rest[0] == "owners" || coll == "groups" {
			return s.refs(k, coll, oid, obj, rest, m)
		}
	case "appRoleAssignedTo", "appRoleAssignments", "oauth2PermissionGrants", "memberOf", "transitiveMemberOf", "federatedIdentityCredentials":
		if m == http.MethodGet {
			return http.StatusOK, k.envelope(rest[0], nil), nil
		}
	}
	return 0, nil, apiError(http.StatusNotFound, "Request_ResourceNotFound", "Resource not found for the segment '%s'.", rest[0])
}

// render is an object as Graph returns it: no internal fields, no password profile, $select applied.
func (s *Service) render(k *call, coll, id string, fields []string) map[string]any {
	out := map[string]any{"@odata.context": k.base + "/$metadata#" + coll + "/$entity"}
	for f, v := range s.st.Objects[coll][id] {
		if strings.HasPrefix(f, "_") || f == "passwordProfile" || fields != nil && f != "id" && !slices.Contains(fields, f) {
			continue
		}
		out[f] = v
	}
	return out
}

func (k *call) envelope(coll string, items []map[string]any) map[string]any {
	if items == nil {
		items = []map[string]any{}
	}
	return map[string]any{"@odata.context": k.base + "/$metadata#" + coll, "value": items}
}

func (s *Service) listColl(k *call, coll string) (int, any, error) {
	match, err := parseFilter(k.q.Get("$filter"))
	if err != nil {
		return 0, nil, apiError(http.StatusBadRequest, "BadRequest", "Invalid filter clause: %v", err)
	}
	var ids []string
	for id, o := range s.st.Objects[coll] {
		if match(o) {
			ids = append(ids, id)
		}
	}
	sortBy(ids, s.st.Objects[coll], k.q.Get("$orderby"))
	total := len(ids)
	skip := 0
	if tok := k.q.Get("$skiptoken"); tok != "" {
		var err error
		if skip, err = strconv.Atoi(tok); err != nil || skip < 0 {
			return 0, nil, badRequest("Invalid $skiptoken: %q.", tok)
		}
	}
	ids = ids[min(skip, len(ids)):]
	next := ""
	if top, _ := strconv.Atoi(k.q.Get("$top")); top > 0 && len(ids) > top {
		ids = ids[:top]
		q := cloneQuery(k.q)
		q.Set("$skiptoken", strconv.Itoa(skip+top))
		next = k.origin + k.r.URL.Path + "?" + q.Encode()
	}
	fields := selectFields(k.q)
	items := make([]map[string]any, len(ids))
	for i, id := range ids {
		items[i] = s.render(k, coll, id, fields)
		delete(items[i], "@odata.context")
	}
	out := k.envelope(coll, items)
	if k.q.Get("$count") == "true" {
		out["@odata.count"] = total
	}
	if next != "" {
		out["@odata.nextLink"] = next
	}
	return http.StatusOK, out, nil
}

func cloneQuery(q url.Values) url.Values {
	out := url.Values{}
	for k, v := range q {
		out[k] = slices.Clone(v)
	}
	return out
}

func selectFields(q url.Values) []string {
	if sel := q.Get("$select"); sel != "" {
		return strings.Split(sel, ",")
	}
	return nil
}

// ---- writes ----

func (s *Service) body(k *call) (map[string]any, error) {
	in := map[string]any{}
	if b := core.Body(k.r); len(b) > 0 {
		if err := json.Unmarshal(b, &in); err != nil {
			return nil, badRequest("Unable to read JSON request payload. Please ensure Content-Type header is set and payload is of valid JSON format.")
		}
	}
	delete(in, "@odata.type")
	return in, nil
}

func required(in map[string]any, kind string, props ...string) error {
	for _, p := range props {
		if v, ok := in[p]; !ok || v == nil || v == "" {
			return badRequest("A value is required for property '%s' of resource '%s'.", p, kind)
		}
	}
	return nil
}

func now() string { return time.Now().UTC().Format("2006-01-02T15:04:05Z") }

func (s *Service) create(k *call, coll string) (int, any, error) {
	in, err := s.body(k)
	if err != nil {
		return 0, nil, err
	}
	id := core.UUID()
	doc := map[string]any{"id": id, "createdDateTime": now(), "deletedDateTime": nil}
	switch coll {
	case "applications":
		if err := required(in, "Application", "displayName"); err != nil {
			return 0, nil, err
		}
		defaults(doc, map[string]any{"appId": core.UUID(), "signInAudience": "AzureADMyOrg", "identifierUris": []any{}, "passwordCredentials": []any{},
			"keyCredentials": []any{}, "requiredResourceAccess": []any{}, "tags": []any{}, "web": map[string]any{"redirectUris": []any{}},
			"publisherDomain": "pogi.onmicrosoft.com", "appRoles": []any{}, "api": map[string]any{"oauth2PermissionScopes": []any{}}, "_owners": []any{}})
	case "servicePrincipals":
		if err := required(in, "ServicePrincipal", "appId"); err != nil {
			return 0, nil, err
		}
		if oid, _ := s.resolve("servicePrincipals", "appId", str(in["appId"]), ""); oid != "" {
			return 0, nil, badRequest("A service principal with the appId '%s' already exists.", in["appId"])
		}
		defaults(doc, map[string]any{"accountEnabled": true, "servicePrincipalType": "Application", "tags": []any{}, "passwordCredentials": []any{},
			"keyCredentials": []any{}, "replyUrls": []any{}, "appRoles": []any{}, "oauth2PermissionScopes": []any{},
			"appOwnerOrganizationId": arm.TenantID, "signInAudience": "AzureADMyOrg", "_owners": []any{}})
		if _, app := s.resolve("applications", "appId", str(in["appId"]), ""); app != nil { // an app registration, as in the portal
			doc["displayName"], doc["appDisplayName"] = app["displayName"], app["displayName"]
			doc["servicePrincipalNames"] = append([]any{app["appId"]}, anySlice(app["identifierUris"])...)
		} else {
			doc["servicePrincipalNames"] = []any{in["appId"]}
		}
	case "users":
		if err := required(in, "User", "userPrincipalName", "displayName"); err != nil {
			return 0, nil, err
		}
		if _, u := s.findUser(str(in["userPrincipalName"])); u != nil {
			return 0, nil, badRequest("Another object with the same value for property userPrincipalName already exists.")
		}
		defaults(doc, map[string]any{"accountEnabled": true, "userType": "Member", "mail": nil, "givenName": nil, "surname": nil,
			"jobTitle": nil, "otherMails": []any{}})
		if pp, ok := in["passwordProfile"].(map[string]any); ok {
			doc["_password"] = str(pp["password"])
		}
	case "groups":
		if err := required(in, "Group", "displayName"); err != nil {
			return 0, nil, err
		}
		defaults(doc, map[string]any{"securityEnabled": true, "mailEnabled": false, "groupTypes": []any{}, "description": nil, "_owners": []any{}, "_members": []any{}})
	}
	for f, v := range in {
		if f != "id" && !strings.HasPrefix(f, "_") {
			doc[f] = v
		}
	}
	if coll == "applications" && in["appId"] == nil {
		doc["appId"] = core.UUID()
	}
	s.st.Objects[coll][id] = doc
	return http.StatusCreated, s.render(k, coll, id, nil), nil
}

func defaults(doc, d map[string]any) {
	for k, v := range d {
		doc[k] = v
	}
}

func anySlice(v any) []any { l, _ := v.([]any); return l }

func (s *Service) update(k *call, coll, id string, obj map[string]any) (int, any, error) {
	in, err := s.body(k)
	if err != nil {
		return 0, nil, err
	}
	if upn := str(in["userPrincipalName"]); coll == "users" && upn != "" {
		if oid, u := s.findUser(upn); u != nil && oid != id {
			return 0, nil, badRequest("Another object with the same value for property userPrincipalName already exists.")
		}
	}
	for f, v := range in {
		switch {
		case f == "id" || f == "appId" && coll != "servicePrincipals" || f == "createdDateTime" || strings.HasPrefix(f, "_"):
		case f == "passwordProfile" && coll == "users":
			if pp, ok := v.(map[string]any); ok && pp["password"] != nil {
				obj["_password"] = str(pp["password"])
			}
		case f == "passwordCredentials" || f == "keyCredentials": // changed with addPassword and addKey, not PATCH
		default:
			obj[f] = v
		}
	}
	return http.StatusNoContent, nil, nil
}

func (s *Service) remove(coll, id string) {
	delete(s.st.Objects[coll], id)
	for key := range s.st.Secrets {
		if strings.HasPrefix(key, id+"/") {
			delete(s.st.Secrets, key)
		}
	}
	for _, c := range collections { // drop references
		for _, o := range s.st.Objects[c] {
			for _, f := range []string{"_owners", "_members"} {
				if l, ok := o[f].([]any); ok {
					o[f] = slices.DeleteFunc(slices.Clone(l), func(v any) bool { return v == id })
				}
			}
		}
	}
}

// ---- secrets ----

func (s *Service) addPassword(k *call, id string, obj map[string]any) (int, any, error) {
	in, err := s.body(k)
	if err != nil {
		return 0, nil, err
	}
	pc, _ := in["passwordCredential"].(map[string]any)
	cred := map[string]any{"customKeyIdentifier": nil, "displayName": nil, "keyId": core.UUID(), "startDateTime": now(),
		"endDateTime": time.Now().UTC().AddDate(0, 6, 0).Format("2006-01-02T15:04:05Z")}
	for _, f := range []string{"displayName", "startDateTime", "endDateTime"} {
		if v, ok := pc[f]; ok {
			cred[f] = v
		}
	}
	secret := core.RandString(3, core.AlphaNum) + "8Q~" + core.RandString(31, core.AlphaNum+"-_.~")
	cred["hint"] = secret[:3]
	s.st.Secrets[id+"/"+str(cred["keyId"])] = secret
	shown := map[string]any{}
	for f, v := range cred {
		shown[f] = v
	}
	cred["secretText"] = nil // stored without it
	obj["passwordCredentials"] = append(anySlice(obj["passwordCredentials"]), cred)
	shown["secretText"] = secret
	return http.StatusOK, shown, nil
}

func (s *Service) removePassword(k *call, id string, obj map[string]any) (int, any, error) {
	in, err := s.body(k)
	if err != nil {
		return 0, nil, err
	}
	key := str(in["keyId"])
	creds := anySlice(obj["passwordCredentials"])
	i := slices.IndexFunc(creds, func(c any) bool { m, _ := c.(map[string]any); return strings.EqualFold(str(m["keyId"]), key) })
	if i < 0 {
		return 0, nil, badRequest("No password credential found with keyId '%s'.", key)
	}
	obj["passwordCredentials"] = slices.Delete(slices.Clone(creds), i, i+1)
	delete(s.st.Secrets, id+"/"+key)
	return http.StatusNoContent, nil, nil
}

// ---- owners and members ----

func (s *Service) refs(k *call, coll, id string, obj map[string]any, rest []string, m string) (int, any, error) {
	field := "_" + rest[0]
	refs := anySlice(obj[field])
	switch {
	case m == http.MethodGet && (len(rest) == 1 || rest[1] == "$ref" && len(rest) == 2):
		var items []map[string]any
		for _, r := range refs {
			c, oid := s.find(str(r))
			if c == "" {
				continue
			}
			if len(rest) == 2 {
				items = append(items, map[string]any{"@odata.id": k.base + "/directoryObjects/" + oid})
				continue
			}
			it := s.render(k, c, oid, nil)
			delete(it, "@odata.context")
			it["@odata.type"] = odataType[c]
			items = append(items, it)
		}
		return http.StatusOK, k.envelope("directoryObjects", items), nil
	case m == http.MethodPost && len(rest) == 2 && rest[1] == "$ref":
		in, err := s.body(k)
		if err != nil {
			return 0, nil, err
		}
		ref := str(in["@odata.id"])
		target := ref[strings.LastIndexByte(ref, '/')+1:]
		if c, _ := s.find(target); c == "" {
			return 0, nil, notFound(target)
		}
		if slices.Contains(refs, any(target)) {
			return 0, nil, badRequest("One or more added object references already exist for the following modified properties: '%s'.", rest[0])
		}
		obj[field] = append(refs, target)
		return http.StatusNoContent, nil, nil
	case m == http.MethodDelete && len(rest) == 3 && rest[2] == "$ref":
		if !slices.Contains(refs, any(rest[1])) {
			return 0, nil, notFound(rest[1])
		}
		obj[field] = slices.DeleteFunc(slices.Clone(refs), func(v any) bool { return v == rest[1] })
		return http.StatusNoContent, nil, nil
	}
	return 0, nil, apiError(http.StatusNotFound, "Request_ResourceNotFound", "Resource not found for the segment '%s'.", rest[len(rest)-1])
}

// ---- persistence and console ----

func (s *Service) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.st = state{Objects: map[string]map[string]map[string]any{}, Secrets: map[string]string{}}
	for _, c := range collections {
		s.st.Objects[c] = map[string]map[string]any{}
	}
	s.seed()
}

// seed adds the first-party service principals that role and permission
// lookups name.
func (s *Service) seed() {
	role := func(id, value, desc string) map[string]any {
		return map[string]any{"id": id, "value": value, "displayName": value, "description": desc, "isEnabled": true, "allowedMemberTypes": []any{"Application"}}
	}
	scope := func(id, value, desc string) map[string]any {
		return map[string]any{"id": id, "value": value, "adminConsentDisplayName": desc, "isEnabled": true, "type": "User"}
	}
	first := map[string]map[string]any{
		"00000003-0000-0000-c000-000000000000": {"displayName": "Microsoft Graph",
			"appRoles": []any{role("df021288-bdef-4463-88db-98f22de89214", "User.Read.All", "Read all users' full profiles"),
				role("5b567255-7703-4780-807c-7be8301ae99b", "Group.Read.All", "Read all groups"),
				role("7ab1d382-f21e-4acd-a863-ba3e13f7da61", "Directory.Read.All", "Read directory data"),
				role("9a5d68dd-52b0-4cc2-bd40-abcf44ac3a30", "Application.Read.All", "Read all applications"),
				role("1bfefb4e-e0b5-418b-a88f-73c46d2cc8e9", "Application.ReadWrite.All", "Read and write all applications")},
			"oauth2PermissionScopes": []any{scope("e1fe6dd8-ba31-4d61-89e7-88639da4683d", "User.Read", "Sign in and read user profile")}},
		"797f4846-ba00-4fd7-ba43-dac1f8f63013": {"displayName": "Windows Azure Service Management API"},
	}
	for appID, f := range first {
		id := core.UUID()
		doc := map[string]any{"id": id, "appId": appID, "servicePrincipalType": "Application", "accountEnabled": true, "appRoles": []any{},
			"oauth2PermissionScopes": []any{}, "servicePrincipalNames": []any{appID}, "passwordCredentials": []any{}, "keyCredentials": []any{},
			"tags": []any{}, "createdDateTime": now(), "deletedDateTime": nil, "appOwnerOrganizationId": "f8cdef31-a31e-4b4a-93e4-5f571e91255a",
			"signInAudience": "AzureADMultipleOrgs", "_owners": []any{}}
		defaults(doc, f)
		doc["appDisplayName"] = doc["displayName"]
		s.st.Objects["servicePrincipals"][id] = doc
	}
}

func (s *Service) Save() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(s.st)
}

func (s *Service) Load(b []byte) error {
	var st state
	if err := json.Unmarshal(b, &st); err != nil {
		return err
	}
	if st.Objects == nil {
		st.Objects = map[string]map[string]map[string]any{}
	}
	for _, c := range collections {
		if st.Objects[c] == nil {
			st.Objects[c] = map[string]map[string]any{}
		}
	}
	if st.Secrets == nil {
		st.Secrets = map[string]string{}
	}
	s.mu.Lock()
	s.st = st
	s.mu.Unlock()
	return nil
}

func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	cols := map[string][]string{"applications": {"Display name", "App ID"}, "servicePrincipals": {"Display name", "App ID"},
		"users": {"Principal name", "Display name"}, "groups": {"Display name", "Description"}}
	var out []core.Table
	for _, c := range collections {
		t := core.Table{Name: c, Columns: append([]string{"Object ID"}, cols[c]...)}
		ids := core.SortedKeys(s.st.Objects[c])
		slices.SortStableFunc(ids, func(a, b string) int {
			return strings.Compare(str(s.st.Objects[c][a]["displayName"]), str(s.st.Objects[c][b]["displayName"]))
		})
		for _, id := range ids {
			o := s.st.Objects[c][id]
			switch c {
			case "applications", "servicePrincipals":
				t.Rows = append(t.Rows, []string{id, str(o["displayName"]), str(o["appId"])})
			case "users":
				t.Rows = append(t.Rows, []string{id, str(o["userPrincipalName"]), str(o["displayName"])})
			default:
				t.Rows = append(t.Rows, []string{id, str(o["displayName"]), str(o["description"])})
			}
		}
		out = append(out, t)
	}
	return out
}

func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	o := s.st.Objects[table][id]
	if o == nil {
		return nil
	}
	out := map[string]any{}
	for f, v := range o {
		if f != "_password" {
			out[f] = v
		}
	}
	return out
}
