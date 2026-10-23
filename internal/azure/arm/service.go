package arm

import (
	"encoding/json"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/nrndev/pogi/internal/core"
)

// Service is the Resource Manager: management.azure.com on pogi's main port.
type Service struct {
	h  *core.Hub
	mu sync.Mutex
	st state
}

type state struct {
	Subs    map[string]string         `json:"subs"`    // subscription id -> display name
	Groups  map[string]map[string]any `json:"groups"`  // lower-cased group ID -> resource group
	Generic map[string]map[string]any `json:"generic"` // lower-cased resource ID -> resource of a type without a Provider
}

// New creates the Resource Manager. Provider resource types register with
// Register, whatever the order of construction.
func New(h *core.Hub) *Service {
	s := &Service{h: h}
	s.Reset()
	Register(h, s.roleDefinitions())
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "arm", Cloud: core.Azure, Title: "Resource Manager", Protocol: "azure-json", Match: match}
}

// match claims the ARM paths that are anonymous or bearer-authenticated (not AWS- or HMAC-signed).
// A missing api-version is answered by check, in ARM's own error.
func match(r *http.Request) bool {
	first, _, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/"), "/")
	switch strings.ToLower(first) {
	case "subscriptions", "providers", "tenants":
	case "metadata":
		if r.URL.Path != "/metadata/endpoints" {
			return false
		}
	default:
		return false
	}
	scheme, _, _ := strings.Cut(r.Header.Get("Authorization"), " ")
	return scheme == "" || strings.EqualFold(scheme, "Bearer")
}

// call is one request.
type call struct {
	r    *http.Request
	c    *core.Ctx
	base string   // scheme://host, for URLs in responses
	seg  []string // path segments
	hdr  http.Header
	body map[string]any
}

func (k *call) path() string { return "/" + strings.Join(k.seg, "/") }

func (k *call) payload() map[string]any {
	if k.body == nil {
		k.body = map[string]any{}
		if b := core.Body(k.r); len(b) > 0 {
			json.Unmarshal(b, &k.body)
		}
	}
	return k.body
}

// members of a resource body that must have one JSON type when they are present and not null.
var (
	objectMembers = []string{"sku", "tags", "properties", "identity", "plan"}
	stringMembers = []string{"location", "kind", "managedBy"}
)

// check refuses a request ARM would refuse before any service sees it: no api-version, an empty path
// segment, or a PUT or PATCH body that is not a JSON object or has a member of the wrong type. Nothing is
// stored for a request that fails here.
func (k *call) check() error {
	if k.r.URL.Path != "/metadata/endpoints" && k.r.URL.Query().Get("api-version") == "" {
		return core.Errorf(http.StatusBadRequest, "MissingApiVersionParameter", "The api-version query parameter (?api-version=) is required for all requests.")
	}
	if slices.Contains(k.seg, "") {
		return core.Errorf(http.StatusBadRequest, "InvalidRequestUri", "The request path '%s' has an empty segment.", k.r.URL.Path)
	}
	if k.r.Method != http.MethodPut && k.r.Method != http.MethodPatch {
		return nil
	}
	b := core.Body(k.r)
	if len(b) == 0 {
		return nil
	}
	var v any
	if err := json.Unmarshal(b, &v); err != nil {
		// A body declared as something other than JSON is text (a runbook's script): the resource gets it as "content".
		if ct := k.r.Header.Get("Content-Type"); ct != "" && !strings.Contains(ct, "json") {
			k.body = map[string]any{"content": string(b)}
			return nil
		}
		return core.Errorf(http.StatusBadRequest, "InvalidRequestContent", "The request content was invalid and could not be deserialized: '%s'.", core.PlainJSON(err))
	}
	body, ok := v.(map[string]any)
	if !ok {
		return core.Errorf(http.StatusBadRequest, "InvalidRequestContent", "The request content must be a JSON object.")
	}
	for _, name := range objectMembers {
		if _, isObj := body[name].(map[string]any); body[name] != nil && !isObj {
			return core.Errorf(http.StatusBadRequest, "InvalidRequestContent", "The request content was invalid: '%s' must be a JSON object.", name)
		}
	}
	for _, name := range stringMembers {
		if _, isStr := body[name].(string); body[name] != nil && !isStr {
			return core.Errorf(http.StatusBadRequest, "InvalidRequestContent", "The request content was invalid: '%s' must be a string.", name)
		}
	}
	k.body = body
	return nil
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	k := &call{r: r, c: core.C(r), base: scheme + "://" + r.Host, hdr: http.Header{},
		seg: strings.Split(strings.Trim(r.URL.Path, "/"), "/")}
	k.c.Op = r.Method + " " + opType(k.seg)
	var status int
	var out any
	err := k.c.Intercept()
	if err == nil {
		err = k.check()
	}
	if err == nil {
		status, out, err = s.route(k)
	}
	h := w.Header()
	h.Set("x-ms-request-id", k.c.RequestID)
	h.Set("x-ms-correlation-request-id", k.c.RequestID)
	for name, v := range k.hdr {
		h[name] = v
	}
	if err != nil {
		e := core.AsError(err)
		k.c.ErrCode = e.Code
		status, out = e.Status, map[string]any{"error": map[string]string{"code": e.Code, "message": e.Msg}}
	}
	if out == nil || r.Method == http.MethodHead {
		w.WriteHeader(status)
		return
	}
	if t, ok := out.(Text); ok {
		h.Set("Content-Type", "application/octet-stream")
		w.WriteHeader(status)
		io.WriteString(w, string(t))
		return
	}
	h.Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(out)
}

// Text is a response body that is sent as it is, as text (a Provider's Doc returns it).
type Text string

// opType names what a request is about, for the request log and chaos rules:
// the resource type of a provider path, else the first segments.
func opType(seg []string) string {
	if i := slices.IndexFunc(seg, func(s string) bool { return strings.EqualFold(s, "providers") }); i >= 0 && len(seg) > i+2 {
		typ := seg[i+1]
		for j := i + 2; j < len(seg); j += 2 {
			typ += "/" + seg[j]
		}
		return typ
	}
	if i := slices.IndexFunc(seg, func(s string) bool { return strings.EqualFold(s, "resourceGroups") }); i >= 0 {
		return "resourceGroups"
	}
	return seg[0]
}

func (s *Service) route(k *call) (int, any, error) {
	seg := k.seg
	switch strings.ToLower(seg[0]) {
	case "metadata":
		return http.StatusOK, metadata(k), nil
	case "tenants":
		return http.StatusOK, list(tenant()), nil
	case "providers":
		switch len(seg) {
		case 1:
			return http.StatusOK, list(s.providers("")...), nil
		case 2:
			return http.StatusOK, s.provider("", seg[1]), nil
		}
		return s.resource(k, k.path())
	case "subscriptions":
		if len(seg) == 1 {
			return http.StatusOK, list(s.subscriptions()...), nil
		}
		return s.subscription(k)
	}
	return 0, nil, core.Errorf(http.StatusNotFound, "InvalidRequestUri", "No route registered for '%s'.", k.path())
}

func (s *Service) subscription(k *call) (int, any, error) {
	sub, rest := k.seg[1], k.seg[2:]
	s.touch(sub)
	if len(rest) == 0 {
		return http.StatusOK, subscriptionObj(sub, s.subName(sub)), nil
	}
	switch strings.ToLower(rest[0]) {
	case "locations":
		return http.StatusOK, list(locations(sub)...), nil
	case "providers":
		switch {
		case len(rest) == 1:
			return http.StatusOK, list(s.providers(sub)...), nil
		case len(rest) == 2:
			return http.StatusOK, s.provider(sub, rest[1]), nil
		case len(rest) == 3 && (strings.EqualFold(rest[2], "register") || strings.EqualFold(rest[2], "unregister")) && k.r.Method == http.MethodPost:
			return http.StatusOK, s.provider(sub, rest[1]), nil
		}
		return s.resource(k, k.path())
	case "resourcegroups":
		return s.groups(k, sub, rest[1:])
	case "resources":
		return http.StatusOK, list(filter(s.allResources(k, ID{Subscription: sub}), k.r.URL.Query().Get("$filter"))...), nil
	case "operationresults", "operationstatuses":
		if len(rest) == 2 {
			return operation(rest[1], rest[0])
		}
	}
	return 0, nil, core.Errorf(http.StatusNotFound, "InvalidRequestUri", "No route registered for '%s'.", k.path())
}

// operation is the status of a long-running operation: they all have
// finished by the time anybody asks.
func operation(id, kind string) (int, any, error) {
	if strings.EqualFold(kind, "operationresults") {
		return http.StatusOK, nil, nil
	}
	now := time.Now().UTC().Format(time.RFC3339)
	return http.StatusOK, map[string]any{"name": id, "status": "Succeeded", "startTime": now, "endTime": now}, nil
}

// async adds the headers of a long-running operation that has already succeeded.
func (k *call) async(sub string) {
	id := core.RandHex(8)
	u := k.base + "/subscriptions/" + sub
	k.hdr.Set("Azure-AsyncOperation", u+"/operationStatuses/"+id+"?api-version=2022-09-01")
	k.hdr.Set("Location", u+"/operationresults/"+id+"?api-version=2022-09-01")
	k.hdr.Set("Retry-After", "1")
}

func list(v ...map[string]any) map[string]any {
	if v == nil {
		v = []map[string]any{}
	}
	return map[string]any{"value": v}
}

// ---- subscriptions and tenants ----

func tenant() map[string]any {
	return map[string]any{"id": "/tenants/" + TenantID, "tenantId": TenantID, "countryCode": "US", "displayName": "pogi",
		"domains": []string{"pogi.onmicrosoft.com"}, "tenantCategory": "Home", "defaultDomain": "pogi.onmicrosoft.com", "tenantType": "AAD"}
}

func subscriptionObj(id, name string) map[string]any {
	return map[string]any{"id": "/subscriptions/" + id, "authorizationSource": "RoleBased", "managedByTenants": []any{},
		"subscriptionId": id, "tenantId": TenantID, "displayName": name, "state": "Enabled",
		"subscriptionPolicies": map[string]any{"locationPlacementId": "Public_2014-09-01", "quotaId": "PayAsYouGo_2014-09-01", "spendingLimit": "Off"}}
}

func (s *Service) subscriptions() []map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []map[string]any
	for _, id := range core.SortedKeys(s.st.Subs) {
		out = append(out, subscriptionObj(id, s.st.Subs[id]))
	}
	return out
}

// touch creates a subscription the first time anybody uses it.
func (s *Service) touch(sub string) {
	s.mu.Lock()
	if _, ok := s.st.Subs[sub]; !ok {
		s.st.Subs[sub] = "pogi " + sub[:min(8, len(sub))]
	}
	s.mu.Unlock()
}

func (s *Service) subName(sub string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.st.Subs[sub]
}

var regions = [][2]string{{"eastus", "East US"}, {"eastus2", "East US 2"}, {"centralus", "Central US"}, {"westus", "West US"},
	{"westus2", "West US 2"}, {"westus3", "West US 3"}, {"northeurope", "North Europe"}, {"westeurope", "West Europe"},
	{"uksouth", "UK South"}, {"francecentral", "France Central"}, {"germanywestcentral", "Germany West Central"},
	{"southeastasia", "Southeast Asia"}, {"japaneast", "Japan East"}, {"australiaeast", "Australia East"},
	{"canadacentral", "Canada Central"}, {"brazilsouth", "Brazil South"}, {"centralindia", "Central India"}}

func locations(sub string) []map[string]any {
	out := make([]map[string]any, len(regions))
	for i, r := range regions {
		out[i] = map[string]any{"id": "/subscriptions/" + sub + "/locations/" + r[0], "name": r[0], "displayName": r[1],
			"regionalDisplayName": "(US) " + r[1], "metadata": map[string]any{"regionType": "Physical", "regionCategory": "Recommended"}}
	}
	return out
}

// ---- resource groups ----

func groupKey(sub, name string) string {
	return strings.ToLower("/subscriptions/" + sub + "/resourceGroups/" + name)
}

// validGroupName is Azure's rule: 1 to 90 letters, digits and _ - . ( ), not ending in a period.
func validGroupName(name string) bool {
	if n := utf8.RuneCountInString(name); n < 1 || n > 90 || strings.HasSuffix(name, ".") {
		return false
	}
	for _, r := range name {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) && !strings.ContainsRune("_-.()", r) {
			return false
		}
	}
	return true
}

func (s *Service) groups(k *call, sub string, rest []string) (int, any, error) {
	m := k.r.Method
	if len(rest) == 0 {
		return http.StatusOK, list(s.listGroups(sub)...), nil
	}
	name := rest[0]
	if len(rest) > 1 {
		switch strings.ToLower(rest[1]) {
		case "resources":
			return http.StatusOK, list(filter(s.allResources(k, ID{Subscription: sub, ResourceGroup: name}), k.r.URL.Query().Get("$filter"))...), nil
		case "providers":
			return s.resource(k, k.path())
		}
		return 0, nil, core.Errorf(http.StatusNotFound, "InvalidRequestUri", "No route registered for '%s'.", k.path())
	}
	key := groupKey(sub, name)
	s.mu.Lock()
	g, ok := s.st.Groups[key]
	s.mu.Unlock()
	switch m {
	case http.MethodPut, http.MethodPatch:
		in := k.payload()
		loc, _ := in["location"].(string)
		if !ok && !validGroupName(name) {
			return 0, nil, core.Errorf(http.StatusBadRequest, "InvalidResourceGroupName",
				"Resource group name '%s' is invalid: use 1 to 90 letters, digits and _ - . ( ), not ending in a period.", name)
		}
		if !ok && m == http.MethodPut && loc == "" {
			return 0, nil, core.Errorf(http.StatusBadRequest, "LocationRequired", "The location property is required for this definition.")
		}
		if !ok && m == http.MethodPatch {
			return 0, nil, groupNotFound(name)
		}
		if ok && loc != "" && !strings.EqualFold(loc, g["location"].(string)) {
			return 0, nil, core.Errorf(http.StatusConflict, "InvalidResourceGroupLocation",
				"Invalid resource group location '%s'. The Resource group already exists in the location '%s' and any attempt to update location is not permitted.", loc, g["location"])
		}
		doc := map[string]any{"id": "/subscriptions/" + sub + "/resourceGroups/" + name, "name": name, "type": "Microsoft.Resources/resourceGroups",
			"location": loc, "tags": in["tags"], "properties": map[string]any{"provisioningState": "Succeeded"}}
		if ok { // keep what the request leaves out
			doc["location"] = g["location"]
			if _, set := in["tags"]; !set {
				doc["tags"] = g["tags"]
			}
		}
		if v, set := in["managedBy"]; set {
			doc["managedBy"] = v
		} else if ok && g["managedBy"] != nil {
			doc["managedBy"] = g["managedBy"]
		}
		if doc["tags"] == nil {
			doc["tags"] = map[string]any{}
		}
		s.mu.Lock()
		s.st.Groups[key] = doc
		s.mu.Unlock()
		if ok {
			return http.StatusOK, doc, nil
		}
		return http.StatusCreated, doc, nil
	case http.MethodGet, http.MethodHead:
		if !ok {
			return 0, nil, groupNotFound(name)
		}
		if m == http.MethodHead {
			return http.StatusNoContent, nil, nil
		}
		return http.StatusOK, g, nil
	case http.MethodDelete:
		if !ok {
			return 0, nil, groupNotFound(name)
		}
		s.deleteGroup(k.c, sub, name)
		k.hdr.Set("Location", k.base+"/subscriptions/"+sub+"/operationresults/"+core.RandHex(8)+"?api-version=2022-09-01")
		k.hdr.Set("Retry-After", "1")
		return http.StatusAccepted, nil, nil
	}
	return 0, nil, core.Errorf(http.StatusMethodNotAllowed, "MethodNotAllowed", "The method %s is not allowed for resource groups.", m)
}

func groupNotFound(name string) error {
	return core.Errorf(http.StatusNotFound, "ResourceGroupNotFound", "Resource group '%s' could not be found.", name)
}

func (s *Service) listGroups(sub string) []map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []map[string]any
	for _, k := range core.SortedKeys(s.st.Groups) {
		if strings.HasPrefix(k, strings.ToLower("/subscriptions/"+sub+"/")) {
			out = append(out, s.st.Groups[k])
		}
	}
	return out
}

func (s *Service) hasGroup(sub, name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.st.Groups[groupKey(sub, name)]
	return ok
}

// deleteGroup deletes the group with everything in it: the resources of
// every Provider's top-level types (children go with their parents) and the
// generic ones.
func (s *Service) deleteGroup(c *core.Ctx, sub, name string) {
	g := ID{Subscription: sub, ResourceGroup: name}
	for _, typ := range s.topTypes() {
		p := Lookup(s.h, typ)
		if p == nil || p.List == nil || p.Delete == nil {
			continue
		}
		items, _ := p.List(c, g)
		for _, it := range items {
			if id, ok := ParseID(str(it["id"])); ok && strings.EqualFold(id.ResourceGroup, name) {
				p.Delete(c, id)
			}
		}
	}
	prefix := strings.ToLower(g.String()) + "/"
	s.mu.Lock()
	for k := range s.st.Generic {
		if strings.HasPrefix(k, prefix) {
			delete(s.st.Generic, k)
		}
	}
	delete(s.st.Groups, groupKey(sub, name))
	s.mu.Unlock()
}

// topTypes are the registered types that live directly in a group.
func (s *Service) topTypes() []string {
	return slices.DeleteFunc(Types(s.h), func(t string) bool { return strings.Count(t, "/") != 1 || !isResource(t) })
}

// allResources lists everything in a subscription or group: the resources of
// every Provider and the generic ones.
func (s *Service) allResources(k *call, scope ID) []map[string]any {
	var out []map[string]any
	for _, typ := range s.topTypes() {
		if p := Lookup(s.h, typ); p != nil && p.List != nil {
			items, _ := p.List(k.c, scope)
			out = append(out, items...)
		}
	}
	prefix := strings.ToLower(scope.String()) + "/"
	s.mu.Lock()
	for _, key := range core.SortedKeys(s.st.Generic) {
		if strings.HasPrefix(key, prefix) && isResource(str(s.st.Generic[key]["type"])) {
			out = append(out, s.st.Generic[key])
		}
	}
	s.mu.Unlock()
	return out
}

// isResource is false for what a resources listing leaves out: role
// definitions and assignments, and deployments.
func isResource(typ string) bool {
	return !strings.HasPrefix(strings.ToLower(typ), "microsoft.authorization/") && !strings.EqualFold(typ, "Microsoft.Resources/deployments")
}

func str(v any) string { s, _ := v.(string); return s }

// ---- persistence and console ----

func (s *Service) Reset() {
	s.mu.Lock()
	s.st = state{Subs: map[string]string{core.AzureSubscription: "pogi"}, Groups: map[string]map[string]any{}, Generic: map[string]map[string]any{}}
	g := core.AzureResourceGroup // the group every other service's resources live in
	s.st.Groups[groupKey(core.AzureSubscription, g)] = map[string]any{"id": "/subscriptions/" + core.AzureSubscription + "/resourceGroups/" + g,
		"name": g, "type": "Microsoft.Resources/resourceGroups", "location": "eastus", "tags": map[string]any{}, "properties": map[string]any{"provisioningState": "Succeeded"}}
	s.mu.Unlock()
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
	if st.Subs == nil {
		st.Subs = map[string]string{core.AzureSubscription: "pogi"}
	}
	if st.Groups == nil {
		st.Groups = map[string]map[string]any{}
	}
	if st.Generic == nil {
		st.Generic = map[string]map[string]any{}
	}
	s.mu.Lock()
	s.st = st
	s.mu.Unlock()
	return nil
}

func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	subs := core.Table{Name: "Subscriptions", Columns: []string{"ID", "Name"}}
	for _, id := range core.SortedKeys(s.st.Subs) {
		subs.Rows = append(subs.Rows, []string{id, s.st.Subs[id]})
	}
	groups := core.Table{Name: "Resource groups", Columns: []string{"Name", "Location", "Subscription"}}
	for _, k := range core.SortedKeys(s.st.Groups) {
		g := s.st.Groups[k]
		id, _ := ParseID(str(g["id"]))
		groups.Rows = append(groups.Rows, []string{str(g["name"]), str(g["location"]), id.Subscription})
	}
	res := core.Table{Name: "Resources", Columns: []string{"ID", "Type", "Name"}}
	for _, k := range core.SortedKeys(s.st.Generic) {
		g := s.st.Generic[k]
		res.Rows = append(res.Rows, []string{str(g["id"]), str(g["type"]), str(g["name"])})
	}
	return []core.Table{subs, groups, res}
}

func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch table {
	case "Subscriptions":
		return subscriptionObj(id, s.st.Subs[id])
	case "Resource groups":
		for _, g := range s.st.Groups {
			if g["name"] == id {
				return g
			}
		}
	case "Resources":
		return s.st.Generic[strings.ToLower(id)]
	}
	return nil
}
