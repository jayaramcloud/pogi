// Package network emulates the Azure Virtual Network management plane:
// virtual networks and subnets, network security groups and rules, public IP
// addresses, network interfaces, route tables and routes, NAT gateways, and
// Private DNS zones with their record sets and virtual network links.
//
// Everything is an ARM Provider registered with arm.Register; there is no data
// plane. What pogi does check, because clients and IaC tools branch on it:
// CIDR syntax, a subnet inside its network's address space and clear of its
// siblings, NIC addresses allocated from (or checked against) the subnet,
// references to other resources, security rule priorities, and that a
// resource still in use cannot be deleted.
//
// ponytail: metadata only. No traffic, peerings, service endpoints, DDoS or
// firewall; subnets do not list the NIC configurations that use them;
// provisioning is instantaneous.
package network

import (
	"encoding/json"
	"maps"
	"net/http"
	"net/netip"
	"slices"
	"strings"
	"sync"

	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/core"
)

const ns = "Microsoft.Network/"

const (
	locRequired = iota // a PUT must say where
	locGlobal          // global, the default
	locNone            // child resources have no location
)

// kind describes one resource type.
type kind struct {
	typ    string // below Microsoft.Network/
	loc    int
	inline string                                                     // property that lists the children in the parent ("subnets"); the children are stored apart
	child  string                                                     // type of those children
	fill   func(s *Service, id arm.ID, old, res map[string]any) error // validates and completes res; old is the stored resource, or nil
	view   func(s *Service, res map[string]any)                       // adds what is computed on read
}

// recordTypes are the Private DNS record set types.
var recordTypes = []string{"A", "AAAA", "CNAME", "MX", "PTR", "SOA", "SRV", "TXT"}

func kinds() []*kind {
	ks := []*kind{
		{typ: "virtualNetworks", inline: "subnets", child: "virtualNetworks/subnets", fill: fillVNet},
		{typ: "virtualNetworks/subnets", loc: locNone, fill: fillSubnet},
		{typ: "networkSecurityGroups", inline: "securityRules", child: "networkSecurityGroups/securityRules", view: viewNSG},
		{typ: "networkSecurityGroups/securityRules", loc: locNone, fill: fillRule},
		{typ: "publicIPAddresses", fill: fillPublicIP},
		{typ: "networkInterfaces", fill: fillNIC},
		{typ: "routeTables", inline: "routes", child: "routeTables/routes", fill: fillRouteTable},
		{typ: "routeTables/routes", loc: locNone, fill: fillRoute},
		{typ: "natGateways", fill: fillNAT},
		{typ: "privateDnsZones", loc: locGlobal, fill: fillZone, view: viewZone},
		{typ: "privateDnsZones/virtualNetworkLinks", loc: locGlobal, fill: fillLink},
	}
	for _, r := range recordTypes {
		ks = append(ks, &kind{typ: "privateDnsZones/" + r, loc: locNone, fill: fillRecord})
	}
	return ks
}

type state struct {
	Res    map[string]map[string]any `json:"resources"` // by lower-cased ID; children are separate entries
	NextIP int                       `json:"nextIP"`    // last fake public address handed out
}

// Service is the Azure Virtual Network emulator.
type Service struct {
	mu    sync.Mutex
	st    state
	kinds map[string]*kind // by lower-cased full type
}

func New(h *core.Hub) *Service {
	s := &Service{kinds: map[string]*kind{}}
	s.Reset()
	for _, k := range kinds() {
		s.kinds[strings.ToLower(ns+k.typ)] = k
		arm.Register(h, s.provider(k))
	}
	// GET .../privateDnsZones/<zone>/ALL lists every record set of the zone.
	arm.Register(h, arm.Provider{Type: ns + "privateDnsZones/ALL", List: func(_ *core.Ctx, parent arm.ID) ([]map[string]any, error) {
		s.mu.Lock()
		defer s.mu.Unlock()
		var out []map[string]any
		for _, r := range recordTypes {
			out = append(out, s.listLocked(ns+"privateDnsZones/"+r, parent)...)
		}
		return out, nil
	}})
	return s
}

func (s *Service) Info() core.Info {
	// The ARM service routes to the providers; this one has no HTTP routes.
	return core.Info{Name: "aznetwork", Cloud: core.Azure, Title: "Azure Virtual Network", Protocol: "azure-json",
		Match: func(*http.Request) bool { return false }}
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) { http.NotFound(w, r) }

// ---- providers ----

func lower(id arm.ID) string { return strings.ToLower(id.String()) }

func (s *Service) provider(k *kind) arm.Provider {
	return arm.Provider{Type: ns + k.typ, Async: !slices.Contains(recordTypes, lastSeg(k.typ)), // record sets are plain calls in the SDK, the rest long-running
		Put: func(_ *core.Ctx, id arm.ID, body map[string]any) (map[string]any, error) {
			s.mu.Lock()
			defer s.mu.Unlock()
			snap, next := maps.Clone(s.st.Res), s.st.NextIP // documents are replaced, never edited: a shallow copy rolls back
			if err := s.put(k, id, body); err != nil {
				s.st.Res, s.st.NextIP = snap, next
				return nil, err
			}
			return s.view(k, lower(id)), nil
		},
		Get: func(_ *core.Ctx, id arm.ID) (map[string]any, error) {
			s.mu.Lock()
			defer s.mu.Unlock()
			if s.st.Res[lower(id)] == nil {
				return nil, arm.NotFound(id)
			}
			return s.view(k, lower(id)), nil
		},
		Delete: func(_ *core.Ctx, id arm.ID) error {
			s.mu.Lock()
			defer s.mu.Unlock()
			key := lower(id)
			if err := s.inUse(k, id, key); err != nil {
				return err
			}
			for rk := range s.st.Res {
				if rk == key || strings.HasPrefix(rk, key+"/") {
					delete(s.st.Res, rk)
				}
			}
			return nil
		},
		List: func(_ *core.Ctx, parent arm.ID) ([]map[string]any, error) {
			s.mu.Lock()
			defer s.mu.Unlock()
			return s.listLocked(ns+k.typ, parent), nil
		},
	}
}

func (s *Service) listLocked(typ string, parent arm.ID) (out []map[string]any) {
	pre := strings.ToLower(parent.String()) + "/"
	for _, key := range core.SortedKeys(s.st.Res) {
		if strings.HasPrefix(key, pre) && strings.EqualFold(str(s.st.Res[key]["type"]), typ) {
			out = append(out, s.view(s.kinds[strings.ToLower(typ)], key))
		}
	}
	return out
}

// view is a stored resource as ARM returns it, with its children inline.
func (s *Service) view(k *kind, key string) map[string]any {
	res := clone(s.st.Res[key])
	props := sub(res, "properties")
	if k.inline != "" {
		kids := []any{}
		for _, ck := range s.childKeys(key, ns+k.child) {
			kids = append(kids, clone(s.st.Res[ck]))
		}
		props[k.inline] = kids
	}
	if k.view != nil {
		k.view(s, res)
	}
	return res
}

// childKeys are the keys of the resources of type typ directly under key.
func (s *Service) childKeys(key, typ string) (out []string) {
	for _, ck := range core.SortedKeys(s.st.Res) {
		if rest, ok := strings.CutPrefix(ck, key+"/"); ok && strings.Count(rest, "/") == 1 && strings.EqualFold(str(s.st.Res[ck]["type"]), typ) {
			out = append(out, ck)
		}
	}
	return out
}

// put stores a resource, and the children listed inline in it. The lock is held.
func (s *Service) put(k *kind, id arm.ID, body map[string]any) error {
	key := lower(id)
	old := s.st.Res[key]
	res := clone(body)
	if res == nil {
		res = map[string]any{}
	}
	props := sub(res, "properties")
	var kids []any
	hasKids := false
	if k.inline != "" {
		kids, hasKids = props[k.inline].([]any)
	}
	delete(props, k.inline)
	delete(props, "defaultSecurityRules")
	res["id"], res["name"], res["type"] = id.String(), id.Name(), id.Type()
	res["etag"] = `W/"` + guid() + `"`
	props["provisioningState"] = "Succeeded"
	switch k.loc {
	case locNone:
		delete(res, "location")
		delete(res, "tags")
	case locGlobal:
		res["location"] = cmpOr(str(res["location"]), "global")
		def(res, "tags", map[string]any{})
	default:
		if str(res["location"]) == "" {
			return core.Errorf(http.StatusBadRequest, "LocationRequired", "The location property is required for this definition.")
		}
		def(res, "tags", map[string]any{})
	}
	if err := s.checkRefs(props); err != nil {
		return err
	}
	if k.fill != nil {
		if err := k.fill(s, id, old, res); err != nil {
			return err
		}
	}
	s.st.Res[key] = res
	if old == nil && k.typ == "privateDnsZones" { // a zone starts with its SOA record
		return s.soa(id)
	}
	if k.inline == "" {
		return nil
	}
	ck := s.kinds[strings.ToLower(ns+k.child)]
	if hasKids { // the list replaces the children
		for _, c := range s.childKeys(key, ns+k.child) {
			delete(s.st.Res, c)
		}
	}
	for _, c := range kids {
		m, _ := c.(map[string]any)
		name := str(m["name"])
		if name == "" {
			return bad("InvalidRequestFormat", "A %s of %s has no name.", k.inline, id.Name())
		}
		if err := s.put(ck, id.Child(lastSeg(k.child), name), m); err != nil {
			return err
		}
	}
	return s.checkSubnets(k, id, key)
}

func lastSeg(typ string) string { return typ[strings.LastIndexByte(typ, '/')+1:] }

// checkSubnets makes sure a virtual network's subnets still fit its address space.
func (s *Service) checkSubnets(k *kind, id arm.ID, key string) error {
	if k.typ != "virtualNetworks" {
		return nil
	}
	space := prefixes(sub(s.st.Res[key], "properties"), "addressSpace")
	for _, ck := range s.childKeys(key, ns+k.child) {
		for _, p := range subnetPrefixes(sub(s.st.Res[ck], "properties")) {
			if !within(space, p) {
				return bad("NetcfgInvalidSubnet", "Subnet '%s' is not valid in virtual network '%s'.", str(s.st.Res[ck]["name"]), id.Name())
			}
		}
	}
	return nil
}

// ---- references ----

// refKeys are the properties that point at another resource: {"id": ...}, or a list of those.
var refKeys = map[string]bool{"networkSecurityGroup": true, "routeTable": true, "natGateway": true, "publicIPAddress": true,
	"subnet": true, "virtualNetwork": true, "publicIpAddresses": true}

// refs lists the resource IDs a property tree refers to, by property name.
func refs(v any, f func(key, id string)) {
	switch v := v.(type) {
	case map[string]any:
		for k, x := range v {
			if refKeys[k] {
				for _, r := range asList(x) {
					if m, ok := r.(map[string]any); ok && str(m["id"]) != "" {
						f(k, str(m["id"]))
					}
				}
			}
			refs(x, f)
		}
	case []any:
		for _, x := range v {
			refs(x, f)
		}
	}
}

func asList(v any) []any {
	if l, ok := v.([]any); ok {
		return l
	}
	return []any{v}
}

// checkRefs fails when a Microsoft.Network resource that props points at does not exist.
func (s *Service) checkRefs(props map[string]any) (err error) {
	refs(props, func(_, ref string) {
		if id, ok := arm.ParseID(ref); err == nil && ok && strings.EqualFold(id.Namespace, "Microsoft.Network") && s.st.Res[strings.ToLower(ref)] == nil {
			err = bad("InvalidResourceReference", "Resource %s referenced by resource was not found. Please make sure that the referenced resource exists, and that both resources are in the same region.", ref)
		}
	})
	return err
}

var inUseCode = map[string]string{"subnets": "InUseSubnetCannotBeDeleted", "virtualNetworks": "InUseSubnetCannotBeDeleted",
	"networkSecurityGroups": "InUseNetworkSecurityGroupCannotBeDeleted", "routeTables": "InUseRouteTableCannotBeDeleted",
	"publicIPAddresses": "PublicIPAddressCannotBeDeleted", "natGateways": "InUseNatGatewayCannotBeDeleted"}

// inUse fails the deletion of a resource, or of a virtual network whose subnets, that another resource still points at.
func (s *Service) inUse(k *kind, id arm.ID, key string) (err error) {
	code := inUseCode[lastSeg(k.typ)]
	if code == "" {
		return nil
	}
	for rk, doc := range s.st.Res {
		if rk == key || strings.HasPrefix(rk, key+"/") {
			continue
		}
		refs(doc["properties"], func(prop, ref string) {
			low := strings.ToLower(ref)
			if err == nil && prop != "virtualNetwork" && (low == key || strings.HasPrefix(low, key+"/")) {
				err = bad(code, "%s %s is in use by %s and cannot be deleted.", lastSeg(k.typ), id.Name(), str(doc["id"]))
			}
		})
	}
	return err
}

// ---- small helpers ----

func guid() string {
	h := core.NewID()
	return h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
}

func clone(m map[string]any) map[string]any {
	b, _ := json.Marshal(m)
	var out map[string]any
	json.Unmarshal(b, &out)
	return out
}

func sub(m map[string]any, key string) map[string]any {
	v, _ := m[key].(map[string]any)
	if v == nil {
		v = map[string]any{}
		m[key] = v
	}
	return v
}

func def(m map[string]any, key string, v any) {
	if _, ok := m[key]; !ok {
		m[key] = v
	}
}

func str(v any) string { s, _ := v.(string); return s }

func cmpOr(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

func bad(code, format string, args ...any) error {
	return core.Errorf(http.StatusBadRequest, code, format, args...)
}

// prefixes reads props[key].addressPrefixes; invalid CIDRs are skipped.
func prefixes(props map[string]any, key string) (out []netip.Prefix) {
	l, _ := sub(props, key)["addressPrefixes"].([]any)
	for _, v := range l {
		if p, err := netip.ParsePrefix(str(v)); err == nil {
			out = append(out, p)
		}
	}
	return out
}

// subnetPrefixes is addressPrefix plus addressPrefixes of a subnet.
func subnetPrefixes(props map[string]any) (out []netip.Prefix) {
	vals := []any{props["addressPrefix"]}
	if l, ok := props["addressPrefixes"].([]any); ok {
		vals = append(vals, l...)
	}
	for _, v := range vals {
		if p, err := netip.ParsePrefix(str(v)); err == nil {
			out = append(out, p)
		}
	}
	return out
}

func within(space []netip.Prefix, p netip.Prefix) bool {
	for _, sp := range space {
		if sp.Bits() <= p.Bits() && sp.Contains(p.Addr()) {
			return true
		}
	}
	return false
}

// ---- persistence and console ----

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
	if st.Res == nil {
		st.Res = map[string]map[string]any{}
	}
	s.mu.Lock()
	s.st = st
	s.mu.Unlock()
	return nil
}

func (s *Service) Reset() { s.Load([]byte("{}")) }

func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := core.Table{Name: "Resources", Columns: []string{"ID", "Type", "Name", "Location"}}
	for _, k := range core.SortedKeys(s.st.Res) {
		r := s.st.Res[k]
		t.Rows = append(t.Rows, []string{str(r["id"]), strings.TrimPrefix(str(r["type"]), ns), str(r["name"]), str(r["location"])})
	}
	return []core.Table{t}
}

func (s *Service) Inspect(_, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r := s.st.Res[strings.ToLower(id)]; r != nil {
		return s.view(s.kinds[strings.ToLower(str(r["type"]))], strings.ToLower(id))
	}
	return nil
}
