package compute

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/netip"
	"slices"
	"strconv"
	"strings"

	"github.com/nrndev/pogi/internal/core"
)

// kind is one stored resource type.
type kind struct {
	kind string // compute#instance
	// fill validates a new resource and completes it; mu is held, so it may change the state.
	fill func(q *req, path string, doc map[string]any) error
	// after runs once the resource is stored, without the lock; its error fails the operation.
	after func(q *req, path string) error
	// del deletes the resource itself, without the lock; nil: delete it unless something refers to it.
	del    func(q *req, path string, doc map[string]any) error
	update bool                                              // PATCH and PUT
	acts   map[string]func(q *req, path string) error        // POST .../<name>/<action>
	reads  map[string]func(q *req, path string) (any, error) // GET .../<name>/<action>
}

var kinds = map[string]*kind{} // by "zones/instances", "global/networks", ...

func init() {
	add := func(scope, coll string, k *kind) { kinds[scope+"/"+coll] = k }
	add("zones", "instances", instanceKind())
	add("zones", "disks", &kind{kind: "compute#disk", fill: fillDisk, del: delDisk, acts: map[string]func(*req, string) error{"setLabels": setLabels}})
	add("global", "networks", &kind{kind: "compute#network", fill: fillNetwork, update: true})
	add("regions", "subnetworks", &kind{kind: "compute#subnetwork", fill: fillSubnet, update: true})
	add("global", "firewalls", &kind{kind: "compute#firewall", fill: fillFirewall, update: true})
	add("regions", "addresses", &kind{kind: "compute#address", fill: fillAddress})
	add("global", "addresses", &kind{kind: "compute#address", fill: fillAddress})
}

// collection serves a collection or a resource of a zone, a region or the project: rest is
// [collection, name, action].
func (s *Service) collection(q *req, scope, sn string, rest []string) (any, error) {
	switch rest[0] {
	case "operations":
		return s.operations(q, scope, sn, rest)
	case "machineTypes", "diskTypes", "images":
		return s.static(q, sn, rest)
	}
	k := kinds[scope+"/"+rest[0]]
	if k == nil {
		return nil, core.Errorf(http.StatusNotFound, "notFound", "Not Found")
	}
	base := "projects/" + q.project + "/" + scope
	if scope != "global" {
		base += "/" + sn
	}
	coll, m := base+"/"+rest[0], q.r.Method
	switch {
	case len(rest) == 1 && m == http.MethodGet:
		return q.list(strings.TrimPrefix(k.kind, "compute#")+"List", coll, s.items(coll)), nil
	case len(rest) == 1 && m == http.MethodPost:
		return s.insert(q, k, scope, sn, base, coll)
	case len(rest) == 2 && m == http.MethodGet:
		doc, err := s.get(coll + "/" + rest[1])
		return doc, err
	case len(rest) == 2 && m == http.MethodDelete:
		return s.remove(q, k, scope, sn, coll+"/"+rest[1])
	case len(rest) == 2 && k.update && (m == http.MethodPatch || m == http.MethodPut):
		return s.update(q, scope, sn, coll+"/"+rest[1], m == http.MethodPatch)
	case len(rest) == 3 && m == http.MethodPost && k.acts[rest[2]] != nil:
		path := coll + "/" + rest[1]
		doc, err := s.get(path)
		if err == nil {
			err = k.acts[rest[2]](q, path)
		}
		if err != nil {
			return nil, err
		}
		return s.op(q, scope, sn, rest[2], path, doc, nil), nil
	case len(rest) == 3 && m == http.MethodGet && k.reads[rest[2]] != nil:
		path := coll + "/" + rest[1]
		if _, err := s.get(path); err != nil {
			return nil, err
		}
		return k.reads[rest[2]](q, path)
	}
	return nil, core.Errorf(http.StatusNotFound, "notFound", "Not Found")
}

func (s *Service) get(path string) (map[string]any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if d := s.st.Res[path]; d != nil {
		return clone(d), nil
	}
	return nil, notFound(path)
}

// items lists the resources directly under a collection path.
func (s *Service) items(coll string) (out []map[string]any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, p := range core.SortedKeys(s.st.Res) {
		if rest, ok := strings.CutPrefix(p, coll+"/"); ok && !strings.Contains(rest, "/") {
			out = append(out, clone(s.st.Res[p]))
		}
	}
	return out
}

func (s *Service) insert(q *req, k *kind, scope, sn, base, coll string) (any, error) {
	doc := clone(q.body)
	name := str(doc["name"])
	if !nameRE.MatchString(name) {
		return nil, invalid("Invalid value for field 'resource.name': '%s'. Must be a match of regex '(?:[a-z](?:[-a-z0-9]{0,61}[a-z0-9])?)'", name)
	}
	path := coll + "/" + name
	doc["kind"], doc["id"], doc["creationTimestamp"], doc["selfLink"] = k.kind, newID(), now(), api+path
	switch scope {
	case "zones":
		doc["zone"] = api + base
	case "regions":
		doc["region"] = api + base
	}
	s.mu.Lock()
	if s.st.Res[path] != nil {
		s.mu.Unlock()
		return nil, core.Errorf(http.StatusConflict, "alreadyExists", "The resource '%s' already exists", path)
	}
	if k.fill != nil {
		if err := k.fill(q, path, doc); err != nil {
			s.mu.Unlock()
			return nil, err
		}
	}
	s.st.Res[path] = doc
	s.mu.Unlock()
	var err error
	if k.after != nil {
		err = k.after(q, path)
	}
	return s.op(q, scope, sn, "insert", path, doc, err), nil
}

func (s *Service) remove(q *req, k *kind, scope, sn, path string) (any, error) {
	doc, err := s.get(path)
	if err != nil {
		return nil, err
	}
	if k.del != nil {
		err = k.del(q, path, doc)
	} else {
		s.mu.Lock()
		if err = s.inUse(path, doc); err == nil {
			delete(s.st.Res, path)
		}
		s.mu.Unlock()
	}
	if err != nil {
		return nil, err
	}
	return s.op(q, scope, sn, "delete", path, doc, nil), nil
}

// inUse fails when another resource refers to this one by its link; mu is held.
func (s *Service) inUse(path string, doc map[string]any) error {
	link := `"` + str(doc["selfLink"]) + `"`
	for _, p := range core.SortedKeys(s.st.Res) {
		if b, _ := jsonOf(s.st.Res[p]); p != path && strings.Contains(b, link) {
			parts := strings.Split(path, "/")
			return core.Errorf(http.StatusBadRequest, "resourceInUseByAnotherResource", "The %s resource '%s' is already being used by '%s'",
				strings.TrimSuffix(parts[len(parts)-2], "s"), path, p)
		}
	}
	return nil
}

// update is PATCH (a merge) or PUT (a replacement) of a network, subnetwork or firewall.
func (s *Service) update(q *req, scope, sn, path string, patch bool) (any, error) {
	s.mu.Lock()
	cur := s.st.Res[path]
	if cur == nil {
		s.mu.Unlock()
		return nil, notFound(path)
	}
	doc := clone(q.body)
	if patch {
		doc = merge(clone(cur), doc)
	}
	for _, f := range []string{"kind", "id", "creationTimestamp", "name", "selfLink", "zone", "region"} {
		doc[f] = cur[f]
	}
	s.st.Res[path] = doc
	s.mu.Unlock()
	return s.op(q, scope, sn, map[bool]string{true: "patch", false: "update"}[patch], path, doc, nil), nil
}

// merge applies a PATCH: objects merge, anything else is replaced.
func merge(dst, src map[string]any) map[string]any {
	for k, v := range src {
		d, dok := dst[k].(map[string]any)
		sv, sok := v.(map[string]any)
		if dok && sok {
			merge(d, sv)
		} else {
			dst[k] = v
		}
	}
	return dst
}

// aggregated lists a collection of every zone and region: items by "zones/z".
func (s *Service) aggregated(q *req, coll string) (any, error) {
	var k *kind
	for _, scope := range []string{"zones", "regions", "global"} {
		if k = kinds[scope+"/"+coll]; k != nil {
			break
		}
	}
	if k == nil {
		return nil, core.Errorf(http.StatusNotFound, "notFound", "Not Found")
	}
	items := map[string]any{}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, p := range core.SortedKeys(s.st.Res) {
		parts := strings.Split(p, "/") // projects, p, zones, z, instances, n
		if len(parts) < 5 || parts[1] != q.project || parts[len(parts)-2] != coll {
			continue
		}
		scope := strings.Join(parts[2:len(parts)-2], "/")
		e, _ := items[scope].(map[string]any)
		if e == nil {
			e = map[string]any{coll: []any{}}
			items[scope] = e
		}
		e[coll] = append(e[coll].([]any), clone(s.st.Res[p]))
	}
	single := strings.TrimPrefix(k.kind, "compute#")
	return map[string]any{"kind": "compute#" + single + "AggregatedList", "id": "projects/" + q.project + "/aggregated/" + coll, "items": items,
		"selfLink": api + "projects/" + q.project + "/aggregated/" + coll}, nil
}

// static serves machine types, disk types and images.
func (s *Service) static(q *req, zone string, rest []string) (any, error) {
	if q.r.Method != http.MethodGet {
		return nil, core.Errorf(http.StatusNotFound, "notFound", "Not Found")
	}
	var docs []map[string]any
	zp := "projects/" + q.project + "/zones/" + zone
	switch rest[0] {
	case "machineTypes":
		for _, m := range machines {
			docs = append(docs, m.doc(q.project, api+zp))
		}
	case "diskTypes":
		for _, n := range diskTypes {
			docs = append(docs, map[string]any{"kind": "compute#diskType", "id": strconv.Itoa(4000 + len(n)), "name": n, "description": n, "validDiskSize": "10GB-65536GB",
				"zone": api + zp, "selfLink": api + zp + "/diskTypes/" + n, "defaultDiskSizeGb": "500"})
		}
	case "images":
		for i := range images {
			if images[i].project == q.project {
				docs = append(docs, images[i].doc())
			}
		}
	}
	coll := strings.Join([]string{"projects", q.project, "zones", zone}, "/") + "/" + rest[0]
	if rest[0] == "images" {
		coll = "projects/" + q.project + "/global/images"
	}
	if len(rest) == 1 {
		return q.list(strings.TrimSuffix(rest[0], "s")+"List", coll, docs), nil
	}
	name, field := rest[len(rest)-1], "name"
	if len(rest) == 3 && rest[1] == "family" {
		field = "family"
	}
	for _, d := range docs {
		if d[field] == name {
			return d, nil
		}
	}
	return nil, notFound(coll + "/" + strings.Join(rest[1:], "/"))
}

// ---- disks ----

func sizeOf(v any) int {
	switch x := v.(type) {
	case float64:
		return int(x)
	case string:
		n, _ := strconv.Atoi(x)
		return n
	}
	return 0
}

func setLabels(q *req, path string) error {
	s := q.s
	s.mu.Lock()
	defer s.mu.Unlock()
	doc := s.st.Res[path]
	if err := checkFingerprint(q.body["labelFingerprint"], doc, "labelFingerprint", "label"); err != nil {
		return err
	}
	doc["labels"], doc["labelFingerprint"] = q.body["labels"], fingerprint()
	if doc["labels"] == nil {
		delete(doc, "labels")
	}
	return nil
}

// fillDisk completes a disk, from a source image when it has one; mu is held. The query parameter
// sourceImage works as the field does.
func fillDisk(q *req, path string, doc map[string]any) error {
	p := strings.Split(path, "/") // projects, p, zones, z, disks, n
	zone := api + strings.Join(p[:4], "/")
	if img := q.r.URL.Query().Get("sourceImage"); img != "" && doc["sourceImage"] == nil {
		doc["sourceImage"] = img
	}
	size := sizeOf(doc["sizeGb"])
	if img := str(doc["sourceImage"]); img != "" {
		im := findImage(img)
		if im == nil {
			return notFound(ref(img, "projects/"+q.project+"/global/images"))
		}
		doc["sourceImage"], doc["sourceImageId"] = api+"projects/"+im.project+"/global/images/"+im.name, str(im.doc()["id"])
		if size == 0 {
			size = 10
		}
	}
	if size < 1 {
		return invalid("Invalid value for field 'resource.sizeGb': '0'. Disk size must be at least 1 GB.")
	}
	typ := ref(firstOf(str(doc["type"]), "pd-standard"), strings.Join(p[:4], "/")+"/diskTypes")
	if !slices.Contains(diskTypes, typ[strings.LastIndexByte(typ, '/')+1:]) {
		return invalid("Invalid value for field 'resource.type': '%s'. Unknown disk type.", typ)
	}
	doc["sizeGb"], doc["type"], doc["status"] = strconv.Itoa(size), api+typ, "READY"
	doc["users"], doc["labelFingerprint"] = []any{}, fingerprint()
	doc["physicalBlockSizeBytes"], doc["zone"] = "4096", zone
	delete(doc, "initializeParams")
	return nil
}

func delDisk(q *req, path string, doc map[string]any) error {
	s := q.s
	s.mu.Lock()
	defer s.mu.Unlock()
	if u := arr(doc, "users"); len(u) > 0 {
		return core.Errorf(http.StatusBadRequest, "resourceInUseByAnotherResource", "The disk resource '%s' is already being used by '%s'", path, strings.TrimPrefix(str(u[0]), api))
	}
	delete(s.st.Res, path)
	return nil
}

// ---- networks ----

func fillNetwork(q *req, path string, doc map[string]any) error {
	if doc["autoCreateSubnetworks"] == nil {
		doc["autoCreateSubnetworks"] = false
	}
	if obj(doc, "routingConfig") == nil {
		doc["routingConfig"] = map[string]any{"routingMode": "REGIONAL"}
	}
	if doc["mtu"] == nil {
		doc["mtu"] = 1460
	}
	doc["subnetworks"] = []any{}
	return nil
}

// network checks a reference to a network of the project and returns its path.
func (s *Service) network(project, in string) (string, error) {
	p := ref(in, "projects/"+project+"/global/networks")
	if s.st.Res[p] == nil {
		return "", notFound(p)
	}
	return p, nil
}

func fillSubnet(q *req, path string, doc map[string]any) error {
	s := q.s
	cidr, err := netip.ParsePrefix(str(doc["ipCidrRange"]))
	if err != nil || !cidr.Addr().Is4() {
		return invalid("Invalid value for field 'resource.ipCidrRange': '%s'. Must be a valid CIDR range.", str(doc["ipCidrRange"]))
	}
	net, err := s.network(q.project, str(doc["network"]))
	if err != nil {
		if str(doc["network"]) == "" {
			return invalid("Required field 'network' not specified")
		}
		return err
	}
	for p, o := range s.st.Res {
		if strings.HasPrefix(p, path[:strings.LastIndexByte(path, '/')+1]) && str(o["network"]) == api+net {
			if other, err := netip.ParsePrefix(str(o["ipCidrRange"])); err == nil && other.Overlaps(cidr) {
				return invalid("Invalid IPCidrRange: %s conflicts with existing subnetwork '%s' in region '%s'.", cidr, o["name"], regionPart(path))
			}
		}
	}
	doc["network"], doc["ipCidrRange"] = api+net, cidr.Masked().String()
	doc["gatewayAddress"] = cidr.Masked().Addr().Next().String()
	doc["fingerprint"] = fingerprint()
	if doc["privateIpGoogleAccess"] == nil {
		doc["privateIpGoogleAccess"] = false
	}
	doc["purpose"], doc["stackType"], doc["privateIpv6GoogleAccess"] = "PRIVATE", "IPV4_ONLY", "DISABLE_GOOGLE_ACCESS"
	return nil
}

func regionPart(path string) string { return strings.Split(path, "/")[3] }

func fillFirewall(q *req, path string, doc map[string]any) error {
	net := firstOf(str(doc["network"]), "default")
	n, err := q.s.network(q.project, net)
	if err != nil {
		return err
	}
	doc["network"] = api + n
	if len(arr(doc, "allowed")) == 0 && len(arr(doc, "denied")) == 0 {
		return invalid("Invalid value for field 'resource': ''. The firewall must specify at least one allowed or denied rule.")
	}
	if doc["direction"] == nil {
		doc["direction"] = "INGRESS"
	}
	if doc["priority"] == nil {
		doc["priority"] = 1000
	} else if p := sizeOf(doc["priority"]); p < 0 || p > 65535 {
		return invalid("Invalid value for field 'resource.priority': '%d'. Must be between 0 and 65535.", p)
	}
	if doc["disabled"] == nil {
		doc["disabled"] = false
	}
	return nil
}

// ---- addresses ----

// extIP is the next public address of the fake range; mu is held.
func (s *Service) extIP() string {
	s.st.IP++
	return fmt.Sprintf("34.100.%d.%d", s.st.IP/250, s.st.IP%250+1)
}

// intIP is the next address of a subnet's range (or 10.128.0.0/20); mu is held.
func (s *Service) intIP(subnet string) string {
	cidr := netip.MustParsePrefix("10.128.0.0/20")
	if d := s.st.Res[ref(subnet, "")]; d != nil {
		if c, err := netip.ParsePrefix(str(d["ipCidrRange"])); err == nil {
			cidr = c
		}
	}
	s.st.IP++
	a := cidr.Masked().Addr()
	for i := 0; i < 1+s.st.IP%250; i++ {
		a = a.Next()
	}
	return a.String()
}

func fillAddress(q *req, path string, doc map[string]any) error {
	if doc["addressType"] == nil {
		doc["addressType"] = "EXTERNAL"
	}
	if doc["address"] == nil {
		if doc["addressType"] == "INTERNAL" {
			doc["address"] = q.s.intIP(str(doc["subnetwork"]))
		} else {
			doc["address"] = q.s.extIP()
		}
	}
	doc["status"], doc["ipVersion"], doc["networkTier"] = "RESERVED", "IPV4", "PREMIUM"
	return nil
}

func jsonOf(m map[string]any) (string, error) {
	b, err := json.Marshal(m)
	return string(b), err
}
