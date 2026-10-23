package generic

import (
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strings"

	"github.com/nrndev/pogi/internal/gcp/gapi"
)

// Per-API overrides, by the API's name in apis.txt.
var apiHooks = map[string]hooks{
	"dns": {
		methods: map[string]hook{
			"dns.changes.create":            dnsChange,
			"dns.managedZones.patch":        dnsZoneUpdate,
			"dns.managedZones.update":       dnsZoneUpdate,
			"dns.resourceRecordSets.create": rrsetCreate,
			"dns.resourceRecordSets.get":    rrsetGet,
			"dns.resourceRecordSets.patch":  rrsetPatch,
			"dns.resourceRecordSets.delete": rrsetDelete,
			"dns.resourceRecordSets.list":   rrsetList,
		},
		required: map[string][]string{"ManagedZone": {"name", "dnsName"}},
		check: map[string]func(*call, map[string]any) error{"ManagedZone": func(_ *call, z map[string]any) error {
			if n, _ := z["name"].(string); !zoneName.MatchString(n) {
				return gapi.Invalid("Invalid value for 'entity.managedZone.name': '%s'", n)
			}
			if d, _ := z["dnsName"].(string); !strings.HasSuffix(d, ".") {
				return gapi.Invalid("Invalid value for 'entity.managedZone.dnsName': '%s'", d)
			}
			return nil
		}},
		created: map[string]func(*call, map[string]any){"ManagedZone": func(_ *call, z map[string]any) {
			if z["visibility"] == nil {
				z["visibility"] = "public"
			}
			z["creationTime"] = now()
			if z["visibility"] == "public" {
				z["nameServers"] = []any{"ns-cloud-e1.googledomains.com.", "ns-cloud-e2.googledomains.com.",
					"ns-cloud-e3.googledomains.com.", "ns-cloud-e4.googledomains.com."}
			}
			publicZone(z)
		}},
	},
}

// publicZone drops the empty private network list Terraform sends with every zone: a public zone
// has no privateVisibilityConfig, and Terraform would plan to remove the block it reads back.
func publicZone(z map[string]any) {
	if cfg, _ := z["privateVisibilityConfig"].(map[string]any); z["visibility"] == "public" {
		if nets, _ := cfg["networks"].([]any); len(nets) == 0 {
			delete(z, "privateVisibilityConfig")
		}
	}
}

var zoneName = regexp.MustCompile(`^[a-z]([-a-z0-9]{0,61}[a-z0-9])?$`)

// dnsNotFound is Cloud DNS's own wording for a missing parent.
func (c *call) dnsMissing(kind, id string) error {
	return gapi.NotFound("The '%s' resource named '%s' does not exist.", kind, id)
}

// zone returns the managed zone the path names.
func (c *call) zone() (map[string]any, error) {
	z := c.a.objs[strings.Join(c.segs[:4], "/")]
	if z == nil {
		return nil, c.dnsMissing("parameters.managedZone", c.segs[3])
	}
	return z, nil
}

// dnsOperation records the finished operation a zone update returns, where managedZoneOperations.get finds it.
func (c *call) dnsOperation(old, zone map[string]any) map[string]any {
	c.a.seq++
	id := fmt.Sprint(c.a.seq)
	op := map[string]any{"id": id, "kind": "dns#operation", "startTime": now(), "status": "done", "type": "update", "user": "",
		"zoneContext": map[string]any{"oldValue": old, "newValue": clone(zone)}}
	c.a.objs[strings.Join(c.segs[:4], "/")+"/operations/"+id] = op
	return clone(op)
}

func dnsZoneUpdate(c *call) (any, error) {
	old := c.a.objs[c.name()]
	if old == nil {
		return nil, c.notFound()
	}
	old = clone(old)
	z, err := c.apply()
	if err != nil {
		return nil, err
	}
	publicZone(z)
	return c.dnsOperation(old, z), nil
}

// rrsetKey is where a record set is stored.
func rrsetKey(c *call, name, typ string) string {
	return strings.Join(c.segs[:4], "/") + "/rrsets/" + strings.ToLower(name) + "/" + typ
}

func rrsetFrom(c *call, obj map[string]any) (map[string]any, string, error) {
	name, _ := obj["name"].(string)
	typ, _ := obj["type"].(string)
	if !strings.HasSuffix(name, ".") || typ == "" {
		return nil, "", gapi.Invalid("Invalid value for 'entity.resourceRecordSet': a name ending in '.' and a type are required (%q, %q)", name, typ)
	}
	if _, ok := obj["rrdatas"]; !ok && obj["routingPolicy"] == nil {
		return nil, "", gapi.Invalid("Invalid value for 'entity.resourceRecordSet.rrdatas' of '%s (%s)': at least one record is required", name, typ)
	}
	rs := clone(obj)
	rs["kind"] = "dns#resourceRecordSet"
	return rs, rrsetKey(c, name, typ), nil
}

func rrsetCreate(c *call) (any, error) {
	if _, err := c.zone(); err != nil {
		return nil, err
	}
	rs, key, err := rrsetFrom(c, c.body)
	if err != nil {
		return nil, err
	}
	if c.a.objs[key] != nil {
		return nil, gapi.Exists("The resource record set '%s (%s)' already exists.", rs["name"], rs["type"])
	}
	c.a.objs[key] = rs
	return clone(rs), nil
}

func rrsetGet(c *call) (any, error) {
	if _, err := c.zone(); err != nil {
		return nil, err
	}
	rs := c.a.objs[rrsetKey(c, c.segs[5], c.segs[6])]
	if rs == nil {
		return nil, c.dnsMissing("parameters.name", c.segs[5])
	}
	return clone(rs), nil
}

func rrsetPatch(c *call) (any, error) {
	if _, err := c.zone(); err != nil {
		return nil, err
	}
	key := rrsetKey(c, c.segs[5], c.segs[6])
	rs := c.a.objs[key]
	if rs == nil {
		return nil, c.dnsMissing("parameters.name", c.segs[5])
	}
	next := clone(rs)
	for k, v := range c.body {
		if k != "name" && k != "type" && k != "kind" {
			next[k] = v
		}
	}
	c.a.objs[key] = next
	return clone(next), nil
}

func rrsetDelete(c *call) (any, error) {
	if _, err := c.zone(); err != nil {
		return nil, err
	}
	key := rrsetKey(c, c.segs[5], c.segs[6])
	if c.a.objs[key] == nil {
		return nil, c.dnsMissing("parameters.name", c.segs[5])
	}
	delete(c.a.objs, key)
	return map[string]any{}, nil
}

// rrsetList lists a zone's record sets, only the ones the name and type query parameters name when they are given.
func rrsetList(c *call) (any, error) {
	if _, err := c.zone(); err != nil {
		return nil, err
	}
	prefix := strings.Join(c.segs[:4], "/") + "/rrsets/"
	var items []map[string]any
	for _, k := range slices.Sorted(maps.Keys(c.a.objs)) {
		rs := c.a.objs[k]
		if !strings.HasPrefix(k, prefix) ||
			c.q.Get("name") != "" && !strings.EqualFold(rs["name"].(string), c.q.Get("name")) ||
			c.q.Get("type") != "" && rs["type"] != c.q.Get("type") {
			continue
		}
		items = append(items, rs)
	}
	return c.page(items)
}

// dnsChange applies a change: its deletions, then its additions, all or nothing. The change is done
// when it is returned.
func dnsChange(c *call) (any, error) {
	a := c.a
	if _, err := c.zone(); err != nil {
		return nil, err
	}
	del, add := map[string]map[string]any{}, map[string]map[string]any{}
	for _, part := range []struct {
		field string
		into  map[string]map[string]any
	}{{"deletions", del}, {"additions", add}} {
		list, _ := c.body[part.field].([]any)
		for i, x := range list {
			m, _ := x.(map[string]any)
			rs, key, err := rrsetFrom(c, m)
			if err != nil {
				return nil, err
			}
			if _, dup := part.into[key]; dup {
				return nil, gapi.Invalid("Invalid value for 'entity.change.%s[%d]': '%s (%s)' appears twice", part.field, i, rs["name"], rs["type"])
			}
			part.into[key] = rs
		}
	}
	if len(del)+len(add) == 0 {
		return nil, gapi.Invalid("A change needs at least one addition or deletion.")
	}
	for key, rs := range del {
		if a.objs[key] == nil {
			return nil, gapi.NotFound("The 'entity.change.deletions[0]' resource named '%s (%s)' does not exist.", rs["name"], rs["type"])
		}
	}
	for key, rs := range add {
		if a.objs[key] != nil && del[key] == nil {
			return nil, gapi.Exists("The resource record set '%s (%s)' already exists.", rs["name"], rs["type"])
		}
	}
	for key := range del {
		delete(a.objs, key)
	}
	maps.Copy(a.objs, add)
	a.seq++
	id := fmt.Sprint(a.seq)
	ch := clone(c.body)
	ch["id"], ch["kind"], ch["startTime"], ch["status"], ch["isServing"] = id, "dns#change", now(), "done", true
	a.objs[strings.Join(c.segs[:4], "/")+"/changes/"+id] = ch
	return clone(ch), nil
}
