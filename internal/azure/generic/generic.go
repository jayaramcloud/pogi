// Package generic is the Azure resource-manager engine: it serves any listed resource type through ARM
// from what Microsoft's REST API specs say about it, the way internal/aws/generic serves AWS services from
// their models. A service is a line in services.txt; a type's shape (API versions, which operations are
// long-running, read-only and required properties, tracked or not, name rules) is generated into
// specs.json.gz by `go generate ./internal/azure/generic`.
//
// # What the engine does
//
// Per listed type it registers an arm.Provider: PUT, GET, PATCH (ARM merges it) and DELETE, lists by resource
// group and by subscription, child and nested types, and extension types at any scope (a policy assignment on
// a resource or a management group). It fills id, name and type, tags and provisioningState Succeeded, and
// answers like a long-running operation where the spec marks one. It refuses a missing location of a tracked
// type (LocationRequired), a missing required property (InvalidRequestContent) and a bad name
// (InvalidResourceName); ARM itself answers ParentResourceNotFound and ResourceNotFound. Read-only
// properties of a request are dropped. Nothing runs behind a resource: it is stored and read back.
//
// A hand-written Provider always wins: a type that arm.Lookup already has is not served here.
// Types that are not listed are refused by ARM with InvalidResourceType, as before.
//
// # Adding a type or a service
//
//  1. Add the type to its service's line in services.txt, or a new line (name | title | spec files | types).
//     A service name starts with "az". A record-set style path whose type is a parameter with an enum
//     (dnsZones/{recordType}/{name}) gives one type per value; list them all.
//
//  2. Run `go generate ./internal/azure/generic` (scripts/azure-specs.sh, needs curl and python3).
//
//  3. A new service needs no code: pogi.go registers every name in Names(). Add a test with the service's
//     official client, a Terraform file in examples/terraform-azure/ and a row in docs/reference/coverage.yaml
//     (and its category).
//
//  4. What the spec does not say, the service does, in <service>.go in this package, from an init function
//     (the example below).
//
// An override sets the hooks of a type:
//
//	generic.Override("Microsoft.Network/dnsZones", generic.Hook{
//		Put:  func(s *generic.Service, c *core.Ctx, id arm.ID, r map[string]any) error { ... }, // before the body is stored
//		View: func(s *generic.Service, id arm.ID, r map[string]any) { ... },                    // computed properties, on every read
//		Actions: map[string]func(*generic.Service, *core.Ctx, arm.ID, map[string]any) (any, error){
//			"listKeys": func(s *generic.Service, c *core.Ctx, id arm.ID, _ map[string]any) (any, error) { ... },
//		},
//	})
//
// Put may change r (defaults, children via s.Create). View changes the copy that is returned. The hooks run
// without the service's lock; s.Has, s.Create and s.Count take it.
//
// # Limits
//
// No validation beyond what is listed above (property values, enums, formats are accepted as sent). Only the
// actions a hook adds (another POST on a resource answers InvalidActionType), no ETag or If-Match checks, nothing beyond a type's item and
// collection paths (a zone's "recordsets" and "all" listings are added by dns.go). API versions are
// accepted whatever they are and only listed back in the provider's resource types.
package generic

//go:generate sh ../../../scripts/azure-specs.sh

import (
	"bytes"
	"cmp"
	"compress/gzip"
	"crypto/sha256"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/azure/armres"
	"github.com/nrndev/pogi/internal/core"
)

//go:embed specs.json.gz
var specsGz []byte

type typeSpec struct {
	Versions []string `json:"v"`
	Ops      []string `json:"ops"`
	LRO      []string `json:"lro"`
	ReadOnly []string `json:"ro"`
	Required []string `json:"req"`
	Tracked  bool     `json:"tracked"`
	Created  int      `json:"created"` // status of a PUT that creates, when the spec's PUT has no 201
	// Patch202: the PATCH is long-running and the spec lists 202 for it, so it answers 202 with an operation.
	// Some long-running PATCHes list only 200 (and 201); they stay plain calls.
	Patch202 bool `json:"patch202"`
	Name     struct {
		Pattern string `json:"pattern"`
		Min     int    `json:"min"`
		Max     int    `json:"max"`
	} `json:"name"`

	typ     string
	pattern *regexp.Regexp
}

type serviceSpec struct {
	Title string               `json:"title"`
	Types map[string]*typeSpec `json:"types"`
}

var specs = sync.OnceValue(func() map[string]*serviceSpec {
	zr, err := gzip.NewReader(bytes.NewReader(specsGz))
	if err != nil {
		panic("azure generic: " + err.Error())
	}
	raw, err := io.ReadAll(zr)
	out := map[string]*serviceSpec{}
	if err == nil {
		err = json.Unmarshal(raw, &out)
	}
	if err != nil {
		panic("azure generic: " + err.Error())
	}
	for _, svc := range out {
		for typ, ts := range svc.Types {
			ts.typ = typ
			if ts.Name.Pattern != "" {
				// A few specs use look-around (?!, (?<!), which Go's regexp does not have: those names are
				// checked by length only.
				ts.pattern, _ = regexp.Compile(ts.Name.Pattern)
			}
		}
	}
	return out
})

// Names lists the services of services.txt, for the registry.
func Names() []string { return core.SortedKeys(specs()) }

// Hook adds per-type behavior to the engine; see the package comment. Either may be nil.
type Hook struct {
	Put  func(s *Service, c *core.Ctx, id arm.ID, r map[string]any) error
	View func(s *Service, id arm.ID, r map[string]any)
	// Extra are more Providers that come with the type, for paths the spec lists beside it (a zone's "recordsets").
	Extra func(s *Service) []arm.Provider
	// Actions are POSTs on a resource by name ("listKeys"), as in arm.Provider; they run only when the resource
	// exists. Without one, a POST answers InvalidActionType.
	Actions map[string]func(s *Service, c *core.Ctx, id arm.ID, body map[string]any) (any, error)
	// Created is the status of a PUT, for services whose client accepts only one (202 for a Static Web App); 201 when 0.
	Created int
	// Untracked drops location and tags from a type whose spec lists them but whose clients never send them
	// (Notification Hubs' authorization rules).
	Untracked bool
	// SyncDelete makes a DELETE answer at once, for a type whose spec marks it long-running but whose clients
	// (another API version) do not poll it.
	SyncDelete bool
	// Default is what a GET answers for a resource that was never stored, for what every subscription has from
	// the start (Defender for Cloud's settings): nil means the resource does not exist. A PUT stores it as usual.
	Default func(id arm.ID) map[string]any
	// LongActions are the Actions that answer like a long-running operation (202), such as a runbook's "publish".
	LongActions []string
}

var hooks = map[string]Hook{} // by lower-cased type; written by init functions only

// Override sets the hook of a resource type.
func Override(typ string, h Hook) { hooks[strings.ToLower(typ)] = h }

// Service is one service of services.txt.
type Service struct {
	name, title string
	hub         *core.Hub
	types       map[string]*typeSpec // the types this service serves, by lower-cased name
	mu          sync.Mutex
	st          state
}

type state struct {
	ARM armres.Store `json:"arm,omitempty"`
}

// New creates the service name and registers its types, except those that a Provider already serves.
func New(h *core.Hub, name string) *Service {
	sp := specs()[name]
	if sp == nil {
		panic("azure generic: no service " + name)
	}
	s := &Service{name: name, title: sp.Title, hub: h, types: map[string]*typeSpec{}}
	for _, typ := range core.SortedKeys(sp.Types) {
		if arm.Lookup(h, typ) != nil {
			continue
		}
		ts := sp.Types[typ]
		if hooks[strings.ToLower(typ)].Untracked {
			ts.Tracked = false
		}
		s.types[strings.ToLower(typ)] = ts
		arm.Register(h, s.provider(ts))
		if x := hooks[strings.ToLower(typ)].Extra; x != nil {
			for _, p := range x(s) {
				arm.Register(h, p)
			}
		}
	}
	return s
}

func (s *Service) Info() core.Info {
	// ARM routes to the Providers; this service has no HTTP routes of its own.
	return core.Info{Name: s.name, Cloud: core.Azure, Title: s.title, Protocol: "azure-json",
		Match: func(*http.Request) bool { return false }}
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) { http.NotFound(w, r) }

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
	s.mu.Lock()
	s.st = st
	s.mu.Unlock()
	return nil
}

func (s *Service) Reset() { s.Load([]byte("{}")) }

func key(id arm.ID) string { return strings.ToLower(id.String()) }

func (s *Service) provider(ts *typeSpec) arm.Provider {
	h := hooks[strings.ToLower(ts.typ)]
	view := func(id arm.ID, r map[string]any) map[string]any {
		if h.View != nil {
			h.View(s, id, r)
		}
		return r
	}
	actions := map[string]func(*core.Ctx, arm.ID, map[string]any) (any, error){}
	for name, act := range h.Actions {
		actions[name] = func(c *core.Ctx, id arm.ID, body map[string]any) (any, error) {
			s.mu.Lock()
			_, ok := s.st.ARM.Get(id)
			s.mu.Unlock()
			if !ok {
				return nil, arm.NotFound(id)
			}
			return act(s, c, id, body)
		}
	}
	return arm.Provider{Type: ts.typ, APIVersions: ts.Versions, ParentNotFound: true, Actions: actions, Created: cmp.Or(h.Created, ts.Created), LongActions: h.LongActions,
		AsyncPut: slices.Contains(ts.LRO, "put"), AsyncDelete: slices.Contains(ts.LRO, "delete") && !h.SyncDelete, AsyncPatch: ts.Patch202,
		Put: func(c *core.Ctx, id arm.ID, body map[string]any) (map[string]any, error) {
			doc, err := ts.build(id, body)
			if err != nil {
				return nil, err
			}
			if h.Put != nil {
				if err := h.Put(s, c, id, doc); err != nil {
					return nil, err
				}
			}
			s.mu.Lock()
			s.st.ARM.Put(id, doc)
			s.mu.Unlock()
			return view(id, armres.Clone(doc)), nil
		},
		Get: func(c *core.Ctx, id arm.ID) (map[string]any, error) {
			s.mu.Lock()
			r, ok := s.st.ARM.Get(id)
			s.mu.Unlock()
			switch {
			case ok:
				return view(id, r), nil
			case h.Default != nil && h.Default(id) != nil:
				return view(id, h.Default(id)), nil
			case !strings.EqualFold(id.Type(), ts.typ): // an extension type, scoped by another resource: its ID is not a plain type chain
				return nil, core.Errorf(http.StatusNotFound, "ResourceNotFound", "The Resource '%s/%s' was not found.", ts.typ, id.Name())
			}
			return nil, arm.NotFound(id)
		},
		Delete: func(c *core.Ctx, id arm.ID) error {
			s.mu.Lock()
			s.st.ARM.Delete(id)
			s.mu.Unlock()
			return nil
		},
		List: func(c *core.Ctx, parent arm.ID) ([]map[string]any, error) {
			var stored []map[string]any
			s.mu.Lock()
			if len(parent.Names) < len(parent.Types) { // the collection of an extension type: .../<scope>/providers/<ns>/<type>
				prefix := key(parent) + "/"
				for _, k := range core.SortedKeys(s.st.ARM) {
					if rest, ok := strings.CutPrefix(k, prefix); ok && !strings.Contains(rest, "/") {
						stored = append(stored, armres.Clone(s.st.ARM[k]))
					}
				}
			} else {
				stored = s.st.ARM.List(parent, ts.typ)
			}
			s.mu.Unlock()
			out := make([]map[string]any, 0, len(stored))
			for _, r := range stored {
				id, _ := arm.ParseID(armres.Str(r, "id"))
				out = append(out, view(id, r))
			}
			return out, nil
		},
	}
}

// build checks a PUT body and makes the resource that is stored from it.
func (ts *typeSpec) build(id arm.ID, body map[string]any) (map[string]any, error) {
	if err := ts.checkName(id.Name()); err != nil {
		return nil, err
	}
	if ts.Tracked && armres.Str(body, "location") == "" {
		return nil, core.Errorf(http.StatusBadRequest, "LocationRequired", "The location property is required for this definition.")
	}
	props, _ := body["properties"].(map[string]any)
	for _, req := range ts.Required {
		var present bool
		path := ""
		if name, ok := strings.CutPrefix(req, "properties."); ok {
			path, present = "properties", props[name] != nil
		} else {
			present = body[req] != nil
		}
		if !present {
			return nil, core.Errorf(http.StatusBadRequest, "InvalidRequestContent",
				"The request content was invalid and could not be deserialized: 'Required property '%s' not found in JSON. Path '%s'.'", req[strings.LastIndexByte(req, '.')+1:], path)
		}
	}
	d := armres.Clone(body)
	if d == nil {
		d = map[string]any{}
	}
	d["id"], d["name"], d["type"] = id.String(), id.Name(), ts.typ
	if ts.Tracked && d["tags"] == nil {
		d["tags"] = map[string]any{}
	}
	systemIdentity(id, d)
	p := armres.Props(d)
	for _, name := range ts.ReadOnly { // the service computes these; a client's value is ignored
		delete(p, name)
	}
	p["provisioningState"] = "Succeeded"
	d["properties"] = p
	return d, nil
}

// systemIdentity gives a system-assigned identity its principal and tenant, as Azure does; they come from the
// resource's ID, so they stay the same for the life of the resource.
func systemIdentity(id arm.ID, d map[string]any) {
	ident, _ := d["identity"].(map[string]any)
	if t, _ := ident["type"].(string); !strings.Contains(t, "SystemAssigned") {
		return
	}
	h := sha256.Sum256([]byte("pogi:identity:" + key(id)))
	ident["principalId"] = fmt.Sprintf("%x-%x-%x-%x-%x", h[0:4], h[4:6], h[6:8], h[8:10], h[10:16])
	ident["tenantId"] = arm.TenantID
}

// checkName applies the name rules of the spec, and refuses an empty name or one with control characters.
func (ts *typeSpec) checkName(name string) error {
	bad := name == "" || len(name) < ts.Name.Min || (ts.Name.Max > 0 && len(name) > ts.Name.Max) ||
		(ts.pattern != nil && !ts.pattern.MatchString(name)) ||
		strings.IndexFunc(name, unicode.IsControl) >= 0
	if bad {
		return core.Errorf(http.StatusBadRequest, "InvalidResourceName", "The resource name '%s' is invalid.", name)
	}
	return nil
}

// ---- for hooks ----

// Has reports whether the resource exists.
func (s *Service) Has(id arm.ID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.st.ARM.Get(id)
	return ok
}

// Raw returns a copy of the stored resource, without the View of its type.
func (s *Service) Raw(id arm.ID) (map[string]any, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.st.ARM.Get(id)
}

// Create stores a resource of one of this service's types as a PUT would, running its checks and its Put
// hook; a hook uses it for the children a resource starts with.
func (s *Service) Create(c *core.Ctx, id arm.ID, body map[string]any) error {
	ts := s.types[strings.ToLower(id.Type())]
	if ts == nil {
		return fmt.Errorf("azure generic: %s serves no type %s", s.name, id.Type())
	}
	doc, err := ts.build(id, body)
	if err != nil {
		return err
	}
	if h := hooks[strings.ToLower(ts.typ)]; h.Put != nil {
		if err := h.Put(s, c, id, doc); err != nil {
			return err
		}
	}
	s.mu.Lock()
	s.st.ARM.Put(id, doc)
	s.mu.Unlock()
	return nil
}

// Children are the resources directly below id, of any type, as a GET returns them.
func (s *Service) Children(id arm.ID) []map[string]any {
	prefix := key(id) + "/"
	var out []map[string]any
	s.mu.Lock()
	for _, k := range core.SortedKeys(s.st.ARM) {
		if rest, ok := strings.CutPrefix(k, prefix); ok && strings.Count(rest, "/") == 1 {
			out = append(out, armres.Clone(s.st.ARM[k]))
		}
	}
	s.mu.Unlock()
	for _, r := range out {
		cid, _ := arm.ParseID(armres.Str(r, "id"))
		if ts := s.types[strings.ToLower(cid.Type())]; ts != nil {
			if h := hooks[strings.ToLower(ts.typ)]; h.View != nil {
				h.View(s, cid, r)
			}
		}
	}
	return out
}

// Count is the number of resources stored below id, children and deeper.
func (s *Service) Count(id arm.ID) int {
	prefix := key(id) + "/"
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for k := range s.st.ARM {
		if strings.HasPrefix(k, prefix) {
			n++
		}
	}
	return n
}

func now() string { return time.Now().UTC().Format(time.RFC3339) }
