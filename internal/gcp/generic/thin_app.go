package generic

// Hooks of the group D Thin APIs (Apigee to Firebase Data Connect). Most of them need none: the
// engine serves them as they are.

import (
	"fmt"
	"strings"

	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/gcp/gapi"
)

// createAs makes a resource under the collection of the request with the id the hook picked, for the
// APIs whose documents don't say where the id is.
func (c *call) createAs(id string, set map[string]any) (any, error) {
	name := c.name() + "/" + id
	if err := c.checkParent(c.segs[:len(c.segs)-1]); err != nil {
		return nil, err
	}
	if c.a.objs[name] != nil {
		return nil, gapi.Exists("Resource '%s' already exists", name)
	}
	obj := clone(c.a.writable(c.m.req, c.body))
	c.fill(obj, c.m.req, name)
	for k, v := range set {
		obj[k] = v
	}
	c.a.objs[name] = obj
	return c.answer(obj, c.m.req, "create", name)
}

func init() {
	// An organization is named after its project: POST /v1/organizations?parent=projects/<p>.
	apiHooks["apigee"] = hooks{
		ownOrg: true,
		// these are named by the short name the client sends, not by their path
		ownName: map[string]bool{"GoogleCloudApigeeV1Environment": true, "GoogleCloudApigeeV1EnvironmentGroup": true,
			"GoogleCloudApigeeV1ApiProduct": true, "GoogleCloudApigeeV1AppGroup": true, "GoogleCloudApigeeV1TargetServer": true,
			"GoogleCloudApigeeV1Keystore": true, "GoogleCloudApigeeV1KeyValueMap": true, "GoogleCloudApigeeV1Space": true,
			"GoogleCloudApigeeV1Instance": true},
		methods: map[string]hook{
			"apigee.organizations.create": func(c *call) (any, error) {
				id := strings.TrimPrefix(c.q.Get("parent"), "projects/")
				if id == "" || id == c.q.Get("parent") {
					return nil, gapi.Invalid("Invalid parent %q: want projects/<project id>.", c.q.Get("parent"))
				}
				return c.createAs(id, map[string]any{"name": id, "projectId": id}) // "name" is output only in the document, but it is the project's id
			},
		},
	}

	// A developer is named by an email, which the body holds but the document doesn't name as the id.
	apiHooks["apigee"].methods["apigee.organizations.developers.create"] = func(c *call) (any, error) {
		email, _ := c.body["email"].(string)
		if email == "" {
			return nil, gapi.Invalid("Missing required field %q in %s.", "email", c.m.req)
		}
		return c.createAs(email, map[string]any{"developerId": core.UUID()})
	}

	// A budget's id is made by the server.
	apiHooks["billingbudgets"] = hooks{newID: map[string]func() string{"GoogleCloudBillingBudgetsV1Budget": uuid}}

	// A debug token or a resource policy goes under an app or a service that the Firebase and Service
	// Usage APIs own: pogi doesn't serve them, so the parent is taken as given.
	assume := func(next hook) hook {
		return func(c *call) (any, error) {
			if parent := strings.Join(c.segs[:len(c.segs)-1], "/"); c.a.objs[parent] == nil {
				c.a.objs[parent] = map[string]any{}
			}
			return next(c)
		}
	}
	apiHooks["firebaseappcheck"] = hooks{newID: map[string]func() string{"GoogleFirebaseAppcheckV1DebugToken": uuid, "GoogleFirebaseAppcheckV1ResourcePolicy": uuid}, methods: map[string]hook{
		"firebaseappcheck.projects.apps.debugTokens.create":          assume((*call).create),
		"firebaseappcheck.projects.services.resourcePolicies.create": assume((*call).create),
	}}

	// the provider takes the connection's etag as an input: a value the server adds is drift
	apiHooks["developerconnect"] = hooks{noEtag: []string{"Connection"}}

	apiHooks["servicemanagement"] = hooks{methods: map[string]hook{
		"servicemanagement.services.configs.submit": func(c *call) (any, error) {
			svc := strings.Join(c.segs[:2], "/")
			if c.a.objs[svc] == nil {
				return nil, gapi.NotFound("Service '%s' not found or permission denied for the requested operation.", c.segs[1])
			}
			src, _ := c.body["configSource"].(map[string]any)
			if files, _ := src["files"].([]any); len(files) == 0 {
				return nil, gapi.Invalid("Missing required field %q in %s.", "configSource.files", c.m.req)
			}
			// pogi does not read the OpenAPI or gRPC files: the config is the service's name and its endpoint.
			cfg := map[string]any{"name": c.segs[1], "title": c.segs[1], "configVersion": 3,
				"producerProjectId": c.a.objs[svc]["producerProjectId"], "endpoints": []any{map[string]any{"name": c.segs[1]}}}
			if id, _ := src["id"].(string); id != "" {
				cfg["id"] = id
			} else {
				cfg["id"] = c.nextID(svc + "/configs")
			}
			if v, _ := c.body["validateOnly"].(bool); !v {
				c.a.objs[svc+"/configs/"+cfg["id"].(string)] = cfg
			}
			return c.lro(map[string]any{"serviceConfig": cfg}, "SubmitConfigSourceResponse", "submit", svc), nil
		},
		"servicemanagement.services.rollouts.create": func(c *call) (any, error) {
			svc := strings.Join(c.segs[:2], "/")
			if c.a.objs[svc] == nil {
				return nil, gapi.NotFound("Service '%s' not found or permission denied for the requested operation.", c.segs[1])
			}
			pcts, _ := lookupMap(c.body, "trafficPercentStrategy.percentages")
			if len(pcts) == 0 {
				return nil, gapi.Invalid("Missing required field %q in %s.", "trafficPercentStrategy", c.m.req)
			}
			for id := range pcts {
				if c.a.objs[svc+"/configs/"+id] == nil {
					return nil, gapi.NotFound("Service config '%s' of service '%s' not found.", id, c.segs[1])
				}
			}
			id := c.nextID(svc + "/rollouts")
			return c.createAs(id, map[string]any{"rolloutId": id, "serviceName": c.segs[1], "status": "SUCCESS", "createdBy": "pogi"})
		},
		// the config a rollout serves: the one asked for, or the newest
		"servicemanagement.services.getConfig": func(c *call) (any, error) {
			svc := strings.Join(c.segs[:2], "/")
			id := c.q.Get("configId")
			if id == "" {
				for k := range c.a.objs {
					if rest, ok := strings.CutPrefix(k, svc+"/configs/"); ok && rest > id && !strings.Contains(rest, "/") {
						id = rest
					}
				}
			}
			if cfg := c.a.objs[svc+"/configs/"+id]; cfg != nil && id != "" {
				return clone(cfg), nil
			}
			return nil, gapi.NotFound("Service config '%s' of service '%s' not found.", id, c.segs[1])
		},
	}}

	apiHooks["licensemanager"] = hooks{created: map[string]func(*call, map[string]any){"Configuration": func(_ *call, o map[string]any) {
		setDefault(o, "state", "STATE_ACTIVE")
	}}}

	// the provider waits for a connection's status to be ACTIVE
	apiHooks["connectors"] = hooks{created: map[string]func(*call, map[string]any){"Connection": func(_ *call, o map[string]any) {
		setDefault(o, "status", map[string]any{"state": "ACTIVE"})
	}}}

	apiHooks["integrations"] = hooks{
		newID: map[string]func() string{"GoogleCloudIntegrationsV1alphaAuthConfig": uuid, "GoogleCloudIntegrationsV1alphaCertificate": uuid},
	}
}

// nextID is the id of the next config or rollout of a service: <date>r<n>, as the real API makes them.
func (c *call) nextID(prefix string) string {
	n := 0
	for k := range c.a.objs {
		if rest, ok := strings.CutPrefix(k, prefix+"/"); ok && !strings.Contains(rest, "/") {
			n++
		}
	}
	return fmt.Sprintf("%sr%02d", now()[:10], n)
}

// lookupMap reads a dotted path that ends in an object.
func lookupMap(m map[string]any, path string) (map[string]any, bool) {
	v, ok := lookup(m, path)
	out, isMap := v.(map[string]any)
	return out, ok && isMap
}
