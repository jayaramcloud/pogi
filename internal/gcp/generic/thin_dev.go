package generic

// Hooks of the group A Thin APIs: Cloud Build, Cloud Deploy, API Keys,
// Service Directory, Binary Authorization, Container Analysis, Essential Contacts, Organization
// Policy and Cloud Asset Inventory. Service Directory and Cloud Deploy need none: the engine
// serves them as they are.

import (
	"cmp"
	"crypto/sha1"
	"encoding/hex"
	"strings"

	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/gcp/gapi"
)

// unwrap turns a request of the form {<field>: <resource>, updateMask: ...} (Cloud Asset's
// CreateFeedRequest and UpdateFeedRequest) into the call
// the engine knows: a body that is the resource, and the mask in the query.
func unwrap(c *call, field string) error {
	inner, _ := c.body[field].(map[string]any)
	if inner == nil {
		return gapi.Invalid("Missing required field %q in %s.", field, c.m.req)
	}
	m := *c.m
	m.req = c.a.schema(c.m.req).Properties[field].Ref
	c.m = &m
	c.q = cloneQuery(c.q)
	if mask, _ := c.body["updateMask"].(string); mask != "" {
		c.q.Set("updateMask", mask)
	}
	c.body = inner
	return nil
}

func cloneQuery(q map[string][]string) map[string][]string {
	out := map[string][]string{}
	for k, v := range q {
		out[k] = append([]string(nil), v...)
	}
	return out
}

func wrapped(field string, do func(c *call) (any, error)) hook {
	return func(c *call) (any, error) {
		if err := unwrap(c, field); err != nil {
			return nil, err
		}
		return do(c)
	}
}

// kindOf sets "kind" from the one of these fields the Note or Occurrence holds.
func kindOf(obj map[string]any) {
	for _, k := range []string{"vulnerability", "build", "image", "package", "deployment", "discovery", "attestation", "upgrade", "compliance", "dsseAttestation", "sbomReference"} {
		if obj[k] != nil && obj["kind"] == nil {
			obj["kind"] = strings.ToUpper(strings.NewReplacer("dsseAttestation", "DSSE_ATTESTATION", "sbomReference", "SBOM_REFERENCE").Replace(k))
		}
	}
}

func uuid() string { return core.UUID() }

func init() {
	apiHooks["cloudbuild"] = hooks{
		newID:   map[string]func() string{"BuildTrigger": uuid},
		ownName: map[string]bool{"BuildTrigger": true}, // a trigger's name is its label; its path has the id
		created: map[string]func(*call, map[string]any){"BuildTrigger": func(c *call, t map[string]any) {
			if len(c.segs) == 3 { // projects/p/triggers
				t["resourceName"] = "projects/" + c.segs[1] + "/locations/global/triggers/" + t["id"].(string)
			} else {
				t["resourceName"] = c.name() + "/" + t["id"].(string)
			}
		}},
	}

	apiHooks["apikeys"] = hooks{
		methods: map[string]hook{
			// the key's secret is made from its uid: the same on every read, and it opens nothing
			"apikeys.projects.locations.keys.getKeyString": func(c *call) (any, error) {
				name := strings.TrimSuffix(c.name(), "/keyString")
				key := c.a.objs[name]
				if key == nil {
					return nil, gapi.NotFound("The resource '%s' was not found", name)
				}
				sum := sha1.Sum([]byte(key["uid"].(string)))
				return map[string]any{"keyString": "AIzaSy" + hex.EncodeToString(sum[:])[:33]}, nil
			},
		},
	}

	apiHooks["binaryauthorization"] = hooks{
		required: map[string][]string{"Attestor": {"userOwnedGrafeasNote"}},
		methods: map[string]hook{
			// a project's policy exists from the start: it admits everything until it is replaced
			"binaryauthorization.projects.getPolicy": func(c *call) (any, error) {
				if p := c.a.objs[c.name()]; p != nil {
					return clone(p), nil
				}
				return map[string]any{"name": c.name(), "defaultAdmissionRule": map[string]any{
					"evaluationMode": "ALWAYS_ALLOW", "enforcementMode": "ENFORCED_BLOCK_AND_AUDIT_LOG"}}, nil
			},
			"binaryauthorization.projects.updatePolicy": func(c *call) (any, error) {
				p := clone(c.a.writable("Policy", c.body))
				if p["defaultAdmissionRule"] == nil {
					return nil, gapi.Invalid("Missing required field %q in Policy.", "defaultAdmissionRule")
				}
				p["name"], p["updateTime"] = c.name(), now()
				c.a.objs[c.name()] = p
				return clone(p), nil
			},
		},
	}

	kind := func(_ *call, obj map[string]any) { kindOf(obj) }
	apiHooks["containeranalysis"] = hooks{
		required: map[string][]string{"Occurrence": {"resourceUri", "noteName"}},
		newID:    map[string]func() string{"Occurrence": uuid},
		created:  map[string]func(*call, map[string]any){"Note": kind, "Occurrence": kind},
	}

	apiHooks["essentialcontacts"] = hooks{
		required: map[string][]string{"GoogleCloudEssentialcontactsV1Contact": {"email", "notificationCategorySubscriptions", "languageTag"}},
		newID:    map[string]func() string{"GoogleCloudEssentialcontactsV1Contact": new(api).numericID},
	}

	apiHooks["orgpolicy"] = hooks{
		check: map[string]func(*call, map[string]any) error{"GoogleCloudOrgpolicyV2Policy": func(_ *call, p map[string]any) error {
			// Terraform reads the spec's etag and update time back
			if spec, _ := p["spec"].(map[string]any); spec != nil {
				spec["updateTime"] = now()
				spec["etag"] = core.RandHex(8)
			}
			return nil
		}},
	}

	apiHooks["cloudasset"] = hooks{
		required: map[string][]string{"Feed": {"feedOutputConfig"}},
		methods: map[string]hook{
			"cloudasset.feeds.create": func(c *call) (any, error) {
				id, _ := c.body["feedId"].(string)
				id = cmp.Or(id, c.q.Get("feedId")) // the Terraform provider puts it in the query
				if id == "" {
					return nil, gapi.Invalid("Missing required field %q in %s.", "feedId", c.m.req)
				}
				if err := unwrap(c, "feed"); err != nil {
					return nil, err
				}
				c.body["name"] = c.name() + "/" + id
				return c.create()
			},
			"cloudasset.feeds.patch": wrapped("feed", func(c *call) (any, error) { return c.update() }),
		},
	}
}
