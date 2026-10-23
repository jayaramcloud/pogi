package generic

// Hooks of the group A (security and identity) Thin APIs: VPC Service Controls, Managed Microsoft AD,
// Assured Workloads, Chrome Enterprise Premium (beyondcorp), Google Security Operations, Cloud
// Identity, Cloud IDS, Compliance Manager, Sensitive Data Protection, Identity-Aware Proxy,
// Privileged Access Manager, reCAPTCHA, Security Command Center and Public CA.
// Chrome Enterprise Premium and Cloud IDS need none: the engine serves them as they are.

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/gcp/gapi"
)

// dlpCreate serves the create methods of Sensitive Data Protection: the request wraps the resource
// ({inspectTemplate: {...}, templateId: ...}), and the id is optional (the server makes one).
func dlpCreate(field, idField, prefix string) hook {
	return func(c *call) (any, error) {
		id, _ := c.body[idField].(string)
		if err := unwrap(c, field); err != nil {
			return nil, err
		}
		if id == "" {
			id = prefix + uuid()
		}
		c.body["name"] = c.name() + "/" + id
		return c.create()
	}
}

func init() {
	apiHooks["accesscontextmanager"] = hooks{
		required: map[string][]string{"AccessPolicy": {"parent", "title"}},
		newID:    map[string]func() string{"AccessPolicy": new(api).numericID},
	}

	apiHooks["managedidentities"] = hooks{
		required: map[string][]string{"Domain": {"locations", "reservedIpRange"}},
		created: map[string]func(*call, map[string]any){"Domain": func(c *call, d map[string]any) {
			d["fqdn"] = d["name"].(string)[strings.LastIndex(d["name"].(string), "/")+1:]
		}},
		methods: map[string]hook{
			// the domain's name is a query parameter (domainName), not an id
			"managedidentities.projects.locations.global.domains.create": func(c *call) (any, error) {
				id := c.q.Get("domainName")
				if id == "" {
					return nil, gapi.Invalid("The query parameter domainName is required.")
				}
				c.body["name"] = c.name() + "/" + id
				return c.create()
			},
		},
	}

	apiHooks["assuredworkloads"] = hooks{
		required: map[string][]string{"GoogleCloudAssuredworkloadsV1Workload": {"displayName", "complianceRegime"}},
		newID:    map[string]func() string{"GoogleCloudAssuredworkloadsV1Workload": new(api).numericID},
	}

	// A Chronicle instance is made when the customer is onboarded, not through the API.
	apiHooks["chronicle"] = hooks{
		implicit: func(name string) bool { return strings.Count(name, "/") == 5 && strings.Contains(name, "/instances/") },
		required: map[string][]string{"ReferenceList": {"description", "entries", "syntaxType"}},
	}

	apiHooks["cloudidentity"] = hooks{
		required: map[string][]string{"Group": {"groupKey", "parent", "labels"}},
		newID:    map[string]func() string{"Group": func() string { return "0" + uuid()[:7] }},
		methods: map[string]hook{
			// Cloud Identity's create answers with a finished operation that has no name of its own: the
			// Terraform provider takes the group's name from its "response"
			"cloudidentity.groups.create": func(c *call) (any, error) {
				op, err := c.create()
				if m, ok := op.(map[string]any); ok {
					delete(m, "name")
				}
				return op, err
			},
		},
	}

	apiHooks["cloudsecuritycompliance"] = hooks{}

	dlp := hooks{methods: map[string]hook{}}
	for _, parent := range []string{"projects", "projects.locations", "organizations", "organizations.locations"} {
		for _, r := range []struct{ res, field, idField, prefix string }{
			{"inspectTemplates", "inspectTemplate", "templateId", "i-"},
			{"deidentifyTemplates", "deidentifyTemplate", "templateId", "d-"},
			{"jobTriggers", "jobTrigger", "triggerId", "i-"},
		} {
			dlp.methods[fmt.Sprintf("dlp.%s.%s.create", parent, r.res)] = dlpCreate(r.field, r.idField, r.prefix)
			dlp.methods[fmt.Sprintf("dlp.%s.%s.patch", parent, r.res)] = wrapped(r.field, func(c *call) (any, error) { return c.update() })
		}
	}
	apiHooks["dlp"] = dlp

	apiHooks["iap"] = hooks{
		// a tunnel location (projects/p/iap_tunnel/locations/l) needs no create
		implicit: func(name string) bool {
			return strings.Contains(name, "/iap_tunnel/locations/") && strings.Count(name, "/") == 4
		},
	}

	pamDelete := map[string]hook{}
	for _, parent := range []string{"projects", "folders", "organizations"} {
		// the operation of a delete holds the entitlement that went (the Go client reads it as one)
		pamDelete["privilegedaccessmanager."+parent+".locations.entitlements.delete"] = func(c *call) (any, error) {
			gone := c.a.objs[c.name()]
			if gone == nil {
				return nil, c.notFound()
			}
			gone = clone(gone)
			op, err := c.delete()
			if m, ok := op.(map[string]any); ok && err == nil {
				gone["@type"] = "type.googleapis.com/google.cloud.privilegedaccessmanager.v1.Entitlement"
				m["response"] = gone
			}
			return op, err
		}
	}
	apiHooks["privilegedaccessmanager"] = hooks{
		methods:  pamDelete,
		required: map[string][]string{"Entitlement": {"maxRequestDuration", "eligibleUsers", "privilegedAccess", "requesterJustificationConfig"}},
		created:  map[string]func(*call, map[string]any){"Entitlement": func(_ *call, e map[string]any) { e["state"] = "AVAILABLE" }},
	}

	apiHooks["recaptchaenterprise"] = hooks{
		required: map[string][]string{"GoogleCloudRecaptchaenterpriseV1Key": {"displayName"}},
		newID:    map[string]func() string{"GoogleCloudRecaptchaenterpriseV1Key": func() string { return "6L" + core.RandHex(19) }},
	}

	apiHooks["securitycenter"] = hooks{
		required: map[string][]string{"Source": {"displayName"}, "NotificationConfig": {"pubsubTopic"}},
		newID:    map[string]func() string{"Source": new(api).numericID},
	}

	// An external account key is shown once, at create: its id and a MAC key (random, and it opens nothing).
	apiHooks["publicca"] = hooks{
		newID: map[string]func() string{"ExternalAccountKey": uuid},
		created: map[string]func(*call, map[string]any){"ExternalAccountKey": func(_ *call, k map[string]any) {
			k["keyId"] = k["name"].(string)[strings.LastIndex(k["name"].(string), "/")+1:]
			mac := make([]byte, 32)
			rand.Read(mac)
			k["b64MacKey"] = base64.URLEncoding.EncodeToString(mac)
		}},
	}
}
