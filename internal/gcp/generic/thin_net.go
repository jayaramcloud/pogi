package generic

// Hooks of the group B Thin APIs: Cloud Domains, App Engine and Cloud Quotas. The others
// (Network Intelligence Center, Cloud NGFW, Cloud Service Mesh, VMware Engine, Distributed Cloud,
// GKE Multi-Cloud, Backup for GKE, Cloud Workstations, Cluster Director, VM Manager and
// Infrastructure Manager) need none: the engine serves them as they are.

import (
	"slices"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/gcp/gapi"
)

// The fields these APIs document as required, for the ones that need no other hook.
func init() {
	apiHooks["networkmanagement"] = hooks{required: map[string][]string{"ConnectivityTest": {"source", "destination"}}}
	apiHooks["vmwareengine"] = hooks{required: map[string][]string{"VmwareEngineNetwork": {"type"}}}
	apiHooks["edgecontainer"] = hooks{required: map[string][]string{"Cluster": {"authorization", "networking", "fleet"}}}
	apiHooks["gkemulticloud"] = hooks{required: map[string][]string{"AzureClient": {"tenantId", "applicationId"}}}
	apiHooks["gkebackup"] = hooks{required: map[string][]string{"BackupPlan": {"cluster"}}}
	apiHooks["workstations"] = hooks{required: map[string][]string{"WorkstationCluster": {"network", "subnetwork"}}}
	apiHooks["osconfig"] = hooks{required: map[string][]string{"PatchDeployment": {"instanceFilter"}}}
	apiHooks["config"] = hooks{required: map[string][]string{"Deployment": {"serviceAccount"}}}
}

func init() {
	apiHooks["domains"] = hooks{
		methods: map[string]hook{
			// A registration is made by registering its domain: the name of the resource is the domain.
			"domains.projects.locations.registrations.register": func(c *call) (any, error) {
				reg, _ := c.body["registration"].(map[string]any)
				if reg == nil {
					return nil, gapi.Invalid("Missing required field %q in %s.", "registration", c.m.req)
				}
				domain, _ := reg["domainName"].(string)
				if !strings.Contains(domain, ".") || strings.ContainsAny(domain, "/ ") {
					return nil, gapi.Invalid("Invalid domain name %q.", domain)
				}
				if reg["contactSettings"] == nil {
					return nil, gapi.Invalid("Missing required field %q in Registration.", "contactSettings")
				}
				if c.body["yearlyPrice"] == nil {
					return nil, gapi.Invalid("Missing required field %q in %s.", "yearlyPrice", c.m.req)
				}
				m := *c.m
				m.req = "Registration"
				c.m = &m
				return c.put(clone(c.a.writable("Registration", reg)), c.name()+"/"+strings.ToLower(domain))
			},
		},
		created: map[string]func(*call, map[string]any){"Registration": func(c *call, r map[string]any) {
			r["name"] = c.name() + "/" + strings.ToLower(r["domainName"].(string))
			r["expireTime"] = time.Now().UTC().AddDate(1, 0, 0).Format("2006-01-02T15:04:05.000Z")
			r["supportedPrivacy"] = []any{"PUBLIC_CONTACT_DATA", "REDACTED_CONTACT_DATA", "PRIVATE_CONTACT_DATA"}
			ms := obj(r, "managementSettings")
			setDefault(ms, "renewalMethod", "AUTOMATIC_RENEWAL")
			setDefault(ms, "transferLockState", "LOCKED")
		}},
	}

	apiHooks["appengine"] = hooks{
		methods: map[string]hook{
			// An application is the project's one: apps/<project>, made by a POST with the project as its id.
			"appengine.apps.create": func(c *call) (any, error) {
				id, _ := c.body["id"].(string)
				for _, f := range []string{"id", "locationId"} {
					if v, _ := c.body[f].(string); v == "" {
						return nil, gapi.Invalid("Missing required field %q in %s.", f, c.m.req)
					}
				}
				app := clone(c.a.writable("Application", c.body))
				app["name"] = "apps/" + id
				setDefault(app, "servingStatus", "SERVING")
				setDefault(app, "authDomain", "gmail.com")
				setDefault(app, "databaseType", "CLOUD_DATASTORE_COMPATIBILITY")
				setDefault(app, "defaultCookieExpiration", "86400s")
				app["defaultHostname"] = id + ".appspot.com"
				app["defaultBucket"] = id + ".appspot.com"
				app["codeBucket"] = "staging." + id + ".appspot.com"
				app["gcrDomain"] = "us.gcr.io"
				setDefault(app, "serviceAccount", id+"@appspot.gserviceaccount.com")
				fs := obj(app, "featureSettings")
				setDefault(fs, "splitHealthChecks", true)
				setDefault(fs, "useContainerOptimizedOs", true)
				return c.put(app, "apps/"+id)
			},
		},
	}

	// Quota adjuster settings are one object of each project, folder and organization, with no id: it
	// is there from the start (off), and a PATCH changes it.
	settings := func(c *call) {
		if c.a.objs[c.name()] == nil {
			c.a.objs[c.name()] = map[string]any{"name": c.name(), "enablement": "DISABLED", "inherited": false}
		}
	}
	m := map[string]hook{}
	for _, root := range []string{"projects", "folders", "organizations"} {
		id := "cloudquotas." + root + ".locations.quotaAdjusterSettings."
		m[id+"getQuotaAdjusterSettings"] = func(c *call) (any, error) { settings(c); return c.get() }
		m[id+"updateQuotaAdjusterSettings"] = func(c *call) (any, error) {
			if e, ok := c.body["enablement"].(string); ok && !slices.Contains([]string{"ENABLED", "DISABLED"}, e) {
				return nil, gapi.Invalid("Invalid value at 'quota_adjuster_settings.enablement': %q", e)
			}
			settings(c)
			return c.update()
		}
	}
	apiHooks["cloudquotas"] = hooks{
		methods:  m,
		required: map[string][]string{"QuotaPreference": {"service", "quotaId", "quotaConfig"}},
		created: map[string]func(*call, map[string]any){"QuotaPreference": func(_ *call, p map[string]any) {
			// a request is granted as asked, at once
			if qc, _ := p["quotaConfig"].(map[string]any); qc != nil {
				setDefault(qc, "grantedValue", qc["preferredValue"])
			}
		}},
	}
}
