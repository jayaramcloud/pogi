package generic

import (
	"maps"
	"slices"
	"strings"

	"github.com/nrndev/pogi/internal/gcp/gapi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// defaultBillingAccount is the one billing account pogi has. Projects link to it and unlink from it.
const defaultBillingAccount = "billingAccounts/000000-000000-000000"

func init() {
	oneAccount := func(*call) (any, error) {
		return nil, status.Errorf(codes.Unimplemented, "pogi has one billing account, %s, and does not create others", defaultBillingAccount)
	}
	apiHooks["cloudbilling"] = hooks{
		seed: map[string]map[string]any{defaultBillingAccount: {"name": defaultBillingAccount,
			"displayName": "My Billing Account", "open": true, "masterBillingAccount": "", "currencyCode": "USD"}},
		methods: map[string]hook{
			"cloudbilling.billingAccounts.create":               oneAccount,
			"cloudbilling.billingAccounts.subAccounts.create":   oneAccount,
			"cloudbilling.organizations.billingAccounts.create": oneAccount,

			// A project with no link has no stored info: it starts with no billing account, as a new
			// Google Cloud project does.
			"cloudbilling.projects.getBillingInfo": func(c *call) (any, error) {
				if info := c.a.objs[c.name()]; info != nil {
					return clone(info), nil
				}
				return map[string]any{"name": c.name(), "projectId": c.segs[1]}, nil
			},
			// An empty billingAccountName unlinks the project.
			"cloudbilling.projects.updateBillingInfo": func(c *call) (any, error) {
				v := c.body["billingAccountName"]
				acct, ok := v.(string)
				if v != nil && !ok {
					return nil, gapi.Invalid("Invalid billingAccountName: want a string, got %v.", v)
				}
				if acct != "" && !strings.HasPrefix(acct, "billingAccounts/") {
					return nil, gapi.Invalid("Invalid billingAccountName %q: want billingAccounts/<id>.", acct)
				}
				if acct != "" && c.a.objs[acct] == nil {
					return nil, gapi.NotFound("Billing account '%s' was not found", acct)
				}
				info := map[string]any{"name": c.name(), "projectId": c.segs[1]}
				if acct == "" {
					delete(c.a.objs, c.name())
					return info, nil
				}
				info["billingAccountName"], info["billingEnabled"] = acct, true
				c.a.objs[c.name()] = info
				return clone(info), nil
			},
			// Every permission asked for is granted, as Resource Manager does. Terraform asks before it
			// links a project.
			"cloudbilling.billingAccounts.testIamPermissions": func(c *call) (any, error) {
				if c.a.objs[c.name()] == nil {
					return nil, c.notFound()
				}
				perms, ok := c.body["permissions"].([]any)
				if c.body["permissions"] != nil && !ok {
					return nil, gapi.Invalid("Invalid permissions: want a list of strings.")
				}
				return map[string]any{"permissions": perms}, nil
			},
			"cloudbilling.billingAccounts.projects.list": func(c *call) (any, error) {
				acct := strings.Join(c.segs[:2], "/")
				if c.a.objs[acct] == nil {
					return nil, gapi.NotFound("The resource '%s' was not found", acct)
				}
				var items []map[string]any
				for _, k := range slices.Sorted(maps.Keys(c.a.objs)) {
					if strings.HasPrefix(k, "projects/") && c.a.objs[k]["billingAccountName"] == acct {
						items = append(items, c.a.objs[k])
					}
				}
				return c.page(items)
			},
		},
	}
}
