package generic

import (
	"strings"

	"github.com/nrndev/pogi/internal/core"
)

// DataZone: domains and projects are stored and read back; nothing is cataloged or governed. A domain is
// AVAILABLE at once. Like AWS, a domain and a project get an id of their own (dzd_..., not the name), and a
// domain without single sign-on reports it as DISABLED.
func init() {
	Override("datazone", "CreateDomain", func(c *Call) (map[string]any, error) {
		c.In["Identifier"] = "dzd_" + strings.ReplaceAll(core.UUID(), "-", "")[:14]
		return c.Default()
	})
	Override("datazone", "CreateProject", func(c *Call) (map[string]any, error) {
		c.In["Identifier"] = strings.ReplaceAll(core.UUID(), "-", "")[:14]
		return c.Default()
	})
	Derive("datazone", "Domain", func(r *Resource) map[string]any {
		out := map[string]any{"PortalUrl": "https://" + r.Name + ".datazone.us-east-1.on.aws"}
		out["RootDomainUnitId"] = "r" + strings.TrimPrefix(r.Name, "dzd_")
		if r.Data["DomainVersion"] == nil {
			out["DomainVersion"] = "V1"
		}
		if r.Data["SingleSignOn"] == nil {
			out["SingleSignOn"] = map[string]any{"Type": "DISABLED", "UserAssignment": "MANUAL"}
		}
		return out
	})
}
