package generic

import "github.com/nrndev/pogi/internal/gcp/gapi"

func init() {
	apiHooks["privateca"] = hooks{
		required: map[string][]string{
			"CaPool":               {"tier"},
			"CertificateAuthority": {"config", "keySpec", "lifetime", "type"},
		},
		created: map[string]func(*call, map[string]any){
			"CertificateAuthority": func(_ *call, ca map[string]any) { ca["state"] = "STAGED" },
		},
		methods: map[string]hook{
			"privateca.projects.locations.caPools.certificateAuthorities.enable":  caState("ENABLED"),
			"privateca.projects.locations.caPools.certificateAuthorities.disable": caState("DISABLED"),
		},
	}
}

// caState moves a certificate authority to a state. A real CA issues from ENABLED only; here the
// state is stored and nothing is signed.
func caState(state string) hook {
	return func(c *call) (any, error) {
		ca := c.a.objs[c.name()]
		if ca == nil {
			return nil, gapi.NotFound("The resource '%s' was not found", c.name())
		}
		ca["state"] = state
		ca["updateTime"] = now()
		return c.lro(clone(ca), "CertificateAuthority", "update", c.name()), nil
	}
}
