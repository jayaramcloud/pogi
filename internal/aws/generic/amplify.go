package generic

import "strings"

// Amplify: an app's ARN is apps/<id>, a branch's apps/<id>/branches/<name>, and an app has a default domain.
// Nothing is built or hosted.
func init() {
	// every operation on an app's children needs the app
	for _, op := range []string{"CreateBranch", "ListBranches", "CreateDomainAssociation", "ListDomainAssociations",
		"CreateBackendEnvironment", "ListBackendEnvironments", "CreateWebhook", "ListWebhooks"} {
		Override("amplify", op, func(c *Call) (map[string]any, error) {
			if c.Get("App", str(c.In["appId"])) == nil {
				return nil, c.Err("NotFoundException", "App %s not found.", str(c.In["appId"]))
			}
			return c.Default()
		})
	}
	arnTemplates["amplify/App"] = "apps/{appId}"
	arnTemplates["amplify/Branch"] = "apps/{appId}/branches/{branchName}"
	Override("amplify", "CreateApp", func(c *Call) (map[string]any, error) {
		if _, err := c.Default(); err != nil {
			return nil, err
		}
		id := c.Res.ID
		s2Set(c, nil, map[string]any{"DefaultDomain": strings.ToLower(id) + ".amplifyapp.com"})
		return c.Fill(c.Res), nil
	})
}
