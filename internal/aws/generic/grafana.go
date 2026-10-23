package generic

// Managed Grafana: the engine keeps what a create call sent, but a workspace reads back under other member names:
// workspaceName as name, authenticationProviders as authentication.providers. A workspace has an endpoint host.
// No Grafana runs behind it.
func init() {
	arnTemplates["grafana/Workspace"] = "/workspaces/{workspaceId}" // real ARNs have the slash right after the account
	Derive("grafana", "Workspace", func(r *Resource) map[string]any {
		out := map[string]any{"endpoint": r.ID + ".grafana-workspace." + r.Region + ".amazonaws.com"}
		if n := str(r.Data["workspaceName"]); n != "" {
			out["name"] = n
		}
		if p := asList(r.Data["authenticationProviders"]); len(p) > 0 {
			out["authentication"] = map[string]any{"providers": p}
		}
		return out
	})
}
