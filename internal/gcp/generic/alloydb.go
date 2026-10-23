package generic

func init() {
	apiHooks["alloydb"] = hooks{
		noEtag: []string{"Cluster"}, // the provider takes etag as an input: a value the server adds is drift
		created: map[string]func(*call, map[string]any){"Cluster": func(_ *call, cl map[string]any) {
			setDefault(cl, "clusterType", "PRIMARY") // Terraform reads it back; without it the next plan wants a change
		}},
	}
}
