package generic

func init() {
	apiHooks["file"] = hooks{
		required: map[string][]string{"Instance": {"tier", "fileShares", "networks"}},
		created: map[string]func(*call, map[string]any){"Instance": func(_ *call, in map[string]any) {
			// where the share is mounted; the address is made up, nothing listens on it
			nets, _ := in["networks"].([]any)
			for _, n := range nets {
				if m, ok := n.(map[string]any); ok && m["ipAddresses"] == nil {
					m["ipAddresses"] = []any{"10.0.0.2"}
				}
			}
		}},
	}
}
