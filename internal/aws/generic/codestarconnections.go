package generic

// CodeStar Connections: a connection is AVAILABLE at once. Real AWS starts it as PENDING until someone completes
// the handshake in the console, which cannot happen here.
func init() {
	Override("codestar-connections", "ListConnections", func(c *Call) (map[string]any, error) {
		out, err := c.Default()
		if err != nil {
			return nil, err
		}
		var keep []any
		for _, e := range asList(out["Connections"]) {
			m := s2Map(e)
			if f := str(c.In["ProviderTypeFilter"]); f != "" && str(m["ProviderType"]) != f {
				continue
			}
			if f := str(c.In["HostArnFilter"]); f != "" && str(m["HostArn"]) != f {
				continue
			}
			keep = append(keep, e)
		}
		out["Connections"] = keep
		return out, nil
	})
}
