package generic

// AppFlow: a connector a user registers is always a custom one, so it is keyed as type CustomConnector.
// The engine would key it by its connector type and then mint a value that is not in the type's enum.
// ponytail: one registered connector per account and region; key by label if that ever matters.
func init() {
	Override("appflow", "RegisterConnector", func(c *Call) (map[string]any, error) {
		c.In["connectorType"] = "CustomConnector"
		return c.Default()
	})
	Override("appflow", "UnregisterConnector", func(c *Call) (map[string]any, error) {
		r := c.Get("Connector", "CustomConnector")
		if r == nil || str(r.Data["connectorLabel"]) != "" && str(r.Data["connectorLabel"]) != str(c.In["connectorLabel"]) {
			return nil, c.NotFound("Connector %s not found", str(c.In["connectorLabel"]))
		}
		c.In["connectorType"] = "CustomConnector"
		return c.Default()
	})
	// DescribeConnectorProfiles lists a profile's properties at the top level, not inside the config it was made
	// with (the provider reads them back and crashes on nil). The credentials are never returned.
	for _, op := range []string{"CreateConnectorProfile", "UpdateConnectorProfile"} {
		Hook("appflow", op, func(c *Call) {
			if p := s2Map(s2Map(c.In["connectorProfileConfig"])["connectorProfileProperties"]); len(p) > 0 {
				s2Set(c, nil, map[string]any{"connectorProfileProperties": p})
			}
		})
	}
}
