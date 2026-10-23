package generic

// Network Manager: DescribeGlobalNetworks answers an empty list for an id that does not exist, as AWS does
// (the Terraform provider waits for a deleted network to show up empty).
func init() {
	Override("networkmanager", "DescribeGlobalNetworks", func(c *Call) (map[string]any, error) {
		out, err := c.Default()
		if err != nil {
			return map[string]any{"GlobalNetworks": []any{}}, nil
		}
		return out, nil
	})
}
