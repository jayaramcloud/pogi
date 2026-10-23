package generic

// Resilience Hub: apps and resiliency policies are stored and read back; no assessment runs.
// The policy's own "policy" member (the failure targets) shares its name with the output member that holds the
// whole policy, so that output member is filled from the stored resource here.
func init() {
	for _, op := range []string{"CreateResiliencyPolicy", "DescribeResiliencyPolicy", "UpdateResiliencyPolicy"} {
		Override("resiliencehub", op, func(c *Call) (map[string]any, error) {
			out, err := c.Default()
			if err != nil {
				return nil, err
			}
			if c.Res != nil {
				for _, n := range c.op.out.names {
					if norm(n) == "policy" {
						out[n] = c.fillItem(c.op.out.Members[n].sh, c.Res, n)
					}
				}
			}
			return out, nil
		})
	}
}
