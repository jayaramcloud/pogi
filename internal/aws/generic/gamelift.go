package generic

import "slices"

// GameLift: fleets, builds, scripts, aliases and queues are stored and read back; no game server runs.
// A fleet is ACTIVE at once.
//
// The model reads a fleet with DescribeFleetAttributes (a plural name with no matching type), so the engine
// would look for a type "FleetAttribute"; this lists the fleets instead.
func init() {
	Override("gamelift", "DescribeFleetAttributes", func(c *Call) (map[string]any, error) {
		want, _ := c.In["FleetIds"].([]any)
		fleets := c.List("Fleet")
		is := func(w any, f *Resource) bool {
			return str(w) == f.ID || str(w) == f.Name || str(w) == f.ARN || str(w) == f.Key
		}
		for _, w := range want {
			if !slices.ContainsFunc(fleets, func(f *Resource) bool { return is(w, f) }) {
				return nil, c.NotFound("Fleet %s not found.", str(w))
			}
		}
		var items []any
		for _, f := range fleets {
			if len(want) == 0 || slices.ContainsFunc(want, func(w any) bool { return is(w, f) }) {
				items = append(items, c.fillItem(c.op.out.Members["FleetAttributes"].sh.Member.sh, f, "FleetAttributes"))
			}
		}
		return map[string]any{"FleetAttributes": items}, nil
	})
}
