package generic

// MediaLive: channels, inputs and input security groups are stored and read back; no video is processed.
// A channel is IDLE and an input DETACHED a moment after the create call, as in AWS (Terraform waits for both).
// A delete leaves the resource in state DELETED, which Terraform waits for; it can still be read.
func init() {
	for typ, states := range map[string][]string{"Channel": {"CREATING", "IDLE"}, "Input": {"CREATING", "DETACHED"}, "InputSecurityGroup": {"IDLE"}} {
		Derive("medialive", typ, func(r *Resource) map[string]any {
			if r.Data["_deleted"] != nil {
				return map[string]any{"State": "DELETED"}
			}
			return map[string]any{"State": Step(r, "state", states...)}
		})
	}
	for op, typ := range map[string]string{"DeleteChannel": "Channel", "DeleteInput": "Input", "DeleteInputSecurityGroup": "InputSecurityGroup"} {
		Override("medialive", op, func(c *Call) (map[string]any, error) {
			r := c.resolve(c.m.types[norm(typ)])
			if r == nil {
				return nil, c.NotFound("%s not found", typ)
			}
			r = r.clone()
			r.Data["_deleted"] = true
			c.Put(r)
			return c.Fill(r), nil
		})
	}
}
