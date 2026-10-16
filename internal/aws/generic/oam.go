package generic

import "strings"

// Observability Access Manager: a link names its sink and has a label made from its template.
func init() {
	Override("oam", "CreateLink", func(c *Call) (map[string]any, error) {
		sink := s2Sink(c, str(c.In["SinkIdentifier"]))
		if sink == nil {
			return nil, c.NotFound("Sink %s not found", str(c.In["SinkIdentifier"]))
		}
		out, err := c.Default()
		if err != nil {
			return nil, err
		}
		label := strings.NewReplacer("$AccountName", c.Account, "$AccountEmail", c.Account+"@pogi.localhost",
			"$AccountEmailNoDomain", c.Account, "$AccountId", c.Account).Replace(str(c.In["LabelTemplate"]))
		s2Set(c, out, map[string]any{"Label": label, "SinkArn": sink.ARN})
		return out, nil
	})
}

// s2Sink finds a sink by ARN or id.
func s2Sink(c *Call, id string) *Resource {
	for _, r := range c.List("Sink") {
		if r.ARN == id || r.ID == id || r.Name == id {
			return r
		}
	}
	return nil
}
