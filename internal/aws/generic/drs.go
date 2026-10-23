package generic

// Elastic Disaster Recovery: a replication configuration template has two tag members, `tags` (the template's)
// and `stagingAreaTags` (put on the staging area's resources). The engine takes every member ending in "tags"
// for the resource's own tags, so these two keep the staging tags as plain data.
func init() {
	keepStaging := func(c *Call) (map[string]any, error) {
		staging, has := c.In["stagingAreaTags"]
		delete(c.In, "stagingAreaTags")
		out, err := c.Default()
		if err != nil || !has {
			return out, err
		}
		r := c.Get("ReplicationConfigurationTemplate", str(out["replicationConfigurationTemplateID"]))
		if r == nil {
			return out, nil
		}
		r.Data["stagingAreaTags"] = staging
		c.Put(r)
		return c.Fill(r), nil
	}
	Override("drs", "CreateReplicationConfigurationTemplate", keepStaging)
	Override("drs", "UpdateReplicationConfigurationTemplate", keepStaging)
}
