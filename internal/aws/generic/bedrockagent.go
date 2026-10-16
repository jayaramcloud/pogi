package generic

import "time"

// Bedrock Agents (control plane): states the engine cannot guess from the model.
func init() {
	aiSeed("bedrock-agent", "CreateAgent", func(c *Call, r *Resource) {
		r.Data["agentStatus"], r.Data["agentVersion"] = "NOT_PREPARED", "DRAFT"
		if r.Data["promptOverrideConfiguration"] == nil { // Terraform reads this member of every agent
			r.Data["promptOverrideConfiguration"] = map[string]any{"promptConfigurations": []any{}}
		}
	})
	// An agent must be prepared again after every change.
	aiPatch("bedrock-agent", "UpdateAgent", func(c *Call, r *Resource) { r.Data["agentStatus"] = "NOT_PREPARED" })
	Override("bedrock-agent", "PrepareAgent", func(c *Call) (map[string]any, error) {
		r := aiFind(c, "Agent", "agentId")
		if r == nil {
			return nil, c.NotFound("Agent %s not found", str(c.In["agentId"]))
		}
		now := time.Now().UTC().Format(time.RFC3339)
		r.Data["agentStatus"], r.Data["preparedAt"] = "PREPARED", now
		c.Put(r)
		return map[string]any{"agentId": r.ID, "agentStatus": "PREPARED", "agentVersion": "DRAFT", "preparedAt": now}, nil
	})
	Override("bedrock-agent", "DeleteAgent", func(c *Call) (map[string]any, error) {
		out, err := c.Default()
		if err != nil {
			return nil, err
		}
		out["agentStatus"] = "DELETING"
		return out, nil
	})
	aiSeed("bedrock-agent", "CreateAgentAlias", func(c *Call, r *Resource) {
		aiRekey(c, r, aiAlnum10())
		r.Data["agentAliasStatus"], r.Data["agentAliasId"], r.Data["agentAliasArn"] = "PREPARED", r.ID, r.ARN
		if c.In["routingConfiguration"] == nil {
			r.Data["routingConfiguration"] = []any{map[string]any{"agentVersion": "1"}}
		}
	})
	aiSeed("bedrock-agent", "StartIngestionJob", func(c *Call, r *Resource) {
		r.Data["status"] = "COMPLETE"
		r.Data["statistics"] = map[string]any{"numberOfDocumentsScanned": 0.0, "numberOfNewDocumentsIndexed": 0.0, "numberOfDocumentsFailed": 0.0}
	})
}
