package generic

// Bedrock AgentCore (control plane): CRUD only; nothing runs. The engine covers it, apart from the
// version a runtime starts at.
func init() {
	aiSeed("bedrock-agentcore-control", "CreateAgentRuntime", func(c *Call, r *Resource) { r.Data["agentRuntimeVersion"] = "1" })
}
