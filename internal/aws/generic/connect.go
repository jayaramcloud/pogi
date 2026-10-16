package generic

import (
	"github.com/nrndev/pogi/internal/core"
)

// Amazon Connect: an instance goes CREATION_IN_PROGRESS, ACTIVE and has an
// access URL and a service role; the rest (queues, users, hours of operation,
// routing and security profiles, flows ...) is the engine's plain CRUD keyed by
// instance. Appintegrations, CodeGuru Reviewer and Lambda microVMs are in their
// own files.
func init() {
	Derive("connect", "Instance", func(r *Resource) map[string]any {
		host := str(r.Data["InstanceAlias"])
		if host == "" {
			host = r.ID
		}
		return map[string]any{"InstanceStatus": Step(r, "inst", "CREATION_IN_PROGRESS", "ACTIVE"), "InstanceAccessUrl": "https://" + host + ".my.connect.localhost",
			"ServiceRole": core.ARN("iam", "", r.Account, "role/aws-service-role/connect.amazonaws.com/AWSServiceRoleForAmazonConnect_"+r.ID[:8])}
	})
	Override("connect", "CreateInstance", func(c *Call) (map[string]any, error) {
		if alias := str(c.In["InstanceAlias"]); alias != "" {
			for _, r := range c.List("Instance") {
				if r.Data["InstanceAlias"] == alias {
					return nil, core.Errorf(409, "DuplicateResourceException", "Instance alias %s is already taken.", alias)
				}
			}
		}
		c.In["InstanceId"] = core.UUID() // an instance is named by a UUID
		out, err := c.Default()
		if err != nil {
			return nil, err
		}
		r := c.Res
		Begin(r, "inst")
		c.Put(r)
		return out, nil
	})
	Override("connect", "UpdateInstanceAttribute", func(c *Call) (map[string]any, error) {
		r := c.Get("Instance", str(c.In["InstanceId"]))
		if r == nil {
			return nil, c.NotFound("Instance %s not found.", str(c.In["InstanceId"]))
		}
		switch t := str(c.In["AttributeType"]); t {
		case "INBOUND_CALLS":
			r.Data["InboundCallsEnabled"] = str(c.In["Value"]) == "true"
		case "OUTBOUND_CALLS":
			r.Data["OutboundCallsEnabled"] = str(c.In["Value"]) == "true"
		default: // other attributes are kept by name
			if r.Data["Attributes"] == nil {
				r.Data["Attributes"] = map[string]any{}
			}
			r.Data["Attributes"].(map[string]any)[t] = str(c.In["Value"])
		}
		c.Put(r)
		return map[string]any{}, nil
	})
	Override("connect", "DescribeInstanceAttribute", func(c *Call) (map[string]any, error) {
		r := c.Get("Instance", str(c.In["InstanceId"]))
		if r == nil {
			return nil, c.NotFound("Instance %s not found.", str(c.In["InstanceId"]))
		}
		t, v := str(c.In["AttributeType"]), ""
		switch t {
		case "INBOUND_CALLS":
			v = str(r.Data["InboundCallsEnabled"])
		case "OUTBOUND_CALLS":
			v = str(r.Data["OutboundCallsEnabled"])
		default:
			a, _ := r.Data["Attributes"].(map[string]any)
			v = str(a[t])
			if v == "" {
				v = "false"
			}
		}
		return map[string]any{"Attribute": map[string]any{"AttributeType": t, "Value": v}}, nil
	})
	// Queue names and statuses are changed by their own calls.
	Override("connect", "UpdateQueueName", func(c *Call) (map[string]any, error) {
		r := c.Get("Queue", str(c.In["InstanceId"]), str(c.In["QueueId"]))
		if r == nil {
			return nil, c.NotFound("Queue %s not found.", str(c.In["QueueId"]))
		}
		for _, k := range []string{"Name", "Description"} {
			if v, ok := c.In[k]; ok {
				r.Data[k] = v
			}
		}
		c.Put(r)
		return map[string]any{}, nil
	})
	Override("connect", "UpdateQueueStatus", func(c *Call) (map[string]any, error) {
		r := c.Get("Queue", str(c.In["InstanceId"]), str(c.In["QueueId"]))
		if r == nil {
			return nil, c.NotFound("Queue %s not found.", str(c.In["QueueId"]))
		}
		r.Data["Status"] = c.In["Status"]
		c.Put(r)
		return map[string]any{}, nil
	})
}
