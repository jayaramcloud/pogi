package generic

import (
	"sort"

	"github.com/nrndev/pogi/internal/core"
)

// Direct Connect: connections and gateways are stored and nothing carries traffic. DescribeTags answers
// one entry for each ARN asked about, with that resource's tags; the Terraform provider reads the first
// entry and crashes on an empty list.
func init() {
	arnTemplates["directconnect/Connection"] = "dxcon/{connectionId}"
	arnTemplates["directconnect/DirectConnectGateway"] = "dx-gateway/{directConnectGatewayId}"
	// real ids: dxcon-<8 characters> for a connection, a UUID for a gateway
	Override("directconnect", "CreateConnection", func(c *Call) (map[string]any, error) {
		c.In["connectionId"] = "dxcon-" + randHex(8)
		return c.Default()
	})
	Override("directconnect", "CreateDirectConnectGateway", func(c *Call) (map[string]any, error) {
		c.In["directConnectGatewayId"] = core.UUID()
		return c.Default()
	})
	// A deleted connection stays readable in the state "deleted", as in AWS; the Terraform provider waits for that.
	Override("directconnect", "DeleteConnection", func(c *Call) (map[string]any, error) {
		r := c.Get("Connection", str(c.In["connectionId"]))
		if r == nil {
			return c.Default()
		}
		r.Data["connectionState"] = "deleted"
		c.Put(r)
		return c.Fill(r), nil
	})
	// a gateway that does not exist (or was deleted) is an empty list, which the Terraform provider waits for
	Override("directconnect", "DescribeDirectConnectGateways", func(c *Call) (map[string]any, error) {
		out, err := c.Default()
		if err != nil {
			return map[string]any{"directConnectGateways": []any{}}, nil
		}
		return out, nil
	})
	Override("directconnect", "DescribeTags", func(c *Call) (map[string]any, error) {
		var out []any
		for _, a := range asList(c.In["resourceArns"]) {
			r := c.Svc.byARN(c.Account, str(a))
			if r == nil {
				continue
			}
			var tags []any
			keys := make([]string, 0, len(r.Tags))
			for k := range r.Tags {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				tags = append(tags, map[string]any{"key": k, "value": r.Tags[k]})
			}
			out = append(out, map[string]any{"resourceArn": r.ARN, "tags": tags})
		}
		return map[string]any{"resourceTags": out}, nil
	})
}
