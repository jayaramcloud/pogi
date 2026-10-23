package generic

import "strings"

// Storage Gateway: tape pools are the engine's. A gateway is made by ActivateGateway with any activation
// key (no appliance exists, so nothing is contacted); it is RUNNING at once. AWS answers a gateway or a pool
// that is not there with InvalidGatewayRequestException.

func sgwGateway(c *Call) (*Resource, error) {
	r := c.Get("Gateway", str(c.In["GatewayARN"]))
	if r == nil {
		return nil, c.Err("InvalidGatewayRequestException", "The specified gateway was not found.")
	}
	return r, nil
}

func init() {
	Override("storagegateway", "ActivateGateway", func(c *Call) (map[string]any, error) {
		id := "sgw-" + strings.ToUpper(randHex(8))
		arn := c.arnOf("gateway/"+id, nil)
		typ := str(c.In["GatewayType"])
		if typ == "" {
			typ = "CACHED"
		}
		now := c.now()
		c.Put(&Resource{Type: "Gateway", Key: arn, ARN: arn, ID: id, Account: c.Account, Region: c.key(), Created: now, Updated: now,
			Tags: tagMap(c.In["Tags"]), Data: map[string]any{"GatewayName": c.In["GatewayName"], "GatewayTimezone": c.In["GatewayTimezone"], "GatewayType": typ}})
		return map[string]any{"GatewayARN": arn}, nil
	})
	Override("storagegateway", "DescribeGatewayInformation", func(c *Call) (map[string]any, error) {
		r, err := sgwGateway(c)
		if err != nil {
			return nil, err
		}
		out := map[string]any{"GatewayARN": r.ARN, "GatewayId": r.ID, "GatewayName": r.Data["GatewayName"], "GatewayTimezone": r.Data["GatewayTimezone"],
			"GatewayType": r.Data["GatewayType"], "GatewayState": "RUNNING", "GatewayNetworkInterfaces": []any{}, "HostEnvironment": "OTHER", "Tags": []any{}}
		if t := r.Data["CloudWatchLogGroupARN"]; t != nil {
			out["CloudWatchLogGroupARN"] = t
		}
		if t := r.Data["GatewayCapacity"]; t != nil {
			out["GatewayCapacity"] = t
		}
		for k, v := range r.Tags {
			out["Tags"] = append(out["Tags"].([]any), map[string]any{"Key": k, "Value": v})
		}
		return out, nil
	})
	Override("storagegateway", "UpdateGatewayInformation", func(c *Call) (map[string]any, error) {
		r, err := sgwGateway(c)
		if err != nil {
			return nil, err
		}
		for _, k := range []string{"GatewayName", "GatewayTimezone", "GatewayCapacity", "CloudWatchLogGroupARN"} {
			if v, ok := c.In[k]; ok {
				r.Data[k] = v
			}
		}
		r.Updated = c.now()
		c.Put(r)
		return map[string]any{"GatewayARN": r.ARN, "GatewayName": r.Data["GatewayName"]}, nil
	})
	Override("storagegateway", "DeleteGateway", func(c *Call) (map[string]any, error) {
		r, err := sgwGateway(c)
		if err != nil {
			return nil, err
		}
		c.Svc.Delete(c.Account, c.Region, "Gateway", r.Key)
		return map[string]any{"GatewayARN": r.ARN}, nil
	})
	Override("storagegateway", "ListGateways", func(c *Call) (map[string]any, error) {
		out := []any{}
		for _, r := range c.List("Gateway") {
			out = append(out, map[string]any{"GatewayARN": r.ARN, "GatewayId": r.ID, "GatewayName": r.Data["GatewayName"], "GatewayType": r.Data["GatewayType"], "GatewayOperationalState": "ACTIVE"})
		}
		return map[string]any{"Gateways": out}, nil
	})
	Override("storagegateway", "CreateTapePool", func(c *Call) (map[string]any, error) {
		c.In["PoolARN"] = c.arnOf("tapepool/pool-"+randHex(8), nil)
		return c.Default()
	})
	Override("storagegateway", "DeleteTapePool", func(c *Call) (map[string]any, error) {
		if c.Get("TapePool", str(c.In["PoolARN"])) == nil {
			return nil, c.Err("InvalidGatewayRequestException", "The specified tape pool was not found.")
		}
		return c.Default()
	})
}
