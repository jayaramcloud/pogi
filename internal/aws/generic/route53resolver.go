package generic

import (
	"crypto/sha256"
	"encoding/hex"
	"time"
)

// Route 53 Resolver: an endpoint needs two to ten IP addresses, and lists them with ids.
func init() {
	Override("route53resolver", "CreateResolverEndpoint", func(c *Call) (map[string]any, error) {
		if n := len(asList(c.In["IpAddresses"])); n < 2 || n > 10 {
			return nil, c.Invalid("IpAddresses must have between two and ten items, got %d", n)
		}
		return c.Default()
	})
	// ponytail: an address given without Ip is listed without one; pogi does not pick it from the subnet.
	Override("route53resolver", "ListResolverEndpointIpAddresses", func(c *Call) (map[string]any, error) {
		id := str(c.In["ResolverEndpointId"])
		r := c.Get("ResolverEndpoint", id)
		if r == nil {
			return nil, c.NotFound("Resolver endpoint with ID '%s' does not exist", id)
		}
		ips := []any{}
		for i, a := range asList(r.Data["IpAddresses"]) {
			m, _ := a.(map[string]any)
			h := sha256.Sum256([]byte(id + str(m["SubnetId"]) + str(m["Ip"]) + string(rune('a'+i))))
			ips = append(ips, map[string]any{"IpId": "rni-" + hex.EncodeToString(h[:9])[:17], "SubnetId": m["SubnetId"], "Ip": m["Ip"], "Ipv6": m["Ipv6"],
				"Status": "ATTACHED", "StatusMessage": "This IP address is operational.",
				"CreationTime": r.Created.Format(time.RFC3339), "ModificationTime": r.Updated.Format(time.RFC3339)})
		}
		return map[string]any{"IpAddresses": ips, "MaxResults": float64(len(ips))}, nil
	})
}
