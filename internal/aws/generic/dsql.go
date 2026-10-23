package generic

import "strings"

// Aurora DSQL: a cluster is ACTIVE at once, with an endpoint name and a VPC endpoint service name
// made from its id. No SQL endpoint answers behind them.

func init() {
	Derive("dsql", "Cluster", func(r *Resource) map[string]any {
		enc := map[string]any{"encryptionType": "AWS_OWNED_KMS_KEY", "encryptionStatus": "ENABLED", "kmsKeyArn": "AWS_OWNED_KMS_KEY"}
		if k := str(r.Data["kmsEncryptionKey"]); k != "" && k != "AWS_OWNED_KMS_KEY" {
			enc["encryptionType"], enc["kmsKeyArn"] = "CUSTOMER_MANAGED_KMS_KEY", k
		}
		return map[string]any{"endpoint": r.Key + ".dsql." + r.Region + ".on.aws", "encryptionDetails": enc}
	})
	Override("dsql", "GetVpcEndpointServiceName", func(c *Call) (map[string]any, error) {
		r := c.Get("Cluster", str(c.In["identifier"]))
		if r == nil {
			return nil, c.NotFound("Cluster %s not found", str(c.In["identifier"]))
		}
		return map[string]any{"serviceName": "com.amazonaws." + c.Region + ".dsql-" + strings.ToLower(r.Key[:4])}, nil
	})
}
