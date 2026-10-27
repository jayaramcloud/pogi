package generic

import "strings"

// Aurora DSQL: a cluster is ACTIVE at once, with an endpoint name and a VPC endpoint service name
// made from its id. Its SQL endpoint is a PostgreSQL server that RDS runs (rds/dsql.go); clients reach it on
// pogi's main port with an auth token.

// sqlHost is what RDS offers DSQL: a server per cluster. Nil when RDS is off.
type sqlHost interface {
	AddSQL(account, region, id string)
	DropSQL(account, region, id string)
}

func sqlOf(c *Call) sqlHost {
	if c.Hub() == nil {
		return nil
	}
	h, _ := c.Hub().Service("rds").(sqlHost)
	return h
}

func init() {
	Hook("dsql", "CreateCluster", func(c *Call) {
		if h := sqlOf(c); h != nil {
			h.AddSQL(c.Account, c.Region, c.Res.Key)
		}
	})
	Hook("dsql", "DeleteCluster", func(c *Call) {
		if h := sqlOf(c); h != nil {
			h.DropSQL(c.Account, c.Region, c.Res.Key)
		}
	})
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
