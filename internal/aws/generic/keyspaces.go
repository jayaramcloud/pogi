package generic

import "regexp"

// Keyspaces: keyspaces and tables are stored, never queried (there is no CQL). A table is ACTIVE at once and
// shows the defaults AWS reports for the settings the create call left out.
var ksName = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_]{0,47}$`)

func init() {
	arnTemplates["keyspaces/Keyspace"] = "/keyspace/{keyspaceName}/"
	arnTemplates["keyspaces/Table"] = "/keyspace/{keyspaceName}/table/{tableName}"
	for op, member := range map[string]string{"CreateKeyspace": "keyspaceName", "CreateTable": "tableName"} {
		Override("keyspaces", op, func(c *Call) (map[string]any, error) {
			if !ksName.MatchString(str(c.In[member])) {
				return nil, c.Invalid("%s %q is not valid: start with a letter or digit, then letters, digits and underscores, 48 characters at most.", member, str(c.In[member]))
			}
			if op == "CreateTable" && c.Get("Keyspace", str(c.In["keyspaceName"])) == nil {
				return nil, c.NotFound("Keyspace %s does not exist.", str(c.In["keyspaceName"]))
			}
			if _, err := c.Default(); err != nil {
				return nil, err
			}
			if op == "CreateKeyspace" {
				if c.In["replicationSpecification"] == nil {
					s2Set(c, nil, map[string]any{"replicationStrategy": "SINGLE_REGION"})
				}
			} else {
				d := map[string]any{}
				for k, v := range map[string]any{
					"capacitySpecification":   map[string]any{"throughputMode": "PAY_PER_REQUEST"},
					"encryptionSpecification": map[string]any{"type": "AWS_OWNED_KMS_KEY"},
					"pointInTimeRecovery":     map[string]any{"status": "DISABLED"},
					"comment":                 map[string]any{"message": ""},
				} {
					if c.In[k] == nil {
						d[k] = v
					}
				}
				s2Set(c, nil, d)
			}
			return c.Fill(c.Res), nil
		})
	}
	// Deleting a keyspace deletes its tables.
	Override("keyspaces", "DeleteKeyspace", func(c *Call) (map[string]any, error) {
		ks := str(c.In["keyspaceName"])
		out, err := c.Default()
		if err != nil {
			return nil, err
		}
		for _, t := range c.List("Table") {
			if str(t.IDs["keyspaceName"]) == ks {
				c.Svc.Delete(c.Account, c.Region, "Table", t.Key)
			}
		}
		return out, nil
	})
}
