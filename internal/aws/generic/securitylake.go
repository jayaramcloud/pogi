package generic

import "slices"

// Security Lake: a data lake is one per region and is COMPLETED at once. Its S3 bucket is named but not made, and
// no logs are collected. The engine alone would return an empty dataLakes list from a create.
func init() {
	lake := func(c *Call, r *Resource) map[string]any {
		out := map[string]any{"region": r.Key, "dataLakeArn": r.ARN, "s3BucketArn": r.Data["s3BucketArn"], "createStatus": "COMPLETED"}
		for _, k := range []string{"encryptionConfiguration", "lifecycleConfiguration", "replicationConfiguration"} {
			if v, ok := r.Data[k]; ok {
				out[k] = v
			}
		}
		return out
	}
	// store makes or changes the lake of each configuration; create says whether an existing one is a conflict.
	store := func(c *Call, create bool) (map[string]any, error) {
		var res []any
		for _, e := range asList(c.In["configurations"]) {
			cfg := s2Map(e)
			region := str(cfg["region"])
			old := c.Get("DataLake", region)
			switch {
			case create && old != nil:
				return nil, c.Exists("A data lake already exists in %s", region)
			case !create && old == nil:
				return nil, c.NotFound("No data lake in %s", region)
			}
			data := map[string]any{"s3BucketArn": "arn:aws:s3:::aws-security-data-lake-" + region + "-" + randHex(20)}
			arn := c.arnOf("data-lake/default", nil)
			if old != nil {
				data, arn = old.Data, old.ARN
			}
			for _, k := range []string{"encryptionConfiguration", "lifecycleConfiguration", "replicationConfiguration"} {
				if v, ok := cfg[k]; ok {
					data[k] = v
				}
			}
			if _, ok := data["encryptionConfiguration"]; !ok {
				data["encryptionConfiguration"] = map[string]any{"kmsKeyId": "S3_MANAGED_KEY"}
			}
			r := s2Store(c, "DataLake", region, region, arn, data)
			if create {
				s2Tags(c, r)
			}
			res = append(res, lake(c, c.Get("DataLake", region)))
		}
		return map[string]any{"dataLakes": res}, nil
	}
	Override("securitylake", "CreateDataLake", func(c *Call) (map[string]any, error) { return store(c, true) })
	Override("securitylake", "UpdateDataLake", func(c *Call) (map[string]any, error) { return store(c, false) })
	Override("securitylake", "ListDataLakes", func(c *Call) (map[string]any, error) {
		want := s2Names(c.In["regions"])
		res := []any{}
		for _, r := range c.List("DataLake") {
			if len(want) == 0 || slices.Contains(want, r.Key) {
				res = append(res, lake(c, r))
			}
		}
		return map[string]any{"dataLakes": res}, nil
	})
	Override("securitylake", "DeleteDataLake", func(c *Call) (map[string]any, error) {
		regions := s2Names(c.In["regions"])
		for _, r := range regions { // all or none
			if c.Get("DataLake", r) == nil {
				return nil, c.NotFound("No data lake in %s", r)
			}
		}
		for _, r := range regions {
			c.Svc.Delete(c.Account, c.Region, "DataLake", r)
		}
		return map[string]any{}, nil
	})
}
