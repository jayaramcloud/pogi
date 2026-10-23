package generic

// Helpers for the services of group K that keep one setting per account and Region
// (Compute Optimizer's enrollment, Firewall Manager's administrator).

// kSetting is the stored setting of a type, or nil when it was never set.
func kSetting(c *Call, typ string) *Resource { return c.Get(typ, "default") }

// kSave merges data into the setting of a type and stores it.
func kSave(c *Call, typ string, data map[string]any) *Resource {
	now := c.now()
	r := &Resource{Type: typ, Key: "default", Name: "default", Account: c.Account, Region: c.key(), Data: map[string]any{}, Created: now, Updated: now}
	if old := kSetting(c, typ); old != nil {
		r.Created = old.Created
		for k, v := range old.Data {
			r.Data[k] = v
		}
	}
	for k, v := range data {
		r.Data[k] = v
	}
	c.Put(r)
	return r
}
