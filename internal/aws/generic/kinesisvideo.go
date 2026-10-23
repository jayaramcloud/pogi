package generic

import "strconv"

// Kinesis Video Streams: streams and signaling channels are stored and read back; no video is ingested.
// Names are unique, as in AWS. A stream or channel has a version that changes when it is updated, and a delete
// that names another version is refused (Terraform sends the version it read).
func kvVersion(r *Resource) string { return strconv.FormatInt(r.Updated.UnixNano(), 36) }

func init() {
	for op, typ := range map[string]string{"CreateStream": "Stream", "CreateSignalingChannel": "SignalingChannel"} {
		member := typ + "Name"
		if typ == "SignalingChannel" {
			member = "ChannelName"
		}
		Override("kinesisvideo", op, func(c *Call) (map[string]any, error) {
			for _, r := range c.List(typ) {
				if str(r.Data[member]) == str(c.In[member]) && str(c.In[member]) != "" {
					return nil, c.Err("ResourceInUseException", "The %s %s already exists.", typ, str(c.In[member]))
				}
			}
			return c.Default()
		})
		Derive("kinesisvideo", typ, func(r *Resource) map[string]any { return map[string]any{"Version": kvVersion(r)} })
	}
	for op, typ := range map[string]string{"DeleteStream": "Stream", "DeleteSignalingChannel": "SignalingChannel"} {
		Override("kinesisvideo", op, func(c *Call) (map[string]any, error) {
			if v := str(c.In["CurrentVersion"]); v != "" {
				if r := c.resolve(c.m.types[norm(typ)]); r != nil && v != kvVersion(r) {
					return nil, c.Err("VersionMismatchException", "The version %s is not the current version.", v)
				}
			}
			return c.Default()
		})
	}
}
