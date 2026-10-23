package generic

import (
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// Payment Cryptography: keys are stored and read back; no key material exists and nothing is encrypted.
// A key is CREATE_COMPLETE at once. As in AWS, DeleteKey does not remove the key: it goes to DELETE_PENDING,
// disabled, for DeleteKeyInDays (7 by default), and can still be read.
func init() {
	arnService["payment-cryptography"] = "payment-cryptography" // not the endpoint prefix, controlplane.payment-cryptography
	arnTemplates["payment-cryptography/Key"] = "key/{KeyIdentifier}"
	Override("payment-cryptography", "CreateKey", func(c *Call) (map[string]any, error) {
		c.In["KeyIdentifier"] = strings.ReplaceAll(core.UUID(), "-", "")[:20] // the id at the end of the key's ARN
		return c.Default()
	})
	Derive("payment-cryptography", "Key", func(r *Resource) map[string]any {
		if r.Data["DeletePendingTimestamp"] != nil {
			return map[string]any{"KeyState": "DELETE_PENDING", "Enabled": false}
		}
		on, set := r.Data["Enabled"].(bool)
		return map[string]any{"KeyState": "CREATE_COMPLETE", "Enabled": on || !set} // enabled unless the create call said not
	})
	Override("payment-cryptography", "DeleteKey", func(c *Call) (map[string]any, error) {
		r := c.resolve(c.op.typ)
		if r == nil {
			return nil, c.NotFound("Key %s not found.", str(c.In["KeyIdentifier"]))
		}
		days := 7.0
		if d, ok := c.In["DeleteKeyInDays"].(float64); ok {
			if d < 3 || d > 180 {
				return nil, c.Invalid("DeleteKeyInDays must be between 3 and 180, got %v.", d)
			}
			days = d
		}
		r = r.clone()
		r.Data["DeletePendingTimestamp"] = c.now().Add(time.Duration(days) * 24 * time.Hour).Format(timeFmt)
		c.Put(r)
		return map[string]any{"Key": c.fillItem(c.op.out.Members["Key"].sh, r, "Key")}, nil
	})
}
