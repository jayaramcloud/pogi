package generic

import (
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// Macie: the account session is one record per account and region. A disabled
// account answers AccessDenied ("Macie is not enabled"), which is how the
// Terraform provider tells it is gone. No buckets are scanned (ponytail).

func macieSession(c *Call) (*Resource, error) {
	if r := c.Get("Session", "default"); r != nil {
		return r, nil
	}
	return nil, c.Err("AccessDeniedException", "Macie is not enabled")
}

func init() {
	Override("macie2", "EnableMacie", func(c *Call) (map[string]any, error) {
		if c.Get("Session", "default") != nil {
			return nil, c.Err("ConflictException", "Macie is already enabled")
		}
		freq, status := str(c.In["findingPublishingFrequency"]), str(c.In["status"])
		if freq == "" {
			freq = "FIFTEEN_MINUTES"
		}
		if status == "" {
			status = "ENABLED"
		}
		now := c.now()
		c.Put(&Resource{Type: "Session", Key: "default", Name: "default", Account: c.Account, Region: c.key(), Created: now, Updated: now,
			Data: map[string]any{"findingPublishingFrequency": freq, "status": status}})
		return map[string]any{}, nil
	})
	Override("macie2", "GetMacieSession", func(c *Call) (map[string]any, error) {
		r, err := macieSession(c)
		if err != nil {
			return nil, err
		}
		return map[string]any{"createdAt": r.Created.Format(time.RFC3339Nano), "updatedAt": r.Updated.Format(time.RFC3339Nano), "findingPublishingFrequency": r.Data["findingPublishingFrequency"], "status": r.Data["status"],
			"serviceRole": core.GlobalARN("iam", c.Region, c.Account, "role/aws-service-role/macie.amazonaws.com/AWSServiceRoleForAmazonMacie")}, nil
	})
	Override("macie2", "UpdateMacieSession", func(c *Call) (map[string]any, error) {
		r, err := macieSession(c)
		if err != nil {
			return nil, err
		}
		for _, k := range []string{"findingPublishingFrequency", "status"} {
			if v, ok := c.In[k]; ok {
				r.Data[k] = v
			}
		}
		r.Updated = c.now()
		c.Put(r)
		return map[string]any{}, nil
	})
	Override("macie2", "DisableMacie", func(c *Call) (map[string]any, error) {
		r, err := macieSession(c)
		if err != nil {
			return nil, err
		}
		c.Svc.Delete(c.Account, c.Region, "Session", r.Key)
		return map[string]any{}, nil
	})
}
