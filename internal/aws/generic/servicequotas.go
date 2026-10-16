package generic

import (
	"cmp"

	"github.com/nrndev/pogi/internal/core"
)

// Service Quotas: a quota increase request gets an Id and stays PENDING, so
// GetRequestedServiceQuotaChange can read it back; an unknown Id is not found.
// Nothing approves a request (ponytail).
func init() {
	Override("service-quotas", "RequestServiceQuotaIncrease", func(c *Call) (map[string]any, error) {
		now, id := c.now(), core.NewID()
		sc, qc := str(c.In["ServiceCode"]), str(c.In["QuotaCode"])
		q := map[string]any{"Id": id, "CaseId": id, "ServiceCode": sc, "ServiceName": sc, "QuotaCode": qc, "QuotaName": qc,
			"DesiredValue": c.In["DesiredValue"], "Status": "PENDING", "Created": float64(now.Unix()), "LastUpdated": float64(now.Unix()),
			"Requester": "arn:aws:iam::" + c.Account + ":root", "QuotaArn": core.ARN("servicequotas", c.Region, c.Account, sc+"/"+qc), "GlobalQuota": false,
			"QuotaRequestedAtLevel": cmp.Or(str(c.In["ContextId"]), "ACCOUNT")}
		c.Put(&Resource{Type: "RequestedServiceQuotaChange", Key: id, IDs: map[string]string{"RequestId": id}, ID: id, Name: id, Account: c.Account,
			Region: c.key(), Created: now, Updated: now, Data: q})
		return map[string]any{"RequestedQuota": q}, nil
	})
	Override("service-quotas", "GetRequestedServiceQuotaChange", func(c *Call) (map[string]any, error) {
		r := c.Get("RequestedServiceQuotaChange", str(c.In["RequestId"]))
		if r == nil {
			return nil, c.NotFound("Resource %s was not found.", str(c.In["RequestId"]))
		}
		return map[string]any{"RequestedQuota": r.Data}, nil
	})
}
