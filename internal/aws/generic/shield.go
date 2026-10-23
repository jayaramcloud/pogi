package generic

import (
	"strings"

	"github.com/nrndev/pogi/internal/core"
)

// Shield Advanced: the account has at most one subscription, a one-year commitment that real Shield will not end
// early, so DeleteSubscription is refused. Protections are the engine's, with the real rule that a resource has
// one protection. Nothing is defended.
const shdSub = "Subscription"

func shdSubscription(c *Call) *Resource { return c.Get(shdSub, "default") }

func init() {
	arnTemplates["shield/Protection"] = "protection/{ProtectionId}"
	Override("shield", "CreateSubscription", func(c *Call) (map[string]any, error) {
		if shdSubscription(c) != nil {
			return nil, c.Err("ResourceAlreadyExistsException", "The account is already subscribed to Shield Advanced.")
		}
		start := c.now()
		s2Store(c, shdSub, "default", "default", c.arnOf("subscription/"+core.UUID(), nil), map[string]any{
			"StartTime": s2Time(start), "EndTime": s2Time(start.AddDate(1, 0, 0)), "TimeCommitmentInSeconds": float64(365 * 24 * 3600),
			"AutoRenew": "ENABLED", "ProactiveEngagementStatus": "DISABLED", "Limits": []any{},
			"SubscriptionLimits": map[string]any{
				"ProtectionLimits":      map[string]any{"ProtectedResourceTypeLimits": []any{}},
				"ProtectionGroupLimits": map[string]any{"MaxProtectionGroups": float64(20), "PatternTypeLimits": map[string]any{"ArbitraryPatternLimits": map[string]any{"MaxMembers": float64(100)}}},
			}}, "SubscriptionId")
		return map[string]any{}, nil
	})
	Override("shield", "DescribeSubscription", func(c *Call) (map[string]any, error) {
		r := shdSubscription(c)
		if r == nil {
			return nil, c.NotFound("The account is not subscribed to Shield Advanced.")
		}
		d := s2Clone(r.Data)
		d["SubscriptionArn"] = r.ARN
		return map[string]any{"Subscription": d}, nil
	})
	Override("shield", "UpdateSubscription", func(c *Call) (map[string]any, error) {
		r := shdSubscription(c)
		if r == nil {
			return nil, c.NotFound("The account is not subscribed to Shield Advanced.")
		}
		if v := str(c.In["AutoRenew"]); v != "" {
			r.Data["AutoRenew"] = v
			c.Put(r)
		}
		return map[string]any{}, nil
	})
	Override("shield", "DeleteSubscription", func(c *Call) (map[string]any, error) {
		if shdSubscription(c) == nil {
			return nil, c.NotFound("The account is not subscribed to Shield Advanced.")
		}
		return nil, c.Err("LockedSubscriptionException", "The subscription is locked until its one-year commitment ends.")
	})
	Override("shield", "GetSubscriptionState", func(c *Call) (map[string]any, error) {
		if shdSubscription(c) == nil {
			return map[string]any{"SubscriptionState": "INACTIVE"}, nil
		}
		return map[string]any{"SubscriptionState": "ACTIVE"}, nil
	})
	Override("shield", "CreateProtection", func(c *Call) (map[string]any, error) {
		arn := str(c.In["ResourceArn"])
		if !strings.HasPrefix(arn, "arn:") {
			return nil, c.Err("InvalidResourceException", "%q is not a resource ARN.", arn)
		}
		for _, r := range c.List("Protection") {
			if str(r.Data["ResourceArn"]) == arn {
				return nil, c.Err("ResourceAlreadyExistsException", "The resource %s already has a protection.", arn)
			}
		}
		c.In["ProtectionId"] = core.UUID()
		return c.Default()
	})
	Override("shield", "DescribeProtection", func(c *Call) (map[string]any, error) {
		if arn := str(c.In["ResourceArn"]); arn != "" && str(c.In["ProtectionId"]) == "" {
			for _, r := range c.List("Protection") {
				if str(r.Data["ResourceArn"]) == arn {
					c.In["ProtectionId"] = r.IDs["ProtectionId"]
				}
			}
			if str(c.In["ProtectionId"]) == "" {
				return nil, c.NotFound("No protection for %s.", arn)
			}
		}
		if str(c.In["ProtectionId"]) == "" {
			return nil, c.Err("InvalidParameterException", "Give a ProtectionId or a ResourceArn.")
		}
		return c.Default()
	})
}
