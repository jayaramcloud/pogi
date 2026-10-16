package generic

import "time"

// CloudWatch RUM: an app monitor's ARN is appmonitor/<name> and its dates are ISO strings.
func init() {
	arnTemplates["rum/AppMonitor"] = "appmonitor/{Name}"
	s2After("rum", "CreateAppMonitor", func(c *Call, out map[string]any) error {
		now := time.Now().UTC().Format("2006-01-02T15:04:05Z")
		s2Set(c, nil, map[string]any{"Created": now, "LastModified": now})
		return nil
	})
	s2After("rum", "UpdateAppMonitor", func(c *Call, out map[string]any) error {
		s2Set(c, nil, map[string]any{"LastModified": time.Now().UTC().Format("2006-01-02T15:04:05Z")})
		return nil
	})
}
