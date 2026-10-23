package generic

import "strings"

// User Notifications: configurations and event rules are the engine's. A configuration's name is unique,
// as in AWS. No event is ever delivered.

func init() {
	Override("notifications", "CreateNotificationConfiguration", func(c *Call) (map[string]any, error) {
		for _, r := range c.List("NotificationConfiguration") {
			if strings.EqualFold(str(r.Data["name"]), str(c.In["name"])) {
				return nil, c.Exists("A notification configuration named %s already exists", str(c.In["name"]))
			}
		}
		return c.Default()
	})
}
