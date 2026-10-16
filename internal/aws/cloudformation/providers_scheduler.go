package cloudformation

import (
	"net/url"
	"strings"
)

// AWS::Scheduler::Schedule (the SAM ScheduleV2 event makes one). The physical
// id is "group/name" (CloudFormation's Ref is the name, so the name is also an
// attribute).
func init() {
	create := func(c *pctx, method, phys string, p map[string]any) (string, map[string]any, error) {
		name, group := str(p["Name"]), or(str(p["GroupName"]), "default")
		if phys != "" {
			group, name, _ = strings.Cut(phys, "/")
		} else if name == "" {
			name = c.name(64, false)
		}
		in := pick(p, "ScheduleExpression", "ScheduleExpressionTimezone", "FlexibleTimeWindow", "State", "Description", "Target", "StartDate", "EndDate", "KmsKeyArn")
		in["GroupName"] = group
		var out struct{ ScheduleArn string }
		if err := c.rest("scheduler", method, "/schedules/"+url.PathEscape(name), in, &out); err != nil {
			return "", nil, err
		}
		return group + "/" + name, map[string]any{"Arn": out.ScheduleArn, "Name": name}, nil
	}
	register("AWS::Scheduler::Schedule", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) { return create(c, "POST", "", p) },
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if str(p["Name"]) != str(old["Name"]) || or(str(p["GroupName"]), "default") != or(str(old["GroupName"]), "default") {
				return nil, true, nil
			}
			_, attrs, err := create(c, "PUT", phys, p)
			return attrs, false, err
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			group, name, _ := strings.Cut(phys, "/")
			return c.rest("scheduler", "DELETE", "/schedules/"+url.PathEscape(name)+"?groupName="+url.QueryEscape(group), nil, nil)
		},
	})
}
