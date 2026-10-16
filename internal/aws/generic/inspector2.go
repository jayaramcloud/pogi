package generic

import (
	"slices"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// Inspector: accounts are enabled for resource types, and suppression filters
// have ARNs of the owner/<account>/filter/<id> form. No scans run, so there are no findings.

var inspectorTypes = map[string]string{"EC2": "ec2", "ECR": "ecr", "LAMBDA": "lambda", "LAMBDA_CODE": "lambdaCode", "CODE_REPOSITORY": "codeRepository"}

// inspectorAccounts are the accounts a request names (the caller when none).
func inspectorAccounts(c *Call) []string {
	var out []string
	for _, a := range asList(c.In["accountIds"]) {
		out = append(out, str(a))
	}
	if len(out) == 0 {
		out = []string{c.Account}
	}
	return out
}

// inspectorState is the resource types enabled in an account.
func inspectorState(c *Call, acct string) []any {
	if r := c.Get("AccountStatus", acct); r != nil {
		return asList(r.Data["resourceTypes"])
	}
	return nil
}

func inspectorSet(c *Call, acct string, types []any) {
	now := c.now()
	r := &Resource{Type: "AccountStatus", Key: acct, Name: acct, Account: c.Account, Region: c.key(), Created: now, Updated: now, Data: map[string]any{"resourceTypes": types}}
	if old := c.Get("AccountStatus", acct); old != nil {
		r.Created = old.Created
	}
	c.Put(r)
}

// inspectorStatus is the status of one resource type of an account.
func inspectorStatus(enabled []any, typ string) string {
	if slices.ContainsFunc(enabled, func(e any) bool { return inspectorTypes[str(e)] == typ }) {
		return "ENABLED"
	}
	return "DISABLED"
}

func init() {
	change := func(on bool) func(*Call) (map[string]any, error) {
		return func(c *Call) (map[string]any, error) {
			want := asList(c.In["resourceTypes"])
			for _, w := range want {
				if _, ok := inspectorTypes[str(w)]; !ok {
					return nil, c.Invalid("Value '%s' at 'resourceTypes' failed to satisfy constraint: Member must satisfy enum value set", str(w))
				}
			}
			out := []any{}
			for _, acct := range inspectorAccounts(c) {
				cur := inspectorState(c, acct)
				switch {
				case on:
					for _, w := range want {
						if !slices.Contains(cur, w) {
							cur = append(cur, w)
						}
					}
				case len(want) == 0:
					cur = nil
				default:
					cur = slices.DeleteFunc(slices.Clone(cur), func(e any) bool { return slices.Contains(want, e) })
				}
				inspectorSet(c, acct, cur)
				rs := map[string]any{}
				for _, f := range inspectorTypes {
					rs[f] = inspectorStatus(cur, f)
				}
				status := "DISABLED"
				if len(cur) > 0 {
					status = "ENABLED"
				}
				out = append(out, map[string]any{"accountId": acct, "status": status, "resourceStatus": rs})
			}
			return map[string]any{"accounts": out, "failedAccounts": []any{}}, nil
		}
	}
	Override("inspector2", "Enable", change(true))
	Override("inspector2", "Disable", change(false))
	Override("inspector2", "BatchGetAccountStatus", func(c *Call) (map[string]any, error) {
		out := []any{}
		for _, acct := range inspectorAccounts(c) {
			cur := inspectorState(c, acct)
			rs := map[string]any{}
			for _, f := range inspectorTypes {
				rs[f] = map[string]any{"status": inspectorStatus(cur, f)}
			}
			status := "DISABLED"
			if len(cur) > 0 {
				status = "ENABLED"
			}
			out = append(out, map[string]any{"accountId": acct, "state": map[string]any{"status": status}, "resourceState": rs})
		}
		return map[string]any{"accounts": out, "failedAccounts": []any{}}, nil
	})

	Override("inspector2", "CreateFilter", func(c *Call) (map[string]any, error) {
		c.In["arn"] = core.ARN("inspector2", c.Region, c.Account, "owner/"+c.Account+"/filter/"+randHex(16))
		return c.Default()
	})
	// A listed filter has its criteria as "criteria" and an owner.
	Override("inspector2", "ListFilters", func(c *Call) (map[string]any, error) {
		out := []any{}
		for _, r := range c.List("Filter") {
			if arns := asList(c.In["arns"]); len(arns) > 0 && !slices.ContainsFunc(arns, func(a any) bool { return str(a) == r.ARN }) {
				continue
			}
			if c.In["action"] != nil && c.In["action"] != r.Data["action"] {
				continue
			}
			f := map[string]any{"arn": r.ARN, "ownerId": c.Account, "name": r.Data["name"], "action": r.Data["action"], "criteria": r.Data["filterCriteria"],
				"description": r.Data["description"], "reason": r.Data["reason"], "createdAt": r.Created.Format(time.RFC3339Nano), "updatedAt": r.Updated.Format(time.RFC3339Nano)}
			if len(r.Tags) > 0 {
				f["tags"] = tagsAs(c.op.out.Members["filters"].sh.Member.sh.Members["tags"].sh, r.Tags)
			}
			out = append(out, f)
		}
		return map[string]any{"filters": out}, nil
	})
	Override("inspector2", "ListAccountPermissions", func(c *Call) (map[string]any, error) {
		out := []any{}
		for _, s := range []string{"EC2", "ECR", "LAMBDA"} {
			for _, o := range []string{"ENABLE_SCANNING", "DISABLE_SCANNING", "ENABLE_REPOSITORY", "DISABLE_REPOSITORY"} {
				out = append(out, map[string]any{"service": s, "operation": o})
			}
		}
		return map[string]any{"permissions": out}, nil
	})
}
