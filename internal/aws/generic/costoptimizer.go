package generic

// Compute Optimizer and Cost Optimization Hub: the opt-in status and the
// preferences are one setting per account. Turning enrollment on takes effect at
// once; no recommendation is ever made.

func init() {
	enroll := func(c *Call) (map[string]any, error) {
		st := str(c.In["status"])
		if st != "Active" && st != "Inactive" {
			return nil, c.Invalid("status must be Active or Inactive, got %q", st)
		}
		kSave(c, "EnrollmentStatus", map[string]any{"status": st, "includeMemberAccounts": c.In["includeMemberAccounts"]})
		return map[string]any{"status": st}, nil
	}
	Override("compute-optimizer", "UpdateEnrollmentStatus", enroll)
	Override("cost-optimization-hub", "UpdateEnrollmentStatus", enroll)

	Override("compute-optimizer", "GetEnrollmentStatus", func(c *Call) (map[string]any, error) {
		r := kSetting(c, "EnrollmentStatus")
		if r == nil {
			return map[string]any{"status": "Inactive", "memberAccountsEnrolled": false, "numberOfMemberAccountsOptedIn": 0}, nil
		}
		out := c.Fill(r)
		out["memberAccountsEnrolled"] = r.Data["includeMemberAccounts"] == true
		out["numberOfMemberAccountsOptedIn"] = 0
		return out, nil
	})
	Override("cost-optimization-hub", "ListEnrollmentStatuses", func(c *Call) (map[string]any, error) {
		items := []any{}
		if r := kSetting(c, "EnrollmentStatus"); r != nil {
			items = append(items, map[string]any{"accountId": c.Account, "status": r.Data["status"],
				"createdTimestamp": r.Created.Format(timeFmt), "lastUpdatedTimestamp": r.Updated.Format(timeFmt)})
		}
		return map[string]any{"items": items, "includeMemberAccounts": false}, nil
	})

	prefs := func(c *Call) map[string]any {
		out := map[string]any{"savingsEstimationMode": "BeforeDiscounts", "memberAccountDiscountVisibility": "All"}
		if r := kSetting(c, "Preferences"); r != nil {
			for k, v := range r.Data {
				out[k] = v
			}
		}
		return out
	}
	Override("cost-optimization-hub", "GetPreferences", func(c *Call) (map[string]any, error) { return prefs(c), nil })
	Override("cost-optimization-hub", "UpdatePreferences", func(c *Call) (map[string]any, error) {
		in := map[string]any{}
		for _, k := range []string{"savingsEstimationMode", "memberAccountDiscountVisibility", "preferredCommitment"} {
			if v, ok := c.In[k]; ok {
				in[k] = v
			}
		}
		kSave(c, "Preferences", in)
		return prefs(c), nil
	})
}
