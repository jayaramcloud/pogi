package generic

import (
	"slices"
	"strings"
)

// Account management: the contact details, alternate contacts and Region
// opt-in of an account. The service is global, so records have no Region.

// accOptIn are the Regions an account has to opt in to; the rest are enabled by default.
var accOptIn = []string{"af-south-1", "ap-east-1", "ap-south-2", "ap-southeast-3", "ap-southeast-4", "ap-southeast-5", "ca-west-1", "eu-central-2", "eu-south-1", "eu-south-2",
	"il-central-1", "me-central-1", "me-south-1", "mx-central-1"}

var accRegions = []string{"af-south-1", "ap-east-1", "ap-northeast-1", "ap-northeast-2", "ap-northeast-3", "ap-south-1", "ap-south-2", "ap-southeast-1", "ap-southeast-2", "ap-southeast-3",
	"ap-southeast-4", "ap-southeast-5", "ca-central-1", "ca-west-1", "eu-central-1", "eu-central-2", "eu-north-1", "eu-south-1", "eu-south-2", "eu-west-1", "eu-west-2", "eu-west-3",
	"il-central-1", "me-central-1", "me-south-1", "mx-central-1", "sa-east-1", "us-east-1", "us-east-2", "us-west-1", "us-west-2"}

// accTarget is the account a call is about: AccountId, else the caller's.
func accTarget(c *Call) string {
	if a := str(c.In["AccountId"]); a != "" {
		return a
	}
	return c.Account
}

func accRegionStatus(c *Call, acct, region string) string {
	if r := c.Svc.find("Region", acct+"/"+region); r != nil {
		return str(r.Data["status"])
	}
	if slices.Contains(accOptIn, region) {
		return "DISABLED"
	}
	return "ENABLED_BY_DEFAULT"
}

func init() {
	// Each record is keyed by the account it describes.
	put := func(typ, key string, data map[string]any) func(*Call) {
		return func(c *Call) {
			now := c.now()
			r := &Resource{Type: typ, Key: key, Name: key, Account: c.Account, Region: c.key(), Created: now, Updated: now, Data: data}
			if old := c.Svc.find(typ, key); old != nil {
				r.Created = old.Created
			}
			c.Put(r)
		}
	}
	Override("account", "PutContactInformation", func(c *Call) (map[string]any, error) {
		put("ContactInformation", accTarget(c), c.In["ContactInformation"].(map[string]any))(c)
		return map[string]any{}, nil
	})
	Override("account", "GetContactInformation", func(c *Call) (map[string]any, error) {
		r := c.Svc.find("ContactInformation", accTarget(c))
		if r == nil {
			return nil, c.NotFound("No contact information is set for account %s", accTarget(c))
		}
		return map[string]any{"ContactInformation": r.Data}, nil
	})
	Override("account", "PutAlternateContact", func(c *Call) (map[string]any, error) {
		data := map[string]any{}
		for _, k := range []string{"AlternateContactType", "EmailAddress", "Name", "PhoneNumber", "Title"} {
			data[k] = c.In[k]
		}
		put("AlternateContact", accTarget(c)+"/"+str(c.In["AlternateContactType"]), data)(c)
		return map[string]any{}, nil
	})
	Override("account", "GetAlternateContact", func(c *Call) (map[string]any, error) {
		r := c.Svc.find("AlternateContact", accTarget(c)+"/"+str(c.In["AlternateContactType"]))
		if r == nil {
			return nil, c.NotFound("No %s alternate contact is set for account %s", strings.ToLower(str(c.In["AlternateContactType"])), accTarget(c))
		}
		return map[string]any{"AlternateContact": r.Data}, nil
	})
	Override("account", "DeleteAlternateContact", func(c *Call) (map[string]any, error) {
		r := c.Svc.find("AlternateContact", accTarget(c)+"/"+str(c.In["AlternateContactType"]))
		if r == nil {
			return nil, c.NotFound("No %s alternate contact is set for account %s", strings.ToLower(str(c.In["AlternateContactType"])), accTarget(c))
		}
		c.Svc.dropRes(r)
		return map[string]any{}, nil
	})

	Override("account", "ListRegions", func(c *Call) (map[string]any, error) {
		want := asList(c.In["RegionOptStatusContains"])
		out := []any{}
		for _, r := range accRegions {
			st := accRegionStatus(c, accTarget(c), r)
			if len(want) == 0 || slices.ContainsFunc(want, func(w any) bool { return str(w) == st }) {
				out = append(out, map[string]any{"RegionName": r, "RegionOptStatus": st})
			}
		}
		return map[string]any{"Regions": out}, nil
	})
	Override("account", "GetRegionOptStatus", func(c *Call) (map[string]any, error) {
		if !slices.Contains(accRegions, str(c.In["RegionName"])) {
			return nil, c.Invalid("%s is not a Region", str(c.In["RegionName"]))
		}
		return map[string]any{"RegionName": c.In["RegionName"], "RegionOptStatus": accRegionStatus(c, accTarget(c), str(c.In["RegionName"]))}, nil
	})
	for op, to := range map[string]string{"EnableRegion": "ENABLED", "DisableRegion": "DISABLED"} {
		Override("account", op, func(c *Call) (map[string]any, error) {
			region := str(c.In["RegionName"])
			if !slices.Contains(accRegions, region) {
				return nil, c.Invalid("%s is not a Region", region)
			}
			if st := accRegionStatus(c, accTarget(c), region); st == "ENABLED_BY_DEFAULT" {
				return nil, c.Exists("Region %s is enabled by default and cannot be changed", region)
			}
			put("Region", accTarget(c)+"/"+region, map[string]any{"status": to})(c)
			return map[string]any{}, nil
		})
	}

	Override("account", "PutAccountName", func(c *Call) (map[string]any, error) {
		put("AccountName", accTarget(c), map[string]any{"name": c.In["AccountName"]})(c)
		return map[string]any{}, nil
	})
	Override("account", "GetAccountInformation", func(c *Call) (map[string]any, error) {
		name := "pogi"
		if r := c.Svc.find("AccountName", accTarget(c)); r != nil {
			name = str(r.Data["name"])
		}
		return map[string]any{"AccountId": accTarget(c), "AccountName": name, "AccountState": "ACTIVE", "AccountCreatedDate": "2020-01-01T00:00:00Z"}, nil
	})
}
