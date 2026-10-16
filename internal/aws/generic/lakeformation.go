package generic

import (
	"encoding/json"
	"slices"

	"github.com/nrndev/pogi/internal/core"
)

// Lake Formation: the data lake settings (one record per catalog), registered
// resources, LF-tags and the permissions granted to principals, which are kept
// per principal and resource and merged on grant, trimmed on revoke. Nothing is
// enforced on any query.
func init() {
	// ---- data lake settings
	Override("lakeformation", "PutDataLakeSettings", func(c *Call) (map[string]any, error) {
		now := c.now()
		r := &Resource{Type: "DataLakeSetting", Key: "default", Account: c.Account, Region: c.Region, Created: now, Updated: now, Data: map[string]any{"DataLakeSettings": c.In["DataLakeSettings"]}}
		if old := c.Get("DataLakeSetting", "default"); old != nil {
			r.Created = old.Created
		}
		c.Put(r)
		return map[string]any{}, nil
	})
	Override("lakeformation", "GetDataLakeSettings", func(c *Call) (map[string]any, error) {
		if r := c.Get("DataLakeSetting", "default"); r != nil {
			return map[string]any{"DataLakeSettings": r.Data["DataLakeSettings"]}, nil
		}
		all := []any{map[string]any{"Principal": map[string]any{"DataLakePrincipalIdentifier": "IAM_ALLOWED_PRINCIPALS"}, "Permissions": []any{"ALL"}}}
		return map[string]any{"DataLakeSettings": map[string]any{"DataLakeAdmins": []any{}, "CreateDatabaseDefaultPermissions": all, "CreateTableDefaultPermissions": all,
			"TrustedResourceOwners": []any{}, "AllowExternalDataFiltering": false, "ExternalDataFilteringAllowList": []any{}, "AuthorizedSessionTagValueList": []any{}, "Parameters": map[string]any{"CROSS_ACCOUNT_VERSION": "4"}}}, nil
	})

	// ---- LF-tags
	Override("lakeformation", "CreateLFTag", func(c *Call) (map[string]any, error) {
		if _, ok := c.In["CatalogId"]; !ok {
			c.In["CatalogId"] = c.Account
		}
		return c.Default()
	})
	Override("lakeformation", "UpdateLFTag", func(c *Call) (map[string]any, error) {
		r := c.Get("LFTag", str(c.In["TagKey"]))
		if r == nil {
			return nil, c.NotFound("LF-Tag %s not found.", str(c.In["TagKey"]))
		}
		var vals []any
		for _, v := range asList(r.Data["TagValues"]) {
			if !slices.Contains(asList(c.In["TagValuesToDelete"]), v) {
				vals = append(vals, v)
			}
		}
		for _, v := range asList(c.In["TagValuesToAdd"]) {
			if !slices.Contains(vals, v) {
				vals = append(vals, v)
			}
		}
		r.Data["TagValues"] = orEmpty(vals)
		c.Put(r)
		return map[string]any{}, nil
	})

	// ---- permissions
	Override("lakeformation", "GrantPermissions", func(c *Call) (map[string]any, error) { return lfChange(c, c.In, true) })
	Override("lakeformation", "RevokePermissions", func(c *Call) (map[string]any, error) { return lfChange(c, c.In, false) })
	for op, grant := range map[string]bool{"BatchGrantPermissions": true, "BatchRevokePermissions": false} {
		Override("lakeformation", op, func(c *Call) (map[string]any, error) {
			var failures []any
			for _, e := range asList(c.In["Entries"]) {
				em := e.(map[string]any)
				if _, err := lfChange(c, em, grant); err != nil {
					failures = append(failures, map[string]any{"RequestEntry": em, "Error": map[string]any{"ErrorCode": core.AsError(err).Code, "ErrorMessage": core.AsError(err).Msg}})
				}
			}
			return map[string]any{"Failures": orEmpty(failures)}, nil
		})
	}
	Override("lakeformation", "ListPermissions", func(c *Call) (map[string]any, error) {
		want, _ := c.In["Principal"].(map[string]any)
		var out []any
		for _, r := range c.List("Permission") {
			p, _ := r.Data["Principal"].(map[string]any)
			if want != nil && p["DataLakePrincipalIdentifier"] != want["DataLakePrincipalIdentifier"] {
				continue
			}
			if res, ok := c.In["Resource"].(map[string]any); ok && lfResourceKey(res) != r.IDs["resource"] {
				continue
			}
			if rt := str(c.In["ResourceType"]); rt != "" && rt != lfResourceType(r.Data["Resource"]) {
				continue
			}
			out = append(out, map[string]any{"Principal": r.Data["Principal"], "Resource": r.Data["Resource"], "Permissions": r.Data["Permissions"],
				"PermissionsWithGrantOption": orEmpty(asList(r.Data["PermissionsWithGrantOption"])), "LastUpdated": r.Updated.Format(timeFmt), "LastUpdatedBy": c.Account})
		}
		page, next, err := pageOf(c, out, "NextToken", "MaxResults")
		if err != nil {
			return nil, err
		}
		res := map[string]any{"PrincipalResourcePermissions": page}
		if next != "" {
			res["NextToken"] = next
		}
		return res, nil
	})

	// ---- LF-tags on resources
	Override("lakeformation", "AddLFTagsToResource", func(c *Call) (map[string]any, error) {
		res, _ := c.In["Resource"].(map[string]any)
		key := lfResourceKey(res)
		r := c.Get("ResourceTags", key)
		if r == nil {
			now := c.now()
			r = &Resource{Type: "ResourceTags", Key: key, IDs: map[string]string{"resource": key}, Account: c.Account, Region: c.Region, Created: now, Updated: now, Data: map[string]any{"Resource": res, "Tags": map[string]any{}}}
		}
		tags := r.Data["Tags"].(map[string]any)
		var failures []any
		for _, t := range asList(c.In["LFTags"]) {
			tm := t.(map[string]any)
			def := c.Get("LFTag", str(tm["TagKey"]))
			var bad bool
			if def == nil {
				bad = true
			} else {
				for _, v := range asList(tm["TagValues"]) {
					bad = bad || !slices.Contains(asList(def.Data["TagValues"]), v)
				}
			}
			if bad {
				failures = append(failures, map[string]any{"LFTag": tm, "Error": map[string]any{"ErrorCode": "EntityNotFoundException", "ErrorMessage": "LF-Tag or value not found: " + str(tm["TagKey"])}})
				continue
			}
			tags[str(tm["TagKey"])] = tm["TagValues"]
		}
		r.Data["Tags"] = tags
		c.Put(r)
		return map[string]any{"Failures": orEmpty(failures)}, nil
	})
	Override("lakeformation", "RemoveLFTagsFromResource", func(c *Call) (map[string]any, error) {
		res, _ := c.In["Resource"].(map[string]any)
		if r := c.Get("ResourceTags", lfResourceKey(res)); r != nil {
			for _, t := range asList(c.In["LFTags"]) {
				delete(r.Data["Tags"].(map[string]any), str(t.(map[string]any)["TagKey"]))
			}
			c.Put(r)
		}
		return map[string]any{"Failures": []any{}}, nil
	})
	Override("lakeformation", "GetResourceLFTags", func(c *Call) (map[string]any, error) {
		res, _ := c.In["Resource"].(map[string]any)
		var pairs []any
		if r := c.Get("ResourceTags", lfResourceKey(res)); r != nil {
			tags := r.Data["Tags"].(map[string]any)
			for _, k := range core.SortedKeys(tags) {
				pairs = append(pairs, map[string]any{"CatalogId": c.Account, "TagKey": k, "TagValues": tags[k]})
			}
		}
		out := map[string]any{}
		switch lfResourceType(res) {
		case "DATABASE":
			out["LFTagOnDatabase"] = orEmpty(pairs)
		case "TABLE":
			out["LFTagsOnTable"] = orEmpty(pairs)
		}
		return out, nil
	})
}

// lfResourceKey identifies a resource given in a request: its JSON, which sorts the members.
func lfResourceKey(res map[string]any) string {
	b, _ := json.Marshal(res)
	return string(b)
}

func lfResourceType(v any) string {
	m, _ := v.(map[string]any)
	switch {
	case m["Catalog"] != nil:
		return "CATALOG"
	case m["Database"] != nil:
		return "DATABASE"
	case m["Table"] != nil, m["TableWithColumns"] != nil:
		return "TABLE"
	case m["DataLocation"] != nil:
		return "DATA_LOCATION"
	case m["LFTag"] != nil, m["LFTagExpression"] != nil:
		return "LF_TAG"
	case m["LFTagPolicy"] != nil:
		return "LF_TAG_POLICY"
	}
	return ""
}

// lfChange grants or revokes: permissions and grant options are merged into, or taken out of, the principal's record for the resource.
func lfChange(c *Call, in map[string]any, grant bool) (map[string]any, error) {
	p, _ := in["Principal"].(map[string]any)
	res, _ := in["Resource"].(map[string]any)
	if str(p["DataLakePrincipalIdentifier"]) == "" || len(res) == 0 {
		return nil, c.Invalid("A principal and a resource are required.")
	}
	if len(asList(in["Permissions"])) == 0 {
		return nil, c.Invalid("At least one permission is required.")
	}
	key := str(p["DataLakePrincipalIdentifier"]) + "|" + lfResourceKey(res)
	r := c.Get("Permission", key)
	if r == nil {
		if !grant {
			return map[string]any{}, nil
		}
		now := c.now()
		r = &Resource{Type: "Permission", Key: key, IDs: map[string]string{"principal": str(p["DataLakePrincipalIdentifier"]), "resource": lfResourceKey(res)}, Account: c.Account, Region: c.Region, Created: now, Updated: now,
			Data: map[string]any{"Principal": p, "Resource": res, "Permissions": []any{}, "PermissionsWithGrantOption": []any{}}}
	}
	for _, f := range [][2]string{{"Permissions", "Permissions"}, {"PermissionsWithGrantOption", "PermissionsWithGrantOption"}} {
		cur := asList(r.Data[f[0]])
		for _, perm := range asList(in[f[1]]) {
			switch has := slices.Contains(cur, perm); {
			case grant && !has:
				cur = append(cur, perm)
			case !grant && has:
				cur = slices.DeleteFunc(cur, func(x any) bool { return x == perm })
			}
		}
		r.Data[f[0]] = orEmpty(cur)
	}
	r.Updated = c.now()
	if len(asList(r.Data["Permissions"]))+len(asList(r.Data["PermissionsWithGrantOption"])) == 0 {
		c.Svc.Delete(c.Account, c.Region, "Permission", key)
	} else {
		c.Put(r)
	}
	return map[string]any{}, nil
}
