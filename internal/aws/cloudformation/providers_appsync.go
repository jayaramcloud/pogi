package cloudformation

import (
	"encoding/base64"
	"net/url"
	"time"
)

// AppSync GraphQL APIs and what hangs off them, and EventBridge Scheduler schedule groups.
// AppSync reads member names without regard to case, so the template's names go as they are.

func init() {
	api := func(id string) string { return "/v1/apis/" + url.PathEscape(id) }
	body := func(p map[string]any, drop ...string) map[string]any {
		in := map[string]any{}
		for k, v := range p {
			in[k] = v
		}
		for _, k := range drop {
			delete(in, k)
		}
		return in
	}
	register("AWS::AppSync::GraphQLApi", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			in := body(p, "Tags")
			if in["Name"] == nil {
				in["Name"] = c.name(64, false)
			}
			if t := tagMap(p["Tags"]); len(t) > 0 {
				in["tags"] = t
			}
			var out struct {
				GraphqlApi struct {
					ApiId, Arn string
					Uris       map[string]string
				}
			}
			if err := c.rest("appsync", "POST", "/v1/apis", in, &out); err != nil {
				return "", nil, err
			}
			g := out.GraphqlApi
			return g.Arn, map[string]any{"ApiId": g.ApiId, "Arn": g.Arn, "GraphQLUrl": g.Uris["GRAPHQL"], "RealtimeUrl": g.Uris["REALTIME"],
				"GraphQLDns": hostOf(g.Uris["GRAPHQL"]), "RealtimeDns": hostOf(g.Uris["REALTIME"])}, nil
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			return nil, false, c.rest("appsync", "POST", api(lastSegment(phys)), body(p, "Tags"), nil)
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.rest("appsync", "DELETE", api(lastSegment(phys)), nil, nil)
		},
	})

	register("AWS::AppSync::GraphQLSchema", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			return str(p["ApiId"]) + "GraphQLSchema", nil, putSchema(c, p)
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if str(old["ApiId"]) != str(p["ApiId"]) {
				return nil, true, nil
			}
			return nil, false, putSchema(c, p)
		},
		del: func(c *pctx, phys string, p map[string]any) error { return nil }, // a schema is not deleted on its own
	})

	register("AWS::AppSync::DataSource", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			var out struct {
				DataSource struct{ DataSourceArn, Name string }
			}
			if err := c.rest("appsync", "POST", api(str(p["ApiId"]))+"/datasources", body(p, "ApiId"), &out); err != nil {
				return "", nil, err
			}
			return out.DataSource.DataSourceArn, map[string]any{"DataSourceArn": out.DataSource.DataSourceArn, "Name": out.DataSource.Name}, nil
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if str(old["ApiId"]) != str(p["ApiId"]) || str(old["Name"]) != str(p["Name"]) {
				return nil, true, nil
			}
			return nil, false, c.rest("appsync", "POST", api(str(p["ApiId"]))+"/datasources/"+url.PathEscape(str(p["Name"])), body(p, "ApiId", "Name"), nil)
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.rest("appsync", "DELETE", api(str(p["ApiId"]))+"/datasources/"+url.PathEscape(str(p["Name"])), nil, nil)
		},
	})

	resolver := func(p map[string]any) string {
		return api(str(p["ApiId"])) + "/types/" + url.PathEscape(str(p["TypeName"])) + "/resolvers"
	}
	register("AWS::AppSync::Resolver", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			var out struct {
				Resolver struct{ ResolverArn, TypeName, FieldName string }
			}
			if err := c.rest("appsync", "POST", resolver(p), body(p, "ApiId", "TypeName"), &out); err != nil {
				return "", nil, err
			}
			r := out.Resolver
			return r.ResolverArn, map[string]any{"ResolverArn": r.ResolverArn, "TypeName": r.TypeName, "FieldName": r.FieldName}, nil
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if differs(pick(old, "ApiId", "TypeName", "FieldName"), pick(p, "ApiId", "TypeName", "FieldName")) {
				return nil, true, nil
			}
			return nil, false, c.rest("appsync", "POST", resolver(p)+"/"+url.PathEscape(str(p["FieldName"])), body(p, "ApiId", "TypeName", "FieldName"), nil)
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.rest("appsync", "DELETE", resolver(p)+"/"+url.PathEscape(str(p["FieldName"])), nil, nil)
		},
	})

	register("AWS::AppSync::ApiKey", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			var out struct{ ApiKey struct{ Id string } }
			if err := c.rest("appsync", "POST", api(str(p["ApiId"]))+"/apikeys", body(p, "ApiId"), &out); err != nil {
				return "", nil, err
			}
			arn := c.arn("appsync", "apis/"+str(p["ApiId"])+"/apikeys/"+out.ApiKey.Id)
			return arn, map[string]any{"ApiKey": out.ApiKey.Id, "Arn": arn}, nil
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if str(old["ApiId"]) != str(p["ApiId"]) {
				return nil, true, nil
			}
			return nil, false, c.rest("appsync", "POST", api(str(p["ApiId"]))+"/apikeys/"+url.PathEscape(lastSegment(phys)), body(p, "ApiId"), nil)
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.rest("appsync", "DELETE", api(str(p["ApiId"]))+"/apikeys/"+url.PathEscape(lastSegment(phys)), nil, nil)
		},
	})

	register("AWS::AppSync::FunctionConfiguration", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			var out struct {
				FunctionConfiguration struct{ FunctionId, FunctionArn, Name, DataSourceName string }
			}
			if err := c.rest("appsync", "POST", api(str(p["ApiId"]))+"/functions", body(p, "ApiId"), &out); err != nil {
				return "", nil, err
			}
			f := out.FunctionConfiguration
			return f.FunctionArn, map[string]any{"FunctionArn": f.FunctionArn, "FunctionId": f.FunctionId, "Name": f.Name, "DataSourceName": f.DataSourceName}, nil
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if str(old["ApiId"]) != str(p["ApiId"]) {
				return nil, true, nil
			}
			return nil, false, c.rest("appsync", "POST", api(str(p["ApiId"]))+"/functions/"+url.PathEscape(lastSegment(phys)), body(p, "ApiId"), nil)
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.rest("appsync", "DELETE", api(str(p["ApiId"]))+"/functions/"+url.PathEscape(lastSegment(phys)), nil, nil)
		},
	})

	register("AWS::Scheduler::ScheduleGroup", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			name := or(str(p["Name"]), c.name(64, false))
			in := map[string]any{}
			if t := tagList(p["Tags"]); len(t) > 0 {
				in["Tags"] = t
			}
			var out struct{ ScheduleGroupArn string }
			if err := c.rest("scheduler", "POST", "/schedule-groups/"+url.PathEscape(name), in, &out); err != nil {
				return "", nil, err
			}
			return name, map[string]any{"Arn": out.ScheduleGroupArn, "Name": name}, nil
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.rest("scheduler", "DELETE", "/schedule-groups/"+url.PathEscape(phys), nil, nil)
		},
	})
}

func hostOf(u string) string {
	if p, err := url.Parse(u); err == nil {
		return p.Host
	}
	return ""
}

// putSchema sets a schema and waits for it to be made.
func putSchema(c *pctx, p map[string]any) error {
	def := str(p["Definition"])
	if err := c.rest("appsync", "POST", "/v1/apis/"+url.PathEscape(str(p["ApiId"]))+"/schemacreation", map[string]any{"definition": base64.StdEncoding.EncodeToString([]byte(def))}, nil); err != nil {
		return err
	}
	var st struct{ Status, Details string }
	var err error
	waitFor(30*time.Second, func() bool {
		if err = c.rest("appsync", "GET", "/v1/apis/"+url.PathEscape(str(p["ApiId"]))+"/schemacreation", nil, &st); err != nil {
			return true
		}
		return st.Status == "SUCCESS" || st.Status == "FAILED" || st.Status == "NOT_APPLICABLE"
	})
	if err == nil && st.Status == "FAILED" {
		err = validation("The schema is not valid: %s", st.Details)
	}
	return err
}
