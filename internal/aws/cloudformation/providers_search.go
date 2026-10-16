package cloudformation

import (
	"net/url"
	"strings"
	"time"
)

// OpenSearch (and Elasticsearch) domains.

func init() {
	for _, a := range []struct{ typ, base string }{{"AWS::OpenSearchService::Domain", "/2021-01-01/opensearch"}, {"AWS::Elasticsearch::Domain", "/2015-01-01/es"}} {
		base := a.base + "/domain"
		// config is the request body for a domain's properties
		config := func(p map[string]any) map[string]any {
			in := map[string]any{}
			for k, v := range p {
				switch k {
				case "Tags":
					in["TagList"] = tagList(v)
				case "AccessPolicies":
					in[k] = jsonText(v)
				default:
					in[k] = v
				}
			}
			return in
		}
		status := func(c *pctx, name string) (map[string]any, error) {
			var out struct{ DomainStatus map[string]any }
			err := c.rest("es", "GET", base+"/"+url.PathEscape(name), nil, &out)
			return out.DomainStatus, err
		}
		attrs := func(c *pctx, name string) (map[string]any, error) {
			var st map[string]any
			var err error
			// the domain is "processing" until its endpoint is up
			waitFor(2*time.Minute, func() bool {
				if st, err = status(c, name); err != nil {
					return true
				}
				return st["Processing"] != true && str(st["Endpoint"]) != ""
			})
			if err != nil {
				return nil, err
			}
			return map[string]any{"Arn": st["ARN"], "DomainArn": st["ARN"], "DomainEndpoint": st["Endpoint"], "Id": st["DomainId"]}, nil
		}
		register(a.typ, &provider{
			create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
				name := strings.ToLower(or(str(p["DomainName"]), c.name(28, true)))
				in := config(p)
				in["DomainName"] = name
				if err := c.rest("es", "POST", base, in, nil); err != nil {
					return "", nil, err
				}
				at, err := attrs(c, name)
				return name, at, err
			},
			update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
				if str(old["DomainName"]) != str(p["DomainName"]) {
					return nil, true, nil
				}
				in := config(p)
				delete(in, "DomainName")
				delete(in, "TagList")
				if err := c.rest("es", "POST", base+"/"+url.PathEscape(phys)+"/config", in, nil); err != nil {
					return nil, false, err
				}
				at, err := attrs(c, phys)
				return at, false, err
			},
			del: func(c *pctx, phys string, p map[string]any) error {
				return c.rest("es", "DELETE", base+"/"+url.PathEscape(phys), nil, nil)
			},
		})
	}
}
