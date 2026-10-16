package cloudformation

import (
	"encoding/base64"
	"net/http"
	"strconv"
	"strings"

	"github.com/nrndev/pogi/internal/core"
)

// CloudFront distributions, policies, origin access, key groups and functions.
// CloudFormation writes a list as a plain list; the CloudFront API as <Quantity> and <Items>.

const cfPath = "/2020-05-31/"

// cfItem names the element of each list.
var cfItem = map[string]string{"Aliases": "CNAME", "Origins": "Origin", "CacheBehaviors": "CacheBehavior", "CustomErrorResponses": "CustomErrorResponse",
	"AllowedMethods": "Method", "CachedMethods": "Method", "TrustedSigners": "AwsAccountNumber", "TrustedKeyGroups": "KeyGroup",
	"LambdaFunctionAssociations": "LambdaFunctionAssociation", "FunctionAssociations": "FunctionAssociation", "OriginCustomHeaders": "OriginCustomHeader",
	"OriginSSLProtocols": "SslProtocol", "Locations": "Location", "AccessControlAllowOrigins": "Origin", "AccessControlAllowHeaders": "Header",
	"AccessControlAllowMethods": "Method", "AccessControlExposeHeaders": "Header", "Items": "Name"}

// cfName renames the members whose CloudFormation name differs from the API's.
var cfName = map[string]string{"IPV6Enabled": "IsIPV6Enabled", "AcmCertificateArn": "ACMCertificateArn", "IamCertificateId": "IAMCertificateId",
	"OriginCustomHeaders": "CustomHeaders", "OriginSSLProtocols": "OriginSslProtocols"}

func cfList(name string, l []any, inside string) string {
	elem := cfItem[name]
	if elem == "" {
		elem = "Name"
	}
	var b strings.Builder
	b.WriteString("<" + cfName2(name) + ">")
	if name == "TrustedSigners" || name == "TrustedKeyGroups" {
		b.WriteString("<Enabled>" + strconv.FormatBool(len(l) > 0) + "</Enabled>")
	}
	b.WriteString("<Quantity>" + strconv.Itoa(len(l)) + "</Quantity>")
	if len(l) > 0 {
		b.WriteString("<Items>")
		for _, e := range l {
			b.WriteString(cf(elem, e))
		}
		b.WriteString("</Items>")
	}
	b.WriteString(inside + "</" + cfName2(name) + ">")
	return b.String()
}

func cfName2(n string) string {
	if to, ok := cfName[n]; ok {
		return to
	}
	return n
}

// cf writes a template value as CloudFront XML.
func cf(name string, v any) string {
	switch x := v.(type) {
	case []any:
		return cfList(name, x, "")
	case map[string]any:
		var b strings.Builder
		b.WriteString("<" + cfName2(name) + ">")
		for _, k := range core.SortedKeys(x) {
			switch l, isList := x[k].([]any); {
			case k == "CachedMethods" && x["AllowedMethods"] != nil: // nested in AllowedMethods
			case k == "AllowedMethods" && isList:
				cached, _ := x["CachedMethods"].([]any)
				b.WriteString(cfList(k, l, cfList("CachedMethods", cached, "")))
			default:
				b.WriteString(cf(k, x[k]))
			}
		}
		b.WriteString("</" + cfName2(name) + ">")
		return b.String()
	}
	return xmlOf(cfName2(name), v, nil)
}

func cfTags(tags any) string {
	var b strings.Builder
	for _, t := range tagList(tags) {
		b.WriteString("<Tag><Key>" + t["Key"] + "</Key><Value>" + t["Value"] + "</Value></Tag>")
	}
	if b.Len() == 0 {
		return ""
	}
	return "<Tags><Items>" + b.String() + "</Items></Tags>"
}

// cfConfig reads a resource's config and its ETag.
func (c *pctx) cfConfig(path string) (*xnode, string, error) {
	n, h, err := c.restXMLh("cloudfront", "GET", cfPath+path, "", nil)
	return n, h.Get("ETag"), err
}

func (c *pctx) cfPut(path, body, etag string) (*xnode, string, error) {
	n, h, err := c.restXMLh("cloudfront", "PUT", cfPath+path, body, http.Header{"If-Match": {etag}})
	return n, h.Get("ETag"), err
}

func (c *pctx) cfDelete(path string) error {
	_, etag, err := c.cfConfig(path + "/config")
	if err != nil {
		return err
	}
	_, _, err = c.restXMLh("cloudfront", "DELETE", cfPath+path, "", http.Header{"If-Match": {etag}})
	return err
}

func distConfig(p map[string]any, ref string, enabled *bool) string {
	cfg, _ := p["DistributionConfig"].(map[string]any)
	m := map[string]any{}
	for k, v := range cfg {
		m[k] = v
	}
	m["CallerReference"] = ref
	if enabled != nil {
		m["Enabled"] = *enabled
	}
	if _, ok := m["Comment"]; !ok {
		m["Comment"] = ""
	}
	return cf("DistributionConfig", m)
}

// cfPolicy is a resource made of one config document (policies, origin access).
func cfPolicy(path, config string) *provider {
	return &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			r, _, err := c.restXMLh("cloudfront", "POST", cfPath+path, cf(config, p[config]), nil)
			if err != nil {
				return "", nil, err
			}
			id := r.findText("Id")
			return id, map[string]any{"Id": id, "S3CanonicalUserId": r.findText("S3CanonicalUserId")}, nil
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			_, etag, err := c.cfConfig(path + "/" + phys + "/config")
			if err != nil {
				return nil, false, err
			}
			_, _, err = c.cfPut(path+"/"+phys+"/config", cf(config, p[config]), etag)
			return nil, false, err
		},
		del: func(c *pctx, phys string, p map[string]any) error { return c.cfDelete(path + "/" + phys) },
	}
}

func init() {
	register("AWS::CloudFront::Distribution", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			body := "<DistributionConfigWithTags>" + distConfig(p, core.UUID(), nil) + cfTags(p["Tags"]) + "</DistributionConfigWithTags>"
			r, _, err := c.restXMLh("cloudfront", "POST", cfPath+"distribution?WithTags", body, nil)
			if err != nil {
				return "", nil, err
			}
			id := r.findText("Id")
			return id, map[string]any{"Id": id, "DomainName": r.findText("DomainName")}, nil
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			cur, etag, err := c.cfConfig("distribution/" + phys + "/config")
			if err != nil {
				return nil, false, err
			}
			r, _, err := c.cfPut("distribution/"+phys+"/config", distConfig(p, cur.text("CallerReference"), nil), etag)
			if err != nil {
				return nil, false, err
			}
			return map[string]any{"Id": phys, "DomainName": r.findText("DomainName")}, false, nil
		},
		// a distribution is disabled before it can be deleted
		del: func(c *pctx, phys string, p map[string]any) error {
			cur, etag, err := c.cfConfig("distribution/" + phys + "/config")
			if err != nil {
				return err
			}
			off := false
			_, etag, err = c.cfPut("distribution/"+phys+"/config", distConfig(p, cur.text("CallerReference"), &off), etag)
			if err != nil {
				return err
			}
			_, _, err = c.restXMLh("cloudfront", "DELETE", cfPath+"distribution/"+phys, "", http.Header{"If-Match": {etag}})
			return err
		},
	})
	register("AWS::CloudFront::CachePolicy", cfPolicy("cache-policy", "CachePolicyConfig"))
	register("AWS::CloudFront::OriginRequestPolicy", cfPolicy("origin-request-policy", "OriginRequestPolicyConfig"))
	register("AWS::CloudFront::ResponseHeadersPolicy", cfPolicy("response-headers-policy", "ResponseHeadersPolicyConfig"))
	register("AWS::CloudFront::OriginAccessControl", cfPolicy("origin-access-control", "OriginAccessControlConfig"))
	register("AWS::CloudFront::CloudFrontOriginAccessIdentity", func() *provider {
		p := cfPolicy("origin-access-identity/cloudfront", "CloudFrontOriginAccessIdentityConfig")
		return p
	}())
	register("AWS::CloudFront::PublicKey", func() *provider {
		p := cfPolicy("public-key", "PublicKeyConfig")
		create := p.create
		p.create = func(c *pctx, props map[string]any) (string, map[string]any, error) {
			cfg, _ := props["PublicKeyConfig"].(map[string]any)
			next := map[string]any{"PublicKeyConfig": cfg}
			if cfg != nil && cfg["CallerReference"] == nil {
				m := map[string]any{"CallerReference": core.UUID()}
				for k, v := range cfg {
					m[k] = v
				}
				next["PublicKeyConfig"] = m
			}
			return create(c, next)
		}
		return p
	}())
	register("AWS::CloudFront::KeyGroup", func() *provider {
		// the key group lists its public keys as <Items><PublicKey>
		body := func(p map[string]any) string {
			cfg, _ := p["KeyGroupConfig"].(map[string]any)
			var b strings.Builder
			for _, k := range list(cfg["Items"]) {
				b.WriteString("<PublicKey>" + k + "</PublicKey>")
			}
			return "<KeyGroupConfig><Name>" + str(cfg["Name"]) + "</Name><Items>" + b.String() + "</Items><Comment>" + str(cfg["Comment"]) + "</Comment></KeyGroupConfig>"
		}
		return &provider{
			create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
				r, _, err := c.restXMLh("cloudfront", "POST", cfPath+"key-group", body(p), nil)
				if err != nil {
					return "", nil, err
				}
				id := r.findText("Id")
				return id, map[string]any{"Id": id}, nil
			},
			update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
				_, etag, err := c.cfConfig("key-group/" + phys + "/config")
				if err != nil {
					return nil, false, err
				}
				_, _, err = c.cfPut("key-group/"+phys+"/config", body(p), etag)
				return nil, false, err
			},
			del: func(c *pctx, phys string, p map[string]any) error { return c.cfDelete("key-group/" + phys) },
		}
	}())

	fnBody := func(root string, p map[string]any) string {
		code := base64.StdEncoding.EncodeToString([]byte(str(p["FunctionCode"])))
		return "<" + root + "><Name>" + str(p["Name"]) + "</Name>" + cf("FunctionConfig", p["FunctionConfig"]) + "<FunctionCode>" + code + "</FunctionCode></" + root + ">"
	}
	publish := func(c *pctx, name, etag string) (string, error) {
		r, _, err := c.restXMLh("cloudfront", "POST", cfPath+"function/"+name+"/publish", "", http.Header{"If-Match": {etag}})
		return r.findText("FunctionARN"), err
	}
	register("AWS::CloudFront::Function", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			name := or(str(p["Name"]), c.name(64, false))
			p = map[string]any{"Name": name, "FunctionCode": p["FunctionCode"], "FunctionConfig": p["FunctionConfig"], "AutoPublish": p["AutoPublish"]}
			r, h, err := c.restXMLh("cloudfront", "POST", cfPath+"function", fnBody("CreateFunctionRequest", p), nil)
			if err != nil {
				return "", nil, err
			}
			arn, stage := r.findText("FunctionARN"), "DEVELOPMENT"
			if str(p["AutoPublish"]) != "false" {
				if _, err := publish(c, name, h.Get("ETag")); err != nil {
					return "", nil, err
				}
				stage = "LIVE"
			}
			return arn, map[string]any{"FunctionARN": arn, "FunctionMetadata.FunctionARN": arn, "Stage": stage}, nil
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if str(old["Name"]) != str(p["Name"]) {
				return nil, true, nil
			}
			name := lastSegment(phys)
			_, etag, err := c.cfConfig("function/" + name + "/describe")
			if err != nil {
				return nil, false, err
			}
			_, etag, err = c.cfPut("function/"+name, fnBody("UpdateFunctionRequest", p), etag)
			if err != nil {
				return nil, false, err
			}
			if str(p["AutoPublish"]) != "false" {
				_, err = publish(c, name, etag)
			}
			return nil, false, err
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			name := lastSegment(phys)
			_, etag, err := c.cfConfig("function/" + name + "/describe")
			if err != nil {
				return err
			}
			_, _, err = c.restXMLh("cloudfront", "DELETE", cfPath+"function/"+name, "", http.Header{"If-Match": {etag}})
			return err
		},
	})
}
