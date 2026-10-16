package cloudformation

import (
	"net/url"
	"strconv"
	"strings"
)

// SES (v2 API) configuration sets, event destinations, identities and templates.

// renameKeys renames members of a template value where the SES API spells them differently.
func renameKeys(v any, names map[string]string) any {
	switch x := v.(type) {
	case map[string]any:
		out := map[string]any{}
		for k, e := range x {
			if to, ok := names[k]; ok {
				k = to
			}
			out[k] = renameKeys(e, names)
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			out[i] = renameKeys(e, names)
		}
		return out
	}
	return v
}

func init() {
	const sesPath = "/v2/email/"
	setOptions := map[string]string{"DeliveryOptions": "delivery-options", "SendingOptions": "sending", "ReputationOptions": "reputation-options",
		"TrackingOptions": "tracking-options", "SuppressionOptions": "suppression-options", "VdmOptions": "vdm-options", "ArchivingOptions": "archiving-options"}
	register("AWS::SES::ConfigurationSet", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			name := or(str(p["Name"]), c.name(64, false))
			in := pick(p, "DeliveryOptions", "SendingOptions", "ReputationOptions", "TrackingOptions", "SuppressionOptions", "VdmOptions", "ArchivingOptions")
			in["ConfigurationSetName"] = name
			if t := tagList(p["Tags"]); len(t) > 0 {
				in["Tags"] = t
			}
			return name, nil, c.rest("ses", "POST", sesPath+"configuration-sets", in, nil)
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			keys := make([]string, 0, len(setOptions))
			for k := range setOptions {
				keys = append(keys, k)
			}
			if differs(old, p, keys...) {
				return nil, true, nil
			}
			for k, path := range setOptions {
				if v, ok := p[k]; ok && !equal(old[k], v) {
					if err := c.rest("ses", "PUT", sesPath+"configuration-sets/"+url.PathEscape(phys)+"/"+path, v, nil); err != nil {
						return nil, false, err
					}
				}
			}
			return nil, false, nil
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.rest("ses", "DELETE", sesPath+"configuration-sets/"+url.PathEscape(phys), nil, nil)
		},
	})

	dest := func(p map[string]any) map[string]any {
		d := renameKeys(p["EventDestination"], map[string]string{"TopicARN": "TopicArn", "IAMRoleARN": "IamRoleArn", "DeliveryStreamARN": "DeliveryStreamArn"}).(map[string]any)
		// CloudFormation spells the event types in lower case, the v2 API in upper case
		var types []any
		for _, t := range list(d["MatchingEventTypes"]) {
			types = append(types, strings.ToUpper(t))
		}
		d["MatchingEventTypes"] = types
		return d
	}
	register("AWS::SES::ConfigurationSetEventDestination", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			d := dest(p)
			name := or(str(d["Name"]), c.name(64, false))
			delete(d, "Name")
			return name, nil, c.rest("ses", "POST", sesPath+"configuration-sets/"+url.PathEscape(str(p["ConfigurationSetName"]))+"/event-destinations",
				map[string]any{"EventDestinationName": name, "EventDestination": d}, nil)
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if str(old["ConfigurationSetName"]) != str(p["ConfigurationSetName"]) {
				return nil, true, nil
			}
			d := dest(p)
			delete(d, "Name")
			return nil, false, c.rest("ses", "PUT", sesPath+"configuration-sets/"+url.PathEscape(str(p["ConfigurationSetName"]))+"/event-destinations/"+url.PathEscape(phys),
				map[string]any{"EventDestination": d}, nil)
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.rest("ses", "DELETE", sesPath+"configuration-sets/"+url.PathEscape(str(p["ConfigurationSetName"]))+"/event-destinations/"+url.PathEscape(phys), nil, nil)
		},
	})

	register("AWS::SES::EmailIdentity", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			in := map[string]any{"EmailIdentity": p["EmailIdentity"]}
			if t := tagList(p["Tags"]); len(t) > 0 {
				in["Tags"] = t
			}
			var out struct {
				DkimAttributes struct{ Tokens []string }
			}
			if err := c.rest("ses", "POST", sesPath+"identities", in, &out); err != nil {
				return "", nil, err
			}
			id := str(p["EmailIdentity"])
			attrs := map[string]any{}
			for i, t := range out.DkimAttributes.Tokens {
				n := strconv.Itoa(i + 1)
				attrs["DkimDNSTokenName"+n] = t + "._domainkey." + id
				attrs["DkimDNSTokenValue"+n] = t + ".dkim.amazonses.com"
			}
			return id, attrs, nil
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.rest("ses", "DELETE", sesPath+"identities/"+url.PathEscape(phys), nil, nil)
		},
	})

	tmpl := func(p map[string]any) map[string]any {
		t, _ := p["Template"].(map[string]any)
		return map[string]any{"Subject": t["SubjectPart"], "Text": t["TextPart"], "Html": t["HtmlPart"]}
	}
	register("AWS::SES::Template", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			t, _ := p["Template"].(map[string]any)
			name := or(str(t["TemplateName"]), c.name(64, false))
			return name, nil, c.rest("ses", "POST", sesPath+"templates", map[string]any{"TemplateName": name, "TemplateContent": tmpl(p)}, nil)
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			ot, _ := old["Template"].(map[string]any)
			nt, _ := p["Template"].(map[string]any)
			if str(ot["TemplateName"]) != str(nt["TemplateName"]) {
				return nil, true, nil
			}
			return nil, false, c.rest("ses", "PUT", sesPath+"templates/"+url.PathEscape(phys), map[string]any{"TemplateContent": tmpl(p)}, nil)
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.rest("ses", "DELETE", sesPath+"templates/"+url.PathEscape(phys), nil, nil)
		},
	})
}
