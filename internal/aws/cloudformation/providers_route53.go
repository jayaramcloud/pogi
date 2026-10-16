package cloudformation

import (
	"net/url"
	"strings"

	"github.com/nrndev/pogi/internal/core"
)

// Route 53 hosted zones, record sets and health checks.

const r53 = "/2013-04-01/"

var r53Items = map[string]string{"ResourceRecords": "ResourceRecord", "ChildHealthChecks": "ChildHealthCheck", "Regions": "Region"}

// zoneID is the hosted zone of a record: its id, or found by its name.
func zoneID(c *pctx, p map[string]any) (string, error) {
	if id := str(p["HostedZoneId"]); id != "" {
		return strings.TrimPrefix(id, "/hostedzone/"), nil
	}
	name := str(p["HostedZoneName"])
	r, err := c.restXML("route53", "GET", r53+"hostedzonesbyname?maxitems=1&dnsname="+url.QueryEscape(name), "")
	if err != nil {
		return "", err
	}
	z := r.get("HostedZones", "HostedZone")
	if z == nil || strings.TrimSuffix(z.text("Name"), ".") != strings.TrimSuffix(name, ".") {
		return "", validation("Hosted zone %s was not found", name)
	}
	return strings.TrimPrefix(z.text("Id"), "/hostedzone/"), nil
}

// recordSet is the XML of one record set of a template.
func recordSet(p map[string]any) string {
	m := pick(p, "Name", "Type", "SetIdentifier", "Weight", "Region", "Failover", "MultiValueAnswer", "TTL", "AliasTarget", "HealthCheckId", "GeoLocation")
	var rrs []any
	for _, v := range list(p["ResourceRecords"]) {
		rrs = append(rrs, map[string]any{"Value": v})
	}
	if rrs != nil {
		m["ResourceRecords"] = rrs
	}
	return xmlOf("ResourceRecordSet", m, r53Items)
}

func changeRecords(c *pctx, zone string, changes ...[2]string) error {
	var b strings.Builder
	b.WriteString(`<ChangeResourceRecordSetsRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/"><ChangeBatch><Changes>`)
	for _, ch := range changes {
		b.WriteString("<Change><Action>" + ch[0] + "</Action>" + ch[1] + "</Change>")
	}
	b.WriteString("</Changes></ChangeBatch></ChangeResourceRecordSetsRequest>")
	_, err := c.restXML("route53", "POST", r53+"hostedzone/"+zone+"/rrset", b.String())
	return err
}

func r53Tags(c *pctx, kind, id string, old, now any) error {
	o, n := tagMap(old), tagMap(now)
	var b strings.Builder
	for _, k := range core.SortedKeys(n) {
		if v, ok := o[k]; !ok || v != n[k] {
			b.WriteString(xmlOf("Tag", map[string]any{"Key": k, "Value": n[k]}, nil))
		}
	}
	var rm strings.Builder
	for _, k := range core.SortedKeys(o) {
		if _, ok := n[k]; !ok {
			rm.WriteString("<Key>" + k + "</Key>")
		}
	}
	if b.Len() == 0 && rm.Len() == 0 {
		return nil
	}
	body := `<ChangeTagsForResourceRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">`
	if b.Len() > 0 {
		body += "<AddTags>" + b.String() + "</AddTags>"
	}
	if rm.Len() > 0 {
		body += "<RemoveTagKeys>" + rm.String() + "</RemoveTagKeys>"
	}
	_, err := c.restXML("route53", "POST", r53+"tags/"+kind+"/"+id, body+"</ChangeTagsForResourceRequest>")
	return err
}

func init() {
	register("AWS::Route53::HostedZone", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			name := str(p["Name"])
			in := map[string]any{"Name": name, "CallerReference": core.UUID()}
			if cfg, ok := p["HostedZoneConfig"].(map[string]any); ok {
				in["HostedZoneConfig"] = pick(cfg, "Comment")
			}
			if vpcs := listMaps(p["VPCs"]); len(vpcs) > 0 {
				in["VPC"] = pick(vpcs[0], "VPCId", "VPCRegion")
			}
			r, err := c.restXML("route53", "POST", r53+"hostedzone", xmlOf("CreateHostedZoneRequest", in, nil))
			if err != nil {
				return "", nil, err
			}
			id := strings.TrimPrefix(r.get("HostedZone").text("Id"), "/hostedzone/")
			var ns []any
			for _, n := range r.get("DelegationSet", "NameServers").Kids {
				ns = append(ns, strings.TrimSpace(n.Text))
			}
			if ns == nil {
				ns = []any{}
			}
			if err := r53Tags(c, "hostedzone", id, nil, p["HostedZoneTags"]); err != nil {
				return "", nil, err
			}
			return id, map[string]any{"Id": id, "NameServers": ns}, nil
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if differs(old, p, "HostedZoneTags") {
				return nil, true, nil
			}
			return nil, false, r53Tags(c, "hostedzone", phys, old["HostedZoneTags"], p["HostedZoneTags"])
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			_, err := c.restXML("route53", "DELETE", r53+"hostedzone/"+phys, "")
			return err
		},
	})

	register("AWS::Route53::RecordSet", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			zone, err := zoneID(c, p)
			if err != nil {
				return "", nil, err
			}
			return str(p["Name"]), nil, changeRecords(c, zone, [2]string{"CREATE", recordSet(p)})
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if str(old["Name"]) != str(p["Name"]) || str(old["Type"]) != str(p["Type"]) || str(old["SetIdentifier"]) != str(p["SetIdentifier"]) ||
				str(old["HostedZoneId"]) != str(p["HostedZoneId"]) || str(old["HostedZoneName"]) != str(p["HostedZoneName"]) {
				return nil, true, nil
			}
			zone, err := zoneID(c, p)
			if err != nil {
				return nil, false, err
			}
			return nil, false, changeRecords(c, zone, [2]string{"UPSERT", recordSet(p)})
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			zone, err := zoneID(c, p)
			if err != nil {
				return err
			}
			return changeRecords(c, zone, [2]string{"DELETE", recordSet(p)})
		},
	})
	register("AWS::Route53::RecordSetGroup", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			zone, err := zoneID(c, p)
			if err != nil {
				return "", nil, err
			}
			var ch [][2]string
			for _, r := range listMaps(p["RecordSets"]) {
				ch = append(ch, [2]string{"CREATE", recordSet(r)})
			}
			return c.name(64, false), nil, changeRecords(c, zone, ch...)
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if str(old["HostedZoneId"]) != str(p["HostedZoneId"]) || str(old["HostedZoneName"]) != str(p["HostedZoneName"]) {
				return nil, true, nil
			}
			zone, err := zoneID(c, p)
			if err != nil {
				return nil, false, err
			}
			keep := map[string]bool{}
			var ch [][2]string
			for _, r := range listMaps(p["RecordSets"]) {
				keep[str(r["Name"])+"|"+str(r["Type"])+"|"+str(r["SetIdentifier"])] = true
				ch = append(ch, [2]string{"UPSERT", recordSet(r)})
			}
			for _, r := range listMaps(old["RecordSets"]) {
				if !keep[str(r["Name"])+"|"+str(r["Type"])+"|"+str(r["SetIdentifier"])] {
					ch = append(ch, [2]string{"DELETE", recordSet(r)})
				}
			}
			return nil, false, changeRecords(c, zone, ch...)
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			zone, err := zoneID(c, p)
			if err != nil {
				return err
			}
			var ch [][2]string
			for _, r := range listMaps(p["RecordSets"]) {
				ch = append(ch, [2]string{"DELETE", recordSet(r)})
			}
			return changeRecords(c, zone, ch...)
		},
	})

	register("AWS::Route53::HealthCheck", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			in := map[string]any{"CallerReference": core.UUID(), "HealthCheckConfig": p["HealthCheckConfig"]}
			r, err := c.restXML("route53", "POST", r53+"healthcheck", xmlOf("CreateHealthCheckRequest", in, r53Items))
			if err != nil {
				return "", nil, err
			}
			id := r.get("HealthCheck").text("Id")
			if err := r53Tags(c, "healthcheck", id, nil, p["HealthCheckTags"]); err != nil {
				return "", nil, err
			}
			return id, map[string]any{"HealthCheckId": id}, nil
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if differs(old, p, "HealthCheckTags") {
				return nil, true, nil
			}
			return nil, false, r53Tags(c, "healthcheck", phys, old["HealthCheckTags"], p["HealthCheckTags"])
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			_, err := c.restXML("route53", "DELETE", r53+"healthcheck/"+phys, "")
			return err
		},
	})
}
