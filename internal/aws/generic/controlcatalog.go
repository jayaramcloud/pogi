package generic

import (
	"slices"
)

// Control Catalog is read-only, so a handful of built-in domains, objectives,
// common controls and controls stand in for AWS's catalog (ponytail: it is a
// sample, not the full catalog).

const ccStamp = "2024-01-01T00:00:00Z"

var ccDomains = []struct{ id, name string }{{"securitydomain", "Security"}, {"resiliencedomain", "Resilience"}}

var ccObjectives = []struct{ id, name, domain string }{
	{"accessobjective", "Access control", "securitydomain"},
	{"encryptionobjective", "Encryption", "securitydomain"},
	{"backupobjective", "Backup and recovery", "resiliencedomain"},
}

var ccCommon = []struct{ id, name, objective string }{
	{"enforceencryptionatrest", "Enforce encryption at rest", "encryptionobjective"},
	{"restrictpublicaccess", "Restrict public access", "accessobjective"},
	{"enablebackups", "Enable backups", "backupobjective"},
}

var ccControls = []struct{ id, name, behavior, severity, provider, resource string }{
	{"AWS-GR_ENCRYPTED_VOLUMES", "Detect whether encryption is enabled for Amazon EBS volumes attached to Amazon EC2 instances", "DETECTIVE", "MEDIUM", "AWS", "AWS::EC2::Volume"},
	{"AWS-GR_RESTRICT_PUBLIC_S3_READ", "Disallow public read access to Amazon S3 buckets", "PREVENTIVE", "CRITICAL", "AWS", "AWS::S3::Bucket"},
	{"AWS-GR_RDS_INSTANCE_PUBLIC_ACCESS_CHECK", "Detect whether public access to Amazon RDS database instances is enabled", "DETECTIVE", "CRITICAL", "AWS", "AWS::RDS::DBInstance"},
	{"AWS-GR_EC2_VOLUME_INUSE_CHECK", "Detect whether Amazon EBS volumes are attached to Amazon EC2 instances", "DETECTIVE", "MEDIUM", "AWS", "AWS::EC2::Volume"},
}

func ccARN(kind, id string) string { return "arn:aws:controlcatalog:::" + kind + "/" + id }

func ccDomain(id string) map[string]any {
	for _, d := range ccDomains {
		if d.id == id {
			return map[string]any{"Arn": ccARN("domain", d.id), "Name": d.name}
		}
	}
	return nil
}

func ccControl(c *Call, k int) map[string]any {
	x := ccControls[k]
	return map[string]any{"Arn": ccARN("control", x.id), "Name": x.id, "Description": x.name, "Aliases": []any{x.id}, "Behavior": x.behavior, "Severity": x.severity,
		"GovernedProviders": []any{x.provider}, "GovernedResources": []any{x.resource}, "CreateTime": ccStamp}
}

func init() {
	Override("controlcatalog", "ListDomains", func(c *Call) (map[string]any, error) {
		out := []any{}
		for _, d := range ccDomains {
			out = append(out, map[string]any{"Arn": ccARN("domain", d.id), "Name": d.name, "Description": d.name + " controls", "CreateTime": ccStamp, "LastUpdateTime": ccStamp})
		}
		return map[string]any{"Domains": out}, nil
	})
	Override("controlcatalog", "ListObjectives", func(c *Call) (map[string]any, error) {
		f, _ := c.In["ObjectiveFilter"].(map[string]any)
		want := asList(f["Domains"])
		out := []any{}
		for _, o := range ccObjectives {
			if len(want) > 0 && !slices.ContainsFunc(want, func(w any) bool { return str(w.(map[string]any)["Arn"]) == ccARN("domain", o.domain) }) {
				continue
			}
			out = append(out, map[string]any{"Arn": ccARN("objective", o.id), "Name": o.name, "Description": o.name, "Domain": ccDomain(o.domain), "CreateTime": ccStamp, "LastUpdateTime": ccStamp})
		}
		return map[string]any{"Objectives": out}, nil
	})
	Override("controlcatalog", "ListCommonControls", func(c *Call) (map[string]any, error) {
		f, _ := c.In["CommonControlFilter"].(map[string]any)
		want := asList(f["Objectives"])
		out := []any{}
		for _, cc := range ccCommon {
			if len(want) > 0 && !slices.ContainsFunc(want, func(w any) bool { return str(w.(map[string]any)["Arn"]) == ccARN("objective", cc.objective) }) {
				continue
			}
			var obj struct{ id, name, domain string }
			for _, o := range ccObjectives {
				if o.id == cc.objective {
					obj = o
				}
			}
			out = append(out, map[string]any{"Arn": ccARN("common-control", cc.id), "Name": cc.name, "Description": cc.name, "Domain": ccDomain(obj.domain),
				"Objective": map[string]any{"Arn": ccARN("objective", obj.id), "Name": obj.name}, "CreateTime": ccStamp, "LastUpdateTime": ccStamp})
		}
		return map[string]any{"CommonControls": out}, nil
	})
	Override("controlcatalog", "ListControls", func(c *Call) (map[string]any, error) {
		out := []any{}
		for k := range ccControls {
			out = append(out, ccControl(c, k))
		}
		return map[string]any{"Controls": out}, nil
	})
	Override("controlcatalog", "GetControl", func(c *Call) (map[string]any, error) {
		for k := range ccControls {
			if ccARN("control", ccControls[k].id) == str(c.In["ControlArn"]) {
				d := ccControl(c, k)
				d["Implementation"] = map[string]any{"Type": "AWS::Config::ConfigRule", "Identifier": ccControls[k].id}
				d["RegionConfiguration"] = map[string]any{"Scope": "REGIONAL", "DeployableRegions": []any{"us-east-1"}}
				return d, nil
			}
		}
		return nil, c.NotFound("Control %s not found", str(c.In["ControlArn"]))
	})
}
