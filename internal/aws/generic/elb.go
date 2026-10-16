package generic

import (
	"encoding/xml"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/nrndev/pogi/internal/core"
)

// Classic ELB: the model quirks the engine cannot infer.
func init() {
	// The DNS name is made by the service; later descriptions show it too.
	Override("elb", "CreateLoadBalancer", func(c *Call) (map[string]any, error) {
		// an IAM server certificate on a listener must exist (floci #4875); ACM ones are not checked
		var iam interface{ ServerCertExists(arn string) bool }
		if c.Hub() != nil {
			iam, _ = c.Hub().Service("iam").(interface{ ServerCertExists(arn string) bool })
		}
		for _, l := range asList(c.In["Listeners"]) {
			listener, _ := l.(map[string]any)
			arn := str(listener["SSLCertificateId"])
			if iam != nil && strings.Contains(arn, ":iam::") && strings.Contains(arn, ":server-certificate/") && !iam.ServerCertExists(arn) {
				return nil, c.Err("CertificateNotFound", "Server Certificate not found for the key: %s", arn)
			}
		}
		out, err := c.Default()
		if err != nil {
			return nil, err
		}
		dns := strings.ToLower(str(c.In["LoadBalancerName"])) + "-" + randHex(8) + ".elb.localhost"
		r := c.Res
		r.Data["DNSName"] = dns
		// A load balancer in a VPC shows the VPC of its subnets and its first security group as the source group.
		if ids := asList(c.In["Subnets"]); len(ids) > 0 {
			var sn struct {
				VPC string `xml:"subnetSet>item>vpcId"`
			}
			if ec2Describe(c, "DescribeSubnets", "SubnetId", ids[:1], &sn) && sn.VPC != "" {
				r.Data["VPCId"] = sn.VPC
			}
		}
		if ids := asList(c.In["SecurityGroups"]); len(ids) > 0 {
			var sg struct {
				Name string `xml:"securityGroupInfo>item>groupName"`
			}
			if ec2Describe(c, "DescribeSecurityGroups", "GroupId", ids[:1], &sg) && sg.Name != "" {
				r.Data["SourceSecurityGroup"] = map[string]any{"OwnerAlias": c.Account, "GroupName": sg.Name}
			}
		}
		c.Put(r)
		out["DNSName"] = dns
		return out, nil
	})
	// A load balancer lists its listeners as {Listener, PolicyNames}, and has a few defaults.
	Override("elb", "DescribeLoadBalancers", func(c *Call) (map[string]any, error) {
		out, err := c.Default()
		if err != nil {
			return nil, err
		}
		items, _ := out["LoadBalancerDescriptions"].([]any)
		for _, it := range items {
			d := it.(map[string]any)
			r := c.Get("LoadBalancer", str(d["LoadBalancerName"]))
			if r == nil {
				continue
			}
			var ls []any
			for _, l := range asList(r.Data["Listeners"]) {
				ls = append(ls, map[string]any{"Listener": l, "PolicyNames": []any{}})
			}
			d["ListenerDescriptions"] = ls
			if d["Scheme"] == nil {
				d["Scheme"] = "internet-facing"
			}
			if d["HealthCheck"] == nil {
				d["HealthCheck"] = map[string]any{"Target": "TCP:80", "Interval": 30.0, "Timeout": 5.0, "UnhealthyThreshold": 2.0, "HealthyThreshold": 10.0}
			}
			d["CanonicalHostedZoneNameID"] = "Z35SXDOTRQ7X7K"
		}
		return out, nil
	})
	// Attributes are a record of their own, with defaults.
	Override("elb", "DescribeLoadBalancerAttributes", func(c *Call) (map[string]any, error) {
		r := c.Get("LoadBalancer", str(c.In["LoadBalancerName"]))
		if r == nil {
			return nil, c.NotFound("There is no ACTIVE Load Balancer named '%s'", str(c.In["LoadBalancerName"]))
		}
		return map[string]any{"LoadBalancerAttributes": elbAttributes(r, nil)}, nil
	})
	Override("elb", "ModifyLoadBalancerAttributes", func(c *Call) (map[string]any, error) {
		r := c.Get("LoadBalancer", str(c.In["LoadBalancerName"]))
		if r == nil {
			return nil, c.NotFound("There is no ACTIVE Load Balancer named '%s'", str(c.In["LoadBalancerName"]))
		}
		attrs := elbAttributes(r, c.In["LoadBalancerAttributes"])
		r.Data["LoadBalancerAttributes"] = attrs
		c.Put(r)
		return map[string]any{"LoadBalancerName": r.Name, "LoadBalancerAttributes": attrs}, nil
	})
	// DescribeTags answers for several load balancers at once.
	Override("elb", "DescribeTags", func(c *Call) (map[string]any, error) {
		var descs []any
		for _, n := range asList(c.In["LoadBalancerNames"]) {
			r := c.Get("LoadBalancer", str(n))
			if r == nil {
				return nil, c.NotFound("There is no ACTIVE Load Balancer named '%s'", str(n))
			}
			var tags []any
			for _, k := range core.SortedKeys(r.Tags) {
				tags = append(tags, map[string]any{"Key": k, "Value": r.Tags[k]})
			}
			descs = append(descs, map[string]any{"LoadBalancerName": r.Name, "Tags": tags})
		}
		return map[string]any{"TagDescriptions": descs}, nil
	})
}

// ec2Describe asks pogi's EC2 about the resources with these ids and decodes the answer; false when it cannot.
func ec2Describe(c *Call, action, param string, ids []any, out any) bool {
	if c.Hub() == nil {
		return false
	}
	form := url.Values{"Action": {action}, "Version": {"2016-11-15"}}
	for i, id := range ids {
		form.Set(param+"."+strconv.Itoa(i+1), str(id))
	}
	status, _, body := c.Hub().Do(c.Account, c.Region, "ec2", http.MethodPost, "/",
		http.Header{"Content-Type": {"application/x-www-form-urlencoded"}}, []byte(form.Encode()))
	return status == http.StatusOK && xml.Unmarshal(body, out) == nil
}

func asList(v any) []any {
	l, _ := v.([]any)
	return l
}

// elbAttributes is the stored attributes of a load balancer with the changes in set applied over the defaults.
func elbAttributes(r *Resource, set any) map[string]any {
	attrs := map[string]any{
		"CrossZoneLoadBalancing": map[string]any{"Enabled": false},
		"ConnectionDraining":     map[string]any{"Enabled": false, "Timeout": 300.0},
		"ConnectionSettings":     map[string]any{"IdleTimeout": 60.0},
		"AccessLog":              map[string]any{"Enabled": false},
	}
	for _, src := range []any{r.Data["LoadBalancerAttributes"], set} {
		m, _ := src.(map[string]any)
		for k, v := range m {
			if sub, ok := v.(map[string]any); ok {
				cur, _ := attrs[k].(map[string]any)
				if cur == nil {
					cur = map[string]any{}
				}
				for sk, sv := range sub {
					cur[sk] = sv
				}
				attrs[k] = cur
			}
		}
	}
	return attrs
}
