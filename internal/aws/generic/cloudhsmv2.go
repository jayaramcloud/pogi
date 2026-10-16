package generic

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// CloudHSM clusters keep their HSMs inside them. A cluster starts UNINITIALIZED
// (what Terraform waits for), InitializeCluster makes it INITIALIZED, and it
// cannot be deleted while it still has HSMs. No HSM runs (ponytail: HSMs are records).

func hsmID(prefix string) string {
	return prefix + "-" + strings.ToLower(core.RandString(11, core.UpperNum))
}

func hsmCluster(c *Call, id string) (*Resource, error) {
	if r := c.Get("Cluster", id); r != nil {
		return r, nil
	}
	return nil, c.Err("CloudHsmResourceNotFoundException", "The Cluster with ID %s was not found", id)
}

func hsmClusterOut(r *Resource) map[string]any {
	out := map[string]any{"ClusterId": r.ID, "HsmType": r.Data["HsmType"], "State": r.Data["State"], "SubnetMapping": r.Data["SubnetMapping"], "VpcId": r.Data["VpcId"],
		"SecurityGroup": r.Data["SecurityGroup"], "BackupPolicy": "DEFAULT", "BackupRetentionPolicy": r.Data["BackupRetentionPolicy"], "Mode": "FIPS", "NetworkType": "IPV4",
		"CreateTimestamp": r.Created.Format(time.RFC3339Nano), "Hsms": r.Data["Hsms"], "Certificates": map[string]any{}}
	if out["Hsms"] == nil {
		out["Hsms"] = []any{}
	}
	if r.Data["SourceBackupId"] != nil {
		out["SourceBackupId"] = r.Data["SourceBackupId"]
	}
	for _, k := range core.SortedKeys(r.Tags) {
		out["TagList"] = append(asList(out["TagList"]), map[string]any{"Key": k, "Value": r.Tags[k]})
	}
	return out
}

func init() {
	Override("cloudhsmv2", "CreateCluster", func(c *Call) (map[string]any, error) {
		id, vpc := hsmID("cluster"), "vpc-"+randHex(8)
		mapping := map[string]any{}
		for i, s := range asList(c.In["SubnetIds"]) {
			mapping[fmt.Sprintf("%s%c", c.Region, 'a'+i)] = s
		}
		now := c.now()
		_, add, _ := takeTags(c.op.in, c.In)
		retention := c.In["BackupRetentionPolicy"]
		if retention == nil {
			retention = map[string]any{"Type": "DAYS", "Value": "90"}
		}
		r := &Resource{Type: "Cluster", Key: id, IDs: map[string]string{"ClusterId": id}, ID: id, Name: id, Account: c.Account, Region: c.key(), Created: now, Updated: now,
			Data: map[string]any{"HsmType": c.In["HsmType"], "State": "UNINITIALIZED", "SubnetMapping": mapping, "VpcId": vpc, "SecurityGroup": "sg-" + randHex(8), "BackupRetentionPolicy": retention,
				"SourceBackupId": c.In["SourceBackupId"]}}
		applyTags(r, add, nil)
		c.Put(r)
		c.Res = r
		return map[string]any{"Cluster": hsmClusterOut(r)}, nil
	})
	Override("cloudhsmv2", "DescribeClusters", func(c *Call) (map[string]any, error) {
		f, _ := c.In["Filters"].(map[string]any)
		out := []any{}
		for _, r := range c.List("Cluster") {
			ok := true
			for key, field := range map[string]string{"clusterIds": "ClusterId", "states": "State", "vpcIds": "VpcId"} {
				if want := asList(f[key]); len(want) > 0 {
					have := r.ID
					if field != "ClusterId" {
						have = str(r.Data[field])
					}
					ok = ok && slices.ContainsFunc(want, func(w any) bool { return str(w) == have })
				}
			}
			if ok {
				out = append(out, hsmClusterOut(r))
			}
		}
		return map[string]any{"Clusters": out}, nil
	})
	Override("cloudhsmv2", "ModifyCluster", func(c *Call) (map[string]any, error) {
		r, err := hsmCluster(c, str(c.In["ClusterId"]))
		if err != nil {
			return nil, err
		}
		for _, k := range []string{"HsmType", "BackupRetentionPolicy"} {
			if v, ok := c.In[k]; ok {
				r.Data[k] = v
			}
		}
		r.Updated = c.now()
		c.Put(r)
		return map[string]any{"Cluster": hsmClusterOut(r)}, nil
	})
	Override("cloudhsmv2", "InitializeCluster", func(c *Call) (map[string]any, error) {
		r, err := hsmCluster(c, str(c.In["ClusterId"]))
		if err != nil {
			return nil, err
		}
		r.Data["State"] = "INITIALIZED"
		c.Put(r)
		return map[string]any{"State": "INITIALIZED", "StateMessage": "The cluster is initialized."}, nil
	})
	Override("cloudhsmv2", "DeleteCluster", func(c *Call) (map[string]any, error) {
		r, err := hsmCluster(c, str(c.In["ClusterId"]))
		if err != nil {
			return nil, err
		}
		if len(asList(r.Data["Hsms"])) > 0 {
			return nil, c.Err("CloudHsmInvalidRequestException", "The Cluster %s still has HSMs; delete them first", r.ID)
		}
		c.Svc.Delete(c.Account, c.Region, "Cluster", r.Key)
		r.Data["State"] = "DELETED"
		return map[string]any{"Cluster": hsmClusterOut(r)}, nil
	})

	Override("cloudhsmv2", "CreateHsm", func(c *Call) (map[string]any, error) {
		r, err := hsmCluster(c, str(c.In["ClusterId"]))
		if err != nil {
			return nil, err
		}
		az := str(c.In["AvailabilityZone"])
		mapping, _ := r.Data["SubnetMapping"].(map[string]any)
		subnet, ok := mapping[az]
		if !ok {
			return nil, c.Err("CloudHsmInvalidRequestException", "The Availability Zone %s is not in the cluster's subnets", az)
		}
		for _, h := range asList(r.Data["Hsms"]) {
			if h.(map[string]any)["AvailabilityZone"] == az {
				return nil, c.Err("CloudHsmInvalidRequestException", "The cluster already has an HSM in %s", az)
			}
		}
		ip := str(c.In["IpAddress"])
		if ip == "" {
			ip = fmt.Sprintf("10.0.%d.%d", len(asList(r.Data["Hsms"])), 10+len(asList(r.Data["Hsms"])))
		}
		h := map[string]any{"HsmId": hsmID("hsm"), "ClusterId": r.ID, "AvailabilityZone": az, "SubnetId": subnet, "EniId": "eni-" + randHex(8), "EniIp": ip, "HsmType": r.Data["HsmType"], "State": "ACTIVE", "StateMessage": "HSM created."}
		r.Data["Hsms"] = append(asList(r.Data["Hsms"]), h)
		c.Put(r)
		return map[string]any{"Hsm": h}, nil
	})
	Override("cloudhsmv2", "DeleteHsm", func(c *Call) (map[string]any, error) {
		r, err := hsmCluster(c, str(c.In["ClusterId"]))
		if err != nil {
			return nil, err
		}
		l := asList(r.Data["Hsms"])
		i := slices.IndexFunc(l, func(e any) bool {
			h := e.(map[string]any)
			return c.In["HsmId"] != nil && h["HsmId"] == c.In["HsmId"] || c.In["EniId"] != nil && h["EniId"] == c.In["EniId"] || c.In["EniIp"] != nil && h["EniIp"] == c.In["EniIp"]
		})
		if i < 0 {
			return nil, c.Err("CloudHsmResourceNotFoundException", "The HSM was not found in cluster %s", r.ID)
		}
		id := l[i].(map[string]any)["HsmId"]
		r.Data["Hsms"] = slices.Delete(l, i, i+1)
		c.Put(r)
		return map[string]any{"HsmId": id}, nil
	})
}
