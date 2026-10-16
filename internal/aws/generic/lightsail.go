package generic

import (
	"crypto/ed25519"
	"crypto/md5"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/binary"
	"encoding/pem"
	"fmt"
	"strings"

	"github.com/nrndev/pogi/internal/core"
)

// Lightsail: instances, static IPs and key pairs, as metadata. Every change
// returns an operation that has already Succeeded (GetOperation shows it), so
// the waiters of the SDKs and Terraform do not hang. The other resources
// (buckets, disks, domains ...) are the engine's plain CRUD.
func init() {
	Derive("lightsail", "Instance", func(r *Resource) map[string]any {
		var s string
		switch r.Data["_phase"] {
		case "stop":
			s = Step(r, "inst", "stopping", "stopped")
		case "reboot":
			s = Step(r, "inst", "stopping", "pending", "running")
		default: // creating, or starting again
			s = Step(r, "inst", "pending", "running")
		}
		return map[string]any{"state": lsState(s)}
	})

	Override("lightsail", "CreateInstances", func(c *Call) (map[string]any, error) {
		var ops []any
		names := asList(c.In["instanceNames"])
		for _, n := range names {
			if c.Get("Instance", str(n)) != nil {
				return nil, c.Exists("Instance %s already exists", str(n))
			}
		}
		for _, n := range names {
			name := str(n)
			az := str(c.In["availabilityZone"])
			if az == "" {
				az = c.Region + "a"
			}
			bundle := orDefault(c.In["bundleId"], "nano_3_0").(string)
			cpu, ram, disk, transfer := lsBundle(bundle)
			blueprint := str(c.In["blueprintId"])
			user := "ubuntu"
			if strings.HasPrefix(blueprint, "amazon_linux") {
				user = "ec2-user"
			}
			data := map[string]any{
				"supportCode": fmt.Sprintf("%d/i-%s", 100000000000+randInt(899999999999), randHex(17)), "resourceType": "Instance",
				"location": map[string]any{"availabilityZone": az, "regionName": c.Region}, "blueprintId": blueprint, "blueprintName": strings.ToUpper(blueprint[:min(1, len(blueprint))]) + strings.ReplaceAll(blueprint[min(1, len(blueprint)):], "_", " "),
				"bundleId": bundle, "isStaticIp": false, "privateIpAddress": fmt.Sprintf("172.26.%d.%d", randInt(250), 1+randInt(250)), "publicIpAddress": lsIP(),
				"username": user, "ipAddressType": orDefault(c.In["ipAddressType"], "dualstack"), "ipv6Addresses": []any{},
				"hardware": map[string]any{"cpuCount": cpu, "ramSizeInGb": ram, "disks": []any{map[string]any{"name": "Disk-" + name + "-system", "sizeInGb": disk, "isSystemDisk": true,
					"iops": 3000.0, "path": "/dev/xvda", "attachedTo": name, "attachmentState": "attached", "resourceType": "Disk", "arn": c.arnOf("Disk/"+core.UUID(), nil)}}},
				"networking": map[string]any{"monthlyTransfer": map[string]any{"gbPerMonthAllocated": transfer}, "ports": lsPorts()},
				"addOns":     orEmpty(asList(c.In["addOns"])), "metadataOptions": map[string]any{"state": "applied", "httpTokens": "optional", "httpEndpoint": "enabled", "httpPutResponseHopLimit": 1.0, "httpProtocolIpv6": "disabled"},
				"_phase": "create",
			}
			if k := str(c.In["keyPairName"]); k != "" {
				data["sshKeyName"] = k
			} else {
				data["sshKeyName"] = "LightsailDefaultKeyPair"
			}
			now := c.now()
			r := &Resource{Type: "Instance", Key: name, IDs: map[string]string{"instanceName": name}, Name: name, ARN: c.arnOf("Instance/"+core.UUID(), nil), Account: c.Account, Region: c.Region,
				Data: data, Tags: tagMap(c.In["tags"]), Created: now, Updated: now}
			if len(r.Tags) == 0 {
				r.Tags = nil
			}
			c.Put(r)
			ops = append(ops, lsOp(c, name, "Instance", "CreateInstance"))
		}
		return map[string]any{"operations": orEmpty(ops)}, nil
	})
	Override("lightsail", "DeleteInstance", func(c *Call) (map[string]any, error) {
		name := str(c.In["instanceName"])
		if _, err := c.Default(); err != nil {
			return nil, c.NotFound("The %s instance does not exist.", name)
		}
		for _, ip := range c.List("StaticIp") { // the static IP stays, detached
			if ip.Data["attachedTo"] == name {
				ip.Data["attachedTo"], ip.Data["isAttached"] = nil, false
				c.Put(ip)
			}
		}
		return map[string]any{"operations": []any{lsOp(c, name, "Instance", "DeleteInstance")}}, nil
	})
	for op, phase := range map[string]string{"StartInstance": "start", "StopInstance": "stop", "RebootInstance": "reboot"} {
		Override("lightsail", op, func(c *Call) (map[string]any, error) {
			r := c.Get("Instance", str(c.In["instanceName"]))
			if r == nil {
				return nil, c.NotFound("The %s instance does not exist.", str(c.In["instanceName"]))
			}
			r.Data["_phase"] = phase
			if phase == "start" {
				r.Data["_phase"] = "create"
			}
			Begin(r, "inst")
			c.Put(r)
			return map[string]any{"operations": []any{lsOp(c, r.Name, "Instance", c.Op)}}, nil
		})
	}
	Override("lightsail", "GetInstanceState", func(c *Call) (map[string]any, error) {
		r := c.Get("Instance", str(c.In["instanceName"]))
		if r == nil {
			return nil, c.NotFound("The %s instance does not exist.", str(c.In["instanceName"]))
		}
		return map[string]any{"state": derived("lightsail", r)["state"]}, nil
	})

	// ---- key pairs: real ed25519 keys
	Override("lightsail", "CreateKeyPair", func(c *Call) (map[string]any, error) {
		out, err := c.Default()
		if err != nil {
			return nil, err
		}
		pub, priv, _ := ed25519.GenerateKey(rand.Reader)
		der, _ := x509.MarshalPKCS8PrivateKey(priv)
		blob := lsSSHBlob(pub)
		r := c.Res
		r.Data["fingerprint"] = lsFingerprint(blob)
		r.Data["resourceType"], r.Data["location"] = "KeyPair", lsLocation(c)
		r.Data["supportCode"] = fmt.Sprintf("%d/%s", 100000000000+randInt(899999999999), r.Name)
		c.Put(r)
		kp := c.Fill(r)["keyPair"]
		out["keyPair"] = kp
		out["publicKeyBase64"] = "ssh-ed25519 " + base64.StdEncoding.EncodeToString(blob)
		out["privateKeyBase64"] = string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))
		out["operation"] = lsOp(c, r.Name, "KeyPair", "CreateKeyPair")
		return out, nil
	})
	Override("lightsail", "ImportKeyPair", func(c *Call) (map[string]any, error) {
		name := str(c.In["keyPairName"])
		if c.Get("KeyPair", name) != nil {
			return nil, c.Exists("KeyPair %s already exists", name)
		}
		key := strings.Fields(str(c.In["publicKeyBase64"])) // "ssh-rsa AAAA... comment", or just the key
		blob := []byte{}
		if len(key) > 0 {
			blob, _ = base64.StdEncoding.DecodeString(key[min(1, len(key)-1)])
		}
		now := c.now()
		c.Put(&Resource{Type: "KeyPair", Key: name, IDs: map[string]string{"keyPairName": name}, Name: name, ARN: c.arnOf("KeyPair/"+core.UUID(), nil), Account: c.Account, Region: c.Region, Created: now, Updated: now,
			Data: map[string]any{"fingerprint": lsFingerprint(blob), "resourceType": "KeyPair", "location": lsLocation(c), "supportCode": fmt.Sprintf("%d/%s", 100000000000+randInt(899999999999), name)}})
		return map[string]any{"operation": lsOp(c, name, "KeyPair", "ImportKeyPair")}, nil
	})
	Override("lightsail", "DeleteKeyPair", func(c *Call) (map[string]any, error) {
		if _, err := c.Default(); err != nil {
			return nil, err
		}
		return map[string]any{"operation": lsOp(c, str(c.In["keyPairName"]), "KeyPair", "DeleteKeyPair")}, nil
	})

	// ---- static IPs
	Override("lightsail", "AllocateStaticIp", func(c *Call) (map[string]any, error) {
		name := str(c.In["staticIpName"])
		if c.Get("StaticIp", name) != nil {
			return nil, c.Exists("StaticIp %s already exists", name)
		}
		now := c.now()
		c.Put(&Resource{Type: "StaticIp", Key: name, IDs: map[string]string{"staticIpName": name}, Name: name, ARN: c.arnOf("StaticIp/"+core.UUID(), nil), Account: c.Account, Region: c.Region, Created: now, Updated: now,
			Data: map[string]any{"ipAddress": lsIP(), "isAttached": false, "resourceType": "StaticIp", "location": lsLocation(c), "supportCode": fmt.Sprintf("%d/%s", 100000000000+randInt(899999999999), name)}})
		return map[string]any{"operations": []any{lsOp(c, name, "StaticIp", "AllocateStaticIp")}}, nil
	})
	Override("lightsail", "ReleaseStaticIp", func(c *Call) (map[string]any, error) {
		name := str(c.In["staticIpName"])
		if !c.Svc.Delete(c.Account, c.Region, "StaticIp", name) {
			return nil, c.NotFound("The %s static IP does not exist.", name)
		}
		return map[string]any{"operations": []any{lsOp(c, name, "StaticIp", "ReleaseStaticIp")}}, nil
	})
	Override("lightsail", "AttachStaticIp", func(c *Call) (map[string]any, error) {
		ip, inst := c.Get("StaticIp", str(c.In["staticIpName"])), c.Get("Instance", str(c.In["instanceName"]))
		if ip == nil || inst == nil {
			return nil, c.NotFound("The static IP or the instance does not exist.")
		}
		ip.Data["attachedTo"], ip.Data["isAttached"] = inst.Name, true
		inst.Data["publicIpAddress"], inst.Data["isStaticIp"] = ip.Data["ipAddress"], true
		c.Put(ip)
		c.Put(inst)
		return map[string]any{"operations": []any{lsOp(c, ip.Name, "StaticIp", "AttachStaticIp")}}, nil
	})
	Override("lightsail", "DetachStaticIp", func(c *Call) (map[string]any, error) {
		ip := c.Get("StaticIp", str(c.In["staticIpName"]))
		if ip == nil {
			return nil, c.NotFound("The %s static IP does not exist.", str(c.In["staticIpName"]))
		}
		if inst := c.Get("Instance", str(ip.Data["attachedTo"])); inst != nil {
			inst.Data["publicIpAddress"], inst.Data["isStaticIp"] = lsIP(), false
			c.Put(inst)
		}
		ip.Data["attachedTo"], ip.Data["isAttached"] = nil, false
		c.Put(ip)
		return map[string]any{"operations": []any{lsOp(c, ip.Name, "StaticIp", "DetachStaticIp")}}, nil
	})
	Override("lightsail", "GetStaticIp", func(c *Call) (map[string]any, error) {
		r := c.Get("StaticIp", str(c.In["staticIpName"]))
		if r == nil {
			return nil, c.NotFound("The %s static IP does not exist.", str(c.In["staticIpName"]))
		}
		return map[string]any{"staticIp": lsItem(c, "staticIp", r)}, nil
	})
	Override("lightsail", "GetStaticIps", func(c *Call) (map[string]any, error) {
		var out []any
		for _, r := range c.List("StaticIp") {
			out = append(out, lsItem(c, "staticIps", r))
		}
		return map[string]any{"staticIps": orEmpty(out)}, nil
	})

	// ---- operations
	Override("lightsail", "GetOperation", func(c *Call) (map[string]any, error) {
		r := c.Get("Operation", str(c.In["operationId"]))
		if r == nil {
			return nil, c.NotFound("The operation %s does not exist.", str(c.In["operationId"]))
		}
		return map[string]any{"operation": r.Data}, nil
	})
	Override("lightsail", "GetOperations", func(c *Call) (map[string]any, error) {
		var out []any
		for _, r := range c.List("Operation") {
			out = append(out, r.Data)
		}
		return map[string]any{"operations": orEmpty(out)}, nil
	})
	Override("lightsail", "GetOperationsForResource", func(c *Call) (map[string]any, error) {
		var out []any
		for _, r := range c.List("Operation") {
			if r.Data["resourceName"] == c.In["resourceName"] {
				out = append(out, r.Data)
			}
		}
		return map[string]any{"operations": orEmpty(out)}, nil
	})
}

func lsState(name string) map[string]any {
	code := map[string]float64{"pending": 0, "running": 16, "stopping": 64, "stopped": 80}[name]
	return map[string]any{"code": code, "name": name}
}

func lsLocation(c *Call) map[string]any {
	return map[string]any{"availabilityZone": c.Region + "a", "regionName": c.Region}
}

// lsItem fills the structure of one member of an output list from a resource.
func lsItem(c *Call, member string, r *Resource) any {
	sh := c.op.out.Members[member].sh
	if sh.Type == "list" {
		sh = sh.Member.sh
	}
	return c.fillItem(sh, r, member)
}

// lsOp records an operation that has Succeeded and returns it.
func lsOp(c *Call, resource, typ, kind string) map[string]any {
	id := core.UUID()
	now := c.now().Format(timeFmt)
	op := map[string]any{"id": id, "resourceName": resource, "resourceType": typ, "createdAt": now, "location": lsLocation(c), "isTerminal": true,
		"operationType": kind, "status": "Succeeded", "statusChangedAt": now}
	t := c.now()
	c.Put(&Resource{Type: "Operation", Key: id, IDs: map[string]string{"operationId": id}, ID: id, Account: c.Account, Region: c.Region, Data: op, Created: t, Updated: t})
	return op
}

func lsBundle(id string) (cpu, ram, disk, transfer float64) {
	switch strings.SplitN(id, "_", 2)[0] {
	case "micro":
		return 2, 1, 40, 2048
	case "small":
		return 2, 2, 60, 3072
	case "medium":
		return 2, 4, 80, 4096
	case "large":
		return 2, 8, 160, 5120
	case "xlarge":
		return 4, 16, 320, 6144
	case "2xlarge":
		return 8, 32, 640, 7168
	}
	return 2, 0.5, 20, 1024
}

func lsPorts() []any {
	var out []any
	for _, p := range []float64{22, 80} {
		out = append(out, map[string]any{"fromPort": p, "toPort": p, "protocol": "tcp", "accessFrom": "Anywhere (0.0.0.0/0)", "accessType": "public",
			"commonName": "", "accessDirection": "inbound", "cidrs": []any{"0.0.0.0/0"}, "ipv6Cidrs": []any{"::/0"}, "cidrListAliases": []any{}})
	}
	return out
}

func lsIP() string { return fmt.Sprintf("52.%d.%d.%d", randInt(250), randInt(250), 1+randInt(250)) }

// lsSSHBlob is an ed25519 public key in SSH wire format.
func lsSSHBlob(pub ed25519.PublicKey) []byte {
	var b []byte
	for _, s := range [][]byte{[]byte("ssh-ed25519"), pub} {
		b = binary.BigEndian.AppendUint32(b, uint32(len(s)))
		b = append(b, s...)
	}
	return b
}

func lsFingerprint(blob []byte) string {
	sum := md5.Sum(blob)
	var parts []string
	for _, b := range sum {
		parts = append(parts, fmt.Sprintf("%02x", b))
	}
	return strings.Join(parts, ":")
}
