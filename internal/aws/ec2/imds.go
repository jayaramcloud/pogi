package ec2

// The instance metadata service, at /_aws/imds/<instance-id>/ on pogi's port.
// Inside an instance container it is also at 169.254.169.254, so every SDK
// finds it with no setting: each instance has a listener of its own on the
// host (pogi knows who asks), and a forwarder container in the instance's
// network namespace gives the address to the instance and passes the calls on.
// Where that cannot be done (an engine sharing the host's network), the
// container has AWS_EC2_METADATA_SERVICE_ENDPOINT, which the CLI, boto3 and
// the Java SDK follow; the Go and JavaScript SDKs drop its path.
// IMDSv2 tokens (PUT latest/api/token) are HMACs of the instance and expiry.

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/docker"
)

// creds are role credentials IMDS handed to an instance.
type creds struct {
	Code            string
	LastUpdated     string
	Type            string
	AccessKeyId     string
	SecretAccessKey string
	Token           string
	Expiration      string
	expires         time.Time
}

func (s *Service) sign(msg string) string {
	m := hmac.New(sha256.New, s.secret)
	m.Write([]byte(msg))
	return hex.EncodeToString(m.Sum(nil))
}

func (s *Service) imds(w http.ResponseWriter, r *http.Request) {
	id, rest, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/_aws/imds/"), "/")
	version, p, _ := strings.Cut(strings.TrimLeft(rest, "/"), "/") // latest, 2021-07-15…
	s.mu.Lock()
	defer s.mu.Unlock()
	reg, i := s.find(id)
	switch {
	case i == nil || i.State.Name != "running" && i.State.Name != "pending":
		http.NotFound(w, r)
		return
	case version == "":
		io.WriteString(w, "latest")
		return
	case p == "api/token":
		ttl, err := strconv.Atoi(r.Header.Get("X-aws-ec2-metadata-token-ttl-seconds"))
		if r.Method != http.MethodPut || err != nil || ttl < 1 || ttl > 21600 {
			http.Error(w, "", http.StatusBadRequest)
			return
		}
		exp := strconv.FormatInt(time.Now().Unix()+int64(ttl), 10)
		w.Header().Set("X-aws-ec2-metadata-token-ttl-seconds", strconv.Itoa(ttl))
		io.WriteString(w, exp+"."+s.sign(id+exp))
		return
	}
	tok := r.Header.Get("X-aws-ec2-metadata-token")
	exp, sig, _ := strings.Cut(tok, ".")
	n, _ := strconv.ParseInt(exp, 10, 64)
	if tok != "" && (time.Now().Unix() >= n || !hmac.Equal([]byte(sig), []byte(s.sign(id+exp)))) ||
		tok == "" && i.MetadataOptions.HttpTokens == "required" {
		http.Error(w, "", http.StatusUnauthorized)
		return
	}
	data := s.metadata(reg, i)
	if i.Role != "" && p == "meta-data/iam/security-credentials/"+i.Role {
		b, _ := json.MarshalIndent(s.credentials(reg, i, i.Role, i.InstanceId), "", "  ")
		w.Write(b)
		return
	}
	if p == identityPath { // the SSM agent registers with these; every instance has them, role or not
		b, _ := json.MarshalIndent(s.credentials(reg, i, "aws:ec2-instance", i.InstanceId+"/identity"), "", "  ")
		w.Write(b)
		return
	}
	if v, ok := data[p]; ok {
		io.WriteString(w, v)
		return
	}
	dir := strings.TrimSuffix(p, "/")
	if dir != "" {
		dir += "/"
	}
	var names []string
	for k := range data {
		if rest, ok := strings.CutPrefix(k, dir); ok {
			name, deeper, _ := strings.Cut(rest, "/")
			if deeper != "" {
				name += "/"
			}
			names = append(names, name)
		}
	}
	if len(names) == 0 {
		http.NotFound(w, r)
		return
	}
	slices.Sort(names)
	io.WriteString(w, strings.Join(slices.Compact(names), "\n"))
}

const identityPath = "meta-data/identity-credentials/ec2/security-credentials/ec2-instance"

// metadata is the instance's metadata tree, by path under latest/; lock held.
func (s *Service) metadata(r *region, i *Instance) map[string]string {
	h := i.InstanceId[2:]
	var groups []string
	for _, g := range i.SecurityGroups {
		groups = append(groups, g.GroupName)
	}
	doc, _ := json.MarshalIndent(map[string]any{"accountId": r.Account, "architecture": i.Architecture, "availabilityZone": i.Placement.AvailabilityZone,
		"billingProducts": nil, "devpayProductCodes": nil, "marketplaceProductCodes": nil, "imageId": i.ImageId, "instanceId": i.InstanceId,
		"instanceType": i.InstanceType, "kernelId": nil, "pendingTime": i.LaunchTime[:19] + "Z", "privateIp": i.PrivateIpAddress, "ramdiskId": nil,
		"region": r.Name, "version": "2017-09-30"}, "", "  ")
	m := map[string]string{
		identityPath:       "", // minted when read
		"meta-data/ami-id": i.ImageId, "meta-data/ami-launch-index": strconv.Itoa(i.AmiLaunchIndex), "meta-data/hostname": i.PrivateDnsName,
		"meta-data/local-hostname": i.PrivateDnsName, "meta-data/instance-id": i.InstanceId, "meta-data/instance-type": i.InstanceType,
		"meta-data/local-ipv4": i.PrivateIpAddress, "meta-data/mac": "02:" + h[:2] + ":" + h[2:4] + ":" + h[4:6] + ":" + h[6:8] + ":" + h[8:10],
		"meta-data/placement/availability-zone": i.Placement.AvailabilityZone, "meta-data/placement/region": r.Name,
		"meta-data/placement/availability-zone-id": zoneID(i.Placement.AvailabilityZone), "meta-data/reservation-id": i.ReservationId,
		"meta-data/security-groups": strings.Join(groups, "\n"), "dynamic/instance-identity/document": string(doc),
	}
	if i.PublicIpAddress != "" {
		m["meta-data/public-ipv4"], m["meta-data/public-hostname"] = i.PublicIpAddress, i.PublicDnsName
	}
	if b, _ := base64.StdEncoding.DecodeString(i.UserData); len(b) > 0 {
		m["user-data"] = string(b)
	}
	if p := i.IamInstanceProfile; p != nil {
		info, _ := json.MarshalIndent(map[string]string{"Code": "Success", "LastUpdated": time.Now().UTC().Format("2006-01-02T15:04:05Z"),
			"InstanceProfileArn": p.Arn, "InstanceProfileId": p.Id}, "", "  ")
		m["meta-data/iam/info"] = string(info)
		if i.Role != "" {
			m["meta-data/iam/security-credentials/"+i.Role] = "" // minted when read
		}
	}
	return m
}

// credentials are the instance's session of a role, kept under key and renewed
// an hour before it expires as EC2 does; lock held.
func (s *Service) credentials(r *region, i *Instance, role, key string) *creds {
	cr := s.creds[key]
	if cr == nil || time.Until(cr.expires) < time.Hour {
		now := time.Now().UTC()
		cr = &creds{Code: "Success", LastUpdated: now.Format("2006-01-02T15:04:05Z"), Type: "AWS-HMAC",
			AccessKeyId: "ASIA" + core.RandString(16, core.UpperNum), SecretAccessKey: core.RandString(40, core.AlphaNum),
			Token: core.RandString(180, core.AlphaNum), expires: now.Add(6 * time.Hour)}
		cr.Expiration = cr.expires.Format("2006-01-02T15:04:05Z")
		s.hub.AddIdentity(cr.AccessKeyId, core.Identity{Account: r.Account, UserID: "AROA" + core.RandString(17, core.UpperNum) + ":" + i.InstanceId,
			ARN: core.GlobalARN("sts", r.Name, r.Account, "assumed-role/"+role+"/"+i.InstanceId), Secret: cr.SecretAccessKey})
		s.creds[key] = cr
	}
	return cr
}

// imdsAddr is where instances find the metadata service, as on EC2.
const imdsAddr = "169.254.169.254"

func imdsName(id string) string { return docker.Name("ec2", id+"-imds") }

// imdsPort starts the instance's own metadata listener (once, until it ends)
// and returns its port; "" when the instance shares the host's network, where
// 169.254.169.254 cannot be added. Lock held.
func (s *Service) imdsPort(id string, ct *docker.Container) string {
	if ct.Network == "host" {
		return ""
	}
	if srv := s.imdsLn[id]; srv != nil {
		return srv.Addr
	}
	ln, err := net.Listen("tcp", net.JoinHostPort(s.hub.Docker().ListenAddr(), "0")) // the forwarder container calls it
	if err != nil {
		return ""
	}
	srv := &http.Server{Addr: strconv.Itoa(ln.Addr().(*net.TCPAddr).Port), Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.URL.Path = "/_aws/imds/" + id + r.URL.Path
		s.imds(w, r)
	})}
	core.Go("ec2.imds", func() { srv.Serve(ln) })
	s.imdsLn[id] = srv
	return srv.Addr
}

// forward gives a running instance container 169.254.169.254: a socat
// container in its network namespace adds the address to the loopback device
// and passes port 80 on to the instance's listener. Then it tells the boot
// script (which waits for this, so UserData can use IMDS) to go on. A failure
// is logged and the instance goes on with the environment variable only.
func (s *Service) forward(name, id, port string) {
	d, ctx := s.hub.Docker(), context.Background()
	if port != "" {
		fwd := imdsName(id)
		err := d.RemoveLeftover(ctx, fwd) // one of the instance's earlier run
		if err == nil {
			_, err = d.Run(ctx, &docker.Container{Name: fwd, Image: "alpine/socat", Service: "ec2", Resource: id + "-imds", Network: "container:" + name,
				CapAdd: []string{"NET_ADMIN"}, Entrypoint: []string{"/bin/sh", "-c", "ip addr add " + imdsAddr + "/32 dev lo && exec socat tcp-listen:80,bind=" + imdsAddr +
					",fork,reuseaddr tcp-connect:" + net.JoinHostPort(d.HostAddr(), port)}})
		}
		if err != nil {
			s.hub.Log.Warn("ec2 instance cannot reach the metadata service at "+imdsAddr, "instance", id, "err", err)
		}
	}
	d.Exec(ctx, name, "sh", "-c", "mkdir -p /var/lib/cloud/instance && touch /var/lib/cloud/instance/pogi-net")
}
