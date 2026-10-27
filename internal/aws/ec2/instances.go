package ec2

// Instances, launch templates, EBS volumes and instance types. With a
// container engine each instance is a container of its AMI's image (see
// images): it runs UserData once and reads metadata and role credentials from
// pogi's IMDS (imds.go). Without one, instances are metadata with the same
// states.

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"maps"
	"math/rand/v2"
	"net/netip"
	"net/url"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/aws/route53"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/docker"
)

func (s *Service) instanceOps() map[string]core.Handler {
	return map[string]core.Handler{
		"RunInstances":                         core.H(s.runInstances),
		"DescribeInstances":                    core.H(s.describeInstances),
		"DescribeInstanceStatus":               core.H(s.describeInstanceStatus),
		"StartInstances":                       core.H(s.startInstances),
		"StopInstances":                        core.H(s.stopInstances),
		"RebootInstances":                      core.H(s.rebootInstances),
		"TerminateInstances":                   core.H(s.terminateInstances),
		"ModifyInstanceAttribute":              core.H(s.modifyInstanceAttribute),
		"DescribeInstanceAttribute":            core.H(s.describeInstanceAttribute),
		"DescribeInstanceTypes":                core.H(s.describeInstanceTypes),
		"DescribeInstanceCreditSpecifications": core.H(s.describeCreditSpecifications),
		"GetInstanceUefiData":                  core.H(s.uefiData),
		"CreateLaunchTemplate":                 core.H(s.createLaunchTemplate),
		"CreateLaunchTemplateVersion":          core.H(s.createLaunchTemplateVersion),
		"DescribeLaunchTemplates":              core.H(s.describeLaunchTemplates),
		"DescribeLaunchTemplateVersions":       core.H(s.describeLaunchTemplateVersions),
		"ModifyLaunchTemplate":                 core.H(s.modifyLaunchTemplate),
		"DeleteLaunchTemplate":                 core.H(s.deleteLaunchTemplate),
		"CreateVolume":                         core.H(s.createVolume),
		"DescribeVolumes":                      core.H(s.describeVolumes),
		"DeleteVolume":                         core.H(s.deleteVolume),
		"AttachVolume":                         core.H(s.attachVolume),
		"DetachVolume":                         core.H(s.detachVolume),
	}
}

// InstanceState is an instance state with its code.
type InstanceState struct {
	Code int    `xml:"code"`
	Name string `xml:"name"`
}

func stateOf(name string) InstanceState {
	return InstanceState{map[string]int{"pending": 0, "running": 16, "shutting-down": 32, "terminated": 48, "stopping": 64, "stopped": 80}[name], name}
}

// Instance is an instance in its wire shape, then pogi's own fields.
type Instance struct {
	InstanceId            string            `xml:"instanceId"`
	ImageId               string            `xml:"imageId"`
	State                 InstanceState     `xml:"instanceState"`
	PrivateDnsName        string            `xml:"privateDnsName"`
	PublicDnsName         string            `xml:"dnsName"`
	StateTransitionReason string            `xml:"reason"`
	KeyName               string            `xml:"keyName,omitempty"`
	AmiLaunchIndex        int               `xml:"amiLaunchIndex"`
	InstanceType          string            `xml:"instanceType"`
	LaunchTime            string            `xml:"launchTime"`
	Placement             Placement         `xml:"placement"`
	Monitoring            Monitoring        `xml:"monitoring"`
	SubnetId              string            `xml:"subnetId"`
	VpcId                 string            `xml:"vpcId"`
	PrivateIpAddress      string            `xml:"privateIpAddress"`
	PublicIpAddress       string            `xml:"ipAddress,omitempty"`
	SourceDestCheck       bool              `xml:"sourceDestCheck"`
	SecurityGroups        []GroupIdentifier `xml:"groupSet>item"`
	StateReason           *StateReason      `xml:"stateReason"`
	Architecture          string            `xml:"architecture"`
	RootDeviceType        string            `xml:"rootDeviceType"`
	RootDeviceName        string            `xml:"rootDeviceName"`
	BlockDeviceMappings   []BlockDevice     `xml:"blockDeviceMapping>item"`
	VirtualizationType    string            `xml:"virtualizationType"`
	ClientToken           string            `xml:"clientToken,omitempty"`
	TagSet                []Tag             `xml:"tagSet>item" json:"-"`
	Hypervisor            string            `xml:"hypervisor"`
	IamInstanceProfile    *ProfileRef       `xml:"iamInstanceProfile"`
	EbsOptimized          bool              `xml:"ebsOptimized"`
	EnaSupport            bool              `xml:"enaSupport"`
	CpuOptions            CpuOptions        `xml:"cpuOptions"`
	MetadataOptions       MetadataOptions   `xml:"metadataOptions"`
	PlatformDetails       string            `xml:"platformDetails"`
	UsageOperation        string            `xml:"usageOperation"`
	NetworkInterfaces     []InstanceNic     `xml:"networkInterfaceSet>item" json:"-"` // made when described, see nics

	ReservationId         string    `xml:"-"`
	UserData              string    `xml:"-"` // base64, as given
	DisableApiTermination bool      `xml:"-"`
	DisableApiStop        bool      `xml:"-"`
	ShutdownBehavior      string    `xml:"-"`
	CpuCredits            string    `xml:"-"`
	Role                  string    `xml:"-"` // of the instance profile: IMDS hands out its credentials
	Container             string    `xml:"-"` // empty: metadata only
	Ports                 []int     `xml:"-"` // container ports published on 127.0.0.1
	Ended                 time.Time `xml:"-"`
}

// InstanceNic is the primary network interface of an instance.
type InstanceNic struct {
	NetworkInterfaceId string            `xml:"networkInterfaceId"`
	SubnetId           string            `xml:"subnetId"`
	VpcId              string            `xml:"vpcId"`
	Description        string            `xml:"description"`
	Status             string            `xml:"status"`
	MacAddress         string            `xml:"macAddress"`
	PrivateIpAddress   string            `xml:"privateIpAddress"`
	PrivateDnsName     string            `xml:"privateDnsName"`
	SourceDestCheck    bool              `xml:"sourceDestCheck"`
	Groups             []GroupIdentifier `xml:"groupSet>item"`
	Attachment         struct {
		AttachmentId        string `xml:"attachmentId"`
		DeviceIndex         int    `xml:"deviceIndex"`
		Status              string `xml:"status"`
		AttachTime          string `xml:"attachTime"`
		DeleteOnTermination bool   `xml:"deleteOnTermination"`
	} `xml:"attachment"`
	PrivateIpAddresses []struct {
		PrivateIpAddress string `xml:"privateIpAddress"`
		PrivateDnsName   string `xml:"privateDnsName"`
		Primary          bool   `xml:"primary"`
	} `xml:"privateIpAddressesSet>item"`
	InterfaceType string `xml:"interfaceType"`
}

// nics is the one network interface an instance has: its security groups and
// addresses, attached when it launched.
// DescribeNetworkInterfaces lists it too (see allEnis).
func (i *Instance) nics() []InstanceNic {
	h := sha256.Sum256([]byte(i.InstanceId))
	n := InstanceNic{NetworkInterfaceId: "eni-" + hex.EncodeToString(h[:9]), SubnetId: i.SubnetId, VpcId: i.VpcId, Status: "in-use",
		MacAddress:       fmt.Sprintf("02:%02x:%02x:%02x:%02x:%02x", h[9], h[10], h[11], h[12], h[13]),
		PrivateIpAddress: i.PrivateIpAddress, PrivateDnsName: i.PrivateDnsName, SourceDestCheck: i.SourceDestCheck, Groups: slices.Clone(i.SecurityGroups),
		InterfaceType: "interface"}
	n.Attachment.AttachmentId, n.Attachment.Status, n.Attachment.AttachTime, n.Attachment.DeleteOnTermination = "eni-attach-"+hex.EncodeToString(h[14:22]), "attached", i.LaunchTime, true
	n.PrivateIpAddresses = []struct {
		PrivateIpAddress string `xml:"privateIpAddress"`
		PrivateDnsName   string `xml:"privateDnsName"`
		Primary          bool   `xml:"primary"`
	}{{i.PrivateIpAddress, i.PrivateDnsName, true}}
	return []InstanceNic{n}
}

// Placement is where an instance runs.
type Placement struct {
	AvailabilityZone string `xml:"availabilityZone"`
	GroupName        string `xml:"groupName"`
	Tenancy          string `xml:"tenancy"`
}

// Monitoring says whether detailed monitoring is on.
type Monitoring struct {
	State string `xml:"state"`
}

// GroupIdentifier is a security group of an instance.
type GroupIdentifier struct {
	GroupId   string `xml:"groupId"`
	GroupName string `xml:"groupName"`
}

// StateReason says why an instance stopped or terminated.
type StateReason struct {
	Code    string `xml:"code"`
	Message string `xml:"message"`
}

// BlockDevice is an EBS volume of an instance.
type BlockDevice struct {
	DeviceName string    `xml:"deviceName"`
	Ebs        EbsDevice `xml:"ebs"`
}

// EbsDevice is a volume's attachment as an instance shows it.
type EbsDevice struct {
	VolumeId            string `xml:"volumeId"`
	Status              string `xml:"status"`
	AttachTime          string `xml:"attachTime"`
	DeleteOnTermination bool   `xml:"deleteOnTermination"`
}

// ProfileRef is an instance's IAM instance profile.
type ProfileRef struct {
	Arn string `xml:"arn"`
	Id  string `xml:"id"`
}

// CpuOptions are an instance's cores and threads.
type CpuOptions struct {
	CoreCount      int `xml:"coreCount"`
	ThreadsPerCore int `xml:"threadsPerCore"`
}

// MetadataOptions configure an instance's metadata service.
type MetadataOptions struct {
	State                   string `xml:"state"`
	HttpTokens              string `xml:"httpTokens"`
	HttpPutResponseHopLimit int    `xml:"httpPutResponseHopLimit"`
	HttpEndpoint            string `xml:"httpEndpoint"`
	HttpProtocolIpv6        string `xml:"httpProtocolIpv6"`
	InstanceMetadataTags    string `xml:"instanceMetadataTags"`
}

func (i *Instance) view() (string, *[]Tag, map[string][]string) {
	i.SecurityGroups, i.BlockDeviceMappings = slices.Clone(i.SecurityGroups), slices.Clone(i.BlockDeviceMappings) // encoded after unlocking
	i.NetworkInterfaces = i.nics()
	attrs := map[string][]string{"instance-id": {i.InstanceId}, "instance-state-name": {i.State.Name}, "instance-state-code": {strconv.Itoa(i.State.Code)},
		"instance-type": {i.InstanceType}, "image-id": {i.ImageId}, "vpc-id": {i.VpcId}, "subnet-id": {i.SubnetId},
		"availability-zone": {i.Placement.AvailabilityZone}, "private-ip-address": {i.PrivateIpAddress}, "ip-address": {i.PublicIpAddress},
		"private-dns-name": {i.PrivateDnsName}, "key-name": {i.KeyName}, "reservation-id": {i.ReservationId}, "architecture": {i.Architecture}}
	for _, g := range i.SecurityGroups {
		attrs["group-id"] = append(attrs["group-id"], g.GroupId)
		attrs["group-name"] = append(attrs["group-name"], g.GroupName)
	}
	attrs["instance.group-id"], attrs["instance.group-name"] = attrs["group-id"], attrs["group-name"]
	if i.IamInstanceProfile != nil {
		attrs["iam-instance-profile.arn"] = []string{i.IamInstanceProfile.Arn}
	}
	return i.InstanceId, &i.TagSet, attrs
}

// reservation is the instances one RunInstances launched.
type reservation struct {
	ReservationId string            `xml:"reservationId"`
	OwnerId       string            `xml:"ownerId"`
	Groups        []GroupIdentifier `xml:"groupSet>item"`
	Instances     []Instance        `xml:"instancesSet>item"`
}

// ---- RunInstances ----

// netIface is a network interface to launch with; only the first counts.
type netIface struct {
	DeviceIndex              int
	SubnetId                 string
	PrivateIpAddress         string
	Groups                   []string `query:"SecurityGroupId"`
	AssociatePublicIpAddress *bool
}

type blockDevice struct {
	DeviceName string
	NoDevice   string
	Ebs        *struct {
		VolumeSize, Iops, Throughput     int
		VolumeType, SnapshotId, KmsKeyId string
		DeleteOnTermination, Encrypted   *bool
	}
}

// runInput is RunInstances' input, and the launch template data it inherits.
type runInput struct {
	ImageId, InstanceType, KeyName, SubnetId, PrivateIpAddress, UserData, ClientToken string
	InstanceInitiatedShutdownBehavior                                                 string
	MinCount, MaxCount                                                                int
	SecurityGroupIds                                                                  []string `query:"SecurityGroupId"`
	SecurityGroups                                                                    []string `query:"SecurityGroup"`
	IamInstanceProfile                                                                *struct{ Arn, Name string }
	LaunchTemplate                                                                    *struct{ LaunchTemplateId, LaunchTemplateName, Version string }
	Placement                                                                         *Placement
	Monitoring                                                                        *struct{ Enabled bool }
	MetadataOptions                                                                   *MetadataOptions
	CreditSpecification                                                               *struct{ CpuCredits string }
	DisableApiTermination, DisableApiStop, EbsOptimized                               *bool
	NetworkInterfaces                                                                 []netIface    `query:"NetworkInterface"`
	BlockDeviceMappings                                                               []blockDevice `query:"BlockDeviceMapping"`
	TagSpecifications                                                                 []tagSpec     `query:"TagSpecification"`
}

// runToken remembers a RunInstances ClientToken: a hash of the other parameters, and the reservation it made.
type runToken struct {
	Hash          string `json:"hash"`
	ReservationId string `json:"reservationId"`
}

func (s *Service) runInstances(c *core.Ctx, in *runInput) (any, error) {
	if err := s.fromTemplate(c, in); err != nil {
		return nil, err
	}
	hash := ""
	if in.ClientToken != "" { // the same token and the same parameters mean the same call
		tok := in.ClientToken
		in.ClientToken = ""
		b, _ := json.Marshal(in)
		in.ClientToken = tok
		sum := sha256.Sum256(b)
		hash = hex.EncodeToString(sum[:])
	}
	switch {
	case in.MinCount < 1 || in.MaxCount < in.MinCount:
		return nil, invalid("Value (%d) for parameter minCount is invalid. Expected a positive integer no greater than maxCount.", in.MinCount)
	case in.ImageId == "":
		return nil, core.Errorf(400, "MissingParameter", "The request must contain the parameter ImageId")
	case !strings.HasPrefix(in.ImageId, "ami-") && !strings.HasPrefix(in.ImageId, "resolve:ssm:"):
		return nil, core.Errorf(400, "InvalidAMIID.Malformed", "Invalid id: \"%s\" (expecting \"ami-...\")", in.ImageId)
	}
	in.InstanceType = cmp.Or(in.InstanceType, "m1.small")
	vcpus, ok := cpus(in.InstanceType)
	if !ok {
		return nil, invalid("Invalid value '%s' for InstanceType.", in.InstanceType)
	}
	if _, err := base64.StdEncoding.DecodeString(in.UserData); err != nil {
		return nil, invalid("Invalid BASE64 encoding of user data.")
	}
	var ni netIface
	if len(in.NetworkInterfaces) > 0 {
		ni = in.NetworkInterfaces[0]
	}
	prof, role, err := s.profile(c, in.IamInstanceProfile)
	if err != nil {
		return nil, err
	}
	img := imageFor(in.ImageId)
	if strings.HasPrefix(in.ImageId, "resolve:ssm:") {
		in.ImageId = img.ImageId
	}
	engine := s.hub.Docker().Available()

	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	if in.ClientToken != "" { // checked under the lock, so concurrent calls with one token make one reservation
		if t, ok := r.Tokens[in.ClientToken]; ok {
			var made []Instance
			for _, i := range r.Instances {
				if i.ReservationId == t.ReservationId {
					made = append(made, withTags(r, i, (*Instance).view))
				}
			}
			if len(made) > 0 {
				if t.Hash != hash {
					return nil, core.Errorf(400, "IdempotentParameterMismatch", "Arguments on this idempotent request are inconsistent with arguments used in previous request(s).")
				}
				slices.SortFunc(made, func(a, b Instance) int { return a.AmiLaunchIndex - b.AmiLaunchIndex })
				return &struct {
					Header
					reservation
				}{reservation: reservation{ReservationId: t.ReservationId, OwnerId: c.Account, Instances: made}}, nil
			}
			delete(r.Tokens, in.ClientToken) // its instances are gone: the token is free again
		}
	}
	if made := r.x().Images[in.ImageId]; made != nil { // an image CreateImage made
		img = made
	} else if images[in.ImageId] == nil {
		s.hub.Log.Info("ec2: unknown AMI, its instances run "+img.Docker, "ami", in.ImageId)
	}
	az := ""
	if in.Placement != nil {
		az = in.Placement.AvailabilityZone
	}
	sn, err := r.subnetFor(cmp.Or(in.SubnetId, ni.SubnetId), az)
	if err != nil {
		return nil, err
	}
	ids := in.SecurityGroupIds
	if len(ids) == 0 {
		ids = ni.Groups
	}
	groups, err := r.groupsFor(sn.VpcId, ids, in.SecurityGroups)
	if err != nil {
		return nil, err
	}
	if in.KeyName != "" && r.Keys[in.KeyName] == nil {
		return nil, core.Errorf(400, "InvalidKeyPair.NotFound", "The key pair '%s' does not exist", in.KeyName)
	}
	meta := MetadataOptions{"applied", "optional", 1, "enabled", "disabled", "disabled"}
	if m := in.MetadataOptions; m != nil {
		meta.HttpTokens, meta.HttpEndpoint = cmp.Or(m.HttpTokens, meta.HttpTokens), cmp.Or(m.HttpEndpoint, meta.HttpEndpoint)
		meta.HttpPutResponseHopLimit = cmp.Or(m.HttpPutResponseHopLimit, meta.HttpPutResponseHopLimit)
		meta.HttpProtocolIpv6, meta.InstanceMetadataTags = cmp.Or(m.HttpProtocolIpv6, meta.HttpProtocolIpv6), cmp.Or(m.InstanceMetadataTags, meta.InstanceMetadataTags)
	}
	public := sn.MapPublicIpOnLaunch
	if ni.AssociatePublicIpAddress != nil {
		public = *ni.AssociatePublicIpAddress
	}
	now := time.Now().UTC().Format(core.ISO)
	resv := reservation{ReservationId: newID("r"), OwnerId: c.Account, Instances: []Instance{}}
	for n := range in.MaxCount {
		ip := in.PrivateIpAddress
		if ip == "" {
			ip = r.freeIP(sn)
		}
		if ip == "" {
			return nil, core.Errorf(400, "InsufficientFreeAddressesInSubnet",
				"There are not enough free addresses in subnet '%s' to satisfy the requested number of instances.", sn.SubnetId)
		}
		i := &Instance{InstanceId: newID("i"), ImageId: in.ImageId, KeyName: in.KeyName, AmiLaunchIndex: n, InstanceType: in.InstanceType,
			LaunchTime: now, Placement: Placement{AvailabilityZone: sn.AvailabilityZone, Tenancy: "default"}, SubnetId: sn.SubnetId,
			VpcId: sn.VpcId, PrivateIpAddress: ip, PrivateDnsName: hostname(ip, r.Name), SourceDestCheck: true, SecurityGroups: groups,
			Architecture: img.Architecture, RootDeviceType: "ebs", RootDeviceName: img.RootDeviceName, VirtualizationType: "hvm",
			ClientToken: in.ClientToken, Hypervisor: "xen", IamInstanceProfile: prof, EnaSupport: true,
			CpuOptions: CpuOptions{max(vcpus/2, 1), min(vcpus, 2)}, MetadataOptions: meta, PlatformDetails: "Linux/UNIX",
			UsageOperation: "RunInstances", ReservationId: resv.ReservationId, UserData: in.UserData, Role: role,
			ShutdownBehavior: cmp.Or(in.InstanceInitiatedShutdownBehavior, "stop"), Ports: r.ports(groups)}
		i.Monitoring.State = "disabled"
		if in.Monitoring != nil && in.Monitoring.Enabled {
			i.Monitoring.State = "enabled"
		}
		if p := in.Placement; p != nil {
			i.Placement.GroupName, i.Placement.Tenancy = p.GroupName, cmp.Or(p.Tenancy, "default")
		}
		if in.CreditSpecification != nil {
			i.CpuCredits = in.CreditSpecification.CpuCredits
		}
		for _, b := range []struct {
			src *bool
			dst *bool
		}{{in.DisableApiTermination, &i.DisableApiTermination}, {in.DisableApiStop, &i.DisableApiStop}, {in.EbsOptimized, &i.EbsOptimized}} {
			if b.src != nil {
				*b.dst = *b.src
			}
		}
		if public {
			i.PublicIpAddress = fmt.Sprintf("54.%d.%d.%d", rand.IntN(256), rand.IntN(256), 1+rand.IntN(254))
			i.PublicDnsName = "ec2-" + strings.ReplaceAll(i.PublicIpAddress, ".", "-") + map[bool]string{true: ".compute-1.amazonaws.com",
				false: "." + r.Name + ".compute.amazonaws.com"}[r.Name == "us-east-1"]
		}
		r.Instances[i.InstanceId] = i
		r.tag(i.InstanceId, typed(in.TagSpecifications, "instance"))
		r.volumes(i, in.BlockDeviceMappings, typed(in.TagSpecifications, "volume"))
		if engine {
			i.Container = docker.Name("ec2", i.InstanceId)
		}
		s.boot(r, i, "run")
		v := withTags(r, i, (*Instance).view)
		v.State = stateOf("pending")
		resv.Instances = append(resv.Instances, v)
	}
	if in.ClientToken != "" {
		if r.Tokens == nil {
			r.Tokens = map[string]runToken{}
		}
		r.Tokens[in.ClientToken] = runToken{hash, resv.ReservationId}
	}
	return &struct {
		Header
		reservation
	}{reservation: resv}, nil
}

// fromTemplate fills what a RunInstances request leaves out from its launch
// template, as EC2 does; their tag specifications add up.
func (s *Service) fromTemplate(c *core.Ctx, in *runInput) error {
	spec := in.LaunchTemplate
	if spec == nil {
		return nil
	}
	s.mu.Lock()
	lt, err := s.reg(c).template(spec.LaunchTemplateId, spec.LaunchTemplateName)
	var v *TemplateVersion
	if err == nil {
		v, err = lt.version(cmp.Or(spec.Version, "$Default"))
	}
	s.mu.Unlock()
	if err != nil {
		return err
	}
	form := url.Values{}
	for k, x := range v.LaunchTemplateData {
		form.Set(k, x)
	}
	var tpl runInput
	if err := core.DecodeQuery(form, &tpl); err != nil {
		return invalid("The launch template %s cannot be launched: %v", lt.LaunchTemplateId, err)
	}
	in.TagSpecifications = append(append(tpl.TagSpecifications, in.TagSpecifications...), tagSpec{"instance",
		[]Tag{{"aws:ec2launchtemplate:id", lt.LaunchTemplateId}, {"aws:ec2launchtemplate:version", strconv.Itoa(v.VersionNumber)}}})
	dst, src := reflect.ValueOf(in).Elem(), reflect.ValueOf(&tpl).Elem()
	for i := range dst.NumField() {
		if dst.Field(i).IsZero() {
			dst.Field(i).Set(src.Field(i))
		}
	}
	return nil
}

// profile resolves an instance profile (arn or name) through IAM, with the
// role IMDS hands out credentials of. Without IAM it is taken as given.
func (s *Service) profile(c *core.Ctx, p *struct{ Arn, Name string }) (*ProfileRef, string, error) {
	if p == nil || p.Arn+p.Name == "" {
		return nil, "", nil
	}
	name := cmp.Or(p.Name, p.Arn[strings.LastIndex(p.Arn, "/")+1:])
	var out struct {
		InstanceProfile struct {
			Arn, InstanceProfileId string
			RoleNames              []string
		}
	}
	err := s.hub.Call(c.Account, c.Region, "AWSIdentityManagementV20100508", "GetInstanceProfile", map[string]string{"InstanceProfileName": name}, &out)
	if err != nil && core.AsError(err).Code == "ServiceNotEnabled" {
		return &ProfileRef{cmp.Or(p.Arn, c.GlobalARN("iam", "instance-profile/"+name)), "AIPA" + core.RandString(17, core.UpperNum)}, "", nil
	}
	if err != nil {
		return nil, "", invalid("Value (%s) for parameter iamInstanceProfile.name is invalid. Invalid IAM Instance Profile name", name)
	}
	ip := out.InstanceProfile
	return &ProfileRef{ip.Arn, ip.InstanceProfileId}, strings.Join(ip.RoleNames[:min(len(ip.RoleNames), 1)], ""), nil
}

// imageFor is the catalog AMI an image id runs: itself, the one a
// resolve:ssm: parameter names, else Amazon Linux 2023.
func imageFor(id string) *Image {
	if img := images[id]; img != nil {
		return img
	}
	if strings.HasPrefix(id, "resolve:ssm:") {
		for _, k := range core.SortedKeys(images) {
			if strings.Contains(id, images[k].Keyword) {
				return images[k]
			}
		}
	}
	return images[defaultAMI]
}

// subnetFor is the subnet named, else the default subnet of the zone asked
// for (or of the first zone); lock held.
func (r *region) subnetFor(id, az string) (*Subnet, error) {
	if id != "" {
		if sn := r.Subnets[id]; sn != nil {
			return sn, nil
		}
		return nil, core.Errorf(400, "InvalidSubnetID.NotFound", "The subnet ID '%s' does not exist", id)
	}
	def := r.defaultVpc()
	if def == nil {
		return nil, core.Errorf(400, "VPCIdNotSpecified", "No default VPC for this user")
	}
	for _, z := range zones(r.Name) {
		for _, k := range core.SortedKeys(r.Subnets) {
			if sn := r.Subnets[k]; sn.VpcId == def.VpcId && sn.DefaultForAz && sn.AvailabilityZone == z && (az == "" || az == z) {
				return sn, nil
			}
		}
	}
	return nil, invalid("Invalid availability zone: [%s]", az)
}

// groupsFor resolves security groups by id, or by name in the default VPC;
// none is the VPC's default group; lock held.
func (r *region) groupsFor(vpc string, ids, names []string) ([]GroupIdentifier, error) {
	ids = slices.Clone(ids)
	for _, n := range names {
		g, err := r.group("", n)
		if err != nil {
			return nil, err
		}
		ids = append(ids, g.GroupId)
	}
	for _, k := range core.SortedKeys(r.Groups) {
		if g := r.Groups[k]; len(ids) == 0 && g.VpcId == vpc && g.GroupName == "default" {
			ids = append(ids, k)
		}
	}
	var out []GroupIdentifier
	for _, id := range ids {
		g := r.Groups[id]
		if g == nil {
			return nil, core.Errorf(400, "InvalidGroup.NotFound", "The security group '%s' does not exist", id)
		}
		out = append(out, GroupIdentifier{g.GroupId, g.GroupName})
	}
	return out, nil
}

// freeIP is the first address of a subnet (after the four AWS keeps) that
// no live instance holds, so addresses are stable; lock held.
func (r *region) freeIP(sn *Subnet) string {
	used := map[string]bool{}
	for _, n := range r.Net.Enis {
		if n.SubnetId == sn.SubnetId {
			used[n.PrivateIpAddress] = true
		}
	}
	for _, i := range r.Instances {
		if i.SubnetId == sn.SubnetId && i.State.Name != "terminated" {
			used[i.PrivateIpAddress] = true
		}
	}
	p, err := netip.ParsePrefix(sn.CidrBlock)
	if err != nil {
		return ""
	}
	for a := p.Masked().Addr().Next().Next().Next().Next(); p.Contains(a.Next()); a = a.Next() {
		if !used[a.String()] {
			return a.String()
		}
	}
	return ""
}

// ports are the tcp ports the groups let in, from ranges of at most 20
// ports: a container publishes them; lock held.
func (r *region) ports(groups []GroupIdentifier) []int {
	set := map[int]bool{}
	for _, gi := range groups {
		if g := r.Groups[gi.GroupId]; g != nil {
			for _, p := range g.IpPermissions {
				if p.IpProtocol == "tcp" && p.FromPort != nil && p.ToPort != nil && *p.ToPort-*p.FromPort < 20 {
					for x := *p.FromPort; x <= *p.ToPort; x++ {
						set[x] = true
					}
				}
			}
		}
	}
	return slices.Sorted(maps.Keys(set))
}

func hostname(ip, region string) string {
	if region == "us-east-1" {
		return "ip-" + strings.ReplaceAll(ip, ".", "-") + ".ec2.internal"
	}
	return "ip-" + strings.ReplaceAll(ip, ".", "-") + "." + region + ".compute.internal"
}

// typed keeps the tag specifications for one resource type.
func typed(specs []tagSpec, rt string) []tagSpec {
	return slices.DeleteFunc(slices.Clone(specs), func(t tagSpec) bool { return t.ResourceType != "" && t.ResourceType != rt })
}

// ---- describe ----

// purge forgets instances terminated over an hour ago, as EC2 does; lock held.
func (r *region) purge() {
	for id, i := range r.Instances {
		if i.State.Name == "terminated" && time.Since(i.Ended) > time.Hour {
			delete(r.Instances, id)
			delete(r.Tags, id)
		}
	}
}

func (s *Service) describeInstances(c *core.Ctx, in *struct {
	InstanceIds []string `query:"InstanceId"`
	Filters     []filter `query:"Filter"`
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	r.purge()
	list, err := describe(r, r.Instances, in.InstanceIds, in.Filters, "InvalidInstanceID.NotFound", "instance ID", (*Instance).view)
	if err != nil {
		return nil, err
	}
	out := &struct {
		Header
		Reservations []reservation `xml:"reservationSet>item"`
	}{Reservations: []reservation{}}
	for _, i := range list {
		k := slices.IndexFunc(out.Reservations, func(x reservation) bool { return x.ReservationId == i.ReservationId })
		if k < 0 {
			out.Reservations, k = append(out.Reservations, reservation{ReservationId: i.ReservationId, OwnerId: r.Account}), len(out.Reservations)
		}
		out.Reservations[k].Instances = append(out.Reservations[k].Instances, i)
	}
	return out, nil
}

type check struct {
	Status  string `xml:"status"`
	Details []struct {
		Name   string `xml:"name"`
		Status string `xml:"status"`
	} `xml:"details>item"`
}

func (s *Service) describeInstanceStatus(c *core.Ctx, in *struct {
	InstanceIds         []string `query:"InstanceId"`
	Filters             []filter `query:"Filter"`
	IncludeAllInstances bool
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	list, err := describe(r, r.Instances, in.InstanceIds, in.Filters, "InvalidInstanceID.NotFound", "instance ID", (*Instance).view)
	type status struct {
		InstanceId       string        `xml:"instanceId"`
		AvailabilityZone string        `xml:"availabilityZone"`
		InstanceState    InstanceState `xml:"instanceState"`
		SystemStatus     check         `xml:"systemStatus"`
		InstanceStatus   check         `xml:"instanceStatus"`
	}
	out := &struct {
		Header
		InstanceStatuses []status `xml:"instanceStatusSet>item"`
	}{}
	for _, i := range list {
		if i.State.Name != "running" && !in.IncludeAllInstances {
			continue
		}
		st := check{Status: "not-applicable"}
		if i.State.Name == "running" {
			st.Status = "ok"
			st.Details = append(st.Details, struct {
				Name   string `xml:"name"`
				Status string `xml:"status"`
			}{"reachability", "passed"})
		}
		out.InstanceStatuses = append(out.InstanceStatuses, status{i.InstanceId, i.Placement.AvailabilityZone, i.State, st, st})
	}
	return out, err
}

// ---- lifecycle ----

type idsInput struct {
	InstanceIds []string `query:"InstanceId"`
}

type stateChange struct {
	InstanceId    string        `xml:"instanceId"`
	CurrentState  InstanceState `xml:"currentState"`
	PreviousState InstanceState `xml:"previousState"`
}

// change applies Start, Stop or Terminate, all or nothing: allow says, per
// instance, whether the call acts on it (false: it is there already) or
// refuses; the response shows the state the call moves through.
func (s *Service) change(c *core.Ctx, ids []string, shown string, allow func(*Instance) (bool, error), act func(*region, *Instance)) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	var list []*Instance
	for _, id := range ids {
		i := r.Instances[id]
		if i == nil {
			return nil, core.Errorf(400, "InvalidInstanceID.NotFound", "The instance ID '%s' does not exist", id)
		}
		list = append(list, i)
	}
	out := &struct {
		Header
		Instances []stateChange `xml:"instancesSet>item"`
	}{}
	acts := make([]bool, len(list))
	for k, i := range list {
		var err error
		if acts[k], err = allow(i); err != nil {
			return nil, err
		}
	}
	for k, i := range list {
		ch := stateChange{i.InstanceId, i.State, i.State}
		if acts[k] {
			act(r, i)
			ch.CurrentState = stateOf(shown)
		}
		out.Instances = append(out.Instances, ch)
	}
	return out, nil
}

func incorrect(i *Instance, verb string) error {
	return core.Errorf(400, "IncorrectInstanceState", "The instance '%s' is not in a state from which it can be %s.", i.InstanceId, verb)
}

func (s *Service) startInstances(c *core.Ctx, in *idsInput) (any, error) {
	return s.change(c, in.InstanceIds, "pending", func(i *Instance) (bool, error) {
		switch i.State.Name {
		case "stopped":
			return true, nil
		case "pending", "running":
			return false, nil
		}
		return false, incorrect(i, "started")
	}, func(r *region, i *Instance) { s.boot(r, i, "start") })
}

func (s *Service) stopInstances(c *core.Ctx, in *idsInput) (any, error) {
	return s.change(c, in.InstanceIds, "stopping", func(i *Instance) (bool, error) {
		switch i.State.Name {
		case "stopped", "stopping":
			return false, nil
		case "pending", "running":
			if i.DisableApiStop {
				return false, core.Errorf(400, "OperationNotPermitted",
					"The instance '%s' may not be stopped. Modify its 'disableApiStop' instance attribute and try again.", i.InstanceId)
			}
			return true, nil
		}
		return false, incorrect(i, "stopped")
	}, s.halt)
}

func (s *Service) terminateInstances(c *core.Ctx, in *idsInput) (any, error) {
	return s.change(c, in.InstanceIds, "shutting-down", func(i *Instance) (bool, error) {
		switch {
		case i.State.Name == "terminated" || i.State.Name == "shutting-down":
			return false, nil
		case i.DisableApiTermination:
			return false, core.Errorf(400, "OperationNotPermitted",
				"The instance '%s' may not be terminated. Modify its 'disableApiTermination' instance attribute and try again.", i.InstanceId)
		}
		return true, nil
	}, func(r *region, i *Instance) {
		s.end(r, i, &StateReason{"Client.UserInitiatedShutdown", "Client.UserInitiatedShutdown: User initiated shutdown"})
	})
}

func (s *Service) rebootInstances(c *core.Ctx, in *idsInput) (any, error) {
	_, err := s.change(c, in.InstanceIds, "running", func(i *Instance) (bool, error) {
		if i.State.Name != "running" {
			return false, incorrect(i, "rebooted")
		}
		return true, nil
	}, func(r *region, i *Instance) { s.boot(r, i, "restart") })
	if err != nil {
		return nil, err
	}
	return ok(), nil
}

// boot runs an instance. Without a container it is running at once; with
// one it is pending until the engine has run ("run": a fresh container),
// started ("start") or restarted ("restart") it. Lock held.
func (s *Service) boot(r *region, i *Instance, verb string) {
	i.StateReason, i.StateTransitionReason = nil, ""
	if i.Container == "" {
		i.State = stateOf("running")
		return
	}
	if verb != "restart" {
		i.State = stateOf("pending")
	}
	ct, id := s.container(r, i), i.InstanceId
	port, account, region, vpcID := s.imdsPort(id, ct), r.Account, r.Name, i.VpcId
	s.serial(id, func() {
		d, ctx := s.hub.Docker(), context.Background()
		if ct.Network != "host" { // the DNS server of the VPC's private zones; not under the lock, it may start a container
			ct.Dns = route53.ContainerDNS(s.hub, account, region, vpcID)
		}
		var err error
		switch {
		case verb == "start" && d.Start(ctx, ct.Name) == nil, verb == "restart" && d.Restart(ctx, ct.Name) == nil:
		default:
			if err = d.RemoveLeftover(ctx, ct.Name); err == nil {
				_, err = d.Run(ctx, ct)
			}
		}
		var info *docker.Info
		if err == nil {
			info, err = d.Inspect(ctx, ct.Name)
		}
		if err == nil {
			s.forward(ct.Name, id, port)
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		r, i := s.find(id)
		if i == nil || i.State.Name != "pending" && i.State.Name != "running" {
			return // stopped or terminated meanwhile: its call is queued after this one
		}
		if err != nil {
			s.hub.Log.Warn("ec2 instance container failed", "instance", id, "err", err)
			s.end(r, i, &StateReason{"Server.InternalError", "Server.InternalError: " + err.Error()})
			return
		}
		i.State = stateOf("running")
		for _, p := range i.Ports {
			hp := p
			if ct.Network != "host" {
				hp = info.HostPort(p)
			}
			if hp != 0 {
				addr := s.hub.Docker().Addr(hp)
				s.hub.SetEndpoint("instance:"+id+":"+strconv.Itoa(p), addr)
				s.hub.SetEndpoint("ip:"+i.PrivateIpAddress+":"+strconv.Itoa(p), addr)
			}
		}
	})
}

// halt stops an instance (its container in the background); lock held.
func (s *Service) halt(r *region, i *Instance) {
	s.unpublish(i)
	i.StateTransitionReason = "User initiated (" + time.Now().UTC().Format("2006-01-02 15:04:05 MST") + ")"
	i.StateReason = &StateReason{"Client.UserInitiatedShutdown", "Client.UserInitiatedShutdown: User initiated shutdown"}
	if i.Container == "" {
		i.State = stateOf("stopped")
		return
	}
	i.State = stateOf("stopping")
	id, name := i.InstanceId, i.Container
	s.serial(id, func() {
		s.hub.Docker().Remove(context.Background(), imdsName(id)) // it shares the instance's network, which a restart remakes
		s.hub.Docker().Stop(context.Background(), name)
		s.mu.Lock()
		if _, i := s.find(id); i != nil && i.State.Name == "stopping" {
			i.State = stateOf("stopped")
		}
		s.mu.Unlock()
	})
}

// end terminates an instance: volumes marked so go with it, its container is
// removed in the background; lock held.
func (s *Service) end(r *region, i *Instance, why *StateReason) {
	s.unpublish(i)
	delete(s.creds, i.InstanceId)
	delete(s.creds, i.InstanceId+"/identity")
	i.StateReason, i.Ended = why, time.Now()
	r.dropInstanceAddresses(i.InstanceId)
	for _, b := range i.BlockDeviceMappings {
		if v := r.Volumes[b.Ebs.VolumeId]; v != nil && b.Ebs.DeleteOnTermination {
			delete(r.Volumes, v.VolumeId)
			delete(r.Tags, v.VolumeId)
		} else if v != nil {
			v.State, v.Attachments = "available", nil
		}
	}
	i.BlockDeviceMappings = nil
	if i.Container == "" {
		i.State = stateOf("terminated")
		return
	}
	i.State = stateOf("shutting-down")
	s.remove(i.InstanceId, i.Container)
}

// remove deletes an instance's container in the background; a shutting-down
// instance is then terminated.
func (s *Service) remove(id, name string) {
	s.serial(id, func() {
		s.hub.Docker().Remove(context.Background(), imdsName(id))
		s.hub.Docker().Remove(context.Background(), name)
		s.mu.Lock()
		if srv := s.imdsLn[id]; srv != nil {
			srv.Close()
			delete(s.imdsLn, id)
		}
		if _, i := s.find(id); i != nil && i.State.Name == "shutting-down" {
			i.State = stateOf("terminated")
		}
		s.mu.Unlock()
	})
}

// unpublish forgets where pogi reaches an instance's ports.
func (s *Service) unpublish(i *Instance) {
	for _, p := range i.Ports {
		s.hub.SetEndpoint("instance:"+i.InstanceId+":"+strconv.Itoa(p), "")
		s.hub.SetEndpoint("ip:"+i.PrivateIpAddress+":"+strconv.Itoa(p), "")
	}
}

// serial runs fn in the background after the engine calls queued before for
// the same instance, so they happen in API order.
func (s *Service) serial(id string, fn func()) {
	s.qmu.Lock()
	prev, done := s.queue[id], make(chan struct{})
	s.queue[id] = done
	s.qmu.Unlock()
	core.Go("ec2.serial", func() {
		if prev != nil {
			<-prev
		}
		defer func() { // runs on a panic too, so later calls are not stuck
			close(done)
			s.qmu.Lock()
			if s.queue[id] == done {
				delete(s.queue, id)
			}
			s.qmu.Unlock()
		}()
		fn()
	})
}

// find looks an instance up in every region; lock held.
func (s *Service) find(id string) (*region, *Instance) {
	for _, r := range s.st.Regions {
		if i := r.Instances[id]; i != nil {
			return r, i
		}
	}
	return nil, nil
}

// bootScript is the entrypoint of instance containers. In order it: installs
// and starts sshd when the instance has a key pair; waits for pogi to set up
// 169.254.169.254 (see forward); runs UserData once (logged as cloud-init
// does); and stays up. Its echo lines are the console output.
const bootScript = `trap 'exit 0' TERM INT
mkdir -p /var/lib/cloud/instance /var/log
rm -f /var/lib/cloud/instance/pogi-net
if [ -n "${POGI_SSH_KEY:-}" ]; then
  if [ ! -x /usr/sbin/sshd ]; then
    pm=; for c in apk apt-get dnf yum; do command -v $c >/dev/null 2>&1 && { pm=$c; break; }; done
    case $pm in
      apk) apk add --no-cache openssh-server >/dev/null 2>&1 ;;
      apt-get) { apt-get update -qq && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq openssh-server; } >/dev/null 2>&1 ;;
      dnf|yum) $pm install -y -q openssh-server >/dev/null 2>&1 ;;
      *) echo "pogi: ssh is off: this image has no package manager (apk, apt-get, dnf or yum) to install sshd" ;;
    esac
    [ -x /usr/sbin/sshd ] || [ -z "$pm" ] || echo "pogi: ssh is off: $pm could not install openssh-server (does the container have network access?)"
  fi
  if [ -x /usr/sbin/sshd ]; then
    mkdir -p /root/.ssh /run/sshd && grep -qxF "$POGI_SSH_KEY" /root/.ssh/authorized_keys 2>/dev/null || printf '%s\n' "$POGI_SSH_KEY" >> /root/.ssh/authorized_keys
    chmod 700 /root/.ssh && chmod 600 /root/.ssh/authorized_keys
    ssh-keygen -A >/dev/null 2>&1; /usr/sbin/sshd && echo "pogi: sshd started"
  fi
fi
n=0; while [ ! -e /var/lib/cloud/instance/pogi-net ] && [ $n -lt 150 ]; do sleep 0.2; n=$((n+1)); done
if [ -n "${POGI_USER_DATA:-}" ] && [ ! -e /var/lib/cloud/instance/boot-finished ]; then
  printf '%s' "$POGI_USER_DATA" > /var/lib/cloud/instance/user-data.txt
  chmod +x /var/lib/cloud/instance/user-data.txt
  env -u POGI_USER_DATA -u POGI_SSH_KEY /var/lib/cloud/instance/user-data.txt >> /var/log/cloud-init-output.log 2>&1
fi
touch /var/lib/cloud/instance/boot-finished
while :; do sleep 3600 & wait $!; done`

// container describes an instance's container; lock held.
func (s *Service) container(r *region, i *Instance) *docker.Container {
	host := s.hub.Docker().HostAddr()
	port := strconv.Itoa(cmp.Or(s.hub.Cfg.Port, 7644))
	if u, err := url.Parse(s.hub.URL); err == nil && u.Port() != "" {
		port = u.Port()
	}
	pogi := "http://" + host + ":" + port
	ct := &docker.Container{Name: i.Container, Image: cmp.Or(s.hub.Cfg.DockerImages["ec2."+i.ImageId], r.imageFor(i.ImageId).Docker),
		Service: "ec2", Resource: i.InstanceId, Entrypoint: []string{"/bin/sh", "-c", bootScript}, Aliases: []string{i.PrivateDnsName},
		Ports: map[int]int{}, Env: []string{"AWS_REGION=" + r.Name, "AWS_DEFAULT_REGION=" + r.Name, "AWS_ENDPOINT_URL=" + pogi,
			"AWS_EC2_METADATA_SERVICE_ENDPOINT=" + pogi + "/_aws/imds/" + i.InstanceId + "/"}}
	if host == "127.0.0.1" { // a Linux engine without a VM reaches pogi's loopback from the host network only
		// ponytail: there instances share the host's ports; give them their own network namespace if that bites
		ct.Network, ct.Aliases = "host", nil
	}
	for _, p := range i.Ports {
		ct.Ports[p] = 0
	}
	if sh := script(i.UserData); sh != "" {
		ct.Env = append(ct.Env, "POGI_USER_DATA="+sh)
	}
	if k := r.Keys[i.KeyName]; k != nil && k.PublicKey != "" {
		ct.Env = append(ct.Env, "POGI_SSH_KEY="+k.PublicKey)
	}
	return ct
}

// script is the shell script UserData runs: a #! script as is, or the
// runcmd list of a #cloud-config.
// ponytail: a runcmd subset (one-line items, plain or [list]); other
// cloud-config modules, MIME multipart and gzip are ignored.
func script(userData string) string {
	b, _ := base64.StdEncoding.DecodeString(userData)
	text := string(b)
	switch {
	case strings.HasPrefix(text, "#!"):
		return text
	case !strings.HasPrefix(text, "#cloud-config"):
		return ""
	}
	var cmds []string
	in := false
	for _, line := range strings.Split(text, "\n") {
		t := strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(line, "runcmd:"):
			in = true
		case in && strings.HasPrefix(t, "- "):
			cmds = append(cmds, shellLine(strings.TrimSpace(t[2:])))
		case in && t != "" && line[0] != ' ' && line[0] != '\t':
			in = false
		}
	}
	if len(cmds) == 0 {
		return ""
	}
	return "#!/bin/sh\n" + strings.Join(cmds, "\n") + "\n"
}

// shellLine turns a runcmd item into a shell line: a string as is, a list
// word by word, quoted.
func shellLine(item string) string {
	if !strings.HasPrefix(item, "[") {
		return unquote(item)
	}
	var words []string
	for _, w := range strings.Split(strings.Trim(item, "[]"), ",") {
		words = append(words, "'"+strings.ReplaceAll(unquote(strings.TrimSpace(w)), "'", `'\''`)+"'")
	}
	return strings.Join(words, " ")
}

func unquote(s string) string {
	if len(s) >= 2 && (s[0] == '"' || s[0] == '\'') && s[len(s)-1] == s[0] {
		if u, err := strconv.Unquote(s); err == nil && s[0] == '"' {
			return u
		}
		return s[1 : len(s)-1]
	}
	return s
}

// consoleOutput is what the instance's boot script and the engine logged:
// base64, as EC2 returns it. An instance without a container has none.
func (s *Service) consoleOutput(c *core.Ctx, in *struct{ InstanceId string }) (any, error) {
	s.mu.Lock()
	i := s.reg(c).Instances[in.InstanceId]
	s.mu.Unlock()
	if i == nil {
		return nil, core.Errorf(400, "InvalidInstanceID.NotFound", "The instance ID '%s' does not exist", in.InstanceId)
	}
	var out strings.Builder
	if i.Container != "" {
		s.hub.Docker().Logs(context.Background(), i.Container, false, func(l string) { out.WriteString(l + "\n") }) // a missing container has no output
	}
	return &struct {
		Header
		InstanceId string `xml:"instanceId"`
		Timestamp  string `xml:"timestamp"`
		Output     string `xml:"output"`
	}{InstanceId: in.InstanceId, Timestamp: time.Now().UTC().Format(core.ISO), Output: base64.StdEncoding.EncodeToString([]byte(out.String()))}, nil
}

// ---- attributes ----

func (s *Service) modifyInstanceAttribute(c *core.Ctx, in *struct {
	InstanceId                                                           string
	InstanceType, InstanceInitiatedShutdownBehavior                      *struct{ Value string }
	UserData                                                             *struct{ Value []byte }
	DisableApiTermination, DisableApiStop, SourceDestCheck, EbsOptimized *struct{ Value bool }
	Groups                                                               []string `query:"GroupId"`
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	i := r.Instances[in.InstanceId]
	if i == nil {
		return nil, core.Errorf(400, "InvalidInstanceID.NotFound", "The instance ID '%s' does not exist", in.InstanceId)
	}
	if (in.InstanceType != nil || in.UserData != nil) && i.State.Name != "stopped" {
		return nil, core.Errorf(400, "IncorrectInstanceState", "The instance '%s' is not in the 'stopped' state.", i.InstanceId)
	}
	if in.InstanceType != nil {
		if _, ok := cpus(in.InstanceType.Value); !ok {
			return nil, invalid("Invalid value '%s' for InstanceType.", in.InstanceType.Value)
		}
	}
	groups := i.SecurityGroups
	if len(in.Groups) > 0 {
		var err error
		if groups, err = r.groupsFor(i.VpcId, in.Groups, nil); err != nil {
			return nil, err
		}
	}
	i.SecurityGroups = groups
	if in.InstanceType != nil {
		i.InstanceType = in.InstanceType.Value
	}
	if in.InstanceInitiatedShutdownBehavior != nil {
		i.ShutdownBehavior = in.InstanceInitiatedShutdownBehavior.Value
	}
	if in.UserData != nil {
		i.UserData = base64.StdEncoding.EncodeToString(in.UserData.Value)
	}
	for _, b := range []struct {
		src *struct{ Value bool }
		dst *bool
	}{{in.DisableApiTermination, &i.DisableApiTermination}, {in.DisableApiStop, &i.DisableApiStop},
		{in.SourceDestCheck, &i.SourceDestCheck}, {in.EbsOptimized, &i.EbsOptimized}} {
		if b.src != nil {
			*b.dst = b.src.Value
		}
	}
	return ok(), nil
}

type strValue struct {
	Value string `xml:"value,omitempty"`
}

func (s *Service) describeInstanceAttribute(c *core.Ctx, in *struct{ InstanceId, Attribute string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	i := s.reg(c).Instances[in.InstanceId]
	if i == nil {
		return nil, core.Errorf(400, "InvalidInstanceID.NotFound", "The instance ID '%s' does not exist", in.InstanceId)
	}
	out := &struct {
		Header
		InstanceId                        string            `xml:"instanceId"`
		InstanceType                      *strValue         `xml:"instanceType"`
		UserData                          *strValue         `xml:"userData"`
		InstanceInitiatedShutdownBehavior *strValue         `xml:"instanceInitiatedShutdownBehavior"`
		RootDeviceName                    *strValue         `xml:"rootDeviceName"`
		DisableApiTermination             *boolValue        `xml:"disableApiTermination"`
		DisableApiStop                    *boolValue        `xml:"disableApiStop"`
		SourceDestCheck                   *boolValue        `xml:"sourceDestCheck"`
		EbsOptimized                      *boolValue        `xml:"ebsOptimized"`
		EnaSupport                        *boolValue        `xml:"enaSupport"`
		Groups                            []GroupIdentifier `xml:"groupSet>item"`
		BlockDeviceMappings               []BlockDevice     `xml:"blockDeviceMapping>item"`
	}{InstanceId: i.InstanceId}
	switch in.Attribute {
	case "instanceType":
		out.InstanceType = &strValue{i.InstanceType}
	case "userData":
		out.UserData = &strValue{i.UserData}
	case "instanceInitiatedShutdownBehavior":
		out.InstanceInitiatedShutdownBehavior = &strValue{i.ShutdownBehavior}
	case "rootDeviceName":
		out.RootDeviceName = &strValue{i.RootDeviceName}
	case "disableApiTermination":
		out.DisableApiTermination = &boolValue{i.DisableApiTermination}
	case "disableApiStop":
		out.DisableApiStop = &boolValue{i.DisableApiStop}
	case "sourceDestCheck":
		out.SourceDestCheck = &boolValue{i.SourceDestCheck}
	case "ebsOptimized":
		out.EbsOptimized = &boolValue{i.EbsOptimized}
	case "enaSupport":
		out.EnaSupport = &boolValue{i.EnaSupport}
	case "groupSet":
		out.Groups = slices.Clone(i.SecurityGroups)
	case "blockDeviceMapping":
		out.BlockDeviceMappings = slices.Clone(i.BlockDeviceMappings)
	case "kernel", "ramdisk", "productCodes", "sriovNetSupport", "enclaveOptions":
	default:
		return nil, invalid("Value (%s) for parameter attribute is invalid. Unknown attribute.", in.Attribute)
	}
	return out, nil
}

func (s *Service) describeCreditSpecifications(c *core.Ctx, in *struct {
	InstanceIds []string `query:"InstanceId"`
	Filters     []filter `query:"Filter"`
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	list, err := describe(r, r.Instances, in.InstanceIds, in.Filters, "InvalidInstanceID.NotFound", "instance ID", (*Instance).view)
	type spec struct {
		InstanceId string `xml:"instanceId"`
		CpuCredits string `xml:"cpuCredits"`
	}
	out := &struct {
		Header
		Specs []spec `xml:"instanceCreditSpecificationSet>item"`
	}{}
	for _, i := range list {
		if n, _ := cpus(i.InstanceType); n > 0 && strings.HasPrefix(i.InstanceType, "t") {
			out.Specs = append(out.Specs, spec{i.InstanceId, cmp.Or(i.CpuCredits, map[bool]string{true: "standard", false: "unlimited"}[strings.HasPrefix(i.InstanceType, "t2.")])})
		}
	}
	return out, err
}

// uefiData answers for instances that boot legacy BIOS: no UEFI variables.
func (s *Service) uefiData(c *core.Ctx, in *struct{ InstanceId string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.reg(c).Instances[in.InstanceId] == nil {
		return nil, core.Errorf(400, "InvalidInstanceID.NotFound", "The instance ID '%s' does not exist", in.InstanceId)
	}
	return &struct {
		Header
		InstanceId string `xml:"instanceId"`
		UefiData   string `xml:"uefiData"`
	}{InstanceId: in.InstanceId}, nil
}

// ---- instance types ----

// TypeInfo describes an instance type.
type TypeInfo struct {
	InstanceType                 string   `xml:"instanceType"`
	CurrentGeneration            bool     `xml:"currentGeneration"`
	FreeTierEligible             bool     `xml:"freeTierEligible"`
	SupportedUsageClasses        []string `xml:"supportedUsageClasses>item"`
	SupportedRootDeviceTypes     []string `xml:"supportedRootDeviceTypes>item"`
	SupportedVirtualizationTypes []string `xml:"supportedVirtualizationTypes>item"`
	BareMetal                    bool     `xml:"bareMetal"`
	Hypervisor                   string   `xml:"hypervisor"`
	ProcessorInfo                struct {
		SupportedArchitectures []string `xml:"supportedArchitectures>item"`
	} `xml:"processorInfo"`
	VCpuInfo struct {
		DefaultVCpus          int `xml:"defaultVCpus"`
		DefaultCores          int `xml:"defaultCores"`
		DefaultThreadsPerCore int `xml:"defaultThreadsPerCore"`
	} `xml:"vCpuInfo"`
	MemoryInfo struct {
		SizeInMiB int `xml:"sizeInMiB"`
	} `xml:"memoryInfo"`
	NetworkInfo struct {
		NetworkPerformance           string    `xml:"networkPerformance"`
		MaximumNetworkInterfaces     int       `xml:"maximumNetworkInterfaces"`
		MaximumNetworkCards          int       `xml:"maximumNetworkCards"`
		DefaultNetworkCardIndex      int       `xml:"defaultNetworkCardIndex"`
		NetworkCards                 []netCard `xml:"networkCards>item"`
		Ipv4AddressesPerInterface    int       `xml:"ipv4AddressesPerInterface"`
		Ipv6AddressesPerInterface    int       `xml:"ipv6AddressesPerInterface"`
		Ipv6Supported                bool      `xml:"ipv6Supported"`
		EnaSupport                   string    `xml:"enaSupport"`
		EfaSupported                 bool      `xml:"efaSupported"`
		EncryptionInTransitSupported bool      `xml:"encryptionInTransitSupported"`
	} `xml:"networkInfo"`
	InstanceStorageSupported      bool `xml:"instanceStorageSupported"`
	BurstablePerformanceSupported bool `xml:"burstablePerformanceSupported"`
}

// netCard is one network card of an instance type.
type netCard struct {
	NetworkCardIndex         int    `xml:"networkCardIndex"`
	NetworkPerformance       string `xml:"networkPerformance"`
	MaximumNetworkInterfaces int    `xml:"maximumNetworkInterfaces"`
}

// nics are the network interfaces and IPv4 addresses per interface of an
// instance size, as AWS lists them for the common families.
var nics = map[string][2]int{"nano": {2, 2}, "micro": {2, 2}, "small": {3, 4}, "medium": {3, 6}, "large": {3, 10}, "xlarge": {4, 15}, "2xlarge": {4, 15}, "metal": {15, 50}}

// instanceTypes are what DescribeInstanceTypes lists; any family.size describes.
var instanceTypes = strings.Fields("t2.micro t2.small t2.medium t3.nano t3.micro t3.small t3.medium t3.large t3.xlarge t4g.micro t4g.small " +
	"t4g.medium t2.nano t3a.nano m5.large m5.xlarge m6i.large m6i.xlarge m7g.large c5.large c6i.large c7g.large r5.large r6i.large")

var (
	armFamily  = regexp.MustCompile(`^[a-z]+\d+[a-z]*g`) // t4g, m6gd, c7gn
	familyName = regexp.MustCompile(`^[a-z][a-z0-9-]*$`)
	tSizes     = strings.Fields("nano micro small medium large xlarge 2xlarge")
)

// cpus returns the vCPUs of an instance type: 2 up to large, 4 per
// xlarge, 96 for metal; ok is false when the name is no family.size.
func cpus(name string) (int, bool) {
	family, size, _ := strings.Cut(name, ".")
	n, err := strconv.Atoi(cmp.Or(strings.TrimSuffix(size, "xlarge"), "1"))
	switch {
	case !familyName.MatchString(family):
		return 0, false
	case slices.Contains(tSizes[:5], size):
		return 2, true
	case size == "metal":
		return 96, true
	case strings.HasSuffix(size, "xlarge") && err == nil && n > 0:
		return 4 * n, true
	}
	return 0, false
}

// describeType describes any family.size.
// ponytail: a formula, not AWS's table: vCPUs by size (cpus), memory 2, 4 or
// 8 GiB a vCPU by family letter (c, m, r), t types by size; t2.micro gets 2 vCPUs, not 1;
// one network card, with interface and address counts by size (nics).
func describeType(name string) TypeInfo {
	vcpus, _ := cpus(name)
	family, size, _ := strings.Cut(name, ".")
	t := TypeInfo{InstanceType: name, CurrentGeneration: true, SupportedUsageClasses: []string{"on-demand", "spot"},
		SupportedRootDeviceTypes: []string{"ebs"}, SupportedVirtualizationTypes: []string{"hvm"}, BareMetal: size == "metal", Hypervisor: "nitro",
		BurstablePerformanceSupported: family[0] == 't', FreeTierEligible: name == "t2.micro" || name == "t3.micro"}
	t.ProcessorInfo.SupportedArchitectures = []string{"x86_64"}
	if armFamily.MatchString(family) {
		t.ProcessorInfo.SupportedArchitectures = []string{"arm64"}
	}
	t.VCpuInfo.DefaultVCpus, t.VCpuInfo.DefaultCores, t.VCpuInfo.DefaultThreadsPerCore = vcpus, max(vcpus/2, 1), min(vcpus, 2)
	t.MemoryInfo.SizeInMiB = vcpus * 1024 * cmp.Or(map[byte]int{'c': 2, 'r': 8, 'x': 16}[family[0]], 4)
	if k := slices.Index(tSizes, size); family[0] == 't' && k >= 0 {
		t.MemoryInfo.SizeInMiB = 512 << k
	}
	n, ok := nics[size]
	if !ok { // 4xlarge and up
		n = [2]int{8, 30}
	}
	t.NetworkInfo.NetworkPerformance = "Up to 10 Gigabit"
	t.NetworkInfo.MaximumNetworkInterfaces, t.NetworkInfo.MaximumNetworkCards, t.NetworkInfo.Ipv4AddressesPerInterface = n[0], 1, n[1]
	t.NetworkInfo.Ipv6AddressesPerInterface, t.NetworkInfo.Ipv6Supported, t.NetworkInfo.EnaSupport = n[1], true, "required"
	t.NetworkInfo.NetworkCards = []netCard{{0, t.NetworkInfo.NetworkPerformance, n[0]}}
	return t
}

func (s *Service) describeInstanceTypes(c *core.Ctx, in *struct {
	InstanceTypes []string `query:"InstanceType"`
	Filters       []filter `query:"Filter"`
}) (any, error) {
	names := in.InstanceTypes
	if len(names) == 0 {
		names = instanceTypes
	}
	out := &struct {
		Header
		InstanceTypes []TypeInfo `xml:"instanceTypeSet>item"`
	}{}
	for _, n := range names {
		if _, ok := cpus(n); !ok {
			return nil, core.Errorf(400, "InvalidInstanceType", "The following supplied instance types do not exist: [%s]", n)
		}
		t := describeType(n)
		if match(in.Filters, map[string][]string{"instance-type": {n}, "current-generation": {"true"},
			"burstable-performance-supported":       {strconv.FormatBool(t.BurstablePerformanceSupported)},
			"processor-info.supported-architecture": t.ProcessorInfo.SupportedArchitectures,
			"vcpu-info.default-vcpus":               {strconv.Itoa(t.VCpuInfo.DefaultVCpus)}, "memory-info.size-in-mib": {strconv.Itoa(t.MemoryInfo.SizeInMiB)},
			"free-tier-eligible": {strconv.FormatBool(t.FreeTierEligible)}}, nil) {
			out.InstanceTypes = append(out.InstanceTypes, t)
		}
	}
	return out, nil
}

// ---- launch templates ----

// LaunchTemplate is a launch template and its versions.
type LaunchTemplate struct {
	LaunchTemplateId     string             `xml:"launchTemplateId"`
	LaunchTemplateName   string             `xml:"launchTemplateName"`
	CreateTime           string             `xml:"createTime"`
	CreatedBy            string             `xml:"createdBy"`
	DefaultVersionNumber int                `xml:"defaultVersionNumber"`
	LatestVersionNumber  int                `xml:"latestVersionNumber"`
	TagSet               []Tag              `xml:"tagSet>item" json:"-"`
	Versions             []*TemplateVersion `xml:"-"`
}

func (lt *LaunchTemplate) view() (string, *[]Tag, map[string][]string) {
	return lt.LaunchTemplateId, &lt.TagSet, map[string][]string{"launch-template-name": {lt.LaunchTemplateName}, "create-time": {lt.CreateTime}}
}

// TemplateVersion is one version of a launch template. Its data keeps the
// Query keys it came with (ImageId, SecurityGroupId.1, …): RunInstances
// decodes them, responses render them (see ltData).
type TemplateVersion struct {
	LaunchTemplateId   string `xml:"launchTemplateId"`
	LaunchTemplateName string `xml:"launchTemplateName"`
	VersionNumber      int    `xml:"versionNumber"`
	VersionDescription string `xml:"versionDescription,omitempty"`
	CreateTime         string `xml:"createTime"`
	CreatedBy          string `xml:"createdBy"`
	DefaultVersion     bool   `xml:"defaultVersion"`
	LaunchTemplateData ltData `xml:"launchTemplateData"`
}

// ltData is launch template data as Query keys.
type ltData map[string]string

// MarshalXML renders the keys as EC2 answers: SecurityGroupId.1 becomes
// securityGroupIdSet/item, Monitoring.Enabled monitoring/enabled.
func (d ltData) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	root := &qtree{}
	for k, v := range d {
		root.add(strings.Split(k, "."), v)
	}
	return root.encode(e, start, "")
}

type qtree struct {
	val  string
	kids map[string]*qtree
}

func (t *qtree) add(path []string, v string) {
	if len(path) == 0 {
		t.val = v
		return
	}
	if t.kids == nil {
		t.kids = map[string]*qtree{}
	}
	if t.kids[path[0]] == nil {
		t.kids[path[0]] = &qtree{}
	}
	t.kids[path[0]].add(path[1:], v)
}

// encode writes t as the element start; numbered children are the <item>s
// of a list. owner is the list t is an item of.
func (t *qtree) encode(e *xml.Encoder, start xml.StartElement, owner string) error {
	if t.kids == nil {
		return e.EncodeElement(t.val, start)
	}
	if err := e.EncodeToken(start); err != nil {
		return err
	}
	keys := slices.SortedFunc(maps.Keys(t.kids), func(a, b string) int { return cmp.Or(cmp.Compare(len(a), len(b)), strings.Compare(a, b)) })
	for _, k := range keys {
		kid, name, item := t.kids[k], strings.ToLower(k[:1])+k[1:], ""
		if _, err := strconv.Atoi(k); err == nil {
			name, item = "item", start.Name.Local
		} else if kid.kids != nil && kid.list() {
			name = map[bool]string{true: "groupSet", false: name + "Set"}[owner == "networkInterfaceSet" && k == "SecurityGroupId"]
			if k == "LicenseSpecification" {
				name = "licenseSet"
			}
		}
		if err := kid.encode(e, xml.StartElement{Name: xml.Name{Local: name}}, item); err != nil {
			return err
		}
	}
	return e.EncodeToken(start.End())
}

func (t *qtree) list() bool {
	for k := range t.kids {
		_, err := strconv.Atoi(k)
		return err == nil
	}
	return false
}

// templateData is the request's LaunchTemplateData as Query keys, from
// either wire: JSON members flatten into the same keys under their own names,
// which RunInstances decodes as well.
func templateData(c *core.Ctx) ltData {
	out := ltData{}
	p := c.Payload()
	if bytes.HasPrefix(p, []byte("{")) {
		var in struct{ LaunchTemplateData any }
		json.Unmarshal(p, &in)
		flat("", in.LaunchTemplateData, out)
		return out
	}
	form, _ := url.ParseQuery(string(p))
	for k, v := range form {
		if rest, ok := strings.CutPrefix(k, "LaunchTemplateData."); ok {
			out[rest] = v[0]
		}
	}
	return out
}

func flat(prefix string, v any, out ltData) {
	switch t := v.(type) {
	case map[string]any:
		for k, x := range t {
			flat(prefix+k+".", x, out)
		}
	case []any:
		for i, x := range t {
			flat(prefix+strconv.Itoa(i+1)+".", x, out)
		}
	case nil:
	default:
		out[strings.TrimSuffix(prefix, ".")] = fmt.Sprint(t)
	}
}

// template finds a launch template by id or name; lock held.
func (r *region) template(id, name string) (*LaunchTemplate, error) {
	if lt := r.Templates[id]; lt != nil {
		return lt, nil
	}
	if id != "" {
		return nil, core.Errorf(400, "InvalidLaunchTemplateId.NotFound", "The specified launch template, with template ID %s, does not exist.", id)
	}
	for _, lt := range r.Templates {
		if lt.LaunchTemplateName == name {
			return lt, nil
		}
	}
	if name == "" {
		return nil, core.Errorf(400, "MissingParameter", "The request must include either LaunchTemplateId or LaunchTemplateName")
	}
	return nil, core.Errorf(400, "InvalidLaunchTemplateName.NotFoundException", "The specified launch template, with template name %s, does not exist.", name)
}

// version resolves $Latest, $Default or a version number.
func (lt *LaunchTemplate) version(v string) (*TemplateVersion, error) {
	n, err := strconv.Atoi(v)
	switch {
	case v == "$Latest":
		n, err = lt.LatestVersionNumber, nil
	case v == "$Default":
		n, err = lt.DefaultVersionNumber, nil
	}
	for _, x := range lt.Versions {
		if err == nil && x.VersionNumber == n {
			cp := *x
			cp.LaunchTemplateId, cp.LaunchTemplateName, cp.DefaultVersion = lt.LaunchTemplateId, lt.LaunchTemplateName, n == lt.DefaultVersionNumber
			return &cp, nil
		}
	}
	return nil, core.Errorf(400, "InvalidLaunchTemplateId.VersionNotFound", "Could not find launch template version %s for launch template %s", v, lt.LaunchTemplateId)
}

// add appends a version; lock held.
func (lt *LaunchTemplate) add(data ltData, desc string) *TemplateVersion {
	lt.LatestVersionNumber++
	v := &TemplateVersion{VersionNumber: lt.LatestVersionNumber, VersionDescription: desc, CreateTime: time.Now().UTC().Format(core.ISO),
		CreatedBy: lt.CreatedBy, LaunchTemplateData: data}
	lt.Versions = append(lt.Versions, v)
	out, _ := lt.version(strconv.Itoa(v.VersionNumber))
	return out
}

var ltName = regexp.MustCompile(`^[a-zA-Z0-9().\-/_]{3,128}$`)

func (s *Service) createLaunchTemplate(c *core.Ctx, in *struct {
	LaunchTemplateName, VersionDescription, ClientToken string
	TagSpecifications                                   []tagSpec `query:"TagSpecification"`
}) (any, error) {
	if !ltName.MatchString(in.LaunchTemplateName) {
		return nil, invalid("Launch template name must be between 3 and 128 characters, and may contain letters, numbers, and the following characters: - ( ) . / _.'")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	if _, err := r.template("", in.LaunchTemplateName); err == nil {
		return nil, core.Errorf(400, "InvalidLaunchTemplateName.AlreadyExistsException", "Launch template name already in use.")
	}
	lt := &LaunchTemplate{LaunchTemplateId: newID("lt"), LaunchTemplateName: in.LaunchTemplateName, CreateTime: time.Now().UTC().Format(core.ISO),
		CreatedBy: c.GlobalARN("iam", "root"), DefaultVersionNumber: 1}
	lt.add(templateData(c), in.VersionDescription)
	r.Templates[lt.LaunchTemplateId] = lt
	r.tag(lt.LaunchTemplateId, typed(in.TagSpecifications, "launch-template"))
	return &struct {
		Header
		LaunchTemplate LaunchTemplate `xml:"launchTemplate"`
	}{LaunchTemplate: withTags(r, lt, (*LaunchTemplate).view)}, nil
}

func (s *Service) createLaunchTemplateVersion(c *core.Ctx, in *struct {
	LaunchTemplateId, LaunchTemplateName, SourceVersion, VersionDescription string
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	lt, err := s.reg(c).template(in.LaunchTemplateId, in.LaunchTemplateName)
	if err != nil {
		return nil, err
	}
	data := templateData(c)
	if in.SourceVersion != "" { // the new keys replace the source's of the same top-level name
		src, err := lt.version(in.SourceVersion)
		if err != nil {
			return nil, err
		}
		top := func(k string) string { t, _, _ := strings.Cut(k, "."); return t }
		for k, v := range src.LaunchTemplateData {
			if !slices.ContainsFunc(slices.Collect(maps.Keys(data)), func(n string) bool { return top(n) == top(k) }) {
				data[k] = v
			}
		}
	}
	return &struct {
		Header
		Version *TemplateVersion `xml:"launchTemplateVersion"`
	}{Version: lt.add(data, in.VersionDescription)}, nil
}

func (s *Service) describeLaunchTemplates(c *core.Ctx, in *struct {
	LaunchTemplateIds   []string `query:"LaunchTemplateId"`
	LaunchTemplateNames []string `query:"LaunchTemplateName"`
	Filters             []filter `query:"Filter"`
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	ids := in.LaunchTemplateIds
	for _, n := range in.LaunchTemplateNames {
		lt, err := r.template("", n)
		if err != nil {
			return nil, err
		}
		ids = append(ids, lt.LaunchTemplateId)
	}
	list, err := describe(r, r.Templates, ids, in.Filters, "InvalidLaunchTemplateId.NotFound", "launch template", (*LaunchTemplate).view)
	return &struct {
		Header
		LaunchTemplates []LaunchTemplate `xml:"launchTemplates>item"`
	}{LaunchTemplates: list}, err
}

func (s *Service) describeLaunchTemplateVersions(c *core.Ctx, in *struct {
	LaunchTemplateId, LaunchTemplateName, MinVersion, MaxVersion string
	Versions                                                     []string `query:"LaunchTemplateVersion"`
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	all := slices.Collect(maps.Values(r.Templates))
	if in.LaunchTemplateId+in.LaunchTemplateName != "" {
		lt, err := r.template(in.LaunchTemplateId, in.LaunchTemplateName)
		if err != nil {
			return nil, err
		}
		all = []*LaunchTemplate{lt}
	}
	out := &struct {
		Header
		Versions []*TemplateVersion `xml:"launchTemplateVersionSet>item"`
	}{}
	lo, _ := strconv.Atoi(in.MinVersion)
	hi, err := strconv.Atoi(in.MaxVersion)
	if err != nil {
		hi = 1 << 30
	}
	for _, lt := range all {
		want := in.Versions
		if len(want) == 0 {
			for _, v := range lt.Versions {
				if v.VersionNumber >= lo && v.VersionNumber <= hi {
					want = append(want, strconv.Itoa(v.VersionNumber))
				}
			}
		}
		for _, w := range want {
			v, err := lt.version(w)
			if err != nil {
				return nil, err
			}
			out.Versions = append(out.Versions, v)
		}
	}
	return out, nil
}

func (s *Service) modifyLaunchTemplate(c *core.Ctx, in *struct {
	LaunchTemplateId, LaunchTemplateName string
	DefaultVersion                       string `query:"SetDefaultVersion"`
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	lt, err := r.template(in.LaunchTemplateId, in.LaunchTemplateName)
	if err != nil {
		return nil, err
	}
	if in.DefaultVersion != "" {
		v, err := lt.version(in.DefaultVersion)
		if err != nil {
			return nil, err
		}
		lt.DefaultVersionNumber = v.VersionNumber
	}
	return &struct {
		Header
		LaunchTemplate LaunchTemplate `xml:"launchTemplate"`
	}{LaunchTemplate: withTags(r, lt, (*LaunchTemplate).view)}, nil
}

func (s *Service) deleteLaunchTemplate(c *core.Ctx, in *struct{ LaunchTemplateId, LaunchTemplateName string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	lt, err := r.template(in.LaunchTemplateId, in.LaunchTemplateName)
	if err != nil {
		return nil, err
	}
	out := withTags(r, lt, (*LaunchTemplate).view)
	delete(r.Templates, lt.LaunchTemplateId)
	delete(r.Tags, lt.LaunchTemplateId)
	return &struct {
		Header
		LaunchTemplate LaunchTemplate `xml:"launchTemplate"`
	}{LaunchTemplate: out}, nil
}

// ---- volumes ----

// Volume is an EBS volume: metadata only.
type Volume struct {
	VolumeId           string             `xml:"volumeId"`
	Size               int                `xml:"size"`
	SnapshotId         string             `xml:"snapshotId"`
	AvailabilityZone   string             `xml:"availabilityZone"`
	State              string             `xml:"status"`
	CreateTime         string             `xml:"createTime"`
	Attachments        []VolumeAttachment `xml:"attachmentSet>item"`
	VolumeType         string             `xml:"volumeType"`
	Iops               int                `xml:"iops,omitempty"`
	Throughput         int                `xml:"throughput,omitempty"`
	Encrypted          bool               `xml:"encrypted"`
	KmsKeyId           string             `xml:"kmsKeyId,omitempty"`
	MultiAttachEnabled bool               `xml:"multiAttachEnabled"`
	TagSet             []Tag              `xml:"tagSet>item" json:"-"`
}

// VolumeAttachment is a volume attached to an instance.
type VolumeAttachment struct {
	VolumeId            string `xml:"volumeId"`
	InstanceId          string `xml:"instanceId"`
	Device              string `xml:"device"`
	State               string `xml:"status"`
	AttachTime          string `xml:"attachTime"`
	DeleteOnTermination bool   `xml:"deleteOnTermination"`
}

func (v *Volume) view() (string, *[]Tag, map[string][]string) {
	v.Attachments = slices.Clone(v.Attachments)
	attrs := map[string][]string{"volume-id": {v.VolumeId}, "status": {v.State}, "availability-zone": {v.AvailabilityZone},
		"volume-type": {v.VolumeType}, "size": {strconv.Itoa(v.Size)}, "encrypted": {strconv.FormatBool(v.Encrypted)}, "snapshot-id": {v.SnapshotId},
		"attachment.instance-id": nil, "attachment.device": nil, "attachment.status": nil, "attachment.delete-on-termination": nil} // an unattached volume matches none of them
	for _, a := range v.Attachments {
		attrs["attachment.instance-id"], attrs["attachment.device"] = []string{a.InstanceId}, []string{a.Device}
		attrs["attachment.status"], attrs["attachment.delete-on-termination"] = []string{a.State}, []string{strconv.FormatBool(a.DeleteOnTermination)}
	}
	return v.VolumeId, &v.TagSet, attrs
}

// newVolume creates a volume with EC2's defaults; lock held.
func (r *region) newVolume(az string, size int, typ string, iops, throughput int, encrypted bool, kms, snapshot string) *Volume {
	if x := r.x(); x.EbsDefault { // EBS encryption by default
		encrypted, kms = true, cmp.Or(kms, x.ebsKeyID())
	}
	typ = cmp.Or(typ, "gp2")
	switch typ {
	case "gp2":
		iops = min(max(100, 3*size), 16000)
	case "gp3":
		iops, throughput = cmp.Or(iops, 3000), cmp.Or(throughput, 125)
	}
	v := &Volume{VolumeId: newID("vol"), Size: size, SnapshotId: snapshot, AvailabilityZone: az, State: "available",
		CreateTime: time.Now().UTC().Format(core.ISO), VolumeType: typ, Iops: iops, Throughput: throughput, Encrypted: encrypted, KmsKeyId: kms}
	r.Volumes[v.VolumeId] = v
	return v
}

// volumes gives a new instance its root volume and the EBS volumes its
// block device mappings ask for; lock held.
func (r *region) volumes(i *Instance, bdms []blockDevice, specs []tagSpec) {
	devices := map[string]blockDevice{i.RootDeviceName: {DeviceName: i.RootDeviceName}}
	for _, b := range bdms {
		if b.Ebs != nil && b.NoDevice == "" {
			devices[b.DeviceName] = b
		}
	}
	for _, name := range core.SortedKeys(devices) {
		size, typ, dot, enc := 8, "gp3", true, false
		var iops, throughput int
		var kms, snapshot string
		if e := devices[name].Ebs; e != nil {
			size, typ, iops, throughput, kms, snapshot = cmp.Or(e.VolumeSize, size), cmp.Or(e.VolumeType, typ), e.Iops, e.Throughput, e.KmsKeyId, e.SnapshotId
			if e.DeleteOnTermination != nil {
				dot = *e.DeleteOnTermination
			}
			if e.Encrypted != nil {
				enc = *e.Encrypted
			}
		}
		v := r.newVolume(i.Placement.AvailabilityZone, size, typ, iops, throughput, enc, kms, snapshot)
		r.tag(v.VolumeId, specs)
		r.attach(i, v, name, dot)
	}
}

// attach records a volume on an instance; lock held.
func (r *region) attach(i *Instance, v *Volume, device string, deleteOnTermination bool) VolumeAttachment {
	now := time.Now().UTC().Format(core.ISO)
	a := VolumeAttachment{v.VolumeId, i.InstanceId, device, "attached", now, deleteOnTermination}
	v.State, v.Attachments = "in-use", []VolumeAttachment{a}
	i.BlockDeviceMappings = append(i.BlockDeviceMappings, BlockDevice{device, EbsDevice{v.VolumeId, "attached", now, deleteOnTermination}})
	return a
}

func (s *Service) createVolume(c *core.Ctx, in *struct {
	AvailabilityZone, VolumeType, KmsKeyId, SnapshotId string
	Size, Iops, Throughput                             int
	Encrypted, MultiAttachEnabled                      bool
	TagSpecifications                                  []tagSpec `query:"TagSpecification"`
}) (any, error) {
	switch {
	case !slices.Contains(zones(c.Region), in.AvailabilityZone):
		return nil, core.Errorf(400, "InvalidZone.NotFound", "The zone '%s' does not exist.", in.AvailabilityZone)
	case in.Size == 0 && in.SnapshotId == "":
		return nil, core.Errorf(400, "MissingParameter", "The request must contain the parameter size or snapshotId")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	v := r.newVolume(in.AvailabilityZone, cmp.Or(in.Size, 8), in.VolumeType, in.Iops, in.Throughput, in.Encrypted, in.KmsKeyId, in.SnapshotId)
	v.MultiAttachEnabled = in.MultiAttachEnabled
	r.tag(v.VolumeId, typed(in.TagSpecifications, "volume"))
	return &struct {
		Header
		Volume
	}{Volume: withTags(r, v, (*Volume).view)}, nil
}

func (s *Service) describeVolumes(c *core.Ctx, in *struct {
	VolumeIds []string `query:"VolumeId"`
	Filters   []filter `query:"Filter"`
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	list, err := describe(r, r.Volumes, in.VolumeIds, in.Filters, "InvalidVolume.NotFound", "volume", (*Volume).view)
	return &struct {
		Header
		Volumes []Volume `xml:"volumeSet>item"`
	}{Volumes: list}, err
}

// volume finds a volume; lock held.
func (r *region) volume(id string) (*Volume, error) {
	if v := r.Volumes[id]; v != nil {
		return v, nil
	}
	return nil, core.Errorf(400, "InvalidVolume.NotFound", "The volume '%s' does not exist.", id)
}

func (s *Service) deleteVolume(c *core.Ctx, in *struct{ VolumeId string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	v, err := r.volume(in.VolumeId)
	if err != nil {
		return nil, err
	}
	if len(v.Attachments) > 0 {
		return nil, core.Errorf(400, "VolumeInUse", "Volume %s is currently attached to %s", v.VolumeId, v.Attachments[0].InstanceId)
	}
	delete(r.Volumes, v.VolumeId)
	delete(r.Tags, v.VolumeId)
	return ok(), nil
}

func (s *Service) attachVolume(c *core.Ctx, in *struct{ Device, InstanceId, VolumeId string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	v, err := r.volume(in.VolumeId)
	if err != nil {
		return nil, err
	}
	i := r.Instances[in.InstanceId]
	switch {
	case i == nil || i.State.Name == "terminated":
		return nil, core.Errorf(400, "InvalidInstanceID.NotFound", "The instance ID '%s' does not exist", in.InstanceId)
	case len(v.Attachments) > 0:
		return nil, core.Errorf(400, "VolumeInUse", "%s is already attached to an instance", v.VolumeId)
	case v.AvailabilityZone != i.Placement.AvailabilityZone:
		return nil, core.Errorf(400, "InvalidVolume.ZoneMismatch", "The volume '%s' is not in the same availability zone as instance '%s'", v.VolumeId, i.InstanceId)
	}
	a := r.attach(i, v, in.Device, false)
	a.State = "attaching"
	return &struct {
		Header
		VolumeAttachment
	}{VolumeAttachment: a}, nil
}

func (s *Service) detachVolume(c *core.Ctx, in *struct {
	VolumeId, InstanceId, Device string
	Force                        bool
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	v, err := r.volume(in.VolumeId)
	if err != nil {
		return nil, err
	}
	if len(v.Attachments) == 0 {
		return nil, core.Errorf(400, "IncorrectState", "Volume '%s' is in the 'available' state.", v.VolumeId)
	}
	a := v.Attachments[0]
	if i := r.Instances[a.InstanceId]; i != nil {
		i.BlockDeviceMappings = slices.DeleteFunc(slices.Clone(i.BlockDeviceMappings), func(b BlockDevice) bool { return b.Ebs.VolumeId == v.VolumeId })
	}
	v.State, v.Attachments, a.State = "available", nil, "detaching"
	return &struct {
		Header
		VolumeAttachment
	}{VolumeAttachment: a}, nil
}
