// Package ec2 emulates Amazon EC2 over the EC2 Query protocol: regions and
// zones, a default VPC per region (subnets, security group, internet gateway,
// main route table and network ACL), VPCs, subnets, security groups and rules,
// tags, key pairs, an AMI catalog, instances (containers when an engine runs,
// see instances.go) with their metadata service (imds.go), launch templates,
// EBS volumes and instance types.
package ec2

import (
	"bytes"
	"cmp"
	"crypto"
	"crypto/ed25519"
	"crypto/md5"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"encoding/xml"
	"fmt"
	"maps"
	"math"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/core"
	"golang.org/x/crypto/ssh"
)

const xmlns = "http://ec2.amazonaws.com/doc/2016-11-15/"

// Service is the EC2 emulator.
type Service struct {
	hub    *core.Hub
	ops    map[string]core.Handler
	mu     sync.Mutex
	st     state
	creds  map[string]*creds       // instance id → role credentials handed out by IMDS; lock held
	secret []byte                  // signs IMDSv2 tokens
	imdsLn map[string]*http.Server // instance id → its metadata listener, which the 169.254.169.254 forwarder calls; lock held

	qmu   sync.Mutex
	queue map[string]chan struct{} // last queued engine call per instance
}

type state struct {
	Regions map[string]*region `json:"regions"` // account/region
}

// region holds one account's resources in one region.
type region struct {
	Account   string                       `json:"account"`
	Name      string                       `json:"name"`
	Vpcs      map[string]*Vpc              `json:"vpcs"`
	Subnets   map[string]*Subnet           `json:"subnets"`
	Groups    map[string]*Group            `json:"groups"`
	Gateways  map[string]*Gateway          `json:"gateways"`
	Tables    map[string]*RouteTable       `json:"tables"`
	Acls      map[string]*Acl              `json:"acls"`
	Keys      map[string]*KeyPair          `json:"keys"` // by key name
	Tags      map[string]map[string]string `json:"tags"` // resource id → tags
	Instances map[string]*Instance         `json:"instances"`
	Volumes   map[string]*Volume           `json:"volumes"`
	Templates map[string]*LaunchTemplate   `json:"templates"` // by id
	Tokens    map[string]runToken          `json:"tokens"`    // RunInstances ClientToken → what it made
	Net       netState                     `json:"net"`       // see routing.go
	X         *extras                      `json:"extras"`    // see extras.go; use x()
}

// fill makes the maps a state saved before they existed lacks.
func (r *region) fill() {
	if r.Instances == nil {
		r.Instances, r.Volumes, r.Templates = map[string]*Instance{}, map[string]*Volume{}, map[string]*LaunchTemplate{}
	}
	r.fillNet()
}

// Tag is an EC2 tag.
type Tag struct {
	Key   string `xml:"key"`
	Value string `xml:"value"`
}

type status struct {
	State string `xml:"state"`
}

// Vpc is a VPC in its wire shape.
type Vpc struct {
	VpcId           string      `xml:"vpcId"`
	OwnerId         string      `xml:"ownerId"`
	State           string      `xml:"state"`
	CidrBlock       string      `xml:"cidrBlock"`
	CidrAssoc       []cidrAssoc `xml:"cidrBlockAssociationSet>item"`
	DhcpOptionsId   string      `xml:"dhcpOptionsId"`
	InstanceTenancy string      `xml:"instanceTenancy"`
	IsDefault       bool        `xml:"isDefault"`
	TagSet          []Tag       `xml:"tagSet>item" json:"-"`
	DnsSupport      bool        `xml:"-"`
	DnsHostnames    bool        `xml:"-"`
	UsageMetrics    bool        `xml:"-"`
}

type cidrAssoc struct {
	AssociationId string `xml:"associationId"`
	CidrBlock     string `xml:"cidrBlock"`
	State         status `xml:"cidrBlockState"`
}

// Subnet is a subnet in its wire shape.
type Subnet struct {
	SubnetId                    string `xml:"subnetId"`
	SubnetArn                   string `xml:"subnetArn"`
	State                       string `xml:"state"`
	VpcId                       string `xml:"vpcId"`
	OwnerId                     string `xml:"ownerId"`
	CidrBlock                   string `xml:"cidrBlock"`
	AvailableIpAddressCount     int    `xml:"availableIpAddressCount"`
	AvailabilityZone            string `xml:"availabilityZone"`
	AvailabilityZoneId          string `xml:"availabilityZoneId"`
	DefaultForAz                bool   `xml:"defaultForAz"`
	MapPublicIpOnLaunch         bool   `xml:"mapPublicIpOnLaunch"`
	AssignIpv6AddressOnCreation bool   `xml:"assignIpv6AddressOnCreation"`
	PrivateDns                  struct {
		HostnameType string `xml:"hostnameType"`
	} `xml:"privateDnsNameOptionsOnLaunch"`
	TagSet []Tag `xml:"tagSet>item" json:"-"`
}

// Group is a security group; rules are stored one source per rule and
// grouped by protocol and ports on output, as EC2 lists them.
type Group struct {
	OwnerId             string `xml:"ownerId"`
	GroupId             string `xml:"groupId"`
	GroupName           string `xml:"groupName"`
	Description         string `xml:"groupDescription"`
	VpcId               string `xml:"vpcId"`
	SecurityGroupArn    string `xml:"securityGroupArn"`
	IpPermissions       []perm `xml:"ipPermissions>item"`
	IpPermissionsEgress []perm `xml:"ipPermissionsEgress>item"`
	TagSet              []Tag  `xml:"tagSet>item" json:"-"`
}

// perm is an IpPermission, for input (Query names) and output (XML names).
type perm struct {
	IpProtocol    string      `xml:"ipProtocol"`
	FromPort      *int        `xml:"fromPort,omitempty"`
	ToPort        *int        `xml:"toPort,omitempty"`
	Groups        []pair      `xml:"groups>item"`
	IpRanges      []ipRange   `xml:"ipRanges>item"`
	Ipv6Ranges    []ipv6Range `xml:"ipv6Ranges>item"`
	PrefixListIds []prefixID  `xml:"prefixListIds>item"`
}

type pair struct {
	UserId      string `xml:"userId,omitempty"`
	GroupId     string `xml:"groupId,omitempty"`
	GroupName   string `xml:"groupName,omitempty"`
	Description string `xml:"description,omitempty"`
}

type ipRange struct {
	CidrIp      string `xml:"cidrIp"`
	Description string `xml:"description,omitempty"`
}

type ipv6Range struct {
	CidrIpv6    string `xml:"cidrIpv6"`
	Description string `xml:"description,omitempty"`
}

type prefixID struct {
	PrefixListId string `xml:"prefixListId"`
	Description  string `xml:"description,omitempty"`
}

// Gateway is an internet gateway.
type Gateway struct {
	InternetGatewayId string       `xml:"internetGatewayId"`
	OwnerId           string       `xml:"ownerId"`
	Attachments       []attachment `xml:"attachmentSet>item"`
	TagSet            []Tag        `xml:"tagSet>item" json:"-"`
}

type attachment struct {
	VpcId string `xml:"vpcId"`
	State string `xml:"state"`
}

// RouteTable is a route table.
type RouteTable struct {
	RouteTableId string    `xml:"routeTableId"`
	VpcId        string    `xml:"vpcId"`
	OwnerId      string    `xml:"ownerId"`
	Routes       []route   `xml:"routeSet>item"`
	Associations []rtAssoc `xml:"associationSet>item"`
	TagSet       []Tag     `xml:"tagSet>item" json:"-"`
}

type route struct {
	DestinationCidrBlock string `xml:"destinationCidrBlock,omitempty"`
	GatewayId            string `xml:"gatewayId,omitempty"`
	State                string `xml:"state"`
	Origin               string `xml:"origin"`

	DestinationIpv6CidrBlock    string `xml:"destinationIpv6CidrBlock,omitempty"`
	DestinationPrefixListId     string `xml:"destinationPrefixListId,omitempty"`
	NatGatewayId                string `xml:"natGatewayId,omitempty"`
	EgressOnlyInternetGatewayId string `xml:"egressOnlyInternetGatewayId,omitempty"`
	InstanceId                  string `xml:"instanceId,omitempty"`
	NetworkInterfaceId          string `xml:"networkInterfaceId,omitempty"`
	VpcPeeringConnectionId      string `xml:"vpcPeeringConnectionId,omitempty"`
	TransitGatewayId            string `xml:"transitGatewayId,omitempty"`
}

type rtAssoc struct {
	RouteTableAssociationId string `xml:"routeTableAssociationId"`
	RouteTableId            string `xml:"routeTableId"`
	Main                    bool   `xml:"main"`
	AssociationState        status `xml:"associationState"`
	SubnetId                string `xml:"subnetId,omitempty"`
	GatewayId               string `xml:"gatewayId,omitempty"`
}

// Acl is a network ACL.
type Acl struct {
	NetworkAclId string     `xml:"networkAclId"`
	VpcId        string     `xml:"vpcId"`
	IsDefault    bool       `xml:"default"`
	OwnerId      string     `xml:"ownerId"`
	Entries      []aclEntry `xml:"entrySet>item"`
	Associations []aclAssoc `xml:"associationSet>item"`
	TagSet       []Tag      `xml:"tagSet>item" json:"-"`
}

type aclEntry struct {
	RuleNumber int    `xml:"ruleNumber"`
	Protocol   string `xml:"protocol"`
	RuleAction string `xml:"ruleAction"`
	Egress     bool   `xml:"egress"`
	CidrBlock  string `xml:"cidrBlock,omitempty"`

	Ipv6CidrBlock string     `xml:"ipv6CidrBlock,omitempty"`
	IcmpTypeCode  *icmpCode  `xml:"icmpTypeCode"`
	PortRange     *portRange `xml:"portRange"`
}

// KeyPair is a key pair; PublicKey is what an instance with this key lets in over ssh.
type KeyPair struct {
	KeyPairId      string `xml:"keyPairId"`
	KeyName        string `xml:"keyName"`
	KeyFingerprint string `xml:"keyFingerprint"`
	KeyType        string `xml:"keyType"`
	CreateTime     string `xml:"createTime"`
	PublicKey      string `xml:"publicKey,omitempty"`
	TagSet         []Tag  `xml:"tagSet>item" json:"-"`
}

// Image is an AMI.
type Image struct {
	ImageId             string        `xml:"imageId"`
	ImageLocation       string        `xml:"imageLocation"`
	ImageState          string        `xml:"imageState"`
	ImageOwnerId        string        `xml:"imageOwnerId"`
	CreationDate        string        `xml:"creationDate"`
	IsPublic            bool          `xml:"isPublic"`
	Architecture        string        `xml:"architecture"`
	ImageType           string        `xml:"imageType"`
	ImageOwnerAlias     string        `xml:"imageOwnerAlias,omitempty"`
	Name                string        `xml:"name"`
	Description         string        `xml:"description"`
	RootDeviceType      string        `xml:"rootDeviceType"`
	RootDeviceName      string        `xml:"rootDeviceName"`
	VirtualizationType  string        `xml:"virtualizationType"`
	Hypervisor          string        `xml:"hypervisor"`
	EnaSupport          bool          `xml:"enaSupport"`
	PlatformDetails     string        `xml:"platformDetails"`
	UsageOperation      string        `xml:"usageOperation"`
	TagSet              []Tag         `xml:"tagSet>item"`
	BlockDeviceMappings []ImageDevice `xml:"blockDeviceMapping>item"`
	Docker              string        `xml:"-"` // the container image its instances run
	Keyword             string        `xml:"-"` // finds it in resolve:ssm: image ids
}

// ami is a catalog entry.
func ami(id, owner, alias, name, desc, root, docker, keyword string) *Image {
	img := &Image{ImageId: id, ImageLocation: cmp.Or(alias, owner) + "/" + name, ImageState: "available", ImageOwnerId: owner,
		CreationDate: "2025-01-01T00:00:00.000Z", IsPublic: true, Architecture: "x86_64", ImageType: "machine", ImageOwnerAlias: alias,
		Name: name, Description: desc, RootDeviceType: "ebs", RootDeviceName: root, VirtualizationType: "hvm", Hypervisor: "xen",
		EnaSupport: true, PlatformDetails: "Linux/UNIX", UsageOperation: "RunInstances", Docker: docker, Keyword: keyword}
	dev := ImageDevice{DeviceName: root} // every AMI has a root volume
	dev.Ebs.DeleteOnTermination, dev.Ebs.VolumeSize, dev.Ebs.VolumeType = true, 8, "gp3"
	img.BlockDeviceMappings = []ImageDevice{dev}
	return img
}

// images are the AMIs every region offers; "ec2.<ami id>" under docker:
// images: in pogi.yaml changes what one runs. Other AMI ids run Amazon Linux 2023.
var images = map[string]*Image{}

const defaultAMI = "ami-0a1b2c3d4e5f60001"

func init() {
	for _, i := range []*Image{
		ami(defaultAMI, "137112412989", "amazon", "al2023-ami-2023.6.20250101.0-kernel-6.1-x86_64",
			"Amazon Linux 2023 AMI 2023.6.20250101.0 x86_64 HVM kernel-6.1", "/dev/xvda", "amazonlinux:2023", "al2023"),
		ami("ami-0a1b2c3d4e5f60002", "099720109477", "", "ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-20250101",
			"Canonical, Ubuntu, 24.04, amd64 noble image", "/dev/sda1", "ubuntu:24.04", "24.04"),
		ami("ami-0a1b2c3d4e5f60003", "099720109477", "", "ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-20250101",
			"Canonical, Ubuntu, 22.04, amd64 jammy image", "/dev/sda1", "ubuntu:22.04", "22.04"),
		ami("ami-0a1b2c3d4e5f60004", "137112412989", "amazon", "amzn2-ami-kernel-5.10-hvm-2.0.20250101.0-x86_64-gp2",
			"Amazon Linux 2 Kernel 5.10 AMI 2.0.20250101.0 x86_64 HVM gp2", "/dev/xvda", "amazonlinux:2", "amzn2"),
		ami("ami-0a1b2c3d4e5f60005", "136693071363", "", "debian-12-amd64-20250101-1980", "Debian 12 (20250101-1980)",
			"/dev/xvda", "debian:12", "debian"),
		ami("ami-0a1b2c3d4e5f60006", "538276064493", "", "alpine-3.21.0-x86_64-bios-cloudinit-r0", "Alpine Linux 3.21.0 x86_64",
			"/dev/xvda", "alpine:3", "alpine"),
	} {
		images[i.ImageId] = i
	}
}

var regions = []string{"af-south-1", "ap-east-1", "ap-northeast-1", "ap-northeast-2", "ap-northeast-3", "ap-south-1", "ap-southeast-1",
	"ap-southeast-2", "ca-central-1", "eu-central-1", "eu-north-1", "eu-south-1", "eu-west-1", "eu-west-2", "eu-west-3", "me-south-1",
	"sa-east-1", "us-east-1", "us-east-2", "us-west-1", "us-west-2"}

// New creates the EC2 service.
func New(hub *core.Hub) *Service {
	s := &Service{hub: hub, creds: map[string]*creds{}, secret: []byte(core.NewID()), queue: map[string]chan struct{}{}, imdsLn: map[string]*http.Server{}}
	s.Reset()
	s.ops = map[string]core.Handler{
		"DescribeRegions":               core.H(s.describeRegions),
		"DescribeAvailabilityZones":     core.H(s.describeZones),
		"DescribeAccountAttributes":     core.H(s.describeAccountAttributes),
		"DescribeVpcs":                  core.H(s.describeVpcs),
		"CreateVpc":                     core.H(s.createVpc),
		"DeleteVpc":                     core.H(s.deleteVpc),
		"DescribeVpcAttribute":          core.H(s.describeVpcAttribute),
		"ModifyVpcAttribute":            core.H(s.modifyVpcAttribute),
		"DescribeSubnets":               core.H(s.describeSubnets),
		"CreateSubnet":                  core.H(s.createSubnet),
		"ModifySubnetAttribute":         core.H(s.modifySubnetAttribute),
		"CreateDhcpOptions":             core.H(s.createDhcpOptions),
		"DescribeDhcpOptions":           core.H(s.describeDhcpOptions),
		"DeleteDhcpOptions":             core.H(s.deleteDhcpOptions),
		"AssociateDhcpOptions":          core.H(s.associateDhcpOptions),
		"ModifyVolume":                  core.H(s.modifyVolume),
		"DeleteSubnet":                  core.H(s.deleteSubnet),
		"DescribeSecurityGroups":        core.H(s.describeGroups),
		"CreateSecurityGroup":           core.H(s.createGroup),
		"DeleteSecurityGroup":           core.H(s.deleteGroup),
		"AuthorizeSecurityGroupIngress": s.rules(false, true),
		"AuthorizeSecurityGroupEgress":  s.rules(true, true),
		"RevokeSecurityGroupIngress":    s.rules(false, false),
		"RevokeSecurityGroupEgress":     s.rules(true, false),
		"DescribeInternetGateways":      core.H(s.describeGateways),
		"DescribeRouteTables":           core.H(s.describeTables),
		"DescribeNetworkAcls":           core.H(s.describeAcls),
		"DescribeVpnGateways":           core.H(empty),
		"CreateTags":                    core.H(s.createTags),
		"DeleteTags":                    core.H(s.deleteTags),
		"DescribeTags":                  core.H(s.describeTags),
		"DescribeImages":                core.H(s.describeImages),
		"GetConsoleOutput":              core.H(s.consoleOutput),
		"DescribeKeyPairs":              core.H(s.describeKeyPairs),
		"CreateKeyPair":                 core.H(s.createKeyPair),
		"ImportKeyPair":                 core.H(s.importKeyPair),
		"DeleteKeyPair":                 core.H(s.deleteKeyPair),
	}
	maps.Copy(s.ops, s.instanceOps())
	maps.Copy(s.ops, s.netOps())
	maps.Copy(s.ops, s.extraOps())
	hub.Mount("/_aws/imds/", http.HandlerFunc(s.imds))
	return s
}

func (s *Service) Info() core.Info {
	// AmazonEC2, the smithy service name, takes pogi's in-process JSON calls (hub.Call).
	return core.Info{Name: "ec2", Cloud: core.AWS, Title: "EC2", Signing: []string{"ec2"}, Targets: []string{"AmazonEC2"}, Protocol: "query"}
}

// Header is embedded first in every output, so responses read
// <OpResponse xmlns=…><requestId>…</requestId>…; EC2 does not wrap results in
// <OpResult> like plain Query does.
type Header struct {
	RequestID string `xml:"requestId"`
}

func (h *Header) setRequestID(id string) { h.RequestID = id }

func ok() any {
	return &struct {
		Header
		Return bool `xml:"return"`
	}{Return: true}
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c := core.C(r)
	body := core.Body(r)
	form, _ := url.ParseQuery(string(body))
	for k, v := range r.URL.Query() {
		if form.Get(k) == "" {
			form[k] = v
		}
	}
	c.Op = form.Get("Action")
	decode, fail := func(v any) error { return core.DecodeQuery(form, v) }, writeErr
	target := r.Header.Get("X-Amz-Target")
	if target != "" { // JSON, from pogi's in-process calls
		_, c.Op, _ = strings.Cut(target, ".")
		form = url.Values{}
		decode = func(v any) error {
			if len(body) == 0 {
				return nil
			}
			return json.Unmarshal(body, v)
		}
		fail = func(w http.ResponseWriter, c *core.Ctx, err error) {
			core.WriteJSONError(w, c, "1.0", core.AsError(err))
		}
	}
	h := s.ops[c.Op]
	if h == nil {
		fail(w, c, core.Errorf(400, "InvalidAction", "The action %s is not valid for this web service.", c.Op))
		return
	}
	if err := c.Intercept(); err != nil {
		fail(w, c, err)
		return
	}
	if err := checkNumbers(c.Op, form); err != nil {
		fail(w, c, err)
		return
	}
	if form.Get("DryRun") == "true" {
		fail(w, c, core.Errorf(412, "DryRunOperation", "Request would have succeeded, but DryRun flag is set."))
		return
	}
	out, err := h(c, decode)
	if err != nil {
		fail(w, c, err)
		return
	}
	out.(interface{ setRequestID(string) }).setRequestID(c.RequestID)
	if target != "" {
		core.WriteJSON(w, c, "1.0", 200, out)
		return
	}
	var b bytes.Buffer
	b.WriteString(xml.Header)
	if err := xml.NewEncoder(&b).EncodeElement(out, xml.StartElement{Name: xml.Name{Space: xmlns, Local: c.Op + "Response"}}); err != nil {
		writeErr(w, c, err)
		return
	}
	w.Header().Set("Content-Type", "text/xml;charset=UTF-8")
	w.Header().Set("x-amzn-RequestId", c.RequestID)
	w.Write(b.Bytes())
}

// maxResults is the range of MaxResults for the calls that do not take 1 or more, as EC2's documentation says.
var maxResults = map[string][2]int{
	"DescribeInstances": {5, 1000}, "DescribeSecurityGroups": {5, 1000}, "DescribeVpcs": {5, 1000},
	"DescribeSubnets": {5, 1000}, "DescribeSnapshots": {5, 1000}, "DescribeVolumes": {5, 500},
}

// volumeSize is the range of Size in GiB for each volume type.
var volumeSize = map[string][2]int{
	"gp2": {1, 16384}, "gp3": {1, 16384}, "io1": {4, 16384}, "io2": {4, 65536},
	"st1": {125, 16384}, "sc1": {125, 16384}, "standard": {1, 1024},
}

// checkNumbers refuses a MaxResults or a volume Size that is not a whole number in range, with EC2's
// InvalidParameterValue. It runs for every request, so each Describe call and each volume call gets it.
func checkNumbers(op string, form url.Values) error {
	if v := form.Get("MaxResults"); v != "" {
		lo, hi := 1, math.MaxInt32
		if r, ok := maxResults[op]; ok {
			lo, hi = r[0], r[1]
		}
		if n, err := strconv.Atoi(v); err != nil || n < lo || n > hi {
			return core.Errorf(400, "InvalidParameterValue", "Value ( %s ) for parameter maxResults is invalid. Expected: 'maxResults' must be an integer between %d and %d.", v, lo, hi)
		}
	}
	if v := form.Get("Size"); v != "" && (op == "CreateVolume" || op == "ModifyVolume") {
		typ := form.Get("VolumeType")
		if typ == "" && op == "CreateVolume" {
			typ = "gp2" // the default type
		}
		lo, hi := 1, 65536 // a ModifyVolume without VolumeType keeps the volume's type: only the widest range is sure
		if r, ok := volumeSize[typ]; ok {
			lo, hi = r[0], r[1]
		}
		n, err := strconv.Atoi(v)
		switch {
		case err != nil:
			return core.Errorf(400, "InvalidParameterValue", "Value ( %s ) for parameter size is invalid. Expected: 'size' must be an integer.", v)
		case n < lo:
			return core.Errorf(400, "InvalidParameterValue", "Volume of %dGiB is too small; minimum is %dGiB.", n, lo)
		case n > hi:
			return core.Errorf(400, "InvalidParameterValue", "Volume of %dGiB is too large; maximum is %dGiB.", n, hi)
		}
	}
	return nil
}

// writeErr writes EC2's error shape: <Response><Errors><Error>…</Errors><RequestID/>.
func writeErr(w http.ResponseWriter, c *core.Ctx, err error) {
	e := core.AsError(err)
	c.ErrCode = e.Code
	b, _ := xml.Marshal(struct {
		XMLName   xml.Name `xml:"Response"`
		Code      string   `xml:"Errors>Error>Code"`
		Message   string   `xml:"Errors>Error>Message"`
		RequestID string   `xml:"RequestID"`
	}{Code: e.Code, Message: e.Msg, RequestID: c.RequestID})
	w.Header().Set("Content-Type", "text/xml;charset=UTF-8")
	w.Header().Set("x-amzn-RequestId", c.RequestID)
	w.WriteHeader(e.Status)
	w.Write(append([]byte(xml.Header), b...))
}

func (s *Service) Reset() { s.Load([]byte("{}")) }

func (s *Service) Save() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(s.st)
}

// Load replaces the state: instances that ran in containers start again
// (persisted state keeps them), and containers the old state had are removed.
func (s *Service) Load(b []byte) error {
	st := state{Regions: map[string]*region{}}
	if err := json.Unmarshal(b, &st); err != nil {
		return err
	}
	keep := map[string]bool{}
	for _, r := range st.Regions {
		r.fill()
		for id, i := range r.Instances {
			keep[id] = i.Container != "" && (i.State.Name == "pending" || i.State.Name == "running")
		}
	}
	docker := slices.Contains(slices.Collect(maps.Values(keep)), true) && s.hub.Docker().Available()
	s.mu.Lock()
	old := s.st
	s.st = st
	for _, r := range st.Regions {
		for id, i := range r.Instances {
			if keep[id] && docker {
				s.boot(r, i, "start")
			}
		}
	}
	s.mu.Unlock()
	for _, r := range old.Regions {
		for id, i := range r.Instances {
			if i.Container != "" && !keep[id] {
				s.unpublish(i)
				s.remove(id, i.Container)
			}
		}
	}
	return nil
}

func invalid(format string, a ...any) error {
	return core.Errorf(400, "InvalidParameterValue", format, a...)
}

func newID(prefix string) string { return prefix + "-" + core.RandHex(9)[:17] }

func zones(region string) []string { return []string{region + "a", region + "b", region + "c"} }

var abbrev = strings.NewReplacer("northeast", "ne", "northwest", "nw", "southeast", "se", "southwest", "sw", "central", "c",
	"north", "n", "south", "s", "east", "e", "west", "w", "-", "")

// zoneID maps us-east-1a to use1-az1.
func zoneID(az string) string {
	return abbrev.Replace(az[:len(az)-1]) + "-az" + strconv.Itoa(int(az[len(az)-1]-'a'+1))
}

// VPCOfSubnet is the VPC of a subnet, or "" when there is none; Route 53 and the
// services that run containers in a VPC use it to find the private zones to resolve.
func (s *Service) VPCOfSubnet(account, region, subnet string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r := s.st.Regions[account+"/"+region]; r != nil && r.Subnets[subnet] != nil {
		return r.Subnets[subnet].VpcId
	}
	return ""
}

// reg returns the caller's region, creating its default VPC on first use; lock held.
func (s *Service) reg(c *core.Ctx) *region {
	k := c.Account + "/" + c.Region
	if r := s.st.Regions[k]; r != nil {
		return r
	}
	r := &region{Account: c.Account, Name: c.Region, Vpcs: map[string]*Vpc{}, Subnets: map[string]*Subnet{}, Groups: map[string]*Group{},
		Gateways: map[string]*Gateway{}, Tables: map[string]*RouteTable{}, Acls: map[string]*Acl{}, Keys: map[string]*KeyPair{},
		Tags: map[string]map[string]string{}}
	r.fill()
	s.st.Regions[k] = r
	v := r.newVpc("172.31.0.0/16", true)
	gw := &Gateway{InternetGatewayId: newID("igw"), OwnerId: r.Account, Attachments: []attachment{{v.VpcId, "available"}}}
	r.Gateways[gw.InternetGatewayId] = gw
	for _, t := range r.Tables {
		t.Routes = append(t.Routes, route{DestinationCidrBlock: "0.0.0.0/0", GatewayId: gw.InternetGatewayId, State: "active", Origin: "CreateRoute"})
	}
	for i, az := range zones(c.Region) {
		sn := r.newSubnet(v, fmt.Sprintf("172.31.%d.0/20", 16*i), az)
		sn.DefaultForAz, sn.MapPublicIpOnLaunch = true, true
	}
	return r
}

// newVpc creates a VPC with its default security group, main route table and network ACL.
func (r *region) newVpc(cidr string, isDefault bool) *Vpc {
	v := &Vpc{VpcId: newID("vpc"), OwnerId: r.Account, State: "available", CidrBlock: cidr, DhcpOptionsId: newID("dopt"),
		InstanceTenancy: "default", IsDefault: isDefault, DnsSupport: true, DnsHostnames: isDefault}
	v.CidrAssoc = []cidrAssoc{{newID("vpc-cidr-assoc"), cidr, status{"associated"}}}
	r.Vpcs[v.VpcId] = v
	sg := r.newGroup(v.VpcId, "default", "default VPC security group")
	sg.IpPermissions = []perm{{IpProtocol: "-1", Groups: []pair{{UserId: r.Account, GroupId: sg.GroupId}}}}
	t := &RouteTable{RouteTableId: newID("rtb"), VpcId: v.VpcId, OwnerId: r.Account, Routes: []route{localRoute(cidr)}}
	t.Associations = []rtAssoc{{RouteTableAssociationId: newID("rtbassoc"), RouteTableId: t.RouteTableId, Main: true, AssociationState: status{"associated"}}}
	r.Tables[t.RouteTableId] = t
	acl := &Acl{NetworkAclId: newID("acl"), VpcId: v.VpcId, IsDefault: true, OwnerId: r.Account, Entries: defaultEntries(true)}
	r.Acls[acl.NetworkAclId] = acl
	return v
}

func (r *region) newGroup(vpcID, name, desc string) *Group {
	g := &Group{OwnerId: r.Account, GroupId: newID("sg"), GroupName: name, Description: desc, VpcId: vpcID,
		IpPermissionsEgress: []perm{{IpProtocol: "-1", IpRanges: []ipRange{{CidrIp: "0.0.0.0/0"}}}}}
	g.SecurityGroupArn = core.ARN("ec2", r.Name, r.Account, "security-group/"+g.GroupId)
	r.Groups[g.GroupId] = g
	return g
}

func (r *region) newSubnet(v *Vpc, cidr, az string) *Subnet {
	_, n, _ := net.ParseCIDR(cidr)
	ones, bits := n.Mask.Size()
	sn := &Subnet{SubnetId: newID("subnet"), State: "available", VpcId: v.VpcId, OwnerId: r.Account, CidrBlock: n.String(),
		AvailableIpAddressCount: 1<<(bits-ones) - 5, AvailabilityZone: az, AvailabilityZoneId: zoneID(az)}
	sn.SubnetArn = core.ARN("ec2", r.Name, r.Account, "subnet/"+sn.SubnetId)
	sn.PrivateDns.HostnameType = "ip-name"
	r.Subnets[sn.SubnetId] = sn
	r.joinDefaultAcl(sn)
	return sn
}

func (r *region) defaultVpc() *Vpc {
	for _, k := range core.SortedKeys(r.Vpcs) {
		if r.Vpcs[k].IsDefault {
			return r.Vpcs[k]
		}
	}
	return nil
}

// ---- filters, tags ----

type filter struct {
	Name   string
	Values []string `query:"Value"`
}

type tagSpec struct {
	ResourceType string
	Tags         []Tag `query:"Tag"`
}

// match reports whether a resource passes every filter; attrs holds the
// values of each filter name, tags serve tag:<key>, tag-key and tag-value.
// ponytail: unknown filter names are ignored instead of rejected.
func match(fs []filter, attrs map[string][]string, tags map[string]string) bool {
	for _, f := range fs {
		var vals []string
		switch {
		case strings.HasPrefix(f.Name, "tag:"):
			if v, ok := tags[f.Name[4:]]; ok {
				vals = []string{v}
			}
		case f.Name == "tag-key":
			vals = slices.Collect(maps.Keys(tags))
		case f.Name == "tag-value":
			vals = slices.Collect(maps.Values(tags))
		default:
			v, known := attrs[f.Name]
			if !known {
				continue
			}
			vals = v
		}
		if !slices.ContainsFunc(vals, func(v string) bool {
			return slices.ContainsFunc(f.Values, func(p string) bool { return glob(p, v) })
		}) {
			return false
		}
	}
	return true
}

// checkFilters refuses a filter name the resource does not serve, as EC2
// does; attrs names the ones it serves (tag filters always work).
func checkFilters(fs []filter, attrs map[string][]string) error {
	for _, f := range fs {
		if _, ok := attrs[f.Name]; !ok && !strings.HasPrefix(f.Name, "tag:") && f.Name != "tag-key" && f.Name != "tag-value" {
			return invalid("The filter '%s' is invalid", f.Name)
		}
	}
	return nil
}

// glob matches EC2 filter wildcards: * and ?.
func glob(pattern, s string) bool {
	if !strings.ContainsAny(pattern, "*?") {
		return pattern == s
	}
	ok, _ := regexp.MatchString("^"+strings.NewReplacer(`\*`, ".*", `\?`, ".").Replace(regexp.QuoteMeta(pattern))+"$", s)
	return ok
}

func (r *region) tagSet(id string) []Tag {
	var out []Tag
	for _, k := range core.SortedKeys(r.Tags[id]) {
		out = append(out, Tag{k, r.Tags[id][k]})
	}
	return out
}

func (r *region) tag(id string, specs []tagSpec) {
	for _, sp := range specs {
		for _, t := range sp.Tags {
			if r.Tags[id] == nil {
				r.Tags[id] = map[string]string{}
			}
			r.Tags[id][t.Key] = t.Value
		}
	}
}

// view prepares a copy of a resource for output: its id, its tag set to fill
// and the values of its filters.
type view[T any] func(*T) (id string, tags *[]Tag, attrs map[string][]string)

// describe returns copies of the resources named by ids (all when none, EC2's
// NotFound for an unknown id) that pass the filters, with their tags.
func describe[T any](r *region, all map[string]*T, ids []string, fs []filter, code, noun string, v view[T]) ([]T, error) {
	keys := ids
	if len(keys) == 0 {
		keys = core.SortedKeys(all)
	}
	out := []T{}
	for _, k := range keys {
		p := all[k]
		if p == nil {
			return nil, core.Errorf(400, code, "The %s '%s' does not exist", noun, k)
		}
		cp := *p
		id, tags, attrs := v(&cp)
		if match(fs, attrs, r.Tags[id]) {
			*tags = r.tagSet(id)
			out = append(out, cp)
		}
	}
	return out, nil
}

// withTags returns a copy of one resource with its tags, for create outputs.
func withTags[T any](r *region, p *T, v view[T]) T {
	cp := *p
	id, tags, _ := v(&cp)
	*tags = r.tagSet(id)
	return cp
}

func (v *Vpc) view() (string, *[]Tag, map[string][]string) {
	def := strconv.FormatBool(v.IsDefault)
	return v.VpcId, &v.TagSet, map[string][]string{"vpc-id": {v.VpcId}, "cidr": {v.CidrBlock}, "cidr-block": {v.CidrBlock}, "cidr-block-association.cidr-block": v.cidrs(),
		"is-default": {def}, "isDefault": {def}, "state": {v.State}, "owner-id": {v.OwnerId}, "dhcp-options-id": {v.DhcpOptionsId}}
}

func (sn *Subnet) view() (string, *[]Tag, map[string][]string) {
	def := strconv.FormatBool(sn.DefaultForAz)
	return sn.SubnetId, &sn.TagSet, map[string][]string{"subnet-id": {sn.SubnetId}, "vpc-id": {sn.VpcId}, "cidr-block": {sn.CidrBlock},
		"cidrBlock": {sn.CidrBlock}, "cidr": {sn.CidrBlock}, "availability-zone": {sn.AvailabilityZone}, "availabilityZone": {sn.AvailabilityZone},
		"availability-zone-id": {sn.AvailabilityZoneId}, "default-for-az": {def}, "defaultForAz": {def}, "state": {sn.State},
		"owner-id": {sn.OwnerId}, "map-public-ip-on-launch": {strconv.FormatBool(sn.MapPublicIpOnLaunch)}}
}

func (g *Group) view() (string, *[]Tag, map[string][]string) {
	g.IpPermissions, g.IpPermissionsEgress = grouped(g.IpPermissions), grouped(g.IpPermissionsEgress)
	return g.GroupId, &g.TagSet, map[string][]string{"group-id": {g.GroupId}, "group-name": {g.GroupName}, "vpc-id": {g.VpcId},
		"description": {g.Description}, "owner-id": {g.OwnerId}}
}

func (g *Gateway) view() (string, *[]Tag, map[string][]string) {
	attrs := map[string][]string{"internet-gateway-id": {g.InternetGatewayId}, "owner-id": {g.OwnerId}}
	for _, a := range g.Attachments {
		attrs["attachment.vpc-id"] = append(attrs["attachment.vpc-id"], a.VpcId)
		attrs["attachment.state"] = append(attrs["attachment.state"], a.State)
	}
	return g.InternetGatewayId, &g.TagSet, attrs
}

func (k *KeyPair) view() (string, *[]Tag, map[string][]string) {
	return k.KeyPairId, &k.TagSet, map[string][]string{"key-name": {k.KeyName}, "key-pair-id": {k.KeyPairId},
		"fingerprint": {k.KeyFingerprint}, "key-type": {k.KeyType}}
}

func (i *Image) view() (string, *[]Tag, map[string][]string) {
	var dev, size, typ, del, enc []string // block-device-mapping.* filters
	for _, b := range i.BlockDeviceMappings {
		dev, size, typ = append(dev, b.DeviceName), append(size, strconv.Itoa(b.Ebs.VolumeSize)), append(typ, b.Ebs.VolumeType)
		del, enc = append(del, strconv.FormatBool(b.Ebs.DeleteOnTermination)), append(enc, strconv.FormatBool(b.Ebs.Encrypted))
	}
	return i.ImageId, &i.TagSet, map[string][]string{"block-device-mapping.device-name": dev, "block-device-mapping.volume-size": size,
		"block-device-mapping.volume-type": typ, "block-device-mapping.delete-on-termination": del, "block-device-mapping.encrypted": enc, "image-id": {i.ImageId}, "name": {i.Name}, "owner-id": {i.ImageOwnerId},
		"owner-alias": {i.ImageOwnerAlias}, "architecture": {i.Architecture}, "state": {i.ImageState}, "image-type": {i.ImageType},
		"root-device-type": {i.RootDeviceType}, "virtualization-type": {i.VirtualizationType}, "is-public": {strconv.FormatBool(i.IsPublic)},
		"platform-details": {i.PlatformDetails}, "description": {i.Description}}
}

// ---- regions, zones, account ----

type regionInfo struct {
	RegionName     string `xml:"regionName"`
	RegionEndpoint string `xml:"regionEndpoint"`
	OptInStatus    string `xml:"optInStatus"`
}

func (s *Service) describeRegions(c *core.Ctx, in *struct {
	RegionNames []string `query:"RegionName"`
	Filters     []filter `query:"Filter"`
}) (any, error) {
	out := &struct {
		Header
		Regions []regionInfo `xml:"regionInfo>item"`
	}{}
	for _, name := range regions {
		ri := regionInfo{name, "ec2." + name + ".amazonaws.com", "opt-in-not-required"}
		if (len(in.RegionNames) == 0 || slices.Contains(in.RegionNames, name)) &&
			match(in.Filters, map[string][]string{"region-name": {name}, "endpoint": {ri.RegionEndpoint}, "opt-in-status": {ri.OptInStatus}}, nil) {
			out.Regions = append(out.Regions, ri)
		}
	}
	return out, nil
}

type zoneInfo struct {
	ZoneName           string   `xml:"zoneName"`
	ZoneId             string   `xml:"zoneId"`
	ZoneState          string   `xml:"zoneState"`
	RegionName         string   `xml:"regionName"`
	ZoneType           string   `xml:"zoneType"`
	OptInStatus        string   `xml:"optInStatus"`
	GroupName          string   `xml:"groupName"`
	NetworkBorderGroup string   `xml:"networkBorderGroup"`
	Messages           []string `xml:"messageSet>item"`
}

func (s *Service) describeZones(c *core.Ctx, in *struct {
	ZoneNames []string `query:"ZoneName"`
	ZoneIds   []string `query:"ZoneId"`
	Filters   []filter `query:"Filter"`
}) (any, error) {
	out := &struct {
		Header
		Zones []zoneInfo `xml:"availabilityZoneInfo>item"`
	}{}
	names := zones(c.Region)
	for _, n := range in.ZoneNames {
		if !slices.Contains(names, n) {
			return nil, invalid("Invalid availability zone: [%s]", n)
		}
	}
	for _, id := range in.ZoneIds {
		if !slices.ContainsFunc(names, func(az string) bool { return zoneID(az) == id }) {
			return nil, invalid("Invalid zone ID: [%s]", id)
		}
	}
	for _, az := range names {
		z := zoneInfo{az, zoneID(az), "available", c.Region, "availability-zone", "opt-in-not-required", c.Region, c.Region, nil}
		attrs := map[string][]string{"zone-name": {az}, "zone-id": {z.ZoneId}, "state": {"available"}, "region-name": {c.Region},
			"zone-type": {z.ZoneType}, "group-name": {c.Region}, "opt-in-status": {z.OptInStatus}, "network-border-group": {c.Region}, "message": nil, "parent-zone-id": nil, "parent-zone-name": nil}
		if err := checkFilters(in.Filters, attrs); err != nil {
			return nil, err
		}
		if (len(in.ZoneNames) == 0 || slices.Contains(in.ZoneNames, az)) && (len(in.ZoneIds) == 0 || slices.Contains(in.ZoneIds, z.ZoneId)) &&
			match(in.Filters, attrs, nil) {
			out.Zones = append(out.Zones, z)
		}
	}
	return out, nil
}

type accountAttr struct {
	AttributeName string `xml:"attributeName"`
	Values        []struct {
		AttributeValue string `xml:"attributeValue"`
	} `xml:"attributeValueSet>item"`
}

func (s *Service) describeAccountAttributes(c *core.Ctx, in *struct {
	AttributeNames []string `query:"AttributeName"`
}) (any, error) {
	s.mu.Lock()
	defaultVpc := "none"
	if v := s.reg(c).defaultVpc(); v != nil {
		defaultVpc = v.VpcId
	}
	s.mu.Unlock()
	out := &struct {
		Header
		Attributes []accountAttr `xml:"accountAttributeSet>item"`
	}{}
	for _, kv := range [][2]string{{"supported-platforms", "VPC"}, {"default-vpc", defaultVpc}, {"max-instances", "20"},
		{"vpc-max-security-groups-per-interface", "5"}, {"max-elastic-ips", "5"}, {"vpc-max-elastic-ips", "5"}} {
		if len(in.AttributeNames) == 0 || slices.Contains(in.AttributeNames, kv[0]) {
			a := accountAttr{AttributeName: kv[0]}
			a.Values = append(a.Values, struct {
				AttributeValue string `xml:"attributeValue"`
			}{kv[1]})
			out.Attributes = append(out.Attributes, a)
		}
	}
	return out, nil
}

// ---- VPCs ----

func (s *Service) describeVpcs(c *core.Ctx, in *struct {
	VpcIds  []string `query:"VpcId"`
	Filters []filter `query:"Filter"`
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	_, _, served := (&Vpc{}).view()
	if err := checkFilters(in.Filters, served); err != nil {
		return nil, err
	}
	vpcs, err := describe(r, r.Vpcs, in.VpcIds, in.Filters, "InvalidVpcID.NotFound", "vpc ID", (*Vpc).view)
	return &struct {
		Header
		Vpcs []Vpc `xml:"vpcSet>item"`
	}{Vpcs: vpcs}, err
}

func (s *Service) createVpc(c *core.Ctx, in *struct {
	CidrBlock       string
	InstanceTenancy string
	TagSpecs        []tagSpec `query:"TagSpecification"`
}) (any, error) {
	_, n, err := net.ParseCIDR(in.CidrBlock)
	if err != nil || n.IP.To4() == nil {
		return nil, invalid("Value (%s) for parameter cidrBlock is invalid. This is not a valid CIDR block.", in.CidrBlock)
	}
	if ones, _ := n.Mask.Size(); ones < 16 || ones > 28 {
		return nil, core.Errorf(400, "InvalidVpc.Range", "The CIDR '%s' is invalid.", in.CidrBlock)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	v := r.newVpc(n.String(), false)
	v.InstanceTenancy = cmp.Or(in.InstanceTenancy, "default")
	r.tag(v.VpcId, in.TagSpecs)
	return &struct {
		Header
		Vpc Vpc `xml:"vpc"`
	}{Vpc: withTags(r, v, (*Vpc).view)}, nil
}

func (r *region) vpc(id string) (*Vpc, error) {
	if v := r.Vpcs[id]; v != nil {
		return v, nil
	}
	return nil, core.Errorf(400, "InvalidVpcID.NotFound", "The vpc ID '%s' does not exist", id)
}

func (s *Service) deleteVpc(c *core.Ctx, in *struct{ VpcId string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	v, err := r.vpc(in.VpcId)
	if err != nil {
		return nil, err
	}
	busy := slices.ContainsFunc(slices.Collect(maps.Values(r.Subnets)), func(sn *Subnet) bool { return sn.VpcId == v.VpcId }) ||
		slices.ContainsFunc(slices.Collect(maps.Values(r.Groups)), func(g *Group) bool { return g.VpcId == v.VpcId && g.GroupName != "default" }) ||
		slices.ContainsFunc(slices.Collect(maps.Values(r.Gateways)), func(g *Gateway) bool {
			return slices.ContainsFunc(g.Attachments, func(a attachment) bool { return a.VpcId == v.VpcId })
		}) || r.netBusy(v.VpcId)
	if busy {
		return nil, core.Errorf(400, "DependencyViolation", "The vpc '%s' has dependencies and cannot be deleted.", v.VpcId)
	}
	for id, g := range r.Groups {
		if g.VpcId == v.VpcId {
			delete(r.Groups, id)
			delete(r.Tags, id)
		}
	}
	for id, t := range r.Tables {
		if t.VpcId == v.VpcId {
			delete(r.Tables, id)
			delete(r.Tags, id)
		}
	}
	for id, a := range r.Acls {
		if a.VpcId == v.VpcId {
			delete(r.Acls, id)
			delete(r.Tags, id)
		}
	}
	delete(r.Vpcs, v.VpcId)
	delete(r.Tags, v.VpcId)
	return ok(), nil
}

type boolValue struct {
	Value bool `xml:"value"`
}

func (s *Service) describeVpcAttribute(c *core.Ctx, in *struct{ VpcId, Attribute string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, err := s.reg(c).vpc(in.VpcId)
	if err != nil {
		return nil, err
	}
	out := &struct {
		Header
		VpcId        string     `xml:"vpcId"`
		DnsSupport   *boolValue `xml:"enableDnsSupport,omitempty"`
		DnsHostnames *boolValue `xml:"enableDnsHostnames,omitempty"`
		UsageMetrics *boolValue `xml:"enableNetworkAddressUsageMetrics,omitempty"`
	}{VpcId: v.VpcId}
	switch in.Attribute {
	case "enableDnsSupport":
		out.DnsSupport = &boolValue{v.DnsSupport}
	case "enableDnsHostnames":
		out.DnsHostnames = &boolValue{v.DnsHostnames}
	case "enableNetworkAddressUsageMetrics":
		out.UsageMetrics = &boolValue{v.UsageMetrics}
	default:
		return nil, invalid("Value (%s) for parameter attribute is invalid. Unknown attribute.", in.Attribute)
	}
	return out, nil
}

func (s *Service) modifyVpcAttribute(c *core.Ctx, in *struct {
	VpcId                                                                  string
	EnableDnsSupport, EnableDnsHostnames, EnableNetworkAddressUsageMetrics *struct{ Value *bool }
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, err := s.reg(c).vpc(in.VpcId)
	if err != nil {
		return nil, err
	}
	for _, f := range []struct {
		in  *struct{ Value *bool }
		dst *bool
	}{{in.EnableDnsSupport, &v.DnsSupport}, {in.EnableDnsHostnames, &v.DnsHostnames}, {in.EnableNetworkAddressUsageMetrics, &v.UsageMetrics}} {
		if f.in != nil && f.in.Value != nil {
			*f.dst = *f.in.Value
		}
	}
	return ok(), nil
}

// ---- subnets ----

func (s *Service) describeSubnets(c *core.Ctx, in *struct {
	SubnetIds []string `query:"SubnetId"`
	Filters   []filter `query:"Filter"`
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	subnets, err := describe(r, r.Subnets, in.SubnetIds, in.Filters, "InvalidSubnetID.NotFound", "subnet ID", (*Subnet).view)
	return &struct {
		Header
		Subnets []Subnet `xml:"subnetSet>item"`
	}{Subnets: subnets}, err
}

func (s *Service) createSubnet(c *core.Ctx, in *struct {
	VpcId              string
	CidrBlock          string
	AvailabilityZone   string
	AvailabilityZoneId string
	TagSpecs           []tagSpec `query:"TagSpecification"`
}) (any, error) {
	_, n, err := net.ParseCIDR(in.CidrBlock)
	if err != nil || n.IP.To4() == nil {
		return nil, invalid("Value (%s) for parameter cidrBlock is invalid. This is not a valid CIDR block.", in.CidrBlock)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	v, err := r.vpc(in.VpcId)
	if err != nil {
		return nil, err
	}
	ones, _ := n.Mask.Size()
	if !v.holds(n) || ones > 28 {
		return nil, core.Errorf(400, "InvalidSubnet.Range", "The CIDR '%s' is invalid.", in.CidrBlock)
	}
	for _, sn := range r.Subnets {
		if _, o, _ := net.ParseCIDR(sn.CidrBlock); sn.VpcId == v.VpcId && (o.Contains(n.IP) || n.Contains(o.IP)) {
			return nil, core.Errorf(400, "InvalidSubnet.Conflict", "The CIDR '%s' conflicts with another subnet", in.CidrBlock)
		}
	}
	az := in.AvailabilityZone
	for _, z := range zones(c.Region) {
		if az == "" && (in.AvailabilityZoneId == "" || zoneID(z) == in.AvailabilityZoneId) {
			az = z
		}
	}
	if !slices.Contains(zones(c.Region), az) {
		return nil, invalid("Value (%s) for parameter availabilityZone is invalid. Subnets can currently only be created in the following availability zones: %s.",
			cmp.Or(az, in.AvailabilityZoneId), strings.Join(zones(c.Region), ", "))
	}
	sn := r.newSubnet(v, n.String(), az)
	r.tag(sn.SubnetId, in.TagSpecs)
	return &struct {
		Header
		Subnet Subnet `xml:"subnet"`
	}{Subnet: withTags(r, sn, (*Subnet).view)}, nil
}

func (s *Service) deleteSubnet(c *core.Ctx, in *struct{ SubnetId string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	if r.Subnets[in.SubnetId] == nil {
		return nil, core.Errorf(400, "InvalidSubnetID.NotFound", "The subnet ID '%s' does not exist", in.SubnetId)
	}
	if slices.ContainsFunc(slices.Collect(maps.Values(r.Net.Enis)), func(n *NetworkInterface) bool { return n.SubnetId == in.SubnetId }) {
		return nil, core.Errorf(400, "DependencyViolation", "The subnet '%s' has dependencies and cannot be deleted.", in.SubnetId)
	}
	r.dropSubnet(in.SubnetId)
	delete(r.Subnets, in.SubnetId)
	delete(r.Tags, in.SubnetId)
	return ok(), nil
}

// modifySubnetAttribute sets MapPublicIpOnLaunch and AssignIpv6AddressOnCreation.
func (s *Service) modifySubnetAttribute(c *core.Ctx, in *struct {
	SubnetId                                         string
	MapPublicIpOnLaunch, AssignIpv6AddressOnCreation *struct{ Value *bool }
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sn := s.reg(c).Subnets[in.SubnetId]
	if sn == nil {
		return nil, core.Errorf(400, "InvalidSubnetID.NotFound", "The subnet ID '%s' does not exist", in.SubnetId)
	}
	if v := in.MapPublicIpOnLaunch; v != nil && v.Value != nil {
		sn.MapPublicIpOnLaunch = *v.Value
	}
	if v := in.AssignIpv6AddressOnCreation; v != nil && v.Value != nil {
		sn.AssignIpv6AddressOnCreation = *v.Value
	}
	return ok(), nil
}

// ---- security groups ----

// group finds a group by id, or by name in the default VPC; lock held.
func (r *region) group(id, name string) (*Group, error) {
	if g := r.Groups[id]; g != nil {
		return g, nil
	}
	if def := r.defaultVpc(); id == "" && def != nil {
		for _, k := range core.SortedKeys(r.Groups) {
			if g := r.Groups[k]; g.VpcId == def.VpcId && g.GroupName == name {
				return g, nil
			}
		}
	}
	return nil, core.Errorf(400, "InvalidGroup.NotFound", "The security group '%s' does not exist", cmp.Or(id, name))
}

func (s *Service) describeGroups(c *core.Ctx, in *struct {
	GroupIds   []string `query:"GroupId"`
	GroupNames []string `query:"GroupName"`
	Filters    []filter `query:"Filter"`
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	ids := in.GroupIds
	for _, n := range in.GroupNames {
		g, err := r.group("", n)
		if err != nil {
			return nil, err
		}
		ids = append(ids, g.GroupId)
	}
	groups, err := describe(r, r.Groups, ids, in.Filters, "InvalidGroup.NotFound", "security group", (*Group).view)
	return &struct {
		Header
		Groups []Group `xml:"securityGroupInfo>item"`
	}{Groups: groups}, err
}

func (s *Service) createGroup(c *core.Ctx, in *struct {
	GroupName   string
	Description string `query:"GroupDescription"`
	VpcId       string
	TagSpecs    []tagSpec `query:"TagSpecification"`
}) (any, error) {
	if in.GroupName == "" || in.GroupName == "default" {
		return nil, invalid("Value (%s) for parameter GroupName is invalid. Cannot use reserved security group name: default", in.GroupName)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	if in.VpcId == "" {
		if def := r.defaultVpc(); def != nil {
			in.VpcId = def.VpcId
		}
	}
	if _, err := r.vpc(in.VpcId); err != nil {
		return nil, err
	}
	for _, g := range r.Groups {
		if g.VpcId == in.VpcId && g.GroupName == in.GroupName {
			return nil, core.Errorf(400, "InvalidGroup.Duplicate", "The security group '%s' already exists for VPC '%s'", in.GroupName, in.VpcId)
		}
	}
	g := r.newGroup(in.VpcId, in.GroupName, in.Description)
	r.tag(g.GroupId, in.TagSpecs)
	return &struct {
		Header
		Return  bool   `xml:"return"`
		GroupId string `xml:"groupId"`
		Arn     string `xml:"securityGroupArn"`
		TagSet  []Tag  `xml:"tagSet>item"`
	}{Return: true, GroupId: g.GroupId, Arn: g.SecurityGroupArn, TagSet: r.tagSet(g.GroupId)}, nil
}

func (s *Service) deleteGroup(c *core.Ctx, in *struct{ GroupId, GroupName string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	g, err := r.group(in.GroupId, in.GroupName)
	if err != nil {
		return nil, err
	}
	if g.GroupName == "default" {
		return nil, core.Errorf(400, "CannotDelete", "the specified group: \"%s\" name: \"default\" cannot be deleted by a user", g.GroupId)
	}
	delete(r.Groups, g.GroupId)
	delete(r.Tags, g.GroupId)
	return &struct {
		Header
		Return  bool   `xml:"return"`
		GroupId string `xml:"groupId"`
	}{Return: true, GroupId: g.GroupId}, nil
}

func protocol(p string) string {
	switch strings.ToLower(p) {
	case "tcp", "6":
		return "tcp"
	case "udp", "17":
		return "udp"
	case "icmp", "1":
		return "icmp"
	case "-1", "all":
		return "-1"
	}
	return p
}

// flatten splits permissions into one-source rules, the unit EC2 de-duplicates on.
func flatten(ps []perm) []perm {
	var out []perm
	for _, p := range ps {
		base := perm{IpProtocol: protocol(p.IpProtocol), FromPort: p.FromPort, ToPort: p.ToPort}
		if base.IpProtocol == "-1" {
			base.FromPort, base.ToPort = nil, nil
		}
		for _, x := range p.IpRanges {
			r := base
			r.IpRanges = []ipRange{x}
			out = append(out, r)
		}
		for _, x := range p.Ipv6Ranges {
			r := base
			r.Ipv6Ranges = []ipv6Range{x}
			out = append(out, r)
		}
		for _, x := range p.PrefixListIds {
			r := base
			r.PrefixListIds = []prefixID{x}
			out = append(out, r)
		}
		for _, x := range p.Groups {
			r := base
			r.Groups = []pair{x}
			out = append(out, r)
		}
	}
	return out
}

func port(p *int) string {
	if p == nil {
		return "-"
	}
	return strconv.Itoa(*p)
}

// ports identifies a permission's protocol and port range.
func ports(p perm) string { return p.IpProtocol + " " + port(p.FromPort) + " " + port(p.ToPort) }

// ruleKey identifies a one-source rule (descriptions do not count).
func ruleKey(p perm) string {
	k := ports(p)
	for _, x := range p.IpRanges {
		k += " " + x.CidrIp
	}
	for _, x := range p.Ipv6Ranges {
		k += " " + x.CidrIpv6
	}
	for _, x := range p.PrefixListIds {
		k += " " + x.PrefixListId
	}
	for _, x := range p.Groups {
		k += " " + x.GroupId + x.GroupName
	}
	return k
}

// grouped merges one-source rules that share protocol and ports.
func grouped(rules []perm) []perm {
	var out []perm
	for _, r := range rules {
		i := slices.IndexFunc(out, func(p perm) bool { return ports(p) == ports(r) })
		if i < 0 {
			out, i = append(out, perm{IpProtocol: r.IpProtocol, FromPort: r.FromPort, ToPort: r.ToPort}), len(out)
		}
		out[i].IpRanges = append(out[i].IpRanges, r.IpRanges...)
		out[i].Ipv6Ranges = append(out[i].Ipv6Ranges, r.Ipv6Ranges...)
		out[i].PrefixListIds = append(out[i].PrefixListIds, r.PrefixListIds...)
		out[i].Groups = append(out[i].Groups, r.Groups...)
	}
	return out
}

// rules serves Authorize/Revoke SecurityGroup Ingress/Egress, all or nothing.
func (s *Service) rules(egress, add bool) core.Handler {
	return core.H(func(c *core.Ctx, in *struct {
		GroupId, GroupName string
		IpPermissions      []perm
		IpProtocol, CidrIp string // the legacy single-rule form
		FromPort, ToPort   *int
	}) (any, error) {
		s.mu.Lock()
		defer s.mu.Unlock()
		r := s.reg(c)
		g, err := r.group(in.GroupId, in.GroupName)
		if err != nil {
			return nil, err
		}
		ps := in.IpPermissions
		if in.CidrIp != "" {
			ps = append(ps, perm{IpProtocol: in.IpProtocol, FromPort: in.FromPort, ToPort: in.ToPort, IpRanges: []ipRange{{CidrIp: in.CidrIp}}})
		}
		dst := &g.IpPermissions
		if egress {
			dst = &g.IpPermissionsEgress
		}
		next := slices.Clone(*dst)
		for _, p := range flatten(ps) {
			for i := range p.Groups {
				p.Groups[i].UserId = cmp.Or(p.Groups[i].UserId, r.Account)
			}
			i := slices.IndexFunc(next, func(x perm) bool { return ruleKey(x) == ruleKey(p) })
			switch {
			case add && i >= 0:
				return nil, core.Errorf(400, "InvalidPermission.Duplicate", "the specified rule \"%s\" already exists", ruleKey(p))
			case add:
				next = append(next, p)
			case i >= 0:
				next = slices.Delete(next, i, i+1)
			}
		}
		*dst = next
		return ok(), nil
	})
}

// ---- gateways, route tables, network ACLs, VPN gateways, instances ----

func (s *Service) describeGateways(c *core.Ctx, in *struct {
	Ids     []string `query:"InternetGatewayId"`
	Filters []filter `query:"Filter"`
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	gws, err := describe(r, r.Gateways, in.Ids, in.Filters, "InvalidInternetGatewayID.NotFound", "internetGateway ID", (*Gateway).view)
	return &struct {
		Header
		Gateways []Gateway `xml:"internetGatewaySet>item"`
	}{Gateways: gws}, err
}

func (s *Service) describeTables(c *core.Ctx, in *struct {
	Ids     []string `query:"RouteTableId"`
	Filters []filter `query:"Filter"`
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	tables, err := describe(r, r.Tables, in.Ids, in.Filters, "InvalidRouteTableID.NotFound", "routeTable ID", (*RouteTable).view)
	return &struct {
		Header
		Tables []RouteTable `xml:"routeTableSet>item"`
	}{Tables: tables}, err
}

func (s *Service) describeAcls(c *core.Ctx, in *struct {
	Ids     []string `query:"NetworkAclId"`
	Filters []filter `query:"Filter"`
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	acls, err := describe(r, r.Acls, in.Ids, in.Filters, "InvalidNetworkAclID.NotFound", "networkAcl ID", (*Acl).view)
	return &struct {
		Header
		Acls []Acl `xml:"networkAclSet>item"`
	}{Acls: acls}, err
}

// empty answers describe calls for resources pogi never has (CDK's VPC
// lookup asks for VPN gateways, for instance).
func empty(*core.Ctx, *struct{}) (any, error) { return &struct{ Header }{}, nil }

// ---- tags ----

var resourceTypes = map[string]string{"vpc": "vpc", "subnet": "subnet", "sg": "security-group", "igw": "internet-gateway",
	"rtb": "route-table", "acl": "network-acl", "key": "key-pair", "ami": "image", "i": "instance", "vol": "volume", "lt": "launch-template",
	"tgw": "transit-gateway", "tgw-attach": "transit-gateway-attachment", "fl": "vpc-flow-log", "h": "dedicated-host", "dopt": "dhcp-options"}

func resourceType(id string) string {
	p, rest, _ := strings.Cut(id, "-")
	if q, _, _ := strings.Cut(rest, "-"); resourceTypes[p+"-"+q] != "" { // tgw-attach-…
		return resourceTypes[p+"-"+q]
	}
	return resourceTypes[p]
}

// exists reports whether a taggable resource exists; lock held.
func (r *region) exists(id string) bool {
	switch resourceType(id) {
	case "vpc":
		return r.Vpcs[id] != nil
	case "subnet":
		return r.Subnets[id] != nil
	case "security-group":
		return r.Groups[id] != nil
	case "internet-gateway":
		return r.Gateways[id] != nil
	case "route-table":
		return r.Tables[id] != nil
	case "network-acl":
		return r.Acls[id] != nil
	case "image":
		return r.allImages()[id] != nil
	case "instance":
		return r.Instances[id] != nil
	case "volume":
		return r.Volumes[id] != nil
	case "launch-template":
		return r.Templates[id] != nil
	case "key-pair":
		return slices.ContainsFunc(slices.Collect(maps.Values(r.Keys)), func(k *KeyPair) bool { return k.KeyPairId == id })
	}
	return r.netExists(id) || r.extraExists(id)
}

func (s *Service) createTags(c *core.Ctx, in *struct {
	Resources []string `query:"ResourceId"`
	Tags      []Tag    `query:"Tag"`
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	for _, id := range in.Resources {
		if !r.exists(id) {
			return nil, core.Errorf(400, "InvalidID", "The ID '%s' is not valid", id)
		}
	}
	for _, id := range in.Resources {
		r.tag(id, []tagSpec{{Tags: in.Tags}})
	}
	return ok(), nil
}

type tagMatch struct {
	Key   string
	Value *string // nil deletes whatever the value
}

// deleteTags removes the given tags, or all tags when none are given.
func (s *Service) deleteTags(c *core.Ctx, in *struct {
	Resources []string   `query:"ResourceId"`
	Tags      []tagMatch `query:"Tag"`
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	for _, id := range in.Resources {
		for k, v := range r.Tags[id] {
			if len(in.Tags) == 0 || slices.ContainsFunc(in.Tags, func(t tagMatch) bool { return t.Key == k && (t.Value == nil || *t.Value == v) }) {
				delete(r.Tags[id], k)
			}
		}
	}
	return ok(), nil
}

type tagDescription struct {
	ResourceId   string `xml:"resourceId"`
	ResourceType string `xml:"resourceType"`
	Key          string `xml:"key"`
	Value        string `xml:"value"`
}

func (s *Service) describeTags(c *core.Ctx, in *struct {
	Filters []filter `query:"Filter"`
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	out := &struct {
		Header
		Tags []tagDescription `xml:"tagSet>item"`
	}{}
	for _, id := range core.SortedKeys(r.Tags) {
		for _, k := range core.SortedKeys(r.Tags[id]) {
			d := tagDescription{id, resourceType(id), k, r.Tags[id][k]}
			if match(in.Filters, map[string][]string{"resource-id": {id}, "resource-type": {d.ResourceType}, "key": {k}, "value": {d.Value}}, r.Tags[id]) {
				out.Tags = append(out.Tags, d)
			}
		}
	}
	return out, nil
}

// ---- images, key pairs ----

func (s *Service) describeImages(c *core.Ctx, in *struct {
	ImageIds []string `query:"ImageId"`
	Owners   []string `query:"Owner"`
	Filters  []filter `query:"Filter"`
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	imgs, err := describe(r, r.allImages(), in.ImageIds, in.Filters, "InvalidAMIID.NotFound", "image id", (*Image).view)
	if err != nil {
		return nil, err
	}
	imgs = slices.DeleteFunc(imgs, func(i Image) bool {
		return len(in.Owners) > 0 && !slices.ContainsFunc(in.Owners, func(o string) bool {
			return o == i.ImageOwnerId || o == i.ImageOwnerAlias || o == "self" && i.ImageOwnerId == c.Account
		})
	})
	return &struct {
		Header
		Images []Image `xml:"imagesSet>item"`
	}{Images: imgs}, nil
}

func (s *Service) describeKeyPairs(c *core.Ctx, in *struct {
	KeyNames         []string `query:"KeyName"`
	KeyPairIds       []string `query:"KeyPairId"`
	Filters          []filter `query:"Filter"`
	IncludePublicKey bool
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	names := in.KeyNames
	for _, id := range in.KeyPairIds {
		name := ""
		for n, k := range r.Keys {
			if k.KeyPairId == id {
				name = n
			}
		}
		if name == "" {
			return nil, core.Errorf(400, "InvalidKeyPair.NotFound", "The key pair '%s' does not exist", id)
		}
		names = append(names, name)
	}
	keys, err := describe(r, r.Keys, names, in.Filters, "InvalidKeyPair.NotFound", "key pair", (*KeyPair).view)
	for i := range keys {
		if !in.IncludePublicKey {
			keys[i].PublicKey = ""
		}
	}
	return &struct {
		Header
		Keys []KeyPair `xml:"keySet>item"`
	}{Keys: keys}, err
}

func colonHex(b []byte) string {
	var parts []string
	for _, x := range b {
		parts = append(parts, hex.EncodeToString([]byte{x}))
	}
	return strings.Join(parts, ":")
}

// addKey stores a new key pair; lock held.
func (r *region) addKey(name, typ, fingerprint, publicKey string, specs []tagSpec) (*KeyPair, error) {
	if r.Keys[name] != nil {
		return nil, core.Errorf(400, "InvalidKeyPair.Duplicate", "The keypair '%s' already exists.", name)
	}
	k := &KeyPair{KeyPairId: newID("key"), KeyName: name, KeyFingerprint: fingerprint, KeyType: typ,
		CreateTime: time.Now().UTC().Format(core.ISO), PublicKey: publicKey}
	r.Keys[name] = k
	r.tag(k.KeyPairId, specs)
	return k, nil
}

// createKeyPair returns a new private key: RSA as PKCS#1 PEM like EC2,
// ed25519 as PKCS#8 PEM (EC2 uses the OpenSSH format, which stdlib lacks).
func (s *Service) createKeyPair(c *core.Ctx, in *struct {
	KeyName  string
	KeyType  string
	TagSpecs []tagSpec `query:"TagSpecification"`
}) (any, error) {
	if in.KeyName == "" {
		return nil, core.Errorf(400, "MissingParameter", "The request must contain the parameter KeyName")
	}
	var der []byte
	var block *pem.Block
	var pub crypto.PublicKey
	if in.KeyType == "ed25519" {
		var priv ed25519.PrivateKey
		pub, priv, _ = ed25519.GenerateKey(rand.Reader)
		der, _ = x509.MarshalPKCS8PrivateKey(priv)
		block = &pem.Block{Type: "PRIVATE KEY", Bytes: der}
	} else {
		priv, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			return nil, err
		}
		pub = &priv.PublicKey
		der, _ = x509.MarshalPKCS8PrivateKey(priv)
		block = &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(priv)}
	}
	sshPub, err := ssh.NewPublicKey(pub) // what an instance with this key lets in
	if err != nil {
		return nil, err
	}
	sum := sha1.Sum(der) // EC2 fingerprints created keys with SHA-1 of the PKCS#8 key
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	k, err := r.addKey(in.KeyName, cmp.Or(in.KeyType, "rsa"), colonHex(sum[:]), strings.TrimSpace(string(ssh.MarshalAuthorizedKey(sshPub))), in.TagSpecs)
	if err != nil {
		return nil, err
	}
	return &struct {
		Header
		KeyName        string `xml:"keyName"`
		KeyFingerprint string `xml:"keyFingerprint"`
		KeyMaterial    string `xml:"keyMaterial"`
		KeyPairId      string `xml:"keyPairId"`
		TagSet         []Tag  `xml:"tagSet>item"`
	}{KeyName: k.KeyName, KeyFingerprint: k.KeyFingerprint, KeyMaterial: string(pem.EncodeToMemory(block)), KeyPairId: k.KeyPairId,
		TagSet: r.tagSet(k.KeyPairId)}, nil
}

func (s *Service) importKeyPair(c *core.Ctx, in *struct {
	KeyName           string
	PublicKeyMaterial []byte
	TagSpecs          []tagSpec `query:"TagSpecification"`
}) (any, error) {
	f := strings.Fields(string(in.PublicKeyMaterial))
	var blob []byte
	if len(f) >= 2 {
		blob, _ = base64.StdEncoding.DecodeString(f[1])
	}
	if len(blob) == 0 || f[0] != "ssh-rsa" && f[0] != "ssh-ed25519" {
		return nil, core.Errorf(400, "InvalidKey.Format", "Key is not in valid OpenSSH public key format")
	}
	typ, md := "rsa", md5.Sum(blob)
	fingerprint := colonHex(md[:]) // RFC 4716 MD5 for RSA, SHA-256 for ed25519, as EC2 does
	if f[0] == "ssh-ed25519" {
		sum := sha256.Sum256(blob)
		typ, fingerprint = "ed25519", base64.StdEncoding.EncodeToString(sum[:])
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	k, err := r.addKey(in.KeyName, typ, fingerprint, strings.TrimSpace(string(in.PublicKeyMaterial)), in.TagSpecs)
	if err != nil {
		return nil, err
	}
	return &struct {
		Header
		KeyName        string `xml:"keyName"`
		KeyFingerprint string `xml:"keyFingerprint"`
		KeyPairId      string `xml:"keyPairId"`
		TagSet         []Tag  `xml:"tagSet>item"`
	}{KeyName: k.KeyName, KeyFingerprint: k.KeyFingerprint, KeyPairId: k.KeyPairId, TagSet: r.tagSet(k.KeyPairId)}, nil
}

func (s *Service) deleteKeyPair(c *core.Ctx, in *struct{ KeyName, KeyPairId string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	for name, k := range r.Keys {
		if name == in.KeyName || k.KeyPairId == in.KeyPairId {
			delete(r.Keys, name)
			delete(r.Tags, k.KeyPairId)
		}
	}
	return ok(), nil // deleting a missing key pair succeeds, as in EC2
}

// ---- tagging API, console ----

// TaggedResources lists tagged resources (ARN → tags) for the tagging API.
func (s *Service) TaggedResources(account, region string) map[string]map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]map[string]string{}
	if r := s.st.Regions[account+"/"+region]; r != nil {
		for id, tags := range r.Tags {
			if len(tags) > 0 {
				out[core.ARN("ec2", region, account, resourceType(id)+"/"+id)] = maps.Clone(tags)
			}
		}
	}
	return out
}

// Resources lists instances, VPCs, subnets, security groups and key pairs.
func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	it := core.Table{Name: "Instances", Columns: []string{"Instance", "State", "Type", "Image", "Private IP", "Region", "Account"}}
	vt := core.Table{Name: "VPCs", Columns: []string{"VPC", "CIDR", "Default", "Region", "Account"}}
	st := core.Table{Name: "Subnets", Columns: []string{"Subnet", "VPC", "CIDR", "Zone"}}
	gt := core.Table{Name: "Security groups", Columns: []string{"Group", "Name", "VPC", "Region"}}
	kt := core.Table{Name: "Key pairs", Columns: []string{"Name", "Type", "Fingerprint", "Region"}}
	for _, k := range core.SortedKeys(s.st.Regions) {
		r := s.st.Regions[k]
		for _, id := range core.SortedKeys(r.Instances) {
			i := r.Instances[id]
			it.Rows = append(it.Rows, []string{id, i.State.Name, i.InstanceType, i.ImageId, i.PrivateIpAddress, r.Name, r.Account})
		}
		for _, id := range core.SortedKeys(r.Vpcs) {
			v := r.Vpcs[id]
			vt.Rows = append(vt.Rows, []string{v.VpcId, v.CidrBlock, strconv.FormatBool(v.IsDefault), r.Name, r.Account})
		}
		for _, id := range core.SortedKeys(r.Subnets) {
			sn := r.Subnets[id]
			st.Rows = append(st.Rows, []string{sn.SubnetId, sn.VpcId, sn.CidrBlock, sn.AvailabilityZone})
		}
		for _, id := range core.SortedKeys(r.Groups) {
			g := r.Groups[id]
			gt.Rows = append(gt.Rows, []string{g.GroupId, g.GroupName, g.VpcId, r.Name})
		}
		for _, name := range core.SortedKeys(r.Keys) {
			kp := r.Keys[name]
			kt.Rows = append(kt.Rows, []string{kp.KeyName, kp.KeyType, kp.KeyFingerprint, r.Name})
		}
	}
	return []core.Table{it, vt, st, gt, kt}
}

// Inspect returns one resource with its tags.
func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.st.Regions {
		switch {
		case table == "Instances" && r.Instances[id] != nil:
			return withTags(r, r.Instances[id], (*Instance).view)
		case table == "VPCs" && r.Vpcs[id] != nil:
			return withTags(r, r.Vpcs[id], (*Vpc).view)
		case table == "Subnets" && r.Subnets[id] != nil:
			return withTags(r, r.Subnets[id], (*Subnet).view)
		case table == "Security groups" && r.Groups[id] != nil:
			return withTags(r, r.Groups[id], (*Group).view)
		case table == "Key pairs" && r.Keys[id] != nil:
			return withTags(r, r.Keys[id], (*KeyPair).view)
		}
	}
	return nil
}
