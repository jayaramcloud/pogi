package ec2

// Flow logs and dedicated hosts: records kept and described. No flow log
// records are written to the destination, and instances do not run on hosts.

import (
	"cmp"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// defaultFlowFormat is the log format EC2 uses when a flow log names none.
const defaultFlowFormat = "${version} ${account-id} ${interface-id} ${srcaddr} ${dstaddr} ${srcport} ${dstport} ${protocol} ${packets} ${bytes} ${start} ${end} ${action} ${log-status}"

type flowDestOptions struct {
	FileFormat               string `xml:"fileFormat"`
	HiveCompatiblePartitions bool   `xml:"hiveCompatiblePartitions"`
	PerHourPartition         bool   `xml:"perHourPartition"`
}

// FlowLog is a flow log.
type FlowLog struct {
	CreationTime             string           `xml:"creationTime"`
	DeliverLogsPermissionArn string           `xml:"deliverLogsPermissionArn,omitempty"`
	DeliverLogsStatus        string           `xml:"deliverLogsStatus"`
	FlowLogId                string           `xml:"flowLogId"`
	FlowLogStatus            string           `xml:"flowLogStatus"`
	LogGroupName             string           `xml:"logGroupName,omitempty"`
	ResourceId               string           `xml:"resourceId"`
	TrafficType              string           `xml:"trafficType"`
	LogDestinationType       string           `xml:"logDestinationType"`
	LogDestination           string           `xml:"logDestination,omitempty"`
	LogFormat                string           `xml:"logFormat"`
	TagSet                   []Tag            `xml:"tagSet>item" json:"-"`
	MaxAggregationInterval   int              `xml:"maxAggregationInterval"`
	DestinationOptions       *flowDestOptions `xml:"destinationOptions"`
}

func (f *FlowLog) view() (string, *[]Tag, map[string][]string) {
	return f.FlowLogId, &f.TagSet, map[string][]string{"flow-log-id": {f.FlowLogId}, "resource-id": {f.ResourceId}, "traffic-type": {f.TrafficType},
		"log-group-name": {f.LogGroupName}, "log-destination-type": {f.LogDestinationType}, "log-destination": {f.LogDestination},
		"deliver-log-status": {f.DeliverLogsStatus}, "log-status": {f.FlowLogStatus}, "flow-log-status": {f.FlowLogStatus}}
}

// flowResources are the resource types a flow log watches, with the id prefix of each.
var flowResources = map[string]string{"VPC": "vpc-", "Subnet": "subnet-", "NetworkInterface": "eni-", "TransitGateway": "tgw-", "TransitGatewayAttachment": "tgw-attach-"}

func (s *Service) createFlowLogs(c *core.Ctx, in *struct {
	ResourceIds                                                    []string `query:"ResourceId"`
	ResourceType, TrafficType, LogDestinationType, LogDestination  string
	LogGroupName, DeliverLogsPermissionArn, LogFormat, ClientToken string
	MaxAggregationInterval                                         int
	DestinationOptions                                             *flowDestOptions
	TagSpecifications                                              []tagSpec `query:"TagSpecification"`
}) (any, error) {
	prefix, known := flowResources[in.ResourceType]
	dest := cmp.Or(in.LogDestinationType, "cloud-watch-logs")
	switch {
	case len(in.ResourceIds) == 0:
		return nil, core.Errorf(400, "MissingParameter", "The request must contain the parameter ResourceId")
	case !known:
		return nil, invalid("Value (%s) for parameter resourceType is invalid.", in.ResourceType)
	case !slices.Contains([]string{"ACCEPT", "REJECT", "ALL"}, in.TrafficType):
		return nil, invalid("Value (%s) for parameter trafficType is invalid.", in.TrafficType)
	case !slices.Contains([]string{"cloud-watch-logs", "s3", "kinesis-data-firehose"}, dest):
		return nil, invalid("Value (%s) for parameter logDestinationType is invalid.", dest)
	case dest != "cloud-watch-logs" && in.LogDestination == "":
		return nil, invalid("LogDestination is required for the %s destination type.", dest)
	case dest == "cloud-watch-logs" && in.LogGroupName == "" && in.LogDestination == "":
		return nil, invalid("LogGroupName or LogDestination is required for the cloud-watch-logs destination type.")
	case in.MaxAggregationInterval != 0 && in.MaxAggregationInterval != 60 && in.MaxAggregationInterval != 600:
		return nil, invalid("Value (%d) for parameter maxAggregationInterval is invalid. It must be 60 or 600.", in.MaxAggregationInterval)
	}
	opts := in.DestinationOptions
	if dest == "s3" {
		if opts == nil {
			opts = &flowDestOptions{}
		}
		opts.FileFormat = cmp.Or(opts.FileFormat, "plain-text")
	} else {
		opts = nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	e := r.x()
	out := &struct {
		Header
		ClientToken  string    `xml:"clientToken,omitempty"`
		FlowLogIds   []string  `xml:"flowLogIdSet>item"`
		Unsuccessful []failure `xml:"unsuccessful>item"`
	}{ClientToken: in.ClientToken}
	for _, id := range in.ResourceIds {
		switch {
		case !strings.HasPrefix(id, prefix):
			out.Unsuccessful = append(out.Unsuccessful, newFailure(id, "InvalidParameter", "Resource id "+id+" is not a "+in.ResourceType+"."))
			continue
		case in.ResourceType == "VPC" && r.Vpcs[id] == nil:
			out.Unsuccessful = append(out.Unsuccessful, newFailure(id, "InvalidVpcID.NotFound", "The vpc ID '"+id+"' does not exist"))
			continue
		case in.ResourceType == "Subnet" && r.Subnets[id] == nil:
			out.Unsuccessful = append(out.Unsuccessful, newFailure(id, "InvalidSubnetID.NotFound", "The subnet ID '"+id+"' does not exist"))
			continue
		}
		f := &FlowLog{CreationTime: time.Now().UTC().Format(core.ISO), DeliverLogsPermissionArn: in.DeliverLogsPermissionArn, DeliverLogsStatus: "SUCCESS",
			FlowLogId: newID("fl"), FlowLogStatus: "ACTIVE", LogGroupName: in.LogGroupName, ResourceId: id, TrafficType: in.TrafficType,
			LogDestinationType: dest, LogDestination: in.LogDestination, LogFormat: cmp.Or(in.LogFormat, defaultFlowFormat),
			MaxAggregationInterval: cmp.Or(in.MaxAggregationInterval, 600), DestinationOptions: opts}
		if dest == "cloud-watch-logs" {
			f.DeliverLogsStatus = "" // set when a permission arn is given
			if in.DeliverLogsPermissionArn != "" {
				f.DeliverLogsStatus = "SUCCESS"
			}
		}
		e.FlowLogs[f.FlowLogId] = f
		r.tag(f.FlowLogId, typed(in.TagSpecifications, "vpc-flow-log"))
		out.FlowLogIds = append(out.FlowLogIds, f.FlowLogId)
	}
	return out, nil
}

// describeFlowLogs lists flow logs; an unknown id is left out, as EC2 does.
func (s *Service) describeFlowLogs(c *core.Ctx, in *struct {
	FlowLogIds []string `query:"FlowLogId"`
	Filters    []filter `query:"Filter"`
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	_, _, served := (&FlowLog{}).view()
	if err := checkFilters(in.Filters, served); err != nil {
		return nil, err
	}
	e := r.x()
	out := &struct {
		Header
		FlowLogs []FlowLog `xml:"flowLogSet>item"`
	}{}
	for _, id := range core.SortedKeys(e.FlowLogs) {
		if len(in.FlowLogIds) > 0 && !slices.Contains(in.FlowLogIds, id) {
			continue
		}
		cp := *e.FlowLogs[id]
		_, tags, attrs := cp.view()
		if match(in.Filters, attrs, r.Tags[id]) {
			*tags = r.tagSet(id)
			out.FlowLogs = append(out.FlowLogs, cp)
		}
	}
	return out, nil
}

func (s *Service) deleteFlowLogs(c *core.Ctx, in *struct {
	FlowLogIds []string `query:"FlowLogId"`
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	e := r.x()
	out := &struct {
		Header
		Unsuccessful []failure `xml:"unsuccessful>item"`
	}{}
	for _, id := range in.FlowLogIds {
		if e.FlowLogs[id] == nil {
			out.Unsuccessful = append(out.Unsuccessful, newFailure(id, "InvalidFlowLogId.NotFound", "These flow log ids ("+id+") do not exist."))
			continue
		}
		delete(e.FlowLogs, id)
		delete(r.Tags, id)
	}
	return out, nil
}

// ---- dedicated hosts ----

type hostProperties struct {
	Cores          int    `xml:"cores"`
	InstanceFamily string `xml:"instanceFamily,omitempty"`
	InstanceType   string `xml:"instanceType,omitempty"`
	Sockets        int    `xml:"sockets"`
	TotalVCpus     int    `xml:"totalVCpus"`
}

// Host is a dedicated host.
// ponytail: every host is a two-socket, 96 vCPU machine whatever its family.
type Host struct {
	HostId             string         `xml:"hostId"`
	AvailabilityZone   string         `xml:"availabilityZone"`
	AvailabilityZoneId string         `xml:"availabilityZoneId"`
	AutoPlacement      string         `xml:"autoPlacement"`
	HostRecovery       string         `xml:"hostRecovery"`
	HostMaintenance    string         `xml:"hostMaintenance"`
	State              string         `xml:"state"`
	AllocationTime     string         `xml:"allocationTime"`
	ReleaseTime        string         `xml:"releaseTime,omitempty"`
	OwnerId            string         `xml:"ownerId"`
	HostProperties     hostProperties `xml:"hostProperties"`
	Instances          []struct{}     `xml:"instances>item"`
	AvailableCapacity  struct {
		AvailableVCpus int `xml:"availableVCpus"`
	} `xml:"availableCapacity"`
	TagSet []Tag `xml:"tagSet>item" json:"-"`
}

func (h *Host) view() (string, *[]Tag, map[string][]string) {
	return h.HostId, &h.TagSet, map[string][]string{"host-id": {h.HostId}, "state": {h.State}, "availability-zone": {h.AvailabilityZone},
		"instance-type": {h.HostProperties.InstanceType}, "auto-placement": {h.AutoPlacement}, "host-recovery": {h.HostRecovery}}
}

var onOff = regexp.MustCompile(`^(on|off)$`)

// setType sets the one instance type or family a host runs.
func (h *Host) setType(typ, family string) error {
	switch {
	case typ != "" && family != "":
		return invalid("Specify either InstanceType or InstanceFamily, not both.")
	case typ != "":
		if _, ok := cpus(typ); !ok {
			return invalid("Invalid value '%s' for InstanceType.", typ)
		}
		h.HostProperties.InstanceType, h.HostProperties.InstanceFamily = typ, strings.SplitN(typ, ".", 2)[0]
	case family != "":
		if !familyName.MatchString(family) {
			return invalid("Invalid value '%s' for InstanceFamily.", family)
		}
		h.HostProperties.InstanceType, h.HostProperties.InstanceFamily = "", family
	}
	return nil
}

func (s *Service) allocateHosts(c *core.Ctx, in *struct {
	AvailabilityZone, InstanceType, InstanceFamily, AutoPlacement, HostRecovery, HostMaintenance string
	Quantity                                                                                     int
	TagSpecifications                                                                            []tagSpec `query:"TagSpecification"`
}) (any, error) {
	switch {
	case in.InstanceType == "" && in.InstanceFamily == "":
		return nil, core.Errorf(400, "MissingParameter", "The request must contain the parameter InstanceType or InstanceFamily")
	case in.Quantity < 1:
		return nil, invalid("Value (%d) for parameter quantity is invalid. Expected a positive integer.", in.Quantity)
	case !slices.Contains(zones(c.Region), in.AvailabilityZone):
		return nil, invalid("Invalid availability zone: [%s]", in.AvailabilityZone)
	}
	for _, v := range []string{in.AutoPlacement, in.HostRecovery, in.HostMaintenance} {
		if v != "" && !onOff.MatchString(v) {
			return nil, invalid("Value (%s) is invalid. Value must be on or off.", v)
		}
	}
	proto := Host{AvailabilityZone: in.AvailabilityZone, AvailabilityZoneId: zoneID(in.AvailabilityZone), AutoPlacement: cmp.Or(in.AutoPlacement, "off"),
		HostRecovery: cmp.Or(in.HostRecovery, "off"), HostMaintenance: cmp.Or(in.HostMaintenance, "off"), State: "available",
		HostProperties: hostProperties{Cores: 48, Sockets: 2, TotalVCpus: 96}}
	if err := proto.setType(in.InstanceType, in.InstanceFamily); err != nil {
		return nil, err
	}
	proto.AvailableCapacity.AvailableVCpus = 96
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	proto.OwnerId = r.Account
	out := &struct {
		Header
		HostIds []string `xml:"hostIdSet>item"`
	}{}
	for range in.Quantity {
		h := proto
		h.HostId, h.AllocationTime = "h-"+core.RandHex(9)[:17], time.Now().UTC().Format(core.ISO)
		r.x().Hosts[h.HostId] = &h
		r.tag(h.HostId, typed(in.TagSpecifications, "dedicated-host"))
		out.HostIds = append(out.HostIds, h.HostId)
	}
	return out, nil
}

func (s *Service) describeHosts(c *core.Ctx, in *struct {
	HostIds []string `query:"HostId"`
	Filters []filter `query:"Filter"`
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	_, _, served := (&Host{}).view()
	if err := checkFilters(in.Filters, served); err != nil {
		return nil, err
	}
	list, err := describe(r, r.x().Hosts, in.HostIds, in.Filters, "InvalidHostID.NotFound", "host ID", (*Host).view)
	return &struct {
		Header
		Hosts []Host `xml:"hostSet>item"`
	}{Hosts: list}, err
}

// hostAnswer is the output of ModifyHosts and ReleaseHosts.
type hostAnswer struct {
	Header
	Successful   []string  `xml:"successful>item"`
	Unsuccessful []failure `xml:"unsuccessful>item"`
}

func (s *Service) modifyHosts(c *core.Ctx, in *struct {
	HostIds                                                                    []string `query:"HostId"`
	AutoPlacement, HostRecovery, HostMaintenance, InstanceType, InstanceFamily string
}) (any, error) {
	for _, v := range []string{in.AutoPlacement, in.HostRecovery, in.HostMaintenance} {
		if v != "" && !onOff.MatchString(v) {
			return nil, invalid("Value (%s) is invalid. Value must be on or off.", v)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.reg(c).x()
	out := &hostAnswer{}
	for _, id := range in.HostIds {
		h := e.Hosts[id]
		if h == nil || h.State != "available" {
			out.Unsuccessful = append(out.Unsuccessful, newFailure(id, "InvalidHostID.NotFound", "The host ID '"+id+"' does not exist"))
			continue
		}
		cp := *h
		if err := cp.setType(in.InstanceType, in.InstanceFamily); err != nil {
			out.Unsuccessful = append(out.Unsuccessful, newFailure(id, "InvalidParameterValue", core.AsError(err).Msg))
			continue
		}
		cp.AutoPlacement, cp.HostRecovery = cmp.Or(in.AutoPlacement, cp.AutoPlacement), cmp.Or(in.HostRecovery, cp.HostRecovery)
		cp.HostMaintenance = cmp.Or(in.HostMaintenance, cp.HostMaintenance)
		*h = cp
		out.Successful = append(out.Successful, id)
	}
	return out, nil
}

func (s *Service) releaseHosts(c *core.Ctx, in *struct {
	HostIds []string `query:"HostId"`
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.reg(c).x()
	out := &hostAnswer{}
	for _, id := range in.HostIds {
		h := e.Hosts[id]
		if h == nil || h.State != "available" {
			out.Unsuccessful = append(out.Unsuccessful, newFailure(id, "InvalidHostID.NotFound", "The host ID '"+id+"' does not exist"))
			continue
		}
		h.State, h.ReleaseTime = "released", time.Now().UTC().Format(core.ISO)
		out.Successful = append(out.Successful, id)
	}
	return out, nil
}
