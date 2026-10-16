package ec2

// DHCP options sets, and the volume change that CloudFormation's AWS::EC2::Volume needs.

import (
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// DhcpOptions is a DHCP options set.
type DhcpOptions struct {
	DhcpOptionsId string       `xml:"dhcpOptionsId"`
	OwnerId       string       `xml:"ownerId"`
	Configuration []dhcpConfig `xml:"dhcpConfigurationSet>item"`
	TagSet        []Tag        `xml:"tagSet>item" json:"-"`
}

type dhcpConfig struct {
	Key    string      `xml:"key"`
	Values []dhcpValue `xml:"valueSet>item"`
}

type dhcpValue struct {
	Value string `xml:"value"`
}

func (d *DhcpOptions) view() (string, *[]Tag, map[string][]string) {
	attrs := map[string][]string{"dhcp-options-id": {d.DhcpOptionsId}, "owner-id": {d.OwnerId}}
	for _, c := range d.Configuration {
		for _, v := range c.Values {
			attrs["key"] = append(attrs["key"], c.Key)
			attrs["value"] = append(attrs["value"], v.Value)
		}
	}
	return d.DhcpOptionsId, &d.TagSet, attrs
}

func (s *Service) createDhcpOptions(c *core.Ctx, in *struct {
	Configs []struct {
		Key    string
		Values []string `query:"Value"`
	} `query:"DhcpConfiguration"`
	TagSpecs []tagSpec `query:"TagSpecification"`
}) (any, error) {
	if len(in.Configs) == 0 {
		return nil, core.Errorf(400, "MissingParameter", "The request must contain the parameter dhcpConfiguration")
	}
	d := &DhcpOptions{DhcpOptionsId: newID("dopt")}
	for _, cf := range in.Configs {
		n := dhcpConfig{Key: cf.Key}
		for _, v := range cf.Values {
			n.Values = append(n.Values, dhcpValue{v})
		}
		d.Configuration = append(d.Configuration, n)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	d.OwnerId = r.Account
	r.x().Dhcp[d.DhcpOptionsId] = d
	r.tag(d.DhcpOptionsId, typed(in.TagSpecs, "dhcp-options"))
	return &struct {
		Header
		Options DhcpOptions `xml:"dhcpOptions"`
	}{Options: withTags(r, d, (*DhcpOptions).view)}, nil
}

func (s *Service) describeDhcpOptions(c *core.Ctx, in *struct {
	Ids     []string `query:"DhcpOptionsId"`
	Filters []filter `query:"Filter"`
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	list, err := describe(r, r.x().Dhcp, in.Ids, in.Filters, "InvalidDhcpOptionID.NotFound", "dhcp options ID", (*DhcpOptions).view)
	return &struct {
		Header
		Options []DhcpOptions `xml:"dhcpOptionsSet>item"`
	}{Options: list}, err
}

func (s *Service) deleteDhcpOptions(c *core.Ctx, in *struct{ DhcpOptionsId string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	if r.x().Dhcp[in.DhcpOptionsId] == nil {
		return nil, core.Errorf(400, "InvalidDhcpOptionID.NotFound", "The dhcp options ID '%s' does not exist", in.DhcpOptionsId)
	}
	for _, v := range r.Vpcs {
		if v.DhcpOptionsId == in.DhcpOptionsId {
			return nil, core.Errorf(400, "DependencyViolation", "The dhcpOptions '%s' has dependencies and cannot be deleted.", in.DhcpOptionsId)
		}
	}
	delete(r.x().Dhcp, in.DhcpOptionsId)
	delete(r.Tags, in.DhcpOptionsId)
	return ok(), nil
}

// associateDhcpOptions gives a VPC a set; "default" takes it away.
func (s *Service) associateDhcpOptions(c *core.Ctx, in *struct{ DhcpOptionsId, VpcId string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	v, err := r.vpc(in.VpcId)
	if err != nil {
		return nil, err
	}
	if in.DhcpOptionsId != "default" && r.x().Dhcp[in.DhcpOptionsId] == nil {
		return nil, core.Errorf(400, "InvalidDhcpOptionID.NotFound", "The dhcp options ID '%s' does not exist", in.DhcpOptionsId)
	}
	v.DhcpOptionsId = in.DhcpOptionsId
	return ok(), nil
}

// modifyVolume changes the size (it can only grow), type, IOPS or throughput of a volume.
func (s *Service) modifyVolume(c *core.Ctx, in *struct {
	VolumeId, VolumeType   string
	Size, Iops, Throughput int
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, err := s.reg(c).volume(in.VolumeId)
	if err != nil {
		return nil, err
	}
	if in.Size != 0 && in.Size < v.Size {
		return nil, invalid("Invalid value for size: the new size (%d) is smaller than the current size (%d).", in.Size, v.Size)
	}
	if in.Size != 0 {
		v.Size = in.Size
	}
	if in.VolumeType != "" {
		v.VolumeType = in.VolumeType
	}
	if in.Iops != 0 {
		v.Iops = in.Iops
	}
	if in.Throughput != 0 {
		v.Throughput = in.Throughput
	}
	now := time.Now().UTC().Format(core.ISO)
	return &struct {
		Header
		Mod volumeMod `xml:"volumeModification"`
	}{Mod: volumeMod{v.VolumeId, "completed", v.Size, v.VolumeType, now, now}}, nil
}

type volumeMod struct {
	VolumeId          string `xml:"volumeId"`
	ModificationState string `xml:"modificationState"`
	TargetSize        int    `xml:"targetSize"`
	TargetVolumeType  string `xml:"targetVolumeType"`
	StartTime         string `xml:"startTime"`
	EndTime           string `xml:"endTime"`
}
