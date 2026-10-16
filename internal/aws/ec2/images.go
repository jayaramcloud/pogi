package ec2

// CreateImage and DeregisterImage, and the IAM instance profile
// associations. An image made from an instance with a container is a
// docker commit of that container, so instances launched from it start
// from what the container held; DeregisterImage removes it again.

import (
	"context"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// amiRepo is the docker repository of the images CreateImage commits.
const amiRepo = "pogi-ami"

// ImageDevice is a block device of an image. A created image keeps no
// snapshots: pogi has none, so its devices show size and type only.
type ImageDevice struct {
	DeviceName string `xml:"deviceName"`
	Ebs        struct {
		DeleteOnTermination bool   `xml:"deleteOnTermination"`
		VolumeSize          int    `xml:"volumeSize"`
		VolumeType          string `xml:"volumeType"`
		Encrypted           bool   `xml:"encrypted"`
	} `xml:"ebs"`
}

// allImages are the AMIs the region shows: the catalog less what it
// deregistered, plus the images it made; lock held.
func (r *region) allImages() map[string]*Image {
	all := maps.Clone(images)
	for _, id := range r.x().Gone {
		delete(all, id)
	}
	maps.Copy(all, r.x().Images)
	return all
}

// imageFor is regionless imageFor, but finds the images the region made.
func (r *region) imageFor(id string) *Image {
	if img := r.x().Images[id]; img != nil {
		return img
	}
	return imageFor(id)
}

func (s *Service) createImage(c *core.Ctx, in *struct {
	InstanceId, Name, Description string
	NoReboot                      bool
	TagSpecifications             []tagSpec `query:"TagSpecification"`
}) (any, error) {
	if n := len(in.Name); n < 3 || n > 128 {
		return nil, core.Errorf(400, "InvalidAMIName.Malformed", "AMI names must be between 3 and 128 characters long.")
	}
	engine := s.hub.Docker().Available()
	s.mu.Lock()
	r := s.reg(c)
	i := r.Instances[in.InstanceId]
	if i == nil || i.State.Name == "terminated" {
		s.mu.Unlock()
		return nil, core.Errorf(400, "InvalidInstanceID.NotFound", "The instance ID '%s' does not exist", in.InstanceId)
	}
	for _, img := range r.allImages() {
		if img.ImageOwnerId == r.Account && img.Name == in.Name {
			s.mu.Unlock()
			return nil, core.Errorf(400, "InvalidAMIName.Duplicate", "AMI name %s is already in use by AMI %s", in.Name, img.ImageId)
		}
	}
	src := r.imageFor(i.ImageId)
	img := &Image{ImageId: newID("ami"), ImageLocation: r.Account + "/" + in.Name, ImageState: "pending", ImageOwnerId: r.Account,
		CreationDate: time.Now().UTC().Format(core.ISO), ImageType: "machine", Architecture: i.Architecture, Name: in.Name,
		Description: in.Description, RootDeviceType: i.RootDeviceType, RootDeviceName: i.RootDeviceName, VirtualizationType: i.VirtualizationType,
		Hypervisor: i.Hypervisor, EnaSupport: i.EnaSupport, PlatformDetails: i.PlatformDetails, UsageOperation: i.UsageOperation,
		Docker: src.Docker, Keyword: src.Keyword}
	for _, b := range i.BlockDeviceMappings {
		d := ImageDevice{DeviceName: b.DeviceName}
		d.Ebs.DeleteOnTermination = true
		if v := r.Volumes[b.Ebs.VolumeId]; v != nil {
			d.Ebs.VolumeSize, d.Ebs.VolumeType, d.Ebs.Encrypted = v.Size, v.VolumeType, v.Encrypted
		}
		img.BlockDeviceMappings = append(img.BlockDeviceMappings, d)
	}
	r.x().Images[img.ImageId] = img
	r.tag(img.ImageId, typed(in.TagSpecifications, "image"))
	container := ""
	if engine {
		container = i.Container
	}
	s.mu.Unlock()

	var err error
	if container != "" {
		err = s.hub.Docker().Commit(context.Background(), container, amiRepo, img.ImageId)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil {
		delete(s.reg(c).x().Images, img.ImageId)
		return nil, core.Errorf(500, "InternalError", "Could not capture the instance: %v", err)
	}
	if container != "" {
		img.Docker = amiRepo + ":" + img.ImageId
	}
	img.ImageState = "available"
	return &struct {
		Header
		ImageId string `xml:"imageId"`
	}{ImageId: img.ImageId}, nil
}

func (s *Service) deregisterImage(c *core.Ctx, in *struct{ ImageId string }) (any, error) {
	s.mu.Lock()
	r := s.reg(c)
	e := r.x()
	docker := ""
	switch img := e.Images[in.ImageId]; {
	case img != nil:
		delete(e.Images, in.ImageId)
		if strings.HasPrefix(img.Docker, amiRepo+":") {
			docker = img.Docker
		}
	case images[in.ImageId] != nil && !slices.Contains(e.Gone, in.ImageId):
		e.Gone = append(e.Gone, in.ImageId)
	default:
		s.mu.Unlock()
		return nil, core.Errorf(400, "InvalidAMIID.NotFound", "The image id '[%s]' does not exist", in.ImageId)
	}
	delete(r.Tags, in.ImageId)
	s.mu.Unlock()
	if docker != "" {
		if err := s.hub.Docker().RemoveImage(context.Background(), docker); err != nil && s.hub.Docker().Available() {
			s.hub.Log.Warn("ec2: could not remove the image of a deregistered AMI", "image", docker, "err", err)
		}
	}
	return ok(), nil
}

// ---- IAM instance profile associations ----

// IipAssociation links an instance to its IAM instance profile.
type IipAssociation struct {
	AssociationId      string     `xml:"associationId"`
	InstanceId         string     `xml:"instanceId"`
	IamInstanceProfile ProfileRef `xml:"iamInstanceProfile"`
	State              string     `xml:"state"`
	Timestamp          string     `xml:"timestamp"`
}

// syncAssocs makes the associations match the instances: one for each
// profile an instance launched with, and the ended ones disassociated; lock held.
func (r *region) syncAssocs() {
	e := r.x()
	for _, id := range core.SortedKeys(r.Instances) {
		i := r.Instances[id]
		if i.IamInstanceProfile == nil || i.State.Name == "terminated" || i.State.Name == "shutting-down" {
			continue
		}
		if !slices.ContainsFunc(slices.Collect(maps.Values(e.Assocs)), func(a *IipAssociation) bool { return a.InstanceId == id && a.State == "associated" }) {
			a := &IipAssociation{newID("iip-assoc"), id, *i.IamInstanceProfile, "associated", i.LaunchTime}
			e.Assocs[a.AssociationId] = a
		}
	}
	for _, a := range e.Assocs {
		if i := r.Instances[a.InstanceId]; a.State == "associated" && (i == nil || i.State.Name == "terminated" || i.IamInstanceProfile == nil) {
			a.State = "disassociated"
		}
	}
}

// current finds the associated association of an id; lock held.
func (r *region) current(id string) (*IipAssociation, error) {
	r.syncAssocs()
	if a := r.x().Assocs[id]; a != nil && a.State == "associated" {
		return a, nil
	}
	return nil, core.Errorf(400, "InvalidAssociationID.NotFound", "The association ID '%s' does not exist", id)
}

// wear gives an instance a profile, or none; lock held.
func (s *Service) wear(i *Instance, prof *ProfileRef, role string) {
	i.IamInstanceProfile, i.Role = prof, role
	delete(s.creds, i.InstanceId)
	delete(s.creds, i.InstanceId+"/identity")
}

type iipOutput struct {
	Header
	Association IipAssociation `xml:"iamInstanceProfileAssociation"`
}

func (s *Service) associateIamInstanceProfile(c *core.Ctx, in *struct {
	InstanceId         string
	IamInstanceProfile *struct{ Arn, Name string }
}) (any, error) {
	prof, role, err := s.profile(c, in.IamInstanceProfile)
	if err != nil {
		return nil, err
	}
	if prof == nil {
		return nil, core.Errorf(400, "MissingParameter", "The request must contain the parameter IamInstanceProfile")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	i := r.Instances[in.InstanceId]
	switch {
	case i == nil || i.State.Name == "terminated":
		return nil, core.Errorf(400, "InvalidInstanceID.NotFound", "The instance ID '%s' does not exist", in.InstanceId)
	case i.State.Name == "shutting-down":
		return nil, incorrect(i, "associate a profile with")
	case i.IamInstanceProfile != nil:
		return nil, core.Errorf(400, "IncorrectState", "There is an existing association for instance %s", i.InstanceId)
	}
	a := &IipAssociation{newID("iip-assoc"), i.InstanceId, *prof, "associated", time.Now().UTC().Format(core.ISO)}
	r.x().Assocs[a.AssociationId] = a
	s.wear(i, prof, role)
	out := *a
	out.State = "associating"
	return &iipOutput{Association: out}, nil
}

func (s *Service) replaceIamInstanceProfile(c *core.Ctx, in *struct {
	AssociationId      string
	IamInstanceProfile *struct{ Arn, Name string }
}) (any, error) {
	prof, role, err := s.profile(c, in.IamInstanceProfile)
	if err != nil {
		return nil, err
	}
	if prof == nil {
		return nil, core.Errorf(400, "MissingParameter", "The request must contain the parameter IamInstanceProfile")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	old, err := r.current(in.AssociationId)
	if err != nil {
		return nil, err
	}
	old.State = "disassociated"
	a := &IipAssociation{newID("iip-assoc"), old.InstanceId, *prof, "associated", time.Now().UTC().Format(core.ISO)}
	r.x().Assocs[a.AssociationId] = a
	s.wear(r.Instances[a.InstanceId], prof, role)
	out := *a
	out.State = "associating"
	return &iipOutput{Association: out}, nil
}

func (s *Service) disassociateIamInstanceProfile(c *core.Ctx, in *struct{ AssociationId string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	a, err := r.current(in.AssociationId)
	if err != nil {
		return nil, err
	}
	a.State = "disassociated"
	s.wear(r.Instances[a.InstanceId], nil, "")
	out := *a
	out.State = "disassociating"
	return &iipOutput{Association: out}, nil
}

func (s *Service) describeIamInstanceProfileAssociations(c *core.Ctx, in *struct {
	AssociationIds []string `query:"AssociationId"`
	Filters        []filter `query:"Filter"`
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	r.syncAssocs()
	e := r.x()
	if err := checkFilters(in.Filters, map[string][]string{"instance-id": nil, "state": nil}); err != nil {
		return nil, err
	}
	for _, id := range in.AssociationIds {
		if e.Assocs[id] == nil {
			return nil, core.Errorf(400, "InvalidAssociationID.NotFound", "The association ID '%s' does not exist", id)
		}
	}
	out := &struct {
		Header
		Associations []IipAssociation `xml:"iamInstanceProfileAssociationSet>item"`
	}{}
	for _, id := range core.SortedKeys(e.Assocs) {
		a := e.Assocs[id]
		if (len(in.AssociationIds) == 0 || slices.Contains(in.AssociationIds, id)) &&
			match(in.Filters, map[string][]string{"instance-id": {a.InstanceId}, "state": {a.State}}, nil) {
			out.Associations = append(out.Associations, *a)
		}
	}
	return out, nil
}
