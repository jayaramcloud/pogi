package bigtable

import (
	"context"
	"regexp"
	"strings"

	"cloud.google.com/go/bigtable/admin/apiv2/adminpb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/gcp/gapi"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// instance is an instance with its clusters and app profiles, by full name.
type instance struct {
	p        *adminpb.Instance
	clusters map[string]*adminpb.Cluster
	profiles map[string]*adminpb.AppProfile
}

var (
	// idRE is the form of instance, cluster and app profile ids: a letter, then letters, digits and hyphens.
	idRE = regexp.MustCompile(`^[a-z][-a-z0-9]*[a-z0-9]$`)
	// parentRE is projects/<p>.
	parentRE = regexp.MustCompile(`^projects/[^/]+$`)
	// instRE is projects/<p>/instances/<i>.
	instRE = regexp.MustCompile(`^projects/[^/]+/instances/[^/]+$`)
)

func checkID(kind, id string, min, max int) error {
	if len(id) < min || len(id) > max || !idRE.MatchString(id) {
		return gapi.Invalid("%s id %q is invalid: use %d to %d lowercase letters, digits and hyphens, starting with a letter", kind, id, min, max)
	}
	return nil
}

// find returns the instance a name (or a name below it) is in.
func (s *Service) find(name string) (*instance, string, error) {
	p := strings.Split(name, "/")
	if len(p) < 4 || p[0] != "projects" || p[2] != "instances" {
		return nil, "", gapi.Invalid("Invalid name %q: want projects/<project>/instances/<instance>", name)
	}
	n := strings.Join(p[:4], "/")
	in := s.inst[n]
	if in == nil {
		return nil, n, gapi.NotFound("Instance %s not found.", n)
	}
	return in, n, nil
}

func (s *Service) done(parent string, meta, resp proto.Message) *longrunningpb.Operation {
	return s.ops.Done(parent, meta, resp)
}

func (s *Service) CreateInstance(_ context.Context, r *adminpb.CreateInstanceRequest) (*longrunningpb.Operation, error) {
	if !parentRE.MatchString(r.Parent) {
		return nil, gapi.Invalid("Invalid parent %q: want projects/<project>", r.Parent)
	}
	if err := checkID("instance", r.InstanceId, 6, 33); err != nil {
		return nil, err
	}
	if r.Instance == nil || r.Instance.DisplayName == "" {
		return nil, gapi.Invalid("instance.display_name is required")
	}
	if len(r.Clusters) == 0 {
		return nil, gapi.Invalid("at least one cluster is required")
	}
	name := r.Parent + "/instances/" + r.InstanceId
	in := &instance{p: proto.Clone(r.Instance).(*adminpb.Instance), clusters: map[string]*adminpb.Cluster{}, profiles: map[string]*adminpb.AppProfile{}}
	in.p.Name, in.p.State, in.p.CreateTime = name, adminpb.Instance_READY, timestamppb.Now()
	if in.p.Type == adminpb.Instance_TYPE_UNSPECIFIED {
		in.p.Type = adminpb.Instance_PRODUCTION
	}
	for _, id := range core.SortedKeys(r.Clusters) {
		c, err := newCluster(name, id, r.Clusters[id])
		if err != nil {
			return nil, err
		}
		in.clusters[c.Name] = c
	}
	in.profiles[name+"/appProfiles/default"] = &adminpb.AppProfile{Name: name + "/appProfiles/default", Description: "Default application profile.",
		RoutingPolicy: &adminpb.AppProfile_MultiClusterRoutingUseAny_{MultiClusterRoutingUseAny: &adminpb.AppProfile_MultiClusterRoutingUseAny{}}}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inst[name] != nil {
		return nil, gapi.Exists("Instance %s already exists.", name)
	}
	s.inst[name] = in
	meta := &adminpb.CreateInstanceMetadata{OriginalRequest: r, RequestTime: timestamppb.Now(), FinishTime: timestamppb.Now()}
	return s.done(name, meta, in.view()), nil
}

// newCluster checks and completes a cluster of instance. SSD and one node are what the API fills in.
func newCluster(instance, id string, c *adminpb.Cluster) (*adminpb.Cluster, error) {
	if err := checkID("cluster", id, 6, 30); err != nil {
		return nil, err
	}
	if c == nil || c.Location == "" {
		return nil, gapi.Invalid("cluster %s: location is required", id)
	}
	c = proto.Clone(c).(*adminpb.Cluster)
	c.Name, c.State = instance+"/clusters/"+id, adminpb.Cluster_READY
	if c.DefaultStorageType == adminpb.StorageType_STORAGE_TYPE_UNSPECIFIED {
		c.DefaultStorageType = adminpb.StorageType_SSD
	}
	if c.ServeNodes == 0 && c.GetClusterConfig() == nil {
		c.ServeNodes = 1
	}
	return c, nil
}

func (in *instance) view() *adminpb.Instance { return proto.Clone(in.p).(*adminpb.Instance) }

func (s *Service) GetInstance(_ context.Context, r *adminpb.GetInstanceRequest) (*adminpb.Instance, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !instRE.MatchString(r.Name) {
		return nil, gapi.Invalid("Invalid name %q: want projects/<project>/instances/<instance>", r.Name)
	}
	in, _, err := s.find(r.Name)
	if err != nil {
		return nil, err
	}
	return in.view(), nil
}

func (s *Service) ListInstances(_ context.Context, r *adminpb.ListInstancesRequest) (*adminpb.ListInstancesResponse, error) {
	if !parentRE.MatchString(r.Parent) {
		return nil, gapi.Invalid("Invalid parent %q: want projects/<project>", r.Parent)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := &adminpb.ListInstancesResponse{}
	for _, n := range core.SortedKeys(s.inst) {
		if strings.HasPrefix(n, r.Parent+"/") {
			out.Instances = append(out.Instances, s.inst[n].view())
		}
	}
	return out, nil
}

// UpdateInstance replaces display name, type and labels (the PUT form).
func (s *Service) UpdateInstance(_ context.Context, r *adminpb.Instance) (*adminpb.Instance, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	in, _, err := s.find(r.Name)
	if err != nil {
		return nil, err
	}
	if err := gapi.Update(in.p, r, nil, "display_name", "type", "labels"); err != nil {
		return nil, err
	}
	return in.view(), nil
}

func (s *Service) PartialUpdateInstance(_ context.Context, r *adminpb.PartialUpdateInstanceRequest) (*longrunningpb.Operation, error) {
	if r.Instance == nil || r.UpdateMask == nil || len(r.UpdateMask.Paths) == 0 {
		return nil, gapi.Invalid("instance and update_mask are required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	in, name, err := s.find(r.Instance.Name)
	if err != nil {
		return nil, err
	}
	if err := gapi.Update(in.p, r.Instance, r.UpdateMask.Paths, "display_name", "type", "labels", "edition"); err != nil {
		return nil, err
	}
	return s.done(name, &adminpb.UpdateInstanceMetadata{OriginalRequest: r, RequestTime: timestamppb.Now(), FinishTime: timestamppb.Now()}, in.view()), nil
}

func (s *Service) DeleteInstance(ctx context.Context, r *adminpb.DeleteInstanceRequest) (*emptypb.Empty, error) {
	s.mu.Lock()
	_, name, err := s.find(r.Name)
	if err == nil {
		delete(s.inst, name)
	}
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	s.dropTables(ctx, name)
	return &emptypb.Empty{}, nil
}

func (s *Service) CreateCluster(_ context.Context, r *adminpb.CreateClusterRequest) (*longrunningpb.Operation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	in, name, err := s.find(r.Parent)
	if err != nil {
		return nil, err
	}
	c, err := newCluster(name, r.ClusterId, r.Cluster)
	if err != nil {
		return nil, err
	}
	if in.clusters[c.Name] != nil {
		return nil, gapi.Exists("Cluster %s already exists.", c.Name)
	}
	in.clusters[c.Name] = c
	return s.done(name, &adminpb.CreateClusterMetadata{OriginalRequest: r, RequestTime: timestamppb.Now(), FinishTime: timestamppb.Now()}, proto.Clone(c)), nil
}

func (s *Service) cluster(name string) (*adminpb.Cluster, error) {
	in, _, err := s.find(name)
	if err != nil {
		return nil, err
	}
	c := in.clusters[name]
	if c == nil {
		return nil, gapi.NotFound("Cluster %s not found.", name)
	}
	return c, nil
}

func (s *Service) GetCluster(_ context.Context, r *adminpb.GetClusterRequest) (*adminpb.Cluster, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, err := s.cluster(r.Name)
	if err != nil {
		return nil, err
	}
	return proto.Clone(c).(*adminpb.Cluster), nil
}

func (s *Service) ListClusters(_ context.Context, r *adminpb.ListClustersRequest) (*adminpb.ListClustersResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	in, _, err := s.find(r.Parent)
	if err != nil {
		return nil, err
	}
	out := &adminpb.ListClustersResponse{}
	for _, n := range core.SortedKeys(in.clusters) {
		out.Clusters = append(out.Clusters, proto.Clone(in.clusters[n]).(*adminpb.Cluster))
	}
	return out, nil
}

// UpdateCluster changes the number of nodes or the autoscaling of a cluster.
func (s *Service) UpdateCluster(_ context.Context, r *adminpb.Cluster) (*longrunningpb.Operation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, err := s.cluster(r.Name)
	if err != nil {
		return nil, err
	}
	c.ServeNodes = r.ServeNodes
	c.Config = r.Config
	instName := r.Name[:strings.Index(r.Name, "/clusters/")]
	return s.done(instName, &adminpb.UpdateClusterMetadata{OriginalRequest: r, RequestTime: timestamppb.Now(), FinishTime: timestamppb.Now()}, proto.Clone(c)), nil
}

// PartialUpdateCluster is what the client's UpdateCluster calls: serve_nodes and cluster_config by mask.
func (s *Service) PartialUpdateCluster(_ context.Context, r *adminpb.PartialUpdateClusterRequest) (*longrunningpb.Operation, error) {
	if r.Cluster == nil || len(r.GetUpdateMask().GetPaths()) == 0 {
		return nil, gapi.Invalid("cluster and update_mask are required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	c, err := s.cluster(r.Cluster.Name)
	if err != nil {
		return nil, err
	}
	if err := gapi.Update(c, r.Cluster, r.UpdateMask.Paths, "serve_nodes", "cluster_config"); err != nil {
		return nil, err
	}
	instName := r.Cluster.Name[:strings.Index(r.Cluster.Name, "/clusters/")]
	return s.done(instName, &adminpb.PartialUpdateClusterMetadata{OriginalRequest: r, RequestTime: timestamppb.Now(), FinishTime: timestamppb.Now()}, proto.Clone(c)), nil
}

func (s *Service) DeleteCluster(_ context.Context, r *adminpb.DeleteClusterRequest) (*emptypb.Empty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.cluster(r.Name); err != nil {
		return nil, err
	}
	in, _, _ := s.find(r.Name)
	delete(in.clusters, r.Name)
	return &emptypb.Empty{}, nil
}

func (s *Service) CreateAppProfile(_ context.Context, r *adminpb.CreateAppProfileRequest) (*adminpb.AppProfile, error) {
	if r.AppProfileId == "" || r.AppProfile == nil {
		return nil, gapi.Invalid("app_profile_id and app_profile are required")
	}
	if r.AppProfile.RoutingPolicy == nil {
		return nil, gapi.Invalid("app_profile: a routing policy is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	in, name, err := s.find(r.Parent)
	if err != nil {
		return nil, err
	}
	p := proto.Clone(r.AppProfile).(*adminpb.AppProfile)
	p.Name = name + "/appProfiles/" + r.AppProfileId
	if in.profiles[p.Name] != nil {
		return nil, gapi.Exists("App profile %s already exists.", p.Name)
	}
	in.profiles[p.Name] = p
	return proto.Clone(p).(*adminpb.AppProfile), nil
}

func (s *Service) profile(name string) (*instance, *adminpb.AppProfile, error) {
	in, _, err := s.find(name)
	if err != nil {
		return nil, nil, err
	}
	p := in.profiles[name]
	if p == nil {
		return nil, nil, gapi.NotFound("App profile %s not found.", name)
	}
	return in, p, nil
}

func (s *Service) GetAppProfile(_ context.Context, r *adminpb.GetAppProfileRequest) (*adminpb.AppProfile, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, p, err := s.profile(r.Name)
	if err != nil {
		return nil, err
	}
	return proto.Clone(p).(*adminpb.AppProfile), nil
}

func (s *Service) ListAppProfiles(_ context.Context, r *adminpb.ListAppProfilesRequest) (*adminpb.ListAppProfilesResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	in, _, err := s.find(r.Parent)
	if err != nil {
		return nil, err
	}
	out := &adminpb.ListAppProfilesResponse{}
	for _, n := range core.SortedKeys(in.profiles) {
		out.AppProfiles = append(out.AppProfiles, proto.Clone(in.profiles[n]).(*adminpb.AppProfile))
	}
	return out, nil
}

func (s *Service) UpdateAppProfile(_ context.Context, r *adminpb.UpdateAppProfileRequest) (*longrunningpb.Operation, error) {
	if r.AppProfile == nil {
		return nil, gapi.Invalid("app_profile is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, p, err := s.profile(r.AppProfile.Name)
	if err != nil {
		return nil, err
	}
	paths := r.GetUpdateMask().GetPaths()
	if len(paths) == 0 {
		return nil, gapi.Invalid("update_mask is required")
	}
	for i, x := range paths { // the routing policy is a oneof: its members are written under their own names
		if strings.HasPrefix(x, "multi_cluster_routing") || strings.HasPrefix(x, "single_cluster_routing") {
			paths[i] = "routing_policy"
		}
	}
	if err := updateProfile(p, r.AppProfile, paths); err != nil {
		return nil, err
	}
	name := r.AppProfile.Name[:strings.Index(r.AppProfile.Name, "/appProfiles/")]
	return s.done(name, &adminpb.UpdateAppProfileMetadata{}, proto.Clone(p)), nil
}

// updateProfile copies the masked fields; "routing_policy" stands for whichever policy src has.
func updateProfile(dst, src *adminpb.AppProfile, paths []string) error {
	for _, x := range paths {
		switch x {
		case "routing_policy":
			dst.RoutingPolicy = src.RoutingPolicy
		case "description":
			dst.Description = src.Description
		case "priority", "standard_isolation", "data_boost_isolation_read_only":
			dst.Isolation = src.Isolation
		default:
			return gapi.Invalid("Invalid update_mask provided in the request: %q", x)
		}
	}
	return nil
}

func (s *Service) DeleteAppProfile(_ context.Context, r *adminpb.DeleteAppProfileRequest) (*emptypb.Empty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	in, _, err := s.profile(r.Name)
	if err != nil {
		return nil, err
	}
	delete(in.profiles, r.Name)
	return &emptypb.Empty{}, nil
}
