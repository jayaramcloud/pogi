// Package gke emulates Google Kubernetes Engine (container.googleapis.com v1
// and v1beta1) over gRPC and its REST mirror: clusters, node pools and GKE's
// own Operation resource. With a container engine every cluster is a real
// Kubernetes API server, a k3s container (the runner EKS uses), and the
// cluster is PROVISIONING, its operation RUNNING, until the node is Ready.
// Node pools are metadata. Without an engine the API answers from metadata,
// clusters are RUNNING at once and their status_message says so.
//
// The API server accepts the bearer tokens of pogi's own Google sign-in
// (ya29.pogi-... access tokens, as `gke-gcloud-auth-plugin` fetches them) and
// grants every holder cluster-admin. GET /_gcp/gke/kubeconfig/<project>/<location>/<cluster>
// (`pogi kubeconfig gke`) answers a kubeconfig with a long-lived one of them.
//
// A cluster's endpoint is host:port, not Google's bare IP: clients prepend
// https:// either way.
package gke

import (
	"cmp"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/container/apiv1/containerpb"
	"github.com/nrndev/pogi/internal/aws/eks"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/docker"
	"github.com/nrndev/pogi/internal/gcp/gapi"
	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

const (
	noDocker = "Docker is not available: the API works, but no Kubernetes API server runs behind this cluster"
	// ponytail: one version, the k3s one; a requested version is accepted and reported as this one
	version  = "1.35.5-gke.1000"
	selfBase = "https://container.googleapis.com/v1/"
	scope    = "https://www.googleapis.com/auth/cloud-platform"
)

var (
	parentRE  = regexp.MustCompile(`^projects/([^/]+)/locations/([^/]+)$`)
	nameRE    = regexp.MustCompile(`^projects/([^/]+)/locations/([^/]+)/clusters/([^/]+)(?:/nodePools/([^/]+))?$`)
	opNameRE  = regexp.MustCompile(`^projects/([^/]+)/locations/([^/]+)/operations/([^/]+)$`)
	clusterID = regexp.MustCompile(`^[a-z]([-a-z0-9]{0,38}[a-z0-9])?$`)
	zoneRE    = regexp.MustCompile(`-[a-z]$`)
)

// Service is the GKE emulator.
type Service struct {
	containerpb.UnimplementedClusterManagerServer
	hub  *core.Hub
	rest *gapi.REST
	ctx  context.Context // k3s work; ends at Close
	stop context.CancelFunc

	mu sync.Mutex
	st state
}

type state struct {
	clusters map[string]*cluster               // project/location/name
	ops      map[string]*containerpb.Operation // project/location/operation
}

type cluster struct {
	p         *containerpb.Cluster
	container string // k3s; empty: metadata only
	token     string // k3s server token, never shown
	cancel    context.CancelFunc
}

// New creates the GKE service.
func New(h *core.Hub) *Service {
	s := &Service{hub: h, st: state{clusters: map[string]*cluster{}, ops: map[string]*containerpb.Operation{}}}
	s.ctx, s.stop = context.WithCancel(context.Background())
	s.rest = &gapi.REST{Versions: []string{"v1", "v1beta1"}, Collections: []string{"clusters", "operations"}, Desc: &containerpb.ClusterManager_ServiceDesc, Impl: s,
		Routes: map[string]gapi.Route{
			"GET locations/*/clusters":                        {Method: "ListClusters", Field: "parent"},
			"POST locations/*/clusters":                       {Method: "CreateCluster", Field: "parent"},
			"GET locations/*/clusters/*":                      {Method: "GetCluster", Field: "name"},
			"PUT locations/*/clusters/*":                      {Method: "UpdateCluster", Field: "name"},
			"DELETE locations/*/clusters/*":                   {Method: "DeleteCluster", Field: "name"},
			"POST locations/*/clusters/*:setResourceLabels":   {Method: "SetLabels", Field: "name"},
			"GET locations/*/clusters/*/nodePools":            {Method: "ListNodePools", Field: "parent"},
			"POST locations/*/clusters/*/nodePools":           {Method: "CreateNodePool", Field: "parent"},
			"GET locations/*/clusters/*/nodePools/*":          {Method: "GetNodePool", Field: "name"},
			"PUT locations/*/clusters/*/nodePools/*":          {Method: "UpdateNodePool", Field: "name"},
			"DELETE locations/*/clusters/*/nodePools/*":       {Method: "DeleteNodePool", Field: "name"},
			"POST locations/*/clusters/*/nodePools/*:setSize": {Method: "SetNodePoolSize", Field: "name"},
			"GET locations/*/operations":                      {Method: "ListOperations", Field: "parent"},
			"GET locations/*/operations/*":                    {Method: "GetOperation", Field: "name"},
			"POST locations/*/operations/*:cancel":            {Method: "CancelOperation", Field: "name"},
		}}
	h.Mount("/_gcp/gke/authn/", http.HandlerFunc(s.authn))
	h.Mount("/_gcp/gke/kubeconfig/", http.HandlerFunc(s.kubeconfig))
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "container", Cloud: core.GCP, Title: "Kubernetes Engine", Match: s.match,
		Targets: []string{"google.container.v1.ClusterManager"}}
}

func (s *Service) RegisterGRPC(g *grpc.Server) { containerpb.RegisterClusterManagerServer(g, s) }

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.rest.ServeHTTP(w, r) }

// match claims the REST calls that are GKE's. Managed Kafka has clusters at the
// same paths (and so does every service that follows Google's resource names),
// so a call on a cluster is claimed only when it can only be GKE's: a node pool,
// a PUT, a :verb, a create without ?clusterId, a call on a GKE cluster, or the
// list of a location that has one (of any location, "-", when the project has one).
// Operations are claimed when they are GKE's.
// ponytail: a GKE list in a location with no GKE cluster yet goes to Managed Kafka (an empty list either way).
func (s *Service) match(r *http.Request) bool {
	project, parts, ok := s.rest.Split(r.URL.Path)
	if !ok || len(parts) < 3 || parts[0] != "locations" {
		return false
	}
	loc, rest := parts[1], parts[3:]
	s.mu.Lock()
	defer s.mu.Unlock()
	switch parts[2] {
	case "operations":
		name, _, _ := strings.Cut(strings.Join(rest, "/"), ":")
		return s.st.ops[project+"/"+loc+"/"+name] != nil
	case "clusters":
		switch {
		case len(rest) == 0 && r.Method == http.MethodPost:
			return !r.URL.Query().Has("clusterId")
		case len(rest) == 0:
			prefix := project + "/" + loc + "/"
			if loc == "-" { // gcloud container clusters list without --location
				prefix = project + "/"
			}
			return r.Method == http.MethodGet && slices.ContainsFunc(core.SortedKeys(s.st.clusters), func(k string) bool { return strings.HasPrefix(k, prefix) })
		case len(rest) > 1:
			return rest[1] == "nodePools"
		}
		name, verb, _ := strings.Cut(rest[0], ":")
		return verb != "" || r.Method == http.MethodPut || s.st.clusters[project+"/"+loc+"/"+name] != nil
	}
	return false
}

// ---- helpers ----

func stamp() string { return time.Now().UTC().Format("2006-01-02T15:04:05+00:00") }

func clusterName(project, loc, name string) string {
	return "projects/" + project + "/locations/" + loc + "/clusters/" + name
}

// find resolves a cluster's (or node pool's) name. Lock held.
func (s *Service) find(name string) (key string, cl *cluster, pool string, err error) {
	m := nameRE.FindStringSubmatch(name)
	if m == nil {
		return "", nil, "", gapi.Invalid("name must look like projects/<project>/locations/<location>/clusters/<cluster>")
	}
	key = m[1] + "/" + m[2] + "/" + m[3]
	if cl = s.st.clusters[key]; cl == nil {
		return "", nil, "", gapi.NotFound("Not found: %s.", clusterName(m[1], m[2], m[3]))
	}
	return key, cl, m[4], nil
}

func (cl *cluster) pool(name string) *containerpb.NodePool {
	for _, np := range cl.p.NodePools {
		if np.Name == name {
			return np
		}
	}
	return nil
}

// count is the nodes of the cluster: those its pools ask for.
func (cl *cluster) count() {
	cl.p.CurrentNodeCount = 0
	for _, np := range cl.p.NodePools {
		cl.p.CurrentNodeCount += np.InitialNodeCount
	}
}

// newOp records an operation on target (a path below /v1/); it is DONE unless running. Lock held.
func (s *Service) newOp(project, loc string, typ containerpb.Operation_Type, target string, running bool) (key string, o *containerpb.Operation) {
	name := "operation-" + strconv.FormatInt(time.Now().UnixMilli(), 10) + "-" + core.RandHex(4)
	o = &containerpb.Operation{Name: name, Zone: loc, Location: loc, OperationType: typ, Status: containerpb.Operation_DONE, StartTime: stamp(),
		SelfLink: selfBase + "projects/" + project + "/locations/" + loc + "/operations/" + name, TargetLink: selfBase + target}
	if running {
		o.Status = containerpb.Operation_RUNNING
	} else {
		o.EndTime = o.StartTime
	}
	key = project + "/" + loc + "/" + name
	s.st.ops[key] = o
	return key, o
}

// finish ends a running operation, failed when err is set. Lock held.
func (s *Service) finish(key string, err error) {
	if o := s.st.ops[key]; o != nil && o.Status != containerpb.Operation_DONE {
		o.Status, o.EndTime = containerpb.Operation_DONE, stamp()
		if err != nil {
			o.StatusMessage, o.Error = err.Error(), &spb.Status{Code: int32(codes.Unknown), Message: err.Error()}
		}
	}
}

func cloneOp(o *containerpb.Operation) *containerpb.Operation {
	return proto.Clone(o).(*containerpb.Operation)
}

// done runs an operation that has already finished.
func (s *Service) done(project, loc string, typ containerpb.Operation_Type, target string) *containerpb.Operation {
	_, o := s.newOp(project, loc, typ, target, false)
	return cloneOp(o)
}

// ---- clusters ----

func (s *Service) CreateCluster(_ context.Context, r *containerpb.CreateClusterRequest) (*containerpb.Operation, error) {
	m := parentRE.FindStringSubmatch(r.Parent)
	if m == nil {
		return nil, gapi.Invalid("parent must look like projects/<project>/locations/<location>")
	}
	project, loc := m[1], m[2]
	if r.Cluster == nil || !clusterID.MatchString(r.Cluster.Name) {
		return nil, gapi.Invalid("Invalid cluster name: it must be 1-40 lowercase letters, digits or hyphens, starting with a letter.")
	}
	p := proto.Clone(r.Cluster).(*containerpb.Cluster)
	key := project + "/" + loc + "/" + p.Name
	target := "projects/" + project + "/locations/" + loc + "/clusters/" + p.Name
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.st.clusters[key] != nil {
		return nil, gapi.Exists("Already exists: %s.", clusterName(project, loc, p.Name))
	}
	p.SelfLink, p.Location, p.Id, p.CreateTime = selfBase+target, loc, core.NewID(), stamp()
	if zoneRE.MatchString(loc) {
		p.Zone = loc
	}
	p.InitialClusterVersion, p.CurrentMasterVersion, p.CurrentNodeVersion = version, version, version
	p.Network, p.Subnetwork = cmp.Or(p.Network, "default"), cmp.Or(p.Subnetwork, "default")
	p.ClusterIpv4Cidr, p.ServicesIpv4Cidr, p.NodeIpv4CidrSize = "10.108.0.0/14", "34.118.224.0/20", 24
	p.LoggingService, p.MonitoringService = cmp.Or(p.LoggingService, "logging.googleapis.com/kubernetes"), cmp.Or(p.MonitoringService, "monitoring.googleapis.com/kubernetes")
	if len(p.Locations) == 0 {
		p.Locations = []string{loc}
		if !zoneRE.MatchString(loc) {
			p.Locations = []string{loc + "-a", loc + "-b", loc + "-c"}
		}
	}
	if p.MasterAuth == nil {
		p.MasterAuth = &containerpb.MasterAuth{}
	}
	p.LabelFingerprint = core.RandHex(4)
	if len(p.NodePools) == 0 && p.InitialNodeCount > 0 && !p.Autopilot.GetEnabled() {
		p.NodePools = []*containerpb.NodePool{{Name: "default-pool", Config: p.NodeConfig, InitialNodeCount: p.InitialNodeCount}}
	}
	for _, np := range p.NodePools {
		fillPool(np, target, containerpb.NodePool_PROVISIONING)
	}
	cl := &cluster{p: p}
	cl.count()
	s.st.clusters[key] = cl
	if !s.hub.Docker().Available() {
		p.Status, p.StatusMessage = containerpb.Cluster_RUNNING, noDocker
		for _, np := range p.NodePools {
			np.Status = containerpb.NodePool_RUNNING
		}
		s.hub.Log.Warn(noDocker, "service", "container", "cluster", p.Name)
		return s.done(project, loc, containerpb.Operation_CREATE_CLUSTER, target), nil
	}
	cl.container, cl.token = docker.Name("gke", project+"-"+loc+"-"+p.Name), core.RandHex(16)
	opKey, op := s.newOp(project, loc, containerpb.Operation_CREATE_CLUSTER, target, true)
	s.boot(key, cl, false, opKey)
	return cloneOp(op), nil
}

// fillPool gives a node pool what GKE adds to it.
func fillPool(np *containerpb.NodePool, cluster string, st containerpb.NodePool_Status) {
	np.Name = cmp.Or(np.Name, "default-pool")
	np.SelfLink, np.Version, np.Status = selfBase+cluster+"/nodePools/"+np.Name, version, st
	np.InitialNodeCount = cmp.Or(np.InitialNodeCount, 1)
	if np.Config == nil {
		np.Config = &containerpb.NodeConfig{}
	}
	np.Config.MachineType, np.Config.ImageType = cmp.Or(np.Config.MachineType, "e2-medium"), cmp.Or(np.Config.ImageType, "COS_CONTAINERD")
	np.Config.DiskType, np.Config.DiskSizeGb = cmp.Or(np.Config.DiskType, "pd-balanced"), cmp.Or(np.Config.DiskSizeGb, 100)
	np.Etag = core.RandHex(4)
}

// boot runs the k3s container of a cluster in the background (restart: the one
// it has), then makes the cluster RUNNING and finishes the operation opKey.
// Lock held.
func (s *Service) boot(key string, cl *cluster, restart bool, opKey string) {
	ctx, cancel := context.WithCancel(s.ctx)
	cl.cancel = cancel
	cl.p.Status = containerpb.Cluster_PROVISIONING
	d := s.hub.Docker()
	port := strconv.Itoa(s.hub.Cfg.Port) // before pogi listens (Load at startup) its URL is unset
	if u, err := url.Parse(s.hub.URL); err == nil && u.Port() != "" {
		port = u.Port()
	}
	// ponytail: an engine without a VM (Linux) cannot reach pogi on 127.0.0.1; kubectl
	// then gets Unauthorized until pogi listens where the container can reach it.
	k := &eks.K3s{Name: cl.container, Image: eks.DefaultImage(), Service: "container", Resource: cl.p.Name, Token: cl.token,
		Webhook: "http://" + d.HostAddr() + ":" + port + "/_gcp/gke/authn/" + key}
	k.Watch, k.OnExit = s.ctx, func(note string) {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.st.clusters[key] == cl && cl.p.Status == containerpb.Cluster_RUNNING {
			cl.p.Status, cl.p.StatusMessage = containerpb.Cluster_ERROR, note
			s.hub.Log.Warn("gke cluster stopped", "cluster", key, "note", note)
		}
	}
	core.Go("gke.boot", func() {
		defer cancel()
		endpoint, ca, err := k.Run(ctx, d, restart)
		s.mu.Lock()
		defer s.mu.Unlock()
		switch {
		case s.st.clusters[key] != cl: // deleted meanwhile
			s.drop(cl)
		case err != nil:
			cl.p.Status, cl.p.StatusMessage = containerpb.Cluster_ERROR, docker.Plain(err)
			s.finish(opKey, err)
			s.hub.Log.Warn("gke cluster failed", "cluster", key, "err", err)
		default:
			cl.p.Status, cl.p.StatusMessage = containerpb.Cluster_RUNNING, ""
			cl.p.Endpoint = strings.TrimPrefix(endpoint, "https://")
			cl.p.MasterAuth.ClusterCaCertificate = base64.StdEncoding.EncodeToString(ca)
			for _, np := range cl.p.NodePools {
				np.Status = containerpb.NodePool_RUNNING
			}
			s.finish(opKey, nil)
		}
	})
}

// drop stops the k3s work of a cluster that is gone and removes its container,
// unless a current cluster runs in it. Lock held.
func (s *Service) drop(cl *cluster) {
	if cl.cancel != nil {
		cl.cancel()
	}
	for _, x := range s.st.clusters {
		if x.container == cl.container {
			return
		}
	}
	if cl.container != "" {
		go s.hub.Docker().Remove(context.Background(), cl.container)
	}
}

func (s *Service) GetCluster(_ context.Context, r *containerpb.GetClusterRequest) (*containerpb.Cluster, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, cl, _, err := s.find(r.Name)
	if err != nil {
		return nil, err
	}
	return proto.Clone(cl.p).(*containerpb.Cluster), nil
}

// ponytail: lists return everything in one page; GKE has no page tokens for clusters either.
func (s *Service) ListClusters(_ context.Context, r *containerpb.ListClustersRequest) (*containerpb.ListClustersResponse, error) {
	m := parentRE.FindStringSubmatch(r.Parent)
	if m == nil {
		return nil, gapi.Invalid("parent must look like projects/<project>/locations/<location>")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := &containerpb.ListClustersResponse{}
	for _, k := range core.SortedKeys(s.st.clusters) {
		if strings.HasPrefix(k, m[1]+"/") && (m[2] == "-" || strings.HasPrefix(k, m[1]+"/"+m[2]+"/")) {
			out.Clusters = append(out.Clusters, proto.Clone(s.st.clusters[k].p).(*containerpb.Cluster))
		}
	}
	return out, nil
}

func (s *Service) DeleteCluster(_ context.Context, r *containerpb.DeleteClusterRequest) (*containerpb.Operation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key, cl, _, err := s.find(r.Name)
	if err != nil {
		return nil, err
	}
	delete(s.st.clusters, key)
	s.drop(cl)
	project, loc, _ := strings.Cut(key, "/")
	loc, _, _ = strings.Cut(loc, "/")
	return s.done(project, loc, containerpb.Operation_DELETE_CLUSTER, strings.TrimPrefix(cl.p.SelfLink, selfBase)), nil
}

// UpdateCluster applies the settings of ClusterUpdate that a cluster here has.
// ponytail: the others are accepted and ignored; add the ones a test needs.
func (s *Service) UpdateCluster(_ context.Context, r *containerpb.UpdateClusterRequest) (*containerpb.Operation, error) {
	u := r.Update
	if u == nil {
		return nil, gapi.Invalid("update is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key, cl, _, err := s.find(r.Name)
	if err != nil {
		return nil, err
	}
	p := cl.p
	if u.DesiredNodePoolId != "" {
		np := cl.pool(u.DesiredNodePoolId)
		if np == nil {
			return nil, gapi.NotFound("Not found: node pool %q.", u.DesiredNodePoolId)
		}
		if u.DesiredNodePoolAutoscaling != nil {
			np.Autoscaling = u.DesiredNodePoolAutoscaling
		}
		if u.DesiredNodeVersion != "" {
			np.Version = u.DesiredNodeVersion
		}
		if u.DesiredImageType != "" {
			np.Config.ImageType = u.DesiredImageType
		}
	} else if u.DesiredNodeVersion != "" {
		p.CurrentNodeVersion = u.DesiredNodeVersion
		for _, np := range p.NodePools {
			np.Version = u.DesiredNodeVersion
		}
	}
	p.CurrentMasterVersion = cmp.Or(u.DesiredMasterVersion, p.CurrentMasterVersion)
	p.LoggingService = cmp.Or(u.DesiredLoggingService, p.LoggingService)
	p.MonitoringService = cmp.Or(u.DesiredMonitoringService, p.MonitoringService)
	if u.DesiredAddonsConfig != nil {
		p.AddonsConfig = u.DesiredAddonsConfig
	}
	if len(u.DesiredLocations) > 0 {
		p.Locations = u.DesiredLocations
	}
	if u.DesiredReleaseChannel != nil {
		p.ReleaseChannel = u.DesiredReleaseChannel
	}
	if u.DesiredBinaryAuthorization != nil {
		p.BinaryAuthorization = u.DesiredBinaryAuthorization
	}
	if u.DesiredMasterAuthorizedNetworksConfig != nil {
		p.MasterAuthorizedNetworksConfig = u.DesiredMasterAuthorizedNetworksConfig
	}
	project, rest, _ := strings.Cut(key, "/")
	loc, _, _ := strings.Cut(rest, "/")
	return s.done(project, loc, containerpb.Operation_UPDATE_CLUSTER, strings.TrimPrefix(p.SelfLink, selfBase)), nil
}

func (s *Service) SetLabels(_ context.Context, r *containerpb.SetLabelsRequest) (*containerpb.Operation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key, cl, _, err := s.find(r.Name)
	if err != nil {
		return nil, err
	}
	cl.p.ResourceLabels, cl.p.LabelFingerprint = r.ResourceLabels, core.RandHex(4)
	project, rest, _ := strings.Cut(key, "/")
	loc, _, _ := strings.Cut(rest, "/")
	return s.done(project, loc, containerpb.Operation_SET_LABELS, strings.TrimPrefix(cl.p.SelfLink, selfBase)), nil
}

// ---- node pools ----

func (s *Service) pool(name string) (key string, cl *cluster, np *containerpb.NodePool, err error) {
	key, cl, pn, err := s.find(name)
	if err != nil {
		return "", nil, nil, err
	}
	if pn == "" {
		return "", nil, nil, gapi.Invalid("name must look like projects/<project>/locations/<location>/clusters/<cluster>/nodePools/<pool>")
	}
	if np = cl.pool(pn); np == nil {
		return "", nil, nil, gapi.NotFound("Not found: %s/nodePools/%s.", name[:strings.Index(name, "/nodePools")], pn)
	}
	return key, cl, np, nil
}

func (s *Service) poolOp(key string, cl *cluster, typ containerpb.Operation_Type, pool string) *containerpb.Operation {
	project, rest, _ := strings.Cut(key, "/")
	loc, _, _ := strings.Cut(rest, "/")
	return s.done(project, loc, typ, strings.TrimPrefix(cl.p.SelfLink, selfBase)+"/nodePools/"+pool)
}

func (s *Service) CreateNodePool(_ context.Context, r *containerpb.CreateNodePoolRequest) (*containerpb.Operation, error) {
	if r.NodePool == nil || r.NodePool.Name == "" {
		return nil, gapi.Invalid("node_pool.name is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key, cl, _, err := s.find(r.Parent)
	if err != nil {
		return nil, err
	}
	np := proto.Clone(r.NodePool).(*containerpb.NodePool)
	if cl.pool(np.Name) != nil {
		return nil, gapi.Exists("Already exists: %s/nodePools/%s.", r.Parent, np.Name)
	}
	fillPool(np, strings.TrimPrefix(cl.p.SelfLink, selfBase), containerpb.NodePool_RUNNING)
	cl.p.NodePools = append(cl.p.NodePools, np)
	cl.count()
	return s.poolOp(key, cl, containerpb.Operation_CREATE_NODE_POOL, np.Name), nil
}

func (s *Service) GetNodePool(_ context.Context, r *containerpb.GetNodePoolRequest) (*containerpb.NodePool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, _, np, err := s.pool(r.Name)
	if err != nil {
		return nil, err
	}
	return proto.Clone(np).(*containerpb.NodePool), nil
}

func (s *Service) ListNodePools(_ context.Context, r *containerpb.ListNodePoolsRequest) (*containerpb.ListNodePoolsResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, cl, _, err := s.find(r.Parent)
	if err != nil {
		return nil, err
	}
	out := &containerpb.ListNodePoolsResponse{}
	for _, np := range cl.p.NodePools {
		out.NodePools = append(out.NodePools, proto.Clone(np).(*containerpb.NodePool))
	}
	return out, nil
}

// UpdateNodePool applies the settings of the request that a pool here has.
// ponytail: the others are accepted and ignored.
func (s *Service) UpdateNodePool(_ context.Context, r *containerpb.UpdateNodePoolRequest) (*containerpb.Operation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key, cl, np, err := s.pool(r.Name)
	if err != nil {
		return nil, err
	}
	np.Version = cmp.Or(r.NodeVersion, np.Version)
	np.Config.ImageType = cmp.Or(r.ImageType, np.Config.ImageType)
	np.Config.MachineType = cmp.Or(r.MachineType, np.Config.MachineType)
	np.Config.DiskType = cmp.Or(r.DiskType, np.Config.DiskType)
	np.Config.DiskSizeGb = cmp.Or(int32(r.DiskSizeGb), np.Config.DiskSizeGb)
	if len(r.Locations) > 0 {
		np.Locations = r.Locations
	}
	if r.UpgradeSettings != nil {
		np.UpgradeSettings = r.UpgradeSettings
	}
	if r.Labels != nil {
		np.Config.Labels = r.Labels.Labels
	}
	if r.Tags != nil {
		np.Config.Tags = r.Tags.Tags
	}
	np.Etag = core.RandHex(4)
	return s.poolOp(key, cl, containerpb.Operation_UPDATE_CLUSTER, np.Name), nil
}

func (s *Service) SetNodePoolSize(_ context.Context, r *containerpb.SetNodePoolSizeRequest) (*containerpb.Operation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key, cl, np, err := s.pool(r.Name)
	if err != nil {
		return nil, err
	}
	np.InitialNodeCount = r.NodeCount
	cl.count()
	return s.poolOp(key, cl, containerpb.Operation_SET_NODE_POOL_SIZE, np.Name), nil
}

func (s *Service) DeleteNodePool(_ context.Context, r *containerpb.DeleteNodePoolRequest) (*containerpb.Operation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key, cl, np, err := s.pool(r.Name)
	if err != nil {
		return nil, err
	}
	cl.p.NodePools = slices.DeleteFunc(cl.p.NodePools, func(x *containerpb.NodePool) bool { return x == np })
	cl.count()
	return s.poolOp(key, cl, containerpb.Operation_DELETE_NODE_POOL, np.Name), nil
}

// ---- operations ----

func (s *Service) operation(name string) (*containerpb.Operation, error) {
	m := opNameRE.FindStringSubmatch(name)
	if m == nil {
		return nil, gapi.Invalid("name must look like projects/<project>/locations/<location>/operations/<operation>")
	}
	o := s.st.ops[m[1]+"/"+m[2]+"/"+m[3]]
	if o == nil {
		return nil, gapi.NotFound("Not found: %s.", name)
	}
	return o, nil
}

func (s *Service) GetOperation(_ context.Context, r *containerpb.GetOperationRequest) (*containerpb.Operation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	o, err := s.operation(r.Name)
	if err != nil {
		return nil, err
	}
	return cloneOp(o), nil
}

func (s *Service) ListOperations(_ context.Context, r *containerpb.ListOperationsRequest) (*containerpb.ListOperationsResponse, error) {
	m := parentRE.FindStringSubmatch(r.Parent)
	if m == nil {
		return nil, gapi.Invalid("parent must look like projects/<project>/locations/<location>")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := &containerpb.ListOperationsResponse{}
	for _, k := range core.SortedKeys(s.st.ops) {
		if strings.HasPrefix(k, m[1]+"/") && (m[2] == "-" || strings.HasPrefix(k, m[1]+"/"+m[2]+"/")) {
			out.Operations = append(out.Operations, cloneOp(s.st.ops[k]))
		}
	}
	return out, nil
}

// CancelOperation marks a running operation ABORTING; it ends with the cluster's start.
func (s *Service) CancelOperation(_ context.Context, r *containerpb.CancelOperationRequest) (*emptypb.Empty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	o, err := s.operation(r.Name)
	if err != nil {
		return nil, err
	}
	if o.Status == containerpb.Operation_RUNNING {
		o.Status = containerpb.Operation_ABORTING
	}
	return &emptypb.Empty{}, nil
}

// ---- credentials ----

// authn is the TokenReview webhook of the clusters' API servers, at
// /_gcp/gke/authn/<project>/<location>/<cluster>: it takes the access tokens
// pogi's Google sign-in issues.
// ponytail: every valid token is cluster-admin; map IAM roles to groups.
func (s *Service) authn(w http.ResponseWriter, r *http.Request) {
	var tr struct {
		APIVersion string `json:"apiVersion"`
		Spec       struct{ Token string }
	}
	json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&tr)
	key := strings.TrimPrefix(r.URL.Path, "/_gcp/gke/authn/")
	s.mu.Lock()
	known := s.st.clusters[key] != nil
	s.mu.Unlock()
	t, ok := gapi.ParseAccessToken(tr.Spec.Token)
	status := map[string]any{"authenticated": false}
	if ok && known {
		status = map[string]any{"authenticated": true, "user": map[string]any{"username": cmp.Or(t.Email, "pogi"), "uid": cmp.Or(t.Email, "pogi"), "groups": []string{"system:masters"}}}
	} else {
		status["error"] = "not a pogi access token, or an expired one, or no such cluster"
		s.hub.Log.Info("gke token rejected", "cluster", key)
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"apiVersion": cmp.Or(tr.APIVersion, "authentication.k8s.io/v1"), "kind": "TokenReview", "status": status})
}

// kubeconfig answers a kubeconfig for a cluster with a static pogi access token
// (valid for ten years), for machines without gke-gcloud-auth-plugin.
func (s *Service) kubeconfig(w http.ResponseWriter, r *http.Request) {
	project, rest, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/_gcp/gke/kubeconfig/"), "/")
	loc, name, _ := strings.Cut(rest, "/")
	s.mu.Lock()
	cfg, err := s.config(project, loc, name)
	s.mu.Unlock()
	if err != nil {
		http.Error(w, core.AsError(err).Msg, core.AsError(err).Status)
		return
	}
	io.WriteString(w, cfg)
}

// Kubeconfig is the kubeconfig of the cluster of that name, for the console. It answers 404 for an
// unknown cluster and 409 for one without an API server yet.
func (s *Service) Kubeconfig(name string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, k := range core.SortedKeys(s.st.clusters) {
		if cl := s.st.clusters[k]; cl.p.Name == name {
			project, rest, _ := strings.Cut(k, "/")
			loc, _, _ := strings.Cut(rest, "/")
			return s.config(project, loc, name)
		}
	}
	return "", core.Errorf(http.StatusNotFound, "NOT_FOUND", "No cluster named %s.", name)
}

// config builds the kubeconfig. Lock held.
func (s *Service) config(project, loc, name string) (string, error) {
	cl := s.st.clusters[project+"/"+loc+"/"+name]
	switch {
	case cl == nil:
		return "", core.Errorf(http.StatusNotFound, "NOT_FOUND", "no cluster %s", clusterName(project, loc, name))
	case cl.p.Endpoint == "":
		return "", core.Errorf(http.StatusConflict, "FAILED_PRECONDITION", "the cluster has no API server (yet): see its status and statusMessage")
	}
	ctx := "gke_" + project + "_" + loc + "_" + name
	tok := gapi.AccessToken("kubectl@"+project+".iam.gserviceaccount.com", scope, 10*365*24*time.Hour)
	return fmt.Sprintf("apiVersion: v1\nkind: Config\nclusters:\n- name: %[1]s\n  cluster:\n    server: https://%[2]s\n    certificate-authority-data: %[3]s\n"+
		"users:\n- name: %[1]s\n  user:\n    token: %[4]s\ncontexts:\n- name: %[1]s\n  context:\n    cluster: %[1]s\n    user: %[1]s\ncurrent-context: %[1]s\n",
		ctx, cl.p.Endpoint, cl.p.MasterAuth.ClusterCaCertificate, tok), nil
}

// ---- persistence, console, shutdown ----

type saved struct {
	Cluster   json.RawMessage `json:"cluster"`
	Container string          `json:"container,omitempty"`
	Token     string          `json:"token,omitempty"`
}

type snapshot struct {
	Clusters map[string]saved           `json:"clusters"`
	Ops      map[string]json.RawMessage `json:"ops"`
}

func (s *Service) Save() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := snapshot{Clusters: map[string]saved{}, Ops: map[string]json.RawMessage{}}
	for k, cl := range s.st.clusters {
		b, err := protojson.Marshal(cl.p)
		if err != nil {
			return nil, err
		}
		out.Clusters[k] = saved{b, cl.container, cl.token}
	}
	for k, o := range s.st.ops {
		b, err := protojson.Marshal(o)
		if err != nil {
			return nil, err
		}
		out.Ops[k] = b
	}
	return json.Marshal(out)
}

// Load restores state. Clusters that ran on k3s start their containers again
// (persisted state leaves them behind on shutdown), or turn ERROR when the
// engine or the container is gone.
func (s *Service) Load(b []byte) error {
	var in snapshot
	if err := json.Unmarshal(b, &in); err != nil {
		return err
	}
	st := state{clusters: map[string]*cluster{}, ops: map[string]*containerpb.Operation{}}
	for k, c := range in.Clusters {
		p := &containerpb.Cluster{}
		if err := protojson.Unmarshal(c.Cluster, p); err != nil {
			return err
		}
		st.clusters[k] = &cluster{p: p, container: c.Container, token: c.Token}
	}
	for k, raw := range in.Ops {
		o := &containerpb.Operation{}
		if err := protojson.Unmarshal(raw, o); err != nil {
			return err
		}
		if o.Status != containerpb.Operation_DONE { // its start was cut short
			o.Status, o.EndTime = containerpb.Operation_DONE, stamp()
		}
		st.ops[k] = o
	}
	engine := false
	for _, cl := range st.clusters {
		if cl.container != "" {
			engine = s.hub.Docker().Available()
			break
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	old := s.st
	s.st = st
	for k, cl := range st.clusters {
		switch {
		case cl.container == "":
		case engine:
			s.boot(k, cl, true, "")
		default:
			cl.p.Status, cl.p.StatusMessage = containerpb.Cluster_ERROR, "Docker is not available, so the k3s container of this cluster cannot start"
		}
	}
	for _, cl := range old.clusters {
		s.drop(cl)
	}
	return nil
}

func (s *Service) Reset() { s.Load([]byte("{}")) }

// Close stops k3s work and removes the clusters' containers, unless state is
// persisted: the next run starts them again.
func (s *Service) Close() error {
	s.stop()
	if cfg := s.hub.Cfg; cfg.Persist || cfg.Storage != "" && cfg.Storage != "memory" {
		return nil
	}
	s.mu.Lock()
	var names []string
	for _, cl := range s.st.clusters {
		if cl.container != "" {
			names = append(names, cl.container)
		}
	}
	s.mu.Unlock()
	for _, n := range names {
		s.hub.Docker().Remove(context.Background(), n)
	}
	return nil
}

// Resources lists the clusters for the console.
func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := core.Table{Name: "Clusters", Columns: []string{"Name", "Status", "Version", "Endpoint", "Node pools", "Location", "Project"}}
	for _, k := range core.SortedKeys(s.st.clusters) {
		p, rest, _ := strings.Cut(k, "/")
		loc, _, _ := strings.Cut(rest, "/")
		cl := s.st.clusters[k].p
		t.Rows = append(t.Rows, []string{cl.Name, cl.Status.String(), cl.CurrentMasterVersion, cl.Endpoint, strconv.Itoa(len(cl.NodePools)), loc, p})
	}
	return []core.Table{t}
}

// Inspect returns a cluster with pogi's side: its container and statusMessage (never its k3s token).
func (s *Service) Inspect(_, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, k := range core.SortedKeys(s.st.clusters) {
		if cl := s.st.clusters[k]; cl.p.Name == id {
			b, _ := protojson.Marshal(cl.p)
			out, _ := json.Marshal(map[string]any{"cluster": json.RawMessage(b), "container": cl.container, "pogiNote": cl.p.StatusMessage})
			return json.RawMessage(out)
		}
	}
	return nil
}
