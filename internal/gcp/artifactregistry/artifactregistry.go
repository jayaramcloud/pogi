// Package artifactregistry emulates Artifact Registry (v1) over gRPC and its
// REST mirror: repositories (create, get, list, update, delete, IAM) and the
// Docker images in them. The registry itself is ECR's OCI endpoint
// (internal/aws/ecr, core.ImageRegistry): a Docker repository claims the names
// "<project>/<repository>/..." there, so docker push and pull go to
// localhost:<ECR port>/<project>/<repository>/<image>:<tag>, and an image
// reference <region>-docker.pkg.dev/<project>/<repository>/<image> that a
// container service runs is pulled from there.
//
// ponytail: only Docker repositories hold anything; the other formats are
// stored as metadata. Packages, versions, tags, files and rules are not
// served, and dockerImages has no Get (its name carries an escaped slash).
// Without the ECR service the repositories are metadata only.
package artifactregistry

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"

	pb "cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"
	"cloud.google.com/go/iam/apiv1/iampb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/gcp/gapi"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Service is the Artifact Registry emulator.
type Service struct {
	pb.UnimplementedArtifactRegistryServer
	hub  *core.Hub
	ops  *gapi.Ops
	rest *gapi.REST

	mu    sync.Mutex
	repos map[string]*pb.Repository
	iam   map[string]*iampb.Policy
}

func New(h *core.Hub) *Service {
	s := &Service{hub: h, ops: gapi.Operations(h)}
	s.Reset()
	r := "locations/*/repositories"
	s.rest = &gapi.REST{Versions: []string{"v1"}, Collections: []string{"repositories"}, Desc: &pb.ArtifactRegistry_ServiceDesc, Impl: s, Routes: map[string]gapi.Route{
		"POST " + r:                           {Method: "CreateRepository", Field: "parent", Body: "repository"},
		"GET " + r:                            {Method: "ListRepositories", Field: "parent"},
		"GET " + r + "/*":                     {Method: "GetRepository", Field: "name"},
		"PATCH " + r + "/*":                   {Method: "UpdateRepository", Field: "repository.name", Body: "repository"},
		"DELETE " + r + "/*":                  {Method: "DeleteRepository", Field: "name"},
		"GET " + r + "/*/dockerImages":        {Method: "ListDockerImages", Field: "parent"},
		"GET " + r + "/*:getIamPolicy":        {Method: "GetIamPolicy", Field: "resource"},
		"POST " + r + "/*:setIamPolicy":       {Method: "SetIamPolicy", Field: "resource"},
		"POST " + r + "/*:testIamPermissions": {Method: "TestIamPermissions", Field: "resource"},
	}}
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "artifactregistry", Cloud: core.GCP, Title: "Artifact Registry", Match: s.rest.Match,
		Targets: []string{"google.devtools.artifactregistry.v1.ArtifactRegistry"}}
}

func (s *Service) RegisterGRPC(g *grpc.Server) { pb.RegisterArtifactRegistryServer(g, s) }

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.rest.ServeHTTP(w, r) }

// ---- names ----

var (
	parentRE = regexp.MustCompile(`^projects/([^/]+)/locations/([^/]+)$`)
	nameRE   = regexp.MustCompile(`^projects/([^/]+)/locations/([^/]+)/repositories/([^/]+)$`)
	idRE     = regexp.MustCompile(`^[a-z]([a-z0-9-]{0,61}[a-z0-9])?$`)
)

// prefix is the repository's names in the registry, "<project>/<repository>".
func prefix(name string) string {
	m := nameRE.FindStringSubmatch(name)
	return m[1] + "/" + m[3]
}

func (s *Service) claim(name string, r *pb.Repository) {
	if reg := core.Registry(s.hub); reg != nil && r.Format == pb.Repository_DOCKER {
		reg.Claim(prefix(name))
	}
}

func notFound(string) error { return gapi.NotFound("Requested entity was not found.") }

// ---- repositories ----

// view is the repository as the API returns it.
func view(r *pb.Repository) *pb.Repository {
	r = proto.Clone(r).(*pb.Repository)
	if m := nameRE.FindStringSubmatch(r.Name); m != nil && r.Format == pb.Repository_DOCKER {
		r.RegistryUri = m[2] + "-docker.pkg.dev/" + m[1] + "/" + m[3]
	}
	return r
}

func (s *Service) CreateRepository(_ context.Context, req *pb.CreateRepositoryRequest) (*longrunningpb.Operation, error) {
	if !parentRE.MatchString(req.Parent) {
		return nil, gapi.Invalid("Invalid parent %q: want projects/<project>/locations/<location>", req.Parent)
	}
	if !idRE.MatchString(req.RepositoryId) {
		return nil, gapi.Invalid("Invalid repository ID %q: lowercase letters, digits and '-', starting with a letter, at most 63 characters", req.RepositoryId)
	}
	r := proto.Clone(req.GetRepository()).(*pb.Repository)
	if r.Format == pb.Repository_FORMAT_UNSPECIFIED {
		return nil, gapi.Invalid("Repository format is required.")
	}
	now := timestamppb.Now()
	r.Name, r.CreateTime, r.UpdateTime, r.SizeBytes, r.RegistryUri = req.Parent+"/repositories/"+req.RepositoryId, now, now, 0, ""
	if r.Mode == pb.Repository_MODE_UNSPECIFIED {
		r.Mode = pb.Repository_STANDARD_REPOSITORY
	}
	s.mu.Lock()
	if s.repos[r.Name] != nil {
		s.mu.Unlock()
		return nil, gapi.Exists("the repository already exists")
	}
	s.repos[r.Name] = r
	s.mu.Unlock()
	s.claim(r.Name, r)
	return s.ops.Done(req.Parent, nil, view(r)), nil
}

func (s *Service) GetRepository(_ context.Context, req *pb.GetRepositoryRequest) (*pb.Repository, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.repos[req.Name]
	if r == nil {
		return nil, notFound(req.Name)
	}
	return view(r), nil
}

func (s *Service) ListRepositories(_ context.Context, req *pb.ListRepositoriesRequest) (*pb.ListRepositoriesResponse, error) {
	if !parentRE.MatchString(req.Parent) {
		return nil, gapi.Invalid("Invalid parent %q: want projects/<project>/locations/<location>", req.Parent)
	}
	s.mu.Lock()
	var all []*pb.Repository
	for _, n := range core.SortedKeys(s.repos) {
		if strings.HasPrefix(n, req.Parent+"/") {
			all = append(all, view(s.repos[n]))
		}
	}
	s.mu.Unlock()
	page, next := gapi.Page(all, req.PageSize, req.PageToken)
	return &pb.ListRepositoriesResponse{Repositories: page, NextPageToken: next}, nil
}

func (s *Service) UpdateRepository(_ context.Context, req *pb.UpdateRepositoryRequest) (*pb.Repository, error) {
	name := req.GetRepository().GetName()
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.repos[name]
	if r == nil {
		return nil, notFound(name)
	}
	if err := gapi.Update(r, req.Repository, req.GetUpdateMask().GetPaths(), "description", "labels", "cleanup_policies", "cleanup_policy_dry_run"); err != nil {
		return nil, err
	}
	r.UpdateTime = timestamppb.Now()
	return view(r), nil
}

func (s *Service) DeleteRepository(_ context.Context, req *pb.DeleteRepositoryRequest) (*longrunningpb.Operation, error) {
	s.mu.Lock()
	r := s.repos[req.Name]
	delete(s.repos, req.Name)
	delete(s.iam, req.Name)
	s.mu.Unlock()
	if r == nil {
		return nil, notFound(req.Name)
	}
	if reg := core.Registry(s.hub); reg != nil && r.Format == pb.Repository_DOCKER {
		reg.Release(prefix(req.Name)) // the images go with it
	}
	return s.ops.Done(req.Name[:strings.Index(req.Name, "/repositories/")], nil, &emptypb.Empty{}), nil
}

// ---- Docker images ----

func (s *Service) ListDockerImages(_ context.Context, req *pb.ListDockerImagesRequest) (*pb.ListDockerImagesResponse, error) {
	s.mu.Lock()
	r := s.repos[req.Parent]
	s.mu.Unlock()
	if r == nil {
		return nil, notFound(req.Parent)
	}
	var all []*pb.DockerImage
	if reg := core.Registry(s.hub); reg != nil && r.Format == pb.Repository_DOCKER {
		m := nameRE.FindStringSubmatch(req.Parent)
		for _, img := range reg.Images(prefix(req.Parent)) {
			all = append(all, &pb.DockerImage{
				Name:           req.Parent + "/dockerImages/" + url.PathEscape(img.Name) + "@" + img.Digest,
				Uri:            m[2] + "-docker.pkg.dev/" + m[1] + "/" + m[3] + "/" + img.Name + "@" + img.Digest,
				Tags:           img.Tags,
				ImageSizeBytes: img.Size,
				MediaType:      img.MediaType,
				UploadTime:     timestamppb.New(img.Pushed),
				BuildTime:      timestamppb.New(img.Pushed),
				UpdateTime:     timestamppb.New(img.Pushed),
			})
		}
	}
	page, next := gapi.Page(all, req.PageSize, req.PageToken)
	return &pb.ListDockerImagesResponse{DockerImages: page, NextPageToken: next}, nil
}

// ---- IAM (stored as given; pogi does not enforce it) ----

func (s *Service) GetIamPolicy(_ context.Context, req *iampb.GetIamPolicyRequest) (*iampb.Policy, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.repos[req.Resource] == nil {
		return nil, notFound(req.Resource)
	}
	return gapi.Policy(s.iam[req.Resource]), nil
}

func (s *Service) SetIamPolicy(_ context.Context, req *iampb.SetIamPolicyRequest) (*iampb.Policy, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.repos[req.Resource] == nil {
		return nil, notFound(req.Resource)
	}
	p, err := gapi.SetPolicy(s.iam[req.Resource], req.Policy)
	if err != nil {
		return nil, err
	}
	s.iam[req.Resource] = p
	return gapi.Policy(p), nil
}

func (s *Service) TestIamPermissions(_ context.Context, req *iampb.TestIamPermissionsRequest) (*iampb.TestIamPermissionsResponse, error) {
	return &iampb.TestIamPermissionsResponse{Permissions: req.Permissions}, nil
}

// ---- state and console ----

func (s *Service) Reset() {
	s.mu.Lock()
	reg := core.Registry(s.hub)
	for n, r := range s.repos {
		if reg != nil && r.Format == pb.Repository_DOCKER {
			reg.Release(prefix(n))
		}
	}
	s.repos, s.iam = map[string]*pb.Repository{}, map[string]*iampb.Policy{}
	s.mu.Unlock()
}

type saved struct {
	Repos []json.RawMessage          `json:"repos"`
	IAM   map[string]json.RawMessage `json:"iam"`
}

func (s *Service) Save() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := saved{IAM: map[string]json.RawMessage{}}
	for _, n := range core.SortedKeys(s.repos) {
		b, _ := protojson.Marshal(s.repos[n])
		out.Repos = append(out.Repos, b)
	}
	for n, p := range s.iam {
		out.IAM[n], _ = protojson.Marshal(p)
	}
	return json.Marshal(out)
}

func (s *Service) Load(b []byte) error {
	var in saved
	if err := json.Unmarshal(b, &in); err != nil {
		return err
	}
	repos, iam := map[string]*pb.Repository{}, map[string]*iampb.Policy{}
	for _, raw := range in.Repos {
		r := &pb.Repository{}
		if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(raw, r); err != nil {
			return err
		}
		repos[r.Name] = r
	}
	for n, raw := range in.IAM {
		p := &iampb.Policy{}
		if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(raw, p); err != nil {
			return err
		}
		iam[n] = p
	}
	s.mu.Lock()
	s.repos, s.iam = repos, iam
	s.mu.Unlock()
	for n, r := range repos {
		s.claim(n, r)
	}
	return nil
}

func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := core.Table{Name: "repositories", Columns: []string{"Name", "Format", "Mode", "Images"}}
	reg := core.Registry(s.hub)
	for _, n := range core.SortedKeys(s.repos) {
		r, imgs := s.repos[n], 0
		if reg != nil && r.Format == pb.Repository_DOCKER {
			imgs = len(reg.Images(prefix(n)))
		}
		t.Rows = append(t.Rows, []string{n, r.Format.String(), r.Mode.String(), strconv.Itoa(imgs)})
	}
	return []core.Table{t}
}

func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	r := s.repos[id]
	s.mu.Unlock()
	if r == nil {
		return nil
	}
	b, _ := protojson.Marshal(view(r))
	out := map[string]any{}
	json.Unmarshal(b, &out)
	if reg := core.Registry(s.hub); reg != nil && r.Format == pb.Repository_DOCKER {
		out["images"] = reg.Images(prefix(id))
	}
	return out
}
