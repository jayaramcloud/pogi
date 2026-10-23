// Package resourcemanager emulates Cloud Resource Manager: projects over the
// v3 gRPC and REST APIs, and over the v1 REST API that Terraform's google
// provider uses; IAM policies on projects (stored, not enforced); and
// read-only folders and organizations. Create, update, move and delete are
// long-running operations that finish at once, on the shared operations service.
package resourcemanager

import (
	"context"
	"encoding/json"
	"math/rand/v2"
	"net/http"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/iam/apiv1/iampb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	pb "cloud.google.com/go/resourcemanager/apiv3/resourcemanagerpb"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/gcp/gapi"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Service is Cloud Resource Manager's Projects service (v1 and v3).
type Service struct {
	pb.UnimplementedProjectsServer
	hub  *core.Hub
	mu   sync.Mutex
	st   state
	rest []*gapi.Mux
}

type state struct {
	Projects map[string]*project `json:"projects"` // by project id
}

type project struct {
	Number      string            `json:"number"`
	ID          string            `json:"id"`
	DisplayName string            `json:"displayName"`
	Parent      string            `json:"parent,omitempty"` // organizations/<n> or folders/<n>
	Deleted     bool              `json:"deleted,omitempty"`
	Created     time.Time         `json:"created"`
	Updated     time.Time         `json:"updated"`
	DeleteTime  time.Time         `json:"deleteTime,omitempty"`
	Labels      map[string]string `json:"labels,omitempty"`
	Etag        string            `json:"etag"`
	Policy      json.RawMessage   `json:"policy,omitempty"`
}

// New creates the Resource Manager service.
func New(h *core.Hub) *Service {
	s := &Service{hub: h}
	s.Reset()
	s.rest = []*gapi.Mux{
		gapi.NewMux(s, append(v3Rules, v1Rules(s)...)...),
		gapi.NewMux(&folders{s: s}, folderRules...),
		gapi.NewMux(&orgs{s: s}, orgRules...),
	}
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "resourcemanager", Cloud: core.GCP, Title: "Resource Manager", Match: s.match,
		Targets: []string{"google.cloud.resourcemanager.v3.Projects", "google.cloud.resourcemanager.v3.Folders", "google.cloud.resourcemanager.v3.Organizations"}}
}

func (s *Service) RegisterGRPC(g *grpc.Server) {
	pb.RegisterProjectsServer(g, s)
	pb.RegisterFoldersServer(g, &folders{s: s})
	pb.RegisterOrganizationsServer(g, &orgs{s: s})
}

func (s *Service) match(r *http.Request) bool {
	for _, m := range s.rest {
		if m.Match(r) {
			return true
		}
	}
	return false
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	for _, m := range s.rest {
		if m.Match(r) {
			m.ServeHTTP(w, r)
			return
		}
	}
}

func (s *Service) Reset() {
	s.mu.Lock()
	s.st = state{Projects: map[string]*project{}}
	s.defaultProject()
	s.mu.Unlock()
}

// defaultProject makes the project that pogi's tools name (core.DefaultProject) exist, as every other
// service lets it: Terraform's google provider reads it back after it enables a service in it.
func (s *Service) defaultProject() {
	if s.st.Projects[core.DefaultProject] == nil {
		now := time.Now()
		s.st.Projects[core.DefaultProject] = &project{ID: core.DefaultProject, Number: "100000000001", DisplayName: core.DefaultProject,
			Created: now, Updated: now, Etag: core.RandHex(8)}
	}
}

func (s *Service) Save() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(s.st)
}

func (s *Service) Load(b []byte) error {
	var st state
	if err := json.Unmarshal(b, &st); err != nil {
		return err
	}
	if st.Projects == nil {
		st.Projects = map[string]*project{}
	}
	s.mu.Lock()
	s.st = st
	s.defaultProject()
	s.mu.Unlock()
	return nil
}

// ---- REST ----

var v3Rules = []gapi.Rule{
	{HTTP: "POST /v3/projects", Method: "CreateProject", Body: "project"},
	{HTTP: "GET /v3/projects", Method: "ListProjects"},
	{HTTP: "GET /v3/projects:search", Method: "SearchProjects"},
	{HTTP: "GET /v3/{name=projects/*}", Method: "GetProject"},
	{HTTP: "PATCH /v3/{project.name=projects/*}", Method: "UpdateProject", Body: "project"},
	{HTTP: "DELETE /v3/{name=projects/*}", Method: "DeleteProject"},
	{HTTP: "POST /v3/{name=projects/*}:undelete", Method: "UndeleteProject"},
	{HTTP: "POST /v3/{name=projects/*}:move", Method: "MoveProject"},
	{HTTP: "POST /v3/{resource=projects/*}:getIamPolicy", Method: "GetIamPolicy"},
	{HTTP: "POST /v3/{resource=projects/*}:setIamPolicy", Method: "SetIamPolicy"},
	{HTTP: "POST /v3/{resource=projects/*}:testIamPermissions", Method: "TestIamPermissions"},
}

func v1Rules(s *Service) []gapi.Rule {
	return []gapi.Rule{
		{HTTP: "POST /v1/projects", Method: "CreateProject", Fn: s.createV1},
		{HTTP: "GET /v1/projects", Method: "ListProjects", Fn: s.listV1},
		{HTTP: "GET /v1/projects/{id}", Method: "GetProject", Fn: s.getV1},
		{HTTP: "PUT /v1/projects/{id}", Method: "UpdateProject", Fn: s.updateV1},
		{HTTP: "DELETE /v1/projects/{id}", Method: "DeleteProject", Fn: s.deleteV1},
		{HTTP: "POST /v1/projects/{id}:undelete", Method: "UndeleteProject", Fn: s.undeleteV1},
		{HTTP: "POST /v1/projects/{resource}:getIamPolicy", Method: "GetIamPolicy"},
		{HTTP: "POST /v1/projects/{resource}:setIamPolicy", Method: "SetIamPolicy"},
		{HTTP: "POST /v1/projects/{resource}:testIamPermissions", Method: "TestIamPermissions"},
	}
}

// ---- projects ----

var (
	idRE     = regexp.MustCompile(`^[a-z][a-z0-9-]{4,28}[a-z0-9]$`)
	parentRE = regexp.MustCompile(`^(organizations|folders)/[0-9]+$`)
)

func (s *Service) ops() *gapi.Ops { return gapi.Operations(s.hub) }

// find resolves projects/<id or number>, or a bare id or number. Lock held.
func (s *Service) find(ref string) (*project, error) {
	ref = strings.TrimPrefix(ref, "projects/")
	if p := s.st.Projects[ref]; p != nil {
		return p, nil
	}
	for _, p := range s.st.Projects {
		if p.Number == ref {
			return p, nil
		}
	}
	return nil, gapi.NotFound("Project %s was not found, or the caller does not have permission to see it.", ref)
}

func (p *project) proto() *pb.Project {
	st := pb.Project_ACTIVE
	if p.Deleted {
		st = pb.Project_DELETE_REQUESTED
	}
	out := &pb.Project{Name: "projects/" + p.Number, Parent: p.Parent, ProjectId: p.ID, State: st, DisplayName: p.DisplayName,
		CreateTime: timestamppb.New(p.Created), UpdateTime: timestamppb.New(p.Updated), Etag: p.Etag, Labels: p.Labels}
	if p.Deleted {
		out.DeleteTime = timestamppb.New(p.DeleteTime)
	}
	return out
}

// v1 is the v1 JSON of a project.
func (p *project) v1() map[string]any {
	m := map[string]any{"projectNumber": p.Number, "projectId": p.ID, "lifecycleState": "ACTIVE", "name": p.DisplayName,
		"createTime": p.Created.UTC().Format(time.RFC3339Nano)}
	if p.Deleted {
		m["lifecycleState"] = "DELETE_REQUESTED"
	}
	if len(p.Labels) > 0 {
		m["labels"] = p.Labels
	}
	if typ, id, ok := strings.Cut(p.Parent, "/"); ok {
		m["parent"] = map[string]string{"type": strings.TrimSuffix(typ, "s"), "id": id}
	}
	return m
}

func (s *Service) create(id, name, parent string, labels map[string]string) (*project, error) {
	if !idRE.MatchString(id) {
		return nil, gapi.Invalid("Invalid project ID %q. Project IDs must be 6-30 characters: lowercase letters, digits and hyphens, starting with a letter and not ending in a hyphen.", id)
	}
	if parent != "" && !parentRE.MatchString(parent) {
		return nil, gapi.Invalid("Invalid parent %q: want organizations/{id} or folders/{id}", parent)
	}
	if name == "" {
		name = id
	}
	now := time.Now()
	p := &project{ID: id, DisplayName: name, Parent: parent, Created: now, Updated: now, Labels: labels, Etag: core.RandHex(8)}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.st.Projects[id] != nil {
		return nil, gapi.Exists("Requested entity already exists")
	}
	for {
		p.Number = strconv.FormatInt(100000000000+rand.Int64N(900000000000), 10)
		if _, err := s.find(p.Number); err != nil {
			break
		}
	}
	s.st.Projects[id] = p
	return p, nil
}

func (s *Service) CreateProject(_ context.Context, req *pb.CreateProjectRequest) (*longrunningpb.Operation, error) {
	in := req.GetProject()
	p, err := s.create(in.GetProjectId(), in.GetDisplayName(), in.GetParent(), in.GetLabels())
	if err != nil {
		return nil, err
	}
	meta := &pb.CreateProjectMetadata{CreateTime: timestamppb.New(p.Created), Gettable: true, Ready: true}
	return s.ops().Done("", meta, p.proto()), nil
}

func (s *Service) GetProject(_ context.Context, req *pb.GetProjectRequest) (*pb.Project, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.find(req.Name)
	if err != nil {
		return nil, err
	}
	return p.proto(), nil
}

func (s *Service) UpdateProject(_ context.Context, req *pb.UpdateProjectRequest) (*longrunningpb.Operation, error) {
	s.mu.Lock()
	p, err := s.find(req.GetProject().GetName())
	if err == nil {
		cur := p.proto()
		if err = gapi.Update(cur, req.Project, req.GetUpdateMask().GetPaths(), "display_name", "labels"); err == nil {
			p.DisplayName, p.Labels = cur.DisplayName, cur.Labels
			p.touch()
		}
	}
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return s.ops().Done("", &pb.UpdateProjectMetadata{}, p.proto()), nil
}

func (p *project) touch() { p.Updated, p.Etag = time.Now(), core.RandHex(8) }

func (s *Service) MoveProject(_ context.Context, req *pb.MoveProjectRequest) (*longrunningpb.Operation, error) {
	if !parentRE.MatchString(req.DestinationParent) {
		return nil, gapi.Invalid("Invalid destination_parent %q: want organizations/{id} or folders/{id}", req.DestinationParent)
	}
	s.mu.Lock()
	p, err := s.find(req.Name)
	if err == nil {
		p.Parent = req.DestinationParent
		p.touch()
	}
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return s.ops().Done("", &pb.MoveProjectMetadata{}, p.proto()), nil
}

// setDeleted requests deletion (a project stays visible in DELETE_REQUESTED) or undoes it.
func (s *Service) setDeleted(name string, deleted bool) (*project, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.find(name)
	if err != nil {
		return nil, err
	}
	if p.Deleted != deleted {
		p.Deleted, p.DeleteTime = deleted, time.Now()
		p.touch()
	}
	return p, nil
}

func (s *Service) DeleteProject(_ context.Context, req *pb.DeleteProjectRequest) (*longrunningpb.Operation, error) {
	p, err := s.setDeleted(req.Name, true)
	if err != nil {
		return nil, err
	}
	return s.ops().Done("", &pb.DeleteProjectMetadata{}, p.proto()), nil
}

func (s *Service) UndeleteProject(_ context.Context, req *pb.UndeleteProjectRequest) (*longrunningpb.Operation, error) {
	p, err := s.setDeleted(req.Name, false)
	if err != nil {
		return nil, err
	}
	return s.ops().Done("", &pb.UndeleteProjectMetadata{}, p.proto()), nil
}

func (s *Service) ListProjects(_ context.Context, req *pb.ListProjectsRequest) (*pb.ListProjectsResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var list []*pb.Project
	for _, id := range core.SortedKeys(s.st.Projects) {
		if p := s.st.Projects[id]; (req.Parent == "" || p.Parent == req.Parent) && (req.ShowDeleted || !p.Deleted) {
			list = append(list, p.proto())
		}
	}
	items, next := gapi.Page(list, req.PageSize, req.PageToken)
	return &pb.ListProjectsResponse{Projects: items, NextPageToken: next}, nil
}

func (s *Service) SearchProjects(_ context.Context, req *pb.SearchProjectsRequest) (*pb.SearchProjectsResponse, error) {
	items, next := gapi.Page(s.search(req.Query), req.PageSize, req.PageToken)
	out := &pb.SearchProjectsResponse{NextPageToken: next}
	for _, p := range items {
		out.Projects = append(out.Projects, p.proto())
	}
	return out, nil
}

// search lists copies of the projects a query matches, in project id order.
func (s *Service) search(query string) []*project {
	keep := matcher(query)
	s.mu.Lock()
	defer s.mu.Unlock()
	var list []*project
	for _, id := range core.SortedKeys(s.st.Projects) {
		if p := *s.st.Projects[id]; keep(&p) {
			list = append(list, &p)
		}
	}
	return list
}

// matcher turns a search query (v3) or list filter (v1) into a predicate.
// Terms are joined by spaces or AND: id, name, displayName, parent, parent.id,
// parent.type, state, lifecycleState, projectNumber and labels.<key>, as
// field:value (a * in the value is a wildcard; labels and names match any
// substring), or a bare word that appears in the id, name or a label.
// Deleted projects only match when the query names their state.
func matcher(query string) func(*project) bool {
	type term struct{ field, val string }
	var terms []term
	byState := false
	for _, t := range strings.Fields(query) {
		if t == "AND" {
			continue
		}
		f, v, ok := strings.Cut(t, ":")
		if !ok {
			f, v, ok = strings.Cut(t, "=")
		}
		if !ok {
			f, v = "", t
		}
		f = strings.ToLower(f)
		byState = byState || f == "state" || f == "lifecyclestate"
		terms = append(terms, term{f, strings.ToLower(strings.Trim(v, `"`))})
	}
	glob := func(have, want string, exact bool) bool {
		have = strings.ToLower(have)
		if strings.Contains(want, "*") {
			ok, _ := path.Match(want, have)
			return ok
		}
		return have == want || !exact && strings.Contains(have, want)
	}
	return func(p *project) bool {
		if p.Deleted && !byState {
			return false
		}
		for _, t := range terms {
			var ok bool
			switch f := t.field; {
			case f == "id" || f == "projectid":
				ok = glob(p.ID, t.val, true)
			case f == "name" || f == "displayname":
				ok = glob(p.DisplayName, t.val, false)
			case f == "projectnumber":
				ok = p.Number == t.val
			case f == "parent":
				ok = strings.EqualFold(p.Parent, t.val)
			case f == "parent.id":
				ok = strings.HasSuffix(p.Parent, "/"+t.val)
			case f == "parent.type":
				ok = strings.HasPrefix(p.Parent, t.val+"s/")
			case f == "state" || f == "lifecyclestate":
				ok = strings.EqualFold(p.proto().State.String(), t.val)
			case strings.HasPrefix(f, "labels."):
				v, has := p.Labels[strings.TrimPrefix(f, "labels.")]
				ok = has && (t.val == "*" || glob(v, t.val, false))
			case f == "":
				ok = glob(p.ID, t.val, false) || glob(p.DisplayName, t.val, false)
				for _, v := range p.Labels {
					ok = ok || glob(v, t.val, false)
				}
			}
			if !ok {
				return false
			}
		}
		return true
	}
}

// ---- IAM (stored, not enforced) ----

func (p *project) policy() *iampb.Policy {
	var cur *iampb.Policy
	if len(p.Policy) > 0 {
		cur = &iampb.Policy{}
		protojson.Unmarshal(p.Policy, cur)
	}
	return gapi.Policy(cur)
}

func (s *Service) GetIamPolicy(_ context.Context, req *iampb.GetIamPolicyRequest) (*iampb.Policy, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.find(req.Resource)
	if err != nil {
		return nil, err
	}
	return p.policy(), nil
}

func (s *Service) SetIamPolicy(_ context.Context, req *iampb.SetIamPolicyRequest) (*iampb.Policy, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.find(req.Resource)
	if err != nil {
		return nil, err
	}
	var cur *iampb.Policy
	if len(p.Policy) > 0 {
		cur = p.policy()
	}
	np, err := gapi.SetPolicy(cur, req.Policy)
	if err != nil {
		return nil, err
	}
	p.Policy, _ = protojson.Marshal(np)
	return proto.Clone(np).(*iampb.Policy), nil
}

// TestIamPermissions grants every permission asked for.
func (s *Service) TestIamPermissions(_ context.Context, req *iampb.TestIamPermissionsRequest) (*iampb.TestIamPermissionsResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.find(req.Resource); err != nil {
		return nil, err
	}
	return &iampb.TestIamPermissionsResponse{Permissions: req.Permissions}, nil
}

// ---- v1 ----

type v1Project struct {
	ProjectID string            `json:"projectId"`
	Name      string            `json:"name"`
	Labels    map[string]string `json:"labels"`
	Parent    *struct {
		Type string `json:"type"`
		ID   string `json:"id"`
	} `json:"parent"`
}

func (in *v1Project) parent() string {
	if in.Parent == nil || in.Parent.ID == "" {
		return ""
	}
	return in.Parent.Type + "s/" + in.Parent.ID
}

func decodeV1(r *http.Request) (*v1Project, error) {
	in := &v1Project{}
	if b := core.Body(r); len(b) > 0 {
		if err := json.Unmarshal(b, in); err != nil {
			return nil, gapi.Invalid("Invalid JSON payload received. %s", core.PlainJSON(err))
		}
	}
	return in, nil
}

func (s *Service) createV1(r *http.Request, _ map[string]string) (any, error) {
	in, err := decodeV1(r)
	if err != nil {
		return nil, err
	}
	p, err := s.create(in.ProjectID, in.Name, in.parent(), in.Labels)
	if err != nil {
		return nil, err
	}
	return s.ops().Done("", nil, p.proto()), nil
}

func (s *Service) getV1(_ *http.Request, v map[string]string) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.find(v["id"])
	if err != nil {
		return nil, err
	}
	return p.v1(), nil
}

func (s *Service) updateV1(r *http.Request, v map[string]string) (any, error) {
	in, err := decodeV1(r)
	if err != nil {
		return nil, err
	}
	if par := in.parent(); par != "" && !parentRE.MatchString(par) {
		return nil, gapi.Invalid("Invalid parent %q", par)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.find(v["id"])
	if err != nil {
		return nil, err
	}
	if in.Name != "" {
		p.DisplayName = in.Name
	}
	p.Labels = in.Labels
	if par := in.parent(); par != "" {
		p.Parent = par
	}
	p.touch()
	return p.v1(), nil
}

func (s *Service) deleteV1(_ *http.Request, v map[string]string) (any, error) {
	_, err := s.setDeleted(v["id"], true)
	return map[string]any{}, err
}

func (s *Service) undeleteV1(_ *http.Request, v map[string]string) (any, error) {
	_, err := s.setDeleted(v["id"], false)
	return map[string]any{}, err
}

func (s *Service) listV1(r *http.Request, _ map[string]string) (any, error) {
	q := r.URL.Query()
	size, err := strconv.ParseInt(q.Get("pageSize"), 10, 32)
	if err != nil && q.Get("pageSize") != "" || size < 0 {
		return nil, gapi.Invalid("Invalid value at 'page_size' (TYPE_INT32), %q", q.Get("pageSize"))
	}
	items, next := gapi.Page(s.search(q.Get("filter")), int32(size), q.Get("pageToken"))
	out := map[string]any{}
	var list []map[string]any
	for _, p := range items {
		list = append(list, p.v1())
	}
	if len(list) > 0 {
		out["projects"] = list
	}
	if next != "" {
		out["nextPageToken"] = next
	}
	return out, nil
}

// ---- console ----

func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := core.Table{Name: "Projects", Columns: []string{"Project ID", "Number", "Name", "Parent", "State", "Created"}}
	for _, id := range core.SortedKeys(s.st.Projects) {
		p := s.st.Projects[id]
		t.Rows = append(t.Rows, []string{id, p.Number, p.DisplayName, p.Parent, p.proto().State.String(), p.Created.Format(time.DateTime)})
	}
	return []core.Table{t}
}

func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := s.st.Projects[id]
	if table != "Projects" || p == nil {
		return nil
	}
	return map[string]any{"project": p.proto(), "policy": p.policy()}
}
