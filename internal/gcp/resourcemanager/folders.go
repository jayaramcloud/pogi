package resourcemanager

import (
	"context"
	"slices"
	"strings"
	"time"

	pb "cloud.google.com/go/resourcemanager/apiv3/resourcemanagerpb"
	"github.com/nrndev/pogi/internal/gcp/gapi"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Folders and organizations are read-only: nothing creates them, so one
// exists exactly while some project names it as its parent.
// ponytail: no hierarchy between them, and no IAM on them; give them state if tests need to build trees.

type folders struct {
	pb.UnimplementedFoldersServer
	s *Service
}

type orgs struct {
	pb.UnimplementedOrganizationsServer
	s *Service
}

var folderRules = []gapi.Rule{
	{HTTP: "GET /v3/folders", Method: "ListFolders"},
	{HTTP: "GET /v3/folders:search", Method: "SearchFolders"},
	{HTTP: "GET /v3/{name=folders/*}", Method: "GetFolder"},
}

var orgRules = []gapi.Rule{
	{HTTP: "GET /v3/organizations:search", Method: "SearchOrganizations"},
	{HTTP: "GET /v3/{name=organizations/*}", Method: "GetOrganization"},
}

// parents lists the folders or organizations (prefix "folders/" or "organizations/") that projects hang from, with when the first did.
func (s *Service) parents(prefix string) (names []string, since map[string]time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	since = map[string]time.Time{}
	for _, p := range s.st.Projects {
		if t, ok := since[p.Parent]; strings.HasPrefix(p.Parent, prefix) && (!ok || p.Created.Before(t)) {
			since[p.Parent] = p.Created
		}
	}
	for n := range since {
		names = append(names, n)
	}
	slices.Sort(names)
	return names, since
}

func (f *folders) folder(name string, t time.Time) *pb.Folder {
	return &pb.Folder{Name: name, DisplayName: strings.Replace(name, "/", "-", 1), State: pb.Folder_ACTIVE, CreateTime: timestamppb.New(t), UpdateTime: timestamppb.New(t)}
}

func (f *folders) GetFolder(_ context.Context, req *pb.GetFolderRequest) (*pb.Folder, error) {
	_, since := f.s.parents("folders/")
	t, ok := since[req.Name]
	if !ok {
		return nil, gapi.NotFound("Folder %s was not found, or the caller does not have permission to see it.", req.Name)
	}
	return f.folder(req.Name, t), nil
}

// ListFolders is always empty: folders have no known parent.
func (f *folders) ListFolders(context.Context, *pb.ListFoldersRequest) (*pb.ListFoldersResponse, error) {
	return &pb.ListFoldersResponse{}, nil
}

func (f *folders) SearchFolders(_ context.Context, req *pb.SearchFoldersRequest) (*pb.SearchFoldersResponse, error) {
	names, since := f.s.parents("folders/")
	out := &pb.SearchFoldersResponse{}
	for _, n := range names {
		out.Folders = append(out.Folders, f.folder(n, since[n]))
	}
	return out, nil
}

func (o *orgs) org(name string, t time.Time) *pb.Organization {
	return &pb.Organization{Name: name, DisplayName: strings.TrimPrefix(name, "organizations/") + ".example.com", State: pb.Organization_ACTIVE,
		CreateTime: timestamppb.New(t), UpdateTime: timestamppb.New(t)}
}

func (o *orgs) GetOrganization(_ context.Context, req *pb.GetOrganizationRequest) (*pb.Organization, error) {
	_, since := o.s.parents("organizations/")
	t, ok := since[req.Name]
	if !ok {
		return nil, gapi.NotFound("Organization %s was not found, or the caller does not have permission to see it.", req.Name)
	}
	return o.org(req.Name, t), nil
}

func (o *orgs) SearchOrganizations(context.Context, *pb.SearchOrganizationsRequest) (*pb.SearchOrganizationsResponse, error) {
	names, since := o.s.parents("organizations/")
	out := &pb.SearchOrganizationsResponse{}
	for _, n := range names {
		out.Organizations = append(out.Organizations, o.org(n, since[n]))
	}
	return out, nil
}
