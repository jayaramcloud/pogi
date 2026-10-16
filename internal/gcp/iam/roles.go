package iam

import (
	"bytes"
	"context"
	"regexp"
	"strings"

	"cloud.google.com/go/iam/admin/apiv1/adminpb"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/gcp/gapi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type role struct {
	Name        string                       `json:"name"`
	Title       string                       `json:"title,omitempty"`
	Description string                       `json:"description,omitempty"`
	Perms       []string                     `json:"perms,omitempty"`
	Stage       adminpb.Role_RoleLaunchStage `json:"stage"`
	Etag        []byte                       `json:"etag"`
	Deleted     bool                         `json:"deleted,omitempty"`
}

func (r *role) proto(full bool) *adminpb.Role {
	p := &adminpb.Role{Name: r.Name, Title: r.Title, Description: r.Description, Stage: r.Stage, Etag: r.Etag, Deleted: r.Deleted}
	if full {
		p.IncludedPermissions = append([]string(nil), r.Perms...)
	}
	return p
}

// predefined are Google's predefined roles for the services pogi models (rolesdata.go); ListRoles,
// GetRole and the IAM authorizer all read this one table.
var predefined = loadRoles()

var predefinedByName = func() map[string]*role {
	m := map[string]*role{}
	for _, r := range predefined {
		m[r.Name] = r
	}
	return m
}()

var roleID = regexp.MustCompile(`^[a-zA-Z0-9_.]{3,64}$`)

func parentOK(p string) bool {
	k, id, ok := strings.Cut(p, "/")
	return ok && (k == "projects" || k == "organizations") && id != "" && !strings.Contains(id, "/")
}

// role finds a custom role (not deleted-filtered). Lock held.
func (s *Service) role(name string) (*role, error) {
	if r := s.st.Roles[name]; r != nil {
		return r, nil
	}
	return nil, gapi.NotFound("The role named %s was not found.", name)
}

func (s *Service) GetRole(_ context.Context, req *adminpb.GetRoleRequest) (*adminpb.Role, error) {
	for _, r := range predefined {
		if r.Name == req.Name {
			return r.proto(true), nil
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r, err := s.role(req.Name)
	if err != nil {
		return nil, err
	}
	return r.proto(true), nil
}

// ListRoles lists the predefined roles (no parent) or a project's or organization's custom ones.
func (s *Service) ListRoles(_ context.Context, req *adminpb.ListRolesRequest) (*adminpb.ListRolesResponse, error) {
	full := req.View == adminpb.RoleView_FULL
	var list []*adminpb.Role
	if req.Parent == "" {
		for _, r := range predefined {
			list = append(list, r.proto(full))
		}
	} else {
		if !parentOK(req.Parent) {
			return nil, gapi.Invalid("Invalid parent %q: want projects/{project} or organizations/{organization}", req.Parent)
		}
		s.mu.Lock()
		for _, n := range core.SortedKeys(s.st.Roles) {
			if r := s.st.Roles[n]; strings.HasPrefix(n, req.Parent+"/roles/") && (req.ShowDeleted || !r.Deleted) {
				list = append(list, r.proto(full))
			}
		}
		s.mu.Unlock()
	}
	items, next := gapi.Page(list, req.PageSize, req.PageToken)
	return &adminpb.ListRolesResponse{Roles: items, NextPageToken: next}, nil
}

func (s *Service) CreateRole(_ context.Context, req *adminpb.CreateRoleRequest) (*adminpb.Role, error) {
	if !parentOK(req.Parent) {
		return nil, gapi.Invalid("Invalid parent %q: want projects/{project} or organizations/{organization}", req.Parent)
	}
	if !roleID.MatchString(req.RoleId) {
		return nil, gapi.Invalid("Invalid role ID %q: use 3-64 letters, digits, _ or .", req.RoleId)
	}
	in := req.GetRole()
	r := &role{Name: req.Parent + "/roles/" + req.RoleId, Title: in.GetTitle(), Description: in.GetDescription(),
		Perms: in.GetIncludedPermissions(), Stage: in.GetStage(), Etag: newEtag()}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.st.Roles[r.Name] != nil {
		return nil, gapi.Exists("The role named %s already exists.", r.Name)
	}
	s.st.Roles[r.Name] = r
	return r.proto(true), nil
}

func checkRoleEtag(got, want []byte) error {
	if len(got) > 0 && !bytes.Equal(got, want) {
		return status.Error(codes.Aborted, "There were concurrent policy changes. Please retry the whole read-modify-write with exponential backoff.")
	}
	return nil
}

func (s *Service) UpdateRole(_ context.Context, req *adminpb.UpdateRoleRequest) (*adminpb.Role, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, err := s.role(req.Name)
	if err != nil {
		return nil, err
	}
	if err := checkRoleEtag(req.GetRole().GetEtag(), r.Etag); err != nil {
		return nil, err
	}
	p := r.proto(true)
	if err := gapi.Update(p, req.Role, req.GetUpdateMask().GetPaths(), "title", "description", "included_permissions", "stage"); err != nil {
		return nil, err
	}
	r.Title, r.Description, r.Perms, r.Stage, r.Etag = p.Title, p.Description, p.IncludedPermissions, p.Stage, newEtag()
	return r.proto(true), nil
}

// DeleteRole only marks the role deleted, as Google does for a week before it is gone for good.
func (s *Service) DeleteRole(_ context.Context, req *adminpb.DeleteRoleRequest) (*adminpb.Role, error) {
	return s.markRole(req.Name, req.Etag, true)
}

func (s *Service) UndeleteRole(_ context.Context, req *adminpb.UndeleteRoleRequest) (*adminpb.Role, error) {
	return s.markRole(req.Name, req.Etag, false)
}

func (s *Service) markRole(name string, etag []byte, deleted bool) (*adminpb.Role, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, err := s.role(name)
	if err != nil {
		return nil, err
	}
	if err := checkRoleEtag(etag, r.Etag); err != nil {
		return nil, err
	}
	r.Deleted, r.Etag = deleted, newEtag()
	return r.proto(true), nil
}
