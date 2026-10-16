package resourcemanager_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"cloud.google.com/go/iam/apiv1/iampb"
	rm "cloud.google.com/go/resourcemanager/apiv3"
	pb "cloud.google.com/go/resourcemanager/apiv3/resourcemanagerpb"
	"github.com/nrndev/pogi"
	crm1 "google.golang.org/api/cloudresourcemanager/v1"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

var ctx = context.Background()

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func start(t *testing.T) *pogi.Running {
	t.Helper()
	return pogi.Start(t, pogi.Config{Services: []string{"resourcemanager", "operations"}})
}

func wantCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if status.Code(err) != want {
		t.Fatalf("got %v, want %v", err, want)
	}
}

// TestProjectsV3 drives the v3 gRPC client: long-running operations, IAM, listing.
func TestProjectsV3(t *testing.T) {
	s := start(t)
	c := must(rm.NewProjectsClient(ctx, option.WithEndpoint(strings.TrimPrefix(s.URL, "http://")), option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials()))))
	defer c.Close()

	op := must(c.CreateProject(ctx, &pb.CreateProjectRequest{Project: &pb.Project{ProjectId: "my-project-1", DisplayName: "My Project",
		Parent: "folders/123", Labels: map[string]string{"env": "dev"}}}))
	p := must(op.Wait(ctx))
	if !strings.HasPrefix(p.Name, "projects/") || len(p.Name) != len("projects/")+12 || p.ProjectId != "my-project-1" ||
		p.State != pb.Project_ACTIVE || p.Parent != "folders/123" || p.CreateTime == nil || p.Etag == "" {
		t.Fatalf("created %v", p)
	}
	_, err := c.CreateProject(ctx, &pb.CreateProjectRequest{Project: &pb.Project{ProjectId: "my-project-1"}})
	wantCode(t, err, codes.AlreadyExists)
	_, err = c.CreateProject(ctx, &pb.CreateProjectRequest{Project: &pb.Project{ProjectId: "X"}})
	wantCode(t, err, codes.InvalidArgument)

	// by number and by id
	if g := must(c.GetProject(ctx, &pb.GetProjectRequest{Name: p.Name})); g.ProjectId != p.ProjectId {
		t.Fatalf("by number %v", g)
	}
	if g := must(c.GetProject(ctx, &pb.GetProjectRequest{Name: "projects/my-project-1"})); g.Name != p.Name {
		t.Fatalf("by id %v", g)
	}
	_, err = c.GetProject(ctx, &pb.GetProjectRequest{Name: "projects/nope-project"})
	wantCode(t, err, codes.NotFound)

	// IAM policy: set, get back, stale etag refused
	empty := must(c.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: p.Name}))
	if len(empty.Bindings) != 0 || len(empty.Etag) == 0 {
		t.Fatalf("empty %v", empty)
	}
	b := []*iampb.Binding{{Role: "roles/viewer", Members: []string{"user:a@example.com", "serviceAccount:s@p.iam.gserviceaccount.com"}}}
	set := must(c.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{Resource: p.Name, Policy: &iampb.Policy{Version: 3, Bindings: b, Etag: empty.Etag}}))
	got := must(c.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: "projects/my-project-1"}))
	if len(got.Bindings) != 1 || len(got.Bindings[0].Members) != 2 || string(got.Etag) != string(set.Etag) || string(got.Etag) == string(empty.Etag) {
		t.Fatalf("policy round trip %v / %v", set, got)
	}
	_, err = c.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{Resource: p.Name, Policy: &iampb.Policy{Etag: empty.Etag}})
	wantCode(t, err, codes.Aborted)

	// update, move
	uop := must(c.UpdateProject(ctx, &pb.UpdateProjectRequest{Project: &pb.Project{Name: p.Name, DisplayName: "Renamed"},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"display_name"}}}))
	if u := must(uop.Wait(ctx)); u.DisplayName != "Renamed" || u.Labels["env"] != "dev" || u.Etag == p.Etag {
		t.Fatalf("updated %v", u)
	}
	mop := must(c.MoveProject(ctx, &pb.MoveProjectRequest{Name: p.Name, DestinationParent: "organizations/9"}))
	if m := must(mop.Wait(ctx)); m.Parent != "organizations/9" {
		t.Fatalf("moved %v", m)
	}

	// list and search
	must(must(c.CreateProject(ctx, &pb.CreateProjectRequest{Project: &pb.Project{ProjectId: "other-proj-2", DisplayName: "Other", Parent: "organizations/9"}})).Wait(ctx))
	ids := func(it *rm.ProjectIterator) (out []string) {
		for {
			p, err := it.Next()
			if err == iterator.Done {
				return
			}
			out = append(out, must(p, err).ProjectId)
		}
	}
	if got := ids(c.ListProjects(ctx, &pb.ListProjectsRequest{Parent: "organizations/9"})); len(got) != 2 {
		t.Fatalf("list %v", got)
	}
	if got := ids(c.SearchProjects(ctx, &pb.SearchProjectsRequest{Query: "id:my-project-*"})); len(got) != 1 || got[0] != "my-project-1" {
		t.Fatalf("search id %v", got)
	}
	if got := ids(c.SearchProjects(ctx, &pb.SearchProjectsRequest{Query: "displayName:other labels.env:dev"})); len(got) != 0 {
		t.Fatalf("search both %v", got)
	}
	if got := ids(c.SearchProjects(ctx, &pb.SearchProjectsRequest{Query: "labels.env:dev parent:organizations/9"})); len(got) != 1 {
		t.Fatalf("search labels %v", got)
	}

	// delete hides the project from lists, keeps it readable as DELETE_REQUESTED, and can be undone
	d := must(must(c.DeleteProject(ctx, &pb.DeleteProjectRequest{Name: p.Name})).Wait(ctx))
	if d.State != pb.Project_DELETE_REQUESTED || d.DeleteTime == nil {
		t.Fatalf("deleted %v", d)
	}
	if got := ids(c.ListProjects(ctx, &pb.ListProjectsRequest{Parent: "organizations/9"})); len(got) != 1 {
		t.Fatalf("list after delete %v", got)
	}
	if got := ids(c.ListProjects(ctx, &pb.ListProjectsRequest{Parent: "organizations/9", ShowDeleted: true})); len(got) != 2 {
		t.Fatalf("list with deleted %v", got)
	}
	if u := must(must(c.UndeleteProject(ctx, &pb.UndeleteProjectRequest{Name: p.Name})).Wait(ctx)); u.State != pb.Project_ACTIVE {
		t.Fatalf("undeleted %v", u)
	}

	// organizations and folders exist while a project hangs from them
	oc := must(rm.NewOrganizationsClient(ctx, option.WithEndpoint(strings.TrimPrefix(s.URL, "http://")), option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials()))))
	defer oc.Close()
	if o := must(oc.GetOrganization(ctx, &pb.GetOrganizationRequest{Name: "organizations/9"})); o.State != pb.Organization_ACTIVE {
		t.Fatalf("org %v", o)
	}
	_, err = oc.GetOrganization(ctx, &pb.GetOrganizationRequest{Name: "organizations/77"})
	wantCode(t, err, codes.NotFound)
	fc := must(rm.NewFoldersClient(ctx, option.WithEndpoint(strings.TrimPrefix(s.URL, "http://")), option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials()))))
	defer fc.Close()
	_, err = fc.GetFolder(ctx, &pb.GetFolderRequest{Name: "folders/123"}) // the only project there moved away
	wantCode(t, err, codes.NotFound)
	must(must(c.CreateProject(ctx, &pb.CreateProjectRequest{Project: &pb.Project{ProjectId: "third-proj-3", Parent: "folders/123"}})).Wait(ctx))
	if f := must(fc.GetFolder(ctx, &pb.GetFolderRequest{Name: "folders/123"})); f.Name != "folders/123" {
		t.Fatalf("folder %v", f)
	}
}

func wantHTTP(t *testing.T, err error, want int) {
	t.Helper()
	var ge *googleapi.Error
	if !errors.As(err, &ge) || ge.Code != want {
		t.Fatalf("got %v, want HTTP %d", err, want)
	}
}

// TestProjectsV1 is the REST client of the v1 API, the one Terraform's google provider calls.
func TestProjectsV1(t *testing.T) {
	s := start(t)
	svc := must(crm1.NewService(ctx, option.WithEndpoint(s.URL), option.WithoutAuthentication()))

	op := must(svc.Projects.Create(&crm1.Project{ProjectId: "tf-project-1", Name: "TF Project",
		Parent: &crm1.ResourceId{Type: "organization", Id: "555"}, Labels: map[string]string{"team": "a"}}).Do())
	if op.Name == "" || !op.Done {
		t.Fatalf("operation %+v", op)
	}
	if got := must(svc.Operations.Get(op.Name).Do()); !got.Done || got.Error != nil {
		t.Fatalf("polled operation %+v", got)
	}
	_, err := svc.Projects.Create(&crm1.Project{ProjectId: "tf-project-1"}).Do()
	wantHTTP(t, err, 409)

	p := must(svc.Projects.Get("tf-project-1").Do())
	if p.ProjectId != "tf-project-1" || p.Name != "TF Project" || p.LifecycleState != "ACTIVE" || p.ProjectNumber == 0 ||
		p.Parent.Type != "organization" || p.Parent.Id != "555" || p.Labels["team"] != "a" || p.CreateTime == "" {
		t.Fatalf("project %+v", p)
	}
	_, err = svc.Projects.Get("no-such-project").Do()
	wantHTTP(t, err, 404)

	// google_project_iam_member: read the policy, add a binding, write it back, read again
	pol := must(svc.Projects.GetIamPolicy("tf-project-1", &crm1.GetIamPolicyRequest{Options: &crm1.GetPolicyOptions{RequestedPolicyVersion: 3}}).Do())
	pol.Bindings = append(pol.Bindings, &crm1.Binding{Role: "roles/editor", Members: []string{"serviceAccount:sa@tf-project-1.iam.gserviceaccount.com"}})
	pol.Version = 3
	set := must(svc.Projects.SetIamPolicy("tf-project-1", &crm1.SetIamPolicyRequest{Policy: pol}).Do())
	back := must(svc.Projects.GetIamPolicy("tf-project-1", &crm1.GetIamPolicyRequest{}).Do())
	if len(back.Bindings) != 1 || back.Bindings[0].Role != "roles/editor" || back.Bindings[0].Members[0] != "serviceAccount:sa@tf-project-1.iam.gserviceaccount.com" ||
		back.Etag != set.Etag || back.Etag == pol.Etag {
		t.Fatalf("policy %+v after %+v", back, set)
	}
	_, err = svc.Projects.SetIamPolicy("tf-project-1", &crm1.SetIamPolicyRequest{Policy: pol}).Do() // pol's etag is stale now
	wantHTTP(t, err, 409)
	if perms := must(svc.Projects.TestIamPermissions("tf-project-1", &crm1.TestIamPermissionsRequest{Permissions: []string{"a.b.c"}}).Do()); len(perms.Permissions) != 1 {
		t.Fatalf("perms %+v", perms)
	}

	// update replaces name and labels
	up := must(svc.Projects.Update("tf-project-1", &crm1.Project{ProjectId: "tf-project-1", Name: "Renamed", Labels: map[string]string{"team": "b"},
		Parent: &crm1.ResourceId{Type: "folder", Id: "42"}}).Do())
	if up.Name != "Renamed" || up.Labels["team"] != "b" || up.Parent.Type != "folder" || up.Parent.Id != "42" {
		t.Fatalf("updated %+v", up)
	}

	must(svc.Projects.Create(&crm1.Project{ProjectId: "tf-project-2", Name: "Second"}).Do())
	if l := must(svc.Projects.List().Filter("id:tf-project-*").Do()); len(l.Projects) != 2 {
		t.Fatalf("list %+v", l.Projects)
	}
	if l := must(svc.Projects.List().Filter("parent.type:folder parent.id:42").Do()); len(l.Projects) != 1 || l.Projects[0].ProjectId != "tf-project-1" {
		t.Fatalf("list by parent %+v", l.Projects)
	}
	if l := must(svc.Projects.List().PageSize(1).Do()); len(l.Projects) != 1 || l.NextPageToken == "" {
		t.Fatalf("page %+v", l)
	}

	must(svc.Projects.Delete("tf-project-1").Do())
	if g := must(svc.Projects.Get("tf-project-1").Do()); g.LifecycleState != "DELETE_REQUESTED" {
		t.Fatalf("after delete %+v", g)
	}
	if l := must(svc.Projects.List().Do()); len(l.Projects) != 2 { // tf-project-2 and the default project
		t.Fatalf("deleted project listed: %+v", l.Projects)
	}
	must(svc.Projects.Undelete("tf-project-1", &crm1.UndeleteProjectRequest{}).Do())
	if g := must(svc.Projects.Get("tf-project-1").Do()); g.LifecycleState != "ACTIVE" {
		t.Fatalf("after undelete %+v", g)
	}
}
