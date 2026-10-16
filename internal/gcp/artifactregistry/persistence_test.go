package artifactregistry_test

import (
	"strings"
	"testing"

	ar "cloud.google.com/go/artifactregistry/apiv1"
	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"
	"cloud.google.com/go/iam/apiv1/iampb"
	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/core"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
)

// Repositories and their IAM policies survive a save and a load, and the repository's names in the registry with them.
func TestPersistence(t *testing.T) {
	srv := pogi.Start(t, pogi.Config{Services: []string{"artifactregistry", "operations", "ecr"}})
	c := must(ar.NewClient(ctx, option.WithEndpoint(strings.TrimPrefix(srv.URL, "http://")), option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials()))))
	defer c.Close()
	name := parent + "/repositories/kept"
	must(c.CreateRepository(ctx, &artifactregistrypb.CreateRepositoryRequest{Parent: parent, RepositoryId: "kept",
		Repository: &artifactregistrypb.Repository{Format: artifactregistrypb.Repository_DOCKER, Description: "kept"}}))
	must(c.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{Resource: name, Policy: &iampb.Policy{
		Bindings: []*iampb.Binding{{Role: "roles/artifactregistry.reader", Members: []string{"user:a@example.com"}}}}}))
	svc := srv.Hub.Service("artifactregistry").(core.Stateful)
	saved := must(svc.Save())
	svc.Reset()
	if _, err := c.GetRepository(ctx, &artifactregistrypb.GetRepositoryRequest{Name: name}); code(err) != codes.NotFound {
		t.Fatalf("after a reset: %v", err)
	}
	if msg := push(t, srv.URL, "proj/kept/app", "v1"); msg == "" {
		t.Fatal("a reset keeps the repository's names")
	}
	if err := svc.Load(saved); err != nil {
		t.Fatal(err)
	}
	if got := must(c.GetRepository(ctx, &artifactregistrypb.GetRepositoryRequest{Name: name})); got.Description != "kept" {
		t.Fatalf("%+v", got)
	}
	if pol := must(c.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: name})); len(pol.Bindings) != 1 {
		t.Fatalf("%+v", pol)
	}
	if msg := push(t, srv.URL, "proj/kept/app", "v1"); msg != "" {
		t.Fatalf("push after a load: %s", msg)
	}
}
