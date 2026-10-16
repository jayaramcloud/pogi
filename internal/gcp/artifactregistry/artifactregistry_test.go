package artifactregistry_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	ar "cloud.google.com/go/artifactregistry/apiv1"
	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"
	"cloud.google.com/go/iam/apiv1/iampb"
	"github.com/nrndev/pogi"
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

const parent = "projects/proj/locations/us-central1"

func digest(b []byte) string { return fmt.Sprintf("sha256:%x", sha256.Sum256(b)) }

func call(t *testing.T, method, url string, body []byte, hdr ...string) (*http.Response, string) {
	t.Helper()
	req, _ := http.NewRequest(method, url, bytes.NewReader(body))
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp, string(b)
}

// push stores an image of one layer as name:tag, the way docker push does.
func push(t *testing.T, base, name, tag string) string {
	t.Helper()
	config, layer := []byte(`{"architecture":"amd64","os":"linux"}`), []byte("a layer of "+name)
	for _, blob := range [][]byte{config, layer} {
		if resp, body := call(t, "POST", base+"/v2/"+name+"/blobs/uploads/?digest="+url.QueryEscape(digest(blob)), blob); resp.StatusCode != 201 {
			return fmt.Sprintf("%s %s", resp.Status, body)
		}
	}
	m := fmt.Appendf(nil, `{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{"mediaType":"application/vnd.oci.image.config.v1+json","digest":%q,"size":%d},`+
		`"layers":[{"mediaType":"application/vnd.oci.image.layer.v1.tar","digest":%q,"size":%d}]}`, digest(config), len(config), digest(layer), len(layer))
	resp, body := call(t, "PUT", base+"/v2/"+name+"/manifests/"+tag, m, "Content-Type", "application/vnd.oci.image.manifest.v1+json")
	if resp.StatusCode != 201 {
		return fmt.Sprintf("%s %s", resp.Status, body)
	}
	return ""
}

func code(err error) codes.Code { return status.Code(err) }

func TestRepositories(t *testing.T) {
	srv := pogi.Start(t, pogi.Config{Services: []string{"artifactregistry", "operations", "ecr"}})
	c := must(ar.NewClient(ctx, option.WithEndpoint(strings.TrimPrefix(srv.URL, "http://")), option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials()))))
	defer c.Close()

	// 1. create (a long-running operation), get, list
	op := must(c.CreateRepository(ctx, &artifactregistrypb.CreateRepositoryRequest{Parent: parent, RepositoryId: "apps",
		Repository: &artifactregistrypb.Repository{Format: artifactregistrypb.Repository_DOCKER, Description: "images", Labels: map[string]string{"env": "test"}}}))
	repo := must(op.Wait(ctx))
	name := parent + "/repositories/apps"
	if repo.Name != name || repo.Format != artifactregistrypb.Repository_DOCKER || repo.RegistryUri != "us-central1-docker.pkg.dev/proj/apps" ||
		repo.Mode != artifactregistrypb.Repository_STANDARD_REPOSITORY || repo.CreateTime == nil {
		t.Fatalf("%+v", repo)
	}
	got := must(c.GetRepository(ctx, &artifactregistrypb.GetRepositoryRequest{Name: name}))
	if got.Description != "images" || got.Labels["env"] != "test" {
		t.Fatalf("%+v", got)
	}
	must(c.CreateRepository(ctx, &artifactregistrypb.CreateRepositoryRequest{Parent: parent, RepositoryId: "maven-libs",
		Repository: &artifactregistrypb.Repository{Format: artifactregistrypb.Repository_MAVEN}}))
	var names []string
	for it := c.ListRepositories(ctx, &artifactregistrypb.ListRepositoriesRequest{Parent: parent}); ; {
		r, err := it.Next()
		if err != nil {
			break
		}
		names = append(names, r.Name)
	}
	if len(names) != 2 || names[0] != name {
		t.Fatalf("repositories %v", names)
	}

	// 2. errors: a second repository of that id, a missing format, a bad id
	_, err := c.CreateRepository(ctx, &artifactregistrypb.CreateRepositoryRequest{Parent: parent, RepositoryId: "apps", Repository: &artifactregistrypb.Repository{Format: artifactregistrypb.Repository_DOCKER}})
	if code(err) != codes.AlreadyExists {
		t.Fatalf("duplicate: %v", err)
	}
	_, err = c.CreateRepository(ctx, &artifactregistrypb.CreateRepositoryRequest{Parent: parent, RepositoryId: "nofmt", Repository: &artifactregistrypb.Repository{}})
	if code(err) != codes.InvalidArgument {
		t.Fatalf("no format: %v", err)
	}
	_, err = c.CreateRepository(ctx, &artifactregistrypb.CreateRepositoryRequest{Parent: parent, RepositoryId: "Bad_ID", Repository: &artifactregistrypb.Repository{Format: artifactregistrypb.Repository_DOCKER}})
	if code(err) != codes.InvalidArgument {
		t.Fatalf("bad id: %v", err)
	}
	if _, err = c.GetRepository(ctx, &artifactregistrypb.GetRepositoryRequest{Name: parent + "/repositories/none"}); code(err) != codes.NotFound {
		t.Fatalf("missing: %v", err)
	}

	// 3. update with a mask: only the named fields change
	up := must(c.UpdateRepository(ctx, &artifactregistrypb.UpdateRepositoryRequest{
		Repository: &artifactregistrypb.Repository{Name: name, Description: "new", Labels: map[string]string{"x": "y"}},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"description"}}}))
	if up.Description != "new" || up.Labels["env"] != "test" || up.Labels["x"] != "" || up.UpdateTime == nil {
		t.Fatalf("%+v", up)
	}

	// 4. docker images: what is pushed under project/repository/ is listed, with its tags
	if msg := push(t, srv.URL, "proj/apps/team/web", "v1"); msg != "" {
		t.Fatal(msg)
	}
	if msg := push(t, srv.URL, "proj/apps/api", "latest"); msg != "" {
		t.Fatal(msg)
	}
	if msg := push(t, srv.URL, "proj/other/api", "latest"); msg == "" { // no such repository
		t.Fatal("a push outside every repository worked")
	}
	var uris []string
	for it := c.ListDockerImages(ctx, &artifactregistrypb.ListDockerImagesRequest{Parent: name}); ; {
		img, err := it.Next()
		if err != nil {
			break
		}
		uris = append(uris, strings.Split(img.Uri, "@")[0]+" "+strings.Join(img.Tags, ","))
		if img.ImageSizeBytes == 0 || img.UploadTime == nil || !strings.Contains(img.Name, "/dockerImages/") {
			t.Fatalf("%+v", img)
		}
	}
	if strings.Join(uris, ";") != "us-central1-docker.pkg.dev/proj/apps/api latest;us-central1-docker.pkg.dev/proj/apps/team/web v1" {
		t.Fatalf("images %v", uris)
	}

	// 5. IAM: set, read back, a stale etag is refused
	pol := must(c.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: name}))
	set := must(c.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{Resource: name, Policy: &iampb.Policy{Etag: pol.Etag,
		Bindings: []*iampb.Binding{{Role: "roles/artifactregistry.reader", Members: []string{"user:a@example.com"}}}}}))
	if back := must(c.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: name})); len(back.Bindings) != 1 || string(back.Etag) != string(set.Etag) {
		t.Fatalf("%+v", back)
	}
	if _, err = c.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{Resource: name, Policy: &iampb.Policy{Etag: pol.Etag}}); code(err) != codes.Aborted {
		t.Fatalf("stale etag: %v", err)
	}

	// 6. the REST mirror answers the same
	if resp, body := call(t, "GET", srv.URL+"/v1/"+parent+"/repositories/apps/dockerImages", nil); resp.StatusCode != 200 || !strings.Contains(body, "us-central1-docker.pkg.dev/proj/apps/api@sha256:") {
		t.Fatalf("REST list = %s %s", resp.Status, body)
	}

	// 7. delete takes the images; the repository is gone, and so is its place in the registry
	must(c.DeleteRepository(ctx, &artifactregistrypb.DeleteRepositoryRequest{Name: name})).Wait(ctx)
	if _, err = c.GetRepository(ctx, &artifactregistrypb.GetRepositoryRequest{Name: name}); code(err) != codes.NotFound {
		t.Fatalf("after delete: %v", err)
	}
	if msg := push(t, srv.URL, "proj/apps/api", "again"); msg == "" {
		t.Fatal("pushed to a deleted repository")
	}
	if _, err = c.DeleteRepository(ctx, &artifactregistrypb.DeleteRepositoryRequest{Name: name}); code(err) != codes.NotFound {
		t.Fatalf("delete twice: %v", err)
	}
}
