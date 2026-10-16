package ecr_test

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io"
	"net"
	"net/http"
	"os/exec"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ecr"
	"github.com/aws/aws-sdk-go-v2/service/ecr/types"
	"github.com/nrndev/pogi"
	ecrsvc "github.com/nrndev/pogi/internal/aws/ecr"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/docker/dockertest"
)

const oci = "application/vnd.oci.image.manifest.v1+json"

func digest(b []byte) string { return fmt.Sprintf("sha256:%x", sha256.Sum256(b)) }

// manifest is an OCI image manifest of a config and one layer.
func manifest(config, layer []byte) []byte {
	return fmt.Appendf(nil, `{"schemaVersion":2,"mediaType":%q,"config":{"mediaType":"application/vnd.oci.image.config.v1+json","digest":%q,"size":%d},`+
		`"layers":[{"mediaType":"application/vnd.oci.image.layer.v1.tar+gzip","digest":%q,"size":%d}]}`, oci, digest(config), len(config), digest(layer), len(layer))
}

// call sends a request (header name/value pairs in hdr) and reads the answer.
func call(t *testing.T, method, url string, body io.Reader, hdr ...string) (*http.Response, string) {
	t.Helper()
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		t.Fatal(err)
	}
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

func TestRegistry(t *testing.T) {
	hub, url := core.ServeTest(t, func(h *core.Hub) core.Service { return ecrsvc.New(h) })
	c := ecr.NewFromConfig(awstest.Config(t, url))
	for _, n := range []string{"app/web", "other"} {
		must(c.CreateRepository(ctx, &ecr.CreateRepositoryInput{RepositoryName: aws.String(n)}))
	}
	base := url + "/v2/app/web"

	if resp, _ := call(t, "GET", url+"/v2/", nil); resp.StatusCode != 200 || resp.Header.Get("Docker-Distribution-API-Version") != "registry/2.0" {
		t.Fatalf("ping = %s", resp.Status)
	}
	if resp, body := call(t, "POST", url+"/v2/nope/blobs/uploads/", nil); resp.StatusCode != 404 || !strings.Contains(body, `"NAME_UNKNOWN"`) {
		t.Fatalf("push to a missing repository = %s %s", resp.Status, body) // ECR creates none on push
	}
	// API Gateway v2 shares /v2/, signed
	req, _ := http.NewRequest("GET", url+"/v2/apis", nil)
	awstest.Sign(t, req, "apigateway")
	if resp := must(http.DefaultClient.Do(req)); resp.Header.Get("Docker-Distribution-API-Version") != "" {
		t.Fatal("the registry took a signed API Gateway v2 call")
	}

	// a layer in chunks: POST, a chunked PATCH, one out of order, a PATCH, PUT ?digest=
	layer := bytes.Repeat([]byte("layer!"), 1000)
	resp, _ := call(t, "POST", base+"/blobs/uploads/", nil)
	loc := url + resp.Header.Get("Location")
	if resp.StatusCode != 202 || resp.Header.Get("Docker-Upload-UUID") == "" {
		t.Fatalf("start upload = %s %v", resp.Status, resp.Header)
	}
	call(t, "PATCH", loc, io.MultiReader(bytes.NewReader(layer[:100]))) // no length: sent chunked
	if resp, _ := call(t, "PATCH", loc, bytes.NewReader(layer[100:]), "Content-Range", "0-5"); resp.StatusCode != 416 {
		t.Fatalf("out of order chunk = %s", resp.Status)
	}
	resp, _ = call(t, "PATCH", loc, bytes.NewReader(layer[100:]), "Content-Range", fmt.Sprintf("100-%d", len(layer)-1))
	if resp.StatusCode != 202 || resp.Header.Get("Range") != fmt.Sprintf("0-%d", len(layer)-1) {
		t.Fatalf("chunk = %s range %q", resp.Status, resp.Header.Get("Range"))
	}
	if resp, _ := call(t, "GET", loc, nil); resp.StatusCode != 204 || resp.Header.Get("Range") != fmt.Sprintf("0-%d", len(layer)-1) {
		t.Fatalf("upload status = %s %v", resp.Status, resp.Header)
	}
	resp, _ = call(t, "PUT", loc+"?digest="+digest(layer), nil)
	if resp.StatusCode != 201 || resp.Header.Get("Docker-Content-Digest") != digest(layer) || resp.Header.Get("Location") != "/v2/app/web/blobs/"+digest(layer) {
		t.Fatalf("finish upload = %s %v", resp.Status, resp.Header)
	}

	// the config in one POST; a wrong digest is refused
	config := []byte(`{"architecture":"arm64","os":"linux"}`)
	if resp, body := call(t, "POST", base+"/blobs/uploads/?digest="+digest(layer), bytes.NewReader(config)); resp.StatusCode != 400 || !strings.Contains(body, `"DIGEST_INVALID"`) {
		t.Fatalf("wrong digest = %s %s", resp.Status, body)
	}
	if resp, _ := call(t, "POST", base+"/blobs/uploads/?digest="+digest(config), bytes.NewReader(config)); resp.StatusCode != 201 {
		t.Fatalf("monolithic upload = %s", resp.Status)
	}

	// manifests need their blobs; the same one goes under two tags
	if resp, body := call(t, "PUT", base+"/manifests/v1", bytes.NewReader(manifest(config, []byte("gone"))), "Content-Type", oci); resp.StatusCode != 400 ||
		!strings.Contains(body, `"MANIFEST_BLOB_UNKNOWN"`) {
		t.Fatalf("manifest without its layer = %s %s", resp.Status, body)
	}
	man := manifest(config, layer)
	for _, tag := range []string{"v1", "v2"} {
		if resp, _ := call(t, "PUT", base+"/manifests/"+tag, bytes.NewReader(man), "Content-Type", oci); resp.StatusCode != 201 || resp.Header.Get("Docker-Content-Digest") != digest(man) {
			t.Fatalf("push manifest = %s %v", resp.Status, resp.Header)
		}
	}
	roundTrip(t, hub) // manifests and blobs survive Save and Load

	// pull: the manifest by tag and by digest, the layer in part
	resp, _ = call(t, "HEAD", base+"/manifests/v1", nil)
	if resp.StatusCode != 200 || resp.Header.Get("Content-Type") != oci || resp.Header.Get("Docker-Content-Digest") != digest(man) || resp.ContentLength != int64(len(man)) {
		t.Fatalf("HEAD manifest = %s %v", resp.Status, resp.Header)
	}
	if _, body := call(t, "GET", base+"/manifests/"+digest(man), nil); body != string(man) {
		t.Fatalf("manifest = %s", body)
	}
	if resp, body := call(t, "GET", base+"/blobs/"+digest(layer), nil, "Range", "bytes=6-11"); resp.StatusCode != 206 || body != "layer!" {
		t.Fatalf("ranged blob = %s %q", resp.Status, body)
	}

	// a cross-repository mount; a blob the source lacks gets an upload instead
	resp, _ = call(t, "POST", url+"/v2/other/blobs/uploads/?mount="+digest(layer)+"&from=app/web", nil)
	if resp.StatusCode != 201 || resp.Header.Get("Location") != "/v2/other/blobs/"+digest(layer) {
		t.Fatalf("mount = %s %v", resp.Status, resp.Header)
	}
	if resp, _ := call(t, "HEAD", url+"/v2/other/blobs/"+digest(layer), nil); resp.StatusCode != 200 || resp.ContentLength != int64(len(layer)) {
		t.Fatalf("mounted blob = %s %d", resp.Status, resp.ContentLength)
	}
	if resp, _ := call(t, "POST", url+"/v2/other/blobs/uploads/?mount="+digest(config)+"&from=nope", nil); resp.StatusCode != 202 {
		t.Fatalf("mount from nowhere = %s", resp.Status)
	}

	// tags and the catalog page; the API sees the image
	resp, body := call(t, "GET", base+"/tags/list?n=1", nil)
	if body != `{"name":"app/web","tags":["v1"]}`+"\n" || resp.Header.Get("Link") != `</v2/app/web/tags/list?n=1&last=v1>; rel="next"` {
		t.Fatalf("tags = %s link %q", body, resp.Header.Get("Link"))
	}
	if _, body := call(t, "GET", base+"/tags/list?n=1&last=v1", nil); body != `{"name":"app/web","tags":["v2"]}`+"\n" {
		t.Fatalf("tags page 2 = %s", body)
	}
	if _, body := call(t, "GET", url+"/v2/_catalog", nil); body != `{"repositories":["app/web","other"]}`+"\n" {
		t.Fatalf("catalog = %s", body)
	}
	img := must(c.DescribeImages(ctx, &ecr.DescribeImagesInput{RepositoryName: aws.String("app/web")})).ImageDetails
	if len(img) != 1 || *img[0].ImageDigest != digest(man) || *img[0].ImageSizeInBytes != int64(len(layer)) || !slices.Equal(img[0].ImageTags, []string{"v1", "v2"}) {
		t.Fatalf("images = %+v", img)
	}

	// delete the manifest, then the blob; the mounted copy stays
	if resp, _ := call(t, "DELETE", base+"/manifests/"+digest(man), nil); resp.StatusCode != 202 {
		t.Fatalf("delete manifest = %s", resp.Status)
	}
	if resp, body := call(t, "GET", base+"/manifests/v1", nil); resp.StatusCode != 404 || !strings.Contains(body, `"MANIFEST_UNKNOWN"`) {
		t.Fatalf("deleted manifest = %s %s", resp.Status, body)
	}
	if resp, _ := call(t, "DELETE", base+"/blobs/"+digest(layer), nil); resp.StatusCode != 202 {
		t.Fatalf("delete blob = %s", resp.Status)
	}
	if resp, body := call(t, "GET", base+"/blobs/"+digest(layer), nil); resp.StatusCode != 404 || !strings.Contains(body, `"BLOB_UNKNOWN"`) {
		t.Fatalf("deleted blob = %s %s", resp.Status, body)
	}
	if resp, _ := call(t, "HEAD", url+"/v2/other/blobs/"+digest(layer), nil); resp.StatusCode != 200 {
		t.Fatalf("mounted blob after delete = %s", resp.Status)
	}
}

func freePort(t *testing.T) int {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

// answers waits until a registry answers.
func answers(t *testing.T, url string, wait time.Duration) {
	t.Helper()
	for end := time.Now().Add(wait); ; time.Sleep(200 * time.Millisecond) {
		resp, err := http.Get(url + "/v2/")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				return
			}
		}
		if time.Now().After(end) {
			t.Fatalf("%s does not answer: %v", url, err)
		}
	}
}

// Without a VM engine, pogi serves the registry port itself.
func TestRegistryPort(t *testing.T) {
	dockertest.Start(t, 0, true) // an engine that is not running
	port := freePort(t)
	srv := pogi.Start(t, pogi.Config{ECRPort: port})
	c := ecr.NewFromConfig(awstest.Config(t, srv.URL))
	repo := must(c.CreateRepository(ctx, &ecr.CreateRepositoryInput{RepositoryName: aws.String("app")})).Repository
	host := fmt.Sprintf("localhost:%d", port)
	auth := must(c.GetAuthorizationToken(ctx, &ecr.GetAuthorizationTokenInput{})).AuthorizationData[0]
	if *repo.RepositoryUri != host+"/app" || *auth.ProxyEndpoint != "http://"+host {
		t.Fatalf("uri %s, proxy endpoint %s", *repo.RepositoryUri, *auth.ProxyEndpoint)
	}
	answers(t, fmt.Sprintf("http://127.0.0.1:%d", port), 10*time.Second)
}

func run(t *testing.T, args ...string) {
	t.Helper()
	if out, err := exec.Command("docker", args...).CombinedOutput(); err != nil {
		t.Fatalf("docker %s: %v\n%s", strings.Join(args, " "), err, out)
	}
}

// docker push and pull through localhost:<ECRPort>, with a real engine.
func TestDockerPush(t *testing.T) {
	port := freePort(t)
	srv := pogi.Start(t, pogi.Config{ECRPort: port})
	if _, err := exec.LookPath("docker"); err != nil || !srv.Hub.Docker().Available() {
		t.Skip("no container engine")
	}
	c := ecr.NewFromConfig(awstest.Config(t, srv.URL))
	must(c.CreateRepository(ctx, &ecr.CreateRepositoryInput{RepositoryName: aws.String("demo")}))
	answers(t, fmt.Sprintf("http://127.0.0.1:%d", port), 3*time.Minute) // a forwarder container on Docker Desktop: its image may need a pull

	image := fmt.Sprintf("localhost:%d/demo:1", port)
	run(t, "tag", "alpine:3", image)
	t.Cleanup(func() { exec.Command("docker", "rmi", "-f", image).Run() })
	run(t, "push", image)
	run(t, "rmi", image)
	run(t, "pull", image)
	imgs := must(c.DescribeImages(ctx, &ecr.DescribeImagesInput{RepositoryName: aws.String("demo")})).ImageDetails
	if !slices.ContainsFunc(imgs, func(d types.ImageDetail) bool { return slices.Contains(d.ImageTags, "1") }) {
		t.Fatalf("images = %+v", imgs)
	}
}

// The Google Cloud Run and Cloud Tasks APIs live under /v2/projects/; the registry must not claim them.
func TestRegistryMatcher(t *testing.T) {
	match := ecrsvc.New(core.NewHub(core.Config{}, nil)).Info().Match
	for path, want := range map[string]bool{
		"/v2/": true, "/v2/_catalog": true, "/v2/app/tags/list": true, "/v2/team/app/manifests/latest": true, "/v2/app/blobs/uploads/": true,
		"/v2/projects/p/locations/l/services": false, "/v2/projects/p/locations/l/queues": false, "/v2/apis": false,
	} {
		r, _ := http.NewRequest("GET", "http://x"+path, nil)
		if got := match(r); got != want {
			t.Errorf("registry claims %s = %v, want %v", path, got, want)
		}
	}
}
