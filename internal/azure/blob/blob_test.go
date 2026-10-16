package blob_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	azb "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/lease"
	"github.com/nrndev/pogi/internal/azure/blob"
	"github.com/nrndev/pogi/internal/core"
)

var ctx = context.Background()

// Azurite's well-known development account key.
const key = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func download(t *testing.T, c *azblob.Client, ctr, name string) string {
	t.Helper()
	res := must(c.DownloadStream(ctx, ctr, name, nil))
	defer res.Body.Close()
	return string(must(io.ReadAll(res.Body)))
}

func TestOfficialSDK(t *testing.T) {
	_, url := core.ServePort(t, func(h *core.Hub) core.Service { return blob.New(h) })
	conn := "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=" + key + ";BlobEndpoint=" + url + "/devstoreaccount1;"
	c := must(azblob.NewClientFromConnectionString(conn, nil))

	must(c.CreateContainer(ctx, "photos", &azblob.CreateContainerOptions{Metadata: map[string]*string{"env": to.Ptr("dev")}}))
	if _, err := c.CreateContainer(ctx, "photos", nil); !bloberror.HasCode(err, bloberror.ContainerAlreadyExists) {
		t.Fatalf("want ContainerAlreadyExists, got %v", err)
	}
	ctr := c.ServiceClient().NewContainerClient("photos")
	if p := must(ctr.GetProperties(ctx, nil)); *p.Metadata["Env"] != "dev" && *p.Metadata["env"] != "dev" {
		t.Fatalf("container metadata = %v", p.Metadata)
	}

	// Small upload (Put Blob) and a large one (staged blocks + commit).
	must(c.UploadBuffer(ctx, "photos", "albums/cat.txt", []byte("meow"), &azblob.UploadBufferOptions{
		Metadata: map[string]*string{"owner": to.Ptr("me")}, HTTPHeaders: &azb.HTTPHeaders{BlobContentType: to.Ptr("text/plain")}}))
	big := bytes.Repeat([]byte("pogi-"), 1<<20) // 5 MiB
	must(c.UploadBuffer(ctx, "photos", "big.bin", big, &azblob.UploadBufferOptions{BlockSize: 1 << 20, Concurrency: 4}))
	if got := download(t, c, "photos", "albums/cat.txt"); got != "meow" {
		t.Fatalf("download = %q", got)
	}
	if got := download(t, c, "photos", "big.bin"); got != string(big) {
		t.Fatalf("big download: %d bytes", len(got))
	}
	cat := ctr.NewBlobClient("albums/cat.txt")
	props := must(cat.GetProperties(ctx, nil))
	if *props.ContentType != "text/plain" || *props.ContentLength != 4 || props.Metadata == nil {
		t.Fatalf("props = %+v", props)
	}
	rng := must(cat.DownloadStream(ctx, &azb.DownloadStreamOptions{Range: azb.HTTPRange{Offset: 1, Count: 2}}))
	if b, _ := io.ReadAll(rng.Body); string(b) != "eo" {
		t.Fatalf("range = %q", b)
	}

	// Hierarchical listing.
	pager := ctr.NewListBlobsHierarchyPager("/", nil)
	var prefixes, names []string
	for pager.More() {
		page := must(pager.NextPage(ctx))
		for _, p := range page.Segment.BlobPrefixes {
			prefixes = append(prefixes, *p.Name)
		}
		for _, b := range page.Segment.BlobItems {
			names = append(names, *b.Name)
		}
	}
	if strings.Join(prefixes, ",") != "albums/" || strings.Join(names, ",") != "big.bin" {
		t.Fatalf("hierarchy = %v %v", prefixes, names)
	}

	// Tags, metadata, append blobs, copies, snapshots.
	must(cat.SetTags(ctx, map[string]string{"kind": "pet"}, nil))
	if tags := must(cat.GetTags(ctx, nil)); len(tags.BlobTagSet) != 1 || *tags.BlobTagSet[0].Value != "pet" {
		t.Fatalf("tags = %+v", tags.BlobTagSet)
	}
	must(cat.SetMetadata(ctx, map[string]*string{"owner": to.Ptr("you")}, nil))
	logs := ctr.NewAppendBlobClient("app.log")
	must(logs.Create(ctx, nil))
	must(logs.AppendBlock(ctx, nopCloser("one\n"), nil))
	must(logs.AppendBlock(ctx, nopCloser("two\n"), nil))
	if got := download(t, c, "photos", "app.log"); got != "one\ntwo\n" {
		t.Fatalf("append blob = %q", got)
	}
	copyRes := must(ctr.NewBlobClient("copy.txt").StartCopyFromURL(ctx, cat.URL(), nil))
	if *copyRes.CopyStatus != "success" || download(t, c, "photos", "copy.txt") != "meow" {
		t.Fatalf("copy = %v", *copyRes.CopyStatus)
	}
	snap := must(cat.CreateSnapshot(ctx, nil))
	must(c.UploadBuffer(ctx, "photos", "albums/cat.txt", []byte("purr"), nil))
	old := must(cat.WithSnapshot(*snap.Snapshot))
	sr := must(old.DownloadStream(ctx, nil))
	if b, _ := io.ReadAll(sr.Body); string(b) != "meow" {
		t.Fatalf("snapshot = %q", b)
	}

	// Leases block unleased writes.
	lc := must(lease.NewBlobClient(ctr.NewBlobClient("big.bin"), nil))
	must(lc.AcquireLease(ctx, -1, nil))
	if _, err := c.UploadBuffer(ctx, "photos", "big.bin", []byte("x"), nil); !bloberror.HasCode(err, bloberror.LeaseIDMissing) {
		t.Fatalf("want LeaseIdMissing, got %v", err)
	}
	must(lc.ReleaseLease(ctx, nil))

	// Deletes.
	must(c.DeleteBlob(ctx, "photos", "copy.txt", nil))
	if _, err := c.DownloadStream(ctx, "photos", "copy.txt", nil); !bloberror.HasCode(err, bloberror.BlobNotFound) {
		t.Fatalf("want BlobNotFound, got %v", err)
	}
	must(c.DeleteContainer(ctx, "photos", nil))
	if _, err := ctr.GetProperties(ctx, &container.GetPropertiesOptions{}); !bloberror.HasCode(err, bloberror.ContainerNotFound) {
		t.Fatalf("want ContainerNotFound, got %v", err)
	}
}

type readSeekCloser struct{ *strings.Reader }

func (readSeekCloser) Close() error { return nil }

func nopCloser(s string) readSeekCloser { return readSeekCloser{strings.NewReader(s)} }

// A failed HEAD has no body, so it must not claim to be XML: the .NET storage
// client (inside the Azure Functions host) parses an empty "application/xml"
// body and throws instead of reading x-ms-error-code.
func TestHeadErrorHasNoContentType(t *testing.T) {
	_, url := core.ServePort(t, func(h *core.Hub) core.Service { return blob.New(h) })
	resp, err := http.Head(url + "/devstoreaccount1/nope/blob")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 404 || resp.Header.Get("x-ms-error-code") != "ContainerNotFound" || resp.Header.Get("Content-Type") != "" {
		t.Fatalf("HEAD error: %d %v", resp.StatusCode, resp.Header)
	}
}

// A page blob is allocated at the size its header names, so the header is capped at the upload
// limit; an absurd or negative size must not crash pogi.
func TestPageBlobSizeCap(t *testing.T) {
	_, url := core.ServePort(t, func(h *core.Hub) core.Service { return blob.New(h) })
	put := func(path, size string) (int, string) {
		req, _ := http.NewRequest(http.MethodPut, url+"/devstoreaccount1/"+path, nil)
		if size != "" {
			req.Header.Set("x-ms-blob-type", "PageBlob")
			req.Header.Set("x-ms-blob-content-length", size)
		}
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(res.Body)
		res.Body.Close()
		return res.StatusCode, string(b)
	}
	if code, _ := put("pages?restype=container", ""); code != 201 {
		t.Fatalf("create container = %d", code)
	}
	if code, body := put("pages/huge", "1099511627776"); code != 413 || !strings.Contains(body, "limited to 10 MB") { // 1 TiB
		t.Fatalf("1 TiB page blob = %d %s", code, body)
	}
	if code, _ := put("pages/neg", "-512"); code != 400 {
		t.Fatalf("negative size = %d", code)
	}
	if code, _ := put("pages/ok", "1048576"); code != 201 {
		t.Fatalf("1 MiB page blob = %d", code)
	}
}

// A snapshot id that is not a timestamp and a copy source that is not a URL are 400s, not 404s.
func TestBadSnapshotAndCopySource(t *testing.T) {
	_, url := core.ServePort(t, func(h *core.Hub) core.Service { return blob.New(h) })
	do := func(method, path string, hdr ...string) int {
		req, _ := http.NewRequest(method, url+"/devstoreaccount1/"+path, strings.NewReader(""))
		for i := 0; i+1 < len(hdr); i += 2 {
			req.Header.Set(hdr[i], hdr[i+1])
		}
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		return res.StatusCode
	}
	do("PUT", "c?restype=container")
	do("PUT", "c/b", "x-ms-blob-type", "BlockBlob")
	for name, got := range map[string]int{
		"get":    do("GET", "c/b?snapshot=garbage"),
		"delete": do("DELETE", "c/b?snapshot=garbage"),
		"copy":   do("PUT", "c/b2", "x-ms-copy-source", "garbage"),
	} {
		if got != 400 {
			t.Errorf("%s = %d, want 400", name, got)
		}
	}
	if got := do("GET", "c/b?snapshot=2020-01-01T00:00:00.0000000Z"); got != 404 {
		t.Errorf("unknown snapshot = %d, want 404", got)
	}
}
