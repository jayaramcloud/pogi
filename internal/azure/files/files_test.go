package files_test

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/file"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/fileerror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/service"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/share"
	"github.com/nrndev/pogi/internal/azure/files"
	"github.com/nrndev/pogi/internal/core"
)

var ctx = context.Background()

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func wantCode(t *testing.T, err error, code fileerror.Code) {
	t.Helper()
	if !fileerror.HasCode(err, code) {
		t.Fatalf("want %s, got %v", code, err)
	}
}

// metaOf reads a metadata value; the SDK may spell the key either way.
func metaOf(m map[string]*string, k string) string {
	for n, v := range m {
		if strings.EqualFold(n, k) && v != nil {
			return *v
		}
	}
	return ""
}

func body(s string) io.ReadSeekCloser { return streaming.NopCloser(bytes.NewReader([]byte(s))) }

func TestOfficialSDK(t *testing.T) {
	_, url := core.ServePort(t, func(h *core.Hub) core.Service { return files.New(h) })
	conn := "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=" + core.AzuriteKey + ";FileEndpoint=" + url + "/devstoreaccount1;"
	svc := must(service.NewClientFromConnectionString(conn, nil))

	// shares
	sh := svc.NewShareClient("docs")
	must(sh.Create(ctx, &share.CreateOptions{Quota: to.Ptr(int32(10)), Metadata: map[string]*string{"env": to.Ptr("dev")}}))
	_, err := sh.Create(ctx, nil)
	wantCode(t, err, fileerror.ShareAlreadyExists)
	if p := must(sh.GetProperties(ctx, nil)); *p.Quota != 10 || metaOf(p.Metadata, "env") != "dev" {
		t.Fatalf("share props: quota %v, meta %v", *p.Quota, p.Metadata)
	}
	must(sh.SetProperties(ctx, &share.SetPropertiesOptions{Quota: to.Ptr(int32(20))}))
	if p := must(sh.GetProperties(ctx, nil)); *p.Quota != 20 {
		t.Fatalf("quota = %d", *p.Quota)
	}
	n := 0
	for pg := svc.NewListSharesPager(nil); pg.More(); {
		n += len(must(pg.NextPage(ctx)).Shares)
	}
	if n != 1 {
		t.Fatalf("shares = %d", n)
	}

	// directories and files
	dir := sh.NewDirectoryClient("reports")
	must(dir.Create(ctx, nil))
	sub := dir.NewSubdirectoryClient("2026")
	must(sub.Create(ctx, nil))
	f := sub.NewFileClient("q1.txt")
	must(f.Create(ctx, 10, &file.CreateOptions{HTTPHeaders: &file.HTTPHeaders{ContentType: to.Ptr("text/plain")}, Metadata: map[string]*string{"owner": to.Ptr("me")}}))
	must(f.UploadRange(ctx, 2, body("hello"), nil))
	must(f.UploadRange(ctx, 8, body("ab"), nil))

	ranges := must(f.GetRangeList(ctx, nil)).Ranges
	if len(ranges) != 2 || *ranges[0].Start != 2 || *ranges[0].End != 6 || *ranges[1].Start != 8 || *ranges[1].End != 9 {
		t.Fatalf("ranges = %v", ranges)
	}
	must(f.ClearRange(ctx, file.HTTPRange{Offset: 4, Count: 2}, nil))
	if r := must(f.GetRangeList(ctx, nil)).Ranges; len(r) != 3 {
		t.Fatalf("ranges after clear = %d", len(r))
	}
	props := must(f.GetProperties(ctx, nil))
	if *props.ContentLength != 10 || *props.ContentType != "text/plain" || metaOf(props.Metadata, "owner") != "me" {
		t.Fatalf("file props = %+v", props)
	}
	res := must(f.DownloadStream(ctx, &file.DownloadStreamOptions{Range: file.HTTPRange{Offset: 2, Count: 2}}))
	if b, _ := io.ReadAll(res.Body); string(b) != "he" {
		t.Fatalf("range download = %q", b)
	}
	res = must(f.DownloadStream(ctx, nil))
	if b, _ := io.ReadAll(res.Body); string(b) != "\x00\x00he\x00\x00o\x00ab" {
		t.Fatalf("download = %q", b)
	}

	// resize keeps the data it keeps
	must(f.Resize(ctx, 4, nil))
	if p := must(f.GetProperties(ctx, nil)); *p.ContentLength != 4 {
		t.Fatalf("resized length = %d", *p.ContentLength)
	}

	// listing
	must(dir.NewFileClient("a.txt").Create(ctx, 1, nil))
	var names []string
	for pg := dir.NewListFilesAndDirectoriesPager(nil); pg.More(); {
		r := must(pg.NextPage(ctx))
		for _, d := range r.Segment.Directories {
			names = append(names, "d:"+*d.Name)
		}
		for _, x := range r.Segment.Files {
			names = append(names, "f:"+*x.Name)
		}
	}
	if len(names) != 2 || names[0] != "d:2026" || names[1] != "f:a.txt" {
		t.Fatalf("listing = %v", names)
	}

	// deletes
	_, err = dir.Delete(ctx, nil)
	wantCode(t, err, fileerror.DirectoryNotEmpty)
	must(f.Delete(ctx, nil))
	must(sub.Delete(ctx, nil))
	must(sh.Delete(ctx, nil))
}

func TestFailures(t *testing.T) {
	_, url := core.ServePort(t, func(h *core.Hub) core.Service { return files.New(h) })
	conn := "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=" + core.AzuriteKey + ";FileEndpoint=" + url + "/devstoreaccount1;"
	svc := must(service.NewClientFromConnectionString(conn, nil))

	// a share that is not there
	ghost := svc.NewShareClient("ghost")
	_, err := ghost.GetProperties(ctx, nil)
	wantCode(t, err, fileerror.ShareNotFound)
	_, err = ghost.NewRootDirectoryClient().NewFileClient("x").Create(ctx, 1, nil)
	wantCode(t, err, fileerror.ShareNotFound)
	_, err = ghost.Delete(ctx, nil)
	wantCode(t, err, fileerror.ShareNotFound)

	// bad names and quotas
	_, err = svc.NewShareClient("Bad_Name").Create(ctx, nil)
	wantCode(t, err, fileerror.InvalidResourceName)
	_, err = svc.NewShareClient("a--b").Create(ctx, nil)
	wantCode(t, err, fileerror.InvalidResourceName)
	_, err = svc.NewShareClient("zeroq").Create(ctx, &share.CreateOptions{Quota: to.Ptr(int32(0))})
	if err == nil {
		t.Fatal("quota 0 was accepted")
	}

	sh := svc.NewShareClient("docs")
	must(sh.Create(ctx, nil))
	root := sh.NewRootDirectoryClient()

	// a missing directory and a missing parent
	_, err = sh.NewDirectoryClient("nope").GetProperties(ctx, nil)
	wantCode(t, err, fileerror.ResourceNotFound)
	_, err = sh.NewDirectoryClient("nope/inner").Create(ctx, nil)
	wantCode(t, err, fileerror.ParentNotFound)
	_, err = sh.NewDirectoryClient("nope").NewFileClient("f").Create(ctx, 1, nil)
	wantCode(t, err, fileerror.ParentNotFound)
	must(sh.NewDirectoryClient("d").Create(ctx, nil))
	_, err = sh.NewDirectoryClient("d").Create(ctx, nil)
	wantCode(t, err, fileerror.ResourceAlreadyExists)

	// a missing file
	f := root.NewFileClient("f.bin")
	_, err = f.GetProperties(ctx, nil)
	wantCode(t, err, fileerror.ResourceNotFound)
	_, err = f.Delete(ctx, nil)
	wantCode(t, err, fileerror.ResourceNotFound)

	// ranges outside the file
	must(f.Create(ctx, 4, nil))
	_, err = f.UploadRange(ctx, 2, body("toolong"), nil)
	wantCode(t, err, fileerror.InvalidRange)
	_, err = f.DownloadStream(ctx, &file.DownloadStreamOptions{Range: file.HTTPRange{Offset: 10, Count: 2}})
	wantCode(t, err, fileerror.InvalidRange)
	_, err = f.ClearRange(ctx, file.HTTPRange{Offset: 3, Count: 5}, nil)
	wantCode(t, err, fileerror.InvalidRange)

	// a directory is not a file
	_, err = root.NewFileClient("d").GetProperties(ctx, nil)
	wantCode(t, err, fileerror.ResourceNotFound)
}
