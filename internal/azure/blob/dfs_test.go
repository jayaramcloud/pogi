package blob_test

import (
	"context"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/datalakeerror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/file"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/filesystem"
	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/azure/armres"
	"github.com/nrndev/pogi/internal/azure/armtest"
	"github.com/nrndev/pogi/internal/core"
)

// dfsEnv is a pogi with Blob and Data Lake and two accounts: "hnsacct" with a hierarchical namespace, "flatacct" without.
type dfsEnv struct {
	t    *testing.T
	addr string // where pogi listens
	tr   *http.Transport
}

func newDFS(t *testing.T) *dfsEnv {
	env := armtest.Start(t, pogi.Config{}, "azblob", "azdatalake")
	for name, hns := range map[string]bool{"hnsacct": true, "flatacct": false} {
		path := "/subscriptions/" + core.AzureSubscription + "/resourceGroups/" + core.AzureResourceGroup + "/providers/Microsoft.Storage/storageAccounts/" + name + "?api-version=2023-05-01"
		body := map[string]any{"location": "eastus", "kind": "StorageV2", "sku": map[string]any{"name": "Standard_LRS"}, "properties": map[string]any{"isHnsEnabled": hns}}
		if status, resp := armres.Send(env.Srv.Hub, "arm", http.MethodPut, "", path, body); status >= 300 {
			t.Fatalf("make account %s: %d %s", name, status, resp)
		}
	}
	addr := env.Srv.URL[strings.Index(env.Srv.URL, "//")+2:]
	// *.localhost names all reach pogi; the host name is what routes the request to a service
	tr := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	}}
	return &dfsEnv{t: t, addr: addr, tr: tr}
}

func (e *dfsEnv) opts() azcore.ClientOptions {
	return azcore.ClientOptions{Transport: &http.Client{Transport: e.tr}}
}

func (e *dfsEnv) fs(account, name string) *filesystem.Client {
	cred := must(azdatalake.NewSharedKeyCredential(account, key))
	return must(filesystem.NewClientWithSharedKeyCredential("http://"+account+".dfs.localhost:"+e.port()+"/"+name, cred, &filesystem.ClientOptions{ClientOptions: e.opts()}))
}

func (e *dfsEnv) port() string { return e.addr[strings.LastIndexByte(e.addr, ':')+1:] }

// raw sends a request to a data-plane host and returns the status and error code.
func (e *dfsEnv) raw(method, host, path string, hdr ...string) (int, string) {
	req, _ := http.NewRequest(method, "http://"+host+".localhost:"+e.port()+path, nil)
	req.Header.Set("x-ms-version", "2025-01-05")
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	resp, err := (&http.Client{Transport: e.tr}).Do(req)
	if err != nil {
		e.t.Fatal(err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	return resp.StatusCode, resp.Header.Get("x-ms-error-code")
}

func wantDFS(t *testing.T, err error, code datalakeerror.StorageErrorCode) {
	t.Helper()
	if !datalakeerror.HasCode(err, code) {
		t.Fatalf("want %s, got %v", code, err)
	}
}

func readFile(t *testing.T, f *file.Client) string {
	t.Helper()
	res := must(f.DownloadStream(ctx, nil))
	defer res.Body.Close()
	return string(must(io.ReadAll(res.Body)))
}

func listPaths(t *testing.T, fs *filesystem.Client, recursive bool) []string {
	t.Helper()
	var out []string
	for p := fs.NewListPathsPager(recursive, nil); p.More(); {
		for _, x := range must(p.NextPage(ctx)).Paths {
			n := *x.Name
			if x.IsDirectory != nil && *x.IsDirectory {
				n += "/"
			}
			out = append(out, n)
		}
	}
	return out
}

func TestDataLake(t *testing.T) {
	e := newDFS(t)
	fs := e.fs("hnsacct", "lake")
	must(fs.Create(ctx, nil))

	// a directory, a file with appended data, flushed
	must(fs.NewDirectoryClient("raw/2026").Create(ctx, nil)) // makes raw/ as well
	f := fs.NewFileClient("raw/2026/a.txt")
	must(f.Create(ctx, nil))
	must(f.AppendData(ctx, 0, streaming.NopCloser(strings.NewReader("hello")), nil))
	must(f.AppendData(ctx, 5, streaming.NopCloser(strings.NewReader(" world")), nil))
	must(f.FlushData(ctx, 11, nil))
	if got := readFile(t, f); got != "hello world" {
		t.Fatalf("read = %q", got)
	}
	if p := must(f.GetProperties(ctx, nil)); *p.ContentLength != 11 {
		t.Fatalf("length = %d", *p.ContentLength)
	}

	// UploadBuffer is create + appends + flush
	big := strings.Repeat("pogi-", 1<<18) // 1.25 MiB
	g := fs.NewFileClient("raw/big.bin")
	must(g.Create(ctx, nil))
	if err := g.UploadBuffer(ctx, []byte(big), &file.UploadBufferOptions{ChunkSize: 256 << 10, Concurrency: 2}); err != nil {
		t.Fatal(err)
	}
	if got := readFile(t, g); got != big {
		t.Fatalf("big read: %d bytes", len(got))
	}

	// listing
	want := "raw/ raw/2026/ raw/2026/a.txt raw/big.bin"
	if got := strings.Join(listPaths(t, fs, true), " "); got != want {
		t.Fatalf("recursive = %s", got)
	}

	// Blob sees the same files
	bc := must(azblob.NewClientWithSharedKeyCredential("http://hnsacct.blob.localhost:"+e.port()+"/", mustKey("hnsacct"), &azblob.ClientOptions{ClientOptions: e.opts()}))
	if got := download(t, bc, "lake", "raw/2026/a.txt"); got != "hello world" {
		t.Fatalf("blob read = %q", got)
	}

	// rename a directory with files below it
	must(fs.NewDirectoryClient("raw").Rename(ctx, "archive", nil))
	if got := strings.Join(listPaths(t, fs, true), " "); got != "archive/ archive/2026/ archive/2026/a.txt archive/big.bin" {
		t.Fatalf("after rename = %s", got)
	}
	if got := readFile(t, fs.NewFileClient("archive/2026/a.txt")); got != "hello world" {
		t.Fatalf("renamed read = %q", got)
	}
	_, err := fs.NewFileClient("raw/2026/a.txt").GetProperties(ctx, nil)
	if err == nil {
		t.Fatal("the old path is still there")
	}

	// rename onto an existing path: fine by default, refused with If-None-Match: *
	must(fs.NewFileClient("x.txt").Create(ctx, nil))
	must(fs.NewFileClient("y.txt").Create(ctx, nil))
	_, err = fs.NewFileClient("x.txt").Rename(ctx, "y.txt", &file.RenameOptions{AccessConditions: &file.AccessConditions{ModifiedAccessConditions: &file.ModifiedAccessConditions{IfNoneMatch: to.Ptr(azcore.ETagAny)}}})
	wantDFS(t, err, datalakeerror.PathAlreadyExists)
	must(fs.NewFileClient("x.txt").Rename(ctx, "y.txt", nil))

	// a missing source
	_, err = fs.NewFileClient("nothing.txt").Rename(ctx, "z.txt", nil)
	wantDFS(t, err, datalakeerror.SourcePathNotFound)

	// delete: a directory that is not empty needs recursive=true
	if st, code := e.raw("DELETE", "hnsacct.dfs", "/lake/archive"); st != 409 || code != "DirectoryNotEmpty" {
		t.Fatalf("delete non-empty: %d %s", st, code)
	}
	if st, _ := e.raw("DELETE", "hnsacct.dfs", "/lake/archive?recursive=true"); st != 200 {
		t.Fatalf("recursive delete: %d", st)
	}
	if got := strings.Join(listPaths(t, fs, true), " "); got != "y.txt" {
		t.Fatalf("after delete = %s", got)
	}
	must(fs.NewFileClient("y.txt").Delete(ctx, nil))
	_, err = fs.NewFileClient("y.txt").Delete(ctx, nil)
	wantDFS(t, err, datalakeerror.PathNotFound)

	// file systems come and go through the dfs endpoint too
	if st, _ := e.raw("PUT", "hnsacct.dfs", "/second?resource=filesystem"); st != 201 {
		t.Fatalf("dfs create filesystem: %d", st)
	}
	if st, code := e.raw("PUT", "hnsacct.dfs", "/second?resource=filesystem"); st != 409 || code != "FilesystemAlreadyExists" {
		t.Fatalf("dfs create twice: %d %s", st, code)
	}
	must(e.fs("hnsacct", "second").Delete(ctx, nil))
}

func TestDataLakeFailures(t *testing.T) {
	e := newDFS(t)
	fs := e.fs("hnsacct", "lake")
	must(fs.Create(ctx, nil))

	// a file system that is not there
	ghost := e.fs("hnsacct", "ghost")
	_, err := ghost.NewFileClient("a").Create(ctx, nil)
	wantDFS(t, err, "FilesystemNotFound")
	if p := ghost.NewListPathsPager(true, nil); p.More() {
		_, err = p.NextPage(ctx)
		wantDFS(t, err, "FilesystemNotFound")
	}

	// a flush at the wrong position, and one over a gap (appends come in any order, a flush needs them contiguous)
	f := fs.NewFileClient("f.txt")
	must(f.Create(ctx, nil))
	must(f.AppendData(ctx, 0, streaming.NopCloser(strings.NewReader("abc")), nil))
	_, err = f.FlushData(ctx, 99, nil)
	wantDFS(t, err, datalakeerror.InvalidFlushPosition)
	must(f.AppendData(ctx, 7, streaming.NopCloser(strings.NewReader("hij")), nil))
	_, err = f.FlushData(ctx, 10, nil)
	wantDFS(t, err, datalakeerror.InvalidFlushPosition)
	must(f.AppendData(ctx, 3, streaming.NopCloser(strings.NewReader("defg")), nil))
	must(f.FlushData(ctx, 10, nil))
	if got := readFile(t, f); got != "abcdefghij" {
		t.Fatalf("read = %q", got)
	}
	wantDFS(t, second(f.AppendData(ctx, 4, streaming.NopCloser(strings.NewReader("x")), nil)), datalakeerror.InvalidFlushPosition) // inside what is flushed

	// an append to a directory, an append to nothing
	must(fs.NewDirectoryClient("d").Create(ctx, nil))
	wantDFS(t, second(fs.NewFileClient("d").AppendData(ctx, 0, streaming.NopCloser(strings.NewReader("x")), nil)), datalakeerror.PathConflict)
	wantDFS(t, second(fs.NewFileClient("none").AppendData(ctx, 0, streaming.NopCloser(strings.NewReader("x")), nil)), datalakeerror.PathNotFound)

	// a file below a file; a file where a directory is
	_, err = fs.NewFileClient("f.txt/inner").Create(ctx, nil)
	wantDFS(t, err, datalakeerror.PathConflict)
	_, err = fs.NewFileClient("d").Create(ctx, nil)
	wantDFS(t, err, datalakeerror.PathConflict)

	// a create that must not overwrite
	_, err = f.Create(ctx, &file.CreateOptions{AccessConditions: &file.AccessConditions{ModifiedAccessConditions: &file.ModifiedAccessConditions{IfNoneMatch: to.Ptr(azcore.ETagAny)}}})
	wantDFS(t, err, datalakeerror.PathAlreadyExists)

	// a bad path, a bad file system name
	if st, code := e.raw("PUT", "hnsacct.dfs", "/lake/a/../b?resource=file"); st != 400 || code != "InvalidResourceName" {
		t.Fatalf("bad path: %d %s", st, code)
	}
	if st, code := e.raw("PUT", "hnsacct.dfs", "/X?resource=filesystem"); st != 400 || code != "InvalidResourceName" {
		t.Fatalf("bad name: %d %s", st, code)
	}

	// an account without a hierarchical namespace
	flat := e.fs("flatacct", "lake")
	if st, code := e.raw("PUT", "flatacct.dfs", "/lake?resource=filesystem"); st != 400 || code != "HierarchicalNamespaceNotEnabled" {
		t.Fatalf("flat create filesystem: %d %s", st, code)
	}
	_, err = flat.NewFileClient("a").Create(ctx, nil)
	if !strings.Contains(err.Error(), "HierarchicalNamespaceNotEnabled") {
		t.Fatalf("flat create file: %v", err)
	}
}

func mustKey(account string) *azblob.SharedKeyCredential {
	return must(azblob.NewSharedKeyCredential(account, key))
}

func second[T any](_ T, err error) error { return err }
