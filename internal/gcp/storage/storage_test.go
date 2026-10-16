package storage_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	gcs "cloud.google.com/go/storage"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/gcp/storage"
	"google.golang.org/api/iterator"
)

var ctx = context.Background()

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func write(t *testing.T, o *gcs.ObjectHandle, data []byte, chunk int, set func(w *gcs.Writer)) error {
	t.Helper()
	w := o.NewWriter(ctx)
	w.ChunkSize = chunk
	if set != nil {
		set(w)
	}
	if _, err := w.Write(data); err != nil {
		return err
	}
	return w.Close()
}

func read(t *testing.T, o *gcs.ObjectHandle) string {
	t.Helper()
	r := must(o.NewReader(ctx))
	defer r.Close()
	return string(must(io.ReadAll(r)))
}

func TestOfficialClient(t *testing.T) {
	_, url := core.ServeTest(t, func(h *core.Hub) core.Service { return storage.New(h) })
	t.Setenv("STORAGE_EMULATOR_HOST", url)
	client := must(gcs.NewClient(ctx))
	defer client.Close()

	bkt := client.Bucket("photos")
	if err := bkt.Create(ctx, "my-project", &gcs.BucketAttrs{VersioningEnabled: true, Labels: map[string]string{"env": "dev"}}); err != nil {
		t.Fatal(err)
	}
	if attrs := must(bkt.Attrs(ctx)); !attrs.VersioningEnabled || attrs.Labels["env"] != "dev" {
		t.Fatalf("bucket attrs = %+v", attrs)
	}
	// gcloud storage cp reads the storage layout before each upload.
	if res := must(http.Get(url + "/storage/v1/b/photos/storageLayout")); res.StatusCode != 200 {
		t.Fatalf("storageLayout = %d", res.StatusCode)
	} else if b, _ := io.ReadAll(res.Body); !strings.Contains(string(b), `"hierarchicalNamespace":{"enabled":false}`) {
		t.Fatalf("storageLayout = %s", b)
	}
	// gcloud quotes the multipart boundary with single quotes.
	body := "--=b=\r\nContent-Type: application/json\r\n\r\n{\"name\":\"gcloud.txt\"}\r\n--=b=\r\nContent-Type: text/plain\r\n\r\nhi\r\n--=b=--\r\n"
	if res := must(http.Post(url+"/upload/storage/v1/b/photos/o?uploadType=multipart", "multipart/related; boundary='=b='", strings.NewReader(body))); res.StatusCode != 200 {
		b, _ := io.ReadAll(res.Body)
		t.Fatalf("gcloud-style upload = %d %s", res.StatusCode, b)
	}
	if err := bkt.Object("gcloud.txt").Delete(ctx); err != nil {
		t.Fatal(err)
	}

	obj := bkt.Object("albums/2026/cat.txt")
	if err := write(t, obj, []byte("meow"), 0, func(w *gcs.Writer) { w.ContentType = "text/plain"; w.Metadata = map[string]string{"k": "v"} }); err != nil {
		t.Fatal(err)
	}
	if got := read(t, obj); got != "meow" {
		t.Fatalf("read = %q", got)
	}
	attrs := must(obj.Attrs(ctx))
	if attrs.Size != 4 || attrs.ContentType != "text/plain" || attrs.Metadata["k"] != "v" || attrs.CRC32C == 0 {
		t.Fatalf("attrs = %+v", attrs)
	}
	rr := must(obj.NewRangeReader(ctx, 1, 2))
	if b, _ := io.ReadAll(rr); string(b) != "eo" {
		t.Fatalf("range = %q", b)
	}

	// Resumable upload in 256 KiB chunks.
	big := bytes.Repeat([]byte("0123456789abcdef"), 40000)
	if err := write(t, bkt.Object("big.bin"), big, 256*1024, nil); err != nil {
		t.Fatal(err)
	}
	if got := read(t, bkt.Object("big.bin")); got != string(big) {
		t.Fatalf("big object round trip: %d bytes", len(got))
	}

	// Preconditions: DoesNotExist fails once the object exists.
	err := write(t, obj.If(gcs.Conditions{DoesNotExist: true}), []byte("again"), 0, nil)
	if err == nil || !strings.Contains(err.Error(), "412") {
		t.Fatalf("precondition should fail with 412, got %v", err)
	}

	// Copy, compose, update and list.
	must(bkt.Object("copy.txt").CopierFrom(obj).Run(ctx))
	must(bkt.Object("both.txt").ComposerFrom(obj, bkt.Object("copy.txt")).Run(ctx))
	if got := read(t, bkt.Object("both.txt")); got != "meowmeow" {
		t.Fatalf("compose = %q", got)
	}
	must(obj.Update(ctx, gcs.ObjectAttrsToUpdate{Metadata: map[string]string{"k": "v2"}}))
	var names, prefixes []string
	it := bkt.Objects(ctx, &gcs.Query{Delimiter: "/"})
	for {
		a, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if a.Prefix != "" {
			prefixes = append(prefixes, a.Prefix)
		} else {
			names = append(names, a.Name)
		}
	}
	if strings.Join(prefixes, ",") != "albums/" || strings.Join(names, ",") != "big.bin,both.txt,copy.txt" {
		t.Fatalf("list = %v %v", prefixes, names)
	}

	// Versioning keeps the old generation after a delete.
	if err := obj.Delete(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := obj.NewReader(ctx); !errors.Is(err, gcs.ErrObjectNotExist) {
		t.Fatalf("after delete: %v", err)
	}
	versions := 0
	vit := bkt.Objects(ctx, &gcs.Query{Prefix: "albums/", Versions: true})
	for {
		if _, err := vit.Next(); err != nil {
			break
		}
		versions++
	}
	if versions != 1 {
		t.Fatalf("noncurrent versions = %d", versions)
	}
	if err := bkt.Delete(ctx); err == nil {
		t.Fatal("deleting a non-empty bucket must fail")
	}
}
