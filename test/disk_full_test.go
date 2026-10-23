package test

// Failure-mode audit D3: the disk fills while pogi saves state. The disk is a small disk image
// made and mounted here (hdiutil, no sudo) and detached at the end; the real disk is never filled.

import (
	"context"
	"crypto/rand"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/awstest"
)

// smallDisk mounts an 8 MB disk image and returns its mount point.
func smallDisk(t *testing.T) string {
	t.Helper()
	if runtime.GOOS != "darwin" {
		t.Skip("uses hdiutil")
	}
	name := "pogiaudit" + strings.ToLower(rand.Text()[:6])
	dir := t.TempDir()
	out, err := exec.Command("hdiutil", "create", "-size", "8m", "-fs", "HFS+", "-volname", name, "-attach", filepath.Join(dir, "d.dmg")).CombinedOutput()
	if err != nil {
		t.Skipf("hdiutil: %v %s", err, out)
	}
	mp := "/Volumes/" + name
	t.Cleanup(func() {
		if out, err := exec.Command("hdiutil", "detach", "-force", mp).CombinedOutput(); err != nil {
			t.Errorf("detach %s: %v %s", mp, err, out)
		}
	})
	return mp
}

func sqsCreate(t *testing.T, base, name string) int {
	t.Helper()
	form := url.Values{"Action": {"CreateQueue"}, "QueueName": {name}, "Version": {"2012-11-05"}}
	req, _ := http.NewRequest("POST", base, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	awstest.Sign(t, req, "sqs")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, res.Body)
	res.Body.Close()
	return res.StatusCode
}

// TestFullDiskIsShownAndLoggedOnce: with --persist the disk fills. The API call that changed state still
// succeeds; the only sign is an error line in the log, every second. The file already on disk stays
// whole (the write is to a .tmp file, renamed), and once space is freed the state is saved again.
func TestFullDiskIsShownAndLoggedOnce(t *testing.T) {
	mp := smallDisk(t)
	data := filepath.Join(mp, "data")
	logs := &syncBuf{}
	log := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelInfo}))
	srv := pogi.New(pogi.Config{Services: []string{"sqs"}, DataDir: data, Persist: true, Storage: "hybrid", FlushInterval: 200 * time.Millisecond, NoTLS: true, NoPorts: true}, log)
	if err := srv.Hub.OpenStorage(); err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithCancel(context.Background())
	flushed := make(chan struct{})
	go func() { defer close(flushed); srv.Hub.Flush(ctx) }()
	ts := httptest.NewServer(srv)
	defer ts.Close()
	defer func() { stop(); <-flushed; srv.Hub.Close() }()

	file := filepath.Join(data, "state", "sqs.json")
	if c := sqsCreate(t, ts.URL, "before"); c != 200 {
		t.Fatalf("create: %d", c)
	}
	waitFor(t, "first save", func() bool { b, _ := os.ReadFile(file); return strings.Contains(string(b), "before") })

	// fill the image
	fill, _ := os.Create(filepath.Join(mp, "fill"))
	chunk := make([]byte, 1<<16)
	for {
		if _, err := fill.Write(chunk); err != nil {
			break
		}
	}
	for small := make([]byte, 512); ; { // then the last blocks
		if _, err := fill.Write(small); err != nil {
			break
		}
	}
	fill.Close()

	for i := 0; i < 30; i++ {
		sqsCreate(t, ts.URL, "pad"+strings.Repeat("x", 60)+strconv.Itoa(i))
	}
	if c := sqsCreate(t, ts.URL, "after"); c != 200 {
		t.Errorf("create on a full disk: HTTP %d (the call succeeds although the state cannot be saved)", c)
	}
	time.Sleep(2 * time.Second)
	b, _ := os.ReadFile(file)
	if !strings.Contains(string(b), "before") {
		t.Errorf("the saved state was damaged by the failed save: %q", b)
	}
	t.Logf("file has the new queue while the disk is full: %v; disk free: %s", strings.Contains(string(b), "after"), freeOf(mp))
	t.Logf("full log:\n%s", logs.String())
	errs := strings.Count(logs.String(), "save state")
	t.Logf("log lines 'save state' after 2s on a full disk: %d; first: %s", errs, firstLine(logs.String(), "save state"))
	if errs != 1 {
		t.Errorf("a failed save must be logged once, got %d lines", errs)
	}
	if _, err := os.Stat(file + ".tmp"); err == nil {
		t.Log("a partial .tmp file is left on the full disk")
	}
	// is the failure visible anywhere a user looks? the status of the admin API
	res, _ := http.Get(ts.URL + "/_pogi/api/info")
	info, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if !strings.Contains(strings.ToLower(string(info)), "disk") && !strings.Contains(strings.ToLower(string(info)), "save") {
		t.Errorf("the admin API status says nothing about the failed saves: %.200s", info)
	}

	os.Remove(filepath.Join(mp, "fill")) // free space
	waitFor(t, "state saved again after space is freed", func() bool { b, _ := os.ReadFile(file); return strings.Contains(string(b), "after") })
}

func firstLine(s, sub string) string {
	for _, l := range strings.Split(s, "\n") {
		if strings.Contains(l, sub) {
			return l
		}
	}
	return ""
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for end := time.Now().Add(15 * time.Second); !cond(); time.Sleep(100 * time.Millisecond) {
		if time.Now().After(end) {
			t.Fatalf("timed out waiting for %s", what)
		}
	}
}

func freeOf(mp string) string {
	out, _ := exec.Command("df", "-k", mp).Output()
	return strings.ReplaceAll(string(out), "\n", " | ")
}
