package test

// Repro tests for the v0.2.0 failure audit, area I (bad input, limits and hangs). Each TestFailMode<ID> runs a
// real pogi and logs what it did. A test that fails shows an open mode. Nothing here fixes anything.

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	neturl "net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/awstest"
)

// TestFailModeChild is the pogi process the I tests start (so they can read its memory). Library mode with no
// extra ports; the upload limit is the default unless POGI_FAIL_MAXMB is set.
func TestFailModeChild(t *testing.T) {
	if os.Getenv("POGI_FAIL_CHILD") != "1" {
		t.Skip("started by the TestFailMode tests")
	}
	mb, _ := strconv.Atoi(os.Getenv("POGI_FAIL_MAXMB"))
	srv := pogi.New(pogi.Config{Port: freePort(t), DataDir: t.TempDir(), NoTLS: true, NoPorts: true, MaxUploadMB: mb}, nil)
	t.Fatal(srv.Run(context.Background(), func(u string) { fmt.Println("pogi-url", u) }))
}

// failPogi starts a pogi child process and returns its URL and pid. tmp is its TMPDIR.
func failPogi(t *testing.T, tmp string, env ...string) (string, int) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestFailModeChild$")
	cmd.Env = append(os.Environ(), append([]string{"POGI_FAIL_CHILD=1", "TMPDIR=" + tmp}, env...)...)
	cmd.Stderr = io.Discard
	out, _ := cmd.StdoutPipe()
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cmd.Process.Kill(); cmd.Wait() })
	sc := bufio.NewScanner(out)
	for sc.Scan() {
		if u, ok := strings.CutPrefix(sc.Text(), "pogi-url "); ok {
			go io.Copy(io.Discard, out)
			return u, cmd.Process.Pid
		}
	}
	t.Fatalf("pogi child exited before it was ready: %v", cmd.Wait())
	return "", 0
}

func rssMB(pid int) int {
	out, _ := exec.Command("ps", "-o", "rss=", "-p", strconv.Itoa(pid)).Output()
	kb, _ := strconv.Atoi(strings.TrimSpace(string(out)))
	return kb / 1024
}

// peakRSS samples the child's memory until stop is called and returns the peak in MB.
func peakRSS(pid int) (stop func() int) {
	var peak atomic.Int64
	done := make(chan struct{})
	fin := make(chan struct{})
	go func() {
		defer close(fin)
		for {
			if m := int64(rssMB(pid)); m > peak.Load() {
				peak.Store(m)
			}
			select {
			case <-done:
				return
			case <-time.After(50 * time.Millisecond):
			}
		}
	}()
	return func() int { close(done); <-fin; return int(peak.Load()) }
}

// stream is n bytes of fill, made on the fly: the test never holds or writes the body.
type stream struct {
	n    int64
	fill byte
}

func (s *stream) Read(p []byte) (int, error) {
	if s.n <= 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > s.n {
		p = p[:s.n]
	}
	for i := range p {
		p[i] = s.fill
	}
	s.n -= int64(len(p))
	return len(p), nil
}

type bigResult struct {
	status  int
	body    string
	err     error
	elapsed time.Duration
	peak    int
}

func (r bigResult) String() string {
	b := r.body
	if len(b) > 200 {
		b = b[:200]
	}
	return fmt.Sprintf("status=%d err=%v after=%s peakRSS=%dMB body=%q", r.status, r.err, r.elapsed.Round(time.Millisecond), r.peak, b)
}

// sendBig sends the request with a body from body, with the declared length (-1: chunked), and watches the
// child's memory. sign is an AWS service name to sign for ("" for none).
func sendBig(t *testing.T, pid int, method, url, ctype, sign string, length int64, body io.Reader) bigResult {
	t.Helper()
	req, _ := http.NewRequest(method, url, body)
	req.ContentLength = length
	if ctype != "" {
		req.Header.Set("Content-Type", ctype)
	}
	if sign != "" {
		req.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
		awstest.Sign(t, req, sign)
	}
	stop := peakRSS(pid)
	start := time.Now()
	c := &http.Client{Timeout: 3 * time.Minute}
	res, err := c.Do(req)
	r := bigResult{err: err}
	if err == nil {
		b, _ := io.ReadAll(io.LimitReader(res.Body, 512<<10))
		res.Body.Close()
		r.status, r.body = res.StatusCode, string(b)
	}
	r.elapsed, r.peak = time.Since(start), stop()
	return r
}

const gig = 1 << 30

// TestHugeBodyToRPCServicesIs413: a too-large body to SQS (query and JSON) or DynamoDB gets 413 in the
// cloud's error shape, not "does not implement", and pogi's memory stays small.
func TestHugeBodyToRPCServicesIs413(t *testing.T) {
	url, pid := failPogi(t, t.TempDir())
	base := rssMB(pid)
	do := func(name, ctype, sign string, length int64, body io.Reader, wantBody string) {
		r := sendBig(t, pid, "POST", url+"/", ctype, sign, length, body)
		t.Logf("%s: %s", name, r)
		if r.status != 413 || !strings.Contains(r.body, wantBody) {
			t.Errorf("%s: status %d body %.150q, want 413 with %q", name, r.status, r.body, wantBody)
		}
		if r.peak-base > 300 {
			t.Errorf("%s: memory rose %dMB", name, r.peak-base)
		}
	}
	pre := "Action=SendMessage&Version=2012-11-05&QueueUrl=x&MessageBody="
	do("SQS query 11MB", "application/x-www-form-urlencoded", "sqs", int64(len(pre))+11<<20,
		io.MultiReader(strings.NewReader(pre), &stream{n: 11 << 20, fill: 'a'}), "EntityTooLarge")
	do("SQS query 1GB", "application/x-www-form-urlencoded", "sqs", int64(len(pre))+gig,
		io.MultiReader(strings.NewReader(pre), &stream{n: gig, fill: 'a'}), "EntityTooLarge")
	jpre := `{"QueueUrl":"x","MessageBody":"`
	do("SQS JSON 1GB", "application/x-amz-json-1.0", "sqs", int64(len(jpre))+gig,
		io.MultiReader(strings.NewReader(jpre), &stream{n: gig, fill: 'a'}), "EntityTooLarge")
	do("DynamoDB JSON 1GB", "application/x-amz-json-1.0", "dynamodb", gig, &stream{n: gig, fill: ' '}, "EntityTooLarge")
}

// dirBytes is the size of the files under dir.
func dirBytes(dir string) (n int64) {
	es, _ := os.ReadDir(dir)
	for _, e := range es {
		if fi, err := e.Info(); err == nil {
			n += fi.Size()
		}
	}
	return n
}

// TestConsoleFormBodyIsLimited: a 120 MB multipart post to a plain console form is refused at the upload
// limit, with the real size in the message, and is not spilled to disk.
func TestConsoleFormBodyIsLimited(t *testing.T) {
	tmp := t.TempDir()
	url, pid := failPogi(t, tmp)
	var peakDisk atomic.Int64
	done := make(chan struct{})
	go func() {
		for {
			if n := dirBytes(tmp); n > peakDisk.Load() {
				peakDisk.Store(n)
			}
			select {
			case <-done:
				return
			case <-time.After(20 * time.Millisecond):
			}
		}
	}()
	const size = 120 << 20
	pr, pw := io.Pipe()
	mw := multipart.NewWriter(pw)
	go func() {
		mw.WriteField("name", "qq")
		fw, _ := mw.CreateFormFile("junk", "junk.bin")
		io.Copy(fw, &stream{n: size, fill: 'z'})
		mw.Close()
		pw.Close()
	}()
	r := sendBig(t, pid, "POST", url+"/_pogi/sqs", mw.FormDataContentType(), "", -1, pr)
	pr.CloseWithError(io.ErrClosedPipe)
	close(done)
	t.Logf("%s; peak TMPDIR use %dMB", r, peakDisk.Load()>>20)
	if peakDisk.Load() > 20<<20 {
		t.Errorf("the form took %dMB of disk before refusing", peakDisk.Load()>>20)
	}
	if r.err != nil {
		t.Errorf("the post failed: %v", r.err)
	}

	// An upload form says why, with the real size, though the body is chunked and has no declared length.
	// new variables: the first writer goroutine may still be closing its own pipe and writer
	upr, upw := io.Pipe()
	umw := multipart.NewWriter(upw)
	go func() {
		umw.WriteField("action", "upload")
		fw, _ := umw.CreateFormFile("file", "big.bin")
		io.Copy(fw, &stream{n: size, fill: 'z'})
		umw.Close()
		upw.Close()
	}()
	req, _ := http.NewRequest("POST", url+"/_pogi/objects/s3/big", upr)
	req.Header.Set("Content-Type", umw.FormDataContentType())
	res, err := (&http.Client{Timeout: time.Minute, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}).Do(req)
	upr.CloseWithError(io.ErrClosedPipe)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	loc, _ := neturl.QueryUnescape(res.Header.Get("Location"))
	if !strings.Contains(loc, "limited to 10 MB") || !strings.Contains(loc, "this one is 12") {
		t.Errorf("upload refusal %d %q: want the 10 MB limit and the real size (about 120 MB)", res.StatusCode, loc)
	}
}

func alive(url string) bool {
	res, err := (&http.Client{Timeout: 3 * time.Second}).Get(url + "/_pogi/")
	if err != nil {
		return false
	}
	res.Body.Close()
	return true
}

// TestDeepTerraformNestingIsRefused: a main.tf with 100,000 nested parentheses gets a plain 400 and pogi stays up.
func TestDeepTerraformNestingIsRefused(t *testing.T) {
	url, _ := failPogi(t, t.TempDir())
	var b bytes.Buffer
	mw := multipart.NewWriter(&b)
	fw, _ := mw.CreateFormFile("main.tf", "main.tf")
	io.WriteString(fw, "locals {\n a = "+strings.Repeat("(", 100000)+"1"+strings.Repeat(")", 100000)+"\n}\n")
	mw.Close()
	res, err := (&http.Client{Timeout: 30 * time.Second}).Post(url+"/_pogi/cost/terraform", mw.FormDataContentType(), &b)
	if err != nil {
		t.Fatal(err)
	}
	msg, _ := io.ReadAll(io.LimitReader(res.Body, 1000))
	res.Body.Close()
	if !alive(url) {
		t.Fatal("pogi died")
	}
	if res.StatusCode >= 500 || !strings.Contains(string(msg), "nested too deep") {
		t.Errorf("status %d body %.200q, want a plain 4xx that says nested too deep", res.StatusCode, msg)
	}
}
