package email_test

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/azure/email"
)

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

type env struct {
	srv *pogi.Running
	svc *email.Service
}

func setup(t *testing.T, cfg ...pogi.Config) *env {
	t.Helper()
	srv := pogi.Start(t, cfg...)
	return &env{srv, srv.Hub.Service("azemail").(*email.Service)}
}

const message = `{
  "senderAddress": "donotreply@pogi.localhost",
  "recipients": {
    "to":  [{"address": "ada@example.com", "displayName": "Ada"}, {"address": "grace@example.com"}],
    "cc":  [{"address": "cc@example.com"}],
    "bcc": [{"address": "bcc@example.com"}]
  },
  "replyTo": [{"address": "support@example.com"}],
  "content": {"subject": "Welcome", "plainText": "Hello Ada", "html": "<h1>Hello Ada</h1>"},
  "headers": {"X-Campaign": "launch"},
  "attachments": [{"name": "hello.txt", "contentType": "text/plain", "contentInBase64": "aGVsbG8gd29ybGQ="}]
}`

func (e *env) do(t *testing.T, method, path, body, host string, header http.Header) (*http.Response, string) {
	t.Helper()
	req, _ := http.NewRequest(method, e.srv.URL+path, strings.NewReader(body))
	if host != "" {
		req.Host = host
	}
	for k, v := range header {
		req.Header[k] = v
	}
	res := must(http.DefaultClient.Do(req))
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	return res, string(b)
}

// sendAndWait posts a message and polls its operation to Succeeded, as the SDKs' pollers do.
func (e *env) sendAndWait(t *testing.T, path, host, body string, signer func(method, uri, body string) http.Header) string {
	t.Helper()
	hdr := func(method, uri, body string) http.Header {
		if signer == nil {
			return nil
		}
		return signer(method, uri, body)
	}
	res, out := e.do(t, "POST", path, body, host, hdr("POST", path, body))
	var op struct{ ID, Status string }
	json.Unmarshal([]byte(out), &op)
	loc := res.Header.Get("operation-location")
	if res.StatusCode != 202 || op.ID == "" || op.Status != "NotStarted" || !strings.Contains(loc, "/emails/operations/"+op.ID+"?api-version=") {
		t.Fatalf("send = %d %s, operation-location %q", res.StatusCode, out, loc)
	}
	// the location is an absolute URL on the host the client used; poll it on the test server
	opPath := loc[strings.Index(loc, "/emails/operations/"):]
	if i := strings.Index(loc, "/_azure/"); i >= 0 {
		opPath = loc[i:]
	}
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		_, got := e.do(t, "GET", opPath, "", host, hdr("GET", opPath, ""))
		var st struct{ ID, Status string }
		json.Unmarshal([]byte(got), &st)
		if st.Status == "Succeeded" && st.ID == op.ID {
			return op.ID
		}
		if time.Now().After(deadline) {
			t.Fatalf("operation = %s", got)
		}
	}
}

func TestSendAndCapture(t *testing.T) {
	e := setup(t)
	id := e.sendAndWait(t, "/emails:send?api-version=2023-03-31", "", message, nil)

	out := e.svc.Outbox()
	if len(out) != 1 {
		t.Fatalf("outbox = %+v", out)
	}
	m := out[0]
	if m.ID != id || m.From != "donotreply@pogi.localhost" || m.Subject != "Welcome" || m.Text != "Hello Ada" || m.HTML != "<h1>Hello Ada</h1>" ||
		len(m.To) != 2 || m.To[0].DisplayName != "Ada" || m.Cc[0].Address != "cc@example.com" || m.Bcc[0].Address != "bcc@example.com" ||
		m.ReplyTo[0].Address != "support@example.com" || m.Headers["X-Campaign"] != "launch" ||
		len(m.Attachments) != 1 || m.Attachments[0].Name != "hello.txt" || m.Attachments[0].Content != "aGVsbG8gd29ybGQ=" {
		t.Fatalf("message = %+v", m)
	}

	// Newer api-versions, the host form and the path form all work, and land in the same outbox.
	e.sendAndWait(t, "/emails:send?api-version=2025-09-01", "", message, nil)
	e.sendAndWait(t, "/emails:send?api-version=2023-03-31", "mail.communication.localhost:7644", message, nil)
	e.sendAndWait(t, "/_azure/communication/mail/emails:send?api-version=2023-03-31", "", message, nil)
	out = e.svc.Outbox()
	if len(out) != 4 || out[2].Resource != "mail" || out[3].Resource != "mail" || out[0].Resource != "default" {
		t.Fatalf("resources = %v", out)
	}

	// The operation of an unknown message.
	if res, _ := e.do(t, "GET", "/emails/operations/nope?api-version=2023-03-31", "", "", nil); res.StatusCode != 404 {
		t.Fatalf("unknown operation = %d", res.StatusCode)
	}

	// The console: the inbox page, the generic table and the inspector.
	_, page := e.do(t, "GET", "/_pogi/inbox", "", "", nil)
	if !strings.Contains(page, "Welcome") || !strings.Contains(page, "ada@example.com") || !strings.Contains(page, "bcc@example.com") || !strings.Contains(page, "azemail") {
		t.Fatalf("inbox page lacks the message:\n%s", page)
	}
	if rows := e.svc.Resources()[0].Rows; len(rows) != 4 || rows[3][0] != id {
		t.Fatalf("Outbox table = %v", rows)
	}
	got, _ := e.svc.Inspect("Outbox", id).(map[string]any)
	files, _ := got["attachments"].([]map[string]any)
	if len(files) != 1 || files[0]["size"] != 11 {
		t.Fatalf("Inspect = %v", got)
	}

	// Save, reset and load.
	saved := must(e.svc.Save())
	e.srv.Reset("azemail")
	if len(e.svc.Outbox()) != 0 {
		t.Fatal("Reset kept the outbox")
	}
	if err := e.svc.Load(saved); err != nil || len(e.svc.Outbox()) != 4 {
		t.Fatalf("Load = %v, outbox %d", err, len(e.svc.Outbox()))
	}
}

func TestValidation(t *testing.T) {
	e := setup(t)
	for name, body := range map[string]string{
		"not JSON":      `nope`,
		"no sender":     `{"recipients":{"to":[{"address":"a@b.c"}]},"content":{"subject":"s","plainText":"t"}}`,
		"no recipients": `{"senderAddress":"a@b.c","recipients":{},"content":{"subject":"s","plainText":"t"}}`,
		"bad recipient": `{"senderAddress":"a@b.c","recipients":{"to":[{"address":"nobody"}]},"content":{"subject":"s","plainText":"t"}}`,
		"no subject":    `{"senderAddress":"a@b.c","recipients":{"to":[{"address":"a@b.c"}]},"content":{"plainText":"t"}}`,
		"no body":       `{"senderAddress":"a@b.c","recipients":{"to":[{"address":"a@b.c"}]},"content":{"subject":"s"}}`,
		"bad attachment": `{"senderAddress":"a@b.c","recipients":{"to":[{"address":"a@b.c"}]},"content":{"subject":"s","plainText":"t"},
			"attachments":[{"name":"x","contentType":"text/plain","contentInBase64":"!!"}]}`,
	} {
		res, out := e.do(t, "POST", "/emails:send?api-version=2023-03-31", body, "", nil)
		var er struct {
			Error struct{ Code, Message string }
		}
		if json.Unmarshal([]byte(out), &er); res.StatusCode != 400 || er.Error.Code != "InvalidRequestBody" || er.Error.Message == "" {
			t.Errorf("%s: %d %s", name, res.StatusCode, out)
		}
	}
	if n := len(e.svc.Outbox()); n != 0 {
		t.Fatalf("rejected messages were captured: %d", n)
	}
	// Plain paths without an api-version are not Email's.
	if res, _ := e.do(t, "POST", "/emails:send", message, "", nil); res.StatusCode == 202 {
		t.Fatal("a request without an api-version was taken")
	}
}

// sign makes the Authorization headers the Communication Services SDKs send.
func sign(method, uri, host, body, key string) http.Header {
	date := time.Now().UTC().Format(http.TimeFormat)
	sum := sha256.Sum256([]byte(body))
	hash := base64.StdEncoding.EncodeToString(sum[:])
	m := hmac.New(sha256.New, must(base64.StdEncoding.DecodeString(key)))
	m.Write([]byte(method + "\n" + uri + "\n" + date + ";" + host + ";" + hash))
	return http.Header{"x-ms-date": {date}, "x-ms-content-sha256": {hash}, "Content-Type": {"application/json"},
		"Authorization": {"HMAC-SHA256 SignedHeaders=x-ms-date;host;x-ms-content-sha256&Signature=" + base64.StdEncoding.EncodeToString(m.Sum(nil))}}
}

func TestHMAC(t *testing.T) {
	e := setup(t, pogi.Config{ValidateSigs: true})
	key := base64.StdEncoding.EncodeToString([]byte("a-secret-access-key"))
	e.svc.SetKey("secure", key)
	host := "secure.communication.localhost"
	uri := "/emails:send?api-version=2023-03-31"
	hostPort := host + ":7644"

	e.sendAndWait(t, uri, hostPort, message, func(method, uri, body string) http.Header { return sign(method, uri, hostPort, body, key) })
	if res, _ := e.do(t, "POST", uri, message, hostPort, nil); res.StatusCode != 401 {
		t.Fatalf("an unsigned request = %d", res.StatusCode)
	}
	bad := sign("POST", uri, hostPort, message, base64.StdEncoding.EncodeToString([]byte("another key")))
	if res, _ := e.do(t, "POST", uri, message, hostPort, bad); res.StatusCode != 401 {
		t.Fatalf("a wrong signature = %d", res.StatusCode)
	}
	tampered := sign("POST", uri, hostPort, message, key)
	if res, _ := e.do(t, "POST", uri, strings.Replace(message, "Welcome", "Tampered", 1), hostPort, tampered); res.StatusCode != 401 {
		t.Fatalf("a body that does not match its hash = %d", res.StatusCode)
	}
	// A resource without a key is not checked.
	e.sendAndWait(t, uri, "open.communication.localhost:7644", message, nil)
	if n := len(e.svc.Outbox()); n != 2 {
		t.Fatalf("outbox = %d, want the 2 accepted messages", n)
	}
}
