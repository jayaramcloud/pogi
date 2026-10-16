package lambda

import (
	"encoding/base64"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/nrndev/pogi/internal/core"
)

// Function code requests get room for a 50 MB zip; every other request keeps the default limit.
func TestBodyLimit(t *testing.T) {
	hub := core.NewHub(core.Config{DataDir: t.TempDir()}, nil)
	s := New(hub)
	for _, c := range []struct {
		method, path string
		code         bool
	}{
		{"POST", "/2015-03-31/functions", true},
		{"POST", "/2015-03-31/functions/", true},
		{"PUT", "/2015-03-31/functions/f/code", true},
		{"POST", "/2018-10-31/layers/l/versions", true},
		{"POST", "/2015-03-31/functions/f/invocations", false},
		{"PUT", "/2015-03-31/functions/f/configuration", false},
	} {
		r, _ := http.NewRequest(c.method, "http://localhost"+c.path, nil)
		if got := s.BodyLimit(r); (got == hub.CodeLimit(true)) != c.code || (got == 0) == c.code {
			t.Errorf("%s %s: limit %d, want code limit %v", c.method, c.path, got, c.code)
		}
	}
}

// A Lambda request over the code limit is refused with a message about the zip, not the base64 body.
func TestCodeLimitMessage(t *testing.T) {
	_, url := core.ServeTest(t, func(h *core.Hub) core.Service { return New(h) })
	zip := base64.StdEncoding.EncodeToString(make([]byte, 60<<20))
	body := `{"FunctionName":"f","Runtime":"python3.12","Handler":"a.b","Role":"arn:aws:iam::000000000000:role/x","Code":{"ZipFile":"` + zip + `"}}`
	req, _ := http.NewRequest("POST", url+"/2015-03-31/functions", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20260101/us-east-1/lambda/aws4_request, SignedHeaders=host, Signature=x")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode != 413 || !strings.Contains(string(b), "Lambda code is limited to 50 MB zipped (this one is 6") {
		t.Fatalf("60 MB zip = %d %s", res.StatusCode, b)
	}
}
