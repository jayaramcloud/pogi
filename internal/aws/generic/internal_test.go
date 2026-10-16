package generic

import (
	"net/http"
	"net/url"
	"regexp"
	"testing"
)

func TestMatchRoutes(t *testing.T) {
	m := &model{meta: &meta{Protocol: "rest-json"}, routes: map[string][]*route{}, ops: map[string]*operation{}}
	for _, o := range []struct{ name, method, uri string }{
		{"GetObject", "GET", "/{Bucket}/{Key+}"},
		{"GetBucket", "GET", "/{Bucket}"},
		{"GetPolicy", "GET", "/{Bucket}?policy"},
		{"Tail", "GET", "/files/{Path+}/meta"},
		{"List", "GET", "/files"},
	} {
		op := &operation{Name: o.name}
		op.HTTP.Method, op.HTTP.RequestURI = o.method, o.uri
		m.ops[o.name] = op
	}
	m.buildRoutes()
	for _, c := range []struct {
		url, op string
		labels  map[string]string
	}{
		{"/b/a/b/c.txt", "GetObject", map[string]string{"Bucket": "b", "Key": "a/b/c.txt"}},
		{"/b", "GetBucket", map[string]string{"Bucket": "b"}},
		{"/b?policy", "GetPolicy", map[string]string{"Bucket": "b"}},
		{"/files/x/y/meta", "Tail", map[string]string{"Path": "x/y"}},
		{"/files", "List", map[string]string{}},
		{"/b/with%20space", "GetObject", map[string]string{"Bucket": "b", "Key": "with space"}},
	} {
		r, _ := http.NewRequest("GET", "http://x"+c.url, nil)
		op, labels := m.match(r)
		if op == nil || op.Name != c.op {
			t.Errorf("%s: matched %v, want %s", c.url, op, c.op)
			continue
		}
		for k, v := range c.labels {
			if labels[k] != v {
				t.Errorf("%s: label %s = %q, want %q", c.url, k, labels[k], v)
			}
		}
	}
	if op, _ := m.match(&http.Request{Method: "POST", URL: &url.URL{Path: "/b"}}); op != nil {
		t.Errorf("POST matched %v", op)
	}
}

func TestFromPattern(t *testing.T) {
	for _, p := range []string{`^(arn:aws[-a-z]*:elasticfilesystem:[0-9a-z-:]+:file-system/fs-[0-9a-f]{8,40}|fs-[0-9a-f]{8,40})$`, `^[a-zA-Z0-9_\-]{1,100}$`, `^[0-9a-f]{32}$`, `[\w-]+`} {
		s, ok := fromPattern(p, 1, 64)
		if !ok || !regexp.MustCompile(p).MatchString(s) {
			t.Errorf("%s: %q %v", p, s, ok)
		}
	}
	if s := mintID(nil, "FileSystem"); len(s) < 5 || s[:3] != "fs-" {
		t.Errorf("minted %q", s)
	}
}

func TestSingular(t *testing.T) {
	for in, want := range map[string]string{"Domains": "Domain", "Repositories": "Repository", "Addresses": "Address", "Status": "Status", "Boxes": "Box", "FileSystems": "FileSystem"} {
		if got := singular(in); got != want {
			t.Errorf("%s: %s, want %s", in, got, want)
		}
	}
}
