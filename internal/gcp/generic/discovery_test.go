package generic

import (
	"net/url"
	"strings"
	"testing"
)

// pogi registers each API at start by its name, without decoding its document (that took most of the
// start-up time). That is only right while every listed API lives at https://<name>.googleapis.com/.
// This decodes every document and checks it, so a new API with another host fails here, not in routing.
func TestDocsDecode(t *testing.T) {
	es := entries()
	if len(es) == 0 {
		t.Fatal("apis.txt lists no API")
	}
	for _, e := range es {
		d := e.doc()
		u, err := url.Parse(d.RootURL)
		if err != nil {
			t.Errorf("%s: rootUrl %q: %v", e.name, d.RootURL, err)
			continue
		}
		if host, _, _ := strings.Cut(u.Host, "."); host != e.label {
			t.Errorf("%s: document host %q, but pogi registers it as %q: add a label to apis.txt", e.name, host, e.label)
		}
	}
}
