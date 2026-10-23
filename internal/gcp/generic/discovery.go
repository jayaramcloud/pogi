package generic

import (
	"compress/gzip"
	"embed"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
)

//go:generate go run gen.go

//go:embed docs/*.json.gz
var docFS embed.FS

//go:embed apis.txt
var apisTxt string

// The trimmed discovery document gen.go writes.
type doc struct {
	Name      string
	Version   string
	RootURL   string
	Schemas   map[string]*schema
	Resources map[string]*resource
}

type resource struct {
	Methods   map[string]*dmethod
	Resources map[string]*resource
}

type dmethod struct {
	ID, Path, FlatPath, HTTPMethod string
	Parameters                     map[string]struct{ Location string }
	Request, Response              *struct {
		Ref string `json:"$ref"`
	}
}

type schema struct {
	Type, Format string
	Ref          string `json:"$ref"`
	Enum         []string
	Default      any
	ReadOnly     bool
	Properties   map[string]*schema
	Items        *schema
}

// entry is one line of apis.txt. Its document is decoded on first use: decoding all 80-odd at start
// took most of pogi's start-up time.
type entry struct {
	name, version, tf, title, label string
	svc                             string // the registry name: the API's name, unless another cloud's service has it (config is AWS Config)
	doc                             func() *doc
}

// entries are the listed APIs; a bad list or document is a bug in the build, so it panics.
var entries = sync.OnceValue(func() []*entry {
	var out []*entry
	for _, line := range strings.Split(apisTxt, "\n") {
		f := strings.Fields(line)
		if len(f) == 0 || strings.HasPrefix(f[0], "#") {
			continue
		}
		if len(f) < 4 {
			panic(fmt.Sprintf("gcp/generic: apis.txt: %q wants: api version terraform-argument title", line))
		}
		e := &entry{name: f[0], version: f[1], tf: f[2], title: strings.Join(f[3:], " ")}
		e.svc = e.name
		if f[0] == "config" {
			e.svc = "gcpconfig"
		}
		e.label = e.name // every listed API is at https://<name>.googleapis.com/; TestDocsDecode checks it
		e.doc = sync.OnceValue(func() *doc { return loadDoc(e.name, e.version) })
		out = append(out, e)
	}
	return out
})

// loadDoc decodes an API's embedded discovery document.
func loadDoc(name, version string) *doc {
	f, err := docFS.Open("docs/" + name + "-" + version + ".json.gz")
	if err != nil {
		panic(fmt.Sprintf("gcp/generic: %v (run go generate ./internal/gcp/generic)", err))
	}
	defer f.Close()
	var d *doc
	zr, err := gzip.NewReader(f)
	if err == nil {
		err = json.NewDecoder(zr).Decode(&d)
	}
	if err != nil {
		panic(fmt.Sprintf("gcp/generic: %s %s: %v", name, version, err))
	}
	return d
}

// walk calls f for every method of the document.
func (d *doc) walk(f func(*dmethod)) {
	var rec func(map[string]*resource)
	rec = func(rs map[string]*resource) {
		for _, n := range slices.Sorted(maps.Keys(rs)) {
			for _, m := range sortedMethods(rs[n].Methods) {
				f(m)
			}
			rec(rs[n].Resources)
		}
	}
	rec(d.Resources)
}

func sortedMethods(m map[string]*dmethod) (out []*dmethod) {
	for _, k := range slices.Sorted(maps.Keys(m)) {
		out = append(out, m[k])
	}
	return out
}
