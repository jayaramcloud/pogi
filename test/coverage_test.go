package test

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/nrndev/pogi"
	"go.yaml.in/yaml/v3"
)

var update = flag.Bool("update", false, "rewrite docs/reference/coverage.md, the Services table of README.md and internal/web/coverage.json")

type covRow struct {
	Title    string   `yaml:"title"`
	Cloud    string   `yaml:"cloud"`
	Depth    string   `yaml:"depth"`
	Services []string `yaml:"services"`
	Rest     bool     `yaml:"rest"`
	Docker   bool     `yaml:"docker"`
	Covered  string   `yaml:"covered"`
	NotYet   string   `yaml:"not_yet"`
}

// coverageEntry is one service in internal/web/coverage.json, which the web console embeds.
type coverageEntry struct {
	Title    string `json:"title"`
	Cloud    string `json:"cloud"`
	Category string `json:"category"`
	Depth    string `json:"depth"`
	Docker   bool   `json:"docker"`
	Covered  string `json:"covered"`
	NotYet   string `json:"not_yet"`
}

var clouds = []struct{ key, title, readme string }{
	{"aws", "AWS", "AWS"}, {"gcp", "Google Cloud", "Google Cloud"}, {"azure", "Azure", "Azure"},
}

// categoryNames are the console's service categories, in the order the console shows them.
var categoryNames = []string{
	"Storage", "Databases", "Messaging & events", "Compute & containers", "Networking",
	"Security & identity", "Observability", "Integration & workflow", "Analytics & AI",
	"Developer tools", "Management & governance",
}

// TestCoverageDocs joins docs/reference/coverage.yaml (depth, covered and not-yet text per row, and each
// service's console category) with the registry (every service's Info) and checks that
// docs/reference/coverage.md, the Services table in README.md and internal/web/coverage.json are what they
// generate. `go generate .` rewrites them.
func TestCoverageDocs(t *testing.T) {
	raw, err := os.ReadFile("../docs/reference/coverage.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Intro      string              `yaml:"intro"`
		Categories map[string][]string `yaml:"categories"`
		Rows       []covRow            `yaml:"rows"`
	}
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	// The category names in file order, which a map does not keep.
	var order struct {
		Categories yaml.Node `yaml:"categories"`
	}
	if err := yaml.Unmarshal(raw, &order); err != nil {
		t.Fatal(err)
	}
	var catNames []string
	for i := 0; i+1 < len(order.Categories.Content); i += 2 {
		catNames = append(catNames, order.Categories.Content[i].Value)
	}

	// The registry: name → cloud and title, in routing order.
	cloud := map[string]string{}
	title := map[string]string{}
	var names []string
	for _, svc := range pogi.New(pogi.Config{DataDir: t.TempDir()}, nil).Hub.Services() {
		i := svc.Info()
		cloud[i.Name] = strings.ToLower(i.Cloud)
		title[i.Name] = i.Title
		names = append(names, i.Name)
	}

	named := map[string]bool{}
	rest := -1
	for i, r := range doc.Rows {
		if !slices.Contains([]string{"Deep", "Core", "Thin"}, r.Depth) {
			t.Errorf("%s: depth %q", r.Title, r.Depth)
		}
		if r.Covered == "" {
			t.Errorf("%s: nothing covered", r.Title)
		}
		if r.Rest {
			if rest >= 0 || r.Cloud != "aws" {
				t.Errorf("%s: one aws row can take the rest", r.Title)
			}
			rest = i
		}
		for _, n := range r.Services {
			if c, ok := cloud[n]; !ok {
				t.Errorf("%s: %q is not a registered service", r.Title, n)
			} else if c != r.Cloud {
				t.Errorf("%s: %q is a %s service", r.Title, n, c)
			}
			named[n] = true
		}
	}
	var others []string // what the rest row takes
	for _, n := range names {
		switch {
		case named[n]:
		case cloud[n] == "aws" && rest >= 0:
			others = append(others, n)
		default:
			t.Errorf("service %q is in no row of docs/reference/coverage.yaml", n)
		}
	}

	// Each category is one of categoryNames and names registered services; each service is in one.
	inCat := map[string]string{}
	for _, c := range catNames {
		if !slices.Contains(categoryNames, c) {
			t.Errorf("category %q is not one of the %d console categories", c, len(categoryNames))
		}
		for _, n := range doc.Categories[c] {
			if _, ok := cloud[n]; !ok {
				t.Errorf("category %q: %q is not a registered service", c, n)
			} else if prev, dup := inCat[n]; dup {
				t.Errorf("service %q is in two categories: %q and %q", n, prev, c)
			}
			inCat[n] = c
		}
	}
	for _, n := range names {
		if inCat[n] == "" {
			t.Errorf("service %q is in no category of docs/reference/coverage.yaml", n)
		}
	}

	var md, tbl strings.Builder
	md.WriteString("---\ntitle: Coverage\ndescription: What each emulated service does today, and what it does not yet.\n---\n\n")
	md.WriteString("<!-- Generated from docs/reference/coverage.yaml and the service registry by `go generate .`; edit the YAML. -->\n\n")
	md.WriteString(strings.TrimRight(doc.Intro, "\n") + "\n\n")
	count := map[string]int{}
	for _, n := range names {
		count[cloud[n]]++
	}
	fmt.Fprintf(&md, "pogi registers %d services: %d AWS, %d Google Cloud and %d Azure. Each service name is followed by its registry id, the name `pogi services` prints.\n",
		len(names), count["aws"], count["gcp"], count["azure"])
	tbl.WriteString("| Cloud | Services |\n|---|---|\n")
	for _, c := range clouds {
		fmt.Fprintf(&md, "\n## %s\n\n| Service | Depth | Covered | Not yet |\n|---|---|---|---|\n", c.title)
		var plain, docker []string
		for _, r := range doc.Rows {
			if r.Cloud != c.key {
				continue
			}
			ids := r.Services
			covered := r.Covered
			if r.Rest {
				ids = []string{fmt.Sprintf("%d others", len(others))}
				covered = strings.ReplaceAll(covered, "{n}", fmt.Sprint(len(others)))
			}
			reg := "`" + strings.Join(ids, "`, `") + "`"
			if r.Rest {
				reg = ids[0]
			}
			fmt.Fprintf(&md, "| %s · %s | %s | %s | %s |\n", r.Title, reg, r.Depth, esc(covered), esc(r.NotYet))
			switch {
			case r.Rest:
			case r.Docker:
				docker = append(docker, r.Title)
			default:
				plain = append(plain, r.Title)
			}
		}
		line := strings.Join(plain, ", ")
		if c.key == "aws" {
			line += fmt.Sprintf(", and %d more long-tail services generated from the AWS API models (see [docs/reference/aws-long-tail.md](docs/reference/aws-long-tail.md))", len(others))
		}
		if len(docker) > 0 {
			line += "; with Docker running: " + strings.Join(docker, ", ")
		}
		fmt.Fprintf(&tbl, "| **%s** | %s |\n", c.readme, line)
	}

	check(t, "../docs/reference/coverage.md", func(string) string { return md.String() })
	check(t, "../README.md", func(old string) string {
		const start, end = "<!-- services:start", "<!-- services:end -->"
		a, b := strings.Index(old, start), strings.Index(old, end)
		if a < 0 || b < a {
			t.Fatalf("README.md has no %q … %q block", start, end)
		}
		a += strings.Index(old[a:], "\n") + 1
		return old[:a] + tbl.String() + old[b:]
	})

	// internal/web/coverage.json: the categories in order, and each service's row data.
	entries := map[string]coverageEntry{}
	for _, r := range doc.Rows {
		for _, n := range r.Services {
			if _, ok := entries[n]; !ok {
				entries[n] = coverageEntry{Title: r.Title, Cloud: cloud[n], Category: inCat[n], Depth: r.Depth,
					Docker: r.Docker, Covered: r.Covered, NotYet: r.NotYet}
			}
		}
	}
	if rest >= 0 {
		r := doc.Rows[rest]
		for _, n := range others {
			entries[n] = coverageEntry{Title: title[n], Cloud: cloud[n], Category: inCat[n], Depth: r.Depth,
				Docker: r.Docker, Covered: strings.ReplaceAll(r.Covered, "{n}", fmt.Sprint(len(others))), NotYet: r.NotYet}
		}
	}
	js, err := json.MarshalIndent(struct {
		Categories []string                 `json:"categories"`
		Services   map[string]coverageEntry `json:"services"`
	}{catNames, entries}, "", " ")
	if err != nil {
		t.Fatal(err)
	}
	check(t, "../internal/web/coverage.json", func(string) string { return string(js) + "\n" })
}

func esc(s string) string { return strings.ReplaceAll(s, "|", `\|`) }

// check compares a generated file with the one on disk, or rewrites it with -update.
func check(t *testing.T, path string, gen func(old string) string) {
	t.Helper()
	old, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	want := gen(string(old))
	if want == string(old) {
		return
	}
	if !*update {
		t.Errorf("%s is stale: run `go generate .` in the repo root", path)
		return
	}
	if err := os.WriteFile(path, []byte(want), 0o644); err != nil {
		t.Fatal(err)
	}
}
