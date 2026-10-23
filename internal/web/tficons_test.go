package web

import (
	"io/fs"
	"regexp"
	"strings"
	"testing"
)

func TestTFIconFallbacks(t *testing.T) {
	for _, c := range []struct{ cloud, service, typ, file, kind string }{
		{"aws", "EC2", "aws_instance", "aws/ec2.svg", ""},
		{"aws", "vpc", "aws_subnet", "aws/vpc.svg", ""},                       // by type prefix
		{"azure", "virtual", "azurerm_virtual_network", "azure/vnet.svg", ""}, // too coarse a service word
		{"aws", "glue", "aws_glue_job", "aws/glue.svg", ""},                   // unpriced: the second word
		{"aws", "foo", "aws_foo_queue", "", "messaging"},                      // not in the table: kind from the name
		{"aws", "foo", "aws_foo_bar", "", "other"},                            // no kind at all
		{"", "", "module", "", "other"},
		{"gcp", "Compute Engine", "google_compute_instance", "gcp/compute-engine.svg", ""},
		{"gcp", "x", "google_compute_network", "", "network"}, // not in the table: kind from the name
	} {
		file, kind := tfIconFor(c.cloud, c.service, c.typ)
		if file != c.file || kind != c.kind {
			t.Errorf("%s %q %s: got %q %q, want %q %q", c.cloud, c.service, c.typ, file, kind, c.file, c.kind)
		}
	}
	for _, o := range tfIconTypes {
		if _, ok := svcIcons[o.key]; !ok {
			t.Errorf("type prefix %s names %q, which is not in the table", o.prefix, o.key)
		}
	}
}

func TestTFIconFiles(t *testing.T) {
	used := map[string]bool{}
	for k, f := range svcIcons {
		if !strings.HasPrefix(k, strings.SplitN(f, "/", 2)[0]+"/") {
			t.Errorf("%s uses another cloud's file %s", k, f)
		}
		if _, err := fs.Stat(iconFiles, "icons/"+f); err != nil {
			t.Errorf("%s: %v", k, err)
		}
		used[f] = true
	}
	total := 0
	err := fs.WalkDir(iconFiles, "icons", func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || strings.HasSuffix(p, "NOTICE") {
			return err
		}
		if !used[strings.TrimPrefix(p, "icons/")] {
			t.Errorf("%s is embedded but no table entry uses it", p)
		}
		b, _ := fs.ReadFile(iconFiles, p)
		total += len(b)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if total >= 1<<20 {
		t.Errorf("icons total %d bytes, want under 1 MB", total)
	}
}

func TestTFIconDrawing(t *testing.T) {
	cfg, est := tfNet("aws", [3]string{}, [3]string{"aws_instance", "i", ""}, [3]string{"aws_sqs_queue", "q", ""}, [3]string{"aws_foo_bar", "x", ""})
	est.Rows[0].Service, est.Rows[1].Service = "EC2", "SQS"
	g := buildTFGraph(cfg, est, nil)
	page, exp := g.svg(false), g.export()
	if !strings.Contains(page, `href="/_pogi/icons/aws/ec2.svg"`) || !strings.Contains(page, `<use href="#tfk-other"`) || strings.Contains(page, "data:") {
		t.Error("the page should link the served files and draw the fallback for an unknown type")
	}
	if !strings.Contains(page, `aria-hidden="true"`) || strings.Count(page, `id="tfi-aws-ec2"`) != 1 {
		t.Error("icons are decorative and defined once")
	}
	// the export is one file: only #… and data: links
	for _, m := range regexp.MustCompile(`href="([^"]*)"`).FindAllStringSubmatch(exp, -1) {
		if !strings.HasPrefix(m[1], "#") && !strings.HasPrefix(m[1], "data:") {
			t.Errorf("export links outside itself: %.60s", m[1])
		}
	}
	if !strings.Contains(exp, "data:image/svg+xml;base64,") || strings.Contains(exp, "var(--") {
		t.Error("the export should embed the icons and carry no theme variables")
	}
}

func TestTFIconGoogle(t *testing.T) {
	cfg, est := tfNet("gcp", [3]string{}, [3]string{"google_compute_instance", "i", ""}, [3]string{"google_storage_bucket", "b", ""})
	est.Rows[0].Service, est.Rows[1].Service = "Compute Engine", "Cloud Storage"
	if s := buildTFGraph(cfg, est, nil).svg(false); !strings.Contains(s, `href="/_pogi/icons/gcp/compute-engine.svg"`) || !strings.Contains(s, `href="/_pogi/icons/gcp/cloud-storage.svg"`) {
		t.Error("a Google box should draw Google's icon")
	}
}

// Every registered service resolves to an official icon or to the glyph of its kind, and every
// file the table names is embedded (TestTFIconFiles checks the files).
func TestServiceIconCoversRegistry(t *testing.T) {
	n := map[string]int{}
	for id, c := range coverage().Services {
		file, kind := serviceIcon(c.Cloud, id)
		if file == "" && tfKindGlyphs[kind] == "" {
			t.Errorf("%s/%s: no icon and no fallback glyph (kind %q)", c.Cloud, id, kind)
		}
		if file != "" {
			n[c.Cloud]++
		}
	}
	t.Logf("official icons: %v", n)
}

// Route 53 types share the aws_route prefix with route tables; they must keep their own icon.
func TestTFIconRoute53NotVPC(t *testing.T) {
	if f, _ := tfIconFor("aws", "route53", "aws_route53_zone"); f != "aws/route53.svg" {
		t.Fatalf("aws_route53_zone got %q", f)
	}
	if f, _ := tfIconFor("aws", "route", "aws_route_table"); f != "aws/vpc.svg" {
		t.Fatalf("aws_route_table got %q", f)
	}
}

// The Map's export is one file: its icon links become data URIs.
func TestMapIconLinks(t *testing.T) {
	node := svcIconSVG("aws", "sqs", 0, 0, 18)
	m := iconLinks.FindStringSubmatch(node)
	if m == nil {
		t.Fatalf("no icon link in %s", node)
	}
	if d, ok := iconData(m[1]); !ok || !strings.HasPrefix(d, "data:image/svg+xml;base64,") {
		t.Errorf("iconData(%q) = %.30s, %v", m[1], d, ok)
	}
	if g := svcIconSVG("aws", "account", 0, 0, 18); strings.Contains(g, "/_pogi/icons/") || !strings.Contains(g, "<svg") {
		t.Errorf("a service with no icon should draw its glyph: %s", g)
	}
}
