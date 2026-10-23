package web_test

import (
	"bytes"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"strings"
	"testing"

	"github.com/nrndev/pogi/internal/terraform"
)

// The Terraform tab: its page, its limits, and the console's gates in front of the upload.
func TestTerraformTab(t *testing.T) {
	c := start(t, "s3")
	want(t, c.get("/_pogi/cost"), `href="/_pogi/cost/terraform"`, "Running in pogi")
	want(t, c.get("/_pogi/"), `href="/_pogi/terraform"`) // in the sidebar, under Tools
	// with nothing loaded, the Cost tab points to the Terraform page and has no load controls
	tab := c.get("/_pogi/cost/terraform")
	want(t, tab, "No Terraform loaded yet", `href="/_pogi/terraform"`)
	if strings.Contains(tab, "webkitdirectory") || strings.Contains(tab, "Choose folder") {
		t.Error("the Cost tab has load controls")
	}
	page := c.get("/_pogi/terraform")
	want(t, page, "Drop a Terraform folder or a plan.json here", "Choose folder", "Choose plan.json", "Try a sample", "webkitdirectory",
		`accept=".json,application/json"`, "terraform show -json plan.out", "Your files stay on this machine", `data-max-files="2000"`, "pogi tf diagram")

	var sample map[string]string
	if err := json.Unmarshal([]byte(c.get("/_pogi/terraform/sample")), &sample); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(sample["main.tf"], "aws_lb") || !strings.Contains(sample["modules/net/main.tf"], "aws_nat_gateway") {
		t.Fatalf("sample files: %v", len(sample))
	}

	post := func(site string, parts map[string]string) (*http.Response, string) {
		var body bytes.Buffer
		mw := multipart.NewWriter(&body)
		for n, v := range parts {
			fw, _ := mw.CreateFormFile(n, n)
			fw.Write([]byte(v))
		}
		mw.Close()
		req, _ := http.NewRequest("POST", c.base+"/_pogi/cost/terraform", &body)
		req.Header.Set("Content-Type", mw.FormDataContentType())
		if site != "" {
			req.Header.Set("Sec-Fetch-Site", site)
		}
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		b, _ := io.ReadAll(res.Body)
		return res, string(b)
	}
	// another website cannot post files to the console, and the answers carry the console's headers
	if res, _ := post("cross-site", map[string]string{"main.tf": ""}); res.StatusCode != 403 {
		t.Fatalf("cross-site post = %d, want 403", res.StatusCode)
	}
	res, body := post("same-origin", map[string]string{"x.json": "{}"})
	if res.StatusCode != 422 || !strings.Contains(body, "not a Terraform plan") || !strings.Contains(res.Header.Get("Content-Security-Policy"), "script-src 'self';") {
		t.Fatalf("same-origin post = %d %s", res.StatusCode, body)
	}

	// the size picker is stateless and checks what it is given
	for _, q := range []string{"column=aws/nope&region=us-east-1", "column=&region=", "column=aws/ec2/linux&region=mars-1&attr=x%22y"} {
		r, err := http.Get(c.base + "/_pogi/cost/terraform/sizes?" + q)
		if err != nil {
			t.Fatal(err)
		}
		r.Body.Close()
		if r.StatusCode != 400 {
			t.Errorf("sizes?%s = %d, want 400", q, r.StatusCode)
		}
	}
}

// The sample must parse and price once the parser and the price lists are merged.
func TestTerraformSample(t *testing.T) {
	c := start(t, "s3")
	var sample map[string]string
	if err := json.Unmarshal([]byte(c.get("/_pogi/terraform/sample")), &sample); err != nil {
		t.Fatal(err)
	}
	files := map[string][]byte{}
	for p, s := range sample {
		files[p] = []byte(s)
	}
	cfg, err := terraform.Parse(files, nil)
	if err != nil && strings.Contains(err.Error(), "not built yet") {
		t.Skip("the parser has not landed yet")
	}
	if err != nil {
		t.Fatal(err)
	}
	blocks := map[string]bool{}
	for _, r := range cfg.Resources {
		blocks[r.Block] = true
	}
	for _, b := range []string{"aws_lb.web", "aws_instance.app", "aws_db_instance.db", "aws_s3_bucket.assets", "module.net.aws_nat_gateway.nat", "module.net.aws_vpc.main"} {
		if !blocks[b] {
			t.Errorf("the sample lacks %s", b)
		}
	}
}
