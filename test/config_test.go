package test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nrndev/pogi"
)

func TestConfigFile(t *testing.T) {
	p := filepath.Join(t.TempDir(), "pogi.yaml")
	os.WriteFile(p, []byte(`
server: {port: 4999, region: eu-west-1}
services: {enable: [all], disable: [cosmos, gcp]}
storage: {mode: wal, dir: ~/pogi-data}
chaos:
  - {name: flaky, service: s3, op: HeadBucket, status: 503}
  - {name: flaky, service: sqs, latency: 20ms, rate: 0.5}
`), 0o644)
	cfg, err := pogi.LoadConfig(p)
	if err != nil {
		t.Fatal(err)
	}
	home, _ := os.UserHomeDir()
	if cfg.Port != 4999 || cfg.Region != "eu-west-1" || cfg.Storage != "wal" || cfg.DataDir != filepath.Join(home, "pogi-data") || cfg.File == nil {
		t.Fatalf("%+v", cfg)
	}
	on := map[string]bool{}
	for _, s := range pogi.Services(cfg) {
		on[s.Name] = s.Enabled
	}
	if !on["s3"] || !on["azblob"] || on["cosmos"] || on["pubsub"] {
		t.Fatalf("enabled: %v", on)
	}

	cfg.DataDir = "" // a temp dir
	srv := pogi.Start(t, cfg)
	rules := srv.Hub.Chaos.List()
	if len(rules) != 2 || rules[0].ID != "flaky" || rules[1].ID == "flaky" || rules[1].LatencyMs != 20 || rules[1].Probability != 0.5 {
		t.Fatalf("chaos rules: %+v", rules)
	}
	if code, _ := call(t, "HEAD", srv.URL+"/some-bucket"); code != 503 {
		t.Fatalf("the file's chaos rule should fail HeadBucket, got %d", code)
	}
	if _, b := call(t, "GET", srv.URL+"/_pogi/api/info"); !strings.Contains(b, `"ready":true`) {
		t.Fatalf("info: %s", b)
	}
}

func TestServicesList(t *testing.T) {
	for _, c := range []struct {
		list    []string
		on, off string
	}{
		{nil, "s3 gcs cosmos", ""},
		{[]string{"all", "-cosmos"}, "s3 gcs", "cosmos"},
		{[]string{"-azure"}, "s3 gcs", "azblob cosmos"},
		{[]string{"aws", "-lambda"}, "s3 sqs", "lambda gcs"},
		{[]string{"s3", " SQS "}, "s3 sqs", "sns gcs"},
	} {
		on := map[string]bool{}
		for _, s := range pogi.Services(pogi.Config{Services: c.list}) {
			on[s.Name] = s.Enabled
		}
		for _, n := range strings.Fields(c.on) {
			if !on[n] {
				t.Errorf("%q: %s should be on", c.list, n)
			}
		}
		for _, n := range strings.Fields(c.off) {
			if on[n] {
				t.Errorf("%q: %s should be off", c.list, n)
			}
		}
	}
}
