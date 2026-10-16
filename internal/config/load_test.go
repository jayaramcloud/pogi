package config

import (
	"os"
	"strings"
	"testing"
	"time"

	"go.yaml.in/yaml/v3"
)

func TestFind(t *testing.T) {
	t.Chdir(t.TempDir())
	t.Setenv("POGI_CONFIG", "")
	if p := Find(""); p != "" {
		t.Fatalf("no file: %q", p)
	}
	os.WriteFile("pogi.yml", nil, 0o644)
	if p := Find(""); p != "pogi.yml" {
		t.Fatalf(".yml: %q", p)
	}
	os.WriteFile("pogi.yaml", nil, 0o644)
	if p := Find(""); p != "pogi.yaml" {
		t.Fatalf(".yaml wins: %q", p)
	}
	t.Setenv("POGI_CONFIG", "env.yaml")
	if p := Find(""); p != "env.yaml" {
		t.Fatalf("env: %q", p)
	}
	if p := Find("flag.yaml"); p != "flag.yaml" {
		t.Fatalf("explicit: %q", p)
	}
}

func TestLoad(t *testing.T) {
	t.Chdir(t.TempDir())
	t.Setenv("POGI_TEST_PORT", "4600")
	os.MkdirAll("conf/infra", 0o755)
	os.WriteFile("conf/infra/local.yaml", []byte("Resources: {}\n"), 0o644)
	os.WriteFile("conf/pogi.yaml", []byte(`
server: {port: ${POGI_TEST_PORT}, region: "${POGI_TEST_REGION:-eu-west-1}"}
ai: {apiKey: "pa$$ $HOME ${POGI_TEST_UNSET}x"}
chaos: [{name: slow, latency: 300ms, rate: 0.5, status: 503}]
stacks: [{name: base, template: infra/local.yaml}]
identities: {aws: {users: [{name: dev, accessKey: AKIADEV0000000000001, secretKey: s}, {name: ci}]}}
resources: {aws: {s3: {buckets: [uploads, {name: logs, versioning: true}]}, secretsmanager: {secrets: {db: hunter2}}}}
`), 0o644)
	f, err := Load("conf/pogi.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if f.Path != "conf/pogi.yaml" || f.Version != 1 || f.Server.Port != 4600 || f.Server.Region != "eu-west-1" ||
		f.AI.APIKey != "pa$$ $HOME x" || f.Chaos[0].Latency != 300*time.Millisecond {
		t.Fatalf("%+v", f)
	}
	if empty, err := os.Create("empty.yaml"); err == nil {
		empty.Close()
	}
	if f, err := Load("empty.yaml"); err != nil || f.Version != 1 {
		t.Fatalf("empty file: %v", err)
	}

	// resources print in a form that loads back
	b, _ := yaml.Marshal(f.Resources)
	var r Resources
	if err := yaml.Unmarshal(b, &r); err != nil {
		t.Fatal(err)
	}
	if bk := r.AWS["s3"]["buckets"]; len(bk) != 2 || bk[0].Name != "uploads" || bk[1].Props["versioning"] != true ||
		r.AWS["secretsmanager"]["secrets"][0].Props["value"] != "hunter2" {
		t.Fatalf("round trip:\n%s", b)
	}
}

func TestLoadErrors(t *testing.T) {
	t.Chdir(t.TempDir())
	os.WriteFile("tpl.yaml", []byte("Resources: {}\n"), 0o644)
	for _, c := range []struct{ src, want string }{
		{"version: 2", "pogi.yaml:1: version: 2 is not supported (use 1)"},
		{"server:\n  port: 70000", "pogi.yaml:2: server.port: 70000 is not a port"},
		{"email: {smtpPort: -2}", "pogi.yaml:1: email.smtpPort: -2 is not a port (-1 turns SMTP off)"},
		{"email:\n  relay: http://mail:1025", `pogi.yaml:2: email.relay: "http://mail:1025" is not smtp://[user:pass@]host[:port]`},
		{"server:\n  prot: 1\n  hots: x", "pogi.yaml:2: field prot not found in type config.Server\npogi.yaml:3: field hots not found in type config.Server"},
		{"server: {port: x}", "pogi.yaml:1: cannot unmarshal !!str `x` into int"},
		{"server: [", "pogi.yaml:1: did not find expected node content"},
		{"storage:\n  mode: disk", `pogi.yaml:2: storage.mode: "disk" is not one of memory, hybrid, persistent, wal`},
		{"storage:\n  services:\n    s3: disk", `pogi.yaml:3: storage.services.s3: "disk" is not one of`},
		{"iam: {mode: on}", `pogi.yaml:1: iam.mode: "on" is not one of off, soft, strict`},
		{"lambda:\n  executor: wasm", `pogi.yaml:2: lambda.executor: "wasm" is not one of auto, native, docker`},
		{"chaos:\n  - {service: s3}\n  - {rate: 1.5}", "pogi.yaml:3: chaos.1.rate: 1.5 is not between 0 and 1"},
		{"chaos:\n  - status: 200", "pogi.yaml:2: chaos.0.status: 200 is not an error status (400-599)"},
		{"chaos:\n  - {probability: 1}", "pogi.yaml:2: field probability not found in type config.Chaos"},
		{"resources:\n  typed:\n    - {name: x}", "pogi.yaml:3: resources.typed.0: missing type"},
		{"resources:\n  typed:\n    - {type: X, propertes: {}}", `pogi.yaml:3: unknown field "propertes"`},
		{"stacks:\n  - template: tpl.yaml", "pogi.yaml:2: stacks.0: missing name"},
		{"stacks:\n  - name: a", "pogi.yaml:2: stacks.0: missing template"},
		{"stacks:\n  - name: a\n    template: nope.yaml", "pogi.yaml:3: stacks.0.template: stat nope.yaml: no such file"},
		{"identities:\n  aws:\n    users:\n      - name: dev\n      - name: dev", `pogi.yaml:5: identities.aws.users.1.name: user "dev" is defined twice`},
		{"identities:\n  aws:\n    users:\n      - {name: dev, accessKey: akia, secretKey: s}", `pogi.yaml:4: identities.aws.users.0.accessKey: "akia" is not 16-128 upper-case letters and digits`},
		{"identities:\n  aws:\n    users:\n      - {name: dev, accessKey: AKIADEV0000000000001}", "pogi.yaml:4: identities.aws.users.0: accessKey needs a secretKey"},
	} {
		os.WriteFile("pogi.yaml", []byte(c.src), 0o644)
		if _, err := Load("pogi.yaml"); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%q:\n got %v\nwant %s", c.src, err, c.want)
		}
	}
}
