package main

import (
	"flag"
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/config"
	"go.yaml.in/yaml/v3"
)

func TestConfigInit(t *testing.T) {
	t.Chdir(t.TempDir())
	if err := configCmd([]string{"init"}); err != nil {
		t.Fatal(err)
	}
	if err := configCmd([]string{"init"}); err == nil {
		t.Fatal("init overwrote pogi.yaml without --force")
	}
	if _, err := config.Load("pogi.yaml"); err != nil {
		t.Fatal(err)
	}
	// the commented-out examples are valid too
	examples := regexp.MustCompile(`(?m)^([ \t]*)# ( *(?:[\w.]+:(?: |$)|- ))`).ReplaceAllString(starter, "$1$2")
	os.WriteFile("all.yaml", []byte(examples), 0o644)
	os.MkdirAll("infra", 0o755)
	os.WriteFile("infra/local.yaml", []byte("Resources: {}\n"), 0o644)
	f, err := config.Load("all.yaml")
	if err != nil {
		t.Fatalf("%v\n%s", err, examples)
	}
	if len(f.Chaos) != 2 || len(f.Stacks) != 1 || len(f.Resources.Typed) != 1 || f.Identities.AWS.Users[0].SecretKey != "devsecret" {
		t.Fatalf("examples were not all uncommented:\n%s", examples)
	}

	// pogi config print masks secrets and prints a file that loads back
	b, _ := yaml.Marshal(asFile(pogi.ConfigFromFile(f)))
	if strings.Contains(string(b), "devsecret") {
		t.Fatalf("secret printed:\n%s", b)
	}
	os.WriteFile("printed.yaml", b, 0o644)
	if _, err := config.Load("printed.yaml"); err != nil {
		t.Fatalf("%v\n%s", err, b)
	}
}

func TestPrecedence(t *testing.T) {
	t.Chdir(t.TempDir())
	for _, k := range []string{"POGI_CONFIG", "POGI_PORT", "POGI_HOST", "POGI_SERVICES", "SERVICES", "POGI_PERSIST", "PERSISTENCE", "GATEWAY_LISTEN", "POGI_IAM", "POGI_LAMBDA", "POGI_HOSTNAME"} {
		t.Setenv(k, "")
	}
	t.Setenv("POGI_REGION", "ap-south-1")
	t.Setenv("POGI_STORAGE", "wal")
	t.Setenv("POGI_VALIDATE_SIGNATURES", "1")
	os.WriteFile("pogi.yaml", []byte(`
server: {port: 5000, region: eu-west-1, hostname: aws.localhost}
services: {enable: [aws], disable: [lambda]}
iam: {mode: soft, validateSignatures: false}
`), 0o644)
	start := func(args ...string) pogi.Config {
		fs := flag.NewFlagSet("start", flag.ContinueOnError)
		apply := serverFlags(fs)
		applyEnvDefaults(fs)
		if err := fs.Parse(args); err != nil {
			t.Fatal(err)
		}
		cfg, err := loadConfig("", apply)
		if err != nil {
			t.Fatal(err)
		}
		return cfg
	}
	cfg := start("--port", "6000")
	if cfg.Port != 6000 || cfg.Region != "ap-south-1" || cfg.Hostname != "aws.localhost" || cfg.IAM != "soft" || cfg.Lambda != "auto" ||
		cfg.Storage != "wal" || !cfg.ValidateSigs || !slices.Equal(cfg.Services, []string{"aws", "-lambda"}) || cfg.File == nil {
		t.Fatalf("flags > env > file > defaults: %+v", cfg)
	}
	if cfg = start("--persist", "--services", "s3,sqs"); !cfg.Persist || cfg.Storage != "wal" || len(cfg.Services) != 2 {
		t.Fatalf("--persist keeps a persistent mode: %+v", cfg)
	}
	t.Setenv("POGI_STORAGE", "")
	t.Setenv("GATEWAY_LISTEN", "0.0.0.0:4600")
	if cfg = start("--persist"); cfg.Storage != "hybrid" || cfg.Host != "0.0.0.0" || cfg.Port != 4600 {
		t.Fatalf("--persist means hybrid; LocalStack env still works: %+v", cfg)
	}
	os.WriteFile("pogi.yaml", []byte("server: {port: 99999}"), 0o644)
	if _, err := loadConfig("", func(*pogi.Config) {}); err == nil || !strings.HasPrefix(err.Error(), "pogi.yaml:1: ") {
		t.Fatalf("bad file: %v", err)
	}
}

func TestSMTPSettings(t *testing.T) {
	t.Chdir(t.TempDir())
	t.Setenv("POGI_CONFIG", "")
	t.Setenv("POGI_SMTP_RELAY", "")
	t.Setenv("POGI_SMTP_PORT", "2600")
	os.WriteFile("pogi.yaml", []byte(`email: {smtpPort: 2700, relay: "smtp://u:secret@mail:1025"}`), 0o644)
	cfg, err := envConfig("")
	if err != nil || cfg.SMTPPort != 2600 || cfg.SMTPRelay != "smtp://u:secret@mail:1025" {
		t.Fatalf("env > file: %+v %v", cfg, err)
	}
	if b, _ := yaml.Marshal(asFile(cfg)); strings.Contains(string(b), "secret") {
		t.Fatalf("relay password printed:\n%s", b)
	}
	t.Setenv("POGI_SMTP_PORT", "")
	os.Remove("pogi.yaml")
	if cfg, _ := envConfig(""); cfg.SMTPPort != 2525 || cfg.SMTPRelay != "" {
		t.Fatalf("defaults: %+v", cfg)
	}
}

func TestWait(t *testing.T) {
	srv := pogi.Start(t)
	if err := waitCmd([]string{"--endpoint", srv.URL, "--timeout", "5s"}); err != nil {
		t.Fatal(err)
	}
	srv.Hub.Ready.Store(false) // still starting
	if err := waitCmd([]string{"--endpoint", srv.URL, "--timeout", "300ms"}); err == nil || !strings.Contains(err.Error(), "not ready") {
		t.Fatalf("want a timeout, got %v", err)
	}
}

// Flags and environment variables get the checks pogi.yaml gets: a bad value fails and says which.
func TestSettingsChecked(t *testing.T) {
	t.Chdir(t.TempDir())
	for _, k := range []string{"POGI_CONFIG", "POGI_PORT", "POGI_IAM", "POGI_SERVICES", "SERVICES", "POGI_STORAGE", "POGI_LAMBDA", "GATEWAY_LISTEN"} {
		t.Setenv(k, "")
	}
	start := func(args ...string) error {
		fs := flag.NewFlagSet("start", flag.ContinueOnError)
		apply := serverFlags(fs)
		if err := applyEnvDefaults(fs); err != nil {
			return err
		}
		if err := fs.Parse(args); err != nil {
			t.Fatal(err)
		}
		_, err := loadConfig("", apply)
		return err
	}
	for _, tc := range []struct {
		args      []string
		env, want string
	}{
		{[]string{"--iam", "stict"}, "", `iam.mode: "stict" is not one of off, soft, strict`},
		{[]string{"--storage", "bogus"}, "", `storage.mode: "bogus" is not one of`},
		{[]string{"--lambda", "bogus"}, "", `lambda.executor: "bogus" is not one of`},
		{[]string{"--services", "s3,dynamodbb"}, "", `no service or cloud "dynamodbb" (did you mean dynamodb?)`},
		{nil, "POGI_PORT=abc", `POGI_PORT="abc" is not a valid --port`},
		{nil, "POGI_IAM=bogus", `iam.mode: "bogus" is not one of`},
		{nil, "SERVICES=s3,nosuch", `no service or cloud "nosuch"`},
		{[]string{"--services", "aws,-lambda", "--iam", "strict", "--storage", "wal"}, "", ""},
	} {
		k, v, _ := strings.Cut(tc.env, "=")
		if k != "" {
			t.Setenv(k, v)
		}
		err := start(tc.args...)
		if k != "" {
			t.Setenv(k, "")
		}
		if tc.want == "" && err != nil || tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)) {
			t.Errorf("%v %s: got %v, want %q", tc.args, tc.env, err, tc.want)
		}
	}
}
