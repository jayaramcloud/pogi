package main

import (
	"cmp"
	"errors"
	"flag"
	"fmt"
	"net/url"
	"os"
	"slices"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/config"
	"github.com/nrndev/pogi/internal/core"
	"go.yaml.in/yaml/v3"
)

// defaults are the server settings nothing else sets.
var defaults = pogi.Config{Port: 7644, Host: "127.0.0.1", Region: core.DefaultRegion, Lambda: "auto", IAM: "off", SMTPPort: 2525}

// serverFlags defines pogi start's settings. applyEnvDefaults presets them
// from the environment and the command line overrides that, so apply copies
// exactly those set onto a config: flags > environment > pogi.yaml > defaults.
func serverFlags(fs *flag.FlagSet) (apply func(*pogi.Config)) {
	port := fs.Int("port", defaults.Port, "main port (all clouds, the console and the admin API)")
	host := fs.String("host", defaults.Host, "listen address (0.0.0.0 to accept other machines/containers)")
	services := fs.String("services", "", "comma-separated services or clouds to enable, -name to leave one out (default: all)")
	persist := fs.Bool("persist", false, "load and save state in the data dir (--storage hybrid)")
	storage := fs.String("storage", "", "state storage: memory | hybrid | persistent | wal (default memory)")
	dataDir := fs.String("data-dir", "", "state, snapshots and init hooks (default ~/.pogi)")
	region := fs.String("region", defaults.Region, "default AWS region")
	lambda := fs.String("lambda", defaults.Lambda, "Lambda executor: auto | native | docker")
	iam := fs.String("iam", defaults.IAM, "IAM enforcement: off | soft | strict")
	sigs := fs.Bool("validate-signatures", false, "reject requests whose SigV4 signature is wrong")
	external := fs.Bool("allow-external", false, "allow a --host other than loopback")
	hostname := fs.String("hostname", "", "host name in URLs; aws.localhost enables virtual-host style (default localhost)")
	tlsCert := fs.String("tls-cert", "", "HTTPS certificate file (default: pogi's local CA)")
	tlsKey := fs.String("tls-key", "", "HTTPS key file for --tls-cert")
	maxUpload := fs.Int("max-upload-mb", 0, "upload size limit in MiB (0: 10)")
	smtpPort := fs.Int("smtp-port", defaults.SMTPPort, "SES SMTP port; mail sent to it shows in the console inbox (-1: off)")
	smtpRelay := fs.String("smtp-relay", "", "also deliver all SES mail to this SMTP server, smtp://[user:pass@]host:port")
	mqttPort := fs.Int("mqtt-port", 1883, "IoT MQTT port, plain TCP, opened on first IoT use (-1: WebSocket at /mqtt only)")
	amqpPort := fs.Int("amqp-port", 5672, "AMQP 1.0 port for Service Bus and Event Hubs, plain TCP (-1: off)")
	transferPort := fs.Int("transfer-port", 2222, "Transfer Family SFTP port, opened with the first server (-1: a free port)")
	return func(cfg *pogi.Config) {
		fs.Visit(func(f *flag.Flag) {
			switch f.Name {
			case "port":
				cfg.Port = *port
			case "host":
				cfg.Host = *host
			case "services":
				cfg.Services = strings.FieldsFunc(*services, func(r rune) bool { return r == ',' })
			case "persist": // hybrid, unless a persistent mode is set (--storage comes later: Visit goes in name order)
				if cfg.Persist = *persist; *persist && or(cfg.Storage, "memory") == "memory" {
					cfg.Storage = "hybrid"
				}
			case "storage":
				cfg.Storage = *storage
			case "data-dir":
				cfg.DataDir = *dataDir
			case "region":
				cfg.Region = *region
			case "lambda":
				cfg.Lambda = *lambda
			case "iam":
				cfg.IAM = *iam
			case "validate-signatures":
				cfg.ValidateSigs = *sigs
			case "allow-external":
				cfg.AllowExternal = *external
			case "hostname":
				cfg.Hostname = *hostname
			case "tls-cert":
				cfg.TLSCert = *tlsCert
			case "tls-key":
				cfg.TLSKey = *tlsKey
			case "max-upload-mb":
				cfg.MaxUploadMB = *maxUpload
			case "smtp-port":
				cfg.SMTPPort = *smtpPort
			case "smtp-relay":
				cfg.SMTPRelay = *smtpRelay
			case "amqp-port":
				cfg.AMQPPort = *amqpPort
			case "mqtt-port":
				cfg.MQTTPort = *mqttPort
			case "transfer-port":
				cfg.TransferPort = *transferPort
			}
		})
	}
}

// loadConfig is defaults < pogi.yaml (path, $POGI_CONFIG or ./pogi.yaml) <
// what apply sets (environment and flags).
func loadConfig(path string, apply func(*pogi.Config)) (pogi.Config, error) {
	var cfg pogi.Config
	if path = config.Find(path); path != "" {
		f, err := config.Load(path)
		if err != nil {
			return cfg, err
		}
		cfg = pogi.ConfigFromFile(f)
	}
	cfg.Port = cmp.Or(cfg.Port, defaults.Port)
	cfg.Host = cmp.Or(cfg.Host, defaults.Host)
	cfg.Region = cmp.Or(cfg.Region, defaults.Region)
	cfg.Lambda = cmp.Or(cfg.Lambda, defaults.Lambda)
	cfg.IAM = cmp.Or(cfg.IAM, defaults.IAM)
	cfg.SMTPPort = cmp.Or(cfg.SMTPPort, defaults.SMTPPort)
	apply(&cfg)
	return cfg, nil
}

// envConfig is what pogi start would use without flags.
func envConfig(path string) (pogi.Config, error) {
	fs := flag.NewFlagSet("start", flag.ContinueOnError)
	apply := serverFlags(fs)
	applyEnvDefaults(fs)
	return loadConfig(path, apply)
}

func configCmd(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: pogi config init [--force] [path] | validate [path] | print [path]")
	}
	fs := flag.NewFlagSet("config "+args[0], flag.ExitOnError)
	force := fs.Bool("force", false, "init: overwrite an existing file")
	fs.Parse(args[1:])
	path := fs.Arg(0)
	switch args[0] {
	case "init":
		path = or(path, "pogi.yaml")
		if _, err := os.Stat(path); err == nil && !*force {
			return fmt.Errorf("%s already exists (--force overwrites it)", path)
		}
		if err := os.WriteFile(path, []byte(starter), 0o644); err != nil {
			return err
		}
		fmt.Println("wrote", path)
	case "validate":
		if path = config.Find(path); path == "" {
			return errors.New("no pogi.yaml here: pass a path, set POGI_CONFIG or run `pogi config init`")
		}
		if _, err := config.Load(path); err != nil {
			fmt.Fprintln(os.Stderr, err) // file:line lines, as editors and CI expect
			os.Exit(1)
		}
		fmt.Println("ok:", path)
	case "print":
		cfg, err := envConfig(path)
		if err != nil {
			return err
		}
		from := "no pogi.yaml"
		if cfg.File != nil {
			from = cfg.File.Path
		}
		fmt.Printf("# effective settings: defaults < %s < environment\n", from)
		enc := yaml.NewEncoder(os.Stdout)
		enc.SetIndent(2)
		return enc.Encode(asFile(cfg))
	default:
		return fmt.Errorf("unknown config command %q (init | validate | print)", args[0])
	}
	return nil
}

// asFile turns cfg back into pogi.yaml form, with secrets masked.
func asFile(cfg pogi.Config) config.File {
	var f config.File
	if cfg.File != nil {
		f = *cfg.File // chaos, identities, resources and stacks come only from the file
	}
	f.Version = 1
	f.Server = config.Server{Port: cfg.Port, Host: cfg.Host, Region: cfg.Region, Hostname: cfg.Hostname,
		TLS: config.TLS{Cert: cfg.TLSCert, Key: cfg.TLSKey}, CORS: config.CORS{Origins: cfg.CORSOrigins, Headers: cfg.CORSHeaders},
		AllowExternal: cfg.AllowExternal, MaxUploadMB: cfg.MaxUploadMB}
	f.Services = config.Services{}
	for _, s := range cfg.Services {
		if x, ok := strings.CutPrefix(s, "-"); ok {
			f.Services.Disable = append(f.Services.Disable, x)
		} else {
			f.Services.Enable = append(f.Services.Enable, s)
		}
	}
	if len(f.Services.Enable) == 0 {
		f.Services.Enable = []string{"all"}
	}
	f.Storage = config.Storage{Mode: or(cfg.Storage, "memory"), Dir: or(cfg.DataDir, "~/.pogi"), FlushInterval: cfg.FlushInterval, Services: cfg.StorageModes}
	f.IAM = config.IAM{Mode: cfg.IAM, ValidateSignatures: cfg.ValidateSigs}
	f.Lambda = config.Lambda{Executor: cfg.Lambda, WarmPool: cfg.WarmPool}
	f.Docker = config.Docker{Host: cfg.DockerHost, Network: cfg.DockerNetwork, Images: cfg.DockerImages}
	f.AI = config.AI{Endpoint: cfg.AIEndpoint, APIKey: mask(cfg.AIKey), Models: cfg.AIModels}
	f.Email = config.Email{SMTPPort: cfg.SMTPPort, Relay: cfg.SMTPRelay}
	if u, err := url.Parse(cfg.SMTPRelay); err == nil {
		f.Email.Relay = u.Redacted() // its password masked
	}
	f.Identities.AWS.Users = slices.Clone(f.Identities.AWS.Users)
	for i := range f.Identities.AWS.Users {
		f.Identities.AWS.Users[i].SecretKey = mask(f.Identities.AWS.Users[i].SecretKey)
	}
	return f
}

func mask(secret string) string {
	if secret == "" {
		return ""
	}
	return "********"
}

// servicesCmd lists the services of a running pogi, or else every service
// and whether the current config (pogi.yaml and environment) enables it.
func servicesCmd(args []string) error {
	fs := flag.NewFlagSet("services", flag.ExitOnError)
	ep := endpointFlag(fs)
	fs.Parse(args)
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	var st core.Status
	if err := api(*ep, "GET", "/_pogi/api/info", nil, &st); err == nil {
		fmt.Fprintln(tw, "CLOUD\tSERVICE\tTITLE\tRESOURCES\tREQUESTS")
		for _, s := range st.Services {
			fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%d\n", s.Cloud, s.Name, s.Title, s.Resources, s.Requests)
		}
		return tw.Flush()
	}
	cfg, err := envConfig("")
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "pogi is not running at %s; the current config enables:\n\n", *ep)
	fmt.Fprintln(tw, "CLOUD\tSERVICE\tENABLED")
	for _, s := range pogi.Services(cfg) {
		fmt.Fprintf(tw, "%s\t%s\t%s\n", s.Cloud, s.Name, map[bool]string{true: "yes", false: "no"}[s.Enabled])
	}
	return tw.Flush()
}

// waitCmd polls until pogi is ready, for CI: pogi start & pogi wait.
func waitCmd(args []string) error {
	fs := flag.NewFlagSet("wait", flag.ExitOnError)
	ep := endpointFlag(fs)
	timeout := fs.Duration("timeout", time.Minute, "give up after this long")
	fs.Parse(args)
	deadline := time.Now().Add(*timeout)
	for {
		var st core.Status
		// ponytail: a request that hangs outlives --timeout; give api a context if that bites
		err := api(*ep, "GET", "/_pogi/api/info", nil, &st)
		if err == nil && st.Ready {
			fmt.Println("ready:", *ep)
			return nil
		}
		if time.Now().After(deadline) {
			if err == nil {
				err = errors.New("it is still starting")
			}
			return fmt.Errorf("not ready at %s after %s: %v", *ep, *timeout, err)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// starter is what `pogi config init` writes.
const starter = `# pogi's settings, seed data, stacks, chaos rules and IAM identities.
#
# pogi start reads --config, $POGI_CONFIG, or ./pogi.yaml (or .yml).
# Command-line flags win over environment variables, which win over this file.
# Values may use ${VAR} and ${VAR:-default}.
# Check it with "pogi config validate"; see the merged result with "pogi config print".
version: 1                   # the format of this file

# Where and how pogi listens.
server:
  port: 7644
  host: 127.0.0.1            # anything but loopback needs allowExternal: true
  region: us-east-1          # the partition follows the region (cn-*, us-gov-*)
  # hostname: localhost      # aws.localhost: virtual-host style URLs everywhere
  # tls: {cert: ./certs/pogi.pem, key: ./certs/pogi-key.pem}   # default: pogi's local CA
  # cors: {origins: ["http://localhost:3000"], headers: []}    # extra browser origins
  # allowExternal: false
  # maxUploadMB: 10          # largest upload in MiB (0 means 10)

# Which services run: clouds (aws, gcp, azure), service names, or all.
# "pogi services" lists them.
services:
  enable: [all]
  # disable: [cosmos]

# How state is kept: memory (default), hybrid (written in the background),
# persistent (written before each response) or wal (a journal per change).
storage:
  mode: memory
  # dir: ~/.pogi             # state, snapshots and init hooks
  # flushInterval: 5s        # hybrid
  # services: {dynamodb: wal, s3: persistent}

# IAM enforcement: off, soft (record allow/deny decisions) or strict (deny).
iam:
  mode: off
  # validateSignatures: false   # reject requests whose SigV4 signature is wrong

# Lambda executor: auto, native (Python and Node.js on this machine) or docker.
lambda:
  executor: auto
  # warmPool: 4              # environments kept warm per function

# The container engine for real databases and Lambda's docker mode.
# docker:
#   host: ""                 # empty: auto-detect Docker, Podman, Colima, OrbStack
#   network: pogi
#   images: {rds.postgres: "postgres:16-alpine"}

# An OpenAI-compatible endpoint (Ollama, LM Studio) for Bedrock and the
# language services; without one they return canned answers.
# ai:
#   endpoint: http://localhost:11434/v1
#   apiKey: ${OPENAI_API_KEY:-}
#   models: {"anthropic.claude-*": llama3.1}

# SES's SMTP interface: mail sent to it shows in the console inbox, as from
# SendRawEmail. A relay also gets every message SES sends (Mailpit, MailHog).
# email:
#   smtpPort: 2525           # -1 turns it off
#   relay: smtp://localhost:1025

# Fault injection for matching calls: rate 0..1 (0 means always), latency,
# and an error status 400-599 with an optional code and message.
# "pogi chaos ls" shows them with their hit counts.
# chaos:
#   - {name: flaky-sqs, service: sqs, op: SendMessage, rate: 0.1, status: 500}
#   - {name: slow-dynamodb, service: dynamodb, latency: 300ms}

# IAM users (with fixed access keys) and roles, created at startup and after
# every reset.
# identities:
#   aws:
#     users:
#       - {name: dev, accessKey: AKIADEV0000000000001, secretKey: devsecret,
#          policies: ["arn:aws:iam::aws:policy/ReadOnlyAccess"]}
#     roles:
#       - {name: app, trust: lambda.amazonaws.com, policies: ["arn:aws:iam::aws:policy/AmazonS3FullAccess"]}

# Resources created at startup and after every reset; existing ones are
# skipped. Friendly forms are cloud: service: kind: [names or maps].
# resources:
#   aws:
#     s3: {buckets: [uploads, {name: logs, versioning: true}]}
#     sqs: {queues: [jobs, {name: events.fifo, fifo: true}]}
#     dynamodb: {tables: [{name: users, key: [pk, sk]}]}
#     secretsmanager: {secrets: {db-password: hunter2}}
#   gcp:
#     pubsub: {topics: [orders], subscriptions: [{name: worker, topic: orders}]}
#   azure:
#     blob: {containers: [images]}
#   typed:                   # CloudFormation form
#     - {type: "AWS::SNS::Topic", properties: {TopicName: alerts}}

# CloudFormation stacks deployed at startup; templates are relative to this file.
# stacks:
#   - {name: base, template: ./infra/local.yaml, parameters: {Env: dev}}
`
