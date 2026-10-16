// Command pogi runs local AWS, Google Cloud and Azure emulators with a CLI,
// a TUI and a web console — one binary, no Docker required.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"
	_ "time/tzdata" // schedules with time zones work in minimal containers too

	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/aws/lambda"
	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/docker"
	"github.com/nrndev/pogi/internal/gcp/functions"
	"github.com/nrndev/pogi/internal/tui"
)

const usage = `pogi — the whole cloud on your machine (AWS, Google Cloud, Azure)

Usage:
  pogi [start] [flags]     start the emulator (TUI in a terminal, logs otherwise)
  pogi tui                 attach the TUI to a running pogi
  pogi status              services, resources and request counts
  pogi services            services of the running pogi, or those the config enables
  pogi wait [--timeout d]  wait until pogi is ready (for CI)
  pogi config <cmd>        init | validate | print the pogi.yaml settings
  pogi env [--shell sh]    print SDK environment variables (eval "$(pogi env)"; --hostname aws.localhost)
  pogi aws <args>          run the aws CLI against pogi
  pogi tf                  print a Terraform provider override for pogi
  pogi kubeconfig gke <cluster>  print a kubeconfig for a GKE cluster (--project, --location)
  pogi logs [-f]           show recent API calls (follow with -f)
  pogi ls [service]        list resources
  pogi reset [service…]    delete state (all services when none given)
  pogi clean               delete containers and volumes left by a pogi that is gone (--dry-run, --yes, --state)
  pogi snapshot <cmd>      save | load | ls | rm | export | import
  pogi chaos <cmd>         add | ls | rm | clear fault-injection rules
  pogi lambda-sdks [pkg…]  install the AWS SDKs Lambda runtimes include (and the Functions Framework), for native functions
  pogi cert                print (and create) pogi's local CA and how to trust it
  pogi doctor              check the server, Lambda runtimes, SDKs, Docker and the CA
  pogi mcp                 MCP server on stdio for coding agents (--print-config: setup)
  pogi agents init         add a pogi brief for coding agents to AGENTS.md (--file CLAUDE.md)
  pogi version

Run "pogi start -h" for server flags. Client commands use --endpoint or
$POGI_ENDPOINT (default http://localhost:7644).
`

func main() {
	args := os.Args[1:]
	cmd := "start"
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		cmd, args = args[0], args[1:]
	}
	var err error
	switch cmd {
	case "start", "up", "run":
		err = start(args)
	case "tui":
		err = attach(args)
	case "status":
		err = status(args)
	case "services":
		err = servicesCmd(args)
	case "wait":
		err = waitCmd(args)
	case "config":
		err = configCmd(args)
	case "env":
		err = env(args)
	case "aws":
		err = awsCLI(args)
	case "tf", "terraform":
		err = terraform(args)
	case "kubeconfig":
		err = kubeconfig(args)
	case "logs":
		err = logs(args)
	case "ls", "resources":
		err = list(args)
	case "reset":
		err = reset(args)
	case "clean":
		err = clean(args)
	case "snapshot", "snapshots", "snap":
		err = snapshot(args)
	case "chaos":
		err = chaos(args)
	case "lambda-sdks":
		err = lambdaSDKs(args)
	case "cert":
		err = cert(args)
	case "doctor":
		err = doctor(args)
	case "mcp":
		err = mcpCmd(args)
	case "agents":
		err = agentsCmd(args)
	case "version", "--version", "-v":
		fmt.Println("pogi", pogi.Version)
	case "help", "-h", "--help":
		fmt.Print(usage)
	default:
		err = fmt.Errorf("unknown command %q\n\n%s", cmd, usage)
	}
	if err != nil {
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			os.Exit(exit.ExitCode())
		}
		fmt.Fprintln(os.Stderr, "pogi:", err)
		os.Exit(1)
	}
}

func getenv(keys ...string) string {
	for _, k := range keys {
		if v := os.Getenv(k); v != "" {
			return v
		}
	}
	return ""
}

func isTerminal(f *os.File) bool {
	st, err := f.Stat()
	return err == nil && st.Mode()&os.ModeCharDevice != 0
}

// ---- server ----

func start(args []string) error {
	fs := flag.NewFlagSet("start", flag.ExitOnError)
	apply := serverFlags(fs)
	file := fs.String("config", "", "pogi.yaml to load (default $POGI_CONFIG, else ./pogi.yaml or ./pogi.yml)")
	headless := fs.Bool("headless", false, "no TUI: print logs and requests")
	quiet := fs.Bool("quiet", false, "headless: do not print each request")
	debug := fs.Bool("debug", false, "verbose logging")
	applyEnvDefaults(fs)
	fs.Parse(args)

	cfg, err := loadConfig(*file, apply)
	if err != nil {
		return err
	}
	level := slog.LevelInfo
	if *debug {
		level = slog.LevelDebug
	}
	useTUI := !*headless && isTerminal(os.Stdout) && isTerminal(os.Stdin)
	var logOut io.Writer = os.Stderr
	if useTUI {
		dir := cfg.DataDir
		if dir == "" {
			home, _ := os.UserHomeDir()
			dir = filepath.Join(home, ".pogi")
		}
		os.MkdirAll(dir, 0o755)
		f, err := os.OpenFile(filepath.Join(dir, "pogi.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return err
		}
		defer f.Close()
		logOut = f
	}
	log := slog.New(slog.NewTextHandler(logOut, &slog.HandlerOptions{Level: level}))
	srv := pogi.New(cfg, log)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	defer func() { fmt.Fprint(os.Stderr, srv.Hub.ShutdownList()) }() // what was stopped; empty if pogi never ran

	if useTUI {
		ready := make(chan string, 1)
		done := make(chan error, 1)
		go func() { done <- srv.Run(ctx, func(u string) { ready <- u }) }()
		select {
		case u := <-ready:
			err := tui.Run(u, true)
			stop()
			<-done
			return err
		case err := <-done:
			return err
		}
	}
	return srv.Run(ctx, func(u string) {
		st := srv.Hub.Status()
		fmt.Fprintf(os.Stderr, "pogi %s  ·  %d services  ·  data %s%s\n", pogi.Version, len(st.Services), srv.Hub.Cfg.DataDir,
			map[bool]string{true: " (persistent)", false: ""}[st.Persist])
		fmt.Fprintf(os.Stderr, "  endpoint  %s\n  console   %s/_pogi/\n  env       eval \"$(pogi env)\"\n", u, u)
		if smtp := srv.Ports["ses"]; smtp != "" {
			fmt.Fprintf(os.Stderr, "  smtp      %s\n", strings.TrimPrefix(smtp, "smtp://"))
		}
		if amqp := srv.Ports["amqp"]; amqp != "" {
			fmt.Fprintf(os.Stderr, "  amqp      %s\n", strings.TrimPrefix(amqp, "amqp://"))
		}
		if cfg.File != nil {
			fmt.Fprintf(os.Stderr, "  config    %s\n", cfg.File.Path)
		}
		fmt.Println("Ready.")
		if !*quiet {
			core.Go("pogi.printRequests", func() { printRequests(ctx, srv.Hub) })
		}
	})
}

// applyEnvDefaults lets POGI_* and common LocalStack variables set flag defaults.
func applyEnvDefaults(fs *flag.FlagSet) {
	set := func(name string, keys ...string) {
		if v := getenv(keys...); v != "" {
			fs.Set(name, v)
		}
	}
	set("port", "POGI_PORT")
	set("host", "POGI_HOST")
	set("services", "POGI_SERVICES", "SERVICES")
	set("data-dir", "POGI_DATA_DIR")
	set("region", "POGI_REGION", "DEFAULT_REGION")
	set("lambda", "POGI_LAMBDA")
	set("iam", "POGI_IAM")
	set("storage", "POGI_STORAGE")
	set("validate-signatures", "POGI_VALIDATE_SIGNATURES")
	set("allow-external", "POGI_ALLOW_EXTERNAL")
	set("hostname", "POGI_HOSTNAME")
	set("smtp-port", "POGI_SMTP_PORT")
	set("smtp-relay", "POGI_SMTP_RELAY")
	set("mqtt-port", "POGI_MQTT_PORT")
	set("amqp-port", "POGI_AMQP_PORT")
	set("transfer-port", "POGI_TRANSFER_PORT")
	if getenv("POGI_PERSIST", "PERSISTENCE") == "1" || getenv("POGI_PERSIST") == "true" {
		fs.Set("persist", "true")
	}
	if getenv("DEBUG", "POGI_DEBUG") == "1" {
		fs.Set("debug", "true")
	}
	if gl := getenv("GATEWAY_LISTEN"); gl != "" { // LocalStack: 0.0.0.0:7644[,…]
		first, _, _ := strings.Cut(gl, ",")
		if h, p, ok := strings.Cut(first, ":"); ok {
			fs.Set("host", h)
			fs.Set("port", p)
		}
	}
}

func printRequests(ctx context.Context, hub *core.Hub) {
	var after int64
	for ctx.Err() == nil {
		hub.Requests.Wait(ctx, after, 30*time.Second)
		for _, e := range hub.Requests.Query(after, 0, "", "", false) {
			after = e.ID
			fmt.Println(formatEntry(e))
		}
	}
}

func formatEntry(e core.Entry) string {
	s := fmt.Sprintf("%s  %-5s %-10s %-32s %3d  %6.1fms", e.Time.Local().Format("15:04:05.000"), e.Cloud, e.Service, e.Op, e.Status, e.Ms)
	if e.Error != "" {
		s += "  " + e.Error
	}
	if e.Chaos {
		s += "  (chaos)"
	}
	return s
}

// ---- client commands (admin API) ----

func endpointFlag(fs *flag.FlagSet) *string {
	def := getenv("POGI_ENDPOINT")
	if def == "" {
		def = "http://localhost:7644"
	}
	return fs.String("endpoint", def, "pogi endpoint")
}

func api(endpoint, method, path string, body io.Reader, out any) error {
	req, err := http.NewRequest(method, strings.TrimRight(endpoint, "/")+path, body)
	if err != nil {
		return err
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("pogi is not reachable at %s (start it with `pogi`): %v", endpoint, err)
	}
	defer res.Body.Close()
	if res.StatusCode >= 400 {
		var e struct{ Error string }
		b, _ := io.ReadAll(res.Body)
		if json.Unmarshal(b, &e) == nil && e.Error != "" {
			return errors.New(e.Error)
		}
		return fmt.Errorf("%s: %s", res.Status, b)
	}
	if out == nil {
		return nil
	}
	if w, ok := out.(io.Writer); ok {
		_, err = io.Copy(w, res.Body)
		return err
	}
	return json.NewDecoder(res.Body).Decode(out)
}

func status(args []string) error {
	fs := flag.NewFlagSet("status", flag.ExitOnError)
	ep := endpointFlag(fs)
	fs.Parse(args)
	var st core.Status
	if err := api(*ep, "GET", "/_pogi/api/info", nil, &st); err != nil {
		return err
	}
	fmt.Printf("pogi %s at %s · up %s · region %s · persistence %v · iam %s\n\n", st.Version, *ep,
		time.Duration(st.Uptime)*time.Second, st.Region, st.Persist, st.IAM)
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "CLOUD\tSERVICE\tRESOURCES\tREQUESTS")
	for _, s := range st.Services {
		fmt.Fprintf(tw, "%s\t%s\t%d\t%d\n", s.Cloud, s.Name, s.Resources, s.Requests)
	}
	return tw.Flush()
}

func env(args []string) error {
	fs := flag.NewFlagSet("env", flag.ExitOnError)
	ep := endpointFlag(fs)
	shell := fs.String("shell", "sh", "sh | fish | pwsh | dotenv")
	region := fs.String("region", getenv("AWS_DEFAULT_REGION", "AWS_REGION", "POGI_REGION"), "AWS region")
	hostname := fs.String("hostname", "", "endpoint host name, e.g. aws.localhost (SDKs then use S3 virtual-host style)")
	fs.Parse(args)
	if *region == "" {
		*region = core.DefaultRegion
	}
	vars := core.Env(*ep, *region)
	if *hostname != "" {
		u, err := url.Parse(*ep)
		if err != nil {
			return err
		}
		at := func(host string) string {
			v := *u
			v.Host = host
			if p := u.Port(); p != "" {
				v.Host += ":" + p
			}
			return v.String()
		}
		vars = core.Env(at(*hostname), *region)
		if *hostname != "localhost" && net.ParseIP(*hostname) == nil {
			// <bucket>.s3.<hostname> is S3 whatever hostname pogi runs with
			vars = append(vars, "AWS_ENDPOINT_URL_S3="+at("s3."+*hostname))
		}
	}
	if f := gcloudTokenFile(); f != "" { // gcloud reads its token from a file
		vars = append(vars, "CLOUDSDK_AUTH_ACCESS_TOKEN_FILE="+f)
	}
	for _, kv := range vars {
		k, v, _ := strings.Cut(kv, "=")
		switch *shell {
		case "fish":
			fmt.Printf("set -gx %s %q;\n", k, v)
		case "pwsh", "powershell":
			fmt.Printf("$env:%s=%q\n", k, v)
		case "dotenv":
			fmt.Printf("%s=%s\n", k, v)
		default:
			fmt.Printf("export %s=%q\n", k, v)
		}
	}
	return nil
}

// gcloudTokenFile writes the dummy access token gcloud is pointed at
// (CLOUDSDK_AUTH_ACCESS_TOKEN_FILE) under ~/.pogi and returns its path, or ""
// when it cannot be written.
func gcloudTokenFile() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	f := filepath.Join(home, ".pogi", "gcloud-access-token")
	if os.MkdirAll(filepath.Dir(f), 0o755) != nil || os.WriteFile(f, []byte("pogi-token"), 0o600) != nil {
		return ""
	}
	return f
}

func awsCLI(args []string) error {
	ep := getenv("POGI_ENDPOINT")
	if ep == "" {
		ep = "http://localhost:7644"
	}
	region := getenv("AWS_DEFAULT_REGION", "AWS_REGION")
	if region == "" {
		region = core.DefaultRegion
	}
	cmd := exec.Command("aws", args...)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stdout, os.Stderr
	cmd.Env = os.Environ()
	for _, kv := range core.AWSEnv(ep, region) {
		k, _, _ := strings.Cut(kv, "=")
		if k == "AWS_ENDPOINT_URL" || os.Getenv(k) == "" {
			cmd.Env = append(cmd.Env, kv)
		}
	}
	if err := cmd.Run(); err != nil {
		if errors.Is(err, exec.ErrNotFound) {
			return errors.New("the aws CLI is not installed (https://aws.amazon.com/cli/)")
		}
		return err
	}
	return nil
}

// terraformEndpoints maps pogi service names to Terraform AWS provider endpoint keys.
var terraformEndpoints = map[string][]string{
	"accessanalyzer": {"accessanalyzer"}, "account": {"account"}, "acm": {"acm"},
	"amp": {"prometheus"}, "apigateway": {"apigateway", "apigatewayv2"}, "appconfig": {"appconfig"},
	"appintegrations": {"appintegrations"}, "application-autoscaling": {"appautoscaling"},
	"appsync": {"appsync"}, "athena": {"athena"}, "autoscaling": {"autoscaling"}, "backup": {"backup"},
	"batch": {"batch"}, "bcm-data-exports": {"bcmdataexports"}, "bedrock": {"bedrock"},
	"bedrock-agent": {"bedrockagent"}, "bedrock-agentcore-control": {"bedrockagentcore"},
	"bedrock-runtime": {"bedrockruntime"}, "budgets": {"budgets"}, "ce": {"ce"},
	"cloudcontrol": {"cloudcontrolapi"}, "cloudformation": {"cloudformation"},
	"cloudfront": {"cloudfront"}, "cloudhsmv2": {"cloudhsmv2"}, "cloudtrail": {"cloudtrail"},
	"cloudwatch": {"cloudwatch"}, "codeartifact": {"codeartifact"}, "codebuild": {"codebuild"},
	"codedeploy": {"deploy"}, "codeguru-reviewer": {"codegurureviewer"},
	"codepipeline": {"codepipeline"}, "cognito-identity": {"cognitoidentity"},
	"cognito-idp": {"cognitoidp"}, "comprehend": {"comprehend"}, "config": {"configservice"},
	"connect": {"connect"}, "controltower": {"controltower"}, "cur": {"cur"}, "datasync": {"datasync"},
	"detective": {"detective"}, "dlm": {"dlm"}, "dms": {"dms"}, "dynamodb": {"dynamodb"},
	"ec2": {"ec2"}, "ecr": {"ecr"}, "ecs": {"ecs"}, "efs": {"efs"}, "eks": {"eks"},
	"elasticache": {"elasticache"}, "elasticbeanstalk": {"elasticbeanstalk"}, "elb": {"elb"},
	"elbv2": {"elbv2"}, "emr": {"emr"}, "emr-serverless": {"emrserverless"}, "events": {"events"},
	"firehose": {"firehose"}, "fis": {"fis"}, "globalaccelerator": {"globalaccelerator"},
	"glue": {"glue"}, "guardduty": {"guardduty"}, "iam": {"iam"}, "identitystore": {"identitystore"},
	"inspector2": {"inspector2"}, "iot": {"iot"}, "kafka": {"kafka"}, "kinesis": {"kinesis"},
	"kinesisanalyticsv2": {"kinesisanalyticsv2"}, "kms": {"kms"}, "lakeformation": {"lakeformation"},
	"lambda": {"lambda"}, "lightsail": {"lightsail"}, "logs": {"cloudwatchlogs"}, "macie2": {"macie2"},
	"memorydb": {"memorydb"}, "mq": {"mq"}, "mwaa": {"mwaa"}, "network-firewall": {"networkfirewall"},
	"oam": {"oam"}, "opensearch": {"opensearch", "es"}, "organizations": {"organizations"},
	"pipes": {"pipes"}, "pricing": {"pricing"}, "ram": {"ram"}, "rds": {"rds", "docdb", "neptune"},
	"rds-data": {"rdsdata"}, "redshift": {"redshift"}, "redshift-data": {"redshiftdata"},
	"redshift-serverless": {"redshiftserverless"}, "rekognition": {"rekognition"},
	"resource-explorer-2": {"resourceexplorer2"}, "route53": {"route53"},
	"route53resolver": {"route53resolver"}, "rum": {"rum"}, "s3": {"s3", "s3control"},
	"s3tables": {"s3tables"}, "s3vectors": {"s3vectors"}, "sagemaker": {"sagemaker"},
	"scheduler": {"scheduler"}, "secretsmanager": {"secretsmanager"}, "securityhub": {"securityhub"},
	"service-quotas": {"servicequotas"}, "servicecatalog": {"servicecatalog"},
	"servicediscovery": {"servicediscovery"}, "ses": {"ses", "sesv2"}, "sns": {"sns"}, "sqs": {"sqs"},
	"ssm": {"ssm"}, "sso": {"sso"}, "sso-admin": {"ssoadmin"}, "stepfunctions": {"sfn"},
	"sts": {"sts"}, "swf": {"swf"}, "timestream-influxdb": {"timestreaminfluxdb"},
	"transcribe": {"transcribe"}, "transfer": {"transfer"},
	"verifiedpermissions": {"verifiedpermissions"}, "wafv2": {"wafv2"},
}

// googleEndpoints maps the Terraform google provider's custom endpoint arguments to the base path
// of each API on pogi. The provider appends its own paths to these, and wants the trailing slash.
var googleEndpoints = map[string]string{
	"storage": "/storage/v1/", "pubsub": "/v1/", "secret_manager": "/v1/", "firestore": "/v1/", "cloud_run_v2": "/v2/",
	"cloudfunctions2": "/v2/", "cloud_tasks": "/v2/", "cloud_scheduler": "/v1/", "eventarc": "/v1/", "managed_kafka": "/v1/",
	"iam": "/v1/", "iam_beta": "/v1/", "iam_credentials": "/v1/", "resource_manager": "/v1/", "service_usage": "/v1/", "kms": "/v1/",
	"sql": "/sql/v1beta4/", "big_query": "/bigquery/v2/", "logging": "/v2/", "monitoring": "/",
	"cloud_resource_manager": "/v1/", "resource_manager_v3": "/v3/",
}

// googleProvider prints the google provider's override: every API on pogi's main port.
// An endpoint of an API that pogi does not serve (yet) is harmless: nothing calls it.
func googleProvider(ep, project, region string) {
	fmt.Printf(`# Generated by "pogi tf --cloud gcp": point the google provider at pogi.
provider "google" {
  project               = %q
  region                = %q
  access_token          = "pogi"
  user_project_override = false
`, project, region)
	for _, k := range slices.Sorted(maps.Keys(googleEndpoints)) {
		fmt.Printf("  %-30s = %q\n", k+"_custom_endpoint", strings.TrimSuffix(ep, "/")+googleEndpoints[k])
	}
	fmt.Println("}")
}

// azureProvider prints the azurerm provider's override. azurerm reads every URL from the metadata
// host, over TLS, and Terraform built with Go trusts only the system's certificates (on macOS
// the keychain, which pogi's CA must not be added to), so the comment shows how to run Terraform
// in a container that trusts the CA through SSL_CERT_FILE. A Key Vault is reached as
// <vault>.vault.localhost, which the container resolves to itself unless it is told otherwise.
func azureProvider(ep, host string) error {
	if host == "" {
		u, err := url.Parse(ep)
		if err != nil || u.Port() == "" {
			return fmt.Errorf("--endpoint %q: want http://host:port", ep)
		}
		host = "host.docker.internal:" + u.Port()
	}
	var st core.Status
	api(ep, "GET", "/_pogi/api/info", nil, &st) // the data dir of the pogi that runs; the default one when it does not
	ca := filepath.Join(defaultDataDir(st.DataDir), "certs", "ca.pem")
	fmt.Printf(`# Generated by "pogi tf --cloud azure": point the azurerm provider at pogi.
# Run Terraform in a container, which trusts pogi's CA (and reaches pogi at host.docker.internal):
#   docker run --rm -it -v "$PWD":/work -w /work --add-host host.docker.internal:host-gateway \
#     --add-host <your-vault>.vault.localhost:host-gateway \
#     -v %s:/pogi-ca.pem -e SSL_CERT_FILE=/pogi-ca.pem \
#     -v "$HOME/.terraform.d/plugin-cache":/cache -e TF_PLUGIN_CACHE_DIR=/cache hashicorp/terraform apply
provider "azurerm" {
  features {}
  metadata_host                   = %q
  subscription_id                 = %q
  tenant_id                       = %q
  client_id                       = "pogi"
  client_secret                   = "pogi"
  resource_provider_registrations = "none"
}
`, ca, host, core.AzureSubscription, arm.TenantID)
	return nil
}

// kubeconfig prints a kubeconfig for a GKE cluster that carries a static pogi access token,
// for machines without gke-gcloud-auth-plugin: pogi kubeconfig gke <cluster> [--project p] [--location l].
func kubeconfig(args []string) error {
	fs := flag.NewFlagSet("kubeconfig", flag.ExitOnError)
	ep := endpointFlag(fs)
	project := fs.String("project", core.DefaultProject, "Google Cloud project")
	location := fs.String("location", "us-central1", "cluster location")
	var pos []string
	if len(args) >= 2 && !strings.HasPrefix(args[0], "-") { // flags may follow the arguments
		pos, args = args[:2], args[2:]
	}
	fs.Parse(args)
	if pos == nil {
		pos = fs.Args()
	}
	if len(pos) != 2 || pos[0] != "gke" {
		return errors.New("usage: pogi kubeconfig gke <cluster> [--project p] [--location l]")
	}
	return api(*ep, "GET", "/_gcp/gke/kubeconfig/"+url.PathEscape(*project)+"/"+url.PathEscape(*location)+"/"+url.PathEscape(pos[1]), nil, os.Stdout)
}

func terraform(args []string) error {
	fs := flag.NewFlagSet("tf", flag.ExitOnError)
	ep := endpointFlag(fs)
	region := fs.String("region", "", "region (default us-east-1 for aws, us-central1 for gcp)")
	cloud := fs.String("cloud", "aws", "aws, gcp or azure")
	project := fs.String("project", core.DefaultProject, "Google Cloud project")
	metadataHost := fs.String("metadata-host", "", "azurerm's metadata_host: host:port (default host.docker.internal and the endpoint's port)")
	fs.Parse(args)
	switch *cloud {
	case "gcp":
		if *region == "" {
			*region = "us-central1"
		}
		googleProvider(*ep, *project, *region)
		return nil
	case "azure":
		return azureProvider(*ep, *metadataHost)
	case "aws":
		if *region == "" {
			*region = core.DefaultRegion
		}
	default:
		return fmt.Errorf("--cloud %q: want aws, gcp or azure", *cloud)
	}
	var keys []string
	var st core.Status
	skipAccount := true // with STS running the provider asks it for the account, and builds ARNs and ids from it (Security Hub's)
	if err := api(*ep, "GET", "/_pogi/api/info", nil, &st); err == nil {
		for _, s := range st.Services {
			keys = append(keys, terraformEndpoints[s.Name]...)
			skipAccount = skipAccount && s.Name != "sts"
		}
	} else {
		for _, ks := range terraformEndpoints {
			keys = append(keys, ks...)
		}
	}
	fmt.Printf(`# Generated by "pogi tf": point the AWS provider at pogi.
provider "aws" {
  region                      = %q
  access_key                  = "test"
  secret_key                  = "test"
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = %t
  s3_use_path_style           = true

  endpoints {
`, *region, skipAccount)
	for _, k := range sortStrings(keys) {
		fmt.Printf("    %-16s = %q\n", k, *ep)
	}
	fmt.Println("  }\n}")
	return nil
}

func logs(args []string) error {
	fs := flag.NewFlagSet("logs", flag.ExitOnError)
	ep := endpointFlag(fs)
	follow := fs.Bool("f", false, "follow")
	service := fs.String("service", "", "only this service")
	errorsOnly := fs.Bool("errors", false, "only failed calls")
	n := fs.Int("n", 50, "how many recent calls to show")
	fs.Parse(args)
	q := url.Values{"service": {*service}, "limit": {strconv.Itoa(*n)}}
	if *errorsOnly {
		q.Set("errors", "1")
	}
	var after int64
	for {
		var entries []core.Entry
		if err := api(*ep, "GET", "/_pogi/api/requests?"+q.Encode(), nil, &entries); err != nil {
			return err
		}
		for _, e := range entries {
			after = e.ID
			fmt.Println(formatEntry(e))
		}
		if !*follow {
			return nil
		}
		q.Set("after", strconv.FormatInt(after, 10))
		q.Set("wait", "25")
		q.Set("limit", "500")
	}
}

func list(args []string) error {
	fs := flag.NewFlagSet("ls", flag.ExitOnError)
	ep := endpointFlag(fs)
	fs.Parse(args)
	path := "/_pogi/api/resources"
	if fs.NArg() > 0 {
		path += "?service=" + url.QueryEscape(fs.Arg(0))
	}
	var res map[string][]core.Table
	if err := api(*ep, "GET", path, nil, &res); err != nil {
		return err
	}
	for _, svc := range core.SortedKeys(res) {
		for _, t := range res[svc] {
			fmt.Printf("%s · %s (%d)\n", svc, t.Name, len(t.Rows))
			if len(t.Rows) == 0 {
				fmt.Println()
				continue
			}
			tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
			fmt.Fprintln(tw, "  "+strings.ToUpper(strings.Join(t.Columns, "\t")))
			for _, r := range t.Rows {
				fmt.Fprintln(tw, "  "+strings.Join(r, "\t"))
			}
			tw.Flush()
			fmt.Println()
		}
	}
	return nil
}

func reset(args []string) error {
	fs := flag.NewFlagSet("reset", flag.ExitOnError)
	ep := endpointFlag(fs)
	fs.Parse(args)
	path := "/_pogi/api/reset"
	if fs.NArg() > 0 {
		path += "?service=" + url.QueryEscape(strings.Join(fs.Args(), ","))
	}
	if err := api(*ep, "POST", path, nil, nil); err != nil {
		return err
	}
	fmt.Println("reset", map[bool]string{true: "all services", false: strings.Join(fs.Args(), ", ")}[fs.NArg() == 0])
	return nil
}

func snapshot(args []string) error {
	fs := flag.NewFlagSet("snapshot", flag.ExitOnError)
	ep := endpointFlag(fs)
	if len(args) == 0 {
		args = []string{"ls"}
	}
	sub := args[0]
	fs.Parse(args[1:])
	name := fs.Arg(0)
	need := func() error {
		if name == "" {
			return fmt.Errorf("usage: pogi snapshot %s NAME", sub)
		}
		return nil
	}
	p := "/_pogi/api/snapshots/" + url.PathEscape(name)
	switch sub {
	case "ls", "list":
		var snaps []core.SnapshotInfo
		if err := api(*ep, "GET", "/_pogi/api/snapshots", nil, &snaps); err != nil {
			return err
		}
		tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "NAME\tSIZE\tSAVED")
		for _, s := range snaps {
			fmt.Fprintf(tw, "%s\t%d\t%s\n", s.Name, s.Size, s.Saved.Local().Format(time.DateTime))
		}
		return tw.Flush()
	case "save":
		if name == "" {
			name = time.Now().Format("2006-01-02_15-04-05")
			p += url.PathEscape(name)
		}
		if err := api(*ep, "POST", p, nil, nil); err != nil {
			return err
		}
		fmt.Println("saved", name)
	case "load", "restore":
		if err := need(); err != nil {
			return err
		}
		if err := api(*ep, "POST", p+"/load", nil, nil); err != nil {
			return err
		}
		fmt.Println("loaded", name)
	case "rm", "delete":
		if err := need(); err != nil {
			return err
		}
		return api(*ep, "DELETE", p, nil, nil)
	case "export":
		if err := need(); err != nil {
			return err
		}
		out := io.Writer(os.Stdout)
		if file := fs.Arg(1); file != "" {
			f, err := os.Create(file)
			if err != nil {
				return err
			}
			defer f.Close()
			out = f
		}
		return api(*ep, "GET", p, nil, out)
	case "import":
		file := fs.Arg(0)
		if file == "" {
			return errors.New("usage: pogi snapshot import FILE [NAME]")
		}
		b, err := os.ReadFile(file)
		if err != nil {
			return err
		}
		name = fs.Arg(1)
		if name == "" {
			name = strings.TrimSuffix(filepath.Base(file), filepath.Ext(file))
		}
		if err := api(*ep, "PUT", "/_pogi/api/snapshots/"+url.PathEscape(name), bytes.NewReader(b), nil); err != nil {
			return err
		}
		fmt.Println("imported", name, "(load it with: pogi snapshot load", name+")")
	default:
		return fmt.Errorf("unknown snapshot command %q (save | load | ls | rm | export | import)", sub)
	}
	return nil
}

func chaos(args []string) error {
	fs := flag.NewFlagSet("chaos", flag.ExitOnError)
	ep := endpointFlag(fs)
	service := fs.String("service", "", "service glob (default: any)")
	op := fs.String("op", "", "operation glob, e.g. Put* (default: any)")
	rate := fs.Float64("rate", 1, "probability 0..1")
	latency := fs.Duration("latency", 0, "added latency, e.g. 300ms")
	status := fs.Int("status", 0, "HTTP status to inject, e.g. 500")
	code := fs.String("code", "", "error code, e.g. ThrottlingException")
	msg := fs.String("message", "", "error message")
	if len(args) == 0 {
		args = []string{"ls"}
	}
	sub := args[0]
	fs.Parse(args[1:])
	switch sub {
	case "ls", "list":
		var rules []core.Rule
		if err := api(*ep, "GET", "/_pogi/api/chaos", nil, &rules); err != nil {
			return err
		}
		tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "ID\tSERVICE\tOP\tRATE\tLATENCY\tERROR\tHITS")
		for _, r := range rules {
			p := r.Probability
			if p == 0 {
				p = 1
			}
			fmt.Fprintf(tw, "%s\t%s\t%s\t%.0f%%\t%dms\t%d %s\t%d\n", r.ID, or(r.Service, "*"), or(r.Op, "*"), p*100, r.LatencyMs, r.Status, r.Code, r.Hits)
		}
		return tw.Flush()
	case "add":
		if *status == 0 && *latency == 0 {
			return errors.New("give --status and/or --latency")
		}
		b, _ := json.Marshal(core.Rule{Service: *service, Op: *op, Probability: *rate, LatencyMs: int(latency.Milliseconds()), Status: *status, Code: *code, Message: *msg})
		var r core.Rule
		if err := api(*ep, "POST", "/_pogi/api/chaos", bytes.NewReader(b), &r); err != nil {
			return err
		}
		fmt.Println("added rule", r.ID)
	case "rm", "remove":
		if fs.NArg() == 0 {
			return errors.New("usage: pogi chaos rm ID")
		}
		return api(*ep, "DELETE", "/_pogi/api/chaos/"+url.PathEscape(fs.Arg(0)), nil, nil)
	case "clear":
		return api(*ep, "DELETE", "/_pogi/api/chaos", nil, nil)
	default:
		return fmt.Errorf("unknown chaos command %q (add | ls | rm | clear)", sub)
	}
	return nil
}

func attach(args []string) error {
	fs := flag.NewFlagSet("tui", flag.ExitOnError)
	ep := endpointFlag(fs)
	fs.Parse(args)
	if err := api(*ep, "GET", "/_pogi/api/info", nil, nil); err != nil {
		return err
	}
	return tui.Run(*ep, false)
}

func or(s, def string) string {
	if s == "" {
		return def
	}
	return s
}

func sortStrings(s []string) []string {
	slices.Sort(s)
	return slices.Compact(s)
}

// nodeSDKs are the AWS SDK v3 packages installed for native Node.js functions:
// the clients CDK's custom resources and typical handlers import.
var nodeSDKs = []string{"client-s3", "lib-storage", "client-dynamodb", "lib-dynamodb", "client-sqs", "client-sns", "client-lambda",
	"client-sfn", "client-eventbridge", "client-scheduler", "client-kinesis", "client-firehose", "client-secrets-manager", "client-ssm",
	"client-kms", "client-sts", "client-iam", "client-cloudformation", "client-cloudwatch", "client-cloudwatch-logs", "client-ecr",
	"client-ec2", "client-ses", "client-sesv2", "client-cognito-identity-provider", "client-api-gateway",
	"client-apigatewaymanagementapi", "client-route-53", "client-acm", "client-resource-groups-tagging-api"}

// lambdaSDKs installs the AWS SDKs that real Lambda runtimes include (AWS SDK
// for JavaScript v3, boto3) where pogi's native runtimes look for them, and the
// Python Functions Framework for Cloud Functions.
func lambdaSDKs(args []string) error {
	fs := flag.NewFlagSet("lambda-sdks", flag.ExitOnError)
	dataDir := fs.String("data-dir", "", "pogi's data dir (default ~/.pogi)")
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: pogi lambda-sdks [--data-dir dir] [extra npm or pip packages…]")
		fs.PrintDefaults()
	}
	fs.Parse(args)
	if *dataDir == "" {
		home, _ := os.UserHomeDir()
		*dataDir = filepath.Join(home, ".pogi")
	}
	dir := lambda.SDKDir(*dataDir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	run := func(name string, a ...string) error {
		fmt.Println("$", name, strings.Join(a, " "))
		c := exec.Command(name, a...)
		c.Stdout, c.Stderr = os.Stdout, os.Stderr
		return c.Run()
	}
	var extraNode, extraPy []string
	for _, p := range fs.Args() {
		if strings.HasPrefix(p, "@") {
			extraNode = append(extraNode, p)
		} else {
			extraPy = append(extraPy, p)
		}
	}
	did := false
	if _, err := exec.LookPath("npm"); err == nil {
		pkgs := extraNode
		for _, p := range nodeSDKs {
			pkgs = append(pkgs, "@aws-sdk/"+p)
		}
		if err := run("npm", append([]string{"install", "--no-audit", "--no-fund", "--prefix", dir}, pkgs...)...); err != nil {
			return err
		}
		did = true
	}
	if py, err := exec.LookPath("python3"); err == nil {
		if err := run(py, append([]string{"-m", "pip", "install", "--quiet", "--upgrade", "--target", filepath.Join(dir, "python"), "boto3"}, extraPy...)...); err != nil {
			return err
		}
		// the Functions Framework that runs Cloud Functions natively
		if err := run(py, "-m", "pip", "install", "--quiet", "--upgrade", "--target", filepath.Join(functions.SDKDir(*dataDir), "python"), "functions-framework"); err != nil {
			return err
		}
		did = true
	}
	if !did {
		return errors.New("neither npm nor python3 is installed")
	}
	fmt.Println("installed into", dir)
	return nil
}

func defaultDataDir(dir string) string {
	if dir != "" {
		return dir
	}
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".pogi")
}

// cert makes sure pogi's local CA exists and says how to trust it.
func cert(args []string) error {
	fs := flag.NewFlagSet("cert", flag.ExitOnError)
	dataDir := fs.String("data-dir", "", "pogi's data dir (default ~/.pogi)")
	fs.Parse(args)
	h := core.NewHub(core.Config{DataDir: defaultDataDir(*dataDir)}, nil)
	if _, err := h.TLS(); err != nil {
		return err
	}
	ca, _, _ := h.CertPaths()
	fmt.Printf("pogi's CA: %s\n\nIt signs the certificate for localhost, *.localhost and host.docker.internal that\nport 7644 serves HTTPS with. Trust it once:\n\n", ca)
	switch runtime.GOOS {
	case "darwin":
		fmt.Printf("  sudo security add-trusted-cert -d -r trustRoot -k /Library/Keychains/System.keychain %s\n", ca)
	case "linux":
		fmt.Printf("  sudo cp %s /usr/local/share/ca-certificates/pogi.crt && sudo update-ca-certificates\n", ca)
	case "windows":
		fmt.Printf("  certutil -addstore -f ROOT %s\n", ca)
	}
	fmt.Printf("\nor per tool: NODE_EXTRA_CA_CERTS=%s, AWS_CA_BUNDLE=%s, SSL_CERT_FILE (replaces the system list)\n", ca, ca)
	return nil
}

// doctor checks what pogi depends on and says how to fix what is missing.
func doctor(args []string) error {
	fs := flag.NewFlagSet("doctor", flag.ExitOnError)
	ep := endpointFlag(fs)
	dataDir := fs.String("data-dir", "", "pogi's data dir (default ~/.pogi)")
	fs.Parse(args)
	dir := defaultDataDir(*dataDir)
	bad := 0
	check := func(ok bool, what, fix string) {
		if ok {
			fmt.Println("  ✓", what)
			return
		}
		bad++
		fmt.Println("  ✗", what, "—", fix)
	}
	version := func(bin string, args ...string) (string, bool) {
		out, err := exec.Command(bin, args...).CombinedOutput()
		return strings.TrimSpace(strings.SplitN(string(out), "\n", 2)[0]), err == nil
	}
	var st core.Status
	err := api(*ep, "GET", "/_pogi/api/info", nil, &st)
	check(err == nil, "server at "+*ep+" "+st.Version, "start it with `pogi start` (or pass --endpoint)")
	py, okPy := version("python3", "--version")
	check(okPy, "python3 for Python Lambdas: "+py, "install Python 3, or run Lambdas with --lambda docker")
	node, okNode := version("node", "--version")
	check(okNode, "node for Node.js Lambdas: "+node, "install Node.js 18+, or run Lambdas with --lambda docker")
	sdk := lambda.SDKDir(dir)
	_, errJS := os.Stat(filepath.Join(sdk, "node_modules", "@aws-sdk", "client-s3"))
	_, errBoto := os.Stat(filepath.Join(sdk, "python", "boto3"))
	check(errJS == nil && errBoto == nil, "AWS SDKs for native Lambdas in "+sdk, "run `pogi lambda-sdks` (CDK custom resources need them)")
	if e, ok := docker.New(docker.Options{}).Engine(); ok {
		fmt.Printf("  ✓ %s %s at %s (Engine API %s)\n", e.Name, e.Version, e.Socket, e.API)
	} else {
		fmt.Println("  · no container engine found (only --lambda docker, RDS and ElastiCache need one):",
			"start Docker Desktop, OrbStack, Colima or Podman, or set DOCKER_HOST")
	}
	if _, err := net.LookupHost("probe.aws.localhost"); err == nil {
		fmt.Println("  ✓ *.localhost names resolve (pogi env --hostname aws.localhost, virtual-host S3, execute-api and lambda-url hosts)")
	} else {
		fmt.Println("  · *.localhost names do not resolve here (musl/Alpine, Linux without systemd-resolved): use localhost and path-style S3")
	}
	ca := filepath.Join(dir, "certs", "ca.pem")
	_, errCA := os.Stat(ca)
	check(errCA == nil, "local CA "+ca, "run `pogi cert` (created on first start)")
	if bad > 0 {
		return fmt.Errorf("%d check(s) need attention", bad)
	}
	return nil
}
