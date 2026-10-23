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

	"github.com/charmbracelet/x/term"
	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/aws/lambda"
	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/docker"
	"github.com/nrndev/pogi/internal/gcp/functions"
	"github.com/nrndev/pogi/internal/tui"
	"github.com/nrndev/pogi/internal/web"
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
  pogi tf diagram [path]   draw a Terraform folder or plan.json in the console (-o diagram.svg: write a file)
  pogi cost [path]         what a Terraform folder or plan.json costs a month (sizes: list sizes); no path: what runs now
  pogi kubeconfig gke <cluster>  print a kubeconfig for a GKE cluster (--project, --location)
  pogi logs [-f]           show recent API calls (-f, --follow: keep printing new ones)
  pogi ls [service]        list resources
  pogi reset [service…]    delete state (all services when none given)
  pogi clean               delete containers and volumes left by a pogi that is gone (--dry-run, --yes, --state)
  pogi pull <path>         download the images a Terraform folder or plan.json needs (--all: every service's, --list: show them)
  pogi snapshot <cmd>      save | load | ls | rm | export | import
  pogi chaos <cmd>         add | ls | rm | clear fault-injection rules
  pogi lambda-sdks [pkg…]  install the AWS SDKs Lambda runtimes include (and the Functions Framework), for native functions
  pogi cert                print (and create) pogi's local CA and how to trust it
  pogi doctor              check the server, Lambda runtimes, SDKs, Docker and the CA
  pogi mcp                 MCP server on stdio for coding agents (--print-config: setup)
  pogi agents init         add a pogi brief for coding agents to AGENTS.md (--file CLAUDE.md)
  pogi version

Run "pogi help <command>" or "pogi <command> -h" for a command's flags, and
"pogi start -h" for server flags. Client commands use --endpoint or
$POGI_ENDPOINT (default http://localhost:7644). status, services, ls, logs,
snapshot ls and chaos ls print JSON with --json.

Docs:   https://pogi.nrnx.dev
Issues: https://github.com/nrndev/pogi/issues
`

// command splits the subcommand off the arguments. With none, or when the first word is a flag,
// the command is start and the flags are its own; --version and -v are the exception.
// "pogi help X" is "pogi X -h".
func command(args []string) (string, []string) {
	if len(args) > 1 && args[0] == "help" {
		return args[1], []string{"-h"}
	}
	if len(args) > 0 && (!strings.HasPrefix(args[0], "-") || args[0] == "--version" || args[0] == "-v") {
		return args[0], args[1:]
	}
	return "start", args
}

func main() {
	cmd, args := command(os.Args[1:])
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
	case "cost":
		err = costCmd(args)
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
	case "pull":
		err = pull(args, os.Stdout)
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
		switch {
		case len(args) > 0 && isHelp(args[0]):
			fmt.Println("usage: pogi version   print pogi's version (also --version or -v)")
		case len(args) > 0:
			err = usagef("version: unexpected argument %q", args[0])
		default:
			fmt.Println("pogi", pogi.Version)
		}
	case "help", "-h", "--help":
		fmt.Print(usage)
	default:
		err = usagef("unknown command %q\n\n%s", cmd, usage)
	}
	if err != nil {
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			os.Exit(exit.ExitCode())
		}
		if errors.Is(err, flag.ErrHelp) {
			return // the command printed its usage; asking for help is not a failure
		}
		fmt.Fprintln(os.Stderr, "pogi:", err)
		if errors.As(err, new(usageError)) {
			os.Exit(2)
		}
		os.Exit(1)
	}
}

// usageError is a mistake in how a command was called: a bad flag, a word it does not take, a
// missing argument. main exits 2 for it and 1 for every other error, as the flag package does.
type usageError struct{ error }

func usagef(format string, a ...any) error { return usageError{fmt.Errorf(format, a...)} }

func getenv(keys ...string) string {
	for _, k := range keys {
		if v := os.Getenv(k); v != "" {
			return v
		}
	}
	return ""
}

// setUsage gives fs the help that -h prints (to stdout, see parseArgs): what the command does, how
// to call it, an example, then its flags. use and example may have several lines.
func setUsage(fs *flag.FlagSet, summary, use, example string) {
	fs.Usage = func() {
		indent := func(s string) string { return "  " + strings.ReplaceAll(s, "\n", "\n  ") }
		fmt.Fprintf(fs.Output(), "%s\n\nUsage:\n%s\n\nExample:\n%s\n", summary, indent(use), indent(example))
		printFlags(fs.Output(), fs)
	}
}

// printFlags lists fs's flags as the docs spell them, --name (or -n for one letter), each with its
// value's type, what it does and its default. The flag package's own list writes -name.
func printFlags(w io.Writer, fs *flag.FlagSet) {
	fmt.Fprintln(w, "\nFlags:")
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fs.VisitAll(func(f *flag.Flag) {
		name := "--" + f.Name
		if len(f.Name) == 1 {
			name = "-" + f.Name
		}
		typ, text := flag.UnquoteUsage(f)
		switch f.DefValue {
		case "", "false", "0", "0s":
		default:
			if typ == "string" {
				text += fmt.Sprintf(" (default %q)", f.DefValue)
			} else {
				text += " (default " + f.DefValue + ")"
			}
		}
		fmt.Fprintf(tw, "  %s\t%s\n", strings.TrimSpace(name+" "+typ), text)
	})
	tw.Flush()
}

// isHelp reports whether arg asks for help in place of a subcommand.
func isHelp(arg string) bool {
	return arg == "-h" || arg == "-help" || arg == "--help" || arg == "help"
}

// parseMax reads flags anywhere among the arguments, so -h works at the end too (see parseArgs), and
// fails when more than max arguments are left, so a word the command does not take is not ignored.
// fs must be flag.ContinueOnError: parseArgs silences the flag package and returns its errors.
func parseMax(fs *flag.FlagSet, args []string, max int) ([]string, error) {
	pos, err := parseArgs(fs, args)
	if err == nil && len(pos) > max {
		err = usagef("%s: unexpected argument %q", fs.Name(), pos[max])
	}
	return pos, err
}

// at is pos[i], or "" when there are fewer arguments.
func at(pos []string, i int) string {
	if i < len(pos) {
		return pos[i]
	}
	return ""
}

// isTerminal reports whether f is an interactive terminal. A character device is not enough:
// /dev/null is one too.
func isTerminal(f *os.File) bool { return term.IsTerminal(f.Fd()) }

// styled reports whether f may get escape codes (bold text, a redrawn progress line): it is a
// terminal, and neither NO_COLOR (https://no-color.org) nor TERM=dumb asks for plain text.
func styled(f *os.File) bool {
	return isTerminal(f) && os.Getenv("NO_COLOR") == "" && os.Getenv("TERM") != "dumb"
}

// printJSON writes v to stdout as indented JSON, for --json.
func printJSON(v any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

// ---- server ----

func start(args []string) error {
	fs := flag.NewFlagSet("start", flag.ContinueOnError)
	setUsage(fs, "Start the emulator: the TUI in a terminal, logs and requests otherwise.",
		"pogi [start] [flags]", "pogi start --services s3,sqs,dynamodb --persist")
	apply := serverFlags(fs)
	file := fs.String("config", "", "pogi.yaml to load (default $POGI_CONFIG, else ./pogi.yaml or ./pogi.yml)")
	headless := fs.Bool("headless", false, "no TUI: print logs and requests")
	quiet := fs.Bool("quiet", false, "headless: do not print each request")
	fs.BoolVar(quiet, "q", false, "same as --quiet")
	debug := fs.Bool("debug", false, "verbose logging")
	if err := applyEnvDefaults(fs); err != nil {
		return err
	}
	if _, err := parseMax(fs, args, 0); err != nil {
		return err
	}

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
	// Stopping can take seconds (containers, open requests). Say so at once, and let a second Ctrl-C
	// quit: stop() hands the signal back to Go, whose default ends pogi. pogi clean and the orphan
	// sweep at the next start handle any container left behind.
	stopping := func() {
		stop()
		fmt.Fprintln(os.Stderr, "Stopping… (press Ctrl-C again to quit now)")
	}
	go func() {
		<-ctx.Done()
		if context.Cause(ctx) != context.Canceled { // a signal, not stop(); errors.Is would match both
			stopping()
		}
	}()

	if useTUI {
		ready := make(chan string, 1)
		done := make(chan error, 1)
		go func() { done <- srv.Run(ctx, func(u string) { ready <- u }) }()
		select {
		case u := <-ready:
			err := tui.Run(u, true)
			stopping()
			<-done
			return err
		case err := <-done:
			return err
		}
	}
	err = srv.Run(ctx, func(u string) {
		st := srv.Hub.Status()
		n := web.ServiceCount(st.Services)
		fmt.Fprintf(os.Stderr, "pogi %s  ·  %d %s  ·  data %s%s\n", pogi.Version, n, plural("service", n), srv.Hub.Cfg.DataDir,
			map[bool]string{true: " (persistent)", false: ""}[st.Persist])
		fmt.Fprintf(os.Stderr, "  endpoint  %s\n  console   %s/_pogi/\n  env       %s\n", u, u, envHint(u))
		if smtp := srv.Ports["ses"]; smtp != "" {
			fmt.Fprintf(os.Stderr, "  smtp      %s\n", strings.TrimPrefix(smtp, "smtp://"))
		}
		if amqp := srv.Ports["amqp"]; amqp != "" {
			fmt.Fprintf(os.Stderr, "  amqp      %s\n", strings.TrimPrefix(amqp, "amqp://"))
		}
		for _, name := range slices.Sorted(maps.Keys(srv.Busy)) {
			fmt.Fprintf(os.Stderr, "  %-9s %s\n", name, srv.Busy[name])
		}
		if cfg.File != nil {
			fmt.Fprintf(os.Stderr, "  config    %s\n", cfg.File.Path)
		}
		fmt.Println("Ready.")
		if !*quiet {
			core.Go("pogi.printRequests", func() { printRequests(ctx, srv.Hub) })
		}
	})
	if errors.Is(err, syscall.EADDRINUSE) {
		return fmt.Errorf("port %d is in use (another pogi?); use --port — %w", cfg.Port, err)
	}
	return err
}

// applyEnvDefaults lets POGI_* and common LocalStack variables set flag defaults. A value the flag
// does not take is an error that names the variable.
func applyEnvDefaults(fs *flag.FlagSet) error {
	var errs []error
	set := func(name string, keys ...string) {
		for _, k := range keys {
			if v := os.Getenv(k); v != "" {
				if fs.Set(name, v) != nil {
					errs = append(errs, fmt.Errorf("%s=%q is not a valid --%s", k, v, name))
				}
				return
			}
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
			if fs.Set("port", p) != nil {
				errs = append(errs, fmt.Errorf("GATEWAY_LISTEN=%q has no valid port", gl))
			}
		}
	}
	return errors.Join(errs...)
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
	return fs.String("endpoint", defaultEndpoint(), "pogi endpoint")
}

// defaultEndpoint is where client commands look for pogi without --endpoint.
func defaultEndpoint() string {
	if ep := getenv("POGI_ENDPOINT"); ep != "" {
		return ep
	}
	return "http://localhost:7644"
}

// envHint is the start banner's command for SDK settings. Plain `pogi env` reaches this pogi only
// when u is its default endpoint (any loopback name counts as the same host).
func envHint(u string) string {
	a, errA := url.Parse(u)
	b, errB := url.Parse(defaultEndpoint())
	if errA == nil && errB == nil && a.Port() == b.Port() &&
		(a.Hostname() == b.Hostname() || core.Loopback(a.Hostname()) && core.Loopback(b.Hostname())) {
		return `eval "$(pogi env)"`
	}
	return `eval "$(pogi env --endpoint ` + u + `)"`
}

// apiClient gives up on a pogi that does not answer, so no command hangs forever. Two minutes leave
// room for a reset or snapshot load that stops containers; logs -f long-polls 25 s at a time.
var apiClient = &http.Client{Timeout: 2 * time.Minute}

func api(endpoint, method, path string, body io.Reader, out any) error {
	req, err := http.NewRequest(method, strings.TrimRight(endpoint, "/")+path, body)
	if err != nil {
		return err
	}
	res, err := apiClient.Do(req)
	if os.IsTimeout(err) {
		return fmt.Errorf("pogi at %s did not answer within %s", endpoint, apiClient.Timeout)
	}
	if err != nil {
		return fmt.Errorf("cannot reach pogi at %s (start it with `pogi`): %v", endpoint, err)
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
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	setUsage(fs, "Show the running pogi's services, with their resource and request counts.", "pogi status [flags]", "pogi status --json")
	ep := endpointFlag(fs)
	asJSON := fs.Bool("json", false, "print the status as JSON")
	if _, err := parseMax(fs, args, 0); err != nil {
		return err
	}
	var st core.Status
	if err := api(*ep, "GET", "/_pogi/api/info", nil, &st); err != nil {
		return err
	}
	if *asJSON {
		return printJSON(st)
	}
	fmt.Printf("pogi %s at %s · up %s · region %s · persistence %v · iam %s\n", st.Version, *ep,
		time.Duration(st.Uptime)*time.Second, st.Region, st.Persist, st.IAM)
	writeProblems(os.Stdout, st.Problems, styled(os.Stdout))
	fmt.Println()
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "CLOUD\tSERVICE\tRESOURCES\tREQUESTS")
	for _, s := range st.Services {
		fmt.Fprintf(tw, "%s\t%s\t%d\t%d\n", s.Cloud, s.Name, s.Resources, s.Requests)
	}
	return tw.Flush()
}

// writeProblems lists what is wrong with pogi as a whole (saved state it can't read or write, a port
// it could not open), one "! " line each, in red when color is true.
func writeProblems(w io.Writer, ps []core.Problem, color bool) {
	for _, p := range ps {
		line := "! " + p.Text
		if color {
			line = "\x1b[31m" + line + "\x1b[0m"
		}
		fmt.Fprintln(w, line)
	}
}

func env(args []string) error {
	fs := flag.NewFlagSet("env", flag.ContinueOnError)
	setUsage(fs, "Print the environment variables that point SDKs and CLIs at pogi.", "pogi env [flags]", `eval "$(pogi env)"`)
	ep := endpointFlag(fs)
	shell := fs.String("shell", "sh", "sh | fish | pwsh | dotenv")
	region := fs.String("region", getenv("AWS_DEFAULT_REGION", "AWS_REGION", "POGI_REGION"), "AWS region")
	hostname := fs.String("hostname", "", "endpoint host name, e.g. aws.localhost (SDKs then use S3 virtual-host style)")
	if _, err := parseMax(fs, args, 0); err != nil {
		return err
	}
	if !slices.Contains([]string{"sh", "bash", "zsh", "fish", "pwsh", "powershell", "dotenv"}, *shell) {
		return usagef("env: --shell %q is not one of sh, bash, zsh, fish, pwsh, dotenv", *shell)
	}
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
	"acm-pca": {"acmpca"}, "amp": {"prometheus"}, "amplify": {"amplify"},
	"apigateway": {"apigateway", "apigatewayv2"}, "appconfig": {"appconfig"}, "appflow": {"appflow"},
	"appintegrations": {"appintegrations"}, "application-autoscaling": {"appautoscaling"},
	"appstream": {"appstream"}, "appsync": {"appsync"}, "athena": {"athena"},
	"autoscaling": {"autoscaling"}, "backup": {"backup"}, "batch": {"batch"},
	"bcm-data-exports": {"bcmdataexports"}, "bedrock": {"bedrock"}, "bedrock-agent": {"bedrockagent"},
	"bedrock-agentcore-control": {"bedrockagentcore"}, "bedrock-runtime": {"bedrockruntime"},
	"budgets": {"budgets"}, "ce": {"ce"}, "cleanrooms": {"cleanrooms"},
	"cloudcontrol": {"cloudcontrolapi"}, "cloudformation": {"cloudformation"},
	"cloudfront": {"cloudfront"}, "cloudhsmv2": {"cloudhsmv2"}, "cloudtrail": {"cloudtrail"},
	"cloudwatch": {"cloudwatch"}, "codeartifact": {"codeartifact"}, "codebuild": {"codebuild"},
	"codecommit": {"codecommit"}, "codedeploy": {"deploy"}, "codepipeline": {"codepipeline"},
	"codestar-connections": {"codestarconnections"}, "cognito-identity": {"cognitoidentity"},
	"cognito-idp": {"cognitoidp"}, "comprehend": {"comprehend"},
	"compute-optimizer": {"computeoptimizer"}, "config": {"configservice"}, "connect": {"connect"},
	"controltower": {"controltower"}, "cost-optimization-hub": {"costoptimizationhub"}, "cur": {"cur"},
	"dataexchange": {"dataexchange"}, "datasync": {"datasync"}, "datazone": {"datazone"},
	"dax": {"dax"}, "detective": {"detective"}, "devicefarm": {"devicefarm"},
	"directconnect": {"directconnect"}, "dlm": {"dlm"}, "dms": {"dms"}, "drs": {"drs"}, "ds": {"ds"},
	"dsql": {"dsql"}, "dynamodb": {"dynamodb"}, "ec2": {"ec2"}, "ecr": {"ecr"}, "ecs": {"ecs"},
	"efs": {"efs"}, "eks": {"eks"}, "elasticache": {"elasticache"},
	"elasticbeanstalk": {"elasticbeanstalk"}, "elb": {"elb"}, "elbv2": {"elbv2"}, "emr": {"emr"},
	"emr-serverless": {"emrserverless"}, "events": {"events"}, "firehose": {"firehose"},
	"fis": {"fis"}, "fms": {"fms"}, "fsx": {"fsx"}, "gamelift": {"gamelift"},
	"globalaccelerator": {"globalaccelerator"}, "glue": {"glue"}, "grafana": {"grafana"},
	"guardduty": {"guardduty"}, "iam": {"iam"}, "identitystore": {"identitystore"},
	"imagebuilder": {"imagebuilder"}, "inspector2": {"inspector2"}, "iot": {"iot"}, "ivs": {"ivs"},
	"kafka": {"kafka"}, "keyspaces": {"keyspaces"}, "kinesis": {"kinesis"},
	"kinesisanalyticsv2": {"kinesisanalyticsv2"}, "kinesisvideo": {"kinesisvideo"}, "kms": {"kms"},
	"lakeformation": {"lakeformation"}, "lambda": {"lambda"}, "license-manager": {"licensemanager"},
	"lightsail": {"lightsail"}, "location": {"location"}, "logs": {"cloudwatchlogs"},
	"macie2": {"macie2"}, "mediaconvert": {"mediaconvert"}, "medialive": {"medialive"},
	"mediapackagev2": {"mediapackagev2"}, "memorydb": {"memorydb"}, "mq": {"mq"}, "mwaa": {"mwaa"},
	"network-firewall": {"networkfirewall"}, "networkmanager": {"networkmanager"},
	"notifications": {"notifications"}, "oam": {"oam"}, "odb": {"odb"},
	"opensearch": {"opensearch", "es"}, "organizations": {"organizations"},
	"payment-cryptography": {"paymentcryptography"}, "pinpoint-sms-voice-v2": {"pinpointsmsvoicev2"},
	"pipes": {"pipes"}, "polly": {"polly"}, "pricing": {"pricing"}, "quicksight": {"quicksight"},
	"ram": {"ram"}, "rds": {"rds", "docdb", "neptune"}, "rds-data": {"rdsdata"},
	"redshift": {"redshift"}, "redshift-data": {"redshiftdata"},
	"redshift-serverless": {"redshiftserverless"}, "rekognition": {"rekognition"},
	"resiliencehub": {"resiliencehub"}, "resource-explorer-2": {"resourceexplorer2"},
	"resource-groups": {"resourcegroups"}, "route53": {"route53"},
	"route53-recovery-control-config": {"route53recoverycontrolconfig"},
	"route53resolver":                 {"route53resolver"}, "rum": {"rum"}, "s3": {"s3", "s3control"},
	"s3tables": {"s3tables"}, "s3vectors": {"s3vectors"}, "sagemaker": {"sagemaker"},
	"scheduler": {"scheduler"}, "secretsmanager": {"secretsmanager"}, "securityhub": {"securityhub"},
	"securitylake": {"securitylake"}, "serverlessrepo": {"serverlessrepo"},
	"service-quotas": {"servicequotas"}, "servicecatalog": {"servicecatalog"},
	"servicediscovery": {"servicediscovery"}, "ses": {"ses", "sesv2"}, "shield": {"shield"},
	"signer": {"signer"}, "sns": {"sns"}, "sqs": {"sqs"}, "ssm": {"ssm"}, "sso": {"sso"},
	"sso-admin": {"ssoadmin"}, "stepfunctions": {"sfn"}, "storagegateway": {"storagegateway"},
	"sts": {"sts"}, "swf": {"swf"}, "timestream-influxdb": {"timestreaminfluxdb"},
	"transcribe": {"transcribe"}, "transfer": {"transfer"},
	"verifiedpermissions": {"verifiedpermissions"}, "vpc-lattice": {"vpclattice"}, "wafv2": {"wafv2"},
	"workspaces": {"workspaces"}, "workspaces-web": {"workspacesweb"}, "xray": {"xray"},
}

// googleEndpoints maps the Terraform google provider's custom endpoint arguments to the base path
// of each API on pogi. The provider appends its own paths to these, and wants the trailing slash.
var googleEndpoints = map[string]string{
	"storage": "/storage/v1/", "pubsub": "/v1/", "secret_manager": "/v1/", "firestore": "/v1/", "cloud_run_v2": "/v2/",
	"cloudfunctions2": "/v2/", "cloud_tasks": "/v2/", "cloud_scheduler": "/v1/", "eventarc": "/v1/", "managed_kafka": "/v1/",
	"iam": "/v1/", "iam_beta": "/v1/", "iam_credentials": "/v1/", "resource_manager": "/v1/", "service_usage": "/v1/", "kms": "/v1/",
	"sql": "/sql/v1beta4/", "big_query": "/bigquery/v2/", "logging": "/v2/", "monitoring": "/",
	"cloud_resource_manager": "/v1/", "resource_manager_v3": "/v3/",
	// the APIs below share their paths with others, so they are served under a prefix (core.googlePrefixed)
	"spanner": "/spanner/v1/", "bigtable": "/bigtableadmin/v2/", "redis": "/redis/v1/", "workflows": "/workflows/v1/",
	"compute": "/compute/v1/",
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
	fs := flag.NewFlagSet("kubeconfig", flag.ContinueOnError)
	setUsage(fs, "Print a kubeconfig for a GKE cluster in pogi, for machines without gke-gcloud-auth-plugin.",
		"pogi kubeconfig gke <cluster> [flags]", "pogi kubeconfig gke my-cluster --location us-central1 > kubeconfig")
	ep := endpointFlag(fs)
	project := fs.String("project", core.DefaultProject, "Google Cloud project")
	location := fs.String("location", "us-central1", "cluster location")
	pos, err := parseMax(fs, args, 2)
	if err != nil {
		return err
	}
	if len(pos) != 2 || pos[0] != "gke" {
		return usagef("usage: pogi kubeconfig gke <cluster> [--project p] [--location l]")
	}
	return api(*ep, "GET", "/_gcp/gke/kubeconfig/"+url.PathEscape(*project)+"/"+url.PathEscape(*location)+"/"+url.PathEscape(pos[1]), nil, os.Stdout)
}

func terraform(args []string) error {
	if len(args) > 0 && args[0] == "diagram" {
		return tfDiagramCmd(os.Stdout, isTerminal(os.Stdout), args[1:])
	}
	fs := flag.NewFlagSet("tf", flag.ContinueOnError)
	setUsage(fs, "Print a Terraform provider override that points the provider at pogi.",
		"pogi tf [flags]\npogi tf diagram [folder | plan.json] [flags]", "pogi tf > pogi_override.tf")
	ep := endpointFlag(fs)
	region := fs.String("region", "", "region (default us-east-1 for aws, us-central1 for gcp)")
	cloud := fs.String("cloud", "aws", "aws, gcp or azure")
	project := fs.String("project", core.DefaultProject, "Google Cloud project")
	metadataHost := fs.String("metadata-host", "", "azurerm's metadata_host: host:port (default host.docker.internal and the endpoint's port)")
	if _, err := parseMax(fs, args, 0); err != nil {
		return err
	}
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
		return usagef("--cloud %q: want aws, gcp or azure", *cloud)
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
	fs := flag.NewFlagSet("logs", flag.ContinueOnError)
	setUsage(fs, "Show recent API calls to pogi, oldest first.", "pogi logs [flags]", "pogi logs -f --service s3 --errors")
	ep := endpointFlag(fs)
	follow := fs.Bool("f", false, "keep printing new calls as they arrive")
	fs.BoolVar(follow, "follow", false, "same as -f")
	service := fs.String("service", "", "only this service")
	errorsOnly := fs.Bool("errors", false, "only failed calls")
	n := fs.Int("n", 50, "how many recent calls to show")
	fs.IntVar(n, "lines", 50, "same as -n")
	asJSON := fs.Bool("json", false, "print each call as one line of JSON")
	if _, err := parseMax(fs, args, 0); err != nil {
		return err
	}
	if *n < 1 {
		return usagef("logs: -n %d is not a positive number", *n)
	}
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
			if *asJSON {
				b, _ := json.Marshal(e)
				fmt.Println(string(b))
			} else {
				fmt.Println(formatEntry(e))
			}
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
	fs := flag.NewFlagSet("ls", flag.ContinueOnError)
	setUsage(fs, "List the resources of one service, or of all.", "pogi ls [service] [flags]", "pogi ls s3")
	ep := endpointFlag(fs)
	asJSON := fs.Bool("json", false, "print the resources as JSON")
	pos, err := parseMax(fs, args, 1)
	if err != nil {
		return err
	}
	path := "/_pogi/api/resources"
	if len(pos) > 0 {
		path += "?service=" + url.QueryEscape(pos[0])
	}
	var res map[string][]core.Table
	if err := api(*ep, "GET", path, nil, &res); err != nil {
		return err
	}
	if *asJSON {
		return printJSON(res)
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
	fs := flag.NewFlagSet("reset", flag.ContinueOnError)
	setUsage(fs, "Delete the state of the services named, or of every service when none is.", "pogi reset [service…] [flags]", "pogi reset s3 sqs")
	ep := endpointFlag(fs)
	names, err := parseArgs(fs, args) // any number of services
	if err != nil {
		return err
	}
	path := "/_pogi/api/reset"
	if len(names) > 0 {
		path += "?service=" + url.QueryEscape(strings.Join(names, ","))
	}
	if err := api(*ep, "POST", path, nil, nil); err != nil {
		return err
	}
	fmt.Println("reset", map[bool]string{true: "all services", false: strings.Join(names, ", ")}[len(names) == 0])
	return nil
}

func snapshot(args []string) error {
	fs := flag.NewFlagSet("snapshot", flag.ContinueOnError)
	setUsage(fs, "Save, load, list and share snapshots of pogi's state. A FILE of - is stdout for export and stdin\nfor import (import - then needs a NAME).",
		"pogi snapshot save [NAME] | load NAME | ls | rm NAME | export NAME [FILE] | import FILE [NAME]",
		"pogi snapshot save clean && pogi snapshot load clean")
	ep := endpointFlag(fs)
	asJSON := fs.Bool("json", false, "ls: print the snapshots as JSON")
	if len(args) > 0 && isHelp(args[0]) {
		args = []string{"-h"} // "pogi snapshot help"; parseArgs prints the help
	}
	pos, err := parseArgs(fs, args) // flags may come before or after the subcommand
	if err != nil {
		return err
	}
	sub := "ls"
	if len(pos) > 0 {
		sub, pos = pos[0], pos[1:]
	}
	// The most arguments each subcommand takes; an unknown one fails in the switch below.
	if n, ok := map[string]int{"ls": 0, "list": 0, "save": 1, "load": 1, "restore": 1, "rm": 1, "delete": 1, "export": 2, "import": 2}[sub]; ok && len(pos) > n {
		return usagef("snapshot %s: unexpected argument %q", sub, pos[n])
	}
	if *asJSON && sub != "ls" && sub != "list" {
		return usagef("snapshot %s: --json only works with snapshot ls", sub)
	}
	name := at(pos, 0)
	need := func() error {
		if name == "" {
			return usagef("usage: pogi snapshot %s NAME", sub)
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
		if *asJSON {
			return printJSON(snaps)
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
		if err := api(*ep, "DELETE", p, nil, nil); err != nil {
			return err
		}
		fmt.Println("removed snapshot", name)
	case "export":
		if err := need(); err != nil {
			return err
		}
		return exportSnapshot(*ep, p, at(pos, 1))
	case "import":
		file := at(pos, 0)
		if file == "" {
			return usagef("usage: pogi snapshot import FILE [NAME]")
		}
		name = at(pos, 1)
		if name == "" && file == "-" {
			return usagef("usage: pogi snapshot import - NAME (stdin has no file name to take the name from)")
		}
		if name == "" {
			name = strings.TrimSuffix(filepath.Base(file), filepath.Ext(file))
		}
		var b []byte
		var err error
		if file == "-" {
			b, err = io.ReadAll(os.Stdin)
		} else {
			b, err = os.ReadFile(file)
		}
		if err != nil {
			return err
		}
		if err := api(*ep, "PUT", "/_pogi/api/snapshots/"+url.PathEscape(name), bytes.NewReader(b), nil); err != nil {
			return err
		}
		fmt.Println("imported", name, "(load it with: pogi snapshot load", name+")")
	default:
		return usagef("unknown snapshot command %q (save | load | ls | rm | export | import)", sub)
	}
	return nil
}

// exportSnapshot writes the snapshot at API path p to file, or to stdout when file is "" or "-". A file
// is written beside its final name and renamed only when the whole snapshot arrived, so a failed
// export (an unknown name, a dropped connection) leaves no file behind.
func exportSnapshot(ep, p, file string) error {
	if file == "" || file == "-" {
		return api(ep, "GET", p, nil, os.Stdout)
	}
	f, err := os.CreateTemp(filepath.Dir(file), ".pogi-export-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name()) // does nothing once renamed
	err = api(ep, "GET", p, nil, f)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}
	return os.Rename(f.Name(), file)
}

func chaos(args []string) error {
	fs := flag.NewFlagSet("chaos", flag.ContinueOnError)
	setUsage(fs, "Add, list and remove fault-injection rules: errors and latency on matching calls.",
		"pogi chaos add --status n | --latency d [flags]\npogi chaos ls | rm ID | clear",
		"pogi chaos add --service s3 --op 'Put*' --status 503 --rate 0.2")
	ep := endpointFlag(fs)
	asJSON := fs.Bool("json", false, "ls: print the rules as JSON")
	service := fs.String("service", "", "service glob (default: any)")
	op := fs.String("op", "", "operation glob, e.g. Put* (default: any)")
	rate := fs.Float64("rate", 1, "probability 0..1")
	latency := fs.Duration("latency", 0, "added latency, e.g. 300ms")
	status := fs.Int("status", 0, "HTTP status to inject, e.g. 500")
	code := fs.String("code", "", "error code, e.g. ThrottlingException")
	msg := fs.String("message", "", "error message")
	if len(args) > 0 && isHelp(args[0]) {
		args = []string{"-h"} // "pogi chaos help"; parseArgs prints the help
	}
	pos, err := parseArgs(fs, args) // flags may come before or after the subcommand
	if err != nil {
		return err
	}
	sub := "ls"
	if len(pos) > 0 {
		sub, pos = pos[0], pos[1:]
	}
	// The most arguments each subcommand takes; an unknown one fails in the switch below.
	if n, ok := map[string]int{"ls": 0, "list": 0, "add": 0, "rm": 1, "remove": 1, "clear": 0}[sub]; ok && len(pos) > n {
		return usagef("chaos %s: unexpected argument %q", sub, pos[n])
	}
	// The rule flags mean something to add only, and --json to ls only; elsewhere they would be ignored.
	if sub != "add" {
		var ruleFlags []string
		fs.Visit(func(f *flag.Flag) {
			if f.Name != "endpoint" && f.Name != "json" {
				ruleFlags = append(ruleFlags, "--"+f.Name)
			}
		})
		if len(ruleFlags) > 0 {
			return usagef("chaos %s: %s only work with chaos add", sub, strings.Join(ruleFlags, ", "))
		}
	}
	if *asJSON && sub != "ls" && sub != "list" {
		return usagef("chaos %s: --json only works with chaos ls", sub)
	}
	switch sub {
	case "ls", "list":
		var rules []core.Rule
		if err := api(*ep, "GET", "/_pogi/api/chaos", nil, &rules); err != nil {
			return err
		}
		if *asJSON {
			return printJSON(rules)
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
			return usagef("chaos add: give --status and/or --latency")
		}
		b, _ := json.Marshal(core.Rule{Service: *service, Op: *op, Probability: *rate, LatencyMs: int(latency.Milliseconds()), Status: *status, Code: *code, Message: *msg})
		var r core.Rule
		if err := api(*ep, "POST", "/_pogi/api/chaos", bytes.NewReader(b), &r); err != nil {
			return err
		}
		fmt.Println("added rule", r.ID)
	case "rm", "remove":
		if len(pos) == 0 {
			return usagef("usage: pogi chaos rm ID")
		}
		if err := api(*ep, "DELETE", "/_pogi/api/chaos/"+url.PathEscape(pos[0]), nil, nil); err != nil {
			return err
		}
		fmt.Println("removed rule", pos[0])
	case "clear":
		if err := api(*ep, "DELETE", "/_pogi/api/chaos", nil, nil); err != nil {
			return err
		}
		fmt.Println("removed all chaos rules")
	default:
		return usagef("unknown chaos command %q (add | ls | rm | clear)", sub)
	}
	return nil
}

func attach(args []string) error {
	fs := flag.NewFlagSet("tui", flag.ContinueOnError)
	setUsage(fs, "Attach the terminal screen to a pogi that is already running.", "pogi tui [flags]", "pogi tui --endpoint http://localhost:7644")
	ep := endpointFlag(fs)
	if _, err := parseMax(fs, args, 0); err != nil {
		return err
	}
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
	fs := flag.NewFlagSet("lambda-sdks", flag.ContinueOnError)
	dataDir := fs.String("data-dir", "", "pogi's data dir (default ~/.pogi)")
	setUsage(fs, "Install the AWS SDKs that Lambda runtimes include (and the Functions Framework), for native functions.\nExtra packages starting with @ go to npm, others to pip.",
		"pogi lambda-sdks [package…] [flags]", "pogi lambda-sdks @aws-sdk/client-bedrock-runtime requests")
	pkgs, err := parseArgs(fs, args) // any number of extra packages
	if err != nil {
		return err
	}
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
	for _, p := range pkgs {
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
	fs := flag.NewFlagSet("cert", flag.ContinueOnError)
	setUsage(fs, "Print (and create) pogi's local certificate authority, and how to trust it.", "pogi cert [flags]", "pogi cert")
	dataDir := fs.String("data-dir", "", "pogi's data dir (default ~/.pogi)")
	if _, err := parseMax(fs, args, 0); err != nil {
		return err
	}
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
	fs := flag.NewFlagSet("doctor", flag.ContinueOnError)
	setUsage(fs, "Check the server, Lambda runtimes, SDKs, Docker and the local CA, and say how to fix what fails.", "pogi doctor [flags]", "pogi doctor")
	ep := endpointFlag(fs)
	dataDir := fs.String("data-dir", "", "pogi's data dir (default ~/.pogi)")
	if _, err := parseMax(fs, args, 0); err != nil {
		return err
	}
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
