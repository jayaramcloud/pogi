package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// mcpCmd serves MCP over stdio by relaying to the running pogi's /_pogi/mcp.
func mcpCmd(args []string) error {
	fs := flag.NewFlagSet("mcp", flag.ExitOnError)
	ep := endpointFlag(fs)
	printConfig := fs.Bool("print-config", false, "print the setup for Claude Code, Cursor and Codex")
	fs.Parse(args)
	if *printConfig {
		printMCPConfig(os.Stdout, *ep)
		return nil
	}
	if isTerminal(os.Stdin) {
		fmt.Fprintln(os.Stderr, "pogi mcp speaks MCP on stdin/stdout for coding agents; `pogi mcp --print-config` shows how to add it.")
	}
	return bridge(context.Background(), *ep, os.Stdin, os.Stdout)
}

// bridge relays JSON-RPC between an agent on in/out and endpoint's MCP server
// using the SDK's transports. Calls run concurrently, so a slow deploy_stack
// does not hold up the rest; a call pogi can't take gets an error answer.
func bridge(ctx context.Context, endpoint string, in io.ReadCloser, out io.WriteCloser) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	remote, err := (&mcp.StreamableClientTransport{Endpoint: strings.TrimRight(endpoint, "/") + "/_pogi/mcp", DisableStandaloneSSE: true}).Connect(ctx)
	if err != nil {
		return err
	}
	defer remote.Close()
	local, err := (&mcp.IOTransport{Reader: in, Writer: out}).Connect(ctx)
	if err != nil {
		return err
	}
	defer local.Close()
	done := make(chan error, 2)
	go func() {
		for {
			msg, err := remote.Read(ctx)
			if err == nil {
				err = local.Write(ctx, msg)
			}
			if err != nil {
				done <- err
				return
			}
		}
	}()
	go func() {
		for {
			msg, err := local.Read(ctx)
			if err != nil {
				done <- err
				return
			}
			go func() {
				err := remote.Write(ctx, msg)
				if req, ok := msg.(*jsonrpc.Request); ok && err != nil && req.IsCall() {
					if errors.As(err, new(*url.Error)) {
						err = fmt.Errorf("pogi is not running at %s: start it with `pogi start`, or point --endpoint or POGI_ENDPOINT at it (%w)", endpoint, err)
					}
					local.Write(ctx, &jsonrpc.Response{ID: req.ID, Error: &jsonrpc.Error{Code: jsonrpc.CodeInternalError, Message: err.Error()}})
				}
			}()
		}
	}()
	if err := <-done; !errors.Is(err, io.EOF) {
		return err
	}
	return nil
}

func printMCPConfig(w io.Writer, ep string) {
	u := strings.TrimRight(ep, "/") + "/_pogi/mcp"
	cmd, args := "pogi mcp", `"mcp"`
	if ep != "http://localhost:7644" {
		cmd += " --endpoint " + ep
		args += `, "--endpoint", "` + ep + `"`
	}
	fmt.Fprintf(w, `# Claude Code: stdio, through pogi mcp
claude mcp add pogi -- %s
# or straight over HTTP
claude mcp add --transport http pogi %s

# Cursor: .cursor/mcp.json
{
  "mcpServers": {
    "pogi": { "url": "%s" }
  }
}

# Codex: ~/.codex/config.toml
[mcp_servers.pogi]
command = "pogi"
args = [%s]

# pogi must be running (pogi start). "pogi agents init" adds a brief on using it to AGENTS.md.
`, cmd, u, u, args)
}

const (
	agentsBegin = "<!-- pogi:begin -->"
	agentsEnd   = "<!-- pogi:end -->"
)

// agentsCmd is `pogi agents init`: pogi's brief for coding agents, in a marked
// section of AGENTS.md or CLAUDE.md.
func agentsCmd(args []string) error {
	if len(args) == 0 || args[0] != "init" {
		return errors.New("usage: pogi agents init [--file AGENTS.md|CLAUDE.md] [--force]")
	}
	fs := flag.NewFlagSet("agents init", flag.ExitOnError)
	ep := endpointFlag(fs)
	file := fs.String("file", "AGENTS.md", "file to write the pogi section into (created if missing)")
	force := fs.Bool("force", false, "replace a pogi section that differs, e.g. one edited by hand")
	fs.Parse(args[1:])
	msg, err := agentsInit(*file, agentsBrief(*ep), *force)
	if err == nil {
		fmt.Println(msg)
	}
	return err
}

// agentsInit puts section into file: appended when the file has none,
// replaced when it differs and force is set.
func agentsInit(file, section string, force bool) (string, error) {
	b, err := os.ReadFile(file)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	old := string(b)
	i, j := strings.Index(old, agentsBegin), strings.Index(old, agentsEnd)
	var out string
	switch {
	case i < 0:
		if old != "" {
			old = strings.TrimRight(old, "\n") + "\n\n"
		}
		out = old + section + "\n"
	case j < i:
		return "", fmt.Errorf("%s has %s without %s after it; fix it by hand", file, agentsBegin, agentsEnd)
	case old[i:j+len(agentsEnd)] == section:
		return file + " is up to date", nil
	case !force:
		return "", fmt.Errorf("%s has a pogi section that differs from this version's; --force replaces it", file)
	default:
		out = old[:i] + section + old[j+len(agentsEnd):]
	}
	return "wrote the pogi section to " + file, os.WriteFile(file, []byte(out), 0o644)
}

// agentsBody is the brief between the markers; ' stands for a backtick, {ep} for the endpoint.
const agentsBody = `## Local cloud: pogi

This project develops against pogi, a local emulator of AWS, Google Cloud and Azure on one endpoint,
{ep} (Azure Storage also uses ports 10000-10002). State is local and disposable; never use real
cloud credentials here.

- **Start and connect:** 'pogi start' ('--headless' without a terminal), then 'eval "$(pogi env)"'
  points the AWS, Google Cloud and Azure SDKs and CLIs at pogi. 'pogi aws …' runs the AWS CLI,
  'pogi tf' prints a Terraform provider block, and CDK works with the env set.
- **MCP tools** (server 'pogi' at {ep}/_pogi/mcp, or 'pogi mcp' over stdio). Make cloud calls with
  the SDK or CLI, and use the tools to look inside pogi and control it:
  - a call failed: 'request_log' with 'errors_only', then 'get_request' for headers and bodies;
    'iam_decisions' when access is denied;
  - state and messages: 'status', 'list_resources', 'inspect_resource', 'peek_queue' (SQS, without
    consuming), 'inbox' (SES email, SNS SMS and email), 'connection_info';
  - control: 'reset', 'snapshot_save' / 'snapshot_load' / 'snapshot_list', 'chaos_add' / 'chaos_list' /
    'chaos_clear' (inject errors and latency), 'deploy_stack' / 'stack_status' / 'delete_stack'
    (CloudFormation), 'invoke_function' / 'function_logs' (Lambda), 'seed' (pogi.yaml resources).
- **Reset state:** 'pogi reset [service…]' or the 'reset' tool; 'pogi snapshot save NAME' and
  'pogi snapshot load NAME' keep known-good states.
- **Request log:** 'pogi logs -f' ('--errors' for failures only), or the 'request_log' tool.
- **Console:** {ep}/_pogi/ (requests, resources, inbox, chaos, snapshots).`

func agentsBrief(ep string) string {
	return agentsBegin + "\n" + strings.NewReplacer("'", "`", "{ep}", strings.TrimRight(ep, "/")).Replace(agentsBody) + "\n" + agentsEnd
}
