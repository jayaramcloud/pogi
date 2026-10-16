---
title: CLI
description: Every pogi command and flag.
---

# CLI

`pogi` with no command starts the server. Every other command talks to a running pogi, or helps you set one up.

Client commands (`status`, `ls`, `logs` and so on) find pogi at `http://localhost:7644`. Point them elsewhere with `--endpoint`, or with the `POGI_ENDPOINT` environment variable.

| Command | Purpose |
|---|---|
| [`pogi start`](#pogi-start-run-the-server) | Run the emulator. |
| [`pogi status`](#pogi-status-show-status), [`pogi services`](#pogi-services-list-services), [`pogi wait`](#pogi-wait-wait-until-pogi-is-ready) | Look inside a running pogi. |
| [`pogi ls`](#pogi-ls-list-resources), [`pogi logs`](#pogi-logs-show-recent-calls) | List resources, show calls. |
| [`pogi env`](#pogi-env-print-sdk-environment-variables), [`pogi aws`](#pogi-aws-run-the-aws-cli-against-pogi) | Connect your tools. |
| [`pogi tf`](#pogi-tf-print-a-terraform-provider-block), [`pogi kubeconfig gke`](#pogi-kubeconfig-gke-print-a-gke-kubeconfig) | Connect Terraform and Kubernetes tools. |
| [`pogi reset`](#pogi-reset-reset-state) | Delete state. |
| [`pogi clean`](#pogi-clean-delete-leftover-containers-and-volumes) | Delete containers and volumes a stopped pogi left behind. |
| [`pogi snapshot`](#pogi-snapshot-save-and-load-snapshots) | Save, load and share state. |
| [`pogi chaos`](#pogi-chaos-inject-faults) | Add fault-injection rules. |
| [`pogi config`](#pogi-config-work-with-pogiyaml) | Write, check and print `pogi.yaml`. |
| [`pogi doctor`](#pogi-doctor-check-your-setup) | Check the server and its tools. |
| [`pogi lambda-sdks`](#pogi-lambda-sdks-install-the-sdks-lambda-needs) | Install the AWS SDKs for native Lambda. |
| [`pogi cert`](#pogi-cert-show-the-local-certificate-authority) | Show the local certificate authority. |
| [`pogi mcp`](#pogi-mcp-run-the-mcp-server-for-ai-agents), [`pogi agents init`](#pogi-agents-init-add-a-brief-to-agentsmd) | Set up AI agents. |
| [`pogi tui`](#pogi-tui-open-the-terminal-screen) | Attach the terminal screen. |
| `pogi version` | Print the version. |

Flags go after the command name: `pogi chaos add --status 500`. You can write `-status` or `--status`.

Some commands have a second name: `up` and `run` for `start`, `terraform` for `tf`, `resources` for `ls`, and `snapshots` and `snap` for `snapshot`.

`pogi --help` and `pogi -h` show the server flags, because a leading flag means `start`.

## pogi start: run the server

`pogi start [flags]`

Runs the emulator. In a terminal it shows the TUI (the live terminal screen). With `--headless` it prints logs, and with `--quiet` it leaves out the line for each request.

The flags, their environment variables and defaults are in [Start flags](configuration.md#start-flags). Flags win over environment variables, and environment variables win over `pogi.yaml`.

```bash
pogi start --headless --port 7644
```

```text title="Output"
pogi 0.1.0  ·  181 services  ·  data /home/me/.pogi
  endpoint  http://127.0.0.1:7644
  console   http://127.0.0.1:7644/_pogi/
  env       eval "$(pogi env)"
Ready.
```

Press Ctrl-C to stop. pogi then prints what it stopped, where it saved state and how long it took:

```text title="Output"
pogi stopped:
  rds      2 containers stopped   orders-db, users-db
  lambda   3 processes stopped    resize, thumbs, notify
  ses      port 2525 closed       SMTP
  state saved to /home/me/.pogi
  done in 3.1s
```

With saved state (`--persist` or a `--storage` mode), containers are stopped, not removed, and the next start runs them again, so the data inside survives. Without saved state they are removed. A list of more than five names ends with `+N more`.

## pogi status: show status

Shows the version, uptime, region, whether state is saved to disk, the IAM (who may do what) mode, and every service with its resource and request counts.

| Flag | Type | Default | Meaning |
|---|---|---|---|
| `--endpoint` | string | `http://localhost:7644` | Where pogi runs. |

```bash
pogi status
```

```text title="Output"
pogi 0.1.0 at http://localhost:7644 · up 7s · region us-east-1 · persistence false · iam off

CLOUD  SERVICE                    RESOURCES  REQUESTS
aws    s3                         0          0
aws    sqs                        0          0
…
```

## pogi services: list services

Lists the services with their titles. If pogi is not running, it lists the services the current config enables. Takes `--endpoint`.

```text title="Output"
CLOUD  SERVICE                    TITLE                                               RESOURCES  REQUESTS
aws    s3                         S3                                                  0          6
aws    sqs                        SQS                                                 0          4
…
```

## pogi wait: wait until pogi is ready

Returns when pogi is ready. It fails with status 1 when the time is up. Use it in CI.

| Flag | Type | Default | Meaning |
|---|---|---|---|
| `--timeout` | duration | `1m0s` | Give up after this long. |
| `--endpoint` | string | `http://localhost:7644` | Where pogi runs. |

```bash
pogi wait --timeout 60s
```

```text title="Output"
ready: http://localhost:7644
```

## pogi ls: list resources

`pogi ls [service]`

Lists the resources of every service, or of one. Takes `--endpoint`.

```bash
pogi ls s3
```

```text title="Output"
s3 · Buckets (1)
  NAME  REGION     ACCOUNT       OBJECTS  SIZE  VERSIONING  CREATED
  demo  us-east-1  000000000000  0        0 B               2026-10-13 15:34:25
```

## pogi logs: show recent calls

`pogi logs [flags]`

Shows recent API calls.

| Flag | Type | Default | Meaning |
|---|---|---|---|
| `-f` | bool | off | Follow new calls. |
| `-n` | number | 50 | How many recent calls to show. |
| `--errors` | bool | off | Only failed calls. |
| `--service` | string | any | Only this service. |
| `--endpoint` | string | `http://localhost:7644` | Where pogi runs. |

```bash
pogi logs --errors -n 2
```

```text title="Output"
21:04:49.035  aws   sqs        ListQueues                       403     0.2ms  AccessDeniedException
```

## pogi env: print SDK environment variables

Prints the environment variables that point SDKs and CLIs at pogi. See [Connect your tools](../how-to/connect-your-tools.md).

| Flag | Type | Default | Meaning |
|---|---|---|---|
| `--shell` | string | `sh` | `sh`, `fish`, `pwsh` (or `powershell`) or `dotenv`. |
| `--hostname` | string | none | Endpoint host name, such as `aws.localhost`. SDKs then use S3 virtual-host style. |
| `--region` | string | `us-east-1` | AWS region. |
| `--endpoint` | string | `http://localhost:7644` | Where pogi runs. |

```bash
pogi env
```

```text title="Output"
export AWS_ENDPOINT_URL="http://localhost:7644"
export AWS_ACCESS_KEY_ID="test"
export AWS_SECRET_ACCESS_KEY="test"
export AWS_DEFAULT_REGION="us-east-1"
…
```

To load the settings into your shell, run `eval "$(pogi env)"`.

## pogi aws: run the AWS CLI against pogi

`pogi aws <args>`

Runs the AWS CLI against pogi, so one-off commands need no environment variables. It passes every argument to `aws`, reads `POGI_ENDPOINT` for the address and needs the AWS CLI installed.

```bash
pogi aws s3 mb s3://demo
```

```text title="Output"
make_bucket: demo
```

## pogi tf: print a Terraform provider block

Prints a Terraform provider block for AWS, Google Cloud or Azure.

| Flag | Type | Default | Meaning |
|---|---|---|---|
| `--cloud` | string | `aws` | `aws`, `gcp` or `azure`. |
| `--region` | string | `us-east-1` | Region. The default is `us-central1` with `--cloud gcp`. |
| `--project` | string | `pogi` | Google Cloud project. |
| `--metadata-host` | string | `host.docker.internal` | The `metadata_host` of `azurerm`, as `host:port`. The port is the endpoint's port. |
| `--endpoint` | string | `http://localhost:7644` | Where pogi runs. |

```bash
pogi tf > pogi_override.tf
```

```hcl title="pogi_override.tf"
# Generated by "pogi tf": point the AWS provider at pogi.
provider "aws" {
  region                      = "us-east-1"
  access_key                  = "test"
  secret_key                  = "test"
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = false
  s3_use_path_style           = true

  endpoints {
    accessanalyzer   = "http://localhost:7644"
    …
```

## pogi kubeconfig gke: print a GKE kubeconfig

`pogi kubeconfig gke <cluster>`

Prints a kubeconfig for a Google Kubernetes Engine cluster that pogi runs. With no such cluster, it stops with `404 Not Found: no cluster projects/pogi/locations/us-central1/clusters/c1`.

| Flag | Type | Default | Meaning |
|---|---|---|---|
| `--project` | string | `pogi` | Google Cloud project. |
| `--location` | string | `us-central1` | Cluster location. |
| `--endpoint` | string | `http://localhost:7644` | Where pogi runs. |

## pogi reset: reset state

`pogi reset [service…]`

Deletes state: everything with no arguments, or only the services you name. Takes `--endpoint`.

```bash
pogi reset s3 sqs
```

```text title="Output"
reset s3, sqs
```

With no arguments it prints `reset all services`.

## pogi clean: delete leftover containers and volumes

`pogi clean [flags]`

Deletes the Docker containers and volumes that a pogi left behind and that no running pogi owns. A pogi leaves them when it is killed, or when it stops with saved state (those containers are stopped, ready for the next start). `pogi clean` talks to Docker directly, so it works when pogi is not running.

Stopped containers of saved state are only ever deleted here: a pogi that starts without saved state leaves them alone. It only looks at things with pogi's own labels. It never touches a running pogi's containers, other containers on your machine, the `pogi` network or images.

It lists what it will delete, grouped by service, and asks `Delete these? [y/N]`. Then, if the data folder holds saved state, it asks `Also delete saved state in <dir>? [y/N]`. The default for both is no. pogi refuses to delete state while a pogi runs on that data folder.

| Flag | Type | Default | Meaning |
|---|---|---|---|
| `--yes` | bool | off | Delete the containers and volumes without asking. It does not delete saved state. |
| `--dry-run` | bool | off | Only list what would be deleted. |
| `--state` | bool | off | Also delete the saved state (`state/` in the data folder), without asking. |
| `--data-dir` | string | what `pogi start` uses | The data folder. |

```bash
pogi clean --dry-run
```

```text title="Output"
Left by a pogi that is no longer running:
  rds   1 container   pogi-rds-orders-db
Saved state in /home/me/.pogi would be asked about.
```

When nothing was left, it says so and exits with 0.

## pogi snapshot: save and load snapshots

`pogi snapshot <command>`

Keeps named copies of the state. With no command it lists them. All commands take `--endpoint`. See [Save and load snapshots](../how-to/save-and-load-snapshots.md).

| Command | What it does |
|---|---|
| `pogi snapshot ls` (or `list`) | Lists snapshots with their size and save time. |
| `pogi snapshot save [NAME]` | Saves all state. Without a name, the date and time is the name. |
| `pogi snapshot load NAME` (or `restore`) | Replaces the current state with the snapshot. |
| `pogi snapshot rm NAME` (or `delete`) | Deletes a snapshot. |
| `pogi snapshot export NAME [FILE]` | Writes the snapshot to a file, or to standard output without a file name. |
| `pogi snapshot import FILE [NAME]` | Stores a file as a snapshot. The name defaults to the file name without its extension. |

```bash
pogi snapshot save demo
pogi snapshot ls
```

```text title="Output"
saved demo
NAME  SIZE   SAVED
demo  13173  2026-10-13 23:42:58
```

## pogi chaos: inject faults

`pogi chaos <command>`

Manages fault-injection rules. With no command it lists them. See [Inject chaos](../how-to/inject-chaos.md).

| Command | What it does |
|---|---|
| `pogi chaos ls` (or `list`) | Lists rules with their hit counts. |
| `pogi chaos add` | Adds a rule. Needs `--status`, `--latency` or both. |
| `pogi chaos rm ID` (or `remove`) | Removes one rule. |
| `pogi chaos clear` | Removes all rules. |

Flags of `pogi chaos add`:

| Flag | Type | Default | Meaning |
|---|---|---|---|
| `--service` | string | any | Service to match, as a pattern. |
| `--op` | string | any | Operation to match, as a pattern, such as `Put*`. |
| `--rate` | number | `1` | Chance that a matching call is affected, from 0 to 1. |
| `--status` | number | none | HTTP status to answer with, such as `500`. |
| `--code` | string | none | Error code to return, such as `ThrottlingException`. |
| `--message` | string | none | Error message to return. |
| `--latency` | duration | none | Delay to add, such as `300ms`. |
| `--endpoint` | string | `http://localhost:7644` | Where pogi runs. |

```bash
pogi chaos add --service sqs --op 'Send*' --status 500 --rate 0.5 --code ThrottlingException
pogi chaos ls
```

```text title="Output"
added rule 14b9b29f
ID        SERVICE  OP     RATE  LATENCY  ERROR                    HITS
14b9b29f  sqs      Send*  50%   0ms      500 ThrottlingException  0
```

- `pogi chaos add` with neither `--status` nor `--latency` stops with `give --status and/or --latency`.
- `pogi chaos rm` with an unknown ID stops with `rule not found`.

## pogi config: work with pogi.yaml

`pogi config <command>`

| Command | What it does |
|---|---|
| `pogi config init [--force] [PATH]` | Writes an annotated `pogi.yaml`, in the current folder by default. Refuses to overwrite a file without `--force`. |
| `pogi config validate [PATH]` | Checks the file. Mistakes come with a line number, and the exit status is 1. |
| `pogi config print [PATH]` | Shows the settings after the file, environment variables and defaults are merged. Secrets are masked. |

Without a path, `validate` and `print` look for the file the same way `pogi start` does.

```bash
pogi config init
pogi config validate
```

```text title="Output"
wrote pogi.yaml
ok: pogi.yaml
```

A mistake in the file:

```text title="Output"
bad.yaml:3: field prot not found in type config.Server
```

`pogi config init` stops with `pogi.yaml already exists (--force overwrites it)` when the file is there. See [pogi.yaml](configuration.md#pogiyaml).

## pogi doctor: check your setup

Checks the server, Python and Node.js for Lambda, the AWS SDKs for native Lambda, Docker, whether `*.localhost` names resolve, and the local certificate authority. It prints a line for each check and exits with status 1 if one needs attention.

| Flag | Type | Default | Meaning |
|---|---|---|---|
| `--data-dir` | string | `~/.pogi` | pogi's data folder. |
| `--endpoint` | string | `http://localhost:7644` | Where pogi runs. |

```text title="Output"
  ✓ server at http://localhost:7644 0.1.0
  ✓ python3 for Python Lambdas: Python 3.9.6
  ✓ node for Node.js Lambdas: v25.9.0
  ✗ AWS SDKs for native Lambdas in ~/.pogi/lambda — run `pogi lambda-sdks` (CDK custom resources need them)
  ✓ Docker Desktop 29.4.1 at unix:///var/run/docker.sock (Engine API 1.54)
  ✓ *.localhost names resolve (pogi env --hostname aws.localhost, virtual-host S3, execute-api and lambda-url hosts)
  ✓ local CA ~/.pogi/certs/ca.pem
pogi: 1 check(s) need attention
```

## pogi lambda-sdks: install the SDKs Lambda needs

`pogi lambda-sdks [--data-dir dir] [packages…]`

Installs the AWS SDKs that real Lambda runtimes include (the JavaScript v3 SDK and boto3) where pogi's native runtimes look for them. It also installs the Functions Framework that runs Cloud Functions natively. The CDK's custom resources need this. It needs `npm` or `python3`.

Extra package names can follow. A name that starts with `@` goes to npm. Any other name goes to pip.

| Flag | Type | Default | Meaning |
|---|---|---|---|
| `--data-dir` | string | `~/.pogi` | pogi's data folder. |

## pogi cert: show the local certificate authority

Prints pogi's local certificate authority, creates it if needed, and says how to trust it on your system.

| Flag | Type | Default | Meaning |
|---|---|---|---|
| `--data-dir` | string | `~/.pogi` | pogi's data folder. |

```text title="Output"
pogi's CA: ~/.pogi/certs/ca.pem

It signs the certificate for localhost, *.localhost and host.docker.internal that
port 7644 serves HTTPS with. Trust it once:

  sudo security add-trusted-cert -d -r trustRoot -k /Library/Keychains/System.keychain ~/.pogi/certs/ca.pem
…
```

## pogi mcp: run the MCP server for AI agents

Runs an MCP server on standard input and output, so coding agents can look inside pogi and control it. The same MCP server is on HTTP at `http://localhost:7644/_pogi/mcp`.

There is no cloud-API passthrough. Agents make calls with the usual SDKs and CLIs.

| Flag | Type | Default | Meaning |
|---|---|---|---|
| `--print-config` | bool | off | Print the setup for Claude Code, Cursor and Codex. |
| `--endpoint` | string | `http://localhost:7644` | Where pogi runs. |

The tools: `status`, `connection_info`, `list_resources`, `inspect_resource`, `request_log`, `get_request`, `iam_decisions`, `peek_queue`, `inbox`, `function_logs`, `invoke_function`, `deploy_stack`, `stack_status`, `delete_stack`, `seed`, `reset`, `snapshot_list`, `snapshot_save`, `snapshot_load`, `chaos_list`, `chaos_add` and `chaos_clear`.

```bash
pogi mcp --print-config
```

```text title="Output"
# Claude Code: stdio, through pogi mcp
claude mcp add pogi -- pogi mcp
# or straight over HTTP
claude mcp add --transport http pogi http://localhost:7644/_pogi/mcp

# Cursor: .cursor/mcp.json
{
  "mcpServers": {
    "pogi": { "url": "http://localhost:7644/_pogi/mcp" }
  }
}

# Codex: ~/.codex/config.toml
[mcp_servers.pogi]
command = "pogi"
args = ["mcp"]

# pogi must be running (pogi start). "pogi agents init" adds a brief on using it to AGENTS.md.
```

## pogi agents init: add a brief to AGENTS.md

Adds a short brief about pogi to `AGENTS.md`. The brief sits between `<!-- pogi:begin -->` and `<!-- pogi:end -->` markers, so running the command again updates it.

| Flag | Type | Default | Meaning |
|---|---|---|---|
| `--file` | string | `AGENTS.md` | File to write into. It is created if missing. |
| `--force` | bool | off | Replace a pogi section that differs, such as one edited by hand. |
| `--endpoint` | string | `http://localhost:7644` | Where pogi runs. |

```bash
pogi agents init --file CLAUDE.md
```

```text title="Output"
wrote the pogi section to CLAUDE.md
```

Running it again prints `CLAUDE.md is up to date`.

## pogi tui: open the terminal screen

Attaches the terminal screen to a pogi that is already running, such as one in another terminal or a container. It stops with an error if it cannot reach pogi. Takes `--endpoint`.

## See also

- [Configuration](configuration.md): start flags, environment variables and `pogi.yaml`.
- [REST API](rest-api.md): the endpoints the CLI calls.
