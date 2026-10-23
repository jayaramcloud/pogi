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
| [`pogi tf`](#pogi-tf-print-a-terraform-provider-block), [`pogi tf diagram`](#pogi-tf-diagram-draw-a-terraform-folder), [`pogi kubeconfig gke`](#pogi-kubeconfig-gke-print-a-gke-kubeconfig) | Connect Terraform and Kubernetes tools. |
| [`pogi cost`](#pogi-cost-estimate-a-terraform-bill) | Price a Terraform folder or plan, or list machine sizes. |
| [`pogi reset`](#pogi-reset-reset-state) | Delete state. |
| [`pogi clean`](#pogi-clean-delete-leftover-containers-and-volumes) | Delete containers and volumes a stopped pogi left behind. |
| [`pogi pull`](#pogi-pull-download-images-ahead-of-time) | Download the container images a stack needs. |
| [`pogi snapshot`](#pogi-snapshot-save-and-load-snapshots) | Save, load and share state. |
| [`pogi chaos`](#pogi-chaos-inject-faults) | Add chaos rules (rules that make chosen calls fail or slow down). |
| [`pogi config`](#pogi-config-work-with-pogiyaml) | Write, check and print `pogi.yaml`. |
| [`pogi doctor`](#pogi-doctor-check-your-setup) | Check the server and its tools. |
| [`pogi lambda-sdks`](#pogi-lambda-sdks-install-the-sdks-lambda-needs) | Install the AWS SDKs for native Lambda. |
| [`pogi cert`](#pogi-cert-show-the-local-certificate-authority) | Show pogi's own certificate authority (CA). |
| [`pogi mcp`](#pogi-mcp-run-the-mcp-server-for-ai-agents), [`pogi agents init`](#pogi-agents-init-add-a-brief-to-agentsmd) | Set up AI agents. |
| [`pogi tui`](#pogi-tui-open-the-terminal-screen) | Attach the terminal screen. |
| `pogi version` | Print the version (also `pogi --version` or `pogi -v`). |

Flags go after the command name, before or after its arguments: `pogi chaos add --status 500` and `pogi ls s3 --endpoint http://localhost:4566` both work. You can write `-status` or `--status`. A flag or argument a command does not take is an error, not ignored.

Add `-h` to the end of any command, or run `pogi help <command>`, to see what it does, an example and its flags. Help goes to standard output, so `pogi logs -h | grep follow` works. `pogi --help` and `pogi -h` show the server flags, because a leading flag means `start`.

Every command exits with `0` when it worked, `1` on an error, and `2` on a usage error: a flag it does not have, a bad flag value, an argument it does not take, or a missing one. Errors go to standard error.

`status`, `services`, `ls`, `snapshot ls` and `chaos ls` print JSON with `--json`, and `logs --json` prints one JSON object per call. A client command gives up after 2 minutes if pogi does not answer.

Bold text and the redrawn progress line of `pogi pull` appear only in a terminal, and not when `NO_COLOR` is set or `TERM=dumb`.

Some commands have a second name: `up` and `run` for `start`, `terraform` for `tf`, `resources` for `ls`, and `snapshots` and `snap` for `snapshot`.

## pogi start: run the server

`pogi start [flags]`

Runs the emulator. In a terminal it shows the terminal screen (the TUI). With `--headless` it prints logs, and with `--quiet` (or `-q`) it leaves out the line for each request.

The flags, their environment variables and defaults are in [Start flags](configuration.md#start-flags). Flags win over environment variables, and environment variables win over `pogi.yaml`.

Keep passwords off the command line, where `ps` and your shell history show them. For an SMTP relay that needs one, write the relay in `pogi.yaml` and take the password from an environment variable with `${VAR}`, not `--smtp-relay smtp://user:pass@host`:

```yaml title="pogi.yaml"
email:
  relay: smtp://me:${SMTP_PASSWORD}@mail.example.com:587
```

If a service's own port is in use (SMTP, AMQP, Azure Storage), pogi starts without that part. With `--headless` the banner says so, next to the service's name. pogi also lists it as a problem in `pogi status`, in the terminal screen's header and Overview tab, and as a banner in the console. `--debug` logs it too.

```bash
pogi start --headless --port 7644
```

```text title="Output"
pogi 0.2.0  ·  355 services  ·  data /home/me/.pogi
  endpoint  http://127.0.0.1:7644
  console   http://127.0.0.1:7644/_pogi/
  env       eval "$(pogi env)"
Ready.
```

If port 10000 is taken, the banner adds a line like this before `Ready.` (ports 10001 and 10002 get the same):

```text title="Output (trimmed)"
  azblob    port 10000 is in use, so this is off — listen tcp 127.0.0.1:10000: bind: address already in use
```

Press Ctrl-C to stop. pogi prints `Stopping… (press Ctrl-C again to quit now)` at once, because stopping containers can take a few seconds. A second Ctrl-C quits without waiting, and `pogi clean` removes any containers that were left. When pogi has stopped, it prints what it stopped, where it saved state and how long it took:

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

Shows the version, uptime, region, whether state is saved to disk, the IAM mode, and every service with its resource and request counts. IAM (the rules for who may call what) can be `off`, `soft` or `strict`. Under the first line it lists any problem with pogi as a whole, one `! ` line each (red in a terminal): saved state it can't read or write, or a port it could not open. A problem does not change the exit code.

| Flag | Type | Default | Meaning |
|---|---|---|---|
| `--json` | bool | off | Print the status as JSON. |
| `--endpoint` | string | `http://localhost:7644` | Where pogi runs. |

```bash
pogi status
```

```text title="Output"
pogi 0.2.0 at http://localhost:7644 · up 7s · region us-east-1 · persistence false · iam off

CLOUD  SERVICE                    RESOURCES  REQUESTS
aws    s3                         0          0
aws    sqs                        0          0
…
```

## pogi services: list services

Lists the services with their titles. If pogi is not running, it lists the services the current config enables. Takes `--json` and `--endpoint`.

```text title="Output"
CLOUD  SERVICE                    TITLE                                               RESOURCES  REQUESTS
aws    s3                         S3                                                  0          6
aws    sqs                        SQS                                                 0          4
…
```

## pogi wait: wait until pogi is ready

Returns when pogi is ready. It exits with status 1 when the time is up. Use it in CI.

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

`pogi ls [<service>]`

Lists the resources of every service, or of one. A service the running pogi does not have is an error (`no enabled service "x"`), so a typo is not mistaken for an empty list. Takes `--json` and `--endpoint`.

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
| `-f`, `--follow` | bool | off | Keep printing new calls as they arrive. |
| `-n`, `--lines` | number | 50 | How many recent calls to show, at least 1. |
| `--json` | bool | off | Print each call as one line of JSON. |
| `--errors` | bool | off | Only failed calls. |
| `--service` | string | any | Only this service. One the running pogi does not have is an error. |
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
| `--shell` | string | `sh` | `sh` (or `bash`, `zsh`), `fish`, `pwsh` (or `powershell`) or `dotenv`. Any other value is an error. |
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

### pogi tf diagram: draw a Terraform folder

`pogi tf diagram [<path>] [flags]`

Draws a Terraform folder or a `plan.json` made with `terraform show -json`. `<path>` is the current folder when you give none. By default it sends the files to the running pogi and opens `/_pogi/terraform` in your browser. pogi holds that stack in memory, so the Cost page's Terraform tab shows it too. With `-o`, it writes an SVG file and needs no running pogi. See [See what a plan changes](../how-to/see-what-a-plan-changes.md).

| Flag | Type | Default | Meaning |
|---|---|---|---|
| `--no-open` | bool | `false` | Print the page's address and do not open the browser. This is the default when standard output is not a terminal, as in CI or a pipe. |
| `-o`, `--output` | string | none | Write the diagram to this file, which must end in `.svg`. The file uses the light theme. |
| `--group` | string | `module` | `module` or `network`. |
| `--var-file` | string, repeatable | none | A `.tfvars` file. To send it to pogi it must be inside the folder. With `-o` any path works. |
| `--endpoint` | string | `http://localhost:7644` | Where pogi runs. |

```bash
pogi tf diagram ./infra --group network
pogi tf diagram plan.json -o plan.svg
```

Exit code `2` is a usage error, such as a `-o` name that doesn't end in `.svg`. Exit code `1` is any other error, written to standard error: a path that isn't there, a var file outside the folder, files over the limits (2000 files, 50 MB), or pogi not running (``pogi isn't running at <endpoint>; start it with `pogi start`, or write a file with -o``). When pogi refuses the files, the command shows pogi's message.

## pogi kubeconfig gke: print a GKE kubeconfig

`pogi kubeconfig gke <cluster>`

Prints a kubeconfig for a Google Kubernetes Engine cluster that pogi runs. With no such cluster, it stops with `404 Not Found: no cluster projects/pogi/locations/us-central1/clusters/c1`.

| Flag | Type | Default | Meaning |
|---|---|---|---|
| `--project` | string | `pogi` | Google Cloud project. |
| `--location` | string | `us-central1` | Cluster location. |
| `--endpoint` | string | `http://localhost:7644` | Where pogi runs. |

## pogi cost: estimate a Terraform bill

`pogi cost [<path>] [flags]`

Works out what a Terraform folder or a plan would cost a month on the real cloud. `<path>` is a folder of `.tf` files, or a `plan.json` made with `terraform show -json`. pogi reads the files itself and never runs Terraform. With a path, `pogi cost` doesn't need a running pogi. See [Estimate Terraform cost](../how-to/estimate-terraform-cost.md).

| Flag | Type | Default | Meaning |
|---|---|---|---|
| `--var-file` | string, repeatable | none | A `.tfvars` file, relative to the folder or absolute. |
| `--size` | `address=size`, repeatable | none | What-if: price this resource as another size, for example `aws_instance.web=m7g.large`. |
| `--usage` | string | none | A usage file (YAML): numbers by resource address. |
| `--usage-template` | bool | `false` | Print a usage file with every key the folder uses, set to 0. |
| `--format` | string | `table` | `table`, `json` or `markdown`. |
| `--json` | bool | off | Same as `--format json`. |
| `--region` | string | none | With no path only: the region to price what is running. |
| `--endpoint` | string | `http://localhost:7644` | With no path only: where pogi is. |

```bash
pogi cost examples/cost/aws-web-stack
```

```text title="Output (trimmed)"
Source: folder examples/cost/aws-web-stack
AWS prices of 2026-10-20, from AWS Price List bulk files (pricing.us-east-1.amazonaws.com), on-demand rows
On-demand list prices, 730 h/month, USD; no free tier or discounts.

RESOURCE            SERVICE  MONTHLY  WHAT DRIVES IT
aws_instance.web_a  EC2       $72.48  assumed  730 h × $0.096 Instance hours (m5.large, Linux)
…
Total        $376.38 a month
```

Exit code `0` means pogi priced the files, even if some rows are `needs` or `unpriced`. Exit code `2` is a bad flag or value. Exit code `1` is any other error, written to standard error: a path that isn't there, a usage file that doesn't parse, a `--size` address that isn't in the folder, or a limit passed.

With **no path**, `pogi cost` prints the bill for what is running in pogi (the same numbers as the Cost page). It needs a running pogi. `--format markdown` isn't available then.

### pogi cost sizes: list sizes

`pogi cost sizes <resource type> [flags]`

Lists every size of a resource type in a region, cheapest first, with its price. The types with a list: `aws_instance`, `aws_autoscaling_group`, `aws_db_instance`, `aws_eks_node_group`, `aws_elasticache_cluster`, `aws_elasticache_replication_group`, `aws_msk_cluster`, `aws_opensearch_domain`, `aws_rds_cluster_instance`, `azurerm_linux_virtual_machine`, `azurerm_windows_virtual_machine`, `azurerm_kubernetes_cluster`, `azurerm_kubernetes_cluster_node_pool`, `azurerm_mssql_database`, `azurerm_mysql_flexible_server`, `azurerm_postgresql_flexible_server`, `azurerm_service_plan`, `google_compute_instance`, `google_container_cluster`, `google_container_node_pool` and `google_sql_database_instance`.

| Flag | Type | Default | Meaning |
|---|---|---|---|
| `--region` | string | the cloud's usual region | The region to price. |
| `--os` | string | `linux` | `linux` or `windows`. |
| `--vcpu` | number | `0` | At least this many vCPUs. |
| `--memory` | number | `0` | At least this much memory, in GB. |
| `--old` | bool | `false` | Include older generations. |

```bash
pogi cost sizes aws_instance --region eu-west-1 --vcpu 4 --memory 16
```

```text title="Output (trimmed)"
aws_instance sizes in eu-west-1, cheapest first (on-demand, 730 h/month, USD)

SIZE        FAMILY  VCPU  MEMORY GB  GPU    $/HOUR  $/MONTH
t4g.xlarge  t4g        4         16       $0.1472  $107.46
```

<!-- gcp-prices -->
`pogi cost sizes` accepts the Google Cloud types, but pogi has no Google Cloud prices yet. It stops with `pogi has no Google Cloud prices yet, so it has no google_compute_instance sizes to list`.

## pogi reset: reset state

`pogi reset [<service>…]`

Deletes state: everything with no arguments, or only the services you name. Takes `--endpoint`. The Docker containers and data volumes of the databases it deletes go with them.

```bash
pogi reset s3 sqs
```

```text title="Output"
reset s3, sqs
```

With no arguments it prints `reset all services`. A name that is empty or not an enabled service stops with `no enabled service "<name>"` and resets nothing, so `pogi reset "$SVC"` with an unset variable does not delete everything.

## pogi clean: delete leftover containers and volumes

`pogi clean [flags]`

Deletes the Docker containers and volumes that a pogi left behind and that no running pogi owns. A pogi leaves them when it is killed, or when it stops with saved state (those containers are stopped, ready for the next start). `pogi clean` talks to Docker directly, so it works when pogi is not running.

Stopped containers of saved state are only ever deleted here: a pogi that starts without saved state leaves them alone. It only looks at things with pogi's own labels. It never touches a running pogi's containers, other containers on your machine, the `pogi` network or images.

Databases keep their data in Docker volumes (`pogi-<service>-<resource>-data`), so those are listed and deleted too. Containers and volumes kept for the saved state of a data folder no pogi runs on are listed apart, by data folder, with the command that deletes them (`pogi clean --data-dir <folder>`). A plain `pogi clean` deletes only the leftovers of no data folder and those of the one you are cleaning.

It lists what it will delete, grouped by service, and asks `Delete these? [y/N]`. Then, if the data folder holds saved state, it asks `Also delete saved state in <dir>? [y/N]`. The default for both is no. pogi refuses to delete state while a pogi runs on that data folder.

When standard input is not a terminal, as in CI, there is no one to ask. Then `pogi clean` needs `--yes` or `--dry-run`, and without either it stops with exit code 2. With `--yes` it keeps saved state unless you also pass `--state`.

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

When nothing was left, it says so and exits with 0. If a container or volume cannot be deleted, pogi prints why, counts only what it did delete, and exits with 1.

## pogi pull: download images ahead of time

`pogi pull <folder | plan.json>`, `pogi pull --all` or `pogi pull --list [<path>]`

Downloads the container images that a stack needs, so the first `terraform apply` doesn't wait for them. pogi reads the folder or plan itself, like [`pogi cost`](#pogi-cost-estimate-a-terraform-bill), and picks each image the same way the service does: an RDS MariaDB 11.4 instance needs `mariadb:11.4`. It looks at databases (RDS, Aurora, ElastiCache, Cloud SQL), Kubernetes clusters (EKS, GKE, AKS), MSK and Spanner. It talks to Docker directly, so pogi doesn't have to be running. It reads `pogi.yaml`, so `docker.images` overrides apply: it downloads the images pogi will run.

| Flag | Type | Default | Meaning |
|---|---|---|---|
| `--all` | bool | off | Download the default image of every container-backed service. Use it to warm a CI cache. |
| `--list` | bool | off | Print each image and whether it is already downloaded. With a path, only the stack's images. Nothing is downloaded. |

Each image gets one line. On a terminal the line shows the percentage while it downloads.

```bash
pogi pull ./infra
```

```text title="Output"
mariadb:11.4                                  downloaded
postgres:16-alpine                            already present
```

- A path that doesn't exist, or isn't a Terraform folder or plan, is an error with exit code 1.
- A stack with nothing that runs in a container prints `Nothing to pull` and exits with 0.
- If Docker isn't running, it stops at once with exit code 1.
- If an image fails, pogi still tries the rest, then exits with 1. A download fails when no data arrives for 2 minutes, or after 30 minutes in all. See [Troubleshoot](../how-to/troubleshoot.md#an-image-download-is-stalled-or-failed).

## pogi snapshot: save and load snapshots

`pogi snapshot <command>`

Keeps named copies of the state. With no command it lists them. All commands take `--endpoint`. See [Save and load snapshots](../how-to/save-and-load-snapshots.md).

| Command | What it does |
|---|---|
| `pogi snapshot ls` (or `list`) | Lists snapshots with their size and save time (`--json`: as JSON). |
| `pogi snapshot save [<name>]` | Saves all state. Without a name, the date and time is the name. |
| `pogi snapshot load <name>` (or `restore`) | Replaces the current state with the snapshot. |
| `pogi snapshot rm <name>` (or `delete`) | Deletes a snapshot and prints `removed snapshot <name>`. |
| `pogi snapshot export <name> [<file>]` | Writes the snapshot to a file, or to standard output without a file name or with `-`. The file appears only once the whole snapshot has arrived. |
| `pogi snapshot import <file> [<name>]` | Stores a file as a snapshot. The name defaults to the file name without its extension. `-` reads standard input, and then needs a `<name>`. |

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

Manages chaos rules, which make chosen calls fail or slow down. With no command it lists them. See [Inject chaos](../how-to/inject-chaos.md).

| Command | What it does |
|---|---|
| `pogi chaos ls` (or `list`) | Lists rules with their hit counts (`--json`: as JSON). |
| `pogi chaos add` | Adds a rule. Needs `--status`, `--latency` or both. |
| `pogi chaos rm <id>` (or `remove`) | Removes one rule and prints `removed rule <id>`. |
| `pogi chaos clear` | Removes all rules and prints `removed all chaos rules`. |

Flags of `pogi chaos add`:

| Flag | Type | Default | Meaning |
|---|---|---|---|
| `--service` | string | any | Service to match, as a pattern. |
| `--op` | string | any | Operation to match, as a pattern, such as `Put*`. |
| `--rate` | number | `1` | Chance that a matching call is affected, from 0 to 1. |
| `--status` | number | none | HTTP status to return, such as `500`. |
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
- A status outside 400-599, a `--rate` outside 0-1 or a negative `--latency` stops with a message that names it.
- The rule flags work with `add` only: `pogi chaos ls --status 500` stops with an error.
- `pogi chaos rm` with an unknown id stops with `rule not found`.

## pogi config: work with pogi.yaml

`pogi config <command>`

| Command | What it does |
|---|---|
| `pogi config init [--force] [<path>]` | Writes an annotated `pogi.yaml`, in the current folder by default. Refuses to overwrite a file without `--force`. |
| `pogi config validate [<path>]` | Checks the file. Mistakes come with a line number, and the exit status is 1. |
| `pogi config print [<path>]` | Shows the settings after the file, environment variables and defaults are merged. Secrets are masked. |

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

Checks the server, Python and Node.js for Lambda, the AWS SDKs for native Lambda, Docker, whether `*.localhost` names resolve, and pogi's own certificate authority. It prints a line for each check and exits with status 1 if one needs attention.

| Flag | Type | Default | Meaning |
|---|---|---|---|
| `--data-dir` | string | `~/.pogi` | pogi's data folder. |
| `--endpoint` | string | `http://localhost:7644` | Where pogi runs. |

```text title="Output"
  ✓ server at http://localhost:7644 0.2.0
  ✓ python3 for Python Lambdas: Python 3.9.6
  ✓ node for Node.js Lambdas: v25.9.0
  ✗ AWS SDKs for native Lambdas in ~/.pogi/lambda — run `pogi lambda-sdks` (CDK custom resources need them)
  ✓ Docker Desktop 29.4.1 at unix:///var/run/docker.sock (Engine API 1.54)
  ✓ *.localhost names resolve (pogi env --hostname aws.localhost, virtual-host S3, execute-api and lambda-url hosts)
  ✓ local CA ~/.pogi/certs/ca.pem
pogi: 1 check(s) need attention
```

## pogi lambda-sdks: install the SDKs Lambda needs

`pogi lambda-sdks [--data-dir <folder>] [<package>…]`

Installs the AWS SDKs that real Lambda runtimes include (the JavaScript v3 SDK and boto3) where pogi's native runtimes look for them. It also installs the Functions Framework that runs Cloud Functions natively. The CDK's custom resources need this. It needs `npm` or `python3`.

Extra package names can follow. A name that starts with `@` goes to npm. Any other name goes to pip.

| Flag | Type | Default | Meaning |
|---|---|---|---|
| `--data-dir` | string | `~/.pogi` | pogi's data folder. |

## pogi cert: show the local certificate authority

Prints where pogi's own certificate authority (CA) is, creates it if needed, and says how to trust it on your system.

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

Runs an MCP server (MCP is the protocol AI coding agents use to call tools) on standard input and output, so agents can look inside pogi and control it. The same MCP server is on HTTP at `http://localhost:7644/_pogi/mcp`.

There is no cloud-API passthrough. Agents make calls with the usual SDKs and CLIs.

| Flag | Type | Default | Meaning |
|---|---|---|---|
| `--print-config` | bool | off | Print the setup for Claude Code, Cursor and Codex. |
| `--endpoint` | string | `http://localhost:7644` | Where pogi runs. |

The tools: `status`, `connection_info`, `list_resources`, `inspect_resource`, `request_log`, `get_request`, `iam_decisions`, `peek_queue`, `inbox`, `function_logs`, `invoke_function`, `deploy_stack`, `stack_status`, `delete_stack`, `seed`, `reset`, `snapshot_list`, `snapshot_save`, `snapshot_load`, `chaos_list`, `chaos_add`, `chaos_clear`, `estimate_terraform_cost` and `list_sizes`.

- `estimate_terraform_cost` takes `path` (a Terraform folder or a `plan.json` on the machine pogi runs on), and optionally `var_files` and `usage_file`. It returns the same JSON as `pogi cost --format json`.
- `list_sizes` takes `resource_type`, and optionally `region`, `os`, `min_vcpu` and `min_memory_gb`. It returns the sizes, cheapest first.

When pogi accepts connections from other machines (`--allow-external`) and doesn't run in a container, `estimate_terraform_cost` refuses to read files, because anyone who can reach the port could read files on that machine. It says to run `pogi cost <path>` in a terminal instead. Inside Docker it works on folders you mounted into the container, such as `docker run -v ./infra:/srv/infra …` with the path `/srv/infra`.

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

The screen has seven tabs. Press a number key to switch:

1. **Overview:** the services in use, with their resources, calls, errors, average time and last call; calls per second; the top errors; the latest calls.
2. **Requests:** every recent call, with the selected call's details under the list. `/` filters, `e` shows only errors, `f` picks a cloud, `s` sorts, `enter` opens the full request and response.
3. **Resources:** the services that hold something, beside their tables. `a` shows every service, `enter` opens a resource.
4. **Chaos:** the chaos rules and how many calls each changed.
5. **Snapshots:** saved snapshots, with their size and age.
6. **Logs:** CloudWatch Logs and Cloud Logging lines, with their level. `/` filters, `w` shows only warnings and errors.
7. **Flows:** the steps where one service hands work to another, such as an SNS message delivered to SQS. `enter` lists that flow's calls in Requests.

The top line shows whether pogi is ready, whether state is saved, the IAM mode, the chaos rules and any problems with saved state. Keys that delete or replace state (`R` resets everything, `x` resets one service, loading or deleting a snapshot) ask first. `q` leaves the screen; inside `pogi start` it also stops pogi.

## See also

- [Configuration](configuration.md): start flags, environment variables and `pogi.yaml`.
- [REST API](rest-api.md): the endpoints the CLI calls.
