---
title: LocalStack to pogi mapping
description: What each LocalStack endpoint, variable, tool and file becomes in pogi.
---

# LocalStack to pogi mapping

pogi keeps most of LocalStack's paths, variables and directories, and replaces the rest.

| Section | LocalStack thing |
|---|---|
| [Endpoint and health](#endpoint-and-health) | The edge port, health and init endpoints, the `Ready.` line. |
| [Environment variables](#environment-variables) | `SERVICES`, `PERSISTENCE`, `DEBUG` and the rest. |
| [Credentials and regions](#credentials-and-regions) | Keys, accounts and ARNs (AWS's IDs for resources). |
| [The command-line tools](#the-command-line-tools) | `localstack`, `awslocal`, `tflocal`, `cdklocal`. |
| [Init hooks](#init-hooks) | `boot.d`, `start.d`, `ready.d` and `shutdown.d`. |
| [Persistence](#persistence) | `PERSISTENCE=1` and the volume. |
| [Hostnames](#hostnames) | `localhost.localstack.cloud` and virtual-host style S3. |

## Endpoint and health

| LocalStack | pogi |
|---|---|
| `http://localhost:4566`, the edge port | `http://localhost:7644`, or `--port 4566` to keep LocalStack's. One port serves AWS, Google Cloud, Azure, the console and the REST API |
| `GET /_localstack/health` | the same path, with `services` (each `running`), `edition` and `version` (`pogi-<version>`). Service names are pogi's ids (`pogi services` lists them) |
| `GET /_localstack/info` | the same, with the usual fields |
| `GET /_localstack/init`, `/init/<stage>` | the same |
| `POST /_localstack/health` (restart, kill) | not supported; restart the process or call `pogi reset` |
| `GET /_aws/sqs/messages`, `/_aws/ses`, `/_aws/sns/sms-messages` | the same |
| `POST /moto-api/reset` | the same (a reset of everything; moto is a Python AWS mock) |
| the `Ready.` log line | the same; `pogi wait` also waits for it |
| Docker `HEALTHCHECK` on the health endpoint | the image has one |

Container ports:

- `7644` for everything.
- `10000` to `10002` for Azure Storage (Azurite's ports).
- Services with their own protocol use their own ports: `4510` (ECR, a Docker registry), `2525` (SES SMTP), `1883` (MQTT; the WebSocket is on `7644` at `/mqtt`), `5672` (AMQP for Service Bus and Event Hubs) and `2222` (Transfer Family SFTP).
- pogi does not use LocalStack's 4510-4559 range for external services.

## Environment variables

| LocalStack | pogi |
|---|---|
| `SERVICES=s3,sqs` | `SERVICES` or `POGI_SERVICES`. It also takes cloud names (`aws`, `gcp`, `azure`) and `-name` to leave one out. Unlike LocalStack, services that are not named are not lazily started on demand: they are not there. Leave it unset and everything is on, which costs little ([why](../explanation/how-pogi-works.md#why-is-pogi-built-this-way)) |
| `PERSISTENCE=1` | `PERSISTENCE=1` or `POGI_PERSIST=1`, or `--persist` (storage mode `hybrid`) |
| `DEFAULT_REGION` | `DEFAULT_REGION` or `POGI_REGION` |
| `DEBUG=1` | `DEBUG=1` or `POGI_DEBUG=1` |
| `GATEWAY_LISTEN=0.0.0.0:4566` | the same (the first address is used), so it also sets the port. pogi also has `POGI_HOST` and `POGI_PORT` |
| `LAMBDA_EXECUTOR` | `POGI_LAMBDA`: `auto` (native Python and Node.js, else Docker), `native` or `docker` |
| `ENFORCE_IAM=1` | `POGI_IAM=soft` (records allow and deny decisions) or `strict` |
| `S3_SKIP_SIGNATURE_VALIDATION=0` | `POGI_VALIDATE_SIGNATURES=1` |
| `LOCALSTACK_HOSTNAME`, `HOSTNAME_EXTERNAL` | `POGI_HOSTNAME`, see [Hostnames](#hostnames) |
| `DOCKER_HOST` | the same; pogi talks to the Docker or Podman API itself |
| `LOCALSTACK_AUTH_TOKEN`, `LOCALSTACK_API_KEY` | not needed and ignored. Nothing in pogi is gated |

Any other variable LocalStack reads is ignored without a warning. [Configuration](configuration.md) lists every pogi setting. A `pogi.yaml` can hold all of them (`pogi config init`).

## Credentials and regions

- pogi accepts any access key and secret, as LocalStack does. `pogi env` prints `test` and `test`.
- Account `000000000000` is the default. Users from `pogi.yaml` or the console get their own accounts.
- ARNs follow the region's partition (`aws-cn`, `aws-us-gov`).

## The command-line tools

| LocalStack tool | With pogi |
|---|---|
| `localstack start` | `pogi start`, or `pogi` for the terminal screen (the TUI). `pogi start --headless` logs like a daemon |
| `localstack status`, `localstack wait` | `pogi status`, `pogi wait --timeout 60s` |
| `localstack logs -f` | `pogi logs -f` (the log of calls; use `docker logs` for the container) |
| `localstack state export/import` and Cloud Pods | `pogi snapshot save <name>`, `load`, `ls`, `rm`, `export <name> file`, `import file`. The files are pogi's, not LocalStack's |
| `awslocal s3 ls` | `pogi aws s3 ls`, or `eval "$(pogi env)"` and use `aws`. To keep the name, `alias awslocal=aws` after `eval "$(pogi env)"` |
| `tflocal apply` | `pogi tf > pogi_override.tf`, then plain `terraform apply`. `pogi tf` writes a provider block with every endpoint, for the aws, google and azurerm providers |
| `cdklocal deploy` | plain `cdk bootstrap` and `cdk deploy` with `AWS_ENDPOINT_URL` set. No wrapper |
| `samlocal`, `awscli-local` | the same as `awslocal`; SAM itself works once `AWS_ENDPOINT_URL` is set |
| chaos API (Enterprise) | `pogi chaos add/ls/rm/clear`, `POST /_pogi/api/chaos`, or `chaos:` in `pogi.yaml` |
| LocalStack web app | `http://localhost:7644/_pogi/`, no account |

## Init hooks

pogi uses LocalStack's directories and stages, so existing hook scripts and volume mounts keep working.

- **Stages:** `boot.d`, `start.d`, `ready.d` and `shutdown.d`.
- **Where:** `/etc/localstack/init/` or `~/.pogi/init/` (`<data folder>/init`).
- **State:** `GET /_localstack/init` and `/_localstack/init/ready` report it, as in LocalStack. See the [REST API](rest-api.md#get-init-hook-status).

Order and environment are in [Init hooks](configuration.md#init-hooks). For resources that need no script, `pogi.yaml` can seed buckets, queues, tables, stacks and more before `Ready.`, and again after every reset.

## Persistence

| | LocalStack | pogi |
|---|---|---|
| On | `PERSISTENCE=1`, volume at `/var/lib/localstack` | `PERSISTENCE=1`, volume at `/var/lib/pogi` (`POGI_DATA_DIR`) |
| Modes | one | `memory` (default), `hybrid` (what `PERSISTENCE=1` selects: written every few seconds and on exit), `persistent` (before each write is acknowledged) and `wal` (DynamoDB, SQS and S3 journal each change) |
| Survives `kill -9` | no guarantee | in `persistent` and `wal`, yes |
| Per service | no | yes, `storage.services` in `pogi.yaml` |

## Hostnames

- pogi does not provide LocalStack's DNS name `localhost.localstack.cloud` or its subdomains. It never needs the network for names.
- The path-style default works with `AWS_ENDPOINT_URL` alone.
- For virtual-host style S3 and for API Gateway and Lambda URLs, start pogi with `--hostname aws.localhost` (or `hostname:` in `pogi.yaml`). Then:
  - S3 answers at `<bucket>.aws.localhost:7644`.
  - API Gateway answers at `<api>.execute-api.localhost:7644`.
  - Lambda URLs answer at `<id>.lambda-url.localhost:7644`.
- `pogi env --hostname aws.localhost` prints matching variables.
- `pogi doctor` says whether `*.localhost` resolves on your machine. It does on macOS and with systemd-resolved, but not in Alpine containers.

## See also

- [Migrate from LocalStack](../how-to/migrate-from-localstack.md): the steps to move.
- [How pogi works](../explanation/how-pogi-works.md#why-is-pogi-built-this-way): why pogi differs.
