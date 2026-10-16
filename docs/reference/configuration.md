---
title: Configuration
description: Start flags, environment variables, ports, pogi.yaml, storage modes and the data folder.
---

# Configuration

## Order of precedence

When the same setting comes from more than one place, the first of these wins:

1. A command-line flag.
2. An environment variable.
3. `pogi.yaml`.
4. The default.

`pogi config print` shows the result after all four are merged. Secrets are masked.

## Start flags

`pogi start` takes these flags. Each flag with an environment variable can be set there instead.

| Flag | Environment variable | Default | Meaning |
|---|---|---|---|
| `--port` | `POGI_PORT` | `7644` | Main port. All clouds, the console and the REST API answer here. |
| `--host` | `POGI_HOST` | `127.0.0.1` (and `::1`) | Listen address. `0.0.0.0` accepts containers and other machines. |
| `--services` | `POGI_SERVICES`, `SERVICES` | all | Comma-separated services (the names `pogi services` prints) or clouds to enable, for example `s3,sqs,gcp`. `-name` leaves one out. |
| `--data-dir` | `POGI_DATA_DIR` | `~/.pogi` | State, snapshots, certificates and init hooks. |
| `--persist` | `POGI_PERSIST=1` (or `true`), `PERSISTENCE=1` | off | Load and save state in the data folder. Same as `--storage hybrid`. |
| `--storage` | `POGI_STORAGE` | `memory` | `memory`, `hybrid`, `persistent` or `wal`. See [Storage modes](#storage-modes). |
| `--region` | `POGI_REGION`, `DEFAULT_REGION` | `us-east-1` | Default AWS region. The partition follows it (`cn-*`, `us-gov-*`). |
| `--lambda` | `POGI_LAMBDA` | `auto` | Lambda executor: `auto`, `native` or `docker`. |
| `--iam` | `POGI_IAM` | `off` | IAM (who may do what) enforcement: `off`, `soft` or `strict`. See [Switch IAM modes](../how-to/switch-iam-modes.md). |
| `--validate-signatures` | `POGI_VALIDATE_SIGNATURES` | off | Reject requests whose SigV4 signature (how AWS signs requests) is wrong. |
| `--allow-external` | `POGI_ALLOW_EXTERNAL` | off (on inside containers) | Allow a `--host` other than loopback. |
| `--hostname` | `POGI_HOSTNAME` | `localhost` | Host name in URLs. `aws.localhost` turns on virtual-host style. |
| `--tls-cert`, `--tls-key` | | pogi's local certificate authority | Your own HTTPS certificate and key. |
| `--max-upload-mb` | | `0` (10 MB) | Largest upload in MiB. Larger ones are refused with `413` (`EntityTooLarge`). |
| `--smtp-port` | `POGI_SMTP_PORT` | `2525` | SES SMTP port. `-1` turns it off. |
| `--smtp-relay` | `POGI_SMTP_RELAY` | none | Also deliver all SES mail to this server, `smtp://[user:pass@]host:port`. |
| `--mqtt-port` | `POGI_MQTT_PORT` | `1883` | IoT MQTT port. Opened on first IoT use. `-1` leaves only the WebSocket at `/mqtt`. |
| `--amqp-port` | `POGI_AMQP_PORT` | `5672` | AMQP 1.0 port for Service Bus and Event Hubs. `-1` turns it off. |
| `--transfer-port` | `POGI_TRANSFER_PORT` | `2222` | Transfer Family SFTP port. Opened with the first server. `-1` picks a free port. |
| `--config` | `POGI_CONFIG` | `./pogi.yaml` if present | The `pogi.yaml` to load. |
| `--headless` | | off | No TUI: print logs and requests. |
| `--quiet` | | off | With `--headless`, do not print each request. |
| `--debug` | `DEBUG=1`, `POGI_DEBUG=1` | off | Verbose logging. |

## Environment variables

Most start flags have an environment variable, listed in [Start flags](#start-flags). These are the others:

| Variable | Used by | Meaning |
|---|---|---|
| `POGI_ENDPOINT` | Client commands | Where `pogi status`, `pogi ls`, `pogi chaos` and the other client commands find pogi. Default `http://localhost:7644`. The `--endpoint` flag beats it. |
| `GATEWAY_LISTEN` | `pogi start` | LocalStack's `host:port[,...]`. The first address sets `--host` and `--port`. |
| `DOCKER_HOST` | Services that use containers | The container engine to use. |
| `DOCKER_TLS_VERIFY` | Services that use containers | Set it (to `1`) to reach a `tcp://` engine over TLS and check its certificate. |
| `DOCKER_CERT_PATH` | Services that use containers | The folder with `ca.pem`, `cert.pem` and `key.pem` for a TLS engine. Default `~/.docker`. |
| `AWS_ENDPOINT_URL` and others | Your SDKs and CLIs | Set by `pogi env`. See [Connect your tools](../how-to/connect-your-tools.md). |

The Docker image sets `POGI_HOST=0.0.0.0`, `POGI_ALLOW_EXTERNAL=1` and `POGI_DATA_DIR=/var/lib/pogi`.

## Ports

| Port | What | How to change it |
|---|---|---|
| `7644` | Everything: AWS, Google Cloud (gRPC and REST), Azure over HTTP and HTTPS, the console (`/_pogi/`), the REST API and MCP. | `--port` |
| `10000`, `10001`, `10002` | Azure Blob, Queue and Table Storage, on Azurite's ports. | Fixed. |
| `4510` | The ECR registry, a Docker registry that `docker push` can use. It runs in a container named `pogi-ecr-4510`, started the first time you use ECR. | Fixed. |
| `2525` | SES SMTP. | `--smtp-port` |
| `1883` | IoT MQTT. | `--mqtt-port` |
| `5672` | AMQP 1.0 for Service Bus and Event Hubs. | `--amqp-port` |
| `2222` | Transfer Family SFTP. | `--transfer-port` |

The main port also speaks HTTPS, with a certificate from pogi's local certificate authority. `pogi cert` shows where it is and how to trust it.

## pogi.yaml

`pogi config init` writes an annotated `pogi.yaml`. pogi reads it from `--config`, `POGI_CONFIG`, or `pogi.yaml` or `pogi.yml` in the current folder. Unknown keys are errors. `pogi config validate` reports mistakes as `pogi.yaml:12: ...`. Values may use `${VAR}` and `${VAR:-default}`.

Top-level keys:

| Key | What it holds |
|---|---|
| `version` | The format of the file. Use `1`. |
| `server` | `port`, `host`, `region`, `hostname`, `tls` (`cert`, `key`), `cors` (`origins`, `headers`), `allowExternal`, `maxUploadMB`. |
| `services` | `enable` and `disable`: clouds (`aws`, `gcp`, `azure`), service names or `all`. |
| `storage` | `mode`, `dir`, `flushInterval` (for `hybrid`), `services` (a mode per service). |
| `iam` | `mode` and `validateSignatures`. |
| `lambda` | `executor` and `warmPool` (idle environments kept warm per function, default 4). |
| `docker` | `host`, `network` and `images` (for example `rds.postgres: "postgres:16-alpine"`). |
| `ai` | `endpoint`, `apiKey` and `models`: an OpenAI-compatible endpoint (Ollama, LM Studio) for Bedrock and the language services. Without one they return canned answers. |
| `email` | `smtpPort` and `relay`. |
| `chaos` | A list of fault rules. See [Inject chaos](../how-to/inject-chaos.md). |
| `identities` | IAM users and roles. |
| `resources` | Resources to create at startup. |
| `stacks` | CloudFormation stacks to deploy at startup. |

Example:

```yaml title="pogi.yaml"
version: 1
server: {port: 7644}
iam: {mode: soft, validateSignatures: true}
identities:
  aws:
    users: [{name: dev, accessKey: AKIADEV0000000000001, secretKey: devsecret,
             policies: ["arn:aws:iam::aws:policy/ReadOnlyAccess"]}]
resources:
  aws:
    s3: {buckets: [uploads, {name: logs, versioning: true}]}
    sqs: {queues: [jobs, {name: events.fifo, fifo: true}]}
    dynamodb: {tables: [{name: users, key: [pk, sk]}]}
  gcp: {pubsub: {topics: [orders], subscriptions: [{name: worker, topic: orders}]}}
  azure: {blob: {containers: [images]}}
  typed: [{type: "AWS::SNS::Topic", properties: {TopicName: alerts}}]
chaos: [{name: flaky-sqs, service: sqs, op: SendMessage, rate: 0.1, status: 500}]
```

Identities, resources and stacks are created before `Ready.` (so `pogi wait` returns after them) and again after every `pogi reset`. Anything that already exists is left alone. Users can also have `inline` policies and `groups`, and roles have `trust` and `policies`. The `typed` form takes a CloudFormation resource type and its properties. A stack has a `name`, a `template` path (relative to the file) and `parameters`.

In Go tests, `cfg, _ := pogi.LoadConfig("testdata/pogi.yaml")` and `pogi.Start(t, cfg)` do the same.

## Storage modes

State is in memory unless you choose a storage mode (`--storage`, or `storage:` in `pogi.yaml`, per service if you like):

| Mode | On disk |
|---|---|
| `memory` | Nothing. This is the default. |
| `hybrid` | Every service's state, written every few seconds and on exit (`--persist`). |
| `persistent` | Written before each write request is answered. |
| `wal` | DynamoDB, SQS and S3 append each change to a journal. Other services work as in `persistent`. |

`persistent` and `wal` survive `kill -9` without losing an acknowledged write. Per service, for example:

```yaml title="pogi.yaml"
storage:
  mode: memory
  services: {dynamodb: wal, s3: persistent}
```

With any mode but `memory`, pogi stops its containers (databases, brokers and the like) when it exits, and starts them again at the next start, so the data inside survives and nothing runs while pogi is off. Each container carries a `pogi.data` label with the data folder it belongs to. Without saved state, containers are removed on exit. `pogi clean` deletes leftovers.

## The data folder

The data folder is `~/.pogi` unless you set `--data-dir`.

| Path | What |
|---|---|
| `state/` | One JSON file per service, when a storage mode other than `memory` is on. |
| `snapshots/` | Named snapshots (`<name>.json`). See [Save and load snapshots](../how-to/save-and-load-snapshots.md). |
| `certs/` | pogi's local certificate authority (`ca.pem`) and the certificate it signs. |
| `lambda/` | The AWS SDKs that `pogi lambda-sdks` installs for native functions. |
| `init/` | Init hooks. |
| `pogi.log` | The log, when pogi runs with the TUI. |

### Init hooks

Scripts run at four stages: `boot.d`, `start.d`, `ready.d` and `shutdown.d`, under `<data folder>/init/` or `/etc/localstack/init/`. They run in name order. `.sh` files run with `sh`, `.py` files with `python3`, and anything executable runs as itself. They get `AWS_ENDPOINT_URL`, test credentials and a region in the environment. `GET /_localstack/init` and `/_localstack/init/ready` report their state.

## Network behavior

- **Listening.** A `--host` other than loopback needs `--allow-external` (`allowExternal: true` in `pogi.yaml`), except inside Docker or Podman.
- **Browsers.** Pages on `localhost`, `*.localhost`, `127.0.0.1` or `[::1]` (any port) may call every API, as with LocalStack. pogi refuses (`403`) every other browser request before it reaches a service, so a website you visit cannot use your local cloud:
  - a request whose `Origin` is another site;
  - a cross-site request with no `Origin` (an image or link), unless it comes from a localhost page;
  - a request whose `Host` is not a local name (`localhost`, `*.localhost`, an IP, a name with no dot, `host.docker.internal`, `hostname` and its subdomains, the hosts in `server.cors.origins`, or a real AWS or Google API host). This stops DNS rebinding.

  Opening the console from another site's link still works.

  SDKs, CLIs, Terraform and containers send no browser headers and are not affected. `server.cors.origins` in `pogi.yaml` adds origins (and their hosts); `["*"]` turns the check off. S3 bucket CORS, API Gateway and Lambda URLs still shape their responses, but only for origins that pass this check. The REST API (`/_pogi/api/`, `/_localstack/` and `/moto-api/`, and the console's form posts) also refuses cross-origin writes.
- **`*.localhost`.** With `hostname: aws.localhost`, S3 answers `<bucket>.aws.localhost`, and `<api>.execute-api.localhost` and `<url-id>.lambda-url.localhost` reach API Gateway and Lambda URLs. `pogi env --hostname aws.localhost` prints matching variables, and `pogi doctor` says whether such names resolve on your machine.
- **Partitions.** ARNs (AWS's IDs for resources) follow the region: `arn:aws-cn:` in `cn-*`, `arn:aws-us-gov:` in `us-gov-*`.
- **Limits.** pogi is a local emulator for tests, so uploads stop at `maxUploadMB` (10 MB, whole file). This holds for every request body, and for files put together from parts (S3 multipart, Azure blocks and appends, GCS resumable chunks, SFTP) and for console uploads. A refused upload gets the cloud's own error (`EntityTooLarge` in AWS, `RequestBodyTooLarge` in Azure, 413 in GCP) with a message that names the limit. Two things are exempt: function code zips (up to 50 MB, or more if you raise the limit) and container image pushes. A body that sends no data for 30 seconds gets `408`; a slow upload that keeps sending is fine.

## Lambda

Pick how functions run with `--lambda auto|native|docker` (`lambda.executor` in `pogi.yaml`).

- **Native** runs Python and Node.js on your machine, or your own `bootstrap` binary for custom runtimes. pogi speaks the Lambda Runtime API (the calls a function uses to fetch events) to a small bootstrap.
- **Docker** uses the AWS base images. It is used when native is not possible, and always for container image functions (`PackageType: Image`, with `ImageConfig` overrides).
- **Docker engine.** Any engine with the Docker API works: Docker, Podman, Colima or OrbStack, on its usual socket or through `DOCKER_HOST`. On Linux without Docker Desktop, function containers use the host network, because a bridge cannot reach a pogi that listens on `127.0.0.1`.
- **What a function gets.** Session credentials for its execution role, and `AWS_ENDPOINT_URL` pointing back at pogi. Its logs go to CloudWatch Logs.
- **Hot reload.** Create the function with the S3 bucket `hot-reload` and an absolute directory as the key. pogi runs the code in that directory.
- **Concurrency.** Each concurrent invocation gets its own environment. Up to `lambda.warmPool` (4) idle environments per function stay warm for 10 minutes, and provisioned concurrency keeps its environments started. Reserved concurrency and the account limit of 1000 concurrent executions throttle synchronous invokes with a `429`. Async events and event source mappings wait for capacity instead.
