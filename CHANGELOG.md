# Changelog

All notable changes to pogi are written down here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and pogi uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-10-17

The first release. pogi is one Go binary that emulates AWS, Google Cloud and Azure on one port
(7644), with a CLI, a terminal UI and a web console. It starts in about 25 ms and uses about 64 MB
at idle ([how we measured](docs/reference/benchmarks.md)).

### Added

- **181 services**: 131 AWS, 26 Google Cloud and 24 Azure.
  [docs/reference/coverage.md](docs/reference/coverage.md) lists each one, what it does and what
  it does not do.
  - AWS: S3, SQS, SNS, DynamoDB, Lambda, API Gateway, Step Functions, EventBridge, CloudFormation
    and the CDK, IAM, KMS, Secrets Manager, ECS, EKS, RDS and many more. 70 of them come from the
    botocore API models ([docs/reference/aws-long-tail.md](docs/reference/aws-long-tail.md)).
  - Google Cloud: Cloud Storage, Pub/Sub, Firestore, BigQuery, Cloud Run, Cloud Functions, Cloud
    SQL, Kubernetes Engine, Compute Engine and more.
  - Azure: Blob, Queue and Table Storage, Cosmos DB, Key Vault, Service Bus, Event Hubs, Functions,
    Azure SQL, Kubernetes Service, Virtual Machines and more. Azure Storage also listens on
    Azurite's ports, 10000-10002.
- **Works with the tools you already use**: the AWS, Google Cloud and Azure SDKs and CLIs,
  Terraform and the CDK. `pogi tf` prints a Terraform provider override and `pogi env` prints the
  SDK settings.
- **LocalStack compatibility**: the `/_localstack/health` and `/_localstack/init` endpoints, init
  hooks, the `Ready.` log line and the `SERVICES`, `PERSISTENCE`, `DEBUG`, `DEFAULT_REGION` and
  `GATEWAY_LISTEN` variables. `--port 4566` keeps the old endpoint. See
  [docs/how-to/migrate-from-localstack.md](docs/how-to/migrate-from-localstack.md).
- **Real engines in containers** when Docker or Podman is running: databases, caches, Kafka,
  OpenSearch, Kubernetes (EKS, GKE and AKS on k3s), container services and VMs. Without Docker those
  resources are kept as metadata only. pogi also runs inside a container with the Docker socket mounted.
- **State that survives restarts**: storage modes `memory`, `hybrid`, `persistent` and `wal`, set
  for all services or per service. `persistent` and `wal` keep every acknowledged write through a
  `kill -9`. Named snapshots can be saved, loaded, exported and imported.
- **`pogi.yaml`** for settings, identities, seed resources, CloudFormation stacks and chaos rules,
  applied at start and after every reset.
- **Testing tools**: IAM enforcement (`--iam soft|strict`), optional SigV4 signature checks, chaos
  rules (errors and latency per service, operation or rate), and `aws-cn` and `aws-us-gov`
  partitions.
- **Input checks**: requests are checked against each API's own limits (lengths, ranges, patterns
  and allowed values), and unknown operations or resource types fail with the cloud's own error.
- **Clean shutdown**: on Ctrl-C or SIGTERM pogi stops what it started and prints the list. With
  saved state, its containers are stopped and come back on the next start. `pogi clean` removes
  containers and volumes left behind by a pogi that is gone.
- **Crash safety**: a panic in one request or background task is logged and answered with a 500 in
  the cloud's own error shape. pogi keeps running.
- **Safe on localhost**: pogi refuses requests that web pages on other sites send to it (cross-site
  requests and DNS rebinding). Uploads are limited to 10 MB per file (`--max-upload-mb`), and an
  upload that stalls for 30 seconds is closed. A local CA gives TLS and `*.localhost` hostnames.
- **For coding agents**: an MCP server (`pogi mcp`, or `/_pogi/mcp` over HTTP) with 22 tools, and
  `pogi agents init` to add a pogi brief to `AGENTS.md`.
- **Commands**: `start`, `tui`, `status`, `services`, `wait`, `config`, `env`, `aws`, `tf`,
  `kubeconfig gke`, `logs`, `ls`, `reset`, `clean`, `snapshot`, `chaos`, `lambda-sdks`, `cert`,
  `doctor`, `mcp`, `agents init` and `version`.
- **Install** with Homebrew on macOS (`brew install nrndev/tap/pogi`), `.deb` and `.rpm` packages
  on Linux, Scoop on Windows, Docker (`ghcr.io/nrndev/pogi`), `go install`, or a release archive
  for Linux, macOS and Windows on amd64 and arm64. The checksums and the image are signed with
  cosign, and the archives and packages carry GitHub build attestations ([how to check a download](docs/how-to/install.md)).

[Unreleased]: https://github.com/nrndev/pogi/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/nrndev/pogi/releases/tag/v0.1.0
