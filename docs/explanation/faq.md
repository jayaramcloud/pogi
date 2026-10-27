---
title: FAQ
description: Short answers to common questions about pogi.
---

# FAQ

## Is pogi free?

Yes. pogi is MIT licensed. It needs no account, license key or sign-up, and there is no paid plan.

## Do I need Docker?

Not for the core services. S3, SQS, SNS, DynamoDB, Lambda in Python and Node.js, and most other services run inside the pogi program. Services that run real software need Docker: RDS, ECS, Cloud SQL and Azure SQL, for example. The [coverage page](../reference/coverage.md) lists them. Any engine with the Docker API works: Docker, Podman, Colima or OrbStack.

## Does it run on Windows?

Yes. Release builds exist for Windows, on Intel and Arm. The easiest way to install is Scoop. You can also download the `.zip` from the [releases page](https://github.com/nrndev/pogi/releases). See [Install pogi](../how-to/install.md). Run `pogi doctor` to check what your machine has.

## Does it phone home?

No. pogi's code has no outgoing network call that sends usage data, checks for updates or contacts a pogi server. An idle pogi makes no outgoing connections.

pogi reaches out only when you ask it to:

- Webhooks and HTTP endpoints you configure, such as an SNS HTTP subscription, an EventBridge API destination, a Cloud Tasks HTTP task or an Event Grid webhook. They go where you point them.
- Container images. Services that run in Docker make your Docker engine pull images from a registry.
- `pogi lambda-sdks`, which runs `npm` and `pip` to download the AWS SDKs for native Lambda functions.
- Mail relay, only if you set `--smtp-relay`.

## Does it need real cloud credentials?

No. `pogi env` sets test credentials. The default AWS key and secret are both `test`. Never put real credentials into a pogi setup.

## What port does it use?

`7644`, which is "pogi" on a phone keypad. Change it with `--port`. To keep LocalStack's port, run `pogi start --port 4566`.

## Can other machines or containers reach it?

By default pogi listens only on your own computer. To accept other machines, set `--host 0.0.0.0` together with `--allow-external`. Inside Docker or Podman, `--allow-external` is already on, so publish the port as `-p 127.0.0.1:7644:7644` to keep it on your machine. Anyone who can reach the port can use every API, so never open it to a network you don't trust.

## How do I keep my data between runs?

Start pogi with `--persist`, or save a named [snapshot](../how-to/save-and-load-snapshots.md).

## How is pogi different from LocalStack?

pogi covers AWS, Google Cloud and Azure in one program, and needs no account. See the [comparison table](../reference/compared.md). To move over, follow [Migrate from LocalStack](../how-to/migrate-from-localstack.md).

## Which services does it cover?

<!-- count:total -->355 services: 144 AWS (90 with real behavior, 54 Thin), 108 Google Cloud (25 with real behavior, 83 Thin) and 103 Azure (30 with real behavior, 73 Thin)<!-- /count -->. A Thin service keeps your resources and reads them back, but nothing runs behind them. The [coverage page](../reference/coverage.md) lists them all.

## Does it work with Terraform and the CDK?

Yes. `pogi tf` prints a provider block for Terraform, and the CDK works with `AWS_ENDPOINT_URL` set. See [Connect your tools](../how-to/connect-your-tools.md#terraform).

## Does pogi run Terraform to price my code?

No. `pogi cost` reads your `.tf` files, or a `plan.json` you made, and never runs Terraform, a provider or a script. See [How Terraform cost works](how-terraform-cost-works.md).

## Does pogi send my Terraform files anywhere?

No. `pogi cost` reads them on your machine. In the console, the browser sends them only to your own pogi, which reads them in memory and keeps nothing.

## Does pogi phone home for prices?

No. The prices are built into the program, and pogi never fetches them. The date of each snapshot is in every estimate. New prices come with new releases.

## Can AI coding agents use it?

Yes. pogi is an MCP server (MCP is the protocol AI coding agents use to call tools). Add it with `claude mcp add pogi -- pogi mcp`. See [CLI](../reference/cli.md#pogi-mcp-run-the-mcp-server-for-ai-agents).

## Something doesn't work. What do I check?

Run `pogi doctor`. It checks the server, the runtimes, Docker and pogi's own certificate authority (CA). [Troubleshoot](../how-to/troubleshoot.md) covers the common problems.

## See also

- [How Terraform cost works](how-terraform-cost-works.md)
- [How pogi works](how-pogi-works.md)
- [Install pogi](../how-to/install.md)
- [Troubleshoot](../how-to/troubleshoot.md)
