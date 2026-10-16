---
title: What is pogi
description: One program that acts like AWS, Google Cloud and Azure, on your own computer.
---

# What is pogi

pogi is one program that stands in for AWS, Google Cloud and Azure. Your code, the cloud CLIs and tools like Terraform talk to it on `localhost` instead of the real clouds. All three clouds answer on one port, `7644` ("pogi" on a phone keypad).

It is free, MIT licensed and works offline. There is no account and no license key. This is version v0.1.0.

Run pogi, point your tools at it, use them as usual, and open the web console:

```bash
pogi start
eval "$(pogi env)"
aws s3 mb s3://demo
open http://localhost:7644/_pogi/
```

## Start here

- **[Install pogi](how-to/install.md)**
- **[First run: your first calls in about five minutes](tutorials/first-run.md)**
- **[Coming from LocalStack? Migrate in a few steps](how-to/migrate-from-localstack.md)**

| Kind | Use it to | Pages |
|---|---|---|
| Tutorials | Learn pogi by doing. | [First run](tutorials/first-run.md), [Least privilege](tutorials/least-privilege.md) |
| How-to | Get one job done. | [Install pogi](how-to/install.md), [Troubleshoot](how-to/troubleshoot.md), [Connect your tools](how-to/connect-your-tools.md), [Switch IAM modes](how-to/switch-iam-modes.md), [Inject chaos](how-to/inject-chaos.md), [Save and load snapshots](how-to/save-and-load-snapshots.md), [Estimate cost](how-to/estimate-cost.md), [Read flows and the map](how-to/read-flows-and-the-map.md), [Migrate from LocalStack](how-to/migrate-from-localstack.md) |
| Reference | Look up a command, setting or endpoint. | [CLI](reference/cli.md), [Configuration](reference/configuration.md), [REST API](reference/rest-api.md), [Console](reference/console.md), [Coverage](reference/coverage.md), [AWS long-tail services](reference/aws-long-tail.md), [LocalStack to pogi mapping](reference/localstack-mapping.md), [pogi compared](reference/compared.md) |
| Explanation | Understand why it works this way. | [How pogi works](explanation/how-pogi-works.md), [Flows and the Map](explanation/flows-and-the-map.md), [IAM: record, then enforce](explanation/iam.md), [FAQ](explanation/faq.md) |

To build pogi, fix a bug or add a service, see [CONTRIBUTING.md](../CONTRIBUTING.md).
