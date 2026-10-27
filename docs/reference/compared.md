---
title: pogi compared
description: Features of pogi, LocalStack and floci side by side, with a source for each claim about the other two.
---

# pogi compared

LocalStack and floci are two other local AWS emulators. This table compares features only. Every LocalStack and floci cell links to its source, and "Paid plan" names the tier. It makes no speed or size claims, because pogi has not measured other tools.

| | pogi | LocalStack | floci |
|---|---|---|---|
| **Clouds** | AWS, Google Cloud and Azure, in one program on one port. | AWS. Azure in preview. No Google Cloud. ([source](https://www.localstack.cloud/localstack-for-azure), [2](https://www.localstack.cloud/pricing)) | AWS. Azure (29 services) and Google Cloud (27 services) as separate emulators on other ports, and an Oracle Cloud emulator. ([source](https://floci.io)) |
| **Account or sign-in** | None. | Account and auth token required, even on the free plan. ([source](https://blog.localstack.cloud/localstack-for-aws-release-2026-03-0/), [2](https://docs.localstack.cloud/aws/getting-started/auth-token/)) | None. ([source](https://github.com/floci-io/floci)) |
| **How it runs** | One binary. A Docker image is also published. | Docker container. ([source](https://github.com/localstack/localstack)) | Docker image. A native binary can be built. ([source](https://floci.io/floci/getting-started/installation/)) |
| **License** | MIT, open source. | No open-source license stated for the current product. ([source](https://github.com/localstack/localstack)) | MIT, open source. ([source](https://github.com/floci-io/floci)) |
| **Web console** | Built in, served by pogi itself. | Hosted web app that connects to your local container. ([source](https://docs.localstack.cloud/aws/connecting/console/resource-browser/)) | Local, as a sidecar container. ([source](https://floci.io/floci/ui/)) |
| **IAM enforcement** | Built in: off, soft or strict. | Paid plan: Base and up. ([source](https://www.localstack.cloud/pricing)) | Opt-in, AWS. ([source](https://floci.io/floci/services/iam/)) |
| **IAM policy written for you** | Built in, for AWS, Google Cloud and Azure. | Paid plan: Ultimate, in preview (IAM Policy Stream). ([source](https://docs.localstack.cloud/aws/capabilities/security-testing/iam-policy-stream/)) | Not offered. ([source](https://floci.io/floci/services/iam/)) |
| **Real-cloud cost estimate** | Built in. | Not offered (Cost Explorer only mocks the API). ([source](https://docs.localstack.cloud/aws/services/ce/)) | Not offered. ([source](https://floci.io/floci/services/ce/)) |
| **Chaos testing** | Built in: error and latency rules. | Paid plan: Enterprise. Fault injection through the FIS emulation is on Ultimate and up. ([source](https://www.localstack.cloud/pricing)) | Not offered. ([source](https://floci.io/floci/services/fis/)) |
| **Snapshots and persistence** | Built in: named snapshots, and four storage modes. | Paid plan: Base and up (Cloud Pods are not on Hobby). ([source](https://www.localstack.cloud/pricing), [2](https://docs.localstack.cloud/aws/licensing/)) | Persistence only. ([source](https://github.com/floci-io/floci)) |
| **Terraform and CDK** | Terraform (AWS, Google, azurerm) and the CDK, with a generated provider block (`pogi tf`). | Directly, or through its CLI (`lstk`). `tflocal` and `cdklocal` are deprecated. ([source](https://docs.localstack.cloud/aws/integrations/infrastructure-as-code/terraform/), [2](https://docs.localstack.cloud/aws/integrations/aws-native-tools/aws-cdk/)) | Terraform directly. CDK covered by its tests. ([source](https://floci.io/floci/getting-started/terraform/)) |
| **MCP server** | Built in (`pogi mcp`). | In preview, needs an auth token. ([source](https://github.com/localstack/localstack-mcp-server)) | Not offered. ([source](https://github.com/floci-io/floci)) |
| **Price** | Free. | Free for non-commercial use (Hobby). Base costs $39 per license a month billed yearly, or $45 billed monthly. ([source](https://docs.localstack.cloud/aws/licensing/), [2](https://www.localstack.cloud/pricing)) | Free. ([source](https://floci.io)) |
| **AWS services** | <!-- count:aws -->144 (90 with real behavior, 54 Thin)<!-- /count -->. Across the three clouds, <!-- count:total -->355 services: 144 AWS (90 with real behavior, 54 Thin), 108 Google Cloud (25 with real behavior, 83 Thin) and 103 Azure (30 with real behavior, 73 Thin)<!-- /count -->. | 30+ on the free Hobby plan, 110+ on Ultimate. ([source](https://www.localstack.cloud/pricing)) | 126. ([source](https://floci.io)) |
| **Maturity** | New: version 0.2.0, 2026. | Since 2016. ([source](https://github.com/localstack/localstack)) | New: since 2026. ([source](https://github.com/floci-io/floci)) |

As of October 2026, from each project's own website, docs and pricing. Names belong to their owners.

## See also

- [Migrate from LocalStack](../how-to/migrate-from-localstack.md): the steps to move.
- [LocalStack to pogi mapping](localstack-mapping.md): their settings and tools, and pogi's. pogi keeps LocalStack's health endpoint, init hooks, `Ready.` log line and usual environment variables.
- [How pogi works](../explanation/how-pogi-works.md#why-is-pogi-built-this-way): why pogi is built this way.
