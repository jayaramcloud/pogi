---
title: What is pogi
description: One program that acts like AWS, Google Cloud and Azure, on your own computer.
---

# What is pogi

pogi is one program that stands in for AWS, Google Cloud and Azure. Your code, the cloud CLIs and tools like Terraform talk to it on `localhost` instead of the real clouds. All three clouds answer on one port, 7644 ("pogi" on a phone keypad).

It is free, MIT licensed and works offline. There is no account and no license key.

To try it, start pogi in one terminal. In a second terminal, point your tools at it and make a bucket:

```bash
pogi start
```

```bash
eval "$(pogi env)"
aws s3 mb s3://demo
```

The web console is at `http://localhost:7644/_pogi/`. Open it in your browser to see the bucket and the calls.

## Start here

- **[Install pogi](how-to/install.md)**
- **[First run: your first calls in about five minutes](tutorials/first-run.md)**
- **[Coming from LocalStack? Migrate in a few steps](how-to/migrate-from-localstack.md)**

| Kind | Use it to | Pages |
|---|---|---|
| Tutorials | Learn pogi by doing. | [First run](tutorials/first-run.md), [Least privilege](tutorials/least-privilege.md), [Estimate a Terraform bill](tutorials/terraform-cost.md) |
| How-to | Get one job done. | [Install pogi](how-to/install.md), [Troubleshoot](how-to/troubleshoot.md), [Connect your tools](how-to/connect-your-tools.md), [Switch IAM modes](how-to/switch-iam-modes.md), [Inject chaos](how-to/inject-chaos.md), [Save and load snapshots](how-to/save-and-load-snapshots.md), [Estimate cost](how-to/estimate-cost.md), [Estimate Terraform cost](how-to/estimate-terraform-cost.md), [See what a plan changes](how-to/see-what-a-plan-changes.md), [Read flows and the map](how-to/read-flows-and-the-map.md), [Migrate from LocalStack](how-to/migrate-from-localstack.md) |
| Reference | Look up a command, setting or endpoint. | [CLI](reference/cli.md), [Configuration](reference/configuration.md), [REST API](reference/rest-api.md), [Console](reference/console.md), [Terraform cost](reference/terraform-cost.md), [Coverage](reference/coverage.md), [AWS long-tail services](reference/aws-long-tail.md), [LocalStack to pogi mapping](reference/localstack-mapping.md), [pogi compared](reference/compared.md) |
| Explanation | Understand why it works this way. | [How pogi works](explanation/how-pogi-works.md), [Flows and the Map](explanation/flows-and-the-map.md), [How Terraform cost works](explanation/how-terraform-cost-works.md), [IAM: record, then enforce](explanation/iam.md), [FAQ](explanation/faq.md) |

## Use a service

Each guide gets one kind of service working, with the commands for AWS, Google Cloud and Azure and the output they print.

- [Use any service](how-to/use-any-service.md), including the Thin ones: Route 53, Cloud DNS, Azure DNS and the AWS long tail
- [Use object storage](how-to/use-object-storage.md): S3, Cloud Storage and Blob Storage
- [Use queues and topics](how-to/use-queues-and-topics.md): SQS, SNS, Pub/Sub and Queue Storage
- [Use key-value and document databases](how-to/use-nosql-databases.md): DynamoDB, Firestore and Table Storage
- [Store secrets and keys](how-to/store-secrets-and-keys.md): Secrets Manager, Parameter Store, KMS, Secret Manager, Cloud KMS and Key Vault
- [Run functions and HTTP APIs](how-to/run-functions-and-http-apis.md): Lambda, API Gateway, Cloud Functions and Azure Functions
- [Run events and workflows](how-to/run-events-and-workflows.md): EventBridge, Step Functions, Cloud Tasks, Cloud Scheduler, Workflows and Event Grid
- [Run Kubernetes clusters](how-to/run-kubernetes-clusters.md): EKS, GKE and AKS
- [Run containers and VMs](how-to/run-containers-and-vms.md): ECR, ECS, EC2, Cloud Run, Compute Engine and Container Instances
- [Run SQL databases and caches](how-to/run-sql-databases-and-caches.md): RDS, ElastiCache, Cloud SQL, Memorystore and Azure Database for PostgreSQL
- [Sign users in](how-to/sign-users-in.md): Cognito, Firebase Auth and managed identity tokens
- [Read logs and metrics](how-to/read-logs-and-metrics.md): CloudWatch and Cloud Logging
- [Deploy a CloudFormation stack](how-to/deploy-a-cloudformation-stack.md), with or without SAM

To build pogi, fix a bug or add a service, see [CONTRIBUTING.md](../CONTRIBUTING.md).
