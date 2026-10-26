---
title: Connect your tools
description: Point SDKs, the cloud CLIs, Terraform and the CDK at pogi.
---

# Connect your tools

pogi works with the AWS, Google Cloud and Azure tools you already use. Point them at it with the steps below.

Before you start, run `pogi start`. The commands below find pogi at `http://localhost:7644`. If it runs on another port, pass `--endpoint` to client commands or set `POGI_ENDPOINT`.

## SDKs and CLIs

`pogi env` prints the settings that point these tools at pogi: the AWS CLI and SDKs, `gcloud` and the Google Cloud client libraries, and the Azure Storage, Cosmos DB and Key Vault clients. Terraform has its own file, covered under [Terraform](#terraform).

Load the settings into your shell. Pick the tab for your shell:

```bash tab="sh (bash, zsh)"
eval "$(pogi env)"
```

```fish tab="fish"
pogi env --shell fish | source
```

```powershell tab="PowerShell"
pogi env --shell pwsh | Invoke-Expression
```

```bash tab="dotenv file"
pogi env --shell dotenv > .env
```

| Shell | Output |
|---|---|
| `sh` | `export NAME="value"` lines. |
| `fish` | `set -gx NAME "value";` lines. |
| `pwsh` | `$env:NAME="value"` lines. `Invoke-Expression` runs them in your session. |
| `dotenv` | `NAME=value` lines, for tools that read a `.env` file. |

Then use your tools as usual. The sections below say what `pogi env` sets for each cloud.

### AWS

`pogi env` sets `AWS_ENDPOINT_URL`, `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` (both `test`), `AWS_DEFAULT_REGION` and `AWS_REGION`. Every current AWS SDK and the AWS CLI read `AWS_ENDPOINT_URL`, so your code does not change. This Go program lists buckets with no pogi-specific code:

```go
package main

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func main() {
	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx) // reads AWS_ENDPOINT_URL and the test credentials
	if err != nil {
		panic(err)
	}
	out, err := s3.NewFromConfig(cfg).ListBuckets(ctx, &s3.ListBucketsInput{})
	if err != nil {
		panic(err)
	}
	for _, b := range out.Buckets {
		fmt.Println(*b.Name)
	}
}
```

With a bucket called `demo`, it prints:

```text title="Output"
demo
```

To run the AWS CLI without loading the variables, use `pogi aws`. It needs the AWS CLI installed.

```bash
pogi aws s3 ls
```

### Google Cloud

`pogi env` sets `STORAGE_EMULATOR_HOST`, `PUBSUB_EMULATOR_HOST`, `FIRESTORE_EMULATOR_HOST`, `DATASTORE_EMULATOR_HOST`, `SPANNER_EMULATOR_HOST`, `BIGTABLE_EMULATOR_HOST` and, for the Firebase Admin SDK, `FIREBASE_AUTH_EMULATOR_HOST`. The client libraries read them. It also sets the `gcloud` settings (`CLOUDSDK_API_ENDPOINT_OVERRIDES_*`, project `pogi`), so `gcloud` talks to pogi.

Some Google APIs have the same paths as others. Memorystore and Filestore both use `/v1/projects/<p>/locations/<l>/instances`, for example. pogi tells them apart by a path prefix: the API's name. For these APIs, include the prefix in the endpoint you give a client library:

```go
redis.NewService(ctx, option.WithEndpoint("http://localhost:7644/redis/"), option.WithoutAuthentication())
```

`pogi env` and `pogi tf --cloud gcp` already add the prefixes for gcloud and Terraform. A gRPC client needs no prefix: `localhost:7644` is enough. The [coverage page](../reference/coverage.md) shows each API's path.

### Azure

`pogi env` sets `AZURE_STORAGE_CONNECTION_STRING` (Azurite ports 10000 to 10002). It also sets `COSMOS_ENDPOINT`, `COSMOS_KEY` and `AZURE_KEYVAULT_URL` for the HTTPS port, plus the managed identity and Microsoft Entra ID settings (`IDENTITY_ENDPOINT`, `AZURE_AUTHORITY_HOST` and `ARM_METADATA_HOSTNAME`). HTTPS services use pogi's own certificate authority (CA). Run `pogi cert` to see where it is and how to trust it.

#### Azure CLI

The Azure CLI (`az`) signs in to pogi with a service principal (an app identity) once you register pogi as a cloud. `pogi env` sets `AZURE_CORE_INSTANCE_DISCOVERY=false`, so `az login` doesn't ask Microsoft's sign-in service about pogi's address first. `az` also has to trust pogi's CA through `REQUESTS_CA_BUNDLE`. `pogi env` leaves that one out, because it replaces the trusted list of every Python tool that reads it. Set it only in the shell where you run `az`:

```bash
eval "$(pogi env)"
export REQUESTS_CA_BUNDLE="$HOME/.pogi/certs/ca.pem"   # pogi's CA; `pogi cert` prints its path
export AZURE_CONFIG_DIR="$HOME/.pogi/azure"            # keeps your own ~/.azure as it is
az cloud show -n pogi > /dev/null 2>&1 || az cloud register -n pogi --endpoint-resource-manager https://localhost:7644/
az cloud set -n pogi --only-show-errors
az login --service-principal -u 00000000-0000-0000-0000-000000000001 -p secret \
  --tenant 00000000-0000-0000-0000-000000000000 --output none
```

You can run the block again: it registers the cloud only the first time.

Any client ID and secret work. The tenant is pogi's own. After that, Azure Resource Manager (ARM) commands such as `az group create`, `az aks create` and `az aks get-credentials` talk to pogi. Your own `~/.azure` doesn't change, so a new shell talks to real Azure again. pogi doesn't serve the browser sign-in, so plain `az login` doesn't work.

### S3 with bucket names in the host

Some code needs the bucket in the host name, as in `<bucket>.s3.aws.localhost`.

1. Start pogi with a host name:

   ```bash
   pogi start --hostname aws.localhost
   ```

2. Print matching settings:

   ```bash
   pogi env --hostname aws.localhost
   ```

   ```text title="Output"
   export AWS_ENDPOINT_URL="http://aws.localhost:7644"
   …
   export AWS_ENDPOINT_URL_S3="http://s3.aws.localhost:7644"
   …
   ```

   To load them, run `eval "$(pogi env --hostname aws.localhost)"`.

`pogi doctor` tells you whether `*.localhost` names resolve on your machine.

## Terraform

`pogi tf` prints a Terraform provider block (the settings that tell Terraform which cloud to call) with every endpoint pointed at pogi.

1. Write it next to your own files. Terraform loads `*_override.tf` files over your own settings, so your configuration stays as it is:

   ```bash
   pogi tf > pogi_override.tf
   ```

   The file starts like this:

   ```hcl title="pogi_override.tf"
   # Generated by "pogi tf": point the AWS provider at pogi.
   provider "aws" {
     region                      = "us-east-1"
     access_key                  = "test"
     secret_key                  = "test"
     skip_credentials_validation = true
     skip_metadata_api_check     = true
     skip_requesting_account_id  = true
     s3_use_path_style           = true
   ```

2. Run Terraform:

   ```bash
   terraform init
   terraform apply
   ```

For the other clouds:

```bash
pogi tf --cloud gcp > pogi_override.tf
pogi tf --cloud azure > pogi_override.tf
```

| Flag | Meaning |
|---|---|
| `--cloud` | `aws` (default), `gcp` or `azure`. |
| `--region` | Region. Default `us-east-1` for AWS, `us-central1` for Google Cloud. |
| `--project` | Google Cloud project. Default `pogi`. |
| `--endpoint` | pogi's address. |
| `--metadata-host` | `host:port` for the `azurerm` provider. |

The `azurerm` provider needs HTTPS. The Azure output starts with comments that show how to run Terraform in a container that trusts pogi's CA.

## CDK

The AWS CDK works without a wrapper, and custom resources work, including the CDK provider framework.

1. Set `AWS_ENDPOINT_URL` and the test credentials in your shell:

   ```bash
   eval "$(pogi env)"
   ```

2. Run `pogi lambda-sdks` once. It gives native Lambda functions the AWS SDKs that real Lambda runtimes include: the JavaScript v3 SDK from `npm` and `boto3` from `pip`, in pogi's data folder. You need Node.js and Python on your machine.

   ```bash
   pogi lambda-sdks
   ```

3. Bootstrap and deploy:

   ```bash
   cdk bootstrap
   cdk deploy
   ```

## Go tests

1. Start pogi inside the test, and set the environment for the code under test:

   ```go
   srv := pogi.Start(t)  // in-process on a random port, cleaned up with the test
   srv.Setenv(t)         // AWS_ENDPOINT_URL, *_EMULATOR_HOST, Azure settings for code under test
   ```

2. Build your client as usual. It now points at pogi:

   ```go
   cfg, _ := config.LoadDefaultConfig(ctx)
   ```

`pogi.Start(t)` runs everything inside the test process, with no container. With `pogi.Start(t, cfg)` and `cfg, _ := pogi.LoadConfig("testdata/pogi.yaml")`, it also creates what the `pogi.yaml` file lists.

With Testcontainers, run the `ghcr.io/nrndev/pogi` image, expose port 7644, and wait for the log line `Ready.` or for `/_localstack/health`.

## Check that it works

1. Check that pogi is up:

   ```bash
   pogi status
   ```

2. Follow every call your tools make:

   ```bash
   pogi logs -f
   ```

If a call does not show up, your tool is not using the endpoint from `pogi env`. See [Troubleshoot](troubleshoot.md).

## See also

- [CLI](../reference/cli.md#pogi-tf-print-a-terraform-provider-block)
- [Configuration](../reference/configuration.md)
- [Install pogi](install.md)
- [Troubleshoot](troubleshoot.md)
