---
title: Use object storage
description: Make a bucket or container, upload a file, list it and download it on AWS, Google Cloud and Azure.
---

# Use object storage

You'll store a file in pogi and get it back, using S3, Cloud Storage or Blob Storage. Each section uses the command-line tool for that cloud.

## Before you start

- Run `pogi start` in another terminal.
- Load the settings that point your tools at pogi:

  ```bash
  eval "$(pogi env)"
  ```

- Install the tools for the sections you use: the AWS CLI, `gcloud` and the Azure CLI (`az`). Azure Blob Storage listens on port 10000, the same as Azurite.
- Make a file to upload:

  ```bash
  echo "hello from pogi" > hello.txt
  ```

## AWS: S3

You'll make a bucket, upload a file, list it, download it, and fetch it again with a presigned URL (a link that works without credentials for a short time).

1. Make a bucket and upload the file:

   ```bash
   aws s3 mb s3://demo
   aws s3 cp hello.txt s3://demo/hello.txt --no-progress
   ```

   ```text title="Output"
   make_bucket: demo
   upload: ./hello.txt to s3://demo/hello.txt
   ```

2. List the bucket:

   ```bash
   aws s3 ls s3://demo
   ```

   ```text title="Output"
   2026-10-26 00:06:26         16 hello.txt
   ```

3. Download the file under a new name and read it:

   ```bash
   aws s3 cp s3://demo/hello.txt copy.txt --no-progress
   cat copy.txt
   ```

   ```text title="Output"
   download: s3://demo/hello.txt to ./copy.txt
   hello from pogi
   ```

4. Make a presigned URL and fetch it with `curl`:

   ```bash
   URL=$(aws s3 presign s3://demo/hello.txt --expires-in 300)
   curl -s "$URL"
   ```

   ```text title="Output"
   hello from pogi
   ```

## Google Cloud: Cloud Storage

You'll do the same with `gcloud storage`. `--no-user-output-enabled` turns off the progress text that `gcloud` prints while it copies.

1. Make a bucket:

   ```bash
   gcloud storage buckets create gs://demo-gcs
   ```

   ```text title="Output"
   Creating gs://demo-gcs/...
   ```

2. Upload the file. It prints nothing:

   ```bash
   gcloud storage cp hello.txt gs://demo-gcs/hello.txt --no-user-output-enabled
   ```

3. List the bucket:

   ```bash
   gcloud storage ls gs://demo-gcs
   ```

   ```text title="Output"
   gs://demo-gcs/hello.txt
   ```

4. Download the file and read it:

   ```bash
   gcloud storage cp gs://demo-gcs/hello.txt copy-gcs.txt --no-user-output-enabled
   cat copy-gcs.txt
   ```

   ```text title="Output"
   hello from pogi
   ```

## Azure: Blob Storage

`az storage` works with the connection string that `pogi env` sets in `AZURE_STORAGE_CONNECTION_STRING`. It doesn't need `az login`.

1. Make a container:

   ```bash
   az storage container create --name demo
   ```

   ```json title="Output"
   {
     "created": true
   }
   ```

2. Upload the file. `--output none` and `--no-progress` keep it quiet, so it prints nothing:

   ```bash
   az storage blob upload --container-name demo --name hello.txt --file hello.txt --no-progress --output none
   ```

3. List the blobs:

   ```bash
   az storage blob list --container-name demo --query "[].name" --output tsv
   ```

   ```text title="Output"
   hello.txt
   ```

4. Download the blob and read it:

   ```bash
   az storage blob download --container-name demo --name hello.txt --file copy-az.txt --no-progress --output none
   cat copy-az.txt
   ```

   ```text title="Output"
   hello from pogi
   ```

## Limits

- S3: an upload over 10 MiB is refused unless you start pogi with a larger `--max-upload-mb`, because pogi holds object bodies in memory. That holds in persistent mode too.
- S3 replication settings are stored but not acted on. Lifecycle transitions are ignored, and so are website routing rules. S3 Select is not available.
- Cloud Storage does not support IAM conditions or retention policies yet.

Each service's row on the [coverage page](../reference/coverage.md) lists the rest: [S3](../reference/coverage.md#aws), [Cloud Storage](../reference/coverage.md#google-cloud) and [Blob Storage](../reference/coverage.md#azure).

## See also

- [Connect your tools](connect-your-tools.md)
- [Use queues and topics](use-queues-and-topics.md)
- [Use any service](use-any-service.md)
- [Coverage](../reference/coverage.md)
