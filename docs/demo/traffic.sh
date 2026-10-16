#!/bin/sh
# Calls from all three clouds, made in the background while demo.tape records the TUI.
# Needs pogi on PATH and the aws, gcloud and az CLIs.
eval "$(pogi env)"
dir=$(mktemp -d); echo hello > "$dir/hello.txt"; cd "$dir"
sleep 2
aws s3 mb s3://photos >/dev/null 2>&1
aws s3 cp hello.txt s3://photos/ >/dev/null 2>&1
gcloud storage buckets create gs://reports >/dev/null 2>&1
aws sqs create-queue --queue-name orders >/dev/null 2>&1
az storage container create -n invoices >/dev/null 2>&1
aws dynamodb create-table --table-name users --attribute-definitions AttributeName=id,AttributeType=S \
  --key-schema AttributeName=id,KeyType=HASH --billing-mode PAY_PER_REQUEST >/dev/null 2>&1
gcloud pubsub topics create events >/dev/null 2>&1
az storage blob upload -c invoices -f hello.txt -n hello.txt >/dev/null 2>&1
aws sqs send-message --queue-url http://localhost:7644/000000000000/orders --message-body hi >/dev/null 2>&1
gcloud storage cp hello.txt gs://reports/ >/dev/null 2>&1
rm -rf "$dir"
