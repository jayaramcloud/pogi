---
title: Use key-value and document databases
description: Store and read items with DynamoDB, Firestore, Azure Table Storage and Cosmos DB against pogi.
---

# Use key-value and document databases

pogi keeps your items in tables and collections, so you can write to them and read them back as you would on the real cloud. You'll make a DynamoDB table and store entities in Azure Table Storage. Firestore and Cosmos DB work through their client libraries.

## Before you start

1. Start pogi in another terminal:

   ```bash
   pogi start
   ```

2. Load the settings that point your tools at pogi:

   ```bash
   eval "$(pogi env)"
   ```

You need the AWS CLI for DynamoDB and the Azure CLI for Table Storage. No Docker needed.

## AWS: DynamoDB

You'll make a table, put an item, get it by key, and query it.

1. Create a table with a string key called `id`:

   ```bash
   aws dynamodb create-table --table-name orders \
     --attribute-definitions AttributeName=id,AttributeType=S \
     --key-schema AttributeName=id,KeyType=HASH \
     --billing-mode PAY_PER_REQUEST \
     --query TableDescription.TableStatus --output text
   ```

   ```text title="Output"
   ACTIVE
   ```

2. Put an item. It prints nothing:

   ```bash
   aws dynamodb put-item --table-name orders --item '{"id":{"S":"o-1"},"total":{"N":"42"}}'
   ```

3. Get the item by its key:

   ```bash
   aws dynamodb get-item --table-name orders --key '{"id":{"S":"o-1"}}'
   ```

   ```text title="Output"
   {
       "Item": {
           "id": {
               "S": "o-1"
           },
           "total": {
               "N": "42"
           }
       }
   }
   ```

4. Query by key:

   ```bash
   aws dynamodb query --table-name orders --key-condition-expression "id = :id" \
     --expression-attribute-values '{":id":{"S":"o-1"}}' --query Items
   ```

   ```text title="Output"
   [
       {
           "id": {
               "S": "o-1"
           },
           "total": {
               "N": "42"
           }
       }
   ]
   ```

## Google Cloud: Firestore

pogi serves Firestore over gRPC, which is what the Google Cloud client libraries use. They find pogi through `FIRESTORE_EMULATOR_HOST`, which `pogi env` sets, so your code needs no change. pogi does not answer Firestore's REST calls for documents, so `curl` and `gcloud` can't write documents: use a client library. See [Connect your tools](connect-your-tools.md#google-cloud). After your code writes a document, `pogi ls firestore` lists its collections and documents.

## Azure: Table Storage

You'll make a table, insert two entities, read one by key, and query a partition. Table Storage runs on its own port, 10002. The connection string from `pogi env` already points the Azure CLI there.

1. Create a table:

   ```bash
   az storage table create --name orders
   ```

   ```text title="Output"
   {
     "created": true
   }
   ```

2. Insert two entities. Each has a partition key and a row key. The commands print nothing:

   ```bash
   az storage entity insert --table-name orders --entity PartitionKey=eu RowKey=1 total=42 --output none
   az storage entity insert --table-name orders --entity PartitionKey=eu RowKey=2 total=7 --output none
   ```

3. Read one entity by its keys:

   ```bash
   az storage entity show --table-name orders --partition-key eu --row-key 1 --query '{partition:PartitionKey,row:RowKey,total:total}'
   ```

   ```text title="Output"
   {
     "partition": "eu",
     "row": "1",
     "total": 42
   }
   ```

4. Query the partition:

   ```bash
   az storage entity query --table-name orders --filter "PartitionKey eq 'eu'" --query 'items[].RowKey'
   ```

   ```text title="Output"
   [
     "1",
     "2"
   ]
   ```

## Azure: Cosmos DB

pogi serves Cosmos DB over HTTPS on the main port, and it works through the Cosmos DB SDKs only. `pogi env` sets `COSMOS_ENDPOINT` and `COSMOS_KEY`, and the SDK needs to trust pogi's certificate authority (run `pogi cert` for how). See [Connect your tools](connect-your-tools.md#azure).

## Limits

- DynamoDB: PartiQL and import and export are not supported yet.
- Firestore: vector search and security rules are not supported yet. pogi answers Firestore over gRPC only.
- Table Storage: `$filter` functions and Atom payloads are not supported yet.
- Cosmos DB: stored procedures run nothing. .NET binary batches and spatial and vector functions are not supported yet.

See the [DynamoDB](../reference/coverage.md#aws), [Firestore](../reference/coverage.md#google-cloud), [Table Storage and Cosmos DB](../reference/coverage.md#azure) rows in the coverage page.

## See also

- [Connect your tools](connect-your-tools.md)
- [Use any service](use-any-service.md)
- [Coverage](../reference/coverage.md)
