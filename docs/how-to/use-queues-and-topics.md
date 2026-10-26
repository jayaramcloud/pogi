---
title: Use queues and topics
description: Send and receive messages with SQS, SNS, Pub/Sub and Queue Storage, and find out what Service Bus and Event Hubs need.
---

# Use queues and topics

You'll send a message into pogi and read it back, using a queue on each cloud, plus an SNS topic that delivers to a queue. Each section uses the command-line tool for that cloud.

## Before you start

- Run `pogi start` in another terminal.
- Load the settings that point your tools at pogi:

  ```bash
  eval "$(pogi env)"
  ```

- Install the tools for the sections you use: the AWS CLI, `gcloud` and the Azure CLI (`az`). Azure Queue Storage listens on port 10001, the same as Azurite.

## AWS: SQS and SNS

You'll send a message to an SQS queue and receive it. Then you'll subscribe the queue to an SNS topic, publish to the topic, and see the message arrive in the queue.

### SQS

1. Make a queue:

   ```bash
   QUEUE=$(aws sqs create-queue --queue-name orders --query QueueUrl --output text)
   echo $QUEUE
   ```

   ```text title="Output"
   http://localhost:7644/000000000000/orders
   ```

2. Send a message:

   ```bash
   aws sqs send-message --queue-url $QUEUE --message-body "order 1" --query MessageId --output text
   ```

   ```text title="Output"
   de4a5166-ead0-42bf-9b52-36208fd341c0
   ```

3. Receive it:

   ```bash
   aws sqs receive-message --queue-url $QUEUE --query 'Messages[0].Body' --output text
   ```

   ```text title="Output"
   order 1
   ```

### SNS to SQS

1. Make a topic, and get the queue's ARN (the ID AWS gives a resource):

   ```bash
   TOPIC=$(aws sns create-topic --name events --query TopicArn --output text)
   QARN=$(aws sqs get-queue-attributes --queue-url $QUEUE --attribute-names QueueArn --query Attributes.QueueArn --output text)
   echo $TOPIC
   echo $QARN
   ```

   ```text title="Output"
   arn:aws:sns:us-east-1:000000000000:events
   arn:aws:sqs:us-east-1:000000000000:orders
   ```

2. Subscribe the queue to the topic. Raw delivery puts your message in the queue as it is, without an SNS envelope around it:

   ```bash
   aws sns subscribe --topic-arn $TOPIC --protocol sqs --notification-endpoint $QARN --attributes RawMessageDelivery=true --query SubscriptionArn --output text
   ```

   ```text title="Output"
   arn:aws:sns:us-east-1:000000000000:events:52866301-3662-4ec2-9e77-3aeb62ae78d1
   ```

3. Publish to the topic:

   ```bash
   aws sns publish --topic-arn $TOPIC --message "hello subscribers" --query MessageId --output text
   ```

   ```text title="Output"
   360fbb81-a89b-404d-88a1-0c939549bcf0
   ```

4. Receive the message from the queue:

   ```bash
   aws sqs receive-message --queue-url $QUEUE --query 'Messages[].Body' --output text
   ```

   ```text title="Output"
   hello subscribers
   ```

## Google Cloud: Pub/Sub

You'll make a topic and a subscription (a named reader of the topic), publish a message and pull it.

1. Make a topic and a subscription:

   ```bash
   gcloud pubsub topics create orders
   gcloud pubsub subscriptions create orders-sub --topic=orders
   ```

   ```text title="Output"
   Created topic [projects/pogi/topics/orders].
   Created subscription [projects/pogi/subscriptions/orders-sub].
   ```

2. Publish a message:

   ```bash
   gcloud pubsub topics publish orders --message="order 1"
   ```

   ```text title="Output"
   messageIds:
   - '1'
   ```

3. Pull it. `--auto-ack` acknowledges the message so it is not delivered again:

   ```bash
   gcloud pubsub subscriptions pull orders-sub --auto-ack
   ```

   ```text title="Output"
   ┌─────────┬────────────┬──────────────┬────────────┬──────────────────┬────────────┐
   │   DATA  │ MESSAGE_ID │ ORDERING_KEY │ ATTRIBUTES │ DELIVERY_ATTEMPT │ ACK_STATUS │
   ├─────────┼────────────┼──────────────┼────────────┼──────────────────┼────────────┤
   │ order 1 │ 1          │              │            │                  │ SUCCESS    │
   └─────────┴────────────┴──────────────┴────────────┴──────────────────┴────────────┘
   ```

## Azure: Queue Storage

`az storage` works with the connection string that `pogi env` sets in `AZURE_STORAGE_CONNECTION_STRING`. It doesn't need `az login`. `--only-show-errors` hides the preview warning that `az` prints for these commands.

1. Make a queue:

   ```bash
   az storage queue create --name orders --only-show-errors
   ```

   ```json title="Output"
   {
     "created": true
   }
   ```

2. Put a message on it:

   ```bash
   az storage message put --queue-name orders --content "order 1" --query content --output tsv --only-show-errors
   ```

   ```text title="Output"
   order 1
   ```

3. Get the message:

   ```bash
   az storage message get --queue-name orders --query "[0].content" --output tsv --only-show-errors
   ```

   ```text title="Output"
   order 1
   ```

## Azure: Service Bus and Event Hubs

Service Bus and Event Hubs send and receive over AMQP, which pogi serves on port 5672 when you start it with its default ports. `az` and `curl` can't send or receive on them, so these two need the official Azure SDK. There are no CLI steps for them here. `pogi env` sets no Service Bus or Event Hubs settings. The [coverage page](../reference/coverage.md#azure) lists what each service supports.

## Limits

- Pub/Sub does not support exactly-once delivery yet, and it has no BigQuery or Cloud Storage subscriptions.
- Queue Storage covers messages: put, get, peek, update, delete, visibility and time to live.
- Service Bus stores auto-delete-on-idle and maximum queue size but does not act on them. Event Hubs keeps AMQP and Kafka as separate logs, with a fixed partition count.

The [coverage page](../reference/coverage.md) lists every row: [SQS and SNS](../reference/coverage.md#aws), [Pub/Sub](../reference/coverage.md#google-cloud) and [Azure](../reference/coverage.md#azure).

## See also

- [Connect your tools](connect-your-tools.md)
- [Use object storage](use-object-storage.md)
- [Use any service](use-any-service.md)
- [Coverage](../reference/coverage.md)
