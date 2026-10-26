---
title: Run events and workflows
description: Route events, run state machines and workflows, and queue timed tasks with EventBridge, Step Functions, Cloud Tasks, Cloud Scheduler, Workflows and Event Grid against pogi.
---

# Run events and workflows

pogi delivers events, runs state machines and workflows, and dispatches queued tasks, so you can test the logic that connects your services. You'll send an event through EventBridge to a queue, run a Step Functions state machine, run Google Cloud tasks, schedules and workflows, and publish to an Azure Event Grid topic.

## Before you start

1. Start pogi in another terminal:

   ```bash
   pogi start
   ```

2. Load the settings that point your tools at pogi. The lines after `eval` sign the Azure CLI (`az`) in to pogi, for the Event Grid section. [Azure CLI](connect-your-tools.md#azure-cli) explains them, and you can run them again:

   ```bash
   eval "$(pogi env)"
   export REQUESTS_CA_BUNDLE="$HOME/.pogi/certs/ca.pem"   # pogi's CA; `pogi cert` prints its path
   export AZURE_CONFIG_DIR="$HOME/.pogi/azure"            # keeps your own ~/.azure as it is
   az cloud show -n pogi > /dev/null 2>&1 || az cloud register -n pogi --endpoint-resource-manager https://localhost:7644/
   az cloud set -n pogi --only-show-errors
   az login --service-principal -u 00000000-0000-0000-0000-000000000001 -p secret \
     --tenant 00000000-0000-0000-0000-000000000000 --output none
   ```

You need the AWS CLI, `gcloud`, the Azure CLI and `curl`. No Docker needed.

## AWS: EventBridge

You'll make a rule that sends matching events to an SQS queue, put one event that matches and one that doesn't, and receive the match.

1. Create a queue and read its ARN (the ID AWS gives a resource):

   ```bash
   QUEUE_URL=$(aws sqs create-queue --queue-name matches --query QueueUrl --output text)
   QUEUE_ARN=$(aws sqs get-queue-attributes --queue-url $QUEUE_URL --attribute-names QueueArn --query Attributes.QueueArn --output text)
   echo $QUEUE_ARN
   ```

   ```text title="Output"
   arn:aws:sqs:us-east-1:000000000000:matches
   ```

2. Make a rule for events from `demo.app`, and point it at the queue:

   ```bash
   aws events put-rule --name demo-rule --event-pattern '{"source":["demo.app"]}'
   aws events put-targets --rule demo-rule --targets "Id=1,Arn=$QUEUE_ARN"
   ```

   ```text title="Output"
   {
       "RuleArn": "arn:aws:events:us-east-1:000000000000:rule/demo-rule"
   }
   {
       "FailedEntryCount": 0,
       "FailedEntries": []
   }
   ```

3. Put two events. Only the first matches the rule:

   ```bash
   aws events put-events --entries '[{"Source":"demo.app","DetailType":"order","Detail":"{\"id\":1}"}]'
   aws events put-events --entries '[{"Source":"other","DetailType":"order","Detail":"{\"id\":2}"}]'
   ```

   ```text title="Output"
   {
       "FailedEntryCount": 0,
       "Entries": [
           {
               "EventId": "46ac0520-8442-476f-9074-66ca23e235f2"
           }
       ]
   }
   {
       "FailedEntryCount": 0,
       "Entries": [
           {
               "EventId": "c85a68e1-9158-4191-9ef4-b5cc91d012bb"
           }
       ]
   }
   ```

4. Receive from the queue. One message arrived:

   ```bash
   aws sqs receive-message --queue-url $QUEUE_URL --wait-time-seconds 2 --max-number-of-messages 10 --query 'Messages[].Body' --output text
   ```

   ```text title="Output"
   {"version":"0","id":"46ac0520-8442-476f-9074-66ca23e235f2","detail-type":"order","source":"demo.app","account":"000000000000","time":"2026-10-26T08:55:55Z","region":"us-east-1","resources":[],"detail":{"id":1}}
   ```

## AWS: Step Functions

You'll make a state machine with a Choice state that picks a branch by order size, run it, and read the result.

1. Write the definition. Orders over 100 take the `Big` branch:

   ```bash
   cat > machine.json <<'JSON'
   {
     "StartAt": "Check",
     "States": {
       "Check": {
         "Type": "Choice",
         "Choices": [{"Variable": "$.total", "NumericGreaterThan": 100, "Next": "Big"}],
         "Default": "Small"
       },
       "Big": {"Type": "Pass", "Result": "big order", "End": true},
       "Small": {"Type": "Pass", "Result": "small order", "End": true}
     }
   }
   JSON
   ```

2. Create the state machine. The role can be any ARN:

   ```bash
   MACHINE=$(aws stepfunctions create-state-machine --name orders --definition file://machine.json \
     --role-arn arn:aws:iam::000000000000:role/sfn --query stateMachineArn --output text)
   echo $MACHINE
   ```

   ```text title="Output"
   arn:aws:states:us-east-1:000000000000:stateMachine:orders
   ```

3. Start an execution with a total of 42:

   ```bash
   EXECUTION=$(aws stepfunctions start-execution --state-machine-arn $MACHINE --input '{"total":42}' --query executionArn --output text)
   ```

4. Describe it:

   ```bash
   aws stepfunctions describe-execution --execution-arn $EXECUTION --query '{status:status,output:output}'
   ```

   ```text title="Output"
   {
       "status": "SUCCEEDED",
       "output": "\"small order\""
   }
   ```

## Google Cloud: Cloud Tasks and Cloud Scheduler

You'll make a task queue whose task calls pogi's Pub/Sub API, so you can see the task run, and then a scheduled job. Name the location in each command: without an App Engine app, `gcloud` asks for `--location`, as it does on Google Cloud.

1. Make a topic and a subscription for the task to publish to:

   ```bash
   gcloud pubsub topics create jobs
   gcloud pubsub subscriptions create jobs-sub --topic=jobs
   ```

   ```text title="Output"
   Created topic [projects/pogi/topics/jobs].
   Created subscription [projects/pogi/subscriptions/jobs-sub].
   ```

2. Make a queue:

   ```bash
   gcloud tasks queues create orders --location=us-central1
   ```

   ```text title="Output"
   Created queue [us-central1/orders].
   ```

3. Add an HTTP task that publishes the message `hello` (`aGVsbG8=` in base64) to the topic:

   ```bash
   gcloud tasks create-http-task --queue=orders --location=us-central1 \
     --url=http://localhost:7644/v1/projects/pogi/topics/jobs:publish --method=POST \
     --header=Content-Type:application/json --body-content='{"messages":[{"data":"aGVsbG8="}]}'
   ```

   ```text title="Output"
   Created task [projects/pogi/locations/us-central1/queues/orders/tasks/2068978760639546097].
   ```

4. Pull the message the task published:

   ```bash
   sleep 3
   gcloud pubsub subscriptions pull jobs-sub --auto-ack
   ```

   ```text title="Output"
   ┌───────┬────────────┬──────────────┬────────────┬──────────────────┬────────────┐
   │  DATA │ MESSAGE_ID │ ORDERING_KEY │ ATTRIBUTES │ DELIVERY_ATTEMPT │ ACK_STATUS │
   ├───────┼────────────┼──────────────┼────────────┼──────────────────┼────────────┤
   │ hello │ 1          │              │            │                  │ SUCCESS    │
   └───────┴────────────┴──────────────┴────────────┴──────────────────┴────────────┘
   ```

5. Make a job that runs every day at 03:00, and list it:

   ```bash
   gcloud scheduler jobs create http nightly --schedule="0 3 * * *" --uri=http://localhost:7644/_localstack/health --location=us-central1 \
     --format='value(name,state)'
   gcloud scheduler jobs list --location=us-central1
   ```

   ```text title="Output"
   projects/pogi/locations/us-central1/jobs/nightly	ENABLED
   ID       LOCATION     SCHEDULE (TZ)        TARGET_TYPE  STATE
   nightly  us-central1  0 3 * * * (Etc/UTC)  HTTP         ENABLED
   ```

## Google Cloud: Workflows

You'll deploy a workflow that builds a greeting, run it with an argument, and read the result.

1. Write the workflow:

   ```bash
   cat > workflow.yaml <<'YAML'
   main:
     params: [args]
     steps:
       - greet:
           assign:
             - message: ${"Hello, " + args.name}
       - done:
           return: ${message}
   YAML
   ```

2. Deploy it:

   ```bash
   gcloud workflows deploy hello --source=workflow.yaml --location=us-central1 --format='value(name,state)'
   ```

   ```text title="Output"
   Waiting for operation [operation-7f177d3e016e8b0e08787785c30570dd] to complete...
   ....done.
   projects/pogi/locations/us-central1/workflows/hello	ACTIVE
   ```

3. Run it:

   ```bash
   gcloud workflows run hello --location=us-central1 --data='{"name":"pogi"}'
   ```

   ```text title="Output"
   Waiting for execution [573527d5-f630-1041-4edb-0a5d85318453] to complete...
   done.
   argument: '{"name":"pogi"}'
   duration: 0.000081s
   endTime: '2026-10-26T08:56:05.825582Z'
   name: projects/pogi/locations/us-central1/workflows/hello/executions/573527d5-f630-1041-4edb-0a5d85318453
   result: '"Hello, pogi"'
   startTime: '2026-10-26T08:56:05.825501Z'
   state: SUCCEEDED
   workflowRevisionId: 000001-9b3
   ```

## Azure: Event Grid

You'll make a custom topic with a subscription that delivers to a Storage queue, publish an event, and read it from the queue. The topic and subscription are Azure Resource Manager (ARM) calls, so `az` makes them. The queue is Azure Storage, which uses the connection string from `pogi env`.

1. Make a resource group and a topic:

   ```bash
   az group create --name demo --location eastus --output none
   az eventgrid topic create --resource-group demo --name orders --location eastus --query '{name:name,state:provisioningState,endpoint:endpoint}'
   ```

   ```json title="Output"
   {
     "endpoint": "https://orders.eventgrid.localhost:7644/api/events",
     "name": "orders",
     "state": "Succeeded"
   }
   ```

2. Make the queue, then subscribe it to the topic. `az` splits the destination into ARM's two properties: `resourceId` is the storage account, and `queueName` sits beside it:

   ```bash
   az storage queue create --name events --only-show-errors --output none
   SUB=$(az account show --query id --output tsv)
   az eventgrid event-subscription create --name to-queue \
     --source-resource-id "/subscriptions/$SUB/resourceGroups/demo/providers/Microsoft.EventGrid/topics/orders" \
     --endpoint-type storagequeue \
     --endpoint "/subscriptions/$SUB/resourceGroups/demo/providers/Microsoft.Storage/storageAccounts/devstoreaccount1/queueServices/default/queues/events" \
     --query '{name:name,state:provisioningState}'
   ```

   ```json title="Output"
   {
     "name": "to-queue",
     "state": "Succeeded"
   }
   ```

3. Publish an event to the topic's own address, the `endpoint` from step 1. pogi's certificate covers it, so `curl` needs `--cacert`. Any `aeg-sas-key` works. The command prints the HTTP status:

   ```bash
   curl -s --cacert ~/.pogi/certs/ca.pem \
     -o /dev/null -w "%{http_code}\n" -X POST https://orders.eventgrid.localhost:7644/api/events \
     -H "aeg-sas-key: any" -H "content-type: application/json" \
     -d '[{"id":"1","eventType":"order.created","subject":"orders/1","eventTime":"2026-10-25T00:00:00Z","data":{"total":42},"dataVersion":"1.0"}]'
   ```

   ```text title="Output"
   200
   ```

4. Give pogi a moment to deliver, then read the queue. The event is base64 text in `content`, so decode it:

   ```bash
   sleep 2
   az storage message get --queue-name events --query '[0].content' --output tsv --only-show-errors | base64 --decode; echo
   ```

   ```text title="Output"
   {"id":"1","topic":"/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/demo/providers/Microsoft.EventGrid/topics/orders","subject":"orders/1","eventType":"order.created","eventTime":"2026-10-25T00:00:00Z","data":{"total":42},"dataVersion":"1.0","metadataVersion":"1"}
   ```

## Limits

- EventBridge: Pipes are a separate service (see the coverage page). A schedule with no input delivers `{}`, where AWS sends a default notification.
- Step Functions: distributed Map and redrive are not supported yet. Every real wait, a Wait state or a retry backoff, ends after at most 60 seconds, so a Wait of an hour ends after one minute.
- Cloud Tasks and Cloud Scheduler: App Engine tasks and targets, queue-level HTTP overrides and cron macros are not supported yet.
- Workflows: `parallel` steps, connectors (`googleapis.*`), callbacks and most of the longer tail of standard library functions are not supported yet. A step pogi can't run fails the execution with an error that names it. `sys.log` output is dropped and doesn't reach Cloud Logging.
- Event Grid: hybrid connections and input schema mapping are not supported yet.

See the [EventBridge and Scheduler](../reference/coverage.md#aws), [Step Functions](../reference/coverage.md#aws), [Cloud Tasks, Cloud Scheduler and Workflows](../reference/coverage.md#google-cloud) and [Event Grid](../reference/coverage.md#azure) rows in the coverage page.

## See also

- [Connect your tools](connect-your-tools.md)
- [Read flows and the map](read-flows-and-the-map.md)
- [Use queues and topics](use-queues-and-topics.md)
- [Run functions and HTTP APIs](run-functions-and-http-apis.md)
- [Coverage](../reference/coverage.md)
