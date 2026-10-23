---
title: REST API
description: Endpoints that scripts call to control a running pogi.
---

# REST API

All paths are on the main port, `http://localhost:7644` by default. Bodies and responses are JSON unless noted. The cloud APIs themselves (S3, SQS, Pub/Sub and so on) are not listed here. Call those with the usual SDKs and CLIs.

pogi's own endpoints do not show up in Activity.

| Method and path | Purpose |
|---|---|
| [`GET /_pogi/cost.json`](#get-the-running-cost) | The monthly bill for what is running. |
| [`GET /_pogi/api/info`](#get-status) | Version, uptime, IAM (who may do what) mode and every service. |
| [`GET /_pogi/api/requests`](#list-requests) | Recent calls, with filters. |
| [`GET /_pogi/api/requests/{id}`](#get-one-request) | One call with headers and bodies. |
| [`GET /_pogi/api/spans`](#list-flow-steps) | Recent steps where one service handed work to another. |
| [`GET /_pogi/api/resources`](#list-resources) | Resource tables of every service, or one. |
| [`GET /_pogi/api/resources/{service}/{table}/{id}`](#get-one-resource) | The detail of one resource. |
| [`POST /_pogi/api/reset`](#reset-state) | Delete state, for all services or some. |
| [`POST /moto-api/reset`](#reset-all-state-moto) | The way moto (a Python AWS mock) and LocalStack reset. |
| [`GET /_pogi/api/snapshots`](#list-snapshots) | Saved snapshots. |
| [`POST /_pogi/api/snapshots/{name}`](#save-a-snapshot) | Save all state under a name. |
| [`POST /_pogi/api/snapshots/{name}/load`](#load-a-snapshot) | Replace the state with a snapshot. |
| [`GET /_pogi/api/snapshots/{name}`](#download-a-snapshot) | Get the snapshot file. |
| [`PUT /_pogi/api/snapshots/{name}`](#import-a-snapshot) | Store a snapshot file under a name. |
| [`DELETE /_pogi/api/snapshots/{name}`](#delete-a-snapshot) | Remove a snapshot. |
| [`GET /_pogi/api/chaos`](#list-chaos-rules) | Active chaos rules. |
| [`POST /_pogi/api/chaos`](#add-a-chaos-rule) | Add a rule. |
| [`DELETE /_pogi/api/chaos/{id}`](#remove-a-chaos-rule) | Remove one rule. |
| [`DELETE /_pogi/api/chaos`](#remove-all-chaos-rules) | Clear every rule. |
| [`GET /_aws/sqs/messages`](#peek-at-sqs-messages) | Read a queue without receiving. |
| [`GET`, `DELETE /_aws/ses`](#list-or-clear-ses-emails) | Emails sent through SES. |
| [`GET`, `DELETE /_aws/sns/sms-messages`](#list-or-clear-sms-messages) | SMS sent through SNS. |
| [`GET`, `DELETE /_aws/sns/platform-endpoint-messages`](#list-or-clear-push-messages) | Push messages sent through SNS. |
| [`GET /_aws/cognito/codes`](#list-cognito-codes) | Codes Cognito "sent". |
| [`GET /_localstack/health`](#get-localstack-health) | LocalStack's health endpoint. |
| [`GET /_localstack/info`](#get-localstack-info) | LocalStack's info endpoint. |
| [`GET /_localstack/init/{stage}`](#get-init-hook-status) | State of the init scripts. Leave out the stage for all four. |
| [`GET /_gcp/gke/kubeconfig/{project}/{location}/{cluster}`](#get-a-gke-kubeconfig) | Kubeconfig for a GKE cluster pogi runs. |
| [`/_pogi/mcp`](#mcp-server) | pogi's MCP endpoint. |

## Same-origin rule

Writes to `/_pogi/api/`, `/_localstack/`, `/moto-api/` and the console's [form posts](console.md#iam-form-posts) get a `403` response when the request carries an `Origin` header that does not match the `Host` header. Tools such as `curl` and server-side code send no `Origin`, so they are not affected. A page in a browser on another origin is.

## Status

### Get status

`GET /_pogi/api/info`

Returns the status of pogi: its version, uptime, IAM mode and services. `pogi status` and `pogi wait` call it.

```bash
curl localhost:7644/_pogi/api/info
```

```json title="Response"
{
 "version": "0.2.0",
 "started": "2026-10-13T23:41:35.879922+05:30",
 "uptime": 15,
 "region": "us-east-1",
 "persist": false,
 "dataDir": "/home/me/.pogi",
 "iam": "off",
 "lambda": "auto",
 "ready": true,
 "services": [
  {"name": "s3", "cloud": "aws", "title": "S3", "resources": 1, "requests": 3},
  {"name": "sqs", "cloud": "aws", "title": "SQS", "resources": 0, "requests": 0}
 ]
}
```

- `uptime` is in seconds.
- `ready` is `true` once pogi serves requests. That is after the `start` init hooks and the seed data in `pogi.yaml` have run.
- `services` has one entry for every enabled service. The example shows two.
- `problems` is there only when something is wrong with pogi as a whole. Each has a `time`, a `text` and a `kind`: `state` for saved state that can't be read or written, `port` for a port that pogi could not open (that part is then off). For example: `{"kind":"port","text":"azblob: port 10000 is in use, so this is off — …"}`.

## Cost

### Get the running cost

`GET /_pogi/cost.json`

The monthly bill for what is running in pogi, the numbers on the Cost page. `pogi cost` with no path calls it. It does not price Terraform: that is `pogi cost <path>`.

| Query | Type | Default | Meaning |
|---|---|---|---|
| `region` | string | the pogi region | The AWS region to price. An unknown region returns `400` with `{"error":"unknown region …"}`. |

```bash
curl localhost:7644/_pogi/cost.json
```

```json title="Response"
{"region":"us-east-1","monthly":0,"basis":"0 requests counted in 1m0s of uptime, as a rate over 30 days","rows":[]}
```

Each row has `cloud`, `service`, `name`, `detail` (what drives the price), `monthly` and `priced`. The console's Terraform tab has its own requests: see [Terraform form posts](console.md#terraform-form-posts).

## Calls and resources

### List requests

`GET /_pogi/api/requests`

Returns the recent calls, oldest first, without headers and bodies. pogi keeps the last 1000.

| Query | Type | Default | Meaning |
|---|---|---|---|
| `limit` | number | 200 | The most calls to return. The newest ones are kept. |
| `after` | number | 0 | Only calls with an `id` above this number. |
| `service` | string | any | Only this service id (the name `pogi services` prints), such as `s3`. A service that is not running, or no service by that name, returns `404` with `{"error":"no enabled service \"x\" (pogi services lists them)"}`. |
| `q` | string | none | Only calls whose service, operation, path or error contains this text, ignoring case. |
| `errors` | any value | off | Only calls with a status of 400 or above. |
| `wait` | seconds | 0 | Wait for a call newer than `after` before responding, at most 60. Use it to follow the log. |

```bash
curl "localhost:7644/_pogi/api/requests?errors=1&limit=1"
```

```json title="Response (trimmed)"
[{"id":3,"time":"2026-10-23T12:09:45.489497+05:30","cloud":"aws","service":"s3","op":"GetObject","method":"GET","path":"/jobs/x","status":404,"ms":0.058,"account":"000000000000","region":"us-east-1","requestId":"b1338d19…","accessKey":"test","principal":"arn:aws:iam::000000000000:root","error":"NoSuchBucket","trace":"1-fd38a21e-ca3441bd8a8502f2ea92b512"}]
```

The fields of an entry:

| Field | Meaning |
|---|---|
| `id` | A number that grows with each call. |
| `time`, `ms` | When the call arrived, and how long it took in milliseconds. |
| `cloud`, `service`, `op` | The cloud, service id and operation. |
| `method`, `path`, `status` | The HTTP method, path and response status. |
| `account`, `region`, `requestId`, `accessKey`, `principal` | Who called, where and as whom. Left out when empty. |
| `error` | The error code, when the call failed. |
| `chaos` | `true` when a [chaos rule](../how-to/inject-chaos.md) changed the call. |
| `iam` | `allow` or `deny: why`, when IAM enforcement is on. |
| `trace` | The flow the call belongs to. See [Flows and the Map](../explanation/flows-and-the-map.md). |

### Get one request

`GET /_pogi/api/requests/{id}`

Returns one call in full. It adds `reqHeaders`, `reqBody` and `respBody`. Secret values show as `[redacted]` and the names stay: the `Authorization` header and other headers that carry a token or key, query values such as `X-Amz-Security-Token` and `key`, and body fields for passwords, tokens, secret values, SecureString and Key Vault values, Lambda environment variables and Google Secret Manager payloads.

```bash
curl localhost:7644/_pogi/api/requests/3
```

```json title="Response (trimmed)"
{"id":3,"service":"s3","op":"GetObject","status":404,"error":"NoSuchBucket","reqHeaders":{"Authorization":"[redacted]","X-Amz-Date":"20261023T063945Z", …},"respBody":"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>NoSuchBucket</Code>…</Error>", …}
```

- A call that has left the log returns `404` with `{"error":"request not found (the log keeps the last 1000)"}`.

### List flow steps

`GET /_pogi/api/spans`

Returns the recent steps that were not calls to pogi, oldest first: a message delivered from one resource to another, a function pogi ran, a message moved to a dead-letter queue, or a notification sent. pogi keeps the last 5000. The Flows tab of the terminal screen (the TUI) reads this list.

| Query | Type | Default | Meaning |
|---|---|---|---|
| `limit` | number | 500 | The most steps to return, from 1 to 5000. The newest ones are kept. Any other value returns `400`. |

```bash
curl "localhost:7644/_pogi/api/spans?limit=1"
```

```json title="Response"
[{"trace":"1-adc0778e-aed8dc55472b20118d59e9bb","time":"2026-10-23T01:52:49.383628+05:30","ms":0.027,"service":"sqs","op":"deliver","from":"arn:aws:sns:us-east-1:000000000000:alerts","to":"arn:aws:sqs:us-east-1:000000000000:orders","kind":"deliver"}]
```

| Field | Meaning |
|---|---|
| `trace` | The flow the step belongs to, the same as a call's `trace`. |
| `time`, `ms` | When the step started, and how long it took in milliseconds. |
| `service`, `op` | The service that did the work, and what it did. |
| `from`, `to` | The resources at each end, as ARNs or full resource names. Left out when empty. |
| `kind` | `deliver`, `invoke`, `redrive` (to a dead-letter queue) or `notify`. |
| `err` | What went wrong, when the step failed. |

### List resources

`GET /_pogi/api/resources`

Returns the resource tables of every service. `pogi ls` calls it.

| Query | Type | Default | Meaning |
|---|---|---|---|
| `service` | string | all | Only this service id. A service that is not running, or no service by that name, returns `404` with `{"error":"no enabled service \"x\" (pogi services lists them)"}`. |

```bash
curl "localhost:7644/_pogi/api/resources?service=s3"
```

```json title="Response"
{"s3":[{"name":"Buckets","columns":["Name","Region","Account","Objects","Size","Versioning","Created"],"rows":[["demo","us-east-1","000000000000","0","0 B","","2026-10-13 18:11:51"]]}]}
```

### Get one resource

`GET /_pogi/api/resources/{service}/{table}/{id}`

Returns the detail of one resource.

```bash
curl localhost:7644/_pogi/api/resources/s3/Buckets/demo
```

```json title="Response (trimmed)"
{"account":"000000000000","config":{"ownershipControls":"<OwnershipControls …>","publicAccessBlock":"<PublicAccessBlockConfiguration …>"},"created":"2026-10-23T06:39:45.030915Z","keys":0,"name":"demo","region":"us-east-1","uploads":0,"versioning":""}
```

- If there is no such resource, the response is `404` with `{"error":"resource not found"}`.

## Reset and snapshots

### Reset state

`POST /_pogi/api/reset`

Deletes state. `pogi reset` calls it. The seed data in `pogi.yaml` is created again afterward.

| Query | Type | Default | Meaning |
|---|---|---|---|
| `service` | string | all | Comma-separated service ids to reset. |

```bash
curl -X POST "localhost:7644/_pogi/api/reset?service=s3,sqs"
```

```json title="Response"
{"reset":["s3","sqs"]}
```

With no `service`, the response is `{"reset":null}`. An unknown service returns `400` with `{"error":"no enabled service …"}`.

### Reset all state (moto)

`POST /moto-api/reset`

Resets every service, the way moto and LocalStack tools expect.

```bash
curl -X POST localhost:7644/moto-api/reset
```

```json title="Response"
{"status":"ok"}
```

### List snapshots

`GET /_pogi/api/snapshots`

Lists the saved snapshots. See [Save and load snapshots](../how-to/save-and-load-snapshots.md).

```bash
curl localhost:7644/_pogi/api/snapshots
```

```json title="Response"
[{"name":"demo","size":13691,"saved":"2026-10-13T23:42:04.620597368+05:30"}]
```

A snapshot name may use letters, digits, `.`, `_` and `-`. The snapshot calls below return `400` with `{"error":"…"}` for a bad name or a failed action.

### Save a snapshot

`POST /_pogi/api/snapshots/{name}`

Saves all state under `{name}`.

```bash
curl -X POST localhost:7644/_pogi/api/snapshots/demo
```

```json title="Response"
{"snapshot":"demo"}
```

### Load a snapshot

`POST /_pogi/api/snapshots/{name}/load`

Replaces the current state with the snapshot `{name}`.

```bash
curl -X POST localhost:7644/_pogi/api/snapshots/demo/load
```

```json title="Response"
{"snapshot":"demo"}
```

### Download a snapshot

`GET /_pogi/api/snapshots/{name}`

Downloads the snapshot file, `{name}.json`.

```bash
curl -o demo.json localhost:7644/_pogi/api/snapshots/demo
```

### Import a snapshot

`PUT /_pogi/api/snapshots/{name}`

Stores the request body, a snapshot file, under `{name}`.

```bash
curl -X PUT --data-binary @demo.json localhost:7644/_pogi/api/snapshots/copy
```

```json title="Response"
{"snapshot":"copy"}
```

### Delete a snapshot

`DELETE /_pogi/api/snapshots/{name}`

Deletes the snapshot.

```bash
curl -X DELETE localhost:7644/_pogi/api/snapshots/copy
```

```json title="Response"
{"snapshot":"copy"}
```

## Chaos

### List chaos rules

`GET /_pogi/api/chaos`

Lists the rules. See [Inject chaos](../how-to/inject-chaos.md).

```bash
curl localhost:7644/_pogi/api/chaos
```

```json title="Response"
[{"id":"f2c47b28","service":"dynamodb","probability":0.25,"latencyMs":200,"hits":0}]
```

### Add a chaos rule

`POST /_pogi/api/chaos`

Adds a rule from a JSON body and returns it with its `id`.

| Field | Type | Default | Meaning |
|---|---|---|---|
| `service` | string | any | Service to match, as a pattern. |
| `op` | string | any | Operation to match, as a pattern, such as `Put*`. |
| `cloud`, `region`, `principal`, `path` | string | any | More patterns to match. |
| `probability` | number | `0` | Chance that a matching call is affected, from 0 to 1. `0` means always. |
| `latencyMs` | number | none | Delay to add, in milliseconds. |
| `status` | number | none | HTTP status to respond with. |
| `code` | string | none | Error code to return. |
| `message` | string | none | Error message to return. |

The response also has `id` and `hits`, how often the rule has fired.

```bash
curl -X POST -H "content-type: application/json" \
  -d '{"service":"dynamodb","latencyMs":200,"probability":0.25}' \
  localhost:7644/_pogi/api/chaos
```

```json title="Response"
{"id":"f2c47b28","service":"dynamodb","probability":0.25,"latencyMs":200,"hits":0}
```

- A body that is not valid JSON returns `400` with `{"error":"…"}`.

### Remove a chaos rule

`DELETE /_pogi/api/chaos/{id}`

Removes one rule.

```bash
curl -X DELETE localhost:7644/_pogi/api/chaos/f2c47b28
```

It returns `204` with no body. If there is no such rule, it returns `404` with `{"error":"rule not found"}`.

### Remove all chaos rules

`DELETE /_pogi/api/chaos`

Removes every rule. It returns `204` with no body.

```bash
curl -X DELETE localhost:7644/_pogi/api/chaos
```

## Outboxes

These endpoints show what pogi captured instead of sending: queue messages, emails, texts, push messages and Cognito codes.

### Peek at SQS messages

`GET /_aws/sqs/messages`

Reads a queue's messages without receiving them. See [Migrate from LocalStack](../how-to/migrate-from-localstack.md).

| Query | Type | Default | Meaning |
|---|---|---|---|
| `QueueUrl` | string | none | The queue URL. |
| `Region` | string | any | The queue's region. pogi finds the queue in any region when it is not given. |

You can also put the queue in the path: `GET /_aws/sqs/messages/{region}/{account}/{queue}`.

```bash
curl localhost:7644/_aws/sqs/messages/us-east-1/000000000000/jobs
```

```json title="Response"
{"messages":[{"MessageId":"12b3bfae-4999-4fd9-aaf6-786d4acdc7ff","Body":"hello","SentTimestamp":"2026-10-13T18:11:57.486148Z","ApproximateReceiveCount":0,"State":"visible"}]}
```

- An unknown queue returns `400` with `{"error":"QueueDoesNotExist: The specified queue does not exist."}`.

### List or clear SES emails

`GET /_aws/ses` and `DELETE /_aws/ses`

Lists, or clears, the emails sent through SES.

| Query | Type | Default | Meaning |
|---|---|---|---|
| `id` | string | all | Only this message id. On `DELETE`, it removes only this message. |
| `email` | string | all | Only messages from this sender. Lists only. |

```bash
curl localhost:7644/_aws/ses
```

```json title="Response"
{"messages":[{"Id":"280a2f0e7aae7dab-39f9247e-fa79-4078-8861-977ca26fd24f-000000","Region":"us-east-1","Account":"000000000000","Source":"a@example.com","Destination":{"ToAddresses":["b@example.com"]},"Subject":"hi","Body":{"text_part":"hello there","html_part":""},"Timestamp":"2026-10-13T18:14:04.490798Z"}]}
```

`DELETE` returns `204`. Without `id`, it removes every message.

### List or clear SMS messages

`GET /_aws/sns/sms-messages` and `DELETE /_aws/sns/sms-messages`

Lists, or clears, the SMS messages sent through SNS, grouped by phone number.

| Query | Type | Default | Meaning |
|---|---|---|---|
| `phoneNumber` | string | all | Only this number. Write `+` as `%2B`. |
| `accountId` | string | all | Only this account. |
| `region` | string | all | Only this region. |

```bash
curl "localhost:7644/_aws/sns/sms-messages?phoneNumber=%2B15555550123"
```

```json title="Response"
{"sms_messages":{"+15555550123":[{"Timestamp":"2026-10-13T18:14:04.69832Z","MessageId":"95dd1863-35ab-4dbb-a2ed-a70009bd536d","AccountId":"000000000000","Region":"us-east-1","Protocol":"sms","Destination":"+15555550123","Message":"code 1"}]}}
```

`DELETE` removes the messages that match the filters and returns `204`.

### List or clear push messages

`GET /_aws/sns/platform-endpoint-messages` and `DELETE /_aws/sns/platform-endpoint-messages`

The same as SMS messages, for push messages. Filter with `endpointArn` instead of `phoneNumber`, and with `accountId` and `region`.

```bash
curl localhost:7644/_aws/sns/platform-endpoint-messages
```

```json title="Response"
{"platform_endpoint_messages":{}}
```

### List Cognito codes

`GET /_aws/cognito/codes`

Lists the confirmation and verification codes Cognito "sent".

| Query | Type | Default | Meaning |
|---|---|---|---|
| `pool` | string | all | Only this user pool id. |
| `username` | string | all | Only this user name or destination address. |

```bash
curl localhost:7644/_aws/cognito/codes
```

```json title="Response"
[{"pool":"us-east-1_ak5Yfk9FS","username":"u@example.com","destination":"u@example.com","code":"961860","type":"SignUp","timestamp":"2026-10-13T23:44:05.547334+05:30"}]
```

## LocalStack compatibility

### Get LocalStack health

`GET /_localstack/health`

LocalStack's health endpoint. It lists every enabled service as `running`. The keys of `services` are pogi's service ids (`pogi services` lists them).

```bash
curl localhost:7644/_localstack/health
```

```json title="Response (trimmed)"
{"edition":"community","services":{"accessanalyzer":"running","account":"running","acm":"running", …},"version":"pogi-0.2.0"}
```

### Get LocalStack info

`GET /_localstack/info`

LocalStack's info endpoint.

```bash
curl localhost:7644/_localstack/info
```

```json title="Response"
{"edition":"community","is_docker":false,"is_license_activated":false,"machine_id":"pogi","server_time_utc":"2026-10-13T18:11:51Z","session_id":"pogi","system":"darwin","uptime":15,"version":"pogi-0.2.0"}
```

### Get init hook status

`GET /_localstack/init` and `GET /_localstack/init/{stage}`

Returns the state of the [init hooks](configuration.md#init-hooks). `{stage}` is `boot`, `start`, `ready` or `shutdown`. Without a stage, the response covers all four.

```bash
curl localhost:7644/_localstack/init/ready
```

```json title="Response"
{"completed":true,"scripts":[{"stage":"READY","name":"10-seed.sh","state":"SUCCESSFUL","output":"make_bucket: from-init\n"}]}
```

With no hooks, `scripts` is `null` for a stage, and `[]` for the whole list:

```json title="Response"
{"completed":{"BOOT":true,"READY":true,"SHUTDOWN":false,"START":true},"scripts":[]}
```

## Other

### Get a GKE kubeconfig

`GET /_gcp/gke/kubeconfig/{project}/{location}/{cluster}`

Returns a kubeconfig for a Google Kubernetes Engine cluster that pogi runs. `pogi kubeconfig gke` calls it.

```bash
curl localhost:7644/_gcp/gke/kubeconfig/pogi/us-central1/dev
```

- With no such cluster, the response is `404` with `no cluster projects/pogi/locations/us-central1/clusters/dev`.

### MCP server

`/_pogi/mcp`

The [MCP](cli.md#pogi-mcp-run-the-mcp-server-for-ai-agents) server, over Streamable HTTP.

The rest of `/_pogi/` is the web console. Its other paths are pages and form posts for the console and may change between releases. Use the endpoints above from scripts.

## See also

- [CLI](cli.md): the commands that call these endpoints.
- [Console](console.md): the pages and form posts behind `/_pogi/`.
