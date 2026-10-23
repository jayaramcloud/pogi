---
title: Flows and the Map
description: How pogi links calls into flows, and what the Map draws.
---

# Flows and the Map

One call often sets off many more. You put a file in S3, S3 tells a queue, a function reads the queue and writes to a database. pogi follows these steps for you and shows them in two places:

- **Flows** (the Flows view on the Activity page) list each chain of steps as one row. A flow is one chain of calls and deliveries that share a trace ID.
- **The Map** draws how your resources are wired.

## How are steps linked?

Every request pogi receives gets a **trace ID**, a label that ties steps together. pogi copies the trace ID into the messages it delivers from one resource to the next, so a chain such as S3 to SQS to Pub/Sub keeps one ID. For Lambda, pogi gives the function the trace ID (in `_X_AMZN_TRACE_ID`), so calls your code makes with an SDK join the same flow.

If your own code makes several calls and you want them in one flow, send the same trace ID with each. pogi reads it from the first of these it finds:

1. `X-Amzn-Trace-Id` (the `Root=` part)
2. `traceparent` (the W3C Trace Context header)
3. `X-Cloud-Trace-Context`

If a request has none, pogi makes a new trace ID.

```bash
TP=00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01
curl -H "traceparent: $TP" "http://localhost:7644/storage/v1/b?project=pogi"
curl -H "traceparent: $TP" -X POST -H "content-type: application/json" -d '{"name":"demo-gcs"}' "http://localhost:7644/storage/v1/b?project=pogi"
curl -H "traceparent: $TP" "http://localhost:7644/storage/v1/b/demo-gcs"
```

All three calls get the same trace ID, `1-4bf92f35-77b34da6a3ce929d0e0e4736`. pogi shows trace IDs in the AWS style and converts a `traceparent` ID to it. Each call in `GET /_pogi/api/requests` has it in its `trace` field. This is the `buckets.insert` call:

```bash
curl "http://localhost:7644/_pogi/api/requests?q=buckets.insert"
```

```json title="Response"
[{"id":5,"time":"2026-10-13T21:14:49.869442+05:30","cloud":"gcp","service":"gcs","op":"buckets.insert","method":"POST","path":"/storage/v1/b?project=pogi","status":200,"ms":0.114,"account":"000000000000","region":"us-east-1","requestId":"46936079f9b522d2e253676ebc32f229","principal":"arn:aws:iam::000000000000:root","trace":"1-4bf92f35-77b34da6a3ce929d0e0e4736"}]
```

In the console, the three calls are one flow. A flow needs at least two steps. A single call with nothing after it is a plain request.

## Why does the Map draw only some lines?

Lines come from two sources: links that services report, and deliveries pogi recorded. Two requests that share a trace ID do not draw a line between their resources, and pogi does not guess links between clouds. If the Map shows a line, something configured it or something delivered along it. Resources with no connection are hidden, and the Map says how many.

## Where does pogi keep the trace?

Beside each message, never inside your message attributes or properties. Your code sees the message it sent, not one changed by pogi.

## See also

- [Read flows and the map](../how-to/read-flows-and-the-map.md)
- [Console](../reference/console.md)
- [How pogi works](how-pogi-works.md)
