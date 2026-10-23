---
title: Read flows and the map
description: Follow one chain of calls in Flows, and see how resources connect on the Map.
---

# Read flows and the map

## Read a flow

1. Open **Activity** and choose **Flows**. Each row shows the call that started the flow, its path, its number of steps and its duration.
2. Filter the list with the chips **All**, **Failed** and **Slow** (over one second). The search box takes a service, a resource name or a trace ID.
3. Click a row to open its timeline. Steps that pogi runs itself sit between the requests: handing a message to a queue, running a function, moving a message to a dead-letter queue.
4. Click any step to open its request and response.

![The Flows list: rows such as S3 to SQS, and a longer path through S3, SQS, Pub/Sub, Blob Storage, Event Grid and Pub/Sub.](../images/c-flows.jpg)

![One flow opened, with a timeline of six steps.](../images/c-flow.jpg)

## Read the Map

The Map shows resources as boxes, grouped by cloud, with a line for each connection. For what each kind of line means, see the [Map legend](../reference/console.md#map).

![The Map: an S3 bucket wired to an SQS queue, a Pub/Sub subscription, and storage buckets feeding Eventarc and Event Grid.](../images/c-map.jpg)

- Click a resource to see what goes in and out of it, and the recent flows that touch it.
- Choose **Export SVG** to save the picture.
- Use the search box to highlight one resource.

## See also

- [Flows and the Map](../explanation/flows-and-the-map.md)
- [Console](../reference/console.md)
