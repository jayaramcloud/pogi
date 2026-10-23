---
title: Benchmarks
description: How pogi's start-up time, memory and speed were measured, and on what.
---

# Benchmarks

pogi's website quotes the numbers below. All of them were measured on 2026-10-16 on an Apple M5
with 32 GB of memory, using the pogi v0.1.0 release build. The machine was also running other work at the time, so
a quiet machine may do better. Your numbers will differ with your machine.

| Measure | Result | How |
|---|---|---|
| Start-up time | about 25 ms | `pogi start` with default flags, timed from launch to the `Ready.` line, warm (after one run that pays the operating system's first-launch check). Each run in a fresh shell and a fresh data folder. Median of 6 runs: 23 ms. |
| Memory at idle | about 64 MB | Resident memory (`ps -o rss`) half a second after `Ready.`. Median of 10 runs. |
| Writes per second | about 20,000 | 20,000 signed DynamoDB `PutItem` calls over localhost, 8 at a time, with `ab` (ApacheBench), after a 2,000-call warm-up. No failed calls. |
| Memory under load | about 140 MB | Resident memory right after that burst. |
| One call, start to finish | about 0.1 ms | The same call with `ab -c 1`, 5,000 calls, mean time per call. |

## Download size

| File | Size |
|---|---|
| Release binary (`-trimpath -s -w`, as the release build makes it) | 77.6 MB |
| Release download (`.tar.gz`) | 27.0 MB |
| A plain `go build` (keeps debug symbols) | 109.7 MB |

## Run it yourself

```bash
pogi start
```

```bash
# In a second terminal: create a table, then send the same signed PutItem 20,000 times, 8 at a time.
eval "$(pogi env)"
aws dynamodb create-table --table-name bench \
  --attribute-definitions AttributeName=pk,AttributeType=S \
  --key-schema AttributeName=pk,KeyType=HASH --billing-mode PAY_PER_REQUEST
```

`ab` needs a request body and the request's headers. Sign one `PutItem` request with your SDK's signer (or copy the
headers from `aws dynamodb put-item --debug`), save the JSON body to `body.json`, then run
`ab -n 20000 -c 8 -p body.json -T application/x-amz-json-1.0 -H "X-Amz-Target: DynamoDB_20120810.PutItem" -H "X-Amz-Date: …" -H "Authorization: …" http://localhost:7644/`.
pogi does not check signatures by default, so one signed request can be sent many times.
