---
title: Benchmarks
description: How pogi's start-up time, memory and speed were measured, and on what.
---

# Benchmarks

pogi's website quotes the numbers below. They were measured on an Apple M5 with 32 GB of memory, using a v0.2.0
build made the way the release build makes it (`-trimpath -s -w`). Start-up was measured on 2026-10-25 on a quiet
machine. The other numbers were measured on 2026-10-24, while the machine was running other work, so a quiet machine
may do better. Your numbers will differ with your machine.

v0.2.0 starts a little more slowly than v0.1.0 (about 25 ms) and uses more memory, because it runs many more
services and its program is larger.

| Measure | Result | How |
|---|---|---|
| Start-up time | about 30 ms | `pogi start` with default flags, timed from launch to the `Ready.` line, warm (after one run that pays the operating system's first-launch check). Each run in a fresh shell and a fresh data folder. Median of 21 runs: 27.6 ms; the middle half took 27.2 to 29.2 ms. |
| Memory at idle | about 75 MB | Resident memory (`ps -o rss`) half a second after `Ready.`. Median of 10 runs. |
| Writes per second | about 20,000 | 20,000 signed DynamoDB `PutItem` calls over localhost, 8 at a time, with `ab` (ApacheBench), after a 2,000-call warm-up. No failed calls. |
| Memory under load | about 150 MB | Resident memory right after that burst. |
| One call, start to finish | about 0.1 ms | The same call with `ab -c 1`, 5,000 calls, mean time per call. |

## Download size

| File | Size |
|---|---|
| Release binary (`-trimpath -s -w`, as the release build makes it) | 84.0 MB |
| Release download (`.tar.gz` of that binary) | 29.9 MB |
| A plain `go build` (keeps debug symbols) | 117.6 MB |

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
