# gcp-serverless (us-central1 and the US multi-region, usage.yaml beside it)

Status: quantities final; unit prices and total **not yet checked** (see gcp-web-db/EXPECTED.md for why).
Pages to check: Cloud Run pricing (request-based billing), Cloud Storage pricing, Pub/Sub pricing, BigQuery pricing.

| Charge | Quantity | Unit | Unit price |
|---|---|---|---|
| api: requests | 2 | 1M requests | TBD |
| api: CPU time (1 vCPU) | 2,000,000 × 0.15 s = 300,000 | vCPU-second | TBD |
| api: memory time (0.5 GB) | 2,000,000 × 0.15 × 0.5 = 150,000 | GB-second | TBD |
| assets: Standard storage, US multi-region | 200 | GB-month | TBD |
| assets: Class A operations | 0.1 | 1M requests | TBD |
| assets: Class B operations | 1 | 1M requests | TBD |
| events: message throughput | 50 | GB | TBD (the catalog's per-TiB price / 1024) |
| logs: active logical storage, US | 500 | GB-month | TBD |
| logs: long-term storage | 0 (no usage given) | GB-month | TBD, adds $0 |

Expected monthly total: TBD. All rows have status `known` (usage given), except `logs`, which is known too.
