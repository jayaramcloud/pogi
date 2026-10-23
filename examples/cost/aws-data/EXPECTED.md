# Expected monthly cost: data services

Region us-east-1. On-demand list prices, 730 hours a month, USD, no free tier. Unit prices come from the
AWS Price List bulk files that pogi's price data is built from (published 2026-10-20). The unit prices were read from the price file; the pricing pages were not rechecked for this stack.

pogi notes these as assumptions: nothing; usage-priced charges (requests, data) are $0 without a usage file.

| Charge | Quantity | Unit | Unit price | Monthly | Pricing page |
|---|---|---|---|---|---|
| DynamoDB read capacity (20 table + 5 index) | 18250 | hour | 0.00013 | 2.37 | https://aws.amazon.com/dynamodb/pricing/provisioned/ |
| DynamoDB write capacity (10 table + 5 index) | 10950 | hour | 0.00065 | 7.12 | https://aws.amazon.com/dynamodb/pricing/provisioned/ |
| Kinesis shard hours (2 shards) | 1460 | hour | 0.015 | 21.90 | https://aws.amazon.com/kinesis/data-streams/pricing/ |
| Kinesis extended retention (48 hours) | 1460 | hour | 0.02 | 29.20 | https://aws.amazon.com/kinesis/data-streams/pricing/ |
| MSK brokers (3 x kafka.m5.large) | 2190 | hour | 0.21 | 459.90 | https://aws.amazon.com/msk/pricing/ |
| MSK storage (3 x 500 GB) | 1500 | GB-month | 0.1 | 150.00 | https://aws.amazon.com/msk/pricing/ |
| OpenSearch data nodes (3 x r6g.large) | 2190 | hour | 0.167 | 365.73 | https://aws.amazon.com/opensearch-service/pricing/ |
| OpenSearch storage (3 x 100 GB gp3) | 300 | GB-month | 0.122 | 36.60 | https://aws.amazon.com/opensearch-service/pricing/ |
| KMS key | 1 | key-month | 1 | 1.00 | https://aws.amazon.com/kms/pricing/ |
| Secrets Manager secret | 1 | secret-month | 0.4 | 0.40 | https://aws.amazon.com/secrets-manager/pricing/ |

**Expected monthly total: $1074.22** (allow 1% for price updates).
