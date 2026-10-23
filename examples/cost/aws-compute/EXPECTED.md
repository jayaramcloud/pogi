# Expected monthly cost: autoscaling group, Fargate and Lambda

Region ap-southeast-1. On-demand list prices, 730 hours a month, USD, no free tier. Unit prices come from the
AWS Price List bulk files that pogi's price data is built from (published 2026-10-20). The unit prices of this region are read from the price file; the pricing pages were not rechecked for this region.

pogi notes these as assumptions: the operating system (Linux) and that the group has no public IPv4 addresses. The Lambda function is usage-priced and adds $0 without a usage file.

| Charge | Quantity | Unit | Unit price | Monthly | Pricing page |
|---|---|---|---|---|---|
| Group instances (3 x t3.small) | 2190 | hour | 0.0264 | 57.82 | https://aws.amazon.com/ec2/pricing/on-demand/ |
| Group disks (3 x 20 GB gp3) | 60 | GB-month | 0.096 | 5.76 | https://aws.amazon.com/ebs/pricing/ |
| Fargate vCPU (2 tasks x 0.5) | 730 | vCPU-hour | 0.05056 | 36.91 | https://aws.amazon.com/fargate/pricing/ |
| Fargate memory (2 tasks x 1 GB) | 1460 | GB-hour | 0.00553 | 8.07 | https://aws.amazon.com/fargate/pricing/ |

**Expected monthly total: $108.56** (allow 1% for price updates).
