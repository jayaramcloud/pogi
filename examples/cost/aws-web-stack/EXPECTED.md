# Expected monthly cost: web, database, cache and NAT

Region us-east-1. On-demand list prices, 730 hours a month, USD, no free tier. Unit prices come from the
AWS Price List bulk files that pogi's price data is built from (published 2026-10-20). Checked by hand against the pricing pages: the Application and Network Load Balancer rates, the gp2, gp3 and io2 storage rates and the public IPv4 rate match them.

pogi notes these as assumptions: the operating system of each instance (Linux, since no usage file says otherwise).

| Charge | Quantity | Unit | Unit price | Monthly | Pricing page |
|---|---|---|---|---|---|
| Instance hours, web_a (m5.large) | 730 | hour | 0.096 | 70.08 | https://aws.amazon.com/ec2/pricing/on-demand/ |
| Root disk, web_a (gp3, 30 GB) | 30 | GB-month | 0.08 | 2.40 | https://aws.amazon.com/ebs/pricing/ |
| Instance hours, web_b (m5.large) | 730 | hour | 0.096 | 70.08 | https://aws.amazon.com/ec2/pricing/on-demand/ |
| Root disk, web_b (gp3, 30 GB) | 30 | GB-month | 0.08 | 2.40 | https://aws.amazon.com/ebs/pricing/ |
| Application load balancer hours | 730 | hour | 0.0225 | 16.43 | https://aws.amazon.com/elasticloadbalancing/pricing/ |
| RDS db.t3.medium, Postgres Multi-AZ | 730 | hour | 0.145 | 105.85 | https://aws.amazon.com/rds/postgresql/pricing/ |
| RDS storage (gp3, Multi-AZ, 100 GB) | 100 | GB-month | 0.23 | 23.00 | https://aws.amazon.com/rds/postgresql/pricing/ |
| Redis nodes (2 x cache.t3.small) | 1460 | hour | 0.034 | 49.64 | https://aws.amazon.com/elasticache/pricing/ |
| Elastic IP (public IPv4) | 730 | hour | 0.005 | 3.65 | https://aws.amazon.com/vpc/pricing/ |
| NAT gateway hours | 730 | hour | 0.045 | 32.85 | https://aws.amazon.com/vpc/pricing/ |

**Expected monthly total: $376.38** (allow 1% for price updates).
