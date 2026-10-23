# Expected monthly cost: Kubernetes cluster

Region eu-west-1. On-demand list prices, 730 hours a month, USD, no free tier. Unit prices come from the
AWS Price List bulk files that pogi's price data is built from (published 2026-10-20). The unit prices of this region are read from the price file; the pricing pages were not rechecked for this region.

pogi notes these as assumptions: the operating system of the nodes (Linux) and that the node disks are gp3.

| Charge | Quantity | Unit | Unit price | Monthly | Pricing page |
|---|---|---|---|---|---|
| EKS cluster hours | 730 | hour | 0.1 | 73.00 | https://aws.amazon.com/eks/pricing/ |
| Nodes (3 x m5.large) | 2190 | hour | 0.107 | 234.33 | https://aws.amazon.com/ec2/pricing/on-demand/ |
| Node disks (3 x 50 GB gp3) | 150 | GB-month | 0.088 | 13.20 | https://aws.amazon.com/ebs/pricing/ |
| Network load balancer hours | 730 | hour | 0.0252 | 18.40 | https://aws.amazon.com/elasticloadbalancing/pricing/ |
| NAT gateway hours | 730 | hour | 0.048 | 35.04 | https://aws.amazon.com/vpc/pricing/ |

**Expected monthly total: $373.97** (allow 1% for price updates).
