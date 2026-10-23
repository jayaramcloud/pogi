# azure-app-service: expected cost

Region `eastus2`. Prices from the Azure Retail Prices API on 2026-10-20 (the data behind the Azure pricing pages and calculator). On-demand list prices, 730 hours a month, USD, no free tier or discounts.

| Charge | Quantity | Unit price | Monthly |
|---|---|---|---|
| Plan hours (P1v3, Linux, 2 instances) | 1460 | $0.155 | $226.30 |
| Service Bus messaging unit hours (Premium, 1 unit) | 730 | $0.9275 | $677.08 |
| SQL compute hours (S1) | 730 | $0.0403208 | $29.43 |

**Expected monthly total: $932.81**

The web app is free (the plan is billed). The function app is priced by use ($0 here: no usage file).
