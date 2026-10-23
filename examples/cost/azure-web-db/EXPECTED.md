# azure-web-db: expected cost

Region `eastus`. Prices from the Azure Retail Prices API on 2026-10-20 (the data behind the Azure pricing pages and calculator). On-demand list prices, 730 hours a month, USD, no free tier or discounts.

| Charge | Quantity | Unit price | Monthly |
|---|---|---|---|
| VM hours (Standard_D2s_v5, Linux) | 730 | $0.096 | $70.08 |
| OS disk (P6 LRS, 64 GB) | 1 | $10.207 | $10.21 |
| Public IP hours (Standard, static) | 730 | $0.005 | $3.65 |
| PostgreSQL compute hours (GP_Standard_D2ds_v5) | 730 | $0.178 | $129.94 |
| PostgreSQL storage (64 GB) | 64 | $0.115 | $7.36 |
| Redis cache hours (Standard C1) | 730 | $0.138 | $100.74 |

**Expected monthly total: $321.98**

