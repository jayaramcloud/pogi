# azure-windows-sql: expected cost

Region `uksouth`. Prices from the Azure Retail Prices API on 2026-10-20 (the data behind the Azure pricing pages and calculator). On-demand list prices, 730 hours a month, USD, no free tier or discounts.

| Charge | Quantity | Unit price | Monthly |
|---|---|---|---|
| VM hours (Standard_D4s_v5, Windows) | 730 | $0.406 | $296.38 |
| OS disk (E10 LRS, 128 GB) | 1 | $10.56 | $10.56 |
| Data disk (P20 LRS, 512 GB) | 1 | $88.594 | $88.59 |
| SQL compute hours (GP_Gen5_2) | 730 | $0.380542 | $277.80 |
| SQL storage (64 GB) | 64 | $0.13225 | $8.46 |

**Expected monthly total: $681.79**

Disk operations and storage account capacity are priced by use ($0 here: no usage file).
