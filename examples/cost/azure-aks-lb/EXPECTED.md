# azure-aks-lb: expected cost

Region `westeurope`. Prices from the Azure Retail Prices API on 2026-10-20 (the data behind the Azure pricing pages and calculator). On-demand list prices, 730 hours a month, USD, no free tier or discounts.

| Charge | Quantity | Unit price | Monthly |
|---|---|---|---|
| Cluster fee (Standard tier) | 730 | $0.1 | $73.00 |
| Node hours (Standard_D4s_v5, 3 nodes) | 2190 | $0.23 | $503.70 |
| OS disks (P10 LRS, 3 nodes) | 3 | $21.68 | $65.04 |
| Node hours (Standard_D8s_v5, 2 nodes) | 1460 | $0.46 | $671.60 |
| OS disks (P10 LRS, 2 nodes) | 2 | $21.68 | $43.36 |
| NAT gateway hours | 730 | $0.045 | $32.85 |
| Public IP hours (Standard, static) | 730 | $0.005 | $3.65 |
| Load balancer hours (Standard) | 730 | $0.025 | $18.25 |

**Expected monthly total: $1411.45**

NAT and load balancer data processed is priced by use ($0 here: no usage file).
