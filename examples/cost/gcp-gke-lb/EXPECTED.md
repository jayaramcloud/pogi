# gcp-gke-lb (us-central1, on-demand, 730 hours a month)

Status: quantities final; unit prices and total **not yet checked** (see gcp-web-db/EXPECTED.md for why).
Pages to check: Kubernetes Engine pricing, Compute Engine VM and disk pricing, Cloud Load Balancing
pricing, Cloud NAT pricing, VPC IP address pricing.

| Charge | Quantity | Unit | Unit price | 
|---|---|---|---|
| cluster management fee (zonal, standard) | 730 | hour | TBD |
| workers: e2-standard-4 instance hours (2 nodes, 1 zone) | 2 × 730 | hour | TBD |
| workers: boot disks pd-standard (2 × 50 GB) | 100 | GB-month | TBD |
| lb address: external IPv4 (assumed attached) | 730 | hour | TBD |
| web: forwarding rule (external) | 730 | hour | TBD |
| web: data processed | usage, none given | GB | TBD, adds $0 |
| nat: gateway uptime (assumed 1 VM) | 730 | hour | TBD |
| nat: data processed | usage, none given | GB | TBD, adds $0 |

Expected monthly total: TBD. Row statuses: cluster known, workers known, lb address assumed,
web usage-or-known, nat assumed.
