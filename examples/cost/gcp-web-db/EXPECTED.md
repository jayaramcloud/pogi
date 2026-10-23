# gcp-web-db (us-central1, on-demand, 730 hours a month)

Status: the quantities are final. The unit prices and the total are **not yet checked**: the Google
price list could not be generated (the gcloud login had expired), and the pricing pages
(cloud.google.com/compute/all-pricing, /sql/pricing, /memorystore/docs/redis/pricing) load their
tables with JavaScript, so a plain fetch could not read them. Fill in the prices from the
generated list or the Google Cloud pricing calculator, then check the total.

| Charge | Quantity | Unit | Unit price | Check against |
|---|---|---|---|---|
| web: n2-standard-2 instance hours (Linux) | 730 | hour | TBD | Compute Engine VM pricing, us-central1 |
| web: boot disk pd-balanced | 50 | GB-month | TBD | Compute Engine disk pricing |
| web: external IPv4 address | 730 | hour | TBD | VPC IP address pricing |
| db: Cloud SQL PostgreSQL vCPU (zonal) | 2 × 730 | vCPU-hour | TBD | Cloud SQL pricing |
| db: Cloud SQL PostgreSQL memory (7680 MB = 7.5 GB) | 7.5 × 730 | GB-hour | TBD | Cloud SQL pricing |
| db: SSD storage | 100 | GB-month | TBD | Cloud SQL pricing |
| cache: Memorystore Redis Basic, M1 tier (2 GB) | 2 × 730 | GB-hour | TBD | Memorystore pricing |

Expected monthly total: TBD (the sum of quantity × unit price above).
Expected row status: all known, except none assumed.
