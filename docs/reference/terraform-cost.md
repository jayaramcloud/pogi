---
title: Terraform cost
description: The resource types pogi prices, usage keys, statuses, defaults, limits, regions and price data for pogi cost and the Terraform tab.
---

# Terraform cost

The resource types, usage keys, defaults and limits behind `pogi cost` and the console's Terraform tab. For the steps, see [Estimate Terraform cost](../how-to/estimate-terraform-cost.md). For the reasoning, see [How Terraform cost works](../explanation/how-terraform-cost-works.md).

**Basis:** on-demand list prices, 730 hours a month, USD. No free tier, discounts or tax.

## Priced resource types

### AWS

| Service | Types |
|---|---|
| EC2 | `aws_instance`, `aws_autoscaling_group`, `aws_ebs_volume` |
| VPC and networking | `aws_eip`, `aws_nat_gateway`, `aws_vpc_endpoint`, `aws_lb`, `aws_alb`, `aws_route53_zone`, `aws_cloudfront_distribution` |
| Databases | `aws_db_instance`, `aws_rds_cluster`, `aws_rds_cluster_instance`, `aws_elasticache_cluster`, `aws_elasticache_replication_group`, `aws_dynamodb_table`, `aws_opensearch_domain`, `aws_msk_cluster` |
| Containers | `aws_eks_cluster`, `aws_eks_node_group`, `aws_ecs_service` |
| Serverless and messaging | `aws_lambda_function`, `aws_sqs_queue`, `aws_sns_topic`, `aws_kinesis_stream` |
| Storage, logs and keys | `aws_s3_bucket`, `aws_cloudwatch_log_group`, `aws_secretsmanager_secret`, `aws_kms_key` |

### Google Cloud

<!-- gcp-prices -->
These types have price rules, but no Google Cloud prices are in pogi yet, so each row shows `unpriced` ("pogi has no Google Cloud prices yet") and is listed under "Not priced". See [Price data](#price-data).

`google_compute_instance`, `google_compute_disk`, `google_compute_address`, `google_compute_router_nat`, `google_compute_forwarding_rule`, `google_container_cluster`, `google_container_node_pool`, `google_sql_database_instance`, `google_redis_instance`, `google_storage_bucket`, `google_cloud_run_v2_service`, `google_cloudfunctions2_function`, `google_pubsub_topic`, `google_bigquery_dataset`.

### Azure

`azurerm_linux_virtual_machine`, `azurerm_windows_virtual_machine`, `azurerm_managed_disk`, `azurerm_public_ip`, `azurerm_nat_gateway`, `azurerm_lb`, `azurerm_application_gateway`, `azurerm_kubernetes_cluster`, `azurerm_kubernetes_cluster_node_pool`, `azurerm_service_plan`, `azurerm_linux_function_app`, `azurerm_windows_function_app`, `azurerm_mssql_database`, `azurerm_postgresql_flexible_server`, `azurerm_mysql_flexible_server`, `azurerm_redis_cache`, `azurerm_cosmosdb_account`, `azurerm_storage_account`, `azurerm_servicebus_namespace`.

## Free types

These cost nothing on their own. They show as `free`. Anything billed by use is priced on the resource that carries it.

- **AWS:** VPCs, subnets, gateways, route tables and associations, security groups and their rules, network ACLs, DHCP options, `aws_eip_association`; IAM roles, policies, users, groups and instance profiles, `aws_key_pair`; launch templates and configurations, autoscaling policies, database, cache and parameter groups, load balancer listeners, listener rules, target groups and attachments; `aws_ecs_cluster`, `aws_ecs_task_definition`, `aws_eks_addon`, Lambda permissions and aliases; S3 bucket policy, versioning, ACL, public access block, ownership and encryption settings; SQS and SNS policies, SNS subscriptions, `aws_cloudwatch_log_stream`, `aws_kms_alias`, `aws_secretsmanager_secret_version` and `aws_route53_record`.
- **Google Cloud:** `google_compute_network`, `google_compute_subnetwork`, `google_compute_firewall`, `google_compute_route`, `google_compute_router`, `google_compute_network_peering`, `google_compute_instance_group`, `google_compute_health_check`, service accounts and their keys, `google_project_service`, project, service account and bucket IAM members and bindings, `google_project_iam_policy` and `google_project_iam_custom_role`.
- **Azure:** resource groups, virtual networks, subnets, network security groups and rules, route tables and routes, network interfaces, application security groups, peerings, user-assigned identities, role assignments and definitions, locks, disk attachments, load balancer rules, pools, probes and NAT rules, storage containers, `azurerm_mssql_server` and its firewall rules, database, configuration and firewall rules of the Postgres and MySQL flexible servers, Service Bus queues, topics and subscriptions, Linux and Windows web apps, and `azurerm_network_watcher`.

Resources from other providers (`random_`, `null_` and so on) show as free with the note "not a cloud resource". A type not in any list above is `unpriced`.

## Statuses

| Status | Means |
|---|---|
| `known` | Every input came from the files. |
| `assumed` | pogi used a default and says so in the row's notes. |
| `usage` | Only usage charges are left, and no usage is known. The unit price shows. The row counts $0. |
| `needs` | A value pogi must have is unknown or missing, or has no price. The row says what. It counts $0. |
| `free` | Costs nothing on its own. |
| `unpriced` | No price rule for this type. It adds nothing, and the output says the real bill could be higher. |

A default that is the provider's own documented default isn't an assumption. A guess is, and the row says "assumed …".

## Usage keys

Set these in a usage file, under the resource's address. Run `pogi cost <path> --usage-template` to list the keys a folder's resources read.

| Type | Keys |
|---|---|
| `aws_instance`, `aws_autoscaling_group` | `os` (`linux` or `windows`) |
| `aws_lambda_function` | `monthly_requests`, `avg_duration_ms` |
| `aws_lb`, `aws_alb` | `monthly_lcu_hours` |
| `aws_nat_gateway`, `aws_vpc_endpoint` | `monthly_gb_processed` |
| `aws_s3_bucket` | `storage_gb`, `monthly_get_requests`, `monthly_put_requests` |
| `aws_dynamodb_table` | `storage_gb`, `monthly_read_request_units`, `monthly_write_request_units` |
| `aws_sqs_queue`, `aws_kms_key`, `aws_secretsmanager_secret` | `monthly_requests` |
| `aws_sns_topic` | `monthly_requests`, `monthly_http_deliveries`, `monthly_email_deliveries`, `monthly_mobile_push_deliveries` |
| `aws_kinesis_stream` | `monthly_put_payload_units`, `monthly_gb_in`, `monthly_gb_out` |
| `aws_cloudwatch_log_group` | `monthly_ingested_gb`, `storage_gb` |
| `aws_cloudfront_distribution` | `monthly_gb_out`, `monthly_http_requests`, `monthly_https_requests` |
| `aws_rds_cluster` | `storage_gb`, `monthly_io_requests` |
| `aws_rds_cluster_instance` | `avg_acus` (Aurora Serverless) |
| `aws_msk_cluster` | `storage_gb` |
| `aws_opensearch_domain` | `warm_storage_gb` |
| `aws_route53_zone` | `monthly_queries` |
| `google_storage_bucket` | `storage_gb`, `monthly_class_a_requests`, `monthly_class_b_requests` |
| `google_bigquery_dataset` | `storage_gb`, `long_term_storage_gb` |
| `google_pubsub_topic` | `monthly_gb_published` |
| `google_compute_router_nat` | `monthly_gb_processed`, `nat_vm_count` |
| `google_compute_forwarding_rule` | `monthly_gb_processed` |
| `google_cloud_run_v2_service`, `google_cloudfunctions2_function` | `monthly_requests`, `avg_duration_ms` |
| `azurerm_lb`, `azurerm_nat_gateway` | `monthly_gb_processed` |
| `azurerm_application_gateway` | `avg_capacity_units`, `monthly_gb_processed` |
| `azurerm_linux_function_app`, `azurerm_windows_function_app` | `monthly_executions`, `monthly_gb_seconds` |
| `azurerm_storage_account` | `storage_gb`, `monthly_read_operations`, `monthly_write_operations` |
| `azurerm_cosmosdb_account` | `storage_gb`, `ru_per_second`, `autoscale_max_ru_per_second`, `monthly_request_units` |
| `azurerm_mssql_database` | `storage_gb`, `monthly_vcore_hours` |
| `azurerm_servicebus_namespace` | `monthly_operations` |
| `azurerm_managed_disk`, VMs, node pools | `monthly_disk_operations` |

A key must be snake_case. A value is a number that isn't negative, except `os`. A row's details in the console list the keys it reads.

## What is assumed

A row notes each default it used. Some examples:

| When | pogi assumes |
|---|---|
| The files name no region | The cloud's usual one: `us-east-1`, `us-central1` or `eastus`. |
| An EC2 instance's AMI | Linux, unless the AMI lookup or `os: windows` in the usage file says otherwise. |
| A root disk size is not set | 8 GB, with the type the image sets (gp3 for AWS). |
| A public address is not set | No public IPv4 address. |
| An address, NAT or load balancer is used | Attached to something. An unused address costs more. |
| Spot or preemptible VMs | The on-demand rate. Spot prices aren't in the price lists. |
| A cluster autoscales | The minimum node count. |
| An Aurora Serverless cluster | Its minimum capacity, unless `avg_acus` is set. |
| A regional GKE cluster | 3 zones, unless `node_locations` is set. |
| A value unknown until apply | A stated default, or the row shows `needs`. |

Other notes name what is left out: for example global table replicas, the Infrequent Access log class (priced as Standard), CloudFront prices outside the US, and CloudWatch Logs storage beyond 7 days of retention.

A `count` or `for_each` that pogi can't work out counts as 1 instance, with a warning.

## Limits

| Limit | Value |
|---|---|
| Files read from a folder | 2,000 |
| Bytes of files read | 50 MB in total. A plan file can be up to 50 MB. |
| Resource instances from `count`, `for_each` and modules | 20,000 |
| Module nesting | 16 levels |
| Usage file | 1 MB |
| Console upload: variable files, what-if sizes or modules in one request | 1,000 each |
| Console upload: one form field | 64 KB |
| Diagram: a module starts as one box above | 40 blocks |

pogi reads `.tf`, `.tf.json`, `.tfvars` and `.tfvars.json` files, and `.terraform/modules`. It skips other hidden folders, `node_modules` and symbolic links. Going over a limit is an error that names the limit.

## Regions

Prices follow the resource's region. A resource gets its region, in order, from its own `region`, `zone` or `location`, then from its provider block, and for Azure and Google Cloud, from a resource it refers to. See [How Terraform cost works](../explanation/how-terraform-cost-works.md#how-a-resource-gets-its-region).

| Cloud | Regions with prices | Default |
|---|---|---|
| AWS | 34 public regions, from `af-south-1` to `us-west-2` | `us-east-1` |
| Azure | 59 regions, such as `eastus`, `westeurope` and `japaneast` | `eastus` |
| Google Cloud | <!-- gcp-prices -->None yet | `us-central1` |

A size or charge that isn't sold in a region shows as `needs`, with "no price for … in <region>".

## Price data

The prices are built into pogi. There is one snapshot per cloud, made from the cloud's own price list before each release. pogi never downloads prices.

| Cloud | Date | Source |
|---|---|---|
| AWS | 2026-10-20 | AWS Price List bulk files, on-demand rows |
| Azure | 2026-10-20 | Azure Retail Prices API, pay-as-you-go list prices |
| Google Cloud | <!-- gcp-prices -->none yet | The data file is a placeholder. Every priced Google Cloud type shows `unpriced`, and `pogi cost sizes` says there are no Google Cloud sizes to list. |

The header of `pogi cost` output, and the console, name the date and source of each cloud in use.

## What is not covered

- Free tiers, committed-use, savings plans, reserved and spot prices, enterprise discounts and tax.
- Paid operating system licenses other than Windows (such as RHEL, SUSE and SQL Server).
- Currencies other than USD.
- Data transfer between regions and to the internet, unless a type has a usage key for it.
- Resource types not listed above: they show as `unpriced`.
- Terragrunt, Pulumi and CloudFormation. pogi reads Terraform files and plans.
- Values that exist only after `apply`. A plan gives more of them than a folder does.

## See also

- [CLI: `pogi cost`](cli.md#pogi-cost-estimate-a-terraform-bill)
- [Console: the Terraform page and tab](console.md#the-terraform-page-and-tab)
