---
title: Estimate cost
description: See a rough monthly bill for what is running in pogi, from your resources and traffic.
---

# Estimate cost

The **Cost** page in the console works out a monthly bill from the resources you created and the traffic your code sent. The total also shows in the sidebar and on the Home page.

![The Cost page: a monthly total, a subtotal for each cloud, and a table of resources with what drives each price.](../images/t-cost.jpg)

## See your estimate

1. Run your code or tests against pogi for a realistic stretch of time. The estimate comes from a rate, so a very short run is a poor guide.
2. Open `http://localhost:7644/_pogi/cost`, or choose **Cost** in the console sidebar.
3. For AWS, pick a region at the top. The list has every AWS region, each with its own prices. The default is your pogi region, or `us-east-1` if that region is not in the list.
4. Read the "What drives it" column. It shows the numbers behind each row.

## How the estimate works

| Part | How it is counted |
|---|---|
| Resources | Always-on resources (a database, a VM, a Service Bus namespace) are billed for a full month of 730 hours. |
| Traffic | pogi counts each service's requests since it started, turns that into a rate and scales it to 30 days. The page shows how many requests it counted and over how much uptime. pogi then splits a service's requests among its resources. |
| Prices | On-demand list prices. No free tier and no discount. |

## What is priced

| Cloud | Priced services |
|---|---|
| AWS | EC2, RDS (MySQL, PostgreSQL and MariaDB, with their storage), S3, DynamoDB, Lambda, SQS, SNS, Kinesis, CloudWatch Logs, Secrets Manager and KMS. Prices come from the AWS price list built into pogi, for the region you pick. |
| Google Cloud | Cloud Storage, Pub/Sub, Cloud Run, Cloud Functions, Firestore and Cloud SQL. These prices are typed in for `us-central1`, until pogi's Google Cloud price list is ready. |
| Azure | Blob Storage, Functions, Service Bus (Standard tier, including its base fee), Cosmos DB (serverless) and Virtual Machines. Prices come from the Azure price list built into pogi, for East US, Linux, pay-as-you-go. |

Step Functions and API Gateway have pricing rules, but the built-in price list has no prices for them yet, so they show as "not priced".

Anything else shows as "not priced" and adds nothing to the total, so the real bill could be higher.

## Guesses pogi makes about traffic

- A quarter of storage requests are writes or lists. The rest are reads.
- An average log event or message is half a kilobyte.
- An average Cosmos DB request costs 5 request units.
- Firestore traffic is 80 percent reads and 20 percent writes.

To get the same numbers outside the console, run `pogi cost` with no path (it prints the same estimate as text), or call `GET /_pogi/cost.json` (see the [REST API](../reference/rest-api.md#get-the-running-cost)).

## See also

- [Console](../reference/console.md)
- [Coverage](../reference/coverage.md)
- [Estimate Terraform cost](estimate-terraform-cost.md): the bill for Terraform code, before you apply it
