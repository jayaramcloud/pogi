---
title: Estimate a Terraform bill
description: Price a Terraform folder and a plan in pogi, and see the resources in a diagram, without an account or a network.
---

# Estimate a Terraform bill

By the end, you've priced a Terraform folder in the console and on the command line, and read what a plan would change. About 10 minutes. You need pogi installed: see [Install pogi](../how-to/install.md). You don't need Terraform for steps 1 and 2.

pogi reads your `.tf` files itself and prices them from a price list built into the program. It never runs Terraform and sends nothing anywhere.

## 1. Try the sample in the console

1. Start pogi and open the console:

   ```bash
   pogi start
   ```

   Open `http://localhost:7644/_pogi/terraform` in your browser, or choose **Terraform** in the console's sidebar.

2. Choose **Try a sample**. pogi prices a small AWS web stack built into the program: a load balancer, two app servers, a database, a bucket and a network module.
3. Read the total above the diagram. For the sample it is **$297.23** a month.
4. Open the table: choose **Cost** in the sidebar, then the **Terraform** tab. It shows the same stack. Find `aws_instance.app[0]`. The **What drives it** column says `Instance hours (m7g.large, Linux) · 730 h × $0.0816`, and the status says `assumed`. Click the row to see why: pogi assumed Linux, because the files don't say.
5. Find `aws_s3_bucket.assets`. Its status is `usage`: a bucket is billed by use, and pogi doesn't know how much you'll store. It shows the unit price, `$0.023 per GB-month`, and counts $0 until you give it numbers. See [Estimate Terraform cost](../how-to/estimate-terraform-cost.md#add-usage).
6. Choose **Open diagram** above the table, or go back to the **Terraform** page. Each box is a resource, with its monthly cost. Arrows run from a resource to the ones that use it. Click a box to see what depends on it.

Nothing left your computer. The browser sent the sample's files to your pogi, which read them in memory and kept nothing.

## 2. Price a folder on the command line

pogi's repository has sample folders in `examples/cost/`. If you installed pogi without the repository, use any folder of your own `.tf` files instead.

```bash
pogi cost examples/cost/aws-web-stack
```

```text title="Output (trimmed)"
Source: folder examples/cost/aws-web-stack
AWS prices of 2026-10-20, from AWS Price List bulk files (pricing.us-east-1.amazonaws.com), on-demand rows
On-demand list prices, 730 h/month, USD; no free tier or discounts.

RESOURCE                                 SERVICE                 MONTHLY  WHAT DRIVES IT
aws_instance.web_a                       EC2                      $72.48  assumed  730 h × $0.096 Instance hours (m5.large, Linux)
aws_instance.web_b                       EC2                      $72.48  assumed  730 h × $0.096 Instance hours (m5.large, Linux)
aws_lb.app                               Elastic Load Balancing   $16.43  730 h × $0.0225 Load balancer hours (application)
aws_db_instance.db                       RDS                     $128.85  730 h × $0.145 Instance hours (db.t3.medium, postgres, multi-az)
…
Total        $376.38 a month
```

`pogi cost` works without a running pogi when you give it a path. The header names the date and source of the prices.

Now try a what-if. What if `web_a` were an `m7g.large`?

```bash
pogi cost examples/cost/aws-web-stack --size aws_instance.web_a=m7g.large
```

`aws_instance.web_a` drops from $72.48 to $61.97. Your `.tf` file doesn't change. To see every size and its price, run `pogi cost sizes aws_instance`.

## 3. Read what a plan changes

A folder tells pogi what you wrote. A plan tells it what Terraform will do. Make one from your own folder:

```bash
terraform plan -out plan.out && terraform show -json plan.out > plan.json
```

This works against pogi too: run `pogi tf > pogi_override.tf` first (see [Connect your tools](../how-to/connect-your-tools.md#terraform)), so nothing touches a real cloud. Then price the plan:

```bash
pogi cost plan.json
```

Here is a plan that deletes an autoscaling group and keeps a service and a load balancer:

```text title="Output (trimmed)"
Source: plan plan.json (Terraform 1.15.8)
…
ACTION    RESOURCE                       SERVICE                 BEFORE   AFTER  WHAT DRIVES IT
− delete  aws_autoscaling_group.workers  EC2 Auto Scaling         $8.23   $0.00  assumed  730 h × $0.0104 Instance hours (1 × t3.micro, Linux)
          aws_ecs_service.web            ECS                     $18.02  $18.02  365 vCPU-hour × $0.04048 vCPU hours (2 tasks × 0.25 vCPU, 0.5 GB)
          aws_lb.web                     Elastic Load Balancing  $16.43  $16.43  730 h × $0.0225 Load balancer hours (application)

Total        $34.45 a month
This plan saves $8.23 a month (from $42.68 to $34.45)

Not priced: aws_ecr_repository.app
These have no price in pogi yet, so the real bill could be higher.
```

Read it from the bottom up:
- **The sentence under the total** is the answer: what the plan adds or saves a month, and the cost before and after.
- **ACTION** says what Terraform will do: create, update, replace or delete.
- **Not priced** lists resources pogi has no price for. They add nothing to the total, so the real bill could be higher.

In the console, drop `plan.json` on the Terraform page. The diagram then colors each box by its change, and marks the resources that depend on a changed one as "affected". See [See what a plan changes](../how-to/see-what-a-plan-changes.md).

## What's next

- [Estimate Terraform cost](../how-to/estimate-terraform-cost.md): variable files, usage, pull requests and scripts.
- [How Terraform cost works](../explanation/how-terraform-cost-works.md): why a folder and a plan differ, and what the labels mean.
- [Terraform cost reference](../reference/terraform-cost.md): which resource types are priced.
