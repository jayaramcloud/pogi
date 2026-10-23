---
title: Estimate Terraform cost
description: Price a Terraform folder or plan with pogi cost or the console, add usage, try another size and share the result.
---

# Estimate Terraform cost

pogi works out what Terraform code would cost each month on the real cloud, at on-demand list prices, for 730 hours a month, in USD. It reads the files itself and never runs Terraform. For a first walk-through, see [Estimate a Terraform bill](../tutorials/terraform-cost.md).

## Price a folder

```bash
pogi cost ./infra
```

pogi reads the `.tf`, `.tf.json`, `.tfvars` and `.tfvars.json` files under the folder, and modules that `terraform init` downloaded into `.terraform/modules`. It does not need pogi to be running.

In the console, open **Cost**, then **Terraform**, and choose **Choose folder** (or drop the folder on the page). Your browser sends only the Terraform files, to your own pogi, which keeps nothing.

A module that is not downloaded, or a value pogi can't work out, shows as a warning. Run `terraform init` once so the modules are in the folder.

## Choose variable files

pogi always reads `terraform.tfvars` and `*.auto.tfvars`. Add more with `--var-file`, once per file. Later files win:

```bash
pogi cost ./infra --var-file prod.tfvars
```

A path is relative to the folder, or absolute. A file that isn't there is an error. In the console, the variable files in the folder show as chips above the table. Click one to turn it on or off.

A variable with no value anywhere shows as a warning. A resource that needs it can then show as `needs`.

## Price a plan

If you have a plan, pogi uses its values instead of working them out, so data sources and functions are exact:

```bash
terraform plan -out plan.out && terraform show -json plan.out > plan.json
pogi cost plan.json
```

The table gets an action column and a **before** and **after** price for each resource. A sentence under it says what the plan adds or saves. See [See what a plan changes](see-what-a-plan-changes.md).

## Add usage

Some prices depend on use: requests, data moved, storage. Without numbers, pogi shows the unit price and counts $0 (status `usage`). To get a usage file with every key a folder uses, ready to fill in:

```bash
pogi cost ./infra --usage-template > usage.yaml
```

```yaml title="usage.yaml"
aws_lb.app:
  monthly_lcu_hours: 0
aws_nat_gateway.main:
  monthly_gb_processed: 0
```

Fill in the numbers, then price again:

```bash
pogi cost ./infra --usage usage.yaml
```

The file is YAML. Each key is a resource address, and each value is a map of numbers. Keys are in snake_case. For VMs, `os: windows` switches the price from Linux. A number can't be negative, and a file over 1 MB is refused. An address that matches no resource gives a warning, and pogi ignores it.

The usage keys for each type are in the [reference](../reference/terraform-cost.md#usage-keys).

## Try another size

**Console.** On a row for a VM, node pool, database or cache, open the row and choose **Try another size**. A drawer lists every size in that row's region, cheapest first. Filter by vCPU, memory or name, and turn on older generations. **Pick** prices the view again with that size. A **what-if** marker shows on the row, and **Reset** puts every size back. The copy button gives the line for your file, such as `instance_type = "m7g.large"`.

**Command line.** Use `--size address=size`, once per resource:

```bash
pogi cost ./infra --size aws_instance.web_a=m7g.large
```

An address that isn't in the folder is an error.

To list sizes without a folder:

```bash
pogi cost sizes aws_instance --region eu-west-1 --vcpu 4 --memory 16
```

```text title="Output (trimmed)"
aws_instance sizes in eu-west-1, cheapest first (on-demand, 730 h/month, USD)

SIZE                 FAMILY     VCPU  MEMORY GB  GPU     $/HOUR      $/MONTH
t4g.xlarge           t4g           4         16         $0.1472      $107.46
t3a.xlarge           t3a           4         16         $0.1632      $119.14
```

The flags are `--region`, `--vcpu` (a minimum), `--memory` (a minimum, in GB), `--os windows` and `--old` for older generations. The AI agent tool `list_sizes` gives the same list.

Neither way changes your `.tf` file.

## Share in a pull request

```bash
pogi cost ./infra --format markdown > cost.md
```

The output is a table, the total, the notes behind each assumption and the basis line, ready to paste into a pull request comment.

## Use it in scripts

```bash
pogi cost ./infra --format json | jq .monthly
```

The JSON has `source`, `pricesPublished`, `rows`, `monthly`, `before` and `clouds`. Each row has `address`, `type`, `service`, `region`, `monthly`, `status`, `notes` and the `lines` that make up the price. For a plan, rows also have `action` and `before`.

Exit codes:

| Code | Meaning |
|---|---|
| `0` | pogi priced the files. This includes rows that need a value or have no price, so check `status` for those. |
| `1` | An error: no such path, a usage file that doesn't parse, an address that isn't in the folder, or too many files. The message goes to standard error. |
| `2` | A bad flag or flag value, such as `--format csv`. The message goes to standard error. |

## Read the statuses

| Status | Means |
|---|---|
| `known` | Every input came from your files. |
| `assumed` | pogi used a default and says which, such as "assumed Linux". Set the value in the `.tf` file or the usage file to remove the guess. |
| `usage` | The price depends on use, and none was given. pogi shows the unit price and counts $0. |
| `needs` | A value pogi must have is missing or unknown, such as `instance_type`. The row counts $0 and says what is missing. |
| `free` | Costs nothing on its own, such as a VPC or a security group. |
| `unpriced` | pogi has no price for this type. It adds nothing, so the real bill could be higher. |

## Ask an AI agent

The MCP tools `estimate_terraform_cost` and `list_sizes` give agents the same numbers. See [`pogi mcp`](../reference/cli.md#pogi-mcp-run-the-mcp-server-for-ai-agents).

## See also

- [Terraform cost reference](../reference/terraform-cost.md)
- [How Terraform cost works](../explanation/how-terraform-cost-works.md)
- [Estimate cost](estimate-cost.md): the bill for what is running in pogi
