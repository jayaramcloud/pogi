---
title: How Terraform cost works
description: How pogi reads Terraform, why it never runs it, and what known, assumed, usage and needs mean.
---

# How Terraform cost works

pogi turns Terraform code into a monthly bill in three steps:

1. **Read.** It gets a list of resources with their values, from a folder or from a plan.
2. **Price.** Each resource type has a rule that says which values matter (size, disk, count) and which prices to look up.
3. **Show.** It prints a table, or draws the table and a diagram in the console.

## A folder or a plan

pogi has two ways to read, and each is right at a different time.

| | Folder of `.tf` files | `plan.json` |
|---|---|---|
| What it is | The code you wrote. | What Terraform worked out it would do. |
| Needs | Nothing. No Terraform, no `init` for plain files, no network, no credentials. | You ran `terraform plan` and `terraform show -json`. |
| Values | pogi works them out from variables, locals, `count`, `for_each` and modules. A value that only exists after apply is unknown. | Every value is the one Terraform resolved, data sources included. |
| Shows | The cost of what the code describes. | The cost before and after, and what changes. |

Use a folder while you write, because it's instant. Use a plan before you apply, because it's exact and shows the change. A plan also covers what a folder can't: the result of a data source or a function pogi doesn't know.

When you give `pogi cost` a file, it checks that it is a plan (it looks for `format_version`, plus `planned_values`, `resource_changes` or `configuration`). Otherwise it says it is not one.

## Why pogi never runs Terraform

pogi reads files and does nothing else with them. It runs no Terraform, no provider, no script and no external data source.

- **Safety.** Running Terraform runs code from providers and modules. Reading files can't.
- **Nothing to set up.** There is no binary to install, no `init`, no cloud login and no network.
- **Your files stay put.** A folder you pick in the console is sent only to your own pogi. It reads the files in memory and keeps nothing.

The cost is that some values are unknown from a folder alone. pogi says so in a warning, for example "pogi doesn't run the function `file()`, so values made with it are unknown". A plan removes that gap.

## How a resource gets its region

Prices differ by region, so each resource needs one. pogi looks, in this order:

1. **AWS:** the resource's own `region`, then its provider block's `region`.
2. **Google Cloud:** the resource's `region`, then its `zone` or `location`, then the provider's `region` or `zone`.
3. **Azure:** the resource's `location`.
4. **A child takes its parent's.** For Azure and Google Cloud, a resource with no location of its own takes the one of a resource it refers to. An AKS node pool lives where its cluster does, and a SQL database where its server does. Chains such as database → server → resource group work too.
5. **Otherwise** the cloud's usual region (`us-east-1`, `us-central1` or `eastus`), and the row says "assumed region".

pogi never reads environment variables such as `AWS_REGION`, because a cost estimate should give the same answer on every machine.

## Known, assumed, usage and needs

Every row says how sure pogi is:

- **known:** every input came from your files.
- **assumed:** pogi used a default and told you which. It is a guess, so it is marked.
- **usage:** the price depends on use (requests, data moved, storage). pogi shows the unit price but counts $0 until you give numbers in a usage file.
- **needs:** a value pogi must have is missing or unknown, so it can't price the row.

Two rules sit behind these labels:

- **Never price silently as $0.** A resource that costs money but can't be priced shows as `needs`, `usage` or `unpriced`, not as zero.
- **Never claim more than was priced.** The total counts only what pogi priced. The output lists the rest, and says the real bill could be higher. For a plan, the change line says how many changed resources it couldn't price.

A provider's documented default isn't a guess, so it isn't marked "assumed".

## Why the prices are built in, and offline

Each release carries a snapshot of the clouds' own price lists: AWS's bulk price files and Azure's Retail Prices API. They are compressed into the program. That means:

- no account, no API key and no network, and the answer is the same on a plane;
- the same files give the same answer on every machine, and in CI;
- pogi never calls out for prices.

The cost is that prices age. The date of each snapshot is in the header of every estimate, and the clouds change prices rarely. New prices arrive with each release.

<!-- gcp-prices -->
The Google Cloud prices are not included yet. Until they are, a Google Cloud resource shows as `unpriced`, with the note "pogi has no Google Cloud prices yet", and is listed under "Not priced". Free Google Cloud types still show as free.

No price is typed into pogi's code. A generator reads each cloud's price list and writes the snapshot, and the pricing rules only look prices up.

## How fixtures keep prices honest

The `examples/cost/` folder in the repository holds sample stacks. Each has an `EXPECTED.md` with the cost worked out by hand from the cloud's pricing pages, row by row. A test prices each folder with the real parser and the real price data, and fails if the total differs from the hand-worked one. So a change to a rule or to the price data shows up as a failing test, not as a surprise on someone's bill.

## See also

- [Estimate Terraform cost](../how-to/estimate-terraform-cost.md)
- [Terraform cost reference](../reference/terraform-cost.md)
- [See what a plan changes](../how-to/see-what-a-plan-changes.md)
