---
title: See what a plan changes
description: "Read the Terraform page in the console, or draw it from the command line: resources, what depends on what, and what a plan creates, updates, replaces or deletes."
---

# See what a plan changes

The Terraform page in the console draws the resources of a Terraform folder or plan, and the references between them. With a plan, it also shows what the plan does to each resource.

You need pogi running, and a folder or a `plan.json`. To make a plan, see [Price a plan](estimate-terraform-cost.md#price-a-plan).

## Open the page

1. Choose **Terraform** in the console sidebar, under Tools, or open `http://localhost:7644/_pogi/terraform`.
2. Choose a folder or a plan: **Choose folder**, **Choose plan.json**, or drop one on the page. **Try a sample** uses a built-in stack.

pogi keeps the last stack that loaded, in memory and never on disk. A reload, the Cost page's Terraform tab and the Terraform page all show the same stack. If a new folder fails to load, the old stack stays. The Cost page's Terraform tab has the table and the totals, and an **Open diagram** link to the Terraform page.

pogi draws the diagram on the server, so the same files always give the same picture. It shows:

- **One box per resource**, with its type and its monthly cost. A resource with `count` or `for_each` is one box marked `×N`.
- **A lane per cloud**, and a dashed band for each module.
- **Arrows** run from a resource to the ones that use it, so foundations such as a network sit on the left.
- **A "?" or a "+"** on a cost means a price is missing: "?" when no price is known, "+" when the cost is at least that much.
- **Not shown:** data sources, because they cost nothing and their arrows add noise.

A module with more than 40 resources starts as one box. Click it to draw it in full.

## Group by network

**Group by** above the diagram offers **Module** (the default) or **Network**. Network draws frames in place of module bands:
- **AWS:** a VPC, with its subnets inside.
- **Azure:** a virtual network, with its subnets inside.
- **Google Cloud:** a network, with its subnetworks inside.

A box sits in the innermost frame among the resources it refers to directly, for example through `subnet_id`.

- If it refers to two subnets of one network, it sits in the network's frame.
- If it refers to two different networks, or to none, it sits in its cloud's lane, outside the frames.
- A collapsed module is a box in the lane too.

A network or a subnet is a frame, not a box. Its label selects it like a box, and no arrow runs from a frame to what is inside it. Other arrows stay, and end at the frame's label.

## Read the change view

Open a `plan.json`. Each box takes the color of what the plan does to it:

| Color | Means |
|---|---|
| create | The plan adds it. |
| update | The plan changes it in place. |
| replace | The plan deletes it and makes a new one. |
| delete | The plan removes it. Its cost shows as a negative amount. |
| affected | The plan does not change it, but it depends on something that changes. |
| unchanged | Nothing happens to it. |

The line above the diagram counts them, for example "3 to create, 1 to replace, 4 affected". The table has an action for each row and a before and after price. The totals at the top show the cost before the plan, after it, and the change.

If the plan changes resources pogi can't price, a banner lists them and says the change doesn't count them.

## See what depends on a resource

Click a box. The boxes and arrows that depend on it, directly or through others, stay bright, and the rest fade. A bar under the diagram names the box, and a panel beside it shows the resource: its type, file and line, price lines and notes.

To clear the selection, click the same box again, click empty space, choose **Clear** or **Close**, or press `Esc`. Pan, zoom and the selected box stay when you change a variable file or the grouping.

## Read the two kinds of line

A solid arrow means "depends on": it runs from a resource to one whose Terraform code refers to it.

A dotted line means "data flows": events or messages move from one resource to another, for example from an SNS topic to the queue that subscribes to it. The dashes move the way the data flows.

pogi draws a line, and no box, for a resource that only wires two others together:
- an SNS subscription
- a Lambda event source mapping
- an S3 bucket notification
- an EventBridge target
- a Pub/Sub or Event Grid subscription

Click the line to see that resource in the side panel. Two cases still get a box. A pull subscription (Pub/Sub, Service Bus) holds messages, so it stays a box, with a line from its topic. If pogi can't tell which resource is the source and which is the target, it also draws a box.

If the stack is also running in pogi, moving dots show the lines that carried traffic in the last 10 seconds. Dots in the error color show failed deliveries. pogi matches a line to a running resource by the ARN (the ID AWS gives a resource) or id in the files, or by its name and region. It never guesses: a name that fits two resources lights nothing.

The dots refresh without redrawing, so pan, zoom and the selection stay. The SVG and PNG exports show the dotted lines, still. With reduced motion turned on in your system, the lines stay dotted and still and the dots are hidden.

## Pan and zoom

- Drag to pan. Scroll to zoom at the pointer.
- **+**, **−** and **Fit** are above the diagram.
- With the diagram focused, the arrow keys pan, `+` and `-` zoom, `0` fits it, and `Tab` moves between boxes. `Enter` selects one.

## Draw it from the command line

`pogi tf diagram [path]` sends a folder or a `plan.json` to the running pogi and opens the Terraform page in your browser. With no path it uses the current folder.

```bash
pogi tf diagram ./infra
pogi tf diagram plan.json --group network
pogi tf diagram ./infra --no-open   # prints the address, does not open the browser
```

To get a file instead of a page, add `-o`. This needs no running pogi:

```bash
pogi tf diagram ./infra -o diagram.svg
```

Without `-o`, if pogi isn't running, the command stops with ``pogi isn't running at http://localhost:7644; start it with `pogi start`, or write a file with -o``. For every flag, see the [CLI reference](../reference/cli.md#pogi-tf-diagram-draw-a-terraform-folder).

## Export

Above the diagram, choose:
- **SVG** for the diagram as a file. It uses the light theme, whatever the console shows.
- **PNG** for the same picture as an image.
- **JSON** or **Markdown** for the estimate. These are the same as `pogi cost --format json` and `--format markdown`.

Files are named `pogi-terraform-diagram.svg`, `pogi-terraform-diagram.png`, `pogi-terraform-cost.json` and `pogi-terraform-cost.md`.

## See also

- [Estimate Terraform cost](estimate-terraform-cost.md)
- [How Terraform cost works](../explanation/how-terraform-cost-works.md)
