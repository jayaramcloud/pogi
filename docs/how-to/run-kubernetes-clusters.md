---
title: Run Kubernetes clusters
description: Create a cluster on pogi with AWS, Google Cloud or Azure calls, then use it with kubectl.
---

# Run Kubernetes clusters

pogi runs each cluster you create as a real Kubernetes API server (k3s, a small Kubernetes) in Docker, so `kubectl` works against it. You'll create a cluster on each cloud and see its node become Ready.

## Before you start

Start pogi in another terminal:

```bash
pogi start
```

Load the settings that point your tools at pogi:

```bash
eval "$(pogi env)"
```

You need Docker and `kubectl`, plus the AWS CLI for the AWS section, `gcloud` for the Google Cloud section and the Azure CLI (`az`) for the Azure section. Each cluster pulls the k3s image the first time, so the first one is slower. Every section deletes its cluster at the end.

To use `az` (the AKS section), sign it in to pogi too. Any client ID and secret work, and `AZURE_CONFIG_DIR` keeps your own `~/.azure` as it is. Run this in the shell where you run `az`. The steps and what each line does are in [Azure CLI](connect-your-tools.md#azure-cli):

```bash
eval "$(pogi env)"
export REQUESTS_CA_BUNDLE="$HOME/.pogi/certs/ca.pem"   # pogi's CA; `pogi cert` prints its path
export AZURE_CONFIG_DIR="$HOME/.pogi/azure"            # keeps your own ~/.azure as it is
az cloud show -n pogi > /dev/null 2>&1 || az cloud register -n pogi --endpoint-resource-manager https://localhost:7644/
az cloud set -n pogi --only-show-errors
az login --service-principal -u 00000000-0000-0000-0000-000000000001 -p secret \
  --tenant 00000000-0000-0000-0000-000000000000 --output none
```

## AWS: EKS

You'll make a cluster with the AWS CLI and read its nodes with `kubectl`.

1. Make a VPC and two subnets. pogi checks that the cluster's subnets are in one VPC:

   ```bash
   VPC=$(aws ec2 create-vpc --cidr-block 10.0.0.0/16 --query Vpc.VpcId --output text)
   S1=$(aws ec2 create-subnet --vpc-id "$VPC" --cidr-block 10.0.1.0/24 --availability-zone us-east-1a --query Subnet.SubnetId --output text)
   S2=$(aws ec2 create-subnet --vpc-id "$VPC" --cidr-block 10.0.2.0/24 --availability-zone us-east-1b --query Subnet.SubnetId --output text)
   echo "$VPC $S1 $S2"
   ```

   ```text title="Output"
   vpc-0eb07965d39f7b806 subnet-cb720af4668dee07b subnet-27fcb790e7d5d08c6
   ```

2. Create the cluster. The role can be any ARN (the ID AWS gives a resource):

   ```bash
   aws eks create-cluster --name demo \
     --role-arn arn:aws:iam::000000000000:role/eks \
     --resources-vpc-config subnetIds=$S1,$S2 \
     --query cluster.status --output text
   ```

   ```text title="Output"
   CREATING
   ```

3. Wait until it is active, then write a kubeconfig. This one goes to its own file, so your `~/.kube/config` stays as it is:

   ```bash
   aws eks wait cluster-active --name demo
   aws eks update-kubeconfig --name demo --kubeconfig eks.kubeconfig
   ```

   ```text title="Output (trimmed)"
   Added new context arn:aws:eks:us-east-1:000000000000:cluster/demo to …/eks.kubeconfig
   ```

4. List the nodes. `kubectl` gets its token by running `aws eks get-token`, so the AWS settings from `pogi env` must be loaded in this shell:

   ```bash
   kubectl --kubeconfig eks.kubeconfig get nodes
   ```

   ```text title="Output"
   NAME           STATUS   ROLES           AGE   VERSION
   aaec15180189   Ready    control-plane   25s   v1.35.5+k3s1
   ```

5. Delete the cluster. This removes its container:

   ```bash
   aws eks delete-cluster --name demo --query cluster.status --output text
   ```

   ```text title="Output"
   DELETING
   ```

## Google Cloud: GKE

You'll make a cluster with `gcloud` and read its nodes with `kubectl`.

1. Create the cluster. `gcloud` waits until the cluster is RUNNING, so this takes a moment. As on Google Cloud, it also adds the cluster to your kubeconfig (`~/.kube/config`); that entry needs the `gke-gcloud-auth-plugin`, so step 3 writes a separate file instead:

   ```bash
   gcloud container clusters create demo --location us-central1 --num-nodes 1 \
     --format "value(name,status)"
   ```

   ```text title="Output"
   Note: Your Pod address range (`--cluster-ipv4-cidr`) can accommodate at most 1008 node(s).
   WARNING: Service container.googleapis.com is not enabled. This operation may not succeed.
   Creating cluster demo in us-central1...
   ........................done.
   Created [http://localhost:7644/v1/projects/pogi/zones/us-central1/clusters/demo].
   To inspect the contents of your cluster, go to: https://console.cloud.google.com/kubernetes/workload_/gcloud/us-central1/demo?project=pogi
   kubeconfig entry generated for demo.
   demo	RUNNING
   ```

2. List the clusters. You don't need `--location`:

   ```bash
   gcloud container clusters list --format "value(name,status)"
   ```

   ```text title="Output"
   demo	RUNNING
   ```

3. Write a kubeconfig. `pogi kubeconfig gke` makes one that works without the `gke-gcloud-auth-plugin`:

   ```bash
   pogi kubeconfig gke demo --location us-central1 > gke.kubeconfig
   ```

4. List the nodes:

   ```bash
   kubectl --kubeconfig gke.kubeconfig get nodes
   ```

   ```text title="Output"
   NAME           STATUS   ROLES           AGE   VERSION
   e249999bd094   Ready    control-plane   3s    v1.35.5+k3s1
   ```

5. Delete the cluster. `gcloud` waits until it is gone, and its container goes away:

   ```bash
   gcloud container clusters delete demo --location us-central1 --quiet
   ```

   ```text title="Output"
   Deleting cluster demo...
   done.
   Deleted [http://localhost:7644/v1/projects/pogi/zones/us-central1/clusters/demo].
   ```

## Azure: AKS

You'll make a cluster with `az aks` (Azure Resource Manager, the API behind the Azure portal), then read its nodes with `kubectl`.

1. Create a resource group and the cluster:

   ```bash
   az group create -n demo -l eastus -o none
   az aks create -g demo -n demo --node-count 1 --no-ssh-key \
     --query provisioningState -o tsv
   ```

   ```text title="Output"
   Succeeded
   ```

2. Save the kubeconfig. This one goes to its own file, so your `~/.kube/config` stays as it is:

   ```bash
   az aks get-credentials -g demo -n demo --file aks.kubeconfig
   ```

   ```text title="Output"
   WARNING: Merged "demo" as current context in aks.kubeconfig
   ```

3. List the nodes:

   ```bash
   kubectl --kubeconfig aks.kubeconfig get nodes
   ```

   ```text title="Output"
   NAME           STATUS   ROLES           AGE   VERSION
   bf25142da618   Ready    control-plane   28s   v1.35.5+k3s1
   ```

4. Delete the cluster and the resource group:

   ```bash
   az aks delete -g demo -n demo --yes
   az group delete -n demo --yes
   ```

   It prints nothing.

## Limits

- A node group (EKS), node pool (GKE) or agent pool (AKS) is stored but adds no nodes. Each cluster has one node.
- pogi runs one Kubernetes version, the one in its k3s image, whatever version you ask for.
- An EKS cluster's endpoint is always `https://127.0.0.1:<port>`, so only your machine can reach it. Version upgrades are not supported.
- On GKE, every valid token is cluster-admin, and lists come back as one page.
- On AKS, the kubeconfig holds a fixed bearer token, not Microsoft Entra ID tokens, so `kubelogin` is not used.

The [coverage page](../reference/coverage.md#aws) lists every limit.

## See also

- [Run containers and VMs](run-containers-and-vms.md)
- [Connect your tools](connect-your-tools.md)
- [Coverage](../reference/coverage.md)
