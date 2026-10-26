---
title: Use any service
description: Find out what a pogi service does, then create, read and list a resource on each cloud.
---

# Use any service

pogi serves hundreds of services, and you call each one with the tool you already use. You'll look a service up, then make and read back a resource on each cloud, using DNS and one long-tail AWS service.

## Find out what a service does

Every service has a depth. Deep and Core services have real behavior. A Thin service stores your resources and reads them back, but nothing runs behind them. Terraform and SDK calls work against a Thin service, and that is often all you need.

To look a service up:

- Run `pogi services`. It lists the running services, with the name each one goes by (the service id), its title, and how many resources and requests it has.
- Open the [coverage page](../reference/coverage.md). Each row names a service, its depth, what works and what does not yet.

Before you start, run `pogi start` in another terminal. Then load the settings that point your tools at pogi. You need the AWS CLI, `gcloud` and the Azure CLI (`az`). The lines after `eval` sign `az` in to pogi, for the Azure section. [Azure CLI](connect-your-tools.md#azure-cli) explains them, and you can run them again.

```bash
eval "$(pogi env)"
export REQUESTS_CA_BUNDLE="$HOME/.pogi/certs/ca.pem"   # pogi's CA; `pogi cert` prints its path
export AZURE_CONFIG_DIR="$HOME/.pogi/azure"            # keeps your own ~/.azure as it is
az cloud show -n pogi > /dev/null 2>&1 || az cloud register -n pogi --endpoint-resource-manager https://localhost:7644/
az cloud set -n pogi --only-show-errors
az login --service-principal -u 00000000-0000-0000-0000-000000000001 -p secret \
  --tenant 00000000-0000-0000-0000-000000000000 --output none
```

Find the DNS services:

```bash
pogi services | grep -E "SERVICE|route53 |dns |azdns"
```

```text title="Output"
CLOUD  SERVICE                          TITLE                                                  RESOURCES  REQUESTS
aws    route53                          Route 53                                               0          0
gcp    dns                              Cloud DNS                                              0          0
azure  azdns                            DNS                                                    0          0
```

The coverage page lists all three DNS services as Thin, and the long-tail AWS services are Thin too.

To see what pogi holds for a service, run `pogi ls <service id>`. The web console shows the same lists, under the Resources page for each service, at `http://localhost:7644/_pogi/`.

## AWS: Route 53

You'll make a hosted zone (a container for the DNS records of one domain), add a record, and read both back.

1. Create the zone:

   ```bash
   ZONE=$(aws route53 create-hosted-zone --name example.test --caller-reference demo-1 --query 'HostedZone.Id' --output text)
   echo $ZONE
   ```

   ```text title="Output"
   /hostedzone/ZGES7ITBN6FPEIZJNQOKY
   ```

2. Add an `A` record:

   ```bash
   aws route53 change-resource-record-sets --hosted-zone-id $ZONE --change-batch '{"Changes":[{"Action":"CREATE","ResourceRecordSet":{"Name":"www.example.test","Type":"A","TTL":60,"ResourceRecords":[{"Value":"192.0.2.10"}]}}]}' --query 'ChangeInfo.Status' --output text
   ```

   ```text title="Output"
   INSYNC
   ```

3. Read the record back:

   ```bash
   aws route53 list-resource-record-sets --hosted-zone-id $ZONE --query 'ResourceRecordSets[?Type==`A`].[Name,Type,ResourceRecords[0].Value]' --output text
   ```

   ```text title="Output"
   www.example.test.	A	192.0.2.10
   ```

4. List the zones:

   ```bash
   aws route53 list-hosted-zones --query 'HostedZones[].Name' --output text
   ```

   ```text title="Output"
   example.test.
   ```

5. See the zone in pogi. In the console, open **Route 53** under "In use" on the Resources page. In the terminal, run:

   ```bash
   pogi ls route53
   ```

   ```text title="Output"
   route53 · Hosted zones (1)
     ZONE                   NAME           RECORDS  PRIVATE  ACCOUNT
     ZGES7ITBN6FPEIZJNQOKY  example.test.  3        false    000000000000

   route53 · Health checks (0)
   ```

## AWS: a long-tail service

The long-tail services are the AWS services that have no row of their own on the coverage page. pogi builds them from AWS's published API descriptions. [AWS long-tail services](../reference/aws-long-tail.md) lists them, and what a create, get, list and delete run finds for each. They are Thin: Terraform and SDK calls work, and nothing runs behind them.

This example uses License Manager. You'll make a license configuration and read it back.

1. Create it:

   ```bash
   aws license-manager create-license-configuration --name demo --license-counting-type vCPU
   ```

   ```json title="Output"
   {
       "LicenseConfigurationArn": "arn:aws:license-manager:us-east-1:000000000000:license-configuration/7bae5603-903b-4fa1-8f97-37cdce43806f"
   }
   ```

2. List it:

   ```bash
   aws license-manager list-license-configurations --query 'LicenseConfigurations[].[Name,LicenseCountingType]' --output text
   ```

   ```text title="Output"
   demo	vCPU
   ```

3. See it in pogi. In the console, open **AWS License Manager** under "In use" on the Resources page. In the terminal, run:

   ```bash
   pogi ls license-manager
   ```

   ```text title="Output"
   license-manager · LicenseConfiguration (1)
     ID                                                                                                         NAME  REGION     CREATED
     arn:aws:license-manager:us-east-1:000000000000:license-configuration/7bae5603-903b-4fa1-8f97-37cdce43806f  demo  us-east-1  2026-10-26 08:54:13
   ```

## Google Cloud: Cloud DNS

You'll make a managed zone and read it back. `pogi env` points `gcloud` at pogi and sets the project to `pogi`.

1. Create the zone:

   ```bash
   gcloud dns managed-zones create demo --dns-name=example.test. --description="demo zone"
   ```

   ```text title="Output"
   Created [http://localhost:7644/projects/pogi/managedZones/demo].
   ```

2. Add a record:

   ```bash
   gcloud dns record-sets create www.example.test. --zone=demo --type=A --ttl=60 --rrdatas=192.0.2.10
   ```

   ```text title="Output"
   NAME               TYPE  TTL  DATA
   www.example.test.  A     60   192.0.2.10
   ```

3. List the zones and the records:

   ```bash
   gcloud dns managed-zones list
   gcloud dns record-sets list --zone=demo
   ```

   ```text title="Output"
   NAME  DNS_NAME       DESCRIPTION  VISIBILITY
   demo  example.test.  demo zone    public
   NAME               TYPE  TTL  DATA
   www.example.test.  A     60   192.0.2.10
   ```

4. See the zone in pogi. In the console, open **Cloud DNS** under "In use" on the Resources page. In the terminal, run `pogi ls dns`.

## Azure: DNS zones

You'll make a resource group and a DNS zone with `az`, add a record, and read the zone back. `az` calls Azure Resource Manager (ARM, Azure's control API), which is why the sign-in block in "Before you start" matters.

1. Make a resource group:

   ```bash
   az group create --name demo --location eastus --query '{name:name,state:properties.provisioningState}'
   ```

   ```json title="Output"
   {
     "name": "demo",
     "state": "Succeeded"
   }
   ```

2. Make the zone:

   ```bash
   az network dns zone create --resource-group demo --name example.test --query '{name:name,type:zoneType}'
   ```

   ```json title="Output"
   {
     "name": "example.test",
     "type": "Public"
   }
   ```

3. Add an `A` record:

   ```bash
   az network dns record-set a add-record --resource-group demo --zone-name example.test --record-set-name www --ipv4-address 192.0.2.10 --query '{name:fqdn,ttl:TTL}'
   ```

   ```json title="Output"
   {
     "name": "www.example.test.",
     "ttl": 3600
   }
   ```

4. List the zones in the group:

   ```bash
   az network dns zone list --resource-group demo --query '[].name' --output tsv
   ```

   ```text title="Output"
   example.test
   ```

5. See the zone in pogi. In the console, open **DNS** under "In use" on the Resources page. In the terminal, run `pogi ls azdns`.

## Limits

- A Thin service stores what you create and reads it back. No DNS server answers queries for your zones, on any cloud.
- Azure DNS does not check a record's properties, and it ignores ETag conditions.
- Cloud DNS has no default NS and SOA records in a new zone. Policies, response policies and DNSSEC keys are only stored.
- A long-tail service gives you create, describe, list, update and tags, plus the checks and errors from AWS's API description. It adds no behavior beyond that. A few long-tail services are listed as "no inferable resource" on that page: its test could not pick a main resource to try.

Each service's own row on the [coverage page](../reference/coverage.md) lists its limits.

## See also

- [Connect your tools](connect-your-tools.md)
- [Coverage](../reference/coverage.md)
- [AWS long-tail services](../reference/aws-long-tail.md)
- [Console](../reference/console.md#resources)
