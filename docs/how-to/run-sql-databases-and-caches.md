---
title: Run SQL databases and caches
description: Create a PostgreSQL database and a Redis cache on pogi with AWS, Google Cloud and Azure calls, and connect to them.
---

# Run SQL databases and caches

pogi runs each database and cache you create as a real server (PostgreSQL or Redis) in Docker. You'll create one of each and run a query or a `ping`.

## Before you start

Start pogi in another terminal:

```bash
pogi start
```

Load the settings that point your tools at pogi:

```bash
eval "$(pogi env)"
```

To use `az` (the Azure section), sign it in to pogi too. Any client ID and secret work, and `AZURE_CONFIG_DIR` keeps your own `~/.azure` as it is. Run this in the shell where you run `az`. The steps and what each line does are in [Azure CLI](connect-your-tools.md#azure-cli):

```bash
eval "$(pogi env)"
export REQUESTS_CA_BUNDLE="$HOME/.pogi/certs/ca.pem"   # pogi's CA; `pogi cert` prints its path
export AZURE_CONFIG_DIR="$HOME/.pogi/azure"            # keeps your own ~/.azure as it is
az cloud show -n pogi > /dev/null 2>&1 || az cloud register -n pogi --endpoint-resource-manager https://localhost:7644/
az cloud set -n pogi --only-show-errors
az login --service-principal -u 00000000-0000-0000-0000-000000000001 -p secret \
  --tenant 00000000-0000-0000-0000-000000000000 --output none
```

You need Docker, plus the AWS CLI for the AWS sections, `gcloud` for the Google Cloud sections, and the Azure CLI (`az`) for the Azure section. The first server of each kind pulls its image, so it takes longer than the next ones. Every section deletes what it made at the end.

Each instance shows its address, which is `localhost` or `127.0.0.1` and a port pogi picked. A client on your machine, such as `psql` or `redis-cli`, connects there. These steps use `docker exec` to run the client inside the server's container instead, so you don't need either one installed.

## AWS: RDS

You'll make a PostgreSQL instance and run `select 1` on it.

1. Create the instance:

   ```bash
   aws rds create-db-instance --db-instance-identifier demo --engine postgres \
     --db-instance-class db.t3.micro --allocated-storage 20 \
     --master-username pogi --master-user-password secret123 \
     --query DBInstance.DBInstanceStatus --output text
   ```

   ```text title="Output"
   creating
   ```

2. Wait until it is available, then read its address and port:

   ```bash
   aws rds wait db-instance-available --db-instance-identifier demo
   aws rds describe-db-instances --db-instance-identifier demo \
     --query 'DBInstances[0].[DBInstanceStatus,Endpoint.Address,Endpoint.Port]' --output text
   ```

   ```text title="Output"
   available	localhost	61016
   ```

3. Run a query. The container is named `pogi-rds-<identifier>`:

   ```bash
   docker exec -e PGPASSWORD=secret123 pogi-rds-demo psql -U pogi -h 127.0.0.1 -d postgres -c 'select 1 as one'
   ```

   ```text title="Output"
    one 
   -----
      1
   (1 row)
   ```

4. Delete the instance:

   ```bash
   aws rds delete-db-instance --db-instance-identifier demo --skip-final-snapshot \
     --query DBInstance.DBInstanceStatus --output text
   ```

   ```text title="Output"
   deleting
   ```

## AWS: ElastiCache

You'll make a Redis cluster and `ping` it.

1. Create the cluster:

   ```bash
   aws elasticache create-cache-cluster --cache-cluster-id demo --engine redis \
     --cache-node-type cache.t3.micro --num-cache-nodes 1 \
     --query CacheCluster.CacheClusterStatus --output text
   ```

   ```text title="Output"
   creating
   ```

2. Wait until it is available:

   ```bash
   aws elasticache wait cache-cluster-available --cache-cluster-id demo
   ```

3. Send a `ping`. The container is named `pogi-elasticache-<id>`:

   ```bash
   docker exec pogi-elasticache-demo redis-cli ping
   ```

   ```text title="Output"
   PONG
   ```

4. Delete the cluster:

   ```bash
   aws elasticache delete-cache-cluster --cache-cluster-id demo \
     --query CacheCluster.CacheClusterStatus --output text
   ```

   ```text title="Output"
   deleting
   ```

## Google Cloud: Cloud SQL

You'll make a PostgreSQL instance with `gcloud` and run `select 1` on it.

1. Create the instance:

   ```bash
   gcloud sql instances create demo --database-version POSTGRES_16 --tier db-f1-micro \
     --region us-central1 --root-password secret123
   ```

   ```text title="Output"
   Creating Cloud SQL instance for POSTGRES_16...
   .done.
   Created [http://localhost:7644/v1beta4/projects/pogi/instances/demo].
   NAME  DATABASE_VERSION  LOCATION       TIER         PRIMARY_ADDRESS  PRIVATE_ADDRESS  STATUS
   demo  POSTGRES_16       us-central1-a  db-f1-micro  127.0.0.1:61187  -                RUNNABLE
   ```

2. Read its state and address. The address is `127.0.0.1` and a port pogi picked:

   ```bash
   gcloud sql instances describe demo --format "value(state,connectionName,ipAddresses)"
   ```

   ```text title="Output"
   RUNNABLE	pogi:us-central1:demo	{'ipAddress': '127.0.0.1:61187', 'type': 'PRIMARY'}
   ```

3. Run a query inside the container. The user is `postgres`:

   ```bash
   docker exec -e PGPASSWORD=secret123 \
     $(docker ps -q --filter label=pogi.service=cloudsql --filter label=pogi.resource=demo) \
     psql -U postgres -h 127.0.0.1 -c 'select 1 as one'
   ```

   ```text title="Output"
    one 
   -----
      1
   (1 row)
   ```

4. Delete the instance:

   ```bash
   gcloud sql instances delete demo --quiet
   ```

   ```text title="Output"
   Deleting Cloud SQL instance...
   .done.
   Deleted [http://localhost:7644/v1beta4/projects/pogi/instances/demo].
   ```

## Google Cloud: Memorystore

You'll make a Redis instance with `gcloud` and `ping` it. `pogi env` already points `gcloud redis` at pogi's `/redis/` prefix.

1. Create the instance:

   ```bash
   gcloud redis instances create demo --region us-central1 --size 1
   ```

   ```text title="Output"
   Create request issued for: [demo]
   Waiting for operation [projects/pogi/locations/us-central1/operations/operation-9ec2ea057b528767202a4c31fbb4c39a] to complete...
   .....done.
   Created instance [demo].
   ```

2. Read its state, host and port:

   ```bash
   gcloud redis instances describe demo --region us-central1 --format "value(state,host,port)"
   ```

   ```text title="Output"
   READY	127.0.0.1	61257
   ```

3. Send a `ping` inside the container:

   ```bash
   docker exec $(docker ps -q --filter label=pogi.service=memorystore --filter label=pogi.resource=us-central1-demo) redis-cli ping
   ```

   ```text title="Output"
   PONG
   ```

4. Delete the instance:

   ```bash
   gcloud redis instances delete demo --region us-central1 --quiet
   ```

   ```text title="Output"
   Delete request issued for: [demo]
   Waiting for operation [projects/pogi/locations/us-central1/operations/operation-3017512f5a8d53aee74c27fe839adace] to complete...
   .....done.
   Deleted instance [demo].
   ```

## Azure: Database for PostgreSQL

You'll make a flexible server with `az` and run `select 1` on it. Sign `az` in to pogi first, as [Connect your tools](connect-your-tools.md#azure-cli) shows. `az mysql flexible-server create` makes a MySQL server the same way.

1. Create a resource group and the server:

   ```bash
   az group create -n demo -l eastus -o none
   az postgres flexible-server create -g demo -n pgdemo -l eastus --admin-user pgadmin \
     --admin-password Secret123pw --public-access None --yes
   ```

   ```text title="Output (trimmed)"
   …
   {
     "connectionString": "postgresql://pgadmin:Secret123pw@localhost/postgres?sslmode=require",
     "databaseName": "postgres",
     "host": "localhost",
     …
     "skuname": "Standard_D2ds_v5",
     "username": "pgadmin",
     "version": "18"
   }
   ```

   Without `--version` and `--sku-name`, `az` picks the newest version pogi lists and the first General Purpose size. pogi runs every size as the same container.

2. Wait until the server is Ready. `az` returns as soon as pogi accepts the server, while its container may still be starting:

   ```bash
   for i in $(seq 90); do
     state=$(az postgres flexible-server show -g demo -n pgdemo --query state -o tsv)
     [ "$state" = Ready ] && break
     sleep 2
   done
   echo "$state"
   ```

   ```text title="Output"
   Ready
   ```

3. Run a query inside the container. The container is named `pogi-azdatabases-pg-<server>-<resource group>`:

   ```bash
   docker exec -e PGPASSWORD=Secret123pw pogi-azdatabases-pg-pgdemo-demo \
     psql -U pgadmin -h 127.0.0.1 -d postgres -c 'select 1 as one'
   ```

   ```text title="Output"
    one 
   -----
      1
   (1 row)
   ```

   The connection string `az` prints has no port. To connect from your machine, ask Docker for the port pogi picked:

   ```bash
   docker port pogi-azdatabases-pg-pgdemo-demo 5432
   ```

   ```text title="Output"
   127.0.0.1:61294
   ```

4. Delete the server and the resource group:

   ```bash
   az postgres flexible-server delete -g demo -n pgdemo --yes
   az group delete -n demo --yes
   ```

   It prints nothing.

## Limits

- **RDS and ElastiCache:** snapshot restore, read replicas, and engine parameters are not supported. A parameter group is stored, but the engine does not use it.
- **Cloud SQL:** the Auth Proxy, TLS, IAM database authentication, CSV and BAK import, replicas, backups and failover are not supported.
- **Memorystore:** the container is always Redis 7, whatever `redisVersion` says. A `STANDARD_HA` instance and read replicas run as one server. TLS, persistence, import, export and failover are not supported.
- **Azure Database for PostgreSQL:** server configurations are stored but not applied, and there is no TLS.

The [coverage page](../reference/coverage.md#aws) lists every limit.

## See also

- [Run Kubernetes clusters](run-kubernetes-clusters.md)
- [Connect your tools](connect-your-tools.md)
- [Coverage](../reference/coverage.md)
