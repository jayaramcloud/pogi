---
title: Run containers and VMs
description: Push an image, run containers and start virtual machines on pogi with AWS, Google Cloud and Azure calls.
---

# Run containers and VMs

pogi runs your containers and virtual machines (VMs) as real Docker containers. You'll push an image, run a task, start a VM and run a web container, on each cloud.

## Before you start

Start pogi in another terminal:

```bash
pogi start
```

Load the settings that point your tools at pogi:

```bash
eval "$(pogi env)"
```

To use `az` (the Container Instances section), sign it in to pogi too. Any client ID and secret work, and `AZURE_CONFIG_DIR` keeps your own `~/.azure` as it is. Run this in the shell where you run `az`. The steps and what each line does are in [Azure CLI](connect-your-tools.md#azure-cli):

```bash
eval "$(pogi env)"
export REQUESTS_CA_BUNDLE="$HOME/.pogi/certs/ca.pem"   # pogi's CA; `pogi cert` prints its path
export AZURE_CONFIG_DIR="$HOME/.pogi/azure"            # keeps your own ~/.azure as it is
az cloud show -n pogi > /dev/null 2>&1 || az cloud register -n pogi --endpoint-resource-manager https://localhost:7644/
az cloud set -n pogi --only-show-errors
az login --service-principal -u 00000000-0000-0000-0000-000000000001 -p secret \
  --tenant 00000000-0000-0000-0000-000000000000 --output none
```

You need Docker, plus the AWS CLI for the AWS sections, `gcloud` for the Google Cloud sections, and the Azure CLI (`az`) and `curl` for Azure. The first run of each image pulls it. Every section deletes what it made at the end.

## AWS: ECR and ECS

You'll push an image to a registry (ECR), run it as a task (ECS), and read what it wrote to its log.

1. Create a repository. pogi runs a real registry at `localhost:4510`:

   ```bash
   aws ecr create-repository --repository-name demo --query repository.repositoryUri --output text
   ```

   ```text title="Output"
   localhost:4510/demo
   ```

2. Log in, then tag and push a small public image. `-q` keeps `docker` from printing every layer:

   ```bash
   aws ecr get-login-password | docker login --username AWS --password-stdin localhost:4510
   docker pull -q alpine
   docker tag alpine localhost:4510/demo:v1
   docker push -q localhost:4510/demo:v1
   ```

   ```text title="Output (trimmed)"
   Login Succeeded
   docker.io/library/alpine:latest
   localhost:4510/demo:v1
   …
   ```

   Docker Desktop may add an `Info` line saying it pushed only the image for your platform.

3. List the image:

   ```bash
   aws ecr describe-images --repository-name demo --query 'imageDetails[].[imageTags[0],imageSizeInBytes]' --output text
   ```

   ```text title="Output"
   v1	4190488
   ```

4. Make a cluster, a log group and a task definition. The task runs `echo` in the image you pushed and sends its output to the log group:

   ```bash
   aws ecs create-cluster --cluster-name demo --query cluster.status --output text
   aws logs create-log-group --log-group-name /ecs/hello
   aws ecs register-task-definition --family hello --query taskDefinition.taskDefinitionArn --output text \
     --container-definitions '[{"name":"hello","image":"localhost:4510/demo:v1","command":["echo","hello from ecs"],"essential":true,"memory":64,"logConfiguration":{"logDriver":"awslogs","options":{"awslogs-group":"/ecs/hello","awslogs-region":"us-east-1","awslogs-stream-prefix":"demo"}}}]'
   ```

   ```text title="Output"
   ACTIVE
   arn:aws:ecs:us-east-1:000000000000:task-definition/hello:1
   ```

5. Run the task and wait for it to stop:

   ```bash
   TASK=$(aws ecs run-task --cluster demo --task-definition hello --query 'tasks[0].taskArn' --output text)
   aws ecs wait tasks-stopped --cluster demo --tasks "$TASK"
   aws ecs describe-tasks --cluster demo --tasks "$TASK" --query 'tasks[0].[lastStatus,containers[0].exitCode]' --output text
   ```

   ```text title="Output"
   STOPPED	0
   ```

6. Read the log:

   ```bash
   STREAM=$(aws logs describe-log-streams --log-group-name /ecs/hello --query 'logStreams[0].logStreamName' --output text)
   aws logs get-log-events --log-group-name /ecs/hello --log-stream-name "$STREAM" --query 'events[].message' --output text
   ```

   ```text title="Output"
   hello from ecs
   ```

7. Clean up:

   ```bash
   aws ecs delete-cluster --cluster demo --query cluster.status --output text
   aws ecs deregister-task-definition --task-definition hello:1 --query taskDefinition.status --output text
   aws logs delete-log-group --log-group-name /ecs/hello
   aws ecr delete-repository --repository-name demo --force --query repository.repositoryName --output text
   docker rmi localhost:4510/demo:v1
   ```

   ```text title="Output"
   INACTIVE
   INACTIVE
   demo
   Untagged: localhost:4510/demo:v1
   ```

## AWS: EC2

You'll start an instance, see it running, and end it. pogi runs an instance as a container made from the image that matches its AMI (the machine image ID).

1. Pick an AMI. pogi has a small catalog:

   ```bash
   aws ec2 describe-images --query 'Images[].[ImageId,Name]' --output text
   ```

   ```text title="Output"
   ami-0a1b2c3d4e5f60001	al2023-ami-2023.6.20250101.0-kernel-6.1-x86_64
   ami-0a1b2c3d4e5f60002	ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-20250101
   ami-0a1b2c3d4e5f60003	ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-20250101
   ami-0a1b2c3d4e5f60004	amzn2-ami-kernel-5.10-hvm-2.0.20250101.0-x86_64-gp2
   ami-0a1b2c3d4e5f60005	debian-12-amd64-20250101-1980
   ami-0a1b2c3d4e5f60006	alpine-3.21.0-x86_64-bios-cloudinit-r0
   ```

2. Start an instance from the Alpine image:

   ```bash
   INSTANCE=$(aws ec2 run-instances --image-id ami-0a1b2c3d4e5f60006 --instance-type t3.micro \
     --query 'Instances[0].InstanceId' --output text)
   aws ec2 wait instance-running --instance-ids "$INSTANCE"
   aws ec2 describe-instances --instance-ids "$INSTANCE" \
     --query 'Reservations[0].Instances[0].[State.Name,InstanceType,ImageId]' --output text
   ```

   ```text title="Output"
   running	t3.micro	ami-0a1b2c3d4e5f60006
   ```

3. Run a command inside it. The container is named `pogi-ec2-<instance id>`:

   ```bash
   docker exec "pogi-ec2-$INSTANCE" cat /etc/alpine-release
   ```

   ```text title="Output"
   3.24.3
   ```

4. Terminate it:

   ```bash
   aws ec2 terminate-instances --instance-ids "$INSTANCE" --query 'TerminatingInstances[0].CurrentState.Name' --output text
   ```

   ```text title="Output"
   shutting-down
   ```

## Google Cloud: Cloud Run

You'll deploy a web container with `gcloud run deploy` and call it. The container starts on the first request. `pogi env` points `gcloud run` at `run.localhost`, because `gcloud` puts the region in front of that name (`us-central1-run.localhost`), and `*.localhost` names reach your own machine on macOS and most Linux systems. `pogi doctor` checks yours.

1. Deploy the service:

   ```bash
   gcloud run deploy hello --image nginx:alpine --port 80 --region us-central1 --allow-unauthenticated
   ```

   ```text title="Output"
   Enabling APIs on project [pogi]...
   Deploying container to Cloud Run service [hello] in project [pogi] region [us-central1]
   Deploying new service...
   Setting IAM Policy.....done
   Done.
   Service [hello] revision [hello-00001-uqj] has been deployed and is serving 100 percent of traffic.
   Service URL: http://hello.run.localhost:7644
   ```

2. List the services and read the address:

   ```bash
   gcloud run services list --region us-central1
   gcloud run services describe hello --region us-central1 --format "value(status.url)"
   ```

   ```text title="Output"
      SERVICE  REGION       URL                               LAST DEPLOYED BY  LAST DEPLOYED AT
   ✔  hello    us-central1  http://hello.run.localhost:7644                    2026-10-26T09:04:40Z
   http://hello.run.localhost:7644
   ```

3. Call it. The service answers at `<service>.run.localhost`:

   ```bash
   curl -s http://hello.run.localhost:7644/ | grep -o "<title>.*</title>"
   ```

   ```text title="Output"
   <title>Welcome to nginx!</title>
   ```

4. Delete the service. This stops its container:

   ```bash
   gcloud run services delete hello --region us-central1 --quiet
   ```

   ```text title="Output"
   Deleting [hello]...
   done.
   Deleted service [hello].
   ```

## Google Cloud: Compute Engine

You'll start a VM with `gcloud` and run a command inside it. pogi runs the VM as a container of its boot image.

1. Create the VM:

   ```bash
   gcloud compute instances create vm1 --zone us-central1-a --machine-type e2-micro \
     --image-family debian-12 --image-project debian-cloud
   ```

   ```text title="Output"
   There is no default service account for project pogi. Instance vm1 will not have scopes.
   Created [https://www.googleapis.com/compute/v1/projects/pogi/zones/us-central1-a/instances/vm1].
   NAME  ZONE           MACHINE_TYPE  PREEMPTIBLE  INTERNAL_IP  EXTERNAL_IP  STATUS
   vm1   us-central1-a  e2-micro                   10.128.0.2   34.100.0.3   RUNNING
   ```

2. Run a command inside it. The container's `pogi.resource` label is the instance's numeric ID:

   ```bash
   ID=$(gcloud compute instances describe vm1 --zone us-central1-a --format "value(id)")
   docker exec $(docker ps -q --filter label=pogi.service=compute --filter label=pogi.resource=$ID) head -1 /etc/os-release
   ```

   ```text title="Output"
   PRETTY_NAME="Debian GNU/Linux 12 (bookworm)"
   ```

3. Delete the VM:

   ```bash
   gcloud compute instances delete vm1 --zone us-central1-a --quiet
   ```

   ```text title="Output"
   Deleted [https://www.googleapis.com/compute/v1/projects/pogi/zones/us-central1-a/instances/vm1].
   ```

## Azure: Container Instances

You'll run a web container with `az container` (Azure Resource Manager, the API behind the Azure portal) and open it with `curl`.

1. Create a resource group and a container group:

   ```bash
   az group create -n demo -l eastus -o none
   az container create -g demo -n hello --image nginx:alpine --ports 80 \
     --dns-name-label hello --location eastus --os-type Linux \
     --query "{name:name,fqdn:ipAddress.fqdn}" -o tsv
   ```

   ```text title="Output"
   hello	hello.eastus.azurecontainer.localhost:7644
   ```

2. Check that the container is running:

   ```bash
   az container show -g demo -n hello --query instanceView.state -o tsv
   ```

   ```text title="Output"
   Running
   ```

3. Call it. The group answers at `<label>.<location>.azurecontainer.localhost`:

   ```bash
   curl -s http://hello.eastus.azurecontainer.localhost:7644/ | grep -o "<title>.*</title>"
   ```

   ```text title="Output"
   <title>Welcome to nginx!</title>
   ```

4. Delete the container group and the resource group:

   ```bash
   az container delete -g demo -n hello --yes -o none
   az group delete -n demo --yes
   ```

   It prints nothing.

## Limits

- **ECR:** one registry host serves every account, and unused image data is never cleaned up.
- **ECS:** CPU shares, task sets, ECS Exec, and EFS and FSx volumes are not supported.
- **Cloud Run:** worker pools, the tasks API, sidecars, scaling, secrets and volumes are not supported.
- **Compute Engine:** live migration, snapshots, instance templates, groups, load balancing and custom images are not supported. Networks carry no traffic, and firewall rules are only stored. Shutdown scripts and `startup-script-url` do not run.
- **Container Instances:** exec and attach, volumes, probes, GPUs, virtual networks, init containers and identities are not supported.

The [coverage page](../reference/coverage.md#aws) lists every limit.

## See also

- [Run Kubernetes clusters](run-kubernetes-clusters.md)
- [Run SQL databases and caches](run-sql-databases-and-caches.md)
- [Connect your tools](connect-your-tools.md)
- [Coverage](../reference/coverage.md)
