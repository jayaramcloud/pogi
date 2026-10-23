---
title: Troubleshoot
description: Fix the problems people hit most when they start pogi or point their tools at it.
---

# Troubleshoot

## Start here: run `pogi doctor`

`pogi doctor` checks the server, Python and Node.js for Lambda, the AWS SDKs for native Lambda, Docker, `*.localhost` names and pogi's own certificate authority (CA).

```bash
pogi doctor
```

A line that starts with `✗` is a check that failed, and the line says how to fix it. Here, the AWS SDKs that native Lambda needs are not installed, and `pogi lambda-sdks` installs them.

```text title="Output"
  ✓ server at http://localhost:7644 0.2.0
  ✓ python3 for Python Lambdas: Python 3.9.6
  ✓ node for Node.js Lambdas: v24.15.0
  ✗ AWS SDKs for native Lambdas in ~/.pogi/lambda — run `pogi lambda-sdks` (CDK custom resources need them)
  ✓ Docker Desktop 29.9.1 at unix:///var/run/docker.sock (Engine API 1.54)
  ✓ *.localhost names resolve (pogi env --hostname aws.localhost, virtual-host S3, execute-api and lambda-url hosts)
  ✓ local CA ~/.pogi/certs/ca.pem
pogi: 1 check(s) need attention
```

- `✓` means the check passed.
- `✗` means something needs fixing. `pogi doctor` then exits with status 1.
- `·` is a note, not a failure.

If the first line is `✗ server at …`, pogi is not running. Start it with `pogi start`.

## "Ready." never prints, or port 7644 is in use

1. Look at the error. If another program already uses the port, pogi stops at once:

   ```text title="Output"
   pogi: port 7644 is in use (another pogi?); use --port — listen tcp 127.0.0.1:7644: bind: address already in use
   ```

2. Find out what holds the port. It is often an older pogi:

   ```bash
   lsof -nP -iTCP:7644 -sTCP:LISTEN
   ```

   On Windows, use `netstat -ano | findstr :7644`.

3. Stop that program, or start pogi on another port and tell your tools about it:

   ```bash
   pogi start --port 7645
   eval "$(pogi env --endpoint http://localhost:7645)"
   ```

   Commands such as `pogi status` and `pogi ls` find pogi with `--endpoint`, or with the `POGI_ENDPOINT` variable.

4. If there is no error but `Ready.` is slow, look for a `start` init hook. pogi prints `Ready.` after the hooks in `<data folder>/init/start.d` finish. The port answers while they run. This shows a hook that has not finished:

   ```bash
   curl localhost:7644/_localstack/init/start
   ```

   ```json title="Response"
   {"completed":false,"scripts":null}
   ```

   `pogi wait` says the same in words, and exits with an error:

   ```text title="Output"
   pogi: not ready at http://localhost:7644 after 2s: it is still starting
   ```

## My CLI or SDK still reaches the real cloud

1. Check that this shell has the pogi settings:

   ```bash
   env | grep AWS_ENDPOINT_URL
   ```

   No output means they are not loaded. Each new terminal needs this once. It prints nothing:

   ```bash
   eval "$(pogi env)"
   ```

   Check again:

   ```bash
   env | grep AWS_ENDPOINT_URL
   ```

   ```text title="Output"
   AWS_ENDPOINT_URL=http://localhost:7644
   ```

2. Check which account answers. pogi answers with account `000000000000` unless you changed it:

   ```bash
   aws sts get-caller-identity
   ```

   ```json title="Output"
   {
       "UserId": "000000000000",
       "Account": "000000000000",
       "Arn": "arn:aws:iam::000000000000:root"
   }
   ```

3. Look for something that sets its own address. These win over `AWS_ENDPOINT_URL`:
   - an `--endpoint-url` flag in a script or alias;
   - an `AWS_ENDPOINT_URL_S3` or similar variable you set earlier, one per service.

   With the flag, the call goes to the address in the flag. The result is an error like this one:

   ```text title="Output"
   aws: [ERROR]: Could not connect to the endpoint URL: "http://localhost:3/"
   ```

4. To skip the environment for one command, use `pogi aws`. It runs the AWS CLI with the right settings:

   ```bash
   pogi aws sts get-caller-identity
   ```

For Google Cloud and Azure tools, check `env | grep -E 'EMULATOR_HOST|CLOUDSDK_API_ENDPOINT'` in the same way.

## `*.localhost` names don't resolve

Names such as `<bucket>.aws.localhost`, `<api>.execute-api.localhost` and `<id>.lambda-url.localhost` need your system to resolve every `*.localhost` name to your own computer. macOS does. Linux does when it runs systemd-resolved. Alpine Linux containers do not.

1. Run `pogi doctor`. If names don't resolve, it prints this line:

   ```text title="Output"
   · *.localhost names do not resolve here (musl/Alpine, Linux without systemd-resolved): use localhost and path-style S3
   ```

2. Start pogi without `--hostname`, and use `localhost`. S3 then uses path-style URLs (`localhost:7644/<bucket>`), which work with `AWS_ENDPOINT_URL` alone:

   ```bash
   pogi start
   eval "$(pogi env)"
   ```

3. API Gateway URLs and Lambda function URLs are the names that need `*.localhost`. Call a function with `aws lambda invoke` instead of its URL, or run your code on a machine where the names resolve.

## Docker isn't running

pogi's core doesn't need Docker. S3, SQS, SNS, DynamoDB, Lambda in Python and Node.js and most other services run inside the pogi program.

1. Run `pogi doctor`. Without a container engine it prints a note, not a failure:

   ```text title="Output"
   · no container engine found (only --lambda docker, RDS and ElastiCache need one): start Docker Desktop, OrbStack, Colima or Podman, or set DOCKER_HOST
   ```

2. Know what changes. Services that run real software, such as databases, need Docker for that part. The API still answers, but nothing runs behind it. For example, RDS creates an instance and says `available`, and pogi logs this warning:

   ```text title="Output"
   level=WARN msg="Docker is not available: the API works, but no database runs behind this endpoint" service=rds id=db1
   ```

   The console's Status column says so next to the status: `available · no database: Docker isn't running`. The same column shows `failed · killed: out of memory` when a container dies later.

3. Lambda needs Docker for runtimes other than Python and Node.js, and for container images. The call fails with a clear message. This one is for `java21`:

   ```text title="Output"
   aws: [ERROR]: An error occurred (InvalidRuntimeException) when calling the Invoke operation (reached max retries: 2): runtime java21 needs Docker (pogi runs python* and nodejs* natively; provided.* runs natively only for binaries built for darwin/arm64)
   ```

4. Start Docker Desktop, OrbStack, Colima or Podman. If your engine uses a non-default socket, set `DOCKER_HOST`. Run `pogi doctor` again.

## Azure Storage ports 10000 to 10002 are taken

Blob, Queue and Table Storage use ports 10000, 10001 and 10002, the same as Azurite (Microsoft's storage emulator). If another program holds them, pogi still starts and prints `Ready.`, but those three services are off. With `--headless`, the banner names each port it could not open:

```text title="Output"
pogi 0.2.0  ·  355 services  ·  data ~/.pogi
  endpoint  http://127.0.0.1:7644
  console   http://127.0.0.1:7644/_pogi/
  env       eval "$(pogi env)"
  azblob    port 10000 is in use, so this is off — listen tcp 127.0.0.1:10000: bind: address already in use
  azqueue   port 10001 is in use, so this is off — listen tcp 127.0.0.1:10001: bind: address already in use
  aztable   port 10002 is in use, so this is off — listen tcp 127.0.0.1:10002: bind: address already in use
Ready.
```

`pogi status` lists the same ports as problems, one `!` line each, and the terminal screen and the console show them too. The first line is the usual status line:

```bash
pogi status
```

```text title="Output (trimmed)"
pogi 0.2.0 at http://localhost:7644 · up 7s · region us-east-1 · persistence false · iam off
! azblob: port 10000 is in use, so this is off — listen tcp 127.0.0.1:10000: bind: address already in use
! azqueue: port 10001 is in use, so this is off — listen tcp 127.0.0.1:10001: bind: address already in use
! aztable: port 10002 is in use, so this is off — listen tcp 127.0.0.1:10002: bind: address already in use
…
```

Everything else works. The connection string from `pogi env` still points at ports 10000 to 10002, so Azure Storage calls reach the other program instead of pogi.

1. Find the program. It is often Azurite:

   ```bash
   lsof -nP -iTCP:10000-10002 -sTCP:LISTEN
   ```

2. Stop it, then restart pogi.

## My upload is refused

pogi limits uploads to 10 MB, counting the whole file even when the client sends it in parts. A bigger upload fails with a message like this:

```text title="Output"
pogi is a local emulator for tests, so uploads are limited to 10 MB (this one is 24 MB). Raise the limit with --max-upload-mb or server.maxUploadMB in pogi.yaml.
```

1. Use a smaller file in your test. This is the best fix: tests rarely need big files.
2. Or raise the limit. Pass the flag, or set the key in `pogi.yaml`:

   ```bash
   pogi start --max-upload-mb 100
   ```

   ```yaml title="pogi.yaml"
   server:
     maxUploadMB: 100
   ```

Function code zips (up to 50 MB) and container image pushes are not limited by this setting.

A `408` error (`RequestTimeout`) means the upload sent no data for 30 seconds. Check that your client or a proxy is not stalling.

## An image download is stalled or failed

A resource such as an RDS database needs a container image. pogi downloads it the first time, which can take a while. The console shows the progress in the resource's row, and the bell tells you when the resource is ready or the download failed.

A download fails when no data arrives for 2 minutes ("the download stalled"), or after 30 minutes in all. The error says why, in a plain sentence. Docker's own text may follow after a dash:

| Message | What to do |
|---|---|
| `Docker isn't running` | Start Docker, OrbStack, Colima or Podman, or set `DOCKER_HOST`. |
| `image not found: mariadb:99` | Check the engine and version. The image name is in the message. |
| `The registry can't be reached` | Check your network, VPN or proxy, then try again. |
| `Docker stopped answering` | Docker quit, or hung, while pogi was using it, or a call took over 2 minutes. Restart Docker, then try again. |
| `killed: out of memory (raise Docker's memory limit)` | A database or cluster container ran out of memory and Docker stopped it. Raise Docker's memory limit, then delete the resource and create it again. |
| `stopped: exit 1 — …` | The container stopped by itself. The last line it wrote follows the dash; `docker logs` has the rest. |
| `The disk is full` | Free disk space (`docker system df` shows what uses it), then try again. |
| `the download stalled` | Check your network, then delete the resource and create it again. |

To download images before you need them (on a slow network, or in CI), run [`pogi pull`](../reference/cli.md#pogi-pull-download-images-ahead-of-time).

## Containers or volumes are left over

pogi stops or removes its containers when it shuts down. One that was killed (`kill -9`, a crash, a power cut) leaves them running. With saved state pogi stops its containers on a clean exit and starts them again next time, so you see them in `docker ps -a` while pogi is off.

1. See what pogi left, without deleting anything:

   ```bash
   pogi clean --dry-run
   ```

2. Delete it. `pogi clean` asks first, and spares anything a running pogi owns:

   ```bash
   pogi clean
   ```

3. A start without saved state also removes the containers and data volumes a crashed pogi left behind.

Containers and volumes kept for saved state are listed by data folder. Delete one folder's with `pogi clean --data-dir <folder>`, or start pogi on that folder to use them again.

`pogi clean` keeps your saved state unless you say yes to its second question (or pass `--state`). It never removes the `pogi` network or images. See [`pogi clean`](../reference/cli.md#pogi-clean-delete-leftover-containers-and-volumes).

## Saved state can't be read or written

Only one pogi can use a data folder at a time. These messages come from saved state (`--persist` or `--storage`):

| What you see | What to do |
|---|---|
| `another pogi (pid N) is using <folder>` | Stop that pogi, or start this one with another `--data-dir`. A pogi that crashed leaves no lock behind. |
| `pogi can't use the data folder` | Fix the folder's permissions, or pick another `--data-dir`. pogi won't run without saving when you asked it to save. |
| `could not read the saved state of <service> (kept as …)` | The file was damaged. pogi moved it to `<name>.corrupt-<time>` in `<data folder>/state`, started that service empty, and shows this in the bell. Fix or restore the file by hand. |
| `pogi can't save its state` (console banner, bell and `/_pogi/api/info`) | The disk is full or the folder isn't writable. Changes made now are lost if pogi stops. The notice goes away by itself once saving works. |

## See also

- [FAQ](../explanation/faq.md)
- [CLI](../reference/cli.md)
- [Configuration](../reference/configuration.md)
