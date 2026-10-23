---
title: Migrate from LocalStack
description: Move a LocalStack or floci setup to pogi, step by step.
---

# Migrate from LocalStack

pogi keeps LocalStack's health endpoint, init hooks and most of its variables, so many setups only need a new image or binary. Before you switch, check the [coverage reference](../reference/coverage.md) for each service you depend on.

## 1. Start pogi

```bash
docker run --rm -p 127.0.0.1:7644:7644 -p 127.0.0.1:10000-10002:10000-10002 ghcr.io/nrndev/pogi   # or: brew install nrndev/tap/pogi && pogi
eval "$(pogi env)"        # sets AWS_ENDPOINT_URL, test credentials, and the Google and Azure variables
pogi wait                 # returns when pogi is ready
```

## 2. Point your SDKs and tools at pogi

Use `http://localhost:7644`. To keep LocalStack's port 4566, run `pogi start --port 4566`, or map it with `docker run -p 127.0.0.1:4566:7644 …`.

The health, info and init endpoints keep their `/_localstack/...` paths. See [endpoint and health](../reference/localstack-mapping.md#endpoint-and-health).

## 3. Swap the settings

1. Replace LocalStack's environment variables with pogi's. pogi ignores any other LocalStack variable, without a warning. See [environment variables](../reference/localstack-mapping.md#environment-variables).
2. Replace the wrapper tools (`awslocal`, `tflocal`, `cdklocal`) and the `localstack` commands. See [the command-line tools](../reference/localstack-mapping.md#the-command-line-tools).
3. Check host names if you use virtual-host style S3, API Gateway or Lambda URLs. See [host names](../reference/localstack-mapping.md#hostnames).

## 4. Move your init scripts

The same directories and stages work, so your existing volume mounts keep working. See [init hooks](../reference/localstack-mapping.md#init-hooks).

Two things to check:

- Scripts that call `awslocal` need it on the `PATH`. The pogi image does not ship it. Use `aws` instead, which sees `AWS_ENDPOINT_URL`.
- The image has Python 3 and Node.js, but not every tool the LocalStack image has.

For resources that need no script, `pogi.yaml` can seed buckets, queues, tables, stacks and more before `Ready.`, and again after every reset.

## 5. Recreate saved state

pogi cannot load state saved by LocalStack. Recreate it with init scripts, Terraform or a `pogi.yaml`, then take a snapshot:

```bash
pogi snapshot save seed
```

To keep state between runs, see [persistence](../reference/localstack-mapping.md#persistence) and [Save and load snapshots](save-and-load-snapshots.md).

## 6. Update your CI and tests

1. Run the `ghcr.io/nrndev/pogi` image.
2. Expose 7644, or map it to 4566.
3. Wait for the log line `Ready.` or for `/_localstack/health`, as with LocalStack.

In Go tests, `pogi.Start(t)` runs everything inside the test process on a random port, with no container.

## 7. Check the migration

1. Run `pogi start`, `eval "$(pogi env)"` and `pogi wait`.
2. Run your test suite or Terraform plan. `pogi logs -f` (or the Requests view on the console's Activity page) shows every call and every error, including operations a service does not implement.
3. For anything that fails, find the service in the [coverage reference](../reference/coverage.md). Its Not yet column lists what is not implemented.

## If you come from floci

floci is another local AWS emulator. floci uses a separate emulator for each cloud, on separate ports. pogi serves AWS, Google Cloud and Azure together on port 7644.

Map floci's settings by what they do:

| floci | pogi |
|---|---|
| Enabled services | `SERVICES`, or `services:` in `pogi.yaml` |
| Region | `POGI_REGION` |
| Storage | `--storage`, or `POGI_STORAGE` |
| Credentials | `identities:` in `pogi.yaml` |
| Init scripts | The LocalStack directories in [init hooks](../reference/localstack-mapping.md#init-hooks) |

pogi ignores anything it does not read. Replace each floci endpoint with `http://localhost:7644`, and keep the Azure Storage ports.

## See also

- [LocalStack mapping](../reference/localstack-mapping.md)
- [Configuration](../reference/configuration.md)
