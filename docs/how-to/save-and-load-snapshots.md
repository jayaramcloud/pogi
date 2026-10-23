---
title: Save and load snapshots
description: Save pogi's state under a name, load it back, share it as a file, and keep state across restarts.
---

# Save and load snapshots

A **snapshot** is a named copy of the state of every service. Save one when your setup is right, such as a seeded database and a few queues. Load it after a test has changed things, or give the file to a teammate.

## Save, list, load and delete

1. Save everything under the name `demo`:

   ```bash
   pogi snapshot save demo
   ```

   ```text title="Output"
   saved demo
   ```

   With no name, `save` uses the date and time as the name.

2. List the snapshots:

   ```bash
   pogi snapshot ls
   ```

   ```text title="Output"
   NAME  SIZE   SAVED
   demo  15606  2026-10-13 21:05:46
   ```

   `pogi snapshot` alone lists snapshots too.

3. Load the snapshot when a test has changed things:

   ```bash
   pogi snapshot load demo
   ```

   ```text title="Output"
   loaded demo
   ```

   `load` replaces the current state of every service. Anything created after the save is gone.

4. Delete the snapshot when you no longer need it:

   ```bash
   pogi snapshot rm demo
   ```

   ```text title="Output"
   removed snapshot demo
   ```

Snapshots are stored in the `snapshots` folder of pogi's data folder. The data folder is `~/.pogi` unless you set `--data-dir`.

## Share a snapshot

1. Export a snapshot to a file. With no file name, or with `-`, it goes to standard output:

   ```bash
   pogi snapshot export demo demo.snap
   ```

2. On the other machine, import the file. The name defaults to the file name without its extension. Give a name to choose your own. To read standard input, use `-` as the file and give a name:

   ```bash
   pogi snapshot import demo.snap team-seed
   ```

   ```text title="Output"
   imported team-seed (load it with: pogi snapshot load team-seed)
   ```

3. Load it when you want it. Importing only stores the snapshot:

   ```bash
   pogi snapshot load team-seed
   ```

## Reset

`pogi reset` deletes state without saving it. Save a snapshot first if you may want the state back.

1. Save a snapshot, if you want one:

   ```bash
   pogi snapshot save before-reset
   ```

2. Reset. Name services to reset only those:

   ```bash
   pogi reset s3 sqs
   ```

   ```text title="Output"
   reset s3, sqs
   ```

## Use the console

The **Snapshots** page lists snapshots. It has buttons to save, load, download, delete and import them, and a **Reset everything** button. **Home** also has a **Save snapshot** button.

![The Snapshots page, with two saved snapshots, each with buttons to load, download or delete it.](../images/t-snapshots.jpg)

## Keep state across restarts

pogi keeps state in memory by default, so stopping pogi clears it. To keep it, start pogi with a storage mode:

```bash
pogi start --persist
```

`--persist` is short for `--storage hybrid`: pogi writes every service's state to disk every few seconds and when it exits. For `--storage persistent` (written before each write is acknowledged), `--storage wal` (a journal for DynamoDB, SQS and S3) and per-service modes, see [Storage modes](../reference/configuration.md#storage-modes).

State files are in the `state` folder of the data folder, one file per service.

## Use it from a script

The same actions are in the REST API under `/_pogi/api/snapshots`.

## When a snapshot is refused

pogi checks a snapshot before it changes anything:

- A snapshot from a newer pogi is refused. The message gives its format and the newest one your pogi reads.
- A snapshot with no services (`{}`) is refused with `this snapshot is not valid: it has no services`.
- If one service fails to decode, the message names it and the state stays exactly as it was.
- A service your pogi does not know is skipped. pogi warns in the log and the console's bell.

## See also

- [REST API](../reference/rest-api.md)
- [Configuration](../reference/configuration.md#storage-modes)
- [CLI](../reference/cli.md)
