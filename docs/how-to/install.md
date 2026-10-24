---
title: Install pogi
description: Install pogi v0.2.0 on macOS, Linux or Windows, or run it with Docker.
---

# Install pogi

pogi is one file. Install it with a package manager, Docker or a download, then check that it runs.

## 1. Install

Choose one way.

```bash tab="macOS (Homebrew)"
brew install nrndev/tap/pogi
```

```bash tab="Linux (.deb)"
# On Arm, use arm64 in place of amd64.
curl -LO https://github.com/nrndev/pogi/releases/download/v0.2.0/pogi_0.2.0_linux_amd64.deb
sudo apt install ./pogi_0.2.0_linux_amd64.deb
```

```bash tab="Linux (.rpm)"
# On Arm, use arm64 in place of amd64.
curl -LO https://github.com/nrndev/pogi/releases/download/v0.2.0/pogi_0.2.0_linux_amd64.rpm
sudo dnf install ./pogi_0.2.0_linux_amd64.rpm
```

```powershell tab="Windows (Scoop)"
scoop bucket add nrndev https://github.com/nrndev/scoop-bucket
scoop install nrndev/pogi
```

```bash tab="Go"
go install github.com/nrndev/pogi/cmd/pogi@latest
```

```bash tab="Docker"
docker run --rm -p 127.0.0.1:7644:7644 -p 127.0.0.1:10000-10002:10000-10002 ghcr.io/nrndev/pogi
```

- The `.deb` and `.rpm` packages put pogi in `/usr/bin`. Use the `.rpm` on Fedora, RHEL and Amazon Linux. To remove pogi, run `sudo apt remove pogi` or `sudo dnf remove pogi`.
- Scoop is a command-line installer for Windows. It puts pogi on your `PATH`, and `scoop update pogi` upgrades it.
- `go install` needs Go 1.27.3 or newer, with Go's bin folder on your `PATH`.
- For the Docker ports and options, see [Run it with Docker](#run-it-with-docker).

### Or download a binary

1. Download the archive for your system from the [releases page](https://github.com/nrndev/pogi/releases). Builds exist for Linux, macOS and Windows, on Intel (amd64) and Arm (arm64). Windows gets a `.zip`. The others get a `.tar.gz`, and Linux also gets `.deb` and `.rpm` packages. Names look like `pogi_0.2.0_windows_amd64.zip`.
2. Check the file against `checksums.txt`, which lists the SHA-256 sum of each file.
3. Unpack it and put `pogi` (or `pogi.exe`) somewhere on your `PATH`. Until then, run it from its folder as `.\pogi.exe start`.

### Check that a download is genuine

GitHub's release job signs every release, with no long-lived keys. You can check a download in three ways, all optional:

- With [cosign](https://docs.sigstore.dev/cosign/system_config/installation/), check `checksums.txt` (download `checksums.txt.sigstore.json` from the release too), then check your archive against it as in step 2:

  ```bash
  cosign verify-blob checksums.txt --bundle checksums.txt.sigstore.json \
    --certificate-identity-regexp '^https://github.com/nrndev/pogi/.github/workflows/release.yml@refs/tags/v' \
    --certificate-oidc-issuer https://token.actions.githubusercontent.com
  ```

- With the GitHub CLI, check the archive's build provenance directly:

  ```bash
  gh attestation verify pogi_0.2.0_linux_amd64.tar.gz -R nrndev/pogi
  ```

- With [slsa-verifier](https://github.com/slsa-framework/slsa-verifier), check the archive against the release's SLSA provenance
  (a signed record of how and from what source it was built). Download `pogi.intoto.jsonl` from the release too:

  ```bash
  slsa-verifier verify-artifact pogi_0.2.0_linux_amd64.tar.gz --provenance-path pogi.intoto.jsonl \
    --source-uri github.com/nrndev/pogi --source-tag v0.2.0
  ```

The Docker image is signed the same way: `cosign verify ghcr.io/nrndev/pogi:0.2.0` with the same two certificate flags.

## 2. Check that it works

```bash
pogi version
```

```text title="Output"
pogi 0.2.0
```

## 3. Start it

```bash
pogi start
```

The web console is at `http://localhost:7644/_pogi/`.

## Run it with Docker

The image has Python and Node.js, so Lambda, Cloud Functions and Azure Functions run inside it. Arguments after the image name go to `pogi start`.

- **Ports.** The command above publishes the main port, 7644. Ports 10000 to 10002 are for Azure Blob, Queue and Table Storage, the same as Azurite. Leave them out if you don't use Azure Storage.
- **IAM.** IAM (the rules for who may call what) is off by default. Add `--iam soft` after the image name to flag calls a policy would deny, while still allowing them.
- **Docker socket.** Services that run real software, such as RDS, ECS and EKS, need Docker. Add `-v /var/run/docker.sock:/var/run/docker.sock` to let pogi start containers. This gives pogi control of your Docker, so use it only on a machine you trust. pogi joins its own container to the `pogi` Docker network, so the containers it starts can call it back.
- **Data folder.** Keep `/var/lib/pogi` on a volume or a host folder (the image declares it as a volume). pogi hands files to those containers from there, and copies in any other folder it needs.

The image listens on every address inside the container. Publish ports on `127.0.0.1` as above, so only your machine can reach them.

## What else you may need

All of these are optional.

| Need | When |
|---|---|
| Docker | Services that run real software: RDS, ECS, Cloud SQL and Azure SQL. Docker, or any engine with the Docker API: Podman, Colima or OrbStack. [Coverage](../reference/coverage.md) says which services need it. On an Arm Mac (Apple Silicon), an image with no Arm build, such as the Azure Functions host, runs as amd64 under emulation. It works but starts slower. |
| Python 3 and Node.js | pogi runs Python and Node.js Lambda functions natively, so these runtimes must be on your machine. `pogi doctor` checks. |
| Trusting pogi's certificate | Some services use HTTPS on the main port, such as Azure Key Vault and Cosmos DB. Run `pogi cert` once to see how to trust pogi's own certificate authority (CA). |
| AWS SDKs for native Lambda | The CDK's custom resources need them. Run `pogi lambda-sdks` once. |

## See also

- [First run](../tutorials/first-run.md)
- [Connect your tools](connect-your-tools.md)
