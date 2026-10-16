---
title: Install pogi
description: Install pogi v0.1.0 on macOS, Linux or Windows, or run it with Docker.
---

# Install pogi

pogi is one file.

## 1. Install

Choose one way.

```bash tab="macOS (Homebrew)"
brew install nrndev/tap/pogi
```

```bash tab="Linux (.deb or .rpm)"
# Debian and Ubuntu. On Arm, use arm64 in place of amd64.
curl -LO https://github.com/nrndev/pogi/releases/download/v0.1.0/pogi_0.1.0_linux_amd64.deb
sudo apt install ./pogi_0.1.0_linux_amd64.deb
# Fedora, RHEL and Amazon Linux: the .rpm from the same release.
#   curl -LO https://github.com/nrndev/pogi/releases/download/v0.1.0/pogi_0.1.0_linux_amd64.rpm
#   sudo dnf install ./pogi_0.1.0_linux_amd64.rpm
# The package puts pogi in /usr/bin. To remove it: sudo apt remove pogi (or sudo dnf remove pogi).
```

```powershell tab="Windows (Scoop)"
# Scoop is a command-line installer for Windows.
# The first command adds the pogi bucket, which is Scoop's name for a list of apps.
scoop bucket add nrndev https://github.com/nrndev/scoop-bucket
# Scoop puts pogi on your PATH. To upgrade later, run: scoop update pogi
scoop install nrndev/pogi
```

```bash tab="Go"
# Needs Go. The Go bin folder must be on your PATH.
go install github.com/nrndev/pogi/cmd/pogi@latest
```

```bash tab="Docker"
# Publishes the main port. Arguments after the image name go to "pogi start".
docker run --rm -p 127.0.0.1:7644:7644 ghcr.io/nrndev/pogi
# For Azure Blob, Queue or Table Storage from outside the container, also add:
#   -p 127.0.0.1:10000-10002:10000-10002
# (those services use ports 10000 to 10002, the same as Azurite)
# The image listens on every address inside the container.
# IAM (who may do what) is off by default. To turn on soft IAM mode,
# which flags calls a policy would deny but still allows them,
# add "--iam soft" after the image name.
# The image has Python and Node.js, so Lambda, Cloud Functions and Azure
# Functions run inside it. For container-backed services (RDS, ECS, EKS…)
# also add -v /var/run/docker.sock:/var/run/docker.sock
# (this gives pogi control of your Docker).
# With the socket, databases, ECS tasks and the other services that run real
# software work too. pogi joins its own container to the "pogi" Docker network,
# so the containers it starts can call it. Keep /var/lib/pogi on a volume or a
# folder (the image declares it as a volume): pogi hands files to those
# containers from there, and copies in any other folder it needs.
```

### Or download a binary

1. Download the archive for your system from the [releases page](https://github.com/nrndev/pogi/releases). Builds exist for Linux, macOS and Windows, on Intel (amd64) and Arm (arm64). Windows gets a `.zip`. The others get a `.tar.gz`, and Linux also gets `.deb` and `.rpm` packages. Names look like `pogi_0.1.0_windows_amd64.zip`.
2. Check the file against `checksums.txt`, which lists the SHA-256 sum of each file.
3. Unpack it and put `pogi` (or `pogi.exe`) somewhere on your `PATH`. Until then, run it from its folder as `.\pogi.exe start`.

### Check that a download is genuine

Every release is signed in GitHub's release job, with no long-lived keys. Two ways to check, both optional:

- With [cosign](https://docs.sigstore.dev/cosign/system_config/installation/), check `checksums.txt` (download `checksums.txt.sigstore.json` from the release too), then check your archive against it as in step 2:

  ```bash
  cosign verify-blob checksums.txt --bundle checksums.txt.sigstore.json \
    --certificate-identity-regexp '^https://github.com/nrndev/pogi/.github/workflows/release.yml@refs/tags/v' \
    --certificate-oidc-issuer https://token.actions.githubusercontent.com
  ```

- With the GitHub CLI, check the archive's build provenance directly:

  ```bash
  gh attestation verify pogi_0.1.0_linux_amd64.tar.gz -R nrndev/pogi
  ```

The Docker image is signed the same way: `cosign verify ghcr.io/nrndev/pogi:0.1.0` with the same two certificate flags.

## 2. Check that it works

```bash
pogi version
```

```text title="Output"
pogi 0.1.0
```

## 3. Start it

```bash
pogi start
```

The console is at `http://localhost:7644/_pogi/`.

## What else you may need

All of these are optional.

| Need | When |
|---|---|
| Docker | Services that run real software: RDS, ECS, Cloud SQL and Azure SQL. Any engine with the Docker API works: Docker, Podman, Colima or OrbStack. [Coverage](../reference/coverage.md) says which services need it. On an Arm Mac (Apple Silicon), an image with no Arm build, such as the Azure Functions host, runs as amd64 under emulation. It works, but starts slower. |
| Python 3 and Node.js | pogi runs Python and Node.js Lambda functions natively, so these runtimes must be on your machine. `pogi doctor` checks. |
| Trusting pogi's certificate | Some services use HTTPS on the main port, such as Azure Key Vault and Cosmos DB. Run `pogi cert` once to see how to trust pogi's local certificate authority. |
| AWS SDKs for native Lambda | The CDK's custom resources need them. Run `pogi lambda-sdks` once. |

## See also

- [First run](../tutorials/first-run.md)
- [Connect your tools](connect-your-tools.md)
