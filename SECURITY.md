# Security policy

## Supported versions

Only the latest release gets security fixes. While pogi is at 0.x, a fix ships in a new release, not as a patch
to an older one, so please upgrade first and check that the problem is still there.

| Version | Supported |
|---|---|
| Latest release | Yes |
| Older releases | No |

## How to report a problem

Use GitHub private vulnerability reporting on [nrndev/pogi](https://github.com/nrndev/pogi):
open the **Security** tab, then **Report a vulnerability**. Please do not open a public issue for a security problem.

Include:
- what you found and which version;
- steps to reproduce it (a short `curl` command is ideal);
- what an attacker gains.

## What to expect

- We read every report and reply to confirm we got it.
- We tell you if we agree it is a vulnerability, and we keep you updated while we fix it.
- We credit you in the release notes if you want that.

To rerun the security scans yourself, see [Security scans](CONTRIBUTING.md#security-scans).

## What counts as a vulnerability

pogi is a local emulator. It listens on `127.0.0.1` by default. These count:
- A web page you visit can read from or change pogi through requests to localhost (CSRF, loose CORS, or reading responses from another origin).
- DNS rebinding: a web page reaches pogi by tricking the browser about the host name.
- A name or path in a request (bucket, key, blob, file, zip entry) that escapes the data folder.
- Another machine on the network reaching pogi when you did not turn on external access.
- Crafted input that runs a command on your machine, or crashes or hangs pogi with one request.
- A known-vulnerable dependency whose code pogi actually calls.

## What is out of scope

- Running your own Lambda or function code. pogi runs it by design.
- Real cloud credentials. They never belong in pogi. Use fake keys.
- Reaching pogi from the network after you chose `--host 0.0.0.0 --allow-external` (or the Docker image, which sets it). That setting opens pogi to the network on purpose. Use it only on a network you trust.
- Findings that need access to your machine or your data folder first.
