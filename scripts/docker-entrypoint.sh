#!/bin/sh
# LocalStack-style entrypoint: pogi logs "Ready." once it serves, and runs
# scripts from /etc/localstack/init/{boot,start,ready,shutdown}.d.
set -e
exec pogi start --headless "$@"
