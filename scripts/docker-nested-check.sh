#!/bin/sh
# Checks that Docker-backed services work when pogi itself runs in a container that has the Docker
# socket mounted (Lambda in docker mode and the container-backed services).
# For the release checklist; CI does not run it.
#
#   scripts/docker-nested-check.sh
#
# For each image it builds it, runs it on host port 47965 (never 7644), and calls:
#   Lambda (python and nodejs), RDS PostgreSQL (select 1 from a throwaway psql container), Athena
#   (DuckDB), an ECS task, an Azure Function; a Google Cloud Function when POGI_CHECK_PIP=1 (its
#   container installs functions-framework from PyPI). Then it stops pogi and checks that no
#   container of this pogi is left. It removes everything it started, and the images it built.
# Needs docker, the AWS CLI, curl and python3. Pulls postgres, alpine, the Lambda base images and,
# for the Azure Function, mcr.microsoft.com/azure-functions/python when they are missing.
set -eu

cd "$(dirname "$0")/.."
port=47965
name=pogi-nested-check
ep=http://localhost:$port
work=$(mktemp -d)
failed=0
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_REGION=us-east-1 AWS_PAGER= AWS_ENDPOINT_URL=$ep

say() { printf '%s\n' "$*"; }

# ours lists the containers pogi (in the container $name) started, by their owner label: the id
# of that container is the host name pogi puts in it. Containers of other pogis are not touched.
ours() {
  id=$(docker inspect -f '{{.Id}}' "$name" 2>/dev/null | cut -c1-12) || return 0
  [ -n "$id" ] || return 0
  docker ps -a --filter label=pogi.managed=true --format '{{.ID}} {{.Names}} {{.Label "pogi.owner"}}' |
    awk -v p="$id/" 'index($3, p) == 1 { print $1, $2 }'
}

cleanup() {
  ours | while read -r cid cname; do docker rm -fv "$cid" >/dev/null 2>&1 || true; done
  docker rm -fv "$name" >/dev/null 2>&1 || true
  docker rmi pogi:nested-check >/dev/null 2>&1 || true
  rm -rf "$work"
}
trap cleanup EXIT INT TERM

# check NAME COMMAND...: runs the command (a function; it stops at its first failing step) and prints ok or FAIL.
check() {
  label=$1
  shift
  set +e # so the subshell's own set -e counts
  (set -e; "$@") >"$work/check.out" 2>&1
  rc=$?
  set -e
  if [ "$rc" = 0 ]; then
    say "  ok    $label"
  else
    say "  FAIL  $label"
    tail -8 "$work/check.out" | sed 's/^/        /'
    failed=1
  fi
}

# wait_for SECONDS COMMAND...: runs the command every two seconds until it succeeds.
wait_for() {
  n=$(($1 / 2))
  shift
  while ! "$@" >/dev/null 2>&1; do
    n=$((n - 1))
    [ "$n" -gt 0 ] || return 1
    sleep 2
  done
}

zip_of() { # zip_of FILE... : writes $work/<first file's name>.zip
  python3 -c 'import sys, zipfile
z = zipfile.ZipFile(sys.argv[1], "w")
for f in sys.argv[2:]:
    z.write(f, f.split("/")[-1])
z.close()' "$@"
}

# ---- the checks ----

lambda_python() {
  printf 'def handler(e, c):\n    return {"ok": e["x"] * 2}\n' >"$work/index.py"
  zip_of "$work/py.zip" "$work/index.py"
  aws lambda create-function --function-name nc-py --runtime python3.12 --handler index.handler \
    --role arn:aws:iam::000000000000:role/nc --zip-file "fileb://$work/py.zip" >/dev/null
  aws lambda invoke --function-name nc-py --payload '{"x":21}' --cli-binary-format raw-in-base64-out "$work/py.out" >/dev/null
  grep -q '"ok": 42' "$work/py.out" || { cat "$work/py.out"; return 1; }
}

lambda_nodejs() {
  printf 'exports.handler = async (e) => ({ ok: e.x * 2 });\n' >"$work/index.js"
  zip_of "$work/js.zip" "$work/index.js"
  aws lambda create-function --function-name nc-js --runtime nodejs20.x --handler index.handler \
    --role arn:aws:iam::000000000000:role/nc --zip-file "fileb://$work/js.zip" >/dev/null
  aws lambda invoke --function-name nc-js --payload '{"x":21}' --cli-binary-format raw-in-base64-out "$work/js.out" >/dev/null
  grep -q '"ok":42' "$work/js.out" || { cat "$work/js.out"; return 1; }
}

rds_postgres() {
  aws rds create-db-instance --db-instance-identifier nc --engine postgres --db-instance-class db.t3.micro \
    --allocated-storage 20 --master-username u --master-user-password pw12345678 >/dev/null
  aws rds wait db-instance-available --db-instance-identifier nc
  p=$(aws rds describe-db-instances --db-instance-identifier nc --query 'DBInstances[0].Endpoint.Port' --output text)
  # the endpoint is localhost:<port> on the host; a throwaway container reaches the host's loopback like this
  if docker info --format '{{.OperatingSystem}}' | grep -qiE 'desktop|orbstack|colima'; then
    set -- --add-host host.docker.internal:host-gateway -e PGHOST=host.docker.internal
  else
    set -- --network host -e PGHOST=127.0.0.1
  fi
  out=$(docker run --rm "$@" -e PGPASSWORD=pw12345678 postgres:17-alpine psql -p "$p" -U u -d postgres -tAc 'select 1')
  [ "$out" = 1 ] || { say "got: $out"; return 1; }
}

athena_duckdb() {
  aws s3 mb s3://nc-bucket >/dev/null
  q=$(aws athena start-query-execution --query-string 'select 41 + 1 as answer' \
    --result-configuration OutputLocation=s3://nc-bucket/out/ --query QueryExecutionId --output text)
  done_query() { [ "$(aws athena get-query-execution --query-execution-id "$q" --query QueryExecution.Status.State --output text)" = SUCCEEDED ]; }
  wait_for 120 done_query || return 1
  [ "$(aws athena get-query-results --query-execution-id "$q" --query 'ResultSet.Rows[1].Data[0].VarCharValue' --output text)" = 42 ]
}

ecs_task() {
  aws ecs create-cluster --cluster-name nc >/dev/null
  aws ecs register-task-definition --family nc --requires-compatibilities FARGATE --network-mode awsvpc --cpu 256 --memory 512 \
    --container-definitions '[{"name":"c","image":"alpine:3","essential":true,"command":["sh","-c","echo hello-from-ecs"]}]' >/dev/null
  t=$(aws ecs run-task --cluster nc --task-definition nc --launch-type FARGATE \
    --network-configuration 'awsvpcConfiguration={subnets=[subnet-1]}' --query 'tasks[0].taskArn' --output text)
  aws ecs wait tasks-stopped --cluster nc --tasks "$t"
  [ "$(aws ecs describe-tasks --cluster nc --tasks "$t" --query 'tasks[0].containers[0].exitCode' --output text)" = 0 ]
}

azure_function() {
  img=mcr.microsoft.com/azure-functions/python:4-python3.12
  rm -rf "$work/az" && mkdir "$work/az"
  cat >"$work/az/function_app.py" <<'EOF'
import azure.functions as func
app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.route(route="hello")
def hello(req: func.HttpRequest) -> func.HttpResponse:
    return func.HttpResponse("hello " + req.params.get("name", "world"))
EOF
  echo '{"version":"2.0"}' >"$work/az/host.json"
  zip_of "$work/az.zip" "$work/az/function_app.py" "$work/az/host.json"
  sub=/subscriptions/00000000-0000-0000-0000-000000000000
  curl -sf -X PUT "$ep$sub/resourcegroups/g?api-version=2021-04-01" -H 'Content-Type: application/json' -d '{"location":"eastus"}' >/dev/null
  curl -sf -X PUT "$ep$sub/resourceGroups/g/providers/Microsoft.Web/sites/hello?api-version=2023-01-01" -H 'Content-Type: application/json' \
    -d '{"location":"eastus","kind":"functionapp,linux","properties":{"siteConfig":{"linuxFxVersion":"Python|3.12","appSettings":[{"name":"FUNCTIONS_WORKER_RUNTIME","value":"python"}]}}}' >/dev/null
  curl -sf -X POST "$ep/_azure/functions/hello/api/zipdeploy" -H 'Content-Type: application/zip' --data-binary "@$work/az.zip" >/dev/null
  [ "$(curl -sf -m 300 "$ep/_azure/functions/hello/api/hello?name=nested")" = "hello nested" ]
}

google_function() {
  rm -rf "$work/gcf" && mkdir "$work/gcf"
  cat >"$work/gcf/main.py" <<'EOF'
import functions_framework

@functions_framework.http
def hello(request):
    return "hello " + request.args.get("name", "world")
EOF
  zip_of "$work/gcf.zip" "$work/gcf/main.py"
  base=$ep/v2/projects/p/locations/us-central1
  up=$(curl -sf -X POST "$base/functions:generateUploadUrl" -H 'Content-Type: application/json' -d '{}')
  url=$(printf '%s' "$up" | python3 -c 'import json, sys; print(json.load(sys.stdin)["uploadUrl"])')
  src=$(printf '%s' "$up" | python3 -c 'import json, sys; print(json.dumps(json.load(sys.stdin)["storageSource"]))')
  curl -sf -X PUT "$url" -H 'Content-Type: application/zip' --data-binary "@$work/gcf.zip" >/dev/null
  curl -sf -X POST "$base/functions?functionId=hello" -H 'Content-Type: application/json' \
    -d '{"buildConfig":{"runtime":"python312","entryPoint":"hello","source":{"storageSource":'"$src"'}}}' >/dev/null
  active() { curl -sf "$base/functions/hello" | grep -q '"state": *"ACTIVE"'; }
  wait_for 300 active || return 1
  [ "$(curl -sf -m 300 "$ep/_gcp/functions/p/us-central1/hello?name=nested")" = "hello nested" ]
}

# ---- one image ----

run_target() {
  target=$1
  case $target in
    default) image=pogi:nested-check; build="docker build -q -t $image ."; extra="-e POGI_LAMBDA=docker" ;; # it runs python and node itself unless told
    *) say "unknown image '$target' (only default)"; exit 2 ;;
  esac
  say "== $target image"
  if docker inspect "$name" >/dev/null 2>&1; then
    say "a container named $name exists; remove it first"
    exit 2
  fi
  $build >/dev/null
  # shellcheck disable=SC2086
  docker run -d --name "$name" -p "127.0.0.1:$port:7644" -v /var/run/docker.sock:/var/run/docker.sock $extra "$image" >/dev/null
  wait_for 60 curl -sf "$ep/_localstack/health" || { docker logs "$name" 2>&1 | tail; failed=1; return; }

  aws iam create-role --role-name nc --assume-role-policy-document '{"Version":"2012-10-17","Statement":[]}' >/dev/null
  check "Lambda python" lambda_python
  check "Lambda nodejs" lambda_nodejs
  check "RDS postgres: select 1 at the endpoint" rds_postgres
  check "Athena (DuckDB)" athena_duckdb
  check "ECS task runs to completion" ecs_task
  if docker image inspect mcr.microsoft.com/azure-functions/python:4-python3.12 >/dev/null 2>&1; then
    check "Azure Function over HTTP" azure_function
  else
    say "  skip  Azure Function: its image is large; run: docker pull mcr.microsoft.com/azure-functions/python:4-python3.12"
  fi
  if [ "${POGI_CHECK_PIP:-}" = 1 ]; then
    check "Google Cloud Function over HTTP" google_function
  else
    say "  skip  Google Cloud Function: it installs functions-framework from PyPI; set POGI_CHECK_PIP=1"
  fi
  docker logs "$name" 2>&1 | grep -E 'level=(WARN|ERROR)' | grep -v 'listens beyond this machine' | sed 's/^/  log   /' || true

  docker stop -t 60 "$name" >/dev/null
  left=$(ours)
  if [ -z "$left" ]; then
    say "  ok    no container of this pogi is left after it stops"
  else
    say "  FAIL  containers left after pogi stopped:"
    printf '%s\n' "$left" | sed 's/^/        /'
    failed=1
  fi
  cleanup_target
}

# cleanup_target removes the container and what it left, so the next image starts clean.
cleanup_target() {
  ours | while read -r cid cname; do docker rm -fv "$cid" >/dev/null 2>&1 || true; done
  docker rm -fv "$name" >/dev/null 2>&1 || true
}

[ $# -gt 0 ] || set -- default
for t in "$@"; do
  run_target "$t"
done
if [ "$failed" = 0 ]; then say "all checks passed"; else say "some checks FAILED"; fi
exit "$failed"
