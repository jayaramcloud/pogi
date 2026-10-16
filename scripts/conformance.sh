#!/bin/sh
# Runs real Terraform (aws and google providers) and CDK projects against a fresh pogi:
#   terraform apply → plan (must show no changes) → destroy
#   cdk bootstrap → deploy → destroy
# Needs terraform, node/npm and python3; downloads providers and packages.
#
#   scripts/conformance.sh [block ...]     no arguments runs every block
#
# Blocks: aws, compute (ECR, ECS behind an ALB, Auto Scaling), google, azurerm (needs Docker), longtail, cdk.
# CI runs each block in its own job, side by side.
set -eu
blocks=${*:-aws compute google azurerm longtail cdk}
want() { case " $blocks " in *" $1 "*) return 0 ;; esac; return 1; }
root=$(cd "$(dirname "$0")/.." && pwd)
work=$(mktemp -d)
port=${POGI_PORT:-4599}
region=${REGION:-us-east-1} # Terraform's; us-gov-west-1 and cn-north-1 cover the other partitions
ep=http://localhost:$port
trap 'kill $pid 2>/dev/null; rm -rf "$work"' EXIT

# nodrift <terraform|tfd>: a second plan must find nothing to change. On drift, print what it wanted to change.
nodrift() { "$1" plan -detailed-exitcode -input=false -no-color > "$work/plan.txt" 2>&1 || { cat "$work/plan.txt" >&2; return 1; }; }

go build -o "$work/pogi" "$root/cmd/pogi"
"$work/pogi" start --headless --quiet --port "$port" --data-dir "$work/data" --iam soft &
pid=$!
until curl -fs "$ep/_localstack/health" >/dev/null; do sleep 0.2; done
"$work/pogi" lambda-sdks --data-dir "$work/data" >/dev/null
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_REGION=us-east-1 AWS_ENDPOINT_URL=$ep

if want aws; then
echo "== terraform"
cp -R "$root/examples/terraform" "$work/tf"
"$work/pogi" tf --endpoint "$ep" --region "$region" > "$work/tf/pogi_override.tf"
(cd "$work/tf" && terraform init -input=false >/dev/null && terraform apply -auto-approve -input=false >/dev/null \
  && terraform output arns && nodrift terraform && terraform destroy -auto-approve -input=false >/dev/null)
echo "terraform ($region): apply, no-drift plan and destroy passed"
fi

if want compute; then

echo "== terraform: ECR, ECS behind an ALB, Auto Scaling"
cp -R "$root/examples/terraform-compute" "$work/tfc"
"$work/pogi" tf --endpoint "$ep" --region "$region" > "$work/tfc/pogi_override.tf"
(cd "$work/tfc" && terraform init -input=false >/dev/null && terraform apply -auto-approve -input=false >/dev/null \
  && if docker info >/dev/null 2>&1; then # the service's nginx tasks answer through the load balancer
    curl -fs -H "Host: $(terraform output -raw url | cut -d/ -f3)" "$ep/" | grep -q nginx; fi \
  && nodrift terraform && terraform destroy -auto-approve -input=false >/dev/null)
echo "terraform compute ($region): apply, ALB → ECS, no-drift plan and destroy passed"
fi

if want google; then

echo "== terraform: google provider"
cp -R "$root/examples/terraform-google" "$work/tfg"
"$work/pogi" tf --cloud gcp --endpoint "$ep" > "$work/tfg/pogi_override.tf"
(cd "$work/tfg" && terraform init -input=false >/dev/null && terraform apply -auto-approve -input=false >/dev/null \
  && nodrift terraform && terraform destroy -auto-approve -input=false >/dev/null)
echo "terraform google: apply, no-drift plan and destroy passed"
fi

if ! want azurerm; then
  :
elif docker info >/dev/null 2>&1; then
  # azurerm reads every URL from pogi's metadata endpoint, over TLS. Terraform on the host would need pogi's CA
  # in the system's trust store (the macOS keychain: not something to change), so it runs in a container
  # that trusts the CA through SSL_CERT_FILE and reaches pogi at host.docker.internal. The *.localhost names
  # the example's Key Vault and storage account have are mapped to the host as well.
  echo "== terraform: azurerm provider (in a container)"
  cp -R "$root/examples/terraform-azure" "$work/tfa"
  "$work/pogi" tf --cloud azure --endpoint "$ep" > "$work/tfa/pogi_override.tf"
  cache=${TF_PLUGIN_CACHE_DIR:-$HOME/.terraform.d/plugin-cache}
  mkdir -p "$cache"
  # Docker Desktop and other VM engines forward host.docker.internal to this machine's loopback. A Linux
  # engine does not reach 127.0.0.1 through its bridge, so there the container shares the host's network.
  # It also runs as this user there, so the files it writes can be cleaned up afterwards.
  gw=host-gateway net=
  [ "$(uname -s)" = Linux ] && gw=127.0.0.1 net="--network=host --user $(id -u):$(id -g) -e HOME=/tmp"
  tfd() {
    docker run --rm $net -v "$work/tfa":/work -w /work --add-host host.docker.internal:$gw \
      --add-host pogi-tf-kv.vault.localhost:$gw --add-host pogitfstore.blob.localhost:$gw \
      --add-host pogitfstore.queue.localhost:$gw --add-host pogitfstore.table.localhost:$gw \
      -v "$work/data/certs/ca.pem":/pogi-ca.pem -e SSL_CERT_FILE=/pogi-ca.pem \
      -v "$cache":/cache -e TF_PLUGIN_CACHE_DIR=/cache -e TF_IN_AUTOMATION=1 hashicorp/terraform "$@" -input=false
  }
  tfd init >/dev/null && tfd apply -auto-approve >/dev/null && nodrift tfd && tfd destroy -auto-approve >/dev/null
  echo "terraform azurerm: apply, no-drift plan and destroy passed"
else
  echo "== terraform: azurerm provider skipped (Docker is not running)"
fi

if want longtail; then
echo "== terraform: long-tail services (generated engine)"
cp -R "$root/examples/terraform-longtail" "$work/tfl"
"$work/pogi" tf --endpoint "$ep" --region "$region" > "$work/tfl/pogi_override.tf"
(cd "$work/tfl" && terraform init -input=false >/dev/null && terraform apply -auto-approve -input=false >/dev/null \
  && nodrift terraform && terraform destroy -auto-approve -input=false >/dev/null)
echo "terraform long tail ($region): apply, no-drift plan and destroy passed"
fi

if want cdk; then

echo "== cdk"
cp -R "$root/examples/cdk" "$work/cdk"
(cd "$work/cdk" && npm install --no-audit --no-fund >/dev/null && npx cdk bootstrap aws://000000000000/us-east-1 >/dev/null \
  && npx cdk deploy --require-approval never >/dev/null && npx cdk destroy --force >/dev/null)
echo "cdk: bootstrap, deploy and destroy passed"
fi
