#!/usr/bin/env bash
# Deploy the Medallion AWS layer with CloudFormation, in dependency order.
# Guide: docs/how-to/deploy-to-production.md
#
#   ENV=prod AWS_REGION=eu-west-1 infra/cloudformation/deploy.sh              # every stack
#   ENV=prod AWS_REGION=eu-west-1 infra/cloudformation/deploy.sh eks msk      # some stacks
#   REVIEW=1 ...      only create change sets; review and execute them in the console
#
# Optional: ADMIN_PRINCIPAL_ARN (EKS cluster admin), API_PUBLIC_CIDR (public EKS API, else private),
#           ALERT_EMAIL (SNS subscription), KAFKA_VERSION (default 4.1.x.kraft).
set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$DIR/../.." && pwd)"
ENV="${ENV:-prod}"
: "${AWS_REGION:?set AWS_REGION (e.g. AWS_REGION=eu-west-1)}"
export AWS_REGION
KAFKA_VERSION="${KAFKA_VERSION:-4.1.x.kraft}"
ORDER="network data-lake ecr eks msk observability workload-iam"

die() { echo "error: $*" >&2; exit 1; }

file_for() {
  case "$1" in
    network) echo 01-network.yaml ;;
    data-lake) echo 02-data-lake.yaml ;;
    ecr) echo 03-ecr.yaml ;;
    eks) echo 04-eks.yaml ;;
    msk) echo 05-msk.yaml ;;
    observability) echo 06-observability.yaml ;;
    workload-iam) echo 07-workload-iam.yaml ;;
    *) return 1 ;;
  esac
}

# Extra Key=Value parameters per stack, one per line (EnvName is always passed).
params_for() {
  case "$1" in
    eks)
      if [ -n "${ADMIN_PRINCIPAL_ARN:-}" ]; then echo "AdminPrincipalArn=$ADMIN_PRINCIPAL_ARN"; fi
      if [ -n "${API_PUBLIC_CIDR:-}" ]; then echo "ApiPublicAccessCidr=$API_PUBLIC_CIDR"; fi ;;
    msk) echo "KafkaVersion=$KAFKA_VERSION" ;;
    observability)
      if [ -n "${ALERT_EMAIL:-}" ]; then echo "AlertEmail=$ALERT_EMAIL"; fi ;;
  esac
  return 0
}

output() {  # output <stack> <OutputKey>
  aws cloudformation describe-stacks --stack-name "${ENV}-medallion-$1" \
    --query "Stacks[0].Outputs[?OutputKey=='$2'].OutputValue" --output text
}

deploy() {
  local stack="$1" file name p
  file="$(file_for "$stack")" || die "unknown stack '$stack' (one of: $ORDER)"
  name="${ENV}-medallion-${stack}"
  local params=("EnvName=$ENV")
  while IFS= read -r p; do [ -n "$p" ] && params+=("$p"); done < <(params_for "$stack")

  if [ "$stack" = msk ]; then
    aws kafka list-kafka-versions --query "KafkaVersions[].Version" --output text | tr '\t' '\n' \
      | grep -qx "$KAFKA_VERSION" \
      || die "Kafka $KAFKA_VERSION is not offered in $AWS_REGION; pick one from: aws kafka list-kafka-versions"
  fi

  echo "==> ${name} (${file})"
  aws cloudformation deploy --stack-name "$name" --template-file "$DIR/$file" \
    --parameter-overrides "${params[@]}" \
    --capabilities CAPABILITY_NAMED_IAM \
    --tags project=medallion env="$ENV" \
    --no-fail-on-empty-changeset \
    ${REVIEW:+--no-execute-changeset}

  if [ "$stack" = observability ] && [ -z "${REVIEW:-}" ]; then load_alert_rules; fi
}

# AMP has no CloudFormation property that reads a file, so the rules are pushed with the CLI.
load_alert_rules() {
  local ws rules="$ROOT/infra/k8s/alerts/medallion-alerts.yaml"
  ws="$(output observability AmpWorkspaceId)"
  if aws amp describe-rule-groups-namespace --workspace-id "$ws" --name medallion >/dev/null 2>&1; then
    aws amp put-rule-groups-namespace --workspace-id "$ws" --name medallion --data "fileb://$rules" >/dev/null
  else
    aws amp create-rule-groups-namespace --workspace-id "$ws" --name medallion --data "fileb://$rules" >/dev/null
  fi
  echo "    alert rules loaded into AMP workspace $ws (namespace medallion)"
}

aws sts get-caller-identity --query Arn --output text >/dev/null \
  || die "no AWS credentials (aws sso login / aws login)"
echo "Account $(aws sts get-caller-identity --query Account --output text), Region $AWS_REGION, ENV=$ENV${REVIEW:+ (change sets only)}"

stacks="${*:-$ORDER}"
for s in $stacks; do deploy "$s"; done

if [ -z "${REVIEW:-}" ]; then
  echo
  echo "Next: docs/how-to/deploy-to-production.md, step 3. Values you'll need:"
  for s in $stacks; do
    aws cloudformation describe-stacks --stack-name "${ENV}-medallion-$s" \
      --query "Stacks[0].Outputs[].[OutputKey,OutputValue]" --output text | sed "s/^/    [$s] /"
  done
fi
