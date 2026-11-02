#!/bin/bash
# Register the five subscription keys for M2~M5. Run this yourself in a terminal; the
# keys are typed at a hidden prompt and never appear in arguments, shell history, logs
# or chat. For each subscription it writes the key to Secrets Manager under the stack's
# prefix (sub-01 already exists empty; the rest are created), then adds the registry row
# that points at the secret (maxConcurrency 5, creditBudget 800 month-to-date).
# Usage, from bench/: AWS_PROFILE=<your-profile> ./register-keys.sh [sub-01 ... sub-05]
set -euo pipefail
cd "$(dirname "$0")"
export AWS_REGION=${AWS_REGION:-us-east-1}
if [ $# -gt 0 ]; then SUBS=("$@"); else SUBS=(sub-01 sub-02 sub-03 sub-04 sub-05); fi
PREFIX=$(aws cloudformation describe-stacks --stack-name KiroFleetData \
  --query "Stacks[0].Outputs[?OutputKey=='SubscriptionSecretPrefix'].OutputValue" --output text)

for sub in "${SUBS[@]}"; do
  name="${PREFIX}${sub}"
  read -rsp "Kiro API key for $sub (hidden): " KIRO_KEY; echo
  [ -n "$KIRO_KEY" ] || { echo "empty key, stopping" >&2; exit 1; }
  KEY_FILE=$(umask 077; mktemp)
  printf '%s' "$KIRO_KEY" > "$KEY_FILE"; unset KIRO_KEY
  if aws secretsmanager describe-secret --secret-id "$name" >/dev/null 2>&1; then
    aws secretsmanager put-secret-value --secret-id "$name" --secret-string "file://$KEY_FILE" >/dev/null
  else
    aws secretsmanager create-secret --name "$name" --secret-string "file://$KEY_FILE" >/dev/null
  fi
  rm -f "$KEY_FILE"
  arn=$(aws secretsmanager describe-secret --secret-id "$name" --query ARN --output text)
  uv run --with boto3 python -m fleetbench.fleetload --aws-profile "${AWS_PROFILE:-default}" \
    register --sub-id "$sub" --secret-ref "$arn"
done
echo "registered: ${SUBS[*]}"
